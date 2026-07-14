package catalogseed

import (
	"compress/gzip"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"sort"

	"github.com/jackc/pgx/v5"

	"github.com/Silo-Server/silo-server/internal/lang"
)

type streamImport struct {
	s       *Service
	ctx     context.Context
	tx      pgx.Tx
	opts    ImportOptions
	result  *ImportResult
	rewrites []PathRewrite

	// Shared state
	currentWork int
	totalWork   int
	folderPaths map[int][]string
	folderIDMap map[int]int
	itemIDs     map[string]struct{}
	itemStates  map[string]bool
	seasonIDs   map[string]struct{}
	episodeIDs  map[string]struct{}

	// Batch buffers (flushed every 1000 records)
	itemBuf       []ItemRecord
	peopleBuf     []PersonRecord
	embeddingBuf  []EmbeddingRecord
	seasonBuf     []SeasonRecord
	episodeBuf    []EpisodeRecord
	fileBuf       []FileRecord
	linkBuf       []LibraryLinkRecord

	// File validation
	unmatchedFilePaths []string

	reportProgress func(string, int, int)
}

func (s *Service) ImportStream(ctx context.Context, r io.Reader, opts ImportOptions, progress func(ImportProgress)) (*ImportResult, error) {
	if opts.ConflictMode == "" {
		opts.ConflictMode = ConflictModeSkipExisting
	}
	if opts.ConflictMode != ConflictModeSkipExisting && opts.ConflictMode != ConflictModeOverwrite {
		return nil, ErrInvalidConflictMode
	}

	prog := func(msg string, cur, total int) {
		if progress == nil {
			return
		}
		progress(ImportProgress{Message: msg, Current: cur, Total: total})
	}

	prog("Reading catalog bundle", 0, 0)

	gr, err := gzip.NewReader(r)
	if err != nil {
		return nil, fmt.Errorf("%w: opening catalog seed bundle: %v", ErrInvalidBundle, err)
	}
	defer gr.Close()

	dec := json.NewDecoder(gr)

	// Read opening {.
	tok, err := dec.Token()
	if err != nil {
		return nil, fmt.Errorf("%w: reading bundle start: %v", ErrInvalidBundle, err)
	}
	if tok != json.Delim('{') {
		return nil, fmt.Errorf("%w: expected opening brace", ErrInvalidBundle)
	}

	// Decode manifest and libraries first (both are small, fit in memory).
	manifest, libraries, err := decodeManifestAndLibraries(dec)
	if err != nil {
		return nil, err
	}
	if manifest.FormatVersion != CurrentBundleVersion {
		return nil, ErrUnsupportedBundleVersion
	}

	si := &streamImport{
		s:       s,
		ctx:     ctx,
		opts:    opts,
		result:  &ImportResult{},
		rewrites: normalizeRewrites(opts.PathRewrites),
	}

	rewrites := normalizeRewrites(opts.PathRewrites)
	si.folderPaths = make(map[int][]string)
	si.folderIDMap = make(map[int]int)
	si.totalWork = len(libraries)*2 + 100

	preparedLibraries, folderPaths, unmatchedRoots, err := prepareLibraries(libraries, rewrites)
	if err != nil {
		return nil, err
	}
	if len(unmatchedRoots) > 0 {
		sort.Strings(unmatchedRoots)
		return nil, &UnmatchedRootsError{Roots: unmatchedRoots}
	}
	si.folderPaths = folderPaths

	prog("Starting catalog import", 0, si.totalWork)

	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return nil, fmt.Errorf("beginning catalog seed import: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	si.tx = tx

	emptyTarget, err := isEmptyCatalogImportTarget(ctx, tx)
	if err != nil {
		return nil, err
	}

	for _, library := range preparedLibraries {
		localID, created, matched, importErr := importLibrary(ctx, tx, library, opts.ConflictMode == ConflictModeOverwrite)
		if importErr != nil {
			return nil, importErr
		}
		si.folderIDMap[library.ExportedID] = localID
		if created {
			si.result.LibrariesCreated++
		}
		if matched {
			si.result.LibrariesMatched++
		}
		si.currentWork++
		prog("Importing libraries", si.currentWork, si.totalWork)
	}

	si.reportProgress = func(msg string, cur, total int) {
		prog(msg, cur, total)
	}

	// Stream-decode remaining arrays.
	if err := si.streamRemainingArrays(dec, emptyTarget); err != nil {
		return nil, err
	}

	// Post-stream: process people and embeddings (both need to be complete).
	if len(si.peopleBuf) > 0 {
		prog("Importing people", si.currentWork, si.totalWork)
		if err := s.replacePeople(ctx, tx, si.peopleBuf, si.itemStates, opts.ConflictMode, si.result); err != nil {
			return nil, err
		}
	}
	if len(si.embeddingBuf) > 0 {
		if err := s.importEmbeddings(ctx, tx, si.embeddingBuf, emptyTarget, si.result, func(processed int) {
			si.currentWork += processed
			prog("Importing embeddings", si.currentWork, si.totalWork)
		}); err != nil {
			return nil, err
		}
	}

	si.currentWork++
	prog("Finalizing catalog import", si.currentWork, si.totalWork)

	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("committing catalog seed import: %w", err)
	}

	prog("Catalog import completed", si.totalWork, si.totalWork)
	return si.result, nil
}

func (si *streamImport) streamRemainingArrays(dec *json.Decoder, emptyTarget bool) error {
	for dec.More() {
		keyTok, err := dec.Token()
		if err != nil {
			return fmt.Errorf("%w: reading bundle key: %v", ErrInvalidBundle, err)
		}
		key, _ := keyTok.(string)

		switch key {
		case "items":
			if err := dec.Decode(&si.itemBuf); err != nil {
				return fmt.Errorf("%w: decoding items: %v", ErrInvalidBundle, err)
			}
			if err := si.flushItems(emptyTarget); err != nil {
				return err
			}

		case "people":
			if err := dec.Decode(&si.peopleBuf); err != nil {
				return fmt.Errorf("%w: decoding people: %v", ErrInvalidBundle, err)
			}

		case "embeddings":
			if err := dec.Decode(&si.embeddingBuf); err != nil {
				return fmt.Errorf("%w: decoding embeddings: %v", ErrInvalidBundle, err)
			}

		case "seasons":
			if err := dec.Decode(&si.seasonBuf); err != nil {
				return fmt.Errorf("%w: decoding seasons: %v", ErrInvalidBundle, err)
			}
			if err := si.flushSeasons(emptyTarget); err != nil {
				return err
			}

		case "episodes":
			if err := dec.Decode(&si.episodeBuf); err != nil {
				return fmt.Errorf("%w: decoding episodes: %v", ErrInvalidBundle, err)
			}
			if err := si.flushEpisodes(emptyTarget); err != nil {
				return err
			}

		case "files":
			if err := dec.Decode(&si.fileBuf); err != nil {
				return fmt.Errorf("%w: decoding files: %v", ErrInvalidBundle, err)
			}
			if err := si.flushFiles(); err != nil {
				return err
			}

		case "library_links":
			if err := dec.Decode(&si.linkBuf); err != nil {
				return fmt.Errorf("%w: decoding library_links: %v", ErrInvalidBundle, err)
			}
			if err := si.flushLinks(); err != nil {
				return err
			}

		default:
			if err := skipJSONValue(dec); err != nil {
				return err
			}
		}
	}
	return nil
}

func (si *streamImport) flushItems(emptyTarget bool) error {
	if len(si.itemBuf) == 0 {
		return nil
	}
	if si.itemIDs == nil {
		si.itemIDs = make(map[string]struct{})
	}
	if si.itemStates == nil {
		si.itemStates = make(map[string]bool)
	}

	for _, item := range si.itemBuf {
		item.OriginalLanguage = lang.Canonical(item.OriginalLanguage)
		item.Countries = lang.CanonicalCountries(item.Countries)
		si.itemIDs[item.ContentID] = struct{}{}
		si.itemStates[item.ContentID] = true
	}

	rows := make([][]any, 0, len(si.itemBuf))
	for _, item := range si.itemBuf {
		studios, networks, countries, keywords := itemRecordStringArrays(item)
		rows = append(rows, []any{
			item.ContentID, item.Type, item.Title, item.SortTitle, item.OriginalTitle, item.Year, item.Genres,
			item.ContentRating, item.Runtime, item.Overview, item.Tagline,
			item.RatingIMDB, item.RatingTMDB, item.RatingRTCritic, item.RatingRTAudience,
			item.ImdbID, item.TmdbID, item.TvdbID,
			item.PosterPath, item.PosterThumbhash, item.BackdropPath, item.BackdropThumbhash, item.LogoPath,
			item.MetadataS3Path, item.MetadataEtag, item.SeasonCount,
			studios, networks, countries, keywords, item.OriginalLanguage, item.ReleaseDate, item.FirstAirDate, item.LastAirDate, item.AirTime, item.AirTimezone,
			item.MatchedAt, item.LastRefreshed, item.RefreshFailures, item.LockedFields, item.Status,
			item.CreatedAt, item.UpdatedAt,
		})
	}

	if emptyTarget {
		if err := copyInsertBatches(si.ctx, si.tx, "media_items",
			[]string{
				"content_id", "type", "title", "sort_title", "original_title", "year", "genres",
				"content_rating", "runtime", "overview", "tagline",
				"rating_imdb", "rating_tmdb", "rating_rt_critic", "rating_rt_audience",
				"imdb_id", "tmdb_id", "tvdb_id",
				"poster_path", "poster_thumbhash", "backdrop_path", "backdrop_thumbhash", "logo_path",
				"metadata_s3_path", "metadata_etag", "season_count",
				"studios", "networks", "countries", "keywords", "original_language", "release_date", "first_air_date", "last_air_date", "air_time", "air_timezone",
				"matched_at", "last_refreshed", "refresh_failures", "locked_fields", "status",
				"created_at", "updated_at",
			},
			rows, func(processed int) {
				si.currentWork += processed
				si.reportProgress("Importing catalog items", si.currentWork, si.totalWork)
			}); err != nil {
			return fmt.Errorf("copy insert items: %w", err)
		}
		si.result.ItemsCreated += len(si.itemBuf)
	} else {
		// For non-empty targets, use INSERT with ON CONFLICT DO NOTHING.
		affected, err := executeInsertBatchesCounted(si.ctx, si.tx, `INSERT INTO media_items (
			content_id, type, title, sort_title, original_title, year, genres,
			content_rating, runtime, overview, tagline,
			rating_imdb, rating_tmdb, rating_rt_critic, rating_rt_audience,
			imdb_id, tmdb_id, tvdb_id,
			poster_path, poster_thumbhash, backdrop_path, backdrop_thumbhash, logo_path,
			metadata_s3_path, metadata_etag, season_count,
			studios, networks, countries, keywords, original_language, release_date, first_air_date, last_air_date, air_time, air_timezone,
			matched_at, last_refreshed, refresh_failures, locked_fields, status,
			created_at, updated_at
		) VALUES `,
			rows, 43, nil, ` ON CONFLICT (content_id) DO NOTHING`, func(processed int) {
				si.currentWork += processed
				si.reportProgress("Importing catalog items", si.currentWork, si.totalWork)
			})
		if err != nil {
			return err
		}
		si.result.ItemsCreated += int(affected)
	}

	si.itemBuf = si.itemBuf[:0]
	return nil
}

func (si *streamImport) flushSeasons(emptyTarget bool) error {
	if len(si.seasonBuf) == 0 {
		return nil
	}
	if si.seasonIDs == nil {
		si.seasonIDs = make(map[string]struct{})
	}

	rows := make([][]any, 0, len(si.seasonBuf))
	for _, s := range si.seasonBuf {
		if _, ok := si.itemIDs[s.SeriesID]; !ok {
			continue
		}
		si.seasonIDs[s.ContentID] = struct{}{}
		rows = append(rows, []any{
			s.ContentID, s.SeriesID, s.SeasonNumber, s.Title, s.Overview, s.AirDate,
			s.PosterPath, s.PosterThumbhash, s.MetadataS3Path, s.MetadataEtag,
			s.CreatedAt, s.UpdatedAt,
		})
	}
	if len(rows) == 0 {
		si.seasonBuf = si.seasonBuf[:0]
		return nil
	}

	if emptyTarget {
		if err := copyInsertBatches(si.ctx, si.tx, "seasons",
			[]string{
				"content_id", "series_id", "season_number", "title", "overview", "air_date",
				"poster_path", "poster_thumbhash", "metadata_s3_path", "metadata_etag", "created_at", "updated_at",
			},
			rows, func(processed int) {
				si.currentWork += processed
				si.reportProgress("Importing seasons", si.currentWork, si.totalWork)
			}); err != nil {
			return fmt.Errorf("copy insert seasons: %w", err)
		}
		si.result.SeasonsCreated += len(rows)
	} else {
		counts, err := executeUpsertBatches(si.ctx, si.tx, `INSERT INTO seasons (
			content_id, series_id, season_number, title, overview, air_date,
			poster_path, poster_thumbhash, metadata_s3_path, metadata_etag, created_at, updated_at
		) VALUES `,
			rows, 12, nil,
			` ON CONFLICT (content_id) DO NOTHING`,
			` ON CONFLICT (content_id) DO UPDATE SET
				series_id = EXCLUDED.series_id,
				season_number = EXCLUDED.season_number,
				title = EXCLUDED.title,
				overview = EXCLUDED.overview,
				air_date = EXCLUDED.air_date,
				poster_path = EXCLUDED.poster_path,
				poster_thumbhash = EXCLUDED.poster_thumbhash,
				metadata_s3_path = EXCLUDED.metadata_s3_path,
				metadata_etag = EXCLUDED.metadata_etag,
				updated_at = EXCLUDED.updated_at`,
			si.opts.ConflictMode, func(processed int) {
				si.currentWork += processed
				si.reportProgress("Importing seasons", si.currentWork, si.totalWork)
			})
		if err != nil {
			return err
		}
		si.result.SeasonsCreated += counts.Created
		si.result.SeasonsUpdated += counts.Updated
		si.result.Skipped += counts.Skipped
	}

	si.seasonBuf = si.seasonBuf[:0]
	return nil
}

func (si *streamImport) flushEpisodes(emptyTarget bool) error {
	if len(si.episodeBuf) == 0 {
		return nil
	}
	if si.episodeIDs == nil {
		si.episodeIDs = make(map[string]struct{})
	}

	rows := make([][]any, 0, len(si.episodeBuf))
	for _, ep := range si.episodeBuf {
		if _, ok := si.itemIDs[ep.SeriesID]; !ok {
			continue
		}
		if ep.SeasonID != "" {
			if _, ok := si.seasonIDs[ep.SeasonID]; !ok {
				continue
			}
		}
		si.episodeIDs[ep.ContentID] = struct{}{}
		rows = append(rows, []any{
			ep.ContentID, ep.SeriesID, nullableString(ep.SeasonID), ep.SeasonNumber, ep.EpisodeNumber,
			ep.Title, ep.Overview, ep.AirDate, ep.Runtime, ep.RatingIMDB, ep.RatingTMDB,
			ep.StillPath, ep.StillThumbhash, ep.MetadataS3Path, ep.MetadataEtag,
			ep.CreatedAt, ep.UpdatedAt,
		})
	}
	if len(rows) == 0 {
		si.episodeBuf = si.episodeBuf[:0]
		return nil
	}

	if emptyTarget {
		if err := copyInsertBatches(si.ctx, si.tx, "episodes",
			[]string{
				"content_id", "series_id", "season_id", "season_number", "episode_number",
				"title", "overview", "air_date", "runtime", "rating_imdb", "rating_tmdb",
				"still_path", "still_thumbhash", "metadata_s3_path", "metadata_etag", "created_at", "updated_at",
			},
			rows, func(processed int) {
				si.currentWork += processed
				si.reportProgress("Importing episodes", si.currentWork, si.totalWork)
			}); err != nil {
			return fmt.Errorf("copy insert episodes: %w", err)
		}
		si.result.EpisodesCreated += len(rows)
	} else {
		counts, err := executeUpsertBatches(si.ctx, si.tx, `INSERT INTO episodes (
			content_id, series_id, season_id, season_number, episode_number,
			title, overview, air_date, runtime, rating_imdb, rating_tmdb,
			still_path, still_thumbhash, metadata_s3_path, metadata_etag, created_at, updated_at
		) VALUES `,
			rows, 17, nil,
			` ON CONFLICT (content_id) DO NOTHING`,
			` ON CONFLICT (content_id) DO UPDATE SET
				series_id = EXCLUDED.series_id,
				season_id = EXCLUDED.season_id,
				season_number = EXCLUDED.season_number,
				episode_number = EXCLUDED.episode_number,
				title = EXCLUDED.title,
				overview = EXCLUDED.overview,
				air_date = EXCLUDED.air_date,
				runtime = EXCLUDED.runtime,
				rating_imdb = EXCLUDED.rating_imdb,
				rating_tmdb = EXCLUDED.rating_tmdb,
				still_path = EXCLUDED.still_path,
				still_thumbhash = EXCLUDED.still_thumbhash,
				metadata_s3_path = EXCLUDED.metadata_s3_path,
				metadata_etag = EXCLUDED.metadata_etag,
				updated_at = EXCLUDED.updated_at`,
			si.opts.ConflictMode, func(processed int) {
				si.currentWork += processed
				si.reportProgress("Importing episodes", si.currentWork, si.totalWork)
			})
		if err != nil {
			return err
		}
		si.result.EpisodesCreated += counts.Created
		si.result.EpisodesUpdated += counts.Updated
		si.result.Skipped += counts.Skipped
	}

	si.episodeBuf = si.episodeBuf[:0]
	return nil
}

func (si *streamImport) flushFiles() error {
	if len(si.fileBuf) == 0 {
		return nil
	}

	rows := make([][]any, 0, len(si.fileBuf))
	for _, file := range si.fileBuf {
		if file.ContentID != "" {
			if _, ok := si.itemIDs[file.ContentID]; !ok {
				continue
			}
		}
		if file.EpisodeID != "" {
			if _, ok := si.episodeIDs[file.EpisodeID]; !ok {
				continue
			}
		}
		rewritten, rewriteErr := rewriteFileRecord(file, si.rewrites, si.folderPaths)
		if rewriteErr != nil {
			return rewriteErr
		}
		rewritten.MediaFolderID = si.folderIDMap[file.MediaFolderID]

		// Validate file root.
		ok := false
		paths := si.folderPaths[rewritten.MediaFolderID]
		for _, root := range paths {
			if hasPathPrefix(rewritten.FilePath, root) {
				ok = true
				break
			}
		}
		if !ok {
			si.unmatchedFilePaths = append(si.unmatchedFilePaths, rewritten.FilePath)
			continue
		}

		videoTracksJSON, _ := json.Marshal(rewritten.VideoTracks)
		audioTracksJSON, _ := json.Marshal(rewritten.AudioTracks)
		subtitleTracksJSON, _ := json.Marshal(rewritten.SubtitleTracks)
		externalSubtitlesJSON, _ := json.Marshal(rewritten.ExternalSubtitles)
		chaptersJSON, _ := json.Marshal(rewritten.Chapters)
		rows = append(rows, []any{
			nullableString(rewritten.ContentID), nullableString(rewritten.EpisodeID), nilIfZero(rewritten.SeasonNumber), nilIfZero(rewritten.EpisodeNumber),
			rewritten.MediaFolderID, rewritten.FilePath, rewritten.FileSize, nullableString(rewritten.FileHash),
			rewritten.CodecVideo, rewritten.CodecAudio, rewritten.Resolution, nilIfZero(rewritten.AudioChannels), rewritten.HDR, rewritten.Container,
			nilIfZero(rewritten.Duration), nilIfZero(rewritten.Bitrate), videoTracksJSON, audioTracksJSON, subtitleTracksJSON, externalSubtitlesJSON,
			chaptersJSON,
			rewritten.IntroStart, rewritten.IntroEnd, rewritten.CreditsStart, rewritten.CreditsEnd,
			nullableString(rewritten.ProbeSource), rewritten.ProbeUpdatedAt, rewritten.MissingSince, rewritten.CreatedAt, rewritten.UpdatedAt,
		})
	}
	if len(rows) == 0 {
		si.fileBuf = si.fileBuf[:0]
		return nil
	}

	emptyTarget := si.result.ItemsCreated > 0 && si.result.ItemsUpdated == 0 && si.result.Skipped == 0
	if emptyTarget {
		if err := copyInsertBatches(si.ctx, si.tx, "media_files",
			[]string{
				"content_id", "episode_id", "season_number", "episode_number",
				"media_folder_id", "file_path", "file_size", "file_hash",
				"codec_video", "codec_audio", "resolution", "audio_channels", "hdr", "container",
				"duration", "bitrate", "video_tracks", "audio_tracks", "subtitle_tracks", "external_subtitles",
				"chapters",
				"intro_start", "intro_end", "credits_start", "credits_end",
				"probe_source", "probe_updated_at", "missing_since", "created_at", "updated_at",
			},
			rows, func(processed int) {
				si.currentWork += processed
				si.reportProgress("Importing media files", si.currentWork, si.totalWork)
			}); err != nil {
			return fmt.Errorf("copy insert files: %w", err)
		}
		si.result.FilesCreated += len(rows)
	} else {
		fileRows := make([][]any, len(rows))
		copy(fileRows, rows)
		counts, err := executeUpsertBatches(si.ctx, si.tx, `INSERT INTO media_files (
			content_id, episode_id, season_number, episode_number,
			media_folder_id, file_path, file_size, file_hash,
			codec_video, codec_audio, resolution, audio_channels, hdr, container,
			duration, bitrate, video_tracks, audio_tracks, subtitle_tracks, external_subtitles,
			chapters,
			intro_start, intro_end, credits_start, credits_end,
			probe_source, probe_updated_at, missing_since, created_at, updated_at
		) VALUES `,
			fileRows, 30,
			map[int]string{16: "::jsonb", 17: "::jsonb", 18: "::jsonb", 19: "::jsonb", 20: "::jsonb"},
			` ON CONFLICT (file_path) DO NOTHING`,
			` ON CONFLICT (file_path) DO UPDATE SET
				content_id = EXCLUDED.content_id,
				episode_id = EXCLUDED.episode_id,
				season_number = EXCLUDED.season_number,
				episode_number = EXCLUDED.episode_number,
				media_folder_id = EXCLUDED.media_folder_id,
				file_size = EXCLUDED.file_size,
				file_hash = EXCLUDED.file_hash,
				codec_video = EXCLUDED.codec_video,
				codec_audio = EXCLUDED.codec_audio,
				resolution = EXCLUDED.resolution,
				audio_channels = EXCLUDED.audio_channels,
				hdr = EXCLUDED.hdr,
				container = EXCLUDED.container,
				duration = EXCLUDED.duration,
				bitrate = EXCLUDED.bitrate,
				video_tracks = EXCLUDED.video_tracks,
				audio_tracks = EXCLUDED.audio_tracks,
				subtitle_tracks = EXCLUDED.subtitle_tracks,
				external_subtitles = EXCLUDED.external_subtitles,
				chapters = EXCLUDED.chapters,
				intro_start = EXCLUDED.intro_start,
				intro_end = EXCLUDED.intro_end,
				credits_start = EXCLUDED.credits_start,
				credits_end = EXCLUDED.credits_end,
				probe_source = EXCLUDED.probe_source,
				probe_updated_at = EXCLUDED.probe_updated_at,
				missing_since = EXCLUDED.missing_since,
				updated_at = EXCLUDED.updated_at`,
			si.opts.ConflictMode, func(processed int) {
				si.currentWork += processed
				si.reportProgress("Importing media files", si.currentWork, si.totalWork)
			})
		if err != nil {
			return err
		}
		si.result.FilesCreated += counts.Created
		si.result.FilesUpdated += counts.Updated
		si.result.Skipped += counts.Skipped
	}

	si.fileBuf = si.fileBuf[:0]
	return nil
}

func (si *streamImport) flushLinks() error {
	if len(si.linkBuf) == 0 {
		return nil
	}

	rows := make([][]any, 0, len(si.linkBuf))
	for _, link := range si.linkBuf {
		localID, ok := si.folderIDMap[link.MediaFolderID]
		if !ok {
			continue
		}
		if _, ok := si.itemIDs[link.ContentID]; !ok {
			continue
		}
		rows = append(rows, []any{link.ContentID, localID, link.FirstSeenAt})
	}
	if len(rows) == 0 {
		si.linkBuf = si.linkBuf[:0]
		return nil
	}

	affected, err := executeInsertBatchesCounted(si.ctx, si.tx, `INSERT INTO media_item_libraries (content_id, media_folder_id, first_seen_at) VALUES `,
		rows, 3, nil, ` ON CONFLICT (content_id, media_folder_id) DO NOTHING`, func(processed int) {
			si.currentWork += processed
			si.reportProgress("Linking items to libraries", si.currentWork, si.totalWork)
		})
	if err != nil {
		return fmt.Errorf("insert library links: %w", err)
	}
	si.result.LinksCreated += int(affected)
	si.linkBuf = si.linkBuf[:0]
	return nil
}

// decodeManifestAndLibraries reads the manifest and libraries arrays from the
// top-level JSON object stream. Both are small enough to fit in memory.
func decodeManifestAndLibraries(dec *json.Decoder) (Manifest, []LibraryRecord, error) {
	var manifest Manifest
	var libraries []LibraryRecord

	for dec.More() {
		keyTok, err := dec.Token()
		if err != nil {
			return manifest, nil, fmt.Errorf("%w: reading bundle key: %v", ErrInvalidBundle, err)
		}
		key, _ := keyTok.(string)

		switch key {
		case "manifest":
			if err := dec.Decode(&manifest); err != nil {
				return manifest, nil, fmt.Errorf("%w: decoding manifest: %v", ErrInvalidBundle, err)
			}
		case "libraries":
			if err := dec.Decode(&libraries); err != nil {
				return manifest, nil, fmt.Errorf("%w: decoding libraries: %v", ErrInvalidBundle, err)
			}
			return manifest, libraries, nil
		default:
			if err := skipJSONValue(dec); err != nil {
				return manifest, nil, err
			}
		}
	}
	return manifest, libraries, nil
}

// skipJSONValue consumes one JSON value from the decoder without allocating
// a Go struct for it.
func skipJSONValue(dec *json.Decoder) error {
	tok, err := dec.Token()
	if err != nil {
		return err
	}
	switch tok {
	case json.Delim('{'):
		for dec.More() {
			if _, err := dec.Token(); err != nil {
				return err
			}
			if err := skipJSONValue(dec); err != nil {
				return err
			}
		}
		if _, err := dec.Token(); err != nil {
			return err
		}
	case json.Delim('['):
		for dec.More() {
			if err := skipJSONValue(dec); err != nil {
				return err
			}
		}
		if _, err := dec.Token(); err != nil {
			return err
		}
	}
	return nil
}
