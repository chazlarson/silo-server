package diagnostics

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"
	"unicode/utf8"
)

const MaxCrashSummaryBytes = 8 * 1024

type ReportState string

const (
	StateReceiving ReportState = "receiving"
	StateReady     ReportState = "ready"
	StateFailed    ReportState = "failed"
)

var (
	ErrNotFound           = errors.New("diagnostic report not found")
	ErrQuotaExceeded      = errors.New("diagnostic report quota exceeded")
	ErrShortIDExhausted   = errors.New("diagnostic short id collision retries exhausted")
	ErrInvalidReportInput = errors.New("invalid diagnostic report input")
	ErrInvalidCursor      = errors.New("invalid diagnostics cursor")
)

type ReportStore interface {
	InsertReceiving(ctx context.Context, input InsertReceivingInput) (InsertReceivingResult, error)
	MarkReady(ctx context.Context, id string, blob BlobInfo) error
	MarkFailed(ctx context.Context, id string) error
	GetByID(ctx context.Context, id string) (*Report, error)
	ListForAdmin(ctx context.Context, filters ListFilters) (ListResult, error)
	DeleteByID(ctx context.Context, id string) (*Report, error)
	RetentionCandidates(ctx context.Context, olderThan time.Time, perUserByteCap int64) ([]Report, error)
	StaleReceiving(ctx context.Context, grace time.Duration) ([]Report, error)
}

type InsertReceivingInput struct {
	UserID                  int
	ProfileID               *string
	CapturedAt              time.Time
	ReportType              string
	Platform                string
	AppVersion              string
	CrashSummary            *string
	Manifest                json.RawMessage
	PlaybackSessionIDs      []string
	ExpectedBlobBytes       int64
	MaxReportsPerUserDay    int
	MaxBytesPerUser         int64
	Now                     time.Time
	ShortIDGenerator        func() (string, error)
	ReportIDGenerator       func() string
	ShortIDCollisionRetries int
}

type InsertReceivingResult struct {
	ID      string
	ShortID string
}

type BlobInfo struct {
	Bucket            string
	Key               string
	Bytes             int64
	UncompressedBytes int64
	SHA256            string
}

type Report struct {
	ID         string      `json:"id"`
	ShortID    string      `json:"short_id"`
	UserID     int         `json:"user_id"`
	ProfileID  *string     `json:"profile_id,omitempty"`
	State      ReportState `json:"state"`
	CapturedAt time.Time   `json:"captured_at"`
	ReceivedAt time.Time   `json:"received_at"`
	ReportType string      `json:"report_type"`
	Platform   string      `json:"platform"`
	AppVersion string      `json:"app_version"`
	// AppBuild is projected from manifest->'report'->>'app_build' so list rows
	// (which omit the full manifest) can still show the build number without
	// dragging every report's manifest JSONB through the DB. Empty when absent.
	AppBuild           string          `json:"app_build"`
	CrashSummary       *string         `json:"crash_summary,omitempty"`
	Manifest           json.RawMessage `json:"manifest,omitempty"`
	PlaybackSessionIDs []string        `json:"playback_session_ids"`
	BlobBucket         *string         `json:"blob_bucket,omitempty"`
	BlobKey            *string         `json:"blob_key,omitempty"`
	BlobBytes          *int64          `json:"blob_bytes,omitempty"`
	UncompressedBytes  *int64          `json:"uncompressed_bytes,omitempty"`
	BlobSHA256         *string         `json:"blob_sha256,omitempty"`
}

type ListFilters struct {
	UserID     *int
	Platform   string
	ReportType string
	From       *time.Time
	To         *time.Time
	ShortID    string
	Limit      int
	Cursor     string
}

type ListResult struct {
	Reports    []Report `json:"reports"`
	NextCursor string   `json:"next_cursor,omitempty"`
}

type QuotaKind string

const (
	QuotaKindReportsPerDay QuotaKind = "reports_per_day"
	QuotaKindBytesPerUser  QuotaKind = "bytes_per_user"
)

type QuotaError struct {
	Kind  QuotaKind
	Limit int64
}

func (e *QuotaError) Error() string {
	if e == nil {
		return ErrQuotaExceeded.Error()
	}
	return fmt.Sprintf("%s: %s limit %d", ErrQuotaExceeded, e.Kind, e.Limit)
}

func (e *QuotaError) Unwrap() error {
	return ErrQuotaExceeded
}

func truncateCrashSummary(summary *string) *string {
	if summary == nil {
		return nil
	}
	value := *summary
	if len(value) <= MaxCrashSummaryBytes {
		return &value
	}
	value = value[:MaxCrashSummaryBytes]
	for !utf8.ValidString(value) && len(value) > 0 {
		value = value[:len(value)-1]
	}
	return &value
}
