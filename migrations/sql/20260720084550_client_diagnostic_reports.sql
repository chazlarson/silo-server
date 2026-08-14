-- +goose Up
CREATE TABLE client_diagnostic_reports (
  id            UUID PRIMARY KEY,
  short_id      TEXT NOT NULL,
  user_id       INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
  profile_id    TEXT,
  state         TEXT NOT NULL CHECK (state IN ('receiving', 'ready', 'failed')),
  captured_at   TIMESTAMPTZ NOT NULL,
  received_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
  report_type   TEXT NOT NULL CHECK (report_type IN ('crash', 'anr', 'native_crash', 'hang', 'abnormal_exit', 'manual')),
  platform      TEXT NOT NULL CHECK (platform IN ('android', 'android-tv', 'ios', 'tvos')),
  app_version   TEXT NOT NULL,
  crash_summary TEXT,
  manifest      JSONB NOT NULL,
  playback_session_ids TEXT[] NOT NULL DEFAULT '{}',
  blob_bucket   TEXT,
  blob_key      TEXT,
  blob_bytes    BIGINT,
  uncompressed_bytes BIGINT,
  blob_sha256   TEXT
);

CREATE UNIQUE INDEX client_diagnostic_reports_short_id_lower_idx
    ON client_diagnostic_reports (lower(short_id));
CREATE INDEX client_diagnostic_reports_user_received_idx
    ON client_diagnostic_reports (user_id, received_at DESC);
CREATE INDEX client_diagnostic_reports_received_idx
    ON client_diagnostic_reports (received_at);

-- +goose Down
DROP TABLE IF EXISTS client_diagnostic_reports;
