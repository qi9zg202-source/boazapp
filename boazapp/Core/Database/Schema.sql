PRAGMA foreign_keys = ON;
CREATE TABLE IF NOT EXISTS health_events (
  event_id TEXT PRIMARY KEY,
  revision INTEGER NOT NULL CHECK (revision > 0),
  operation TEXT NOT NULL CHECK (operation IN ('upsert', 'delete')),
  kind TEXT NOT NULL,
  type_identifier TEXT NOT NULL,
  start_utc TEXT,
  end_utc TEXT,
  value REAL,
  unit TEXT,
  payload BLOB NOT NULL,
  content_hash TEXT NOT NULL,
  updated_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS health_events_type_time ON health_events(type_identifier, start_utc DESC);
-- Cover only workout heart-rate readings. A global extra index would tax every
-- HealthKit event write while the dashboard reads at most 20 workout IDs.
CREATE INDEX IF NOT EXISTS health_events_workout_hr_id ON health_events(event_id, value)
  WHERE operation='upsert' AND type_identifier='boaz.workout.heart_rate';
CREATE TABLE IF NOT EXISTS query_anchors (
  type_identifier TEXT PRIMARY KEY,
  anchor BLOB NOT NULL,
  updated_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS upload_outbox (
  event_id TEXT NOT NULL,
  revision INTEGER NOT NULL,
  state TEXT NOT NULL CHECK (state IN ('pending', 'sending', 'cloud_saved', 'metrics_current')),
  batch_id TEXT,
  attempts INTEGER NOT NULL DEFAULT 0,
  next_attempt_at TEXT,
  last_error TEXT,
  updated_at TEXT NOT NULL,
  PRIMARY KEY (event_id, revision),
  FOREIGN KEY (event_id) REFERENCES health_events(event_id) ON DELETE CASCADE
);
CREATE INDEX IF NOT EXISTS upload_outbox_state ON upload_outbox(state, next_attempt_at);
CREATE TABLE IF NOT EXISTS upload_batches (
  batch_id TEXT PRIMARY KEY,
  body BLOB NOT NULL,
  body_hash TEXT NOT NULL,
  state TEXT NOT NULL CHECK (state IN ('pending', 'cloud_saved', 'metrics_current')),
  receipt BLOB,
  attempts INTEGER NOT NULL DEFAULT 0,
  next_attempt_at TEXT,
  last_error TEXT,
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS sync_audit_log (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  phase TEXT NOT NULL,
  outcome TEXT NOT NULL,
  detail TEXT NOT NULL,
  event_count INTEGER NOT NULL DEFAULT 0,
  created_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS workout_detail_jobs (
  workout_id TEXT PRIMARY KEY,
  event_offset INTEGER NOT NULL DEFAULT 0,
  events_done INTEGER NOT NULL DEFAULT 0,
  heart_rate_done INTEGER NOT NULL DEFAULT 0,
  updated_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS local_control_state (
  key TEXT PRIMARY KEY,
  value TEXT NOT NULL
);
-- v2: every writer, including a separate SQLite connection, advances the
-- history generation. Readers compare it between paged sleep-history reads.
CREATE TABLE IF NOT EXISTS local_change_clock (
  id INTEGER PRIMARY KEY CHECK (id = 1),
  generation INTEGER NOT NULL CHECK (generation >= 0)
);
INSERT OR IGNORE INTO local_change_clock(id, generation) VALUES (1, 0);
CREATE TRIGGER IF NOT EXISTS health_events_clock_insert AFTER INSERT ON health_events
BEGIN
  UPDATE local_change_clock SET generation = generation + 1 WHERE id = 1;
END;
CREATE TRIGGER IF NOT EXISTS health_events_clock_update AFTER UPDATE ON health_events
BEGIN
  UPDATE local_change_clock SET generation = generation + 1 WHERE id = 1;
END;
CREATE TRIGGER IF NOT EXISTS health_events_clock_delete AFTER DELETE ON health_events
BEGIN
  UPDATE local_change_clock SET generation = generation + 1 WHERE id = 1;
END;
