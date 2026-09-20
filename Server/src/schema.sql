CREATE TABLE IF NOT EXISTS pairing_codes (
    code_hash TEXT PRIMARY KEY,
    expires_at TEXT NOT NULL,
    used_at TEXT
);
CREATE TABLE IF NOT EXISTS devices (
    device_id TEXT PRIMARY KEY,
    token_hash TEXT NOT NULL UNIQUE,
    created_at TEXT NOT NULL,
    revoked_at TEXT
);
CREATE TABLE IF NOT EXISTS events (
    device_id TEXT NOT NULL,
    event_id TEXT NOT NULL,
    revision INTEGER NOT NULL,
    operation TEXT NOT NULL,
    kind TEXT NOT NULL,
    health_type TEXT NOT NULL,
    source_json TEXT,
    start_utc TEXT,
    end_utc TEXT,
    value REAL,
    unit TEXT,
    payload_json TEXT NOT NULL,
    payload_hash TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    PRIMARY KEY(device_id, event_id),
    FOREIGN KEY(device_id) REFERENCES devices(device_id)
);
CREATE INDEX IF NOT EXISTS events_type ON events(device_id, health_type, operation);
CREATE TABLE IF NOT EXISTS receipts (
    commit_sequence INTEGER PRIMARY KEY AUTOINCREMENT,
    batch_id TEXT NOT NULL UNIQUE,
    device_id TEXT NOT NULL,
    content_hash TEXT NOT NULL,
    accepted_events INTEGER NOT NULL,
    changed_events INTEGER NOT NULL,
    requires_projection INTEGER NOT NULL DEFAULT 0,
    received_at TEXT NOT NULL,
    projected_at TEXT,
    projected_generation TEXT,
    projection_mapping_version INTEGER,
    FOREIGN KEY(device_id) REFERENCES devices(device_id)
);
CREATE INDEX IF NOT EXISTS receipts_device ON receipts(device_id, commit_sequence);
CREATE TABLE IF NOT EXISTS audit (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    device_id TEXT NOT NULL,
    action TEXT NOT NULL,
    batch_id TEXT,
    at TEXT NOT NULL,
    detail TEXT,
    FOREIGN KEY(device_id) REFERENCES devices(device_id)
);
CREATE TABLE IF NOT EXISTS outbox (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    device_id TEXT NOT NULL,
    batch_id TEXT NOT NULL,
    metric_name TEXT NOT NULL,
    created_at TEXT NOT NULL,
    processed_at TEXT,
    attempts INTEGER NOT NULL DEFAULT 0,
    last_error TEXT,
    FOREIGN KEY(device_id) REFERENCES devices(device_id),
    FOREIGN KEY(batch_id) REFERENCES receipts(batch_id)
);
CREATE INDEX IF NOT EXISTS outbox_pending ON outbox(processed_at, device_id, metric_name);
CREATE TABLE IF NOT EXISTS erasures (
    device_id TEXT PRIMARY KEY,
    erasure_id TEXT NOT NULL UNIQUE,
    erasure_secret_hash TEXT NOT NULL UNIQUE,
    requested_at TEXT NOT NULL,
    metrics_deleted_at TEXT,
    backups_expired_at TEXT,
    backup_delete_by TEXT NOT NULL,
    last_error TEXT
);

CREATE TABLE IF NOT EXISTS storage_meta (
    singleton INTEGER PRIMARY KEY CHECK(singleton = 1),
    control_store_id TEXT NOT NULL,
    created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS projection_state (
    singleton INTEGER PRIMARY KEY CHECK(singleton = 1),
    generation_id TEXT NOT NULL,
    mapping_version INTEGER NOT NULL,
    storage_identity TEXT NOT NULL,
    updated_at TEXT NOT NULL
);
