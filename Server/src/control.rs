use crate::{
    ack_journal::AckJournal,
    custody::{CustodyClient, CustodyReservationV2, CustodyState},
};
use chrono::{DateTime, Utc};
use fs2::FileExt;
use rusqlite::{Connection, OpenFlags, OptionalExtension, TransactionBehavior, params};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{
    collections::{BTreeMap, BTreeSet},
    fmt,
    fs::{self, File, OpenOptions},
    io::{self, Read, Write},
    path::{Path, PathBuf},
    sync::Arc,
    time::Duration,
};

pub const CONTROL_APPLICATION_ID: i64 = 0x425A4843; // BZHC
pub const CONTROL_SCHEMA_VERSION: i64 = 3;
const LEGACY_CONTROL_SCHEMA_VERSION: i64 = 1;
const CONTROL_SCHEMA_V2_VERSION: i64 = 2;
const GENESIS_HASH: &str = "0000000000000000000000000000000000000000000000000000000000000000";

const CONTROL_SCHEMA_V1: &str = r#"
CREATE TABLE control_meta (
    singleton INTEGER PRIMARY KEY CHECK(singleton = 1),
    store_id TEXT NOT NULL UNIQUE,
    created_at TEXT NOT NULL
);
CREATE TABLE control_events (
    sequence INTEGER PRIMARY KEY AUTOINCREMENT,
    event_id TEXT NOT NULL UNIQUE,
    event_type TEXT NOT NULL CHECK(event_type IN (
        'credential_revoked','erasure_intent','health_erasure_verified',
        'metrics_erasure_verified','backups_expired_verified',
        'backup_created','backup_deleted','restore_started',
        'restore_replayed','restore_completed','legacy_imported'
    )),
    device_id TEXT,
    token_hash TEXT,
    erasure_id TEXT,
    secret_hash TEXT,
    snapshot_id TEXT,
    restore_epoch TEXT,
    occurred_at TEXT NOT NULL,
    deadline_at TEXT,
    previous_hash TEXT NOT NULL,
    current_hash TEXT NOT NULL UNIQUE
);
CREATE INDEX control_events_device ON control_events(device_id, sequence);
CREATE INDEX control_events_erasure ON control_events(erasure_id, sequence);
CREATE INDEX control_events_token_type ON control_events(token_hash, event_type);
CREATE INDEX control_events_type_sequence ON control_events(event_type, sequence);
"#;

const CONTROL_SCHEMA_V2: &str = r#"
CREATE TABLE control_meta (
    singleton INTEGER PRIMARY KEY CHECK(singleton = 1),
    store_id TEXT NOT NULL UNIQUE,
    created_at TEXT NOT NULL
);
CREATE TABLE control_events (
    sequence INTEGER PRIMARY KEY AUTOINCREMENT,
    event_id TEXT NOT NULL UNIQUE,
    event_type TEXT NOT NULL CHECK(event_type IN (
        'credential_revoked','erasure_intent','health_erasure_verified',
        'metrics_erasure_verified','backups_expired_verified',
        'backup_created','backup_delete_intent','backup_deleted','restore_started',
        'restore_replayed','restore_completed','legacy_imported'
    )),
    device_id TEXT,
    token_hash TEXT,
    erasure_id TEXT,
    secret_hash TEXT,
    snapshot_id TEXT,
    restore_epoch TEXT,
    occurred_at TEXT NOT NULL,
    deadline_at TEXT,
    previous_hash TEXT NOT NULL,
    current_hash TEXT NOT NULL UNIQUE,
    hash_version INTEGER NOT NULL DEFAULT 2 CHECK(hash_version IN (1,2)),
    evidence_digest TEXT,
    artifact_name TEXT
);
CREATE INDEX control_events_device ON control_events(device_id, sequence);
CREATE INDEX control_events_erasure ON control_events(erasure_id, sequence);
CREATE INDEX control_events_token_type ON control_events(token_hash, event_type);
CREATE INDEX control_events_type_sequence ON control_events(event_type, sequence);
"#;

// v3 does not copy or discard the historical table. The old v1/v2 table is
// renamed into an immutable prefix; all new facts are appended to a separate
// suffix. The view preserves the existing read-only query contract.
const CONTROL_SCHEMA_V3_TAIL: &str = r#"
CREATE TABLE control_layout (
    singleton INTEGER PRIMARY KEY CHECK(singleton = 1),
    prefix_version INTEGER NOT NULL CHECK(prefix_version IN (1,2))
);
CREATE TABLE control_events_suffix (
    sequence INTEGER PRIMARY KEY,
    event_id TEXT NOT NULL UNIQUE,
    event_type TEXT NOT NULL CHECK(event_type IN (
        'credential_revoked','erasure_intent','health_erasure_verified',
        'metrics_erasure_verified','backups_expired_verified',
        'backup_created','backup_delete_intent','backup_deleted','restore_started',
        'restore_replayed','restore_completed','legacy_imported'
    )),
    device_id TEXT,
    token_hash TEXT,
    erasure_id TEXT,
    secret_hash TEXT,
    snapshot_id TEXT,
    restore_epoch TEXT,
    occurred_at TEXT NOT NULL,
    deadline_at TEXT,
    previous_hash TEXT NOT NULL,
    current_hash TEXT NOT NULL UNIQUE,
    hash_version INTEGER NOT NULL DEFAULT 2 CHECK(hash_version = 2),
    evidence_digest TEXT,
    artifact_name TEXT
);
CREATE INDEX control_events_suffix_device ON control_events_suffix(device_id, sequence);
CREATE INDEX control_events_suffix_erasure ON control_events_suffix(erasure_id, sequence);
CREATE INDEX control_events_suffix_token_type ON control_events_suffix(token_hash, event_type);
CREATE INDEX control_events_suffix_type_sequence ON control_events_suffix(event_type, sequence);
CREATE TRIGGER control_events_prefix_no_insert BEFORE INSERT ON control_events_prefix
BEGIN SELECT RAISE(ABORT, 'control history is immutable'); END;
CREATE TRIGGER control_events_prefix_no_update BEFORE UPDATE ON control_events_prefix
BEGIN SELECT RAISE(ABORT, 'control history is immutable'); END;
CREATE TRIGGER control_events_prefix_no_delete BEFORE DELETE ON control_events_prefix
BEGIN SELECT RAISE(ABORT, 'control history is immutable'); END;
CREATE TRIGGER control_events_suffix_no_update BEFORE UPDATE ON control_events_suffix
BEGIN SELECT RAISE(ABORT, 'control history is append-only'); END;
CREATE TRIGGER control_events_suffix_no_delete BEFORE DELETE ON control_events_suffix
BEGIN SELECT RAISE(ABORT, 'control history is append-only'); END;
CREATE TRIGGER control_events_suffix_guard BEFORE INSERT ON control_events_suffix
BEGIN
    SELECT RAISE(ABORT, 'duplicate historical event ID')
      WHERE EXISTS (SELECT 1 FROM control_events_prefix WHERE event_id=NEW.event_id);
    SELECT RAISE(ABORT, 'duplicate historical event hash')
      WHERE EXISTS (SELECT 1 FROM control_events_prefix WHERE current_hash=NEW.current_hash);
    SELECT RAISE(ABORT, 'control sequence is not contiguous')
      WHERE NEW.sequence != COALESCE((SELECT MAX(sequence) FROM control_events), 0) + 1;
END;
"#;

fn create_v3_objects(connection: &Connection, prefix_version: i64) -> ControlResult<()> {
    if prefix_version != LEGACY_CONTROL_SCHEMA_VERSION
        && prefix_version != CONTROL_SCHEMA_V2_VERSION
    {
        return Err(ControlError::Invalid(
            "unsupported control prefix".to_owned(),
        ));
    }
    connection.execute_batch("ALTER TABLE control_events RENAME TO control_events_prefix;")?;
    // A v1 prefix has no v2 metadata columns. NULLs in the compatibility view
    // preserve the exact original v1 hash preimage.
    let prefix_columns = if prefix_version == LEGACY_CONTROL_SCHEMA_VERSION {
        "1 AS hash_version,NULL AS evidence_digest,NULL AS artifact_name"
    } else {
        "hash_version,evidence_digest,artifact_name"
    };
    connection.execute_batch(&format!(
        "CREATE VIEW control_events AS
         SELECT sequence,event_id,event_type,device_id,token_hash,erasure_id,secret_hash,
                snapshot_id,restore_epoch,occurred_at,deadline_at,previous_hash,current_hash,
                {prefix_columns} FROM control_events_prefix
         UNION ALL
         SELECT sequence,event_id,event_type,device_id,token_hash,erasure_id,secret_hash,
                snapshot_id,restore_epoch,occurred_at,deadline_at,previous_hash,current_hash,
                hash_version,evidence_digest,artifact_name FROM control_events_suffix;"
    ))?;
    connection.execute_batch(CONTROL_SCHEMA_V3_TAIL)?;
    connection.execute(
        "INSERT INTO control_layout(singleton,prefix_version) VALUES (1,?1)",
        [prefix_version],
    )?;
    Ok(())
}

#[derive(Debug)]
pub enum ControlError {
    Invalid(String),
    Integrity(String),
    Sql(rusqlite::Error),
    Io(io::Error),
    Json(serde_json::Error),
}

impl fmt::Display for ControlError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Invalid(message) => write!(formatter, "control store is incompatible: {message}"),
            Self::Integrity(message) => {
                write!(formatter, "control chain integrity failed: {message}")
            }
            Self::Sql(error) => write!(formatter, "control database operation failed: {error}"),
            Self::Io(error) => write!(formatter, "control store I/O failed: {error}"),
            Self::Json(error) => write!(formatter, "control record is invalid: {error}"),
        }
    }
}

impl std::error::Error for ControlError {}

impl From<rusqlite::Error> for ControlError {
    fn from(value: rusqlite::Error) -> Self {
        Self::Sql(value)
    }
}

impl From<io::Error> for ControlError {
    fn from(value: io::Error) -> Self {
        Self::Io(value)
    }
}

impl From<serde_json::Error> for ControlError {
    fn from(value: serde_json::Error) -> Self {
        Self::Json(value)
    }
}

pub type ControlResult<T> = Result<T, ControlError>;

#[derive(Clone)]
pub struct ControlStore {
    db_path: PathBuf,
    head_path: PathBuf,
    mirror_dir: PathBuf,
    publication_lock_path: PathBuf,
    custody: Option<Arc<dyn CustodyClient>>,
    ack_journal: Option<Arc<AckJournal>>,
    custody_lock_path: Option<PathBuf>,
    pending_intent_path: PathBuf,
    managed_backup_root: Option<PathBuf>,
}

impl fmt::Debug for ControlStore {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ControlStore")
            .field("db_path", &self.db_path)
            .field("head_path", &self.head_path)
            .field("mirror_dir", &self.mirror_dir)
            .field("custody_enabled", &self.custody.is_some())
            .finish()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PendingErasure {
    pub device_id: String,
    pub erasure_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ErasureIntentAuth {
    pub device_id: String,
    pub token_hash: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CertifiedErasureFacts {
    pub auth: ErasureIntentAuth,
    pub requested_at: String,
    pub backup_delete_by: String,
    pub metrics_deleted_at: Option<String>,
    pub backups_expired_at: Option<String>,
}

#[derive(Debug, Clone)]
struct PendingHealthErasure {
    device_id: String,
    erasure_id: String,
    secret_hash: String,
    requested_at: String,
    deadline_at: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
struct ControlHead {
    store_id: String,
    sequence: i64,
    current_hash: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ControlCheckpoint {
    pub store_id: String,
    pub sequence: i64,
    pub current_hash: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ActiveBackup {
    pub snapshot_id: String,
    pub file_sha256: String,
    pub occurred_at: String,
    pub prior_checkpoint: ControlCheckpoint,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PendingBackupDelete {
    pub snapshot_id: String,
    pub file_sha256: String,
    pub artifact_name: String,
    pub occurred_at: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RestoreReplayReport {
    pub revoked_devices: usize,
    pub erased_devices: usize,
    pub cleared_pairing_codes: usize,
}

#[derive(Debug, Clone)]
struct RestoreErasure {
    device_id: String,
    erasure_id: String,
    secret_hash: String,
    requested_at: String,
    deadline_at: String,
    metrics_deleted_at: Option<String>,
    backups_expired_at: Option<String>,
    device_id_already_anonymized: bool,
}

#[derive(Debug, Clone, Serialize)]
struct LegacyRevocation {
    device_id: String,
    token_hash: String,
    revoked_at: String,
}

#[derive(Debug, Clone, Serialize)]
struct LegacyErasure {
    device_id: String,
    erasure_id: String,
    secret_hash: String,
    requested_at: String,
    deadline_at: String,
    metrics_deleted_at: Option<String>,
    backups_expired_at: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
struct LegacySeedSource {
    format: &'static str,
    revocations: Vec<LegacyRevocation>,
    erasures: Vec<LegacyErasure>,
}

#[derive(Debug, Clone)]
struct OwnedEvent {
    stable_key: String,
    event_type: &'static str,
    device_id: Option<String>,
    token_hash: Option<String>,
    erasure_id: Option<String>,
    secret_hash: Option<String>,
    snapshot_id: Option<String>,
    restore_epoch: Option<String>,
    occurred_at: String,
    deadline_at: Option<String>,
}

impl OwnedEvent {
    fn borrowed(&self) -> NewEvent<'_> {
        NewEvent {
            stable_key: &self.stable_key,
            event_type: self.event_type,
            device_id: self.device_id.as_deref(),
            token_hash: self.token_hash.as_deref(),
            erasure_id: self.erasure_id.as_deref(),
            secret_hash: self.secret_hash.as_deref(),
            snapshot_id: self.snapshot_id.as_deref(),
            restore_epoch: self.restore_epoch.as_deref(),
            occurred_at: &self.occurred_at,
            deadline_at: self.deadline_at.as_deref(),
            evidence_digest: None,
            artifact_name: None,
        }
    }
}

#[derive(Debug, Clone, Serialize)]
struct HashRecord<'a> {
    event_id: &'a str,
    event_type: &'a str,
    device_id: Option<&'a str>,
    token_hash: Option<&'a str>,
    erasure_id: Option<&'a str>,
    secret_hash: Option<&'a str>,
    snapshot_id: Option<&'a str>,
    restore_epoch: Option<&'a str>,
    occurred_at: &'a str,
    deadline_at: Option<&'a str>,
    previous_hash: &'a str,
}

#[derive(Debug, Clone, Serialize)]
struct HashRecordV2<'a> {
    hash_version: i64,
    #[serde(flatten)]
    record: &'a HashRecord<'a>,
    evidence_digest: Option<&'a str>,
    artifact_name: Option<&'a str>,
}

#[derive(Debug, Clone)]
struct NewEvent<'a> {
    stable_key: &'a str,
    event_type: &'a str,
    device_id: Option<&'a str>,
    token_hash: Option<&'a str>,
    erasure_id: Option<&'a str>,
    secret_hash: Option<&'a str>,
    snapshot_id: Option<&'a str>,
    restore_epoch: Option<&'a str>,
    occurred_at: &'a str,
    deadline_at: Option<&'a str>,
    evidence_digest: Option<&'a str>,
    artifact_name: Option<&'a str>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
struct IntentEvent {
    stable_key: String,
    event_type: String,
    device_id: Option<String>,
    token_hash: Option<String>,
    erasure_id: Option<String>,
    secret_hash: Option<String>,
    snapshot_id: Option<String>,
    restore_epoch: Option<String>,
    occurred_at: String,
    deadline_at: Option<String>,
    evidence_digest: Option<String>,
    artifact_name: Option<String>,
}

impl IntentEvent {
    fn from_event(event: &NewEvent<'_>) -> Self {
        Self {
            stable_key: event.stable_key.to_owned(),
            event_type: event.event_type.to_owned(),
            device_id: event.device_id.map(str::to_owned),
            token_hash: event.token_hash.map(str::to_owned),
            erasure_id: event.erasure_id.map(str::to_owned),
            secret_hash: event.secret_hash.map(str::to_owned),
            snapshot_id: event.snapshot_id.map(str::to_owned),
            restore_epoch: event.restore_epoch.map(str::to_owned),
            occurred_at: event.occurred_at.to_owned(),
            deadline_at: event.deadline_at.map(str::to_owned),
            evidence_digest: event.evidence_digest.map(str::to_owned),
            artifact_name: event.artifact_name.map(str::to_owned),
        }
    }

    fn borrowed(&self) -> NewEvent<'_> {
        NewEvent {
            stable_key: &self.stable_key,
            event_type: &self.event_type,
            device_id: self.device_id.as_deref(),
            token_hash: self.token_hash.as_deref(),
            erasure_id: self.erasure_id.as_deref(),
            secret_hash: self.secret_hash.as_deref(),
            snapshot_id: self.snapshot_id.as_deref(),
            restore_epoch: self.restore_epoch.as_deref(),
            occurred_at: &self.occurred_at,
            deadline_at: self.deadline_at.as_deref(),
            evidence_digest: self.evidence_digest.as_deref(),
            artifact_name: self.artifact_name.as_deref(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
struct ControlIntentBody {
    format: u8,
    event: IntentEvent,
    event_id: String,
    predecessor: CustodyState,
    successor: CustodyState,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    backup_proof: Option<BackupArtifactProof>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    expiry_proof: Option<BackupExpiryProof>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
struct BackupFileIdentity {
    device: u64,
    inode: u64,
    bytes: u64,
    sha256: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
struct BackupArtifactProof {
    root: PathBuf,
    root_device: u64,
    root_inode: u64,
    artifact_name: String,
    database: Option<BackupFileIdentity>,
    manifest: Option<BackupFileIdentity>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
struct BackupExpiryProof {
    root: PathBuf,
    root_device: u64,
    root_inode: u64,
    requested_at: String,
    complete_inventory_sha256: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
struct ControlIntentDisk {
    body: ControlIntentBody,
    reservation: Option<CustodyReservationV2>,
}

impl ControlIntentDisk {
    fn validate(&self) -> ControlResult<()> {
        let body = &self.body;
        if body.format != 1
            || body.predecessor.format != 2
            || body.successor.format != 2
            || body.predecessor.revision.checked_add(1) != Some(body.successor.revision)
            || body.predecessor.ack != body.successor.ack
            || body.predecessor.baseline_sha256 != body.successor.baseline_sha256
            || body.predecessor.control.store_id != body.successor.control.store_id
            || body.predecessor.control.sequence.checked_add(1)
                != Some(body.successor.control.sequence)
            || body.event_id != event_id(&body.event.borrowed())
        {
            return Err(ControlError::Integrity(
                "control intent is not a single monotonic authority transition".to_owned(),
            ));
        }
        let event = body.event.borrowed();
        if let Some(proof) = &body.backup_proof
            && (!matches!(
                event.event_type,
                "backup_created" | "backup_delete_intent" | "backup_deleted"
            ) || event.artifact_name != Some(proof.artifact_name.as_str())
                || !proof.root.is_absolute()
                || proof.root.components().any(|part| {
                    matches!(
                        part,
                        std::path::Component::CurDir | std::path::Component::ParentDir
                    )
                }))
        {
            return Err(ControlError::Integrity(
                "backup proof does not bind its control event".to_owned(),
            ));
        }
        if let Some(proof) = &body.expiry_proof {
            if event.event_type != "backups_expired_verified"
                || !proof.root.is_absolute()
                || proof.root.components().any(|part| {
                    matches!(
                        part,
                        std::path::Component::CurDir | std::path::Component::ParentDir
                    )
                })
                || DateTime::parse_from_rfc3339(&proof.requested_at).is_err()
            {
                return Err(ControlError::Integrity(
                    "backup expiry proof does not bind its control event".to_owned(),
                ));
            }
            validate_sha256(&proof.complete_inventory_sha256)?;
        }
        validate_event_semantics(
            2,
            event.event_type,
            event.secret_hash,
            event.evidence_digest,
            event.artifact_name,
        )?;
        let expected_hash = event_hash_versioned(
            2,
            &HashRecord {
                event_id: &body.event_id,
                event_type: event.event_type,
                device_id: event.device_id,
                token_hash: event.token_hash,
                erasure_id: event.erasure_id,
                secret_hash: event.secret_hash,
                snapshot_id: event.snapshot_id,
                restore_epoch: event.restore_epoch,
                occurred_at: event.occurred_at,
                deadline_at: event.deadline_at,
                previous_hash: &body.predecessor.control.current_hash,
            },
            event.evidence_digest,
            event.artifact_name,
        )?;
        if body.successor.control.current_hash != expected_hash {
            return Err(ControlError::Integrity(
                "control intent successor does not match exact event bytes".to_owned(),
            ));
        }
        if let Some(reservation) = &self.reservation
            && (reservation.predecessor != body.predecessor
                || reservation.operation_id != body.event_id
                || reservation.intent_sha256 != digest(&serde_json::to_vec(body)?))
        {
            return Err(ControlError::Integrity(
                "stored custody reservation differs from the control intent".to_owned(),
            ));
        }
        Ok(())
    }
}

type ExistingEvent = (
    String,
    Option<String>,
    Option<String>,
    Option<String>,
    Option<String>,
    Option<String>,
    Option<String>,
    String,
    Option<String>,
    Option<String>,
    Option<String>,
);

impl ControlStore {
    pub fn new(db_path: PathBuf, mirror_dir: PathBuf) -> ControlResult<Self> {
        let parent = db_path
            .parent()
            .ok_or_else(|| ControlError::Invalid("database has no parent".to_owned()))?;
        let head_path = parent.join("control.head.json");
        let publication_lock_path = parent.join("control.publish.lock");
        let pending_intent_path = parent.join("control.pending-intent.json");
        Ok(Self {
            db_path,
            head_path,
            mirror_dir,
            publication_lock_path,
            custody: None,
            ack_journal: None,
            custody_lock_path: None,
            pending_intent_path,
            managed_backup_root: None,
        })
    }

    /// Enables the operational off-host publication boundary. A plain store
    /// remains available only to offline initialization/migration and synthetic
    /// tests; the receiver must construct this variant before any authority
    /// can be reported to a caller.
    pub fn with_custody(
        mut self,
        custody: Arc<dyn CustodyClient>,
        custody_lock_path: PathBuf,
    ) -> ControlResult<Self> {
        if !custody_lock_path.is_absolute()
            || custody_lock_path.components().any(|component| {
                matches!(
                    component,
                    std::path::Component::ParentDir | std::path::Component::CurDir
                )
            })
            || !custody_lock_path.parent().is_some_and(Path::is_dir)
        {
            return Err(ControlError::Invalid(
                "custody operation lock path must be absolute in an existing directory".to_owned(),
            ));
        }
        self.custody = Some(custody);
        self.custody_lock_path = Some(custody_lock_path);
        Ok(self)
    }

    pub fn is_custodied(&self) -> bool {
        self.custody.is_some()
    }

    /// Bind the opened acknowledgement journal itself, not caller-supplied
    /// hashes or counters. Operational control publication must attach this
    /// before checking the independent v2 state or committing a new event.
    pub fn with_ack_journal(mut self, journal: Arc<AckJournal>) -> ControlResult<Self> {
        if self.custody.is_none() {
            return Err(ControlError::Invalid(
                "ack journal custody requires an off-host client".to_owned(),
            ));
        }
        self.ack_journal = Some(journal);
        Ok(self)
    }

    /// The operational caller supplies the already validated managed backup
    /// directory. A persisted proof is never allowed to select a new root.
    pub fn with_managed_backup_root(mut self, root: PathBuf) -> ControlResult<Self> {
        if !root.is_absolute()
            || root.components().any(|part| {
                matches!(
                    part,
                    std::path::Component::CurDir | std::path::Component::ParentDir
                )
            })
            || root.canonicalize()? != root
        {
            return Err(ControlError::Invalid(
                "managed backup root must be an absolute canonical directory".to_owned(),
            ));
        }
        let metadata = fs::symlink_metadata(&root)?;
        if !metadata.is_dir() || metadata.file_type().is_symlink() {
            return Err(ControlError::Invalid(
                "managed backup root is not a real directory".to_owned(),
            ));
        }
        self.managed_backup_root = Some(root);
        Ok(self)
    }

    pub fn db_path(&self) -> &Path {
        &self.db_path
    }

    pub fn mirror_dir(&self) -> &Path {
        &self.mirror_dir
    }

    pub fn head_path(&self) -> &Path {
        &self.head_path
    }

    pub fn initialize(db_path: PathBuf, mirror_dir: PathBuf) -> ControlResult<Self> {
        let store = Self::new(db_path, mirror_dir)?;
        if store.db_path.exists() || store.head_path.exists() {
            return Err(ControlError::Invalid(
                "init requires absent control database and head".to_owned(),
            ));
        }
        let parent = store
            .db_path
            .parent()
            .ok_or_else(|| ControlError::Invalid("database has no parent".to_owned()))?;
        if !parent.is_dir() || !store.mirror_dir.is_dir() {
            return Err(ControlError::Invalid(
                "control directories must exist before initialization".to_owned(),
            ));
        }
        OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&store.db_path)?;
        let result = (|| -> ControlResult<()> {
            let connection =
                Connection::open_with_flags(&store.db_path, OpenFlags::SQLITE_OPEN_READ_WRITE)?;
            configure(&connection)?;
            connection.execute_batch(CONTROL_SCHEMA_V2)?;
            create_v3_objects(&connection, CONTROL_SCHEMA_V2_VERSION)?;
            let store_id = uuid::Uuid::new_v4().to_string();
            connection.execute(
                "INSERT INTO control_meta(singleton,store_id,created_at) VALUES (1,?1,?2)",
                params![store_id, Utc::now().to_rfc3339()],
            )?;
            connection.execute_batch(&format!(
                "PRAGMA application_id={CONTROL_APPLICATION_ID}; PRAGMA user_version={CONTROL_SCHEMA_VERSION};"
            ))?;
            set_private_file(&store.db_path)?;
            write_head_atomic(
                &store.head_path,
                &ControlHead {
                    store_id,
                    sequence: 0,
                    current_hash: GENESIS_HASH.to_owned(),
                },
            )?;
            Ok(())
        })();
        if result.is_err() {
            let _ = fs::remove_file(&store.db_path);
            let _ = fs::remove_file(store.db_path.with_extension("db-wal"));
            let _ = fs::remove_file(store.db_path.with_extension("db-shm"));
            let _ = fs::remove_file(&store.head_path);
        }
        result?;
        store.preflight_migration()?;
        Ok(store)
    }

    /// Compatibility entry point for the operational CLI. Migrates either
    /// reviewed v1 or v2 layout to v3 without copying or dropping events.
    pub fn migrate_v1_to_v2(&self) -> ControlResult<()> {
        self.migrate_to_v3()
    }

    pub fn migrate_to_v3(&self) -> ControlResult<()> {
        self.migrate_to_v3_inner(false)
    }

    /// Read-only classification for a previously initialized control store.
    /// The caller must hold the offline lifecycle fence. A WAL/SHM or rollback
    /// journal requires a separate, explicit checkpoint/recovery step: an
    /// immutable read must never hide committed control facts in a WAL.
    pub fn preflight_migration(&self) -> ControlResult<String> {
        validate_control_authority_paths(self)?;
        validate_preflight_sidecars(&self.db_path)?;
        let connection = self.open_preflight_read_only()?;
        let application_id: i64 =
            connection.query_row("PRAGMA application_id", [], |row| row.get(0))?;
        let version: i64 = connection.query_row("PRAGMA user_version", [], |row| row.get(0))?;
        if application_id != CONTROL_APPLICATION_ID {
            return Err(ControlError::Invalid(
                "unknown control application ID".to_owned(),
            ));
        }
        if version != LEGACY_CONTROL_SCHEMA_VERSION
            && version != CONTROL_SCHEMA_V2_VERSION
            && version != CONTROL_SCHEMA_VERSION
        {
            return Err(ControlError::Invalid(
                "unsupported control schema version".to_owned(),
            ));
        }
        drop(connection);
        self.verify_at_version_using(version, true)?;
        let connection = self.open_preflight_read_only()?;
        connection
            .query_row(
                "SELECT store_id FROM control_meta WHERE singleton=1",
                [],
                |row| row.get(0),
            )
            .map_err(ControlError::from)
    }

    /// Proves that an existing v1/v2 control chain is exactly a prefix of the
    /// legacy health facts before any schema migration or seed append occurs.
    pub fn preflight_legacy_seed_prefix(&self, health: &Connection) -> ControlResult<()> {
        self.preflight_migration()?;
        let source = load_legacy_seed_source(health)?;
        let fingerprint = digest(&serde_json::to_vec(&source)?);
        let events = legacy_seed_events(&source, &fingerprint);
        let expected_ids = events
            .iter()
            .map(|event| event_id(&event.borrowed()))
            .collect::<Vec<_>>();
        let connection = self.open_preflight_read_only()?;
        let version: i64 = connection.query_row("PRAGMA user_version", [], |row| row.get(0))?;
        let mut statement =
            connection.prepare("SELECT event_id FROM control_events ORDER BY sequence")?;
        let existing_ids = statement
            .query_map([], |row| row.get::<_, String>(0))?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        if existing_ids.len() > expected_ids.len()
            || existing_ids != expected_ids[..existing_ids.len()]
        {
            return Err(ControlError::Integrity(
                "control store contains events unrelated to the legacy seed".to_owned(),
            ));
        }
        for event in events.iter().take(existing_ids.len()) {
            let borrowed = event.borrowed();
            let existing =
                find_existing_event_for_version(&connection, &event_id(&borrowed), version)?;
            if !existing
                .as_ref()
                .is_some_and(|existing| event_matches(existing, &borrowed))
            {
                return Err(ControlError::Integrity(
                    "legacy control event content differs from health source".to_owned(),
                ));
            }
        }
        Ok(())
    }

    fn migrate_to_v3_inner(&self, fail_before_commit: bool) -> ControlResult<()> {
        if self.custody.is_some() {
            return Err(ControlError::Invalid(
                "control migration requires an offline pre-adoption store".to_owned(),
            ));
        }
        self.preflight_migration()?;
        let _publication = self.acquire_publication_lock()?;
        self.preflight_migration()?;
        let connection = self.open_read_only()?;
        let application_id: i64 =
            connection.query_row("PRAGMA application_id", [], |row| row.get(0))?;
        let version: i64 = connection.query_row("PRAGMA user_version", [], |row| row.get(0))?;
        if application_id != CONTROL_APPLICATION_ID {
            return Err(ControlError::Invalid(
                "unknown control application ID".to_owned(),
            ));
        }
        if version == CONTROL_SCHEMA_VERSION {
            return Ok(());
        }
        if version != LEGACY_CONTROL_SCHEMA_VERSION && version != CONTROL_SCHEMA_V2_VERSION {
            return Err(ControlError::Invalid(
                "unsupported control schema version".to_owned(),
            ));
        }
        drop(connection);
        let mut connection = self.open_read_write()?;
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let old_count: i64 =
            transaction.query_row("SELECT COUNT(*) FROM control_events", [], |row| row.get(0))?;
        create_v3_objects(&transaction, version)?;
        let prefix_count: i64 =
            transaction.query_row("SELECT COUNT(*) FROM control_events_prefix", [], |row| {
                row.get(0)
            })?;
        let view_count: i64 =
            transaction.query_row("SELECT COUNT(*) FROM control_events", [], |row| row.get(0))?;
        if old_count != prefix_count || old_count != view_count {
            return Err(ControlError::Integrity(
                "control migration changed the historical event set".to_owned(),
            ));
        }
        if fail_before_commit {
            return Err(ControlError::Integrity(
                "injected interruption before migration commit".to_owned(),
            ));
        }
        transaction.execute_batch("PRAGMA user_version=3;")?;
        transaction.commit()?;
        drop(connection);
        self.preflight_migration().map(|_| ())
    }

    pub fn store_id(&self) -> ControlResult<String> {
        let connection = self.open_read_only()?;
        connection
            .query_row(
                "SELECT store_id FROM control_meta WHERE singleton=1",
                [],
                |row| row.get(0),
            )
            .map_err(ControlError::from)
    }

    pub fn checkpoint(&self) -> ControlResult<ControlCheckpoint> {
        self.verify()?;
        let head: ControlHead = serde_json::from_slice(&fs::read(&self.head_path)?)?;
        Ok(ControlCheckpoint {
            store_id: head.store_id,
            sequence: head.sequence,
            current_hash: head.current_hash,
        })
    }

    pub fn contains_checkpoint(&self, checkpoint: &ControlCheckpoint) -> ControlResult<bool> {
        self.verify()?;
        if checkpoint.store_id != self.store_id()? {
            return Ok(false);
        }
        if checkpoint.sequence == 0 {
            return Ok(checkpoint.current_hash == GENESIS_HASH);
        }
        let connection = self.open_read_only()?;
        connection
            .query_row(
                "SELECT EXISTS(SELECT 1 FROM control_events WHERE sequence=?1 AND current_hash=?2)",
                params![checkpoint.sequence, checkpoint.current_hash],
                |row| row.get(0),
            )
            .map_err(ControlError::from)
    }

    pub fn verify(&self) -> ControlResult<()> {
        self.verify_at_version(CONTROL_SCHEMA_VERSION)
    }

    /// Repairs only the publication tail after an independently attested DB
    /// tip. A prior expected head is intentionally insufficient authority.
    pub fn reconcile_publication(&self, expected: &ControlCheckpoint) -> ControlResult<()> {
        if self.custody.is_some() {
            return Err(ControlError::Invalid(
                "custodied publication repair requires the persisted reservation".to_owned(),
            ));
        }
        let _publication = self.acquire_publication_lock()?;
        self.reconcile_publication_locked(expected)
    }

    fn reconcile_publication_locked(&self, expected: &ControlCheckpoint) -> ControlResult<()> {
        reject_link_or_non_file(&self.db_path)?;
        if !self.mirror_dir.is_dir() {
            return Err(ControlError::Invalid(
                "control mirror directory is missing".to_owned(),
            ));
        }
        let connection = self.open_read_only()?;
        let application_id: i64 =
            connection.query_row("PRAGMA application_id", [], |row| row.get(0))?;
        let user_version: i64 =
            connection.query_row("PRAGMA user_version", [], |row| row.get(0))?;
        if application_id != CONTROL_APPLICATION_ID || user_version != CONTROL_SCHEMA_VERSION {
            return Err(ControlError::Invalid(
                "unsupported control database".to_owned(),
            ));
        }
        verify_control_schema(&connection, CONTROL_SCHEMA_VERSION)?;
        let quick: String = connection.query_row("PRAGMA quick_check", [], |row| row.get(0))?;
        if quick != "ok" {
            return Err(ControlError::Integrity(
                "control SQLite quick_check failed".to_owned(),
            ));
        }
        let foreign_key_failure: Option<i64> = connection
            .query_row("PRAGMA foreign_key_check", [], |row| row.get(0))
            .optional()?;
        if foreign_key_failure.is_some() {
            return Err(ControlError::Integrity(
                "control foreign keys failed".to_owned(),
            ));
        }
        let store_id: String = connection.query_row(
            "SELECT store_id FROM control_meta WHERE singleton=1",
            [],
            |row| row.get(0),
        )?;
        let mut statement = connection.prepare(
            "SELECT sequence,event_id,event_type,device_id,token_hash,erasure_id,secret_hash,
                    snapshot_id,restore_epoch,occurred_at,deadline_at,previous_hash,current_hash,
                    hash_version,evidence_digest,artifact_name
             FROM control_events ORDER BY sequence",
        )?;
        let mut previous = GENESIS_HASH.to_owned();
        let mut records = Vec::new();
        let mut seen_event_ids = BTreeSet::new();
        let rows = statement.query_map([], |row| {
            Ok((
                row.get::<_, i64>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
                row.get::<_, Option<String>>(3)?,
                row.get::<_, Option<String>>(4)?,
                row.get::<_, Option<String>>(5)?,
                row.get::<_, Option<String>>(6)?,
                row.get::<_, Option<String>>(7)?,
                row.get::<_, Option<String>>(8)?,
                row.get::<_, String>(9)?,
                row.get::<_, Option<String>>(10)?,
                row.get::<_, String>(11)?,
                row.get::<_, String>(12)?,
                row.get::<_, i64>(13)?,
                row.get::<_, Option<String>>(14)?,
                row.get::<_, Option<String>>(15)?,
            ))
        })?;
        for row in rows {
            let (
                sequence,
                id,
                kind,
                device,
                token,
                erasure,
                secret,
                snapshot,
                epoch,
                at,
                deadline,
                stored_previous,
                hash,
                version,
                evidence,
                artifact,
            ) = row?;
            if !seen_event_ids.insert(id.clone())
                || sequence != records.len() as i64 + 1
                || stored_previous != previous
            {
                return Err(ControlError::Integrity(
                    "control event ID or chain is not contiguous".to_owned(),
                ));
            }
            let record = HashRecord {
                event_id: &id,
                event_type: &kind,
                device_id: device.as_deref(),
                token_hash: token.as_deref(),
                erasure_id: erasure.as_deref(),
                secret_hash: secret.as_deref(),
                snapshot_id: snapshot.as_deref(),
                restore_epoch: epoch.as_deref(),
                occurred_at: &at,
                deadline_at: deadline.as_deref(),
                previous_hash: &stored_previous,
            };
            validate_event_semantics(
                version,
                &kind,
                secret.as_deref(),
                evidence.as_deref(),
                artifact.as_deref(),
            )?;
            if event_hash_versioned(version, &record, evidence.as_deref(), artifact.as_deref())?
                != hash
            {
                return Err(ControlError::Integrity(
                    "control event hash differs".to_owned(),
                ));
            }
            previous = hash.clone();
            records.push((sequence, id, hash));
        }
        let tip = ControlCheckpoint {
            store_id: store_id.clone(),
            sequence: records.len() as i64,
            current_hash: previous,
        };
        if &tip != expected {
            return Err(ControlError::Integrity(
                "independent expected head does not equal the database tip".to_owned(),
            ));
        }
        let mut expected_names = BTreeSet::new();
        let mut missing_latest = false;
        for (sequence, id, hash) in &records {
            let path = mirror_path(&self.mirror_dir, *sequence, hash);
            let name = path
                .file_name()
                .and_then(|value| value.to_str())
                .ok_or_else(|| {
                    ControlError::Integrity("control mirror name is invalid".to_owned())
                })?
                .to_owned();
            expected_names.insert(name);
            match fs::symlink_metadata(&path) {
                Ok(_) => verify_mirror(&self.mirror_dir, &store_id, *sequence, hash, id)?,
                Err(error)
                    if error.kind() == io::ErrorKind::NotFound && *sequence == tip.sequence =>
                {
                    missing_latest = true
                }
                Err(error) if error.kind() == io::ErrorKind::NotFound => {
                    return Err(ControlError::Integrity(
                        "a historical control mirror is missing".to_owned(),
                    ));
                }
                Err(error) => return Err(error.into()),
            }
        }
        let actual_names = fs::read_dir(&self.mirror_dir)?
            .map(|entry| {
                entry.and_then(|entry| {
                    entry.file_name().into_string().map_err(|_| {
                        io::Error::new(io::ErrorKind::InvalidData, "non-UTF8 mirror name")
                    })
                })
            })
            .collect::<io::Result<BTreeSet<_>>>()?;
        if !actual_names.is_subset(&expected_names) {
            return Err(ControlError::Integrity(
                "extra control mirror exists".to_owned(),
            ));
        }
        let existing_head = match fs::symlink_metadata(&self.head_path) {
            Ok(_) => {
                reject_link_or_non_file(&self.head_path)?;
                Some(serde_json::from_slice::<ControlHead>(&fs::read(
                    &self.head_path,
                )?)?)
            }
            Err(error) if error.kind() == io::ErrorKind::NotFound => None,
            Err(error) => return Err(error.into()),
        };
        if let Some(head) = &existing_head {
            let valid_tip = head.store_id == store_id
                && head.sequence == tip.sequence
                && head.current_hash == tip.current_hash;
            let valid_prior = head.store_id == store_id
                && head.sequence + 1 == tip.sequence
                && records.last().is_some_and(|(_, _, _)| {
                    head.current_hash
                        == if records.len() == 1 {
                            GENESIS_HASH
                        } else {
                            records[records.len() - 2].2.as_str()
                        }
                });
            if !valid_tip && !valid_prior {
                return Err(ControlError::Integrity(
                    "existing control head diverges".to_owned(),
                ));
            }
        }
        if missing_latest {
            let (sequence, id, hash) = records.last().ok_or_else(|| {
                ControlError::Integrity("missing mirror for empty chain".to_owned())
            })?;
            let bytes = serde_json::to_vec(&serde_json::json!({
                "store_id":store_id, "sequence":sequence,
                "event_id":id, "current_hash":hash,
            }))?;
            write_mirror_create_new(&self.mirror_dir, *sequence, hash, &bytes)?;
        }
        if existing_head.as_ref().is_none_or(|head| {
            head.sequence != tip.sequence || head.current_hash != tip.current_hash
        }) {
            write_head_atomic(
                &self.head_path,
                &ControlHead {
                    store_id,
                    sequence: tip.sequence,
                    current_hash: tip.current_hash,
                },
            )?;
        }
        self.verify()
    }

    fn verify_at_version(&self, expected_version: i64) -> ControlResult<()> {
        self.verify_at_version_using(expected_version, false)
    }

    fn verify_at_version_using(
        &self,
        expected_version: i64,
        immutable_preflight: bool,
    ) -> ControlResult<()> {
        validate_control_authority_paths(self)?;
        let connection = if immutable_preflight {
            self.open_preflight_read_only()?
        } else {
            self.open_read_only()?
        };
        let quick: String = connection.query_row("PRAGMA quick_check", [], |row| row.get(0))?;
        if quick != "ok" {
            return Err(ControlError::Integrity(format!(
                "SQLite quick_check returned {quick}"
            )));
        }
        let application_id: i64 =
            connection.query_row("PRAGMA application_id", [], |row| row.get(0))?;
        let user_version: i64 =
            connection.query_row("PRAGMA user_version", [], |row| row.get(0))?;
        if application_id != CONTROL_APPLICATION_ID || user_version != expected_version {
            return Err(ControlError::Invalid(
                "application ID or schema version is unsupported".to_owned(),
            ));
        }
        verify_control_schema(&connection, expected_version)?;
        let foreign_key_failure: Option<i64> = connection
            .query_row("PRAGMA foreign_key_check", [], |row| row.get(0))
            .optional()?;
        if foreign_key_failure.is_some() {
            return Err(ControlError::Integrity(
                "control database foreign key validation failed".to_owned(),
            ));
        }
        let store_id: String = connection.query_row(
            "SELECT store_id FROM control_meta WHERE singleton=1",
            [],
            |row| row.get(0),
        )?;
        let head: ControlHead = serde_json::from_slice(&fs::read(&self.head_path)?)?;
        if head.store_id != store_id {
            return Err(ControlError::Integrity(
                "head belongs to a different control store".to_owned(),
            ));
        }
        let mut previous_hash = GENESIS_HASH.to_owned();
        let mut latest_sequence = 0_i64;
        let mut seen_event_ids = BTreeSet::new();
        let mut expected_mirrors = BTreeSet::new();
        let verification_query = if expected_version == LEGACY_CONTROL_SCHEMA_VERSION {
            "SELECT sequence,event_id,event_type,device_id,token_hash,erasure_id,secret_hash,snapshot_id,restore_epoch,occurred_at,deadline_at,previous_hash,current_hash,1,NULL,NULL FROM control_events ORDER BY sequence"
        } else {
            "SELECT sequence,event_id,event_type,device_id,token_hash,erasure_id,secret_hash,snapshot_id,restore_epoch,occurred_at,deadline_at,previous_hash,current_hash,hash_version,evidence_digest,artifact_name FROM control_events ORDER BY sequence"
        };
        let mut statement = connection.prepare(verification_query)?;
        let rows = statement.query_map([], |row| {
            Ok((
                row.get::<_, i64>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
                row.get::<_, Option<String>>(3)?,
                row.get::<_, Option<String>>(4)?,
                row.get::<_, Option<String>>(5)?,
                row.get::<_, Option<String>>(6)?,
                row.get::<_, Option<String>>(7)?,
                row.get::<_, Option<String>>(8)?,
                row.get::<_, String>(9)?,
                row.get::<_, Option<String>>(10)?,
                row.get::<_, String>(11)?,
                row.get::<_, String>(12)?,
                row.get::<_, i64>(13)?,
                row.get::<_, Option<String>>(14)?,
                row.get::<_, Option<String>>(15)?,
            ))
        })?;
        for row in rows {
            let (
                sequence,
                event_id,
                event_type,
                device_id,
                token_hash,
                erasure_id,
                secret_hash,
                snapshot_id,
                restore_epoch,
                occurred_at,
                deadline_at,
                stored_previous,
                current_hash,
                hash_version,
                evidence_digest,
                artifact_name,
            ) = row?;
            if !seen_event_ids.insert(event_id.clone())
                || sequence != latest_sequence + 1
                || stored_previous != previous_hash
            {
                return Err(ControlError::Integrity(
                    "control event ID, sequence, or previous hash is not contiguous".to_owned(),
                ));
            }
            validate_event_semantics(
                hash_version,
                &event_type,
                secret_hash.as_deref(),
                evidence_digest.as_deref(),
                artifact_name.as_deref(),
            )?;
            let record = HashRecord {
                event_id: &event_id,
                event_type: &event_type,
                device_id: device_id.as_deref(),
                token_hash: token_hash.as_deref(),
                erasure_id: erasure_id.as_deref(),
                secret_hash: secret_hash.as_deref(),
                snapshot_id: snapshot_id.as_deref(),
                restore_epoch: restore_epoch.as_deref(),
                occurred_at: &occurred_at,
                deadline_at: deadline_at.as_deref(),
                previous_hash: &stored_previous,
            };
            let expected = event_hash_versioned(
                hash_version,
                &record,
                evidence_digest.as_deref(),
                artifact_name.as_deref(),
            )?;
            if expected != current_hash {
                return Err(ControlError::Integrity(
                    "event content hash does not match".to_owned(),
                ));
            }
            verify_mirror(
                &self.mirror_dir,
                &store_id,
                sequence,
                &current_hash,
                &event_id,
            )?;
            expected_mirrors.insert(
                mirror_path(&self.mirror_dir, sequence, &current_hash)
                    .file_name()
                    .and_then(|name| name.to_str())
                    .ok_or_else(|| {
                        ControlError::Integrity("control mirror filename is invalid".to_owned())
                    })?
                    .to_owned(),
            );
            previous_hash = current_hash;
            latest_sequence = sequence;
        }
        let mut actual_mirrors = BTreeSet::new();
        for entry in fs::read_dir(&self.mirror_dir)? {
            let entry = entry?;
            let metadata = entry.metadata()?;
            if !metadata.file_type().is_file() {
                return Err(ControlError::Integrity(
                    "control mirror directory contains a non-file entry".to_owned(),
                ));
            }
            let name = entry.file_name().into_string().map_err(|_| {
                ControlError::Integrity("control mirror filename is not UTF-8".to_owned())
            })?;
            actual_mirrors.insert(name);
        }
        if actual_mirrors != expected_mirrors {
            return Err(ControlError::Integrity(
                "control mirror inventory does not exactly match the database chain".to_owned(),
            ));
        }
        if head.sequence != latest_sequence || head.current_hash != previous_hash {
            return Err(ControlError::Integrity(
                "database and control head are not at the same generation".to_owned(),
            ));
        }
        Ok(())
    }

    pub fn verify_tail(&self) -> ControlResult<()> {
        if self.custody.is_some() && self.read_pending_intent()?.is_some() {
            return Err(ControlError::Integrity(
                "control custody publication is unresolved".to_owned(),
            ));
        }
        reject_link_or_non_file(&self.db_path)?;
        reject_link_or_non_file(&self.head_path)?;
        let connection = self.open_read_only()?;
        let application_id: i64 =
            connection.query_row("PRAGMA application_id", [], |row| row.get(0))?;
        let user_version: i64 =
            connection.query_row("PRAGMA user_version", [], |row| row.get(0))?;
        if application_id != CONTROL_APPLICATION_ID || user_version != CONTROL_SCHEMA_VERSION {
            return Err(ControlError::Invalid(
                "application ID or schema version is unsupported".to_owned(),
            ));
        }
        let store_id: String = connection.query_row(
            "SELECT store_id FROM control_meta WHERE singleton=1",
            [],
            |row| row.get(0),
        )?;
        let head: ControlHead = serde_json::from_slice(&fs::read(&self.head_path)?)?;
        let latest: Option<(i64, String, String)> = connection
            .query_row(
                "SELECT sequence,event_id,current_hash FROM control_events ORDER BY sequence DESC LIMIT 1",
                [],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .optional()?;
        match latest {
            None => {
                if head.store_id != store_id
                    || head.sequence != 0
                    || head.current_hash != GENESIS_HASH
                {
                    return Err(ControlError::Integrity(
                        "empty database and control head differ".to_owned(),
                    ));
                }
            }
            Some((sequence, event_id, current_hash)) => {
                if head.store_id != store_id
                    || head.sequence != sequence
                    || head.current_hash != current_hash
                {
                    return Err(ControlError::Integrity(
                        "database and control head tail differ".to_owned(),
                    ));
                }
                verify_mirror(
                    &self.mirror_dir,
                    &store_id,
                    sequence,
                    &current_hash,
                    &event_id,
                )?;
            }
        }
        Ok(())
    }

    pub fn token_tombstoned(&self, token_hash: &str) -> ControlResult<bool> {
        // Full chain/head/mirror verification is performed at startup, before
        // every append, and during reconciliation. Authentication stays an
        // indexed read so request cost does not grow with control history.
        self.verify_tail()?;
        let connection = self.open_read_only()?;
        connection
            .query_row(
                "SELECT EXISTS(SELECT 1 FROM control_events WHERE token_hash=?1 AND event_type IN ('credential_revoked','erasure_intent'))",
                [token_hash],
                |row| row.get(0),
            )
            .map_err(ControlError::from)
    }

    pub fn device_erasure_tombstoned(&self, device_id: &str) -> ControlResult<bool> {
        self.verify_tail()?;
        let connection = self.open_read_only()?;
        connection
            .query_row(
                "SELECT EXISTS(SELECT 1 FROM control_events WHERE device_id=?1 AND event_type='erasure_intent')",
                [device_id],
                |row| row.get(0),
            )
            .map_err(ControlError::from)
    }

    pub fn matching_erasure_intent(
        &self,
        erasure_id: &str,
        secret_hash: &str,
    ) -> ControlResult<Option<ErasureIntentAuth>> {
        self.verify_tail()?;
        let connection = self.open_read_only()?;
        connection
            .query_row(
                "SELECT device_id,token_hash FROM control_events WHERE event_type='erasure_intent' AND erasure_id=?1 AND secret_hash=?2",
                params![erasure_id, secret_hash],
                |row| {
                    Ok(ErasureIntentAuth {
                        device_id: row.get(0)?,
                        token_hash: row.get(1)?,
                    })
                },
            )
            .optional()
            .map_err(ControlError::from)
    }

    /// Read one erasure's status facts only while its complete control chain
    /// and the off-host custody head are jointly settled. A health-ledger row
    /// is a projection of these facts and cannot independently assert success.
    pub fn certified_erasure_facts(
        &self,
        erasure_id: &str,
        secret_hash: &str,
    ) -> ControlResult<Option<CertifiedErasureFacts>> {
        let _custody = self.acquire_custody_operation_lock()?;
        self.verify_custody_while_locked()?;
        self.verify_tail()?;
        let connection = self.open_read_only()?;
        connection
            .query_row(
                "SELECT intent.device_id,intent.token_hash,intent.occurred_at,intent.deadline_at,
                        (SELECT done.occurred_at FROM control_events done
                         WHERE done.event_type='metrics_erasure_verified'
                           AND done.erasure_id=intent.erasure_id
                           AND done.device_id=intent.device_id
                         ORDER BY done.sequence DESC LIMIT 1),
                        (SELECT done.occurred_at FROM control_events done
                         WHERE done.event_type='backups_expired_verified'
                           AND done.erasure_id=intent.erasure_id
                           AND done.device_id=intent.device_id
                         ORDER BY done.sequence DESC LIMIT 1)
                 FROM control_events intent
                 WHERE intent.event_type='erasure_intent'
                   AND intent.erasure_id=?1 AND intent.secret_hash=?2",
                params![erasure_id, secret_hash],
                |row| {
                    Ok(CertifiedErasureFacts {
                        auth: ErasureIntentAuth {
                            device_id: row.get(0)?,
                            token_hash: row.get(1)?,
                        },
                        requested_at: row.get(2)?,
                        backup_delete_by: row.get(3)?,
                        metrics_deleted_at: row.get(4)?,
                        backups_expired_at: row.get(5)?,
                    })
                },
            )
            .optional()
            .map_err(ControlError::from)
    }

    pub fn append_credential_revoked(
        &self,
        device_id: &str,
        token_hash: &str,
        at: &str,
    ) -> ControlResult<()> {
        self.append_event(NewEvent {
            stable_key: token_hash,
            event_type: "credential_revoked",
            device_id: Some(device_id),
            token_hash: Some(token_hash),
            erasure_id: None,
            secret_hash: None,
            snapshot_id: None,
            restore_epoch: None,
            occurred_at: at,
            deadline_at: None,
            evidence_digest: None,
            artifact_name: None,
        })
    }

    pub fn append_erasure_intent(
        &self,
        device_id: &str,
        token_hash: &str,
        erasure_id: &str,
        secret_hash: &str,
        requested_at: &str,
        deadline_at: &str,
    ) -> ControlResult<()> {
        self.append_event(NewEvent {
            stable_key: erasure_id,
            event_type: "erasure_intent",
            device_id: Some(device_id),
            token_hash: Some(token_hash),
            erasure_id: Some(erasure_id),
            secret_hash: Some(secret_hash),
            snapshot_id: None,
            restore_epoch: None,
            occurred_at: requested_at,
            deadline_at: Some(deadline_at),
            evidence_digest: None,
            artifact_name: None,
        })
    }

    pub fn append_health_erasure_verified(
        &self,
        device_id: &str,
        erasure_id: &str,
        at: &str,
    ) -> ControlResult<()> {
        self.append_event(NewEvent {
            stable_key: erasure_id,
            event_type: "health_erasure_verified",
            device_id: Some(device_id),
            token_hash: None,
            erasure_id: Some(erasure_id),
            secret_hash: None,
            snapshot_id: None,
            restore_epoch: None,
            occurred_at: at,
            deadline_at: None,
            evidence_digest: None,
            artifact_name: None,
        })
    }

    pub fn append_metrics_erasure_verified(
        &self,
        device_id: &str,
        erasure_id: &str,
        at: &str,
    ) -> ControlResult<()> {
        self.append_event(NewEvent {
            stable_key: erasure_id,
            event_type: "metrics_erasure_verified",
            device_id: Some(device_id),
            token_hash: None,
            erasure_id: Some(erasure_id),
            secret_hash: None,
            snapshot_id: None,
            restore_epoch: None,
            occurred_at: at,
            deadline_at: None,
            evidence_digest: None,
            artifact_name: None,
        })
    }

    pub fn append_backups_expired_verified(
        &self,
        device_id: &str,
        erasure_id: &str,
        at: &str,
    ) -> ControlResult<()> {
        self.append_event(NewEvent {
            stable_key: erasure_id,
            event_type: "backups_expired_verified",
            device_id: Some(device_id),
            token_hash: None,
            erasure_id: Some(erasure_id),
            secret_hash: None,
            snapshot_id: None,
            restore_epoch: None,
            occurred_at: at,
            deadline_at: None,
            evidence_digest: None,
            artifact_name: None,
        })
    }

    pub fn append_backup_created(
        &self,
        snapshot_id: &str,
        file_sha256: &str,
        at: &str,
        expected_prior: &ControlCheckpoint,
    ) -> ControlResult<()> {
        self.append_backup_created_inner(snapshot_id, file_sha256, None, at, expected_prior)
    }

    pub fn append_backup_created_with_artifact(
        &self,
        snapshot_id: &str,
        file_sha256: &str,
        artifact_name: &str,
        at: &str,
        expected_prior: &ControlCheckpoint,
    ) -> ControlResult<()> {
        validate_artifact_name(artifact_name)?;
        self.append_backup_created_inner(
            snapshot_id,
            file_sha256,
            Some(artifact_name),
            at,
            expected_prior,
        )
    }

    fn append_backup_created_inner(
        &self,
        snapshot_id: &str,
        file_sha256: &str,
        artifact_name: Option<&str>,
        at: &str,
        expected_prior: &ControlCheckpoint,
    ) -> ControlResult<()> {
        validate_sha256(file_sha256)?;
        let _custody = self.acquire_custody_operation_lock()?;
        let _publication = self.acquire_publication_lock()?;
        self.append_event_locked(
            NewEvent {
                stable_key: snapshot_id,
                event_type: "backup_created",
                device_id: None,
                token_hash: None,
                erasure_id: None,
                secret_hash: Some(file_sha256),
                snapshot_id: Some(snapshot_id),
                restore_epoch: None,
                occurred_at: at,
                deadline_at: None,
                evidence_digest: None,
                artifact_name,
            },
            Some(expected_prior),
        )
    }

    pub fn append_backup_deleted(&self, snapshot_id: &str, at: &str) -> ControlResult<()> {
        self.verify()?;
        let connection = self.open_read_only()?;
        let intent: PendingBackupDelete = connection
            .query_row(
                "SELECT snapshot_id,secret_hash,artifact_name,occurred_at FROM control_events
             WHERE event_type='backup_delete_intent' AND snapshot_id=?1",
                [snapshot_id],
                |row| {
                    Ok(PendingBackupDelete {
                        snapshot_id: row.get(0)?,
                        file_sha256: row.get(1)?,
                        artifact_name: row.get(2)?,
                        occurred_at: row.get(3)?,
                    })
                },
            )
            .optional()?
            .ok_or_else(|| {
                ControlError::Integrity("backup deletion has no matching control intent".to_owned())
            })?;
        self.append_backup_deleted_with_hash(
            snapshot_id,
            &intent.file_sha256,
            &intent.artifact_name,
            at,
        )
    }

    pub fn append_backup_delete_intent(
        &self,
        snapshot_id: &str,
        file_sha256: &str,
        artifact_name: &str,
        at: &str,
    ) -> ControlResult<()> {
        validate_sha256(file_sha256)?;
        validate_artifact_name(artifact_name)?;
        let _custody = self.acquire_custody_operation_lock()?;
        let _publication = self.acquire_publication_lock()?;
        self.verify()?;
        let connection = self.open_read_only()?;
        let created: Option<String> = connection.query_row(
            "SELECT secret_hash FROM control_events WHERE event_type='backup_created' AND snapshot_id=?1",
            [snapshot_id], |row| row.get(0)
        ).optional()?;
        if created.as_deref() != Some(file_sha256) {
            return Err(ControlError::Integrity(
                "backup deletion intent does not match a created snapshot".to_owned(),
            ));
        }
        drop(connection);
        self.append_event_locked(
            NewEvent {
                stable_key: snapshot_id,
                event_type: "backup_delete_intent",
                device_id: None,
                token_hash: None,
                erasure_id: None,
                secret_hash: Some(file_sha256),
                snapshot_id: Some(snapshot_id),
                restore_epoch: None,
                occurred_at: at,
                deadline_at: None,
                evidence_digest: None,
                artifact_name: Some(artifact_name),
            },
            None,
        )
    }

    pub fn append_backup_deleted_with_hash(
        &self,
        snapshot_id: &str,
        file_sha256: &str,
        artifact_name: &str,
        at: &str,
    ) -> ControlResult<()> {
        validate_sha256(file_sha256)?;
        validate_artifact_name(artifact_name)?;
        let _custody = self.acquire_custody_operation_lock()?;
        let _publication = self.acquire_publication_lock()?;
        self.verify()?;
        let connection = self.open_read_only()?;
        let intent: Option<(String, String)> = connection.query_row(
            "SELECT secret_hash,artifact_name FROM control_events WHERE event_type='backup_delete_intent' AND snapshot_id=?1",
            [snapshot_id], |row| Ok((row.get(0)?,row.get(1)?))
        ).optional()?;
        if intent
            .as_ref()
            .map(|(hash, name)| (hash.as_str(), name.as_str()))
            != Some((file_sha256, artifact_name))
        {
            return Err(ControlError::Integrity(
                "backup deletion does not match its durable intent".to_owned(),
            ));
        }
        drop(connection);
        self.append_event_locked(
            NewEvent {
                stable_key: snapshot_id,
                event_type: "backup_deleted",
                device_id: None,
                token_hash: None,
                erasure_id: None,
                secret_hash: Some(file_sha256),
                snapshot_id: Some(snapshot_id),
                restore_epoch: None,
                occurred_at: at,
                deadline_at: None,
                evidence_digest: None,
                artifact_name: Some(artifact_name),
            },
            None,
        )
    }

    pub fn pending_backup_delete_intents(&self) -> ControlResult<Vec<PendingBackupDelete>> {
        self.verify()?;
        let connection = self.open_read_only()?;
        let mut statement = connection.prepare(
            "SELECT intent.snapshot_id,intent.secret_hash,intent.artifact_name,intent.occurred_at
             FROM control_events intent
             WHERE intent.event_type='backup_delete_intent'
               AND NOT EXISTS (
                   SELECT 1 FROM control_events done
                   WHERE done.event_type='backup_deleted' AND done.snapshot_id=intent.snapshot_id
               ) ORDER BY intent.sequence",
        )?;
        let rows = statement.query_map([], |row| {
            Ok(PendingBackupDelete {
                snapshot_id: row.get(0)?,
                file_sha256: row.get(1)?,
                artifact_name: row.get(2)?,
                occurred_at: row.get(3)?,
            })
        })?;
        let pending = rows.collect::<rusqlite::Result<Vec<_>>>()?;
        for item in &pending {
            validate_sha256(&item.file_sha256)?;
            validate_artifact_name(&item.artifact_name)?;
        }
        Ok(pending)
    }

    pub fn active_backup_inventory(&self) -> ControlResult<Vec<ActiveBackup>> {
        let _publication = self.acquire_publication_lock()?;
        self.verify()?;
        let connection = self.open_read_only()?;
        let store_id: String = connection.query_row(
            "SELECT store_id FROM control_meta WHERE singleton=1",
            [],
            |row| row.get(0),
        )?;
        let mut statement = connection.prepare(
            "SELECT created.snapshot_id,created.secret_hash,created.occurred_at,
                    created.sequence,created.previous_hash
             FROM control_events created
             WHERE created.event_type='backup_created'
               AND NOT EXISTS (
                   SELECT 1 FROM control_events deleted
                   WHERE deleted.event_type='backup_deleted'
                     AND deleted.snapshot_id=created.snapshot_id
               )
             ORDER BY created.sequence",
        )?;
        let rows = statement.query_map([], |row| {
            let sequence = row.get::<_, i64>(3)?;
            Ok(ActiveBackup {
                snapshot_id: row.get(0)?,
                file_sha256: row.get(1)?,
                occurred_at: row.get(2)?,
                prior_checkpoint: ControlCheckpoint {
                    store_id: store_id.clone(),
                    sequence: sequence - 1,
                    current_hash: row.get(4)?,
                },
            })
        })?;
        let backups = rows.collect::<rusqlite::Result<Vec<_>>>()?;
        for backup in &backups {
            validate_sha256(&backup.file_sha256)?;
        }
        Ok(backups)
    }

    pub fn verify_backup_artifact(
        &self,
        snapshot_id: &str,
        file_sha256: &str,
    ) -> ControlResult<bool> {
        validate_sha256(file_sha256)?;
        Ok(self
            .active_backup_inventory()?
            .iter()
            .any(|backup| backup.snapshot_id == snapshot_id && backup.file_sha256 == file_sha256))
    }

    pub fn append_restore_started(
        &self,
        restore_epoch: &str,
        snapshot_id: &str,
        at: &str,
    ) -> ControlResult<()> {
        self.append_restore_event("restore_started", restore_epoch, snapshot_id, at)
    }

    pub fn append_restore_replayed(
        &self,
        restore_epoch: &str,
        snapshot_id: &str,
        at: &str,
    ) -> ControlResult<()> {
        self.append_restore_event("restore_replayed", restore_epoch, snapshot_id, at)
    }

    pub fn append_restore_completed(
        &self,
        _restore_epoch: &str,
        _snapshot_id: &str,
        _at: &str,
    ) -> ControlResult<()> {
        Err(ControlError::Invalid(
            "restore completion requires a verified evidence digest".to_owned(),
        ))
    }

    pub fn append_restore_completed_with_evidence(
        &self,
        restore_epoch: &str,
        snapshot_id: &str,
        at: &str,
        evidence_digest: &str,
    ) -> ControlResult<()> {
        validate_sha256(evidence_digest)?;
        let _custody = self.acquire_custody_operation_lock()?;
        let _publication = self.acquire_publication_lock()?;
        self.verify()?;
        let connection = self.open_read_only()?;
        let prior_stages: i64 = connection.query_row(
            "SELECT COUNT(*) FROM control_events
             WHERE restore_epoch=?1 AND snapshot_id=?2
               AND event_type IN ('restore_started','restore_replayed')",
            params![restore_epoch, snapshot_id],
            |row| row.get(0),
        )?;
        if prior_stages != 2 {
            return Err(ControlError::Integrity(
                "restore completion requires matching start and replay events".to_owned(),
            ));
        }
        drop(connection);
        self.append_event_locked(
            NewEvent {
                stable_key: restore_epoch,
                event_type: "restore_completed",
                device_id: None,
                token_hash: None,
                erasure_id: None,
                secret_hash: None,
                snapshot_id: Some(snapshot_id),
                restore_epoch: Some(restore_epoch),
                occurred_at: at,
                deadline_at: None,
                evidence_digest: Some(evidence_digest),
                artifact_name: None,
            },
            None,
        )
    }

    fn append_restore_event(
        &self,
        event_type: &'static str,
        restore_epoch: &str,
        snapshot_id: &str,
        at: &str,
    ) -> ControlResult<()> {
        self.append_event(NewEvent {
            stable_key: restore_epoch,
            event_type,
            device_id: None,
            token_hash: None,
            erasure_id: None,
            secret_hash: None,
            snapshot_id: Some(snapshot_id),
            restore_epoch: Some(restore_epoch),
            occurred_at: at,
            deadline_at: None,
            evidence_digest: None,
            artifact_name: None,
        })
    }

    pub fn pending_metric_erasures(&self) -> ControlResult<Vec<PendingErasure>> {
        self.verify()?;
        let connection = self.open_read_only()?;
        let mut statement = connection.prepare(
            "SELECT intent.device_id,intent.erasure_id
             FROM control_events intent
             WHERE intent.event_type='erasure_intent'
               AND EXISTS (SELECT 1 FROM control_events done WHERE done.event_type='health_erasure_verified' AND done.erasure_id=intent.erasure_id)
               AND NOT EXISTS (SELECT 1 FROM control_events done WHERE done.event_type='metrics_erasure_verified' AND done.erasure_id=intent.erasure_id)
             ORDER BY intent.sequence",
        )?;
        let rows = statement.query_map([], |row| {
            Ok(PendingErasure {
                device_id: row.get(0)?,
                erasure_id: row.get(1)?,
            })
        })?;
        Ok(rows.collect::<rusqlite::Result<Vec<_>>>()?)
    }

    pub fn seed_legacy(&self, health: &Connection) -> ControlResult<()> {
        self.seed_legacy_for_migration(health)?;
        Ok(())
    }

    pub fn legacy_seed_fingerprint(health: &Connection) -> ControlResult<String> {
        let source = load_legacy_seed_source(health)?;
        Ok(digest(&serde_json::to_vec(&source)?))
    }

    pub fn seed_legacy_for_migration(&self, health: &Connection) -> ControlResult<String> {
        if self.custody.is_some() {
            return Err(ControlError::Invalid(
                "legacy seed is only allowed before custody adoption".to_owned(),
            ));
        }
        let source = load_legacy_seed_source(health)?;
        let fingerprint = digest(&serde_json::to_vec(&source)?);
        let events = legacy_seed_events(&source, &fingerprint);
        let _publication = self.acquire_publication_lock()?;
        self.verify()?;
        let existing_ids = self.ordered_event_ids()?;
        let expected_ids = events
            .iter()
            .map(|event| event_id(&event.borrowed()))
            .collect::<Vec<_>>();
        if existing_ids.len() > expected_ids.len()
            || existing_ids != expected_ids[..existing_ids.len()]
        {
            return Err(ControlError::Integrity(
                "control store contains events unrelated to the legacy seed".to_owned(),
            ));
        }
        for event in events.iter().skip(existing_ids.len()) {
            self.append_event_locked(event.borrowed(), None)?;
        }
        if !self.verify_legacy_seed_locked(&events)? {
            return Err(ControlError::Integrity(
                "legacy seed verification did not match its source fingerprint".to_owned(),
            ));
        }
        Ok(fingerprint)
    }

    pub fn verify_legacy_seed(&self, health: &Connection) -> ControlResult<bool> {
        let source = load_legacy_seed_source(health)?;
        let fingerprint = digest(&serde_json::to_vec(&source)?);
        let events = legacy_seed_events(&source, &fingerprint);
        let _publication = self.acquire_publication_lock()?;
        self.verify()?;
        self.verify_legacy_seed_locked(&events)
    }

    pub fn has_incomplete_legacy_seed(&self, health: &Connection) -> ControlResult<bool> {
        let source = load_legacy_seed_source(health)?;
        let fingerprint = digest(&serde_json::to_vec(&source)?);
        let expected = legacy_seed_events(&source, &fingerprint)
            .iter()
            .map(|event| event_id(&event.borrowed()))
            .collect::<Vec<_>>();
        let _publication = self.acquire_publication_lock()?;
        self.verify()?;
        let existing = self.ordered_event_ids()?;
        Ok(existing.len() < expected.len() && existing == expected[..existing.len()])
    }

    pub fn reconcile_health(&self, health: &mut Connection) -> ControlResult<usize> {
        self.verify()?;
        let revoked = {
            let connection = self.open_read_only()?;
            let mut statement = connection.prepare(
                "SELECT device_id,occurred_at FROM control_events WHERE event_type='credential_revoked' ORDER BY sequence",
            )?;
            let rows = statement.query_map([], |row| {
                Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
            })?;
            rows.collect::<rusqlite::Result<Vec<_>>>()?
        };
        for (device_id, revoked_at) in revoked {
            health.execute(
                "UPDATE devices SET revoked_at=coalesce(revoked_at,?2) WHERE device_id=?1",
                params![device_id, revoked_at],
            )?;
        }
        let verified_erasures = {
            let connection = self.open_read_only()?;
            let mut statement = connection.prepare(
                "SELECT event_type,device_id,erasure_id,occurred_at FROM control_events WHERE event_type IN ('metrics_erasure_verified','backups_expired_verified') ORDER BY sequence",
            )?;
            let rows = statement.query_map([], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                    row.get::<_, String>(3)?,
                ))
            })?;
            rows.collect::<rusqlite::Result<Vec<_>>>()?
        };
        for (event_type, device_id, erasure_id, verified_at) in verified_erasures {
            if event_type == "metrics_erasure_verified" {
                health.execute(
                    "UPDATE erasures
                     SET device_id=CASE WHEN metrics_deleted_at IS NULL THEN ?1 ELSE device_id END,
                         metrics_deleted_at=coalesce(metrics_deleted_at,?3),last_error=NULL
                     WHERE erasure_id=?2",
                    params![digest(device_id.as_bytes()), erasure_id, verified_at],
                )?;
            } else {
                health.execute(
                    "UPDATE erasures
                     SET backups_expired_at=coalesce(backups_expired_at,?3)
                     WHERE erasure_id=?2",
                    params![device_id, erasure_id, verified_at],
                )?;
            }
        }
        let pending = self.pending_health_erasures()?;
        let mut reconciled = 0;
        for erasure in pending {
            let transaction = health.transaction_with_behavior(TransactionBehavior::Immediate)?;
            transaction.execute(
                "DELETE FROM outbox WHERE device_id=?1",
                [&erasure.device_id],
            )?;
            transaction.execute(
                "DELETE FROM receipts WHERE device_id=?1",
                [&erasure.device_id],
            )?;
            transaction.execute(
                "DELETE FROM events WHERE device_id=?1",
                [&erasure.device_id],
            )?;
            transaction.execute("DELETE FROM audit WHERE device_id=?1", [&erasure.device_id])?;
            transaction.execute(
                "DELETE FROM devices WHERE device_id=?1",
                [&erasure.device_id],
            )?;
            transaction.execute(
                "INSERT INTO erasures(device_id,erasure_id,erasure_secret_hash,requested_at,backup_delete_by)
                 VALUES (?1,?2,?3,?4,?5)
                 ON CONFLICT(device_id) DO UPDATE SET
                    erasure_id=excluded.erasure_id,
                    erasure_secret_hash=excluded.erasure_secret_hash,
                    requested_at=excluded.requested_at,
                    metrics_deleted_at=NULL,
                    backups_expired_at=NULL,
                    backup_delete_by=excluded.backup_delete_by,
                    last_error=NULL",
                params![
                    erasure.device_id,
                    erasure.erasure_id,
                    erasure.secret_hash,
                    erasure.requested_at,
                    erasure.deadline_at
                ],
            )?;
            transaction.commit()?;
            self.append_health_erasure_verified(
                &erasure.device_id,
                &erasure.erasure_id,
                &Utc::now().to_rfc3339(),
            )?;
            reconciled += 1;
        }
        Ok(reconciled)
    }

    pub fn replay_all_tombstones_for_restore(
        &self,
        health: &mut Connection,
    ) -> ControlResult<RestoreReplayReport> {
        let _publication = self.acquire_publication_lock()?;
        self.verify()?;
        let control = self.open_read_only()?;
        let store_id: String = control.query_row(
            "SELECT store_id FROM control_meta WHERE singleton=1",
            [],
            |row| row.get(0),
        )?;
        let health_store_id: String = health.query_row(
            "SELECT control_store_id FROM storage_meta WHERE singleton=1",
            [],
            |row| row.get(0),
        )?;
        if health_store_id != store_id {
            return Err(ControlError::Integrity(
                "restore health snapshot belongs to a different control store".to_owned(),
            ));
        }
        let revoked = {
            let mut statement = control.prepare(
                "SELECT device_id,occurred_at
                 FROM control_events
                 WHERE event_type='credential_revoked'
                 ORDER BY sequence",
            )?;
            let rows = statement.query_map([], |row| {
                Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
            })?;
            rows.collect::<rusqlite::Result<Vec<_>>>()?
        };
        let erasures = {
            let mut statement = control.prepare(
                "SELECT intent.device_id,intent.erasure_id,intent.secret_hash,
                        intent.occurred_at,intent.deadline_at,
                        (SELECT done.occurred_at FROM control_events done
                         WHERE done.event_type='metrics_erasure_verified'
                           AND done.erasure_id=intent.erasure_id
                         ORDER BY done.sequence DESC LIMIT 1),
                        (SELECT done.occurred_at FROM control_events done
                         WHERE done.event_type='backups_expired_verified'
                           AND done.erasure_id=intent.erasure_id
                         ORDER BY done.sequence DESC LIMIT 1),
                        EXISTS(
                            SELECT 1 FROM control_events legacy
                            WHERE legacy.event_type='legacy_imported'
                              AND legacy.erasure_id=intent.erasure_id
                        )
                 FROM control_events intent
                 WHERE intent.event_type='erasure_intent'
                 ORDER BY intent.sequence",
            )?;
            let rows = statement.query_map([], |row| {
                Ok(RestoreErasure {
                    device_id: row.get(0)?,
                    erasure_id: row.get(1)?,
                    secret_hash: row.get(2)?,
                    requested_at: row.get(3)?,
                    deadline_at: row.get(4)?,
                    metrics_deleted_at: row.get(5)?,
                    backups_expired_at: row.get(6)?,
                    device_id_already_anonymized: row.get(7)?,
                })
            })?;
            rows.collect::<rusqlite::Result<Vec<_>>>()?
        };

        let transaction = health.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let cleared_pairing_codes = transaction.execute("DELETE FROM pairing_codes", [])?;
        for (device_id, revoked_at) in &revoked {
            transaction.execute(
                "UPDATE devices SET revoked_at=coalesce(revoked_at,?2) WHERE device_id=?1",
                params![device_id, revoked_at],
            )?;
        }
        for erasure in &erasures {
            let anonymized_device_id = if erasure.device_id_already_anonymized {
                erasure.device_id.clone()
            } else {
                digest(erasure.device_id.as_bytes())
            };
            for retired_id in [&erasure.device_id, &anonymized_device_id] {
                transaction.execute("DELETE FROM outbox WHERE device_id=?1", [retired_id])?;
                transaction.execute("DELETE FROM receipts WHERE device_id=?1", [retired_id])?;
                transaction.execute("DELETE FROM events WHERE device_id=?1", [retired_id])?;
                transaction.execute("DELETE FROM audit WHERE device_id=?1", [retired_id])?;
                transaction.execute("DELETE FROM devices WHERE device_id=?1", [retired_id])?;
            }
            transaction.execute(
                "DELETE FROM erasures WHERE erasure_id=?1 OR device_id IN (?2,?3)",
                params![erasure.erasure_id, erasure.device_id, anonymized_device_id],
            )?;
            let stored_device_id = if erasure.metrics_deleted_at.is_some() {
                anonymized_device_id
            } else {
                erasure.device_id.clone()
            };
            transaction.execute(
                "INSERT INTO erasures(
                    device_id,erasure_id,erasure_secret_hash,requested_at,
                    metrics_deleted_at,backups_expired_at,backup_delete_by,last_error
                 ) VALUES (?1,?2,?3,?4,?5,?6,?7,NULL)",
                params![
                    stored_device_id,
                    erasure.erasure_id,
                    erasure.secret_hash,
                    erasure.requested_at,
                    erasure.metrics_deleted_at,
                    erasure.backups_expired_at,
                    erasure.deadline_at
                ],
            )?;
        }
        transaction.commit()?;
        Ok(RestoreReplayReport {
            revoked_devices: revoked.len(),
            erased_devices: erasures.len(),
            cleared_pairing_codes,
        })
    }

    fn pending_health_erasures(&self) -> ControlResult<Vec<PendingHealthErasure>> {
        self.verify()?;
        let connection = self.open_read_only()?;
        let mut statement = connection.prepare(
            "SELECT intent.device_id,intent.erasure_id,intent.secret_hash,intent.occurred_at,intent.deadline_at
             FROM control_events intent
             WHERE intent.event_type='erasure_intent'
               AND NOT EXISTS (SELECT 1 FROM control_events done WHERE done.event_type='health_erasure_verified' AND done.erasure_id=intent.erasure_id)
             ORDER BY intent.sequence",
        )?;
        let rows = statement.query_map([], |row| {
            Ok(PendingHealthErasure {
                device_id: row.get(0)?,
                erasure_id: row.get(1)?,
                secret_hash: row.get(2)?,
                requested_at: row.get(3)?,
                deadline_at: row.get(4)?,
            })
        })?;
        Ok(rows.collect::<rusqlite::Result<Vec<_>>>()?)
    }

    fn ordered_event_ids(&self) -> ControlResult<Vec<String>> {
        let connection = self.open_read_only()?;
        let mut statement =
            connection.prepare("SELECT event_id FROM control_events ORDER BY sequence")?;
        let rows = statement.query_map([], |row| row.get::<_, String>(0))?;
        Ok(rows.collect::<rusqlite::Result<Vec<_>>>()?)
    }

    fn verify_legacy_seed_locked(&self, events: &[OwnedEvent]) -> ControlResult<bool> {
        let expected_ids = events
            .iter()
            .map(|event| event_id(&event.borrowed()))
            .collect::<Vec<_>>();
        if self.ordered_event_ids()? != expected_ids {
            return Ok(false);
        }
        let connection = self.open_read_only()?;
        for event in events {
            let borrowed = event.borrowed();
            let existing = find_existing_event(&connection, &event_id(&borrowed))?;
            if !existing
                .as_ref()
                .is_some_and(|existing| event_matches(existing, &borrowed))
            {
                return Ok(false);
            }
        }
        Ok(true)
    }

    fn append_event(&self, event: NewEvent<'_>) -> ControlResult<()> {
        // SQLite serializes database writers, but the durable publication also
        // spans a create_new mirror record and an atomic head replacement. A
        // separate cross-process file lock makes that whole sequence one
        // ordered publication unit instead of allowing two writers to race
        // after either SQLite transaction commits.
        let _custody = self.acquire_custody_operation_lock()?;
        let _publication = self.acquire_publication_lock()?;
        self.append_event_locked(event, None)
    }

    fn append_event_locked(
        &self,
        event: NewEvent<'_>,
        expected_previous: Option<&ControlCheckpoint>,
    ) -> ControlResult<()> {
        if self.custody.is_some() {
            return self.append_event_custodied_locked(event, expected_previous);
        }
        self.append_event_local_locked(event, expected_previous)
    }

    /// Re-enters the one durable authority operation, if any. Call this while
    /// the outer lifecycle/health fence is held, before serving or beginning a
    /// different write. An unresolved reservation is never silently skipped.
    pub fn resume_pending_custody(&self) -> ControlResult<()> {
        if self.custody.is_none() {
            return Err(ControlError::Invalid(
                "off-host custody is required for publication recovery".to_owned(),
            ));
        }
        let _custody = self.acquire_custody_operation_lock()?;
        let _publication = self.acquire_publication_lock()?;
        if let Some(intent) = self.read_pending_intent()? {
            self.resume_intent_locked(intent, false)?;
        }
        self.verify_custody_locked()
    }

    /// Exact current-head check; a local file cannot substitute for the
    /// independently administered control checkpoint.
    pub fn verify_custody(&self) -> ControlResult<()> {
        let _custody = self.acquire_custody_operation_lock()?;
        self.verify_custody_while_locked()
    }

    /// The caller must already hold the shared custody-operation lock. This
    /// avoids recursive file-lock acquisition on the acknowledgement path.
    pub fn verify_custody_while_locked(&self) -> ControlResult<()> {
        let _publication = self.acquire_publication_lock()?;
        if self.read_pending_intent()?.is_some() {
            return Err(ControlError::Integrity(
                "a control custody operation is unresolved".to_owned(),
            ));
        }
        self.verify_custody_locked()
    }

    fn verify_custody_locked(&self) -> ControlResult<()> {
        let custody = self.custody.as_ref().ok_or_else(|| {
            ControlError::Invalid("off-host custody is not configured".to_owned())
        })?;
        let local = self.checkpoint()?;
        let remote = custody.read_v2(&local.store_id).map_err(custody_failure)?;
        if remote.control != local {
            return Err(ControlError::Integrity(
                "off-host and local control heads differ".to_owned(),
            ));
        }
        self.verify_remote_binding(&remote)?;
        Ok(())
    }

    fn verify_remote_binding(&self, remote: &CustodyState) -> ControlResult<()> {
        if let Some(journal) = &self.ack_journal {
            journal.verify_generation_zero_custody(remote)?;
        } else if remote.ack.is_some()
            || remote.baseline_sha256.is_some()
            || u64::try_from(remote.control.sequence).ok() != Some(remote.revision)
        {
            // An unbound store can only describe fresh, pre-adoption control
            // publications. It must not silently accept another authority.
            return Err(ControlError::Integrity(
                "off-host custody has an unverified journal, baseline, or revision".to_owned(),
            ));
        }
        Ok(())
    }

    fn append_event_custodied_locked(
        &self,
        event: NewEvent<'_>,
        expected_previous: Option<&ControlCheckpoint>,
    ) -> ControlResult<()> {
        let requested_id = event_id(&event);
        if let Some(pending) = self.read_pending_intent()? {
            let same = pending.body.event_id == requested_id
                && pending.body.event == IntentEvent::from_event(&event);
            self.resume_intent_locked(pending, false)?;
            if same {
                return Ok(());
            }
        }
        let previous = self.checkpoint()?;
        if let Some(expected) = expected_previous
            && *expected != previous
        {
            return Err(ControlError::Integrity(
                "control head advanced after the required prior checkpoint".to_owned(),
            ));
        }
        if event.event_type.starts_with("restore_") {
            return Err(ControlError::Integrity(
                "operational restore facts require the P0-R2.2 verified activation path".to_owned(),
            ));
        }
        let backup_proof = if matches!(
            event.event_type,
            "backup_created" | "backup_delete_intent" | "backup_deleted"
        ) {
            Some(self.capture_backup_proof(&event, &previous)?)
        } else {
            None
        };
        let expiry_proof = if event.event_type == "backups_expired_verified" {
            Some(self.capture_backup_expiry_proof(&event)?)
        } else {
            None
        };
        let custody = self.custody.as_ref().ok_or_else(|| {
            ControlError::Invalid("off-host custody is not configured".to_owned())
        })?;
        let remote = custody
            .read_v2(&previous.store_id)
            .map_err(custody_failure)?;
        if remote.control != previous {
            return Err(ControlError::Integrity(
                "off-host and local control heads differ".to_owned(),
            ));
        }
        self.verify_remote_binding(&remote)?;
        let connection = self.open_read_only()?;
        if let Some(existing) = find_existing_event(&connection, &requested_id)? {
            if !event_matches(&existing, &event) {
                return Err(ControlError::Integrity(
                    "stable event ID was reused with different content".to_owned(),
                ));
            }
            return Ok(());
        }
        let current_hash = event_hash_versioned(
            2,
            &HashRecord {
                event_id: &requested_id,
                event_type: event.event_type,
                device_id: event.device_id,
                token_hash: event.token_hash,
                erasure_id: event.erasure_id,
                secret_hash: event.secret_hash,
                snapshot_id: event.snapshot_id,
                restore_epoch: event.restore_epoch,
                occurred_at: event.occurred_at,
                deadline_at: event.deadline_at,
                previous_hash: &previous.current_hash,
            },
            event.evidence_digest,
            event.artifact_name,
        )?;
        let mut successor = remote.clone();
        successor.revision = successor
            .revision
            .checked_add(1)
            .ok_or_else(|| ControlError::Integrity("custody revision overflow".to_owned()))?;
        successor.control = ControlCheckpoint {
            store_id: previous.store_id.clone(),
            sequence: previous
                .sequence
                .checked_add(1)
                .ok_or_else(|| ControlError::Integrity("control sequence overflow".to_owned()))?,
            current_hash,
        };
        let intent = ControlIntentDisk {
            body: ControlIntentBody {
                format: 1,
                event: IntentEvent::from_event(&event),
                event_id: requested_id,
                predecessor: remote,
                successor,
                backup_proof,
                expiry_proof,
            },
            reservation: None,
        };
        self.write_pending_intent(&intent, true)?;
        self.resume_intent_locked(intent, true)
    }

    fn resume_intent_locked(
        &self,
        mut intent: ControlIntentDisk,
        artifact_preconditions_checked: bool,
    ) -> ControlResult<()> {
        intent.validate()?;
        self.verify_remote_binding(&intent.body.predecessor)?;
        self.verify_remote_binding(&intent.body.successor)?;
        self.verify_intent_evidence(&intent, artifact_preconditions_checked)?;
        let custody = self.custody.as_ref().ok_or_else(|| {
            ControlError::Invalid("off-host custody is not configured".to_owned())
        })?;
        let intent_sha = digest(&serde_json::to_vec(&intent.body)?);
        let remote = custody.read_v2(&intent.body.predecessor.control.store_id);
        if let Ok(state) = &remote {
            self.verify_remote_binding(state)?;
            if state == &intent.body.successor {
                self.reconcile_intent_local_locked(&intent, false)?;
                self.remove_pending_intent()?;
                return Ok(());
            }
            if state != &intent.body.predecessor {
                return Err(ControlError::Integrity(
                    "off-host custody diverges from the durable control intent".to_owned(),
                ));
            }
        }
        // A read may fail solely because this exact reservation is pending.
        // Reserve is idempotent by predecessor, event ID and intent digest;
        // transport failure remains an error and cannot authorize local work.
        let reservation = custody
            .reserve_v2(&intent.body.predecessor, &intent.body.event_id, &intent_sha)
            .map_err(custody_failure)?;
        if let Some(stored) = &intent.reservation
            && stored != &reservation
        {
            return Err(ControlError::Integrity(
                "off-host reservation differs from the persisted reservation".to_owned(),
            ));
        }
        if intent.reservation.is_none() {
            intent.reservation = Some(reservation.clone());
            self.write_pending_intent(&intent, false)?;
        }
        self.reconcile_intent_local_locked(&intent, true)?;
        // A durable SQLite tail alone does not prove that its external
        // artifact survived until independent confirmation.
        self.verify_intent_evidence(&intent, artifact_preconditions_checked)?;
        let cas_error = match custody.compare_and_swap_v2(&reservation, &intent.body.successor) {
            Ok(actual) if actual == intent.body.successor => None,
            Ok(_) => {
                return Err(ControlError::Integrity(
                    "off-host CAS returned a different control successor".to_owned(),
                ));
            }
            Err(error) => Some(error),
        };
        // A matching CAS reply is not proof that the independent store
        // persisted the successor. Keep this durable intent until a separate
        // exact read succeeds, including after a lost or forged CAS reply.
        let readback = custody
            .read_v2(&intent.body.predecessor.control.store_id)
            .map_err(|read_error| custody_failure(cas_error.unwrap_or(read_error)))?;
        if readback != intent.body.successor {
            return Err(ControlError::Integrity(
                "off-host CAS is not confirmed by exact readback".to_owned(),
            ));
        }
        self.verify_remote_binding(&readback)?;
        self.verify()?;
        self.remove_pending_intent()
    }

    fn reconcile_intent_local_locked(
        &self,
        intent: &ControlIntentDisk,
        may_commit: bool,
    ) -> ControlResult<()> {
        let connection = self.open_read_only()?;
        let (sequence, hash): (i64, String) = connection
            .query_row(
                "SELECT sequence,current_hash FROM control_events ORDER BY sequence DESC LIMIT 1",
                [],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .optional()?
            .unwrap_or((0, GENESIS_HASH.to_owned()));
        if sequence == intent.body.successor.control.sequence
            && hash == intent.body.successor.control.current_hash
        {
            drop(connection);
            return self.reconcile_publication_locked(&intent.body.successor.control);
        }
        if sequence != intent.body.predecessor.control.sequence
            || hash != intent.body.predecessor.control.current_hash
        {
            return Err(ControlError::Integrity(
                "local control database diverges from the durable intent".to_owned(),
            ));
        }
        drop(connection);
        self.verify()?;
        if !may_commit {
            return Err(ControlError::Integrity(
                "off-host head advanced while local control event is absent".to_owned(),
            ));
        }
        self.append_event_local_locked(
            intent.body.event.borrowed(),
            Some(&intent.body.predecessor.control),
        )?;
        if self.checkpoint()? != intent.body.successor.control {
            return Err(ControlError::Integrity(
                "locally committed event differs from the reserved successor".to_owned(),
            ));
        }
        Ok(())
    }

    fn verify_intent_evidence(
        &self,
        intent: &ControlIntentDisk,
        caller_preconditions_checked: bool,
    ) -> ControlResult<()> {
        let event = intent.body.event.borrowed();
        if let Some(expected) = &intent.body.backup_proof {
            let actual = self.capture_backup_proof(&event, &intent.body.predecessor.control)?;
            if actual != *expected {
                return Err(ControlError::Integrity(
                    "managed backup artifact or manifest changed after custody reservation"
                        .to_owned(),
                ));
            }
        } else if matches!(
            event.event_type,
            "backup_created" | "backup_delete_intent" | "backup_deleted"
        ) {
            return Err(ControlError::Integrity(
                "custodied backup intent has no durable artifact proof".to_owned(),
            ));
        }
        if let Some(expected) = &intent.body.expiry_proof {
            let actual = self.capture_backup_expiry_proof(&event)?;
            if actual != *expected {
                return Err(ControlError::Integrity(
                    "managed backup inventory changed after custody reservation".to_owned(),
                ));
            }
        } else if event.event_type == "backups_expired_verified" {
            return Err(ControlError::Integrity(
                "backup expiry intent has no complete inventory proof".to_owned(),
            ));
        } else if !caller_preconditions_checked
            && matches!(
                event.event_type,
                "restore_started"
                    | "restore_replayed"
                    | "restore_completed"
                    | "health_erasure_verified"
                    | "metrics_erasure_verified"
            )
        {
            return Err(ControlError::Integrity(
                "external recovery or erasure evidence requires operator revalidation".to_owned(),
            ));
        }
        Ok(())
    }

    fn capture_backup_proof(
        &self,
        event: &NewEvent<'_>,
        predecessor: &ControlCheckpoint,
    ) -> ControlResult<BackupArtifactProof> {
        let root = self.managed_backup_root.as_ref().ok_or_else(|| {
            ControlError::Integrity("custodied backup has no validated managed root".to_owned())
        })?;
        let (root_device, root_inode) = backup_root_identity(root)?;
        let name = event.artifact_name.ok_or_else(|| {
            ControlError::Integrity("custodied backup has no artifact name".to_owned())
        })?;
        validate_artifact_name(name)?;
        let snapshot_id = event.snapshot_id.ok_or_else(|| {
            ControlError::Integrity("custodied backup has no snapshot ID".to_owned())
        })?;
        let expected_hash = event.secret_hash.ok_or_else(|| {
            ControlError::Integrity("custodied backup has no file hash".to_owned())
        })?;
        validate_sha256(expected_hash)?;
        let manifest_name = format!("{name}.meta.json");
        let (database, manifest) = if event.event_type == "backup_deleted" {
            let connection = self.open_read_only()?;
            let existing: Option<(String, String)> = connection
                .query_row(
                    "SELECT secret_hash,artifact_name FROM control_events \
                     WHERE event_type='backup_delete_intent' AND snapshot_id=?1",
                    [snapshot_id],
                    |row| Ok((row.get(0)?, row.get(1)?)),
                )
                .optional()?;
            if existing
                .as_ref()
                .map(|(hash, artifact)| (hash.as_str(), artifact.as_str()))
                != Some((expected_hash, name))
            {
                return Err(ControlError::Integrity(
                    "deleted backup does not match its committed deletion intent".to_owned(),
                ));
            }
            require_absent_backup_entry(root, name)?;
            require_absent_backup_entry(root, &manifest_name)?;
            (None, None)
        } else {
            let (database, _) = fingerprint_backup_entry(root, name, false)?;
            if database.sha256 != expected_hash {
                return Err(ControlError::Integrity(
                    "managed backup bytes differ from the control event".to_owned(),
                ));
            }
            let (manifest, bytes) = fingerprint_backup_entry(root, &manifest_name, true)?;
            let value: serde_json::Value = serde_json::from_slice(&bytes)?;
            if value.get("snapshot_id").and_then(serde_json::Value::as_str) != Some(snapshot_id)
                || value.get("file_sha256").and_then(serde_json::Value::as_str)
                    != Some(expected_hash)
            {
                return Err(ControlError::Integrity(
                    "managed backup manifest differs from the control event".to_owned(),
                ));
            }
            if value
                .get("source_schema_version")
                .and_then(serde_json::Value::as_i64)
                != Some(crate::database::HEALTH_SCHEMA_VERSION)
                || !value.get("source_commit_sequence").is_some_and(|sequence| {
                    sequence.is_null() || sequence.as_i64().is_some_and(|number| number >= 0)
                })
            {
                return Err(ControlError::Integrity(
                    "managed backup manifest has an unsupported schema or commit sequence"
                        .to_owned(),
                ));
            }
            let started_at = value
                .get("snapshot_started_at")
                .and_then(serde_json::Value::as_str)
                .ok_or_else(|| {
                    ControlError::Integrity(
                        "managed backup manifest has no snapshot time".to_owned(),
                    )
                })?;
            DateTime::parse_from_rfc3339(started_at).map_err(|_| {
                ControlError::Integrity(
                    "managed backup manifest has invalid snapshot time".to_owned(),
                )
            })?;
            let checkpoint: ControlCheckpoint = serde_json::from_value(
                value.get("control_checkpoint").cloned().ok_or_else(|| {
                    ControlError::Integrity("backup manifest has no control checkpoint".to_owned())
                })?,
            )?;
            let (expected_checkpoint, expected_started_at) = if event.event_type == "backup_created"
            {
                (predecessor.clone(), event.occurred_at.to_owned())
            } else {
                let connection = self.open_read_only()?;
                let created: Option<(i64, String, String, String)> = connection
                    .query_row(
                        "SELECT sequence,previous_hash,secret_hash,occurred_at FROM control_events \
                         WHERE event_type='backup_created' AND snapshot_id=?1",
                        [snapshot_id],
                        |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
                    )
                    .optional()?;
                let (sequence, previous_hash, created_hash, created_at) =
                    created.ok_or_else(|| {
                        ControlError::Integrity("backup creation fact is missing".to_owned())
                    })?;
                if created_hash != expected_hash {
                    return Err(ControlError::Integrity(
                        "backup creation hash differs from deletion".to_owned(),
                    ));
                }
                (
                    ControlCheckpoint {
                        store_id: predecessor.store_id.clone(),
                        sequence: sequence - 1,
                        current_hash: previous_hash,
                    },
                    created_at,
                )
            };
            if checkpoint != expected_checkpoint || started_at != expected_started_at {
                return Err(ControlError::Integrity(
                    "managed backup manifest control checkpoint or time changed".to_owned(),
                ));
            }
            (Some(database), Some(manifest))
        };
        // A second manifest advertising the same snapshot would make a
        // deletion or creation claim ambiguous even if the named pair matches.
        for entry in fs::read_dir(root)? {
            let entry = entry?;
            let filename = entry.file_name();
            let filename = filename.to_str().ok_or_else(|| {
                ControlError::Integrity("non-UTF-8 managed backup entry".to_owned())
            })?;
            if !filename.ends_with(".db.meta.json") || filename == manifest_name {
                if filename.starts_with("boaz-health-") && filename.ends_with(".db") {
                    validate_artifact_name(filename)?;
                    require_existing_backup_entry(root, &format!("{filename}.meta.json"))?;
                }
                continue;
            }
            let (_, bytes) = fingerprint_backup_entry(root, filename, true)?;
            let value: serde_json::Value = serde_json::from_slice(&bytes)?;
            if value.get("snapshot_id").and_then(serde_json::Value::as_str) == Some(snapshot_id) {
                return Err(ControlError::Integrity(
                    "another managed manifest advertises this snapshot".to_owned(),
                ));
            }
        }
        if backup_root_identity(root)? != (root_device, root_inode) {
            return Err(ControlError::Integrity(
                "managed backup directory changed during verification".to_owned(),
            ));
        }
        Ok(BackupArtifactProof {
            root: root.clone(),
            root_device,
            root_inode,
            artifact_name: name.to_owned(),
            database,
            manifest,
        })
    }

    fn capture_backup_expiry_proof(
        &self,
        event: &NewEvent<'_>,
    ) -> ControlResult<BackupExpiryProof> {
        let root = self.managed_backup_root.as_ref().ok_or_else(|| {
            ControlError::Integrity("backup expiry has no validated managed root".to_owned())
        })?;
        let (root_device, root_inode) = backup_root_identity(root)?;
        let erasure_id = event
            .erasure_id
            .ok_or_else(|| ControlError::Integrity("backup expiry has no erasure ID".to_owned()))?;
        let device_id = event
            .device_id
            .ok_or_else(|| ControlError::Integrity("backup expiry has no device ID".to_owned()))?;
        let connection = self.open_read_only()?;
        let erasure: Option<(String, String)> = connection
            .query_row(
                "SELECT device_id,occurred_at FROM control_events \
                 WHERE event_type='erasure_intent' AND erasure_id=?1",
                [erasure_id],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .optional()?;
        let (stored_device, requested_at) = erasure.ok_or_else(|| {
            ControlError::Integrity("backup expiry has no erasure intent".to_owned())
        })?;
        if stored_device != device_id {
            return Err(ControlError::Integrity(
                "backup expiry device differs from erasure intent".to_owned(),
            ));
        }
        let requested = DateTime::parse_from_rfc3339(&requested_at).map_err(|_| {
            ControlError::Integrity("erasure intent has invalid request time".to_owned())
        })?;
        let store_id: String = connection.query_row(
            "SELECT store_id FROM control_meta WHERE singleton=1",
            [],
            |row| row.get(0),
        )?;
        let mut statement = connection.prepare(
            "SELECT created.snapshot_id,created.secret_hash,created.occurred_at, \
                    created.sequence,created.previous_hash \
             FROM control_events created \
             WHERE created.event_type='backup_created' \
               AND NOT EXISTS (SELECT 1 FROM control_events deleted \
                               WHERE deleted.event_type='backup_deleted' \
                                 AND deleted.snapshot_id=created.snapshot_id)",
        )?;
        let rows = statement.query_map([], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
                row.get::<_, i64>(3)?,
                row.get::<_, String>(4)?,
            ))
        })?;
        let mut active = BTreeMap::new();
        for row in rows {
            let (snapshot, hash, created_at, sequence, previous_hash) = row?;
            if active
                .insert(snapshot, (hash, created_at, sequence, previous_hash))
                .is_some()
            {
                return Err(ControlError::Integrity(
                    "duplicate active backup snapshot".to_owned(),
                ));
            }
        }
        let mut databases = BTreeSet::new();
        let mut manifests = BTreeSet::new();
        for entry in fs::read_dir(root)? {
            let entry = entry?;
            let name = entry.file_name();
            let name = name.to_str().ok_or_else(|| {
                ControlError::Integrity("non-UTF-8 managed backup entry".to_owned())
            })?;
            if let Some(database_name) = name.strip_suffix(".meta.json") {
                validate_artifact_name(database_name)?;
                manifests.insert(database_name.to_owned());
            } else {
                validate_artifact_name(name)?;
                databases.insert(name.to_owned());
            }
        }
        if databases != manifests || databases.len() != active.len() {
            return Err(ControlError::Integrity(
                "managed backup files, manifests and control inventory differ".to_owned(),
            ));
        }
        let mut inventory = Vec::new();
        for name in databases {
            let (_, manifest_bytes) =
                fingerprint_backup_entry(root, &format!("{name}.meta.json"), true)?;
            let value: serde_json::Value = serde_json::from_slice(&manifest_bytes)?;
            let snapshot_id = value
                .get("snapshot_id")
                .and_then(serde_json::Value::as_str)
                .ok_or_else(|| ControlError::Integrity("backup has no snapshot ID".to_owned()))?;
            let (hash, created_at, sequence, previous_hash) =
                active.remove(snapshot_id).ok_or_else(|| {
                    ControlError::Integrity("backup has no active control fact".to_owned())
                })?;
            let prior = ControlCheckpoint {
                store_id: store_id.clone(),
                sequence: sequence - 1,
                current_hash: previous_hash,
            };
            let proof_event = NewEvent {
                stable_key: snapshot_id,
                event_type: "backup_created",
                device_id: None,
                token_hash: None,
                erasure_id: None,
                secret_hash: Some(&hash),
                snapshot_id: Some(snapshot_id),
                restore_epoch: None,
                occurred_at: &created_at,
                deadline_at: None,
                evidence_digest: None,
                artifact_name: Some(&name),
            };
            let proof = self.capture_backup_proof(&proof_event, &prior)?;
            let started_text = value
                .get("snapshot_started_at")
                .and_then(serde_json::Value::as_str)
                .ok_or_else(|| ControlError::Integrity("backup has no snapshot time".to_owned()))?;
            let started = DateTime::parse_from_rfc3339(started_text).map_err(|_| {
                ControlError::Integrity("backup has invalid snapshot time".to_owned())
            })?;
            if started <= requested {
                return Err(ControlError::Integrity(
                    "a pre-erasure managed backup is still retained".to_owned(),
                ));
            }
            inventory.push((name, snapshot_id.to_owned(), proof));
        }
        if !active.is_empty() || backup_root_identity(root)? != (root_device, root_inode) {
            return Err(ControlError::Integrity(
                "managed backup inventory changed during expiry verification".to_owned(),
            ));
        }
        Ok(BackupExpiryProof {
            root: root.clone(),
            root_device,
            root_inode,
            requested_at,
            complete_inventory_sha256: digest(&serde_json::to_vec(&inventory)?),
        })
    }

    fn read_pending_intent(&self) -> ControlResult<Option<ControlIntentDisk>> {
        match fs::symlink_metadata(&self.pending_intent_path) {
            Ok(_) => {
                reject_link_or_non_file(&self.pending_intent_path)?;
                let intent: ControlIntentDisk =
                    serde_json::from_slice(&fs::read(&self.pending_intent_path)?)?;
                intent.validate()?;
                Ok(Some(intent))
            }
            Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(None),
            Err(error) => Err(error.into()),
        }
    }

    fn write_pending_intent(&self, intent: &ControlIntentDisk, first: bool) -> ControlResult<()> {
        let bytes = serde_json::to_vec(intent)?;
        if first {
            let mut file = OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(&self.pending_intent_path)?;
            file.write_all(&bytes)?;
            file.sync_all()?;
            set_private_file(&self.pending_intent_path)?;
        } else {
            reject_link_or_non_file(&self.pending_intent_path)?;
            let parent = self
                .pending_intent_path
                .parent()
                .ok_or_else(|| ControlError::Invalid("control intent has no parent".to_owned()))?;
            let temp = parent.join(format!(".control-intent-{}.tmp", uuid::Uuid::new_v4()));
            let mut file = OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(&temp)?;
            file.write_all(&bytes)?;
            file.sync_all()?;
            set_private_file(&temp)?;
            fs::rename(&temp, &self.pending_intent_path)?;
        }
        let parent = self
            .pending_intent_path
            .parent()
            .ok_or_else(|| ControlError::Invalid("control intent has no parent".to_owned()))?;
        OpenOptions::new().read(true).open(parent)?.sync_all()?;
        Ok(())
    }

    fn remove_pending_intent(&self) -> ControlResult<()> {
        reject_link_or_non_file(&self.pending_intent_path)?;
        fs::remove_file(&self.pending_intent_path)?;
        let parent = self
            .pending_intent_path
            .parent()
            .ok_or_else(|| ControlError::Invalid("control intent has no parent".to_owned()))?;
        OpenOptions::new().read(true).open(parent)?.sync_all()?;
        Ok(())
    }

    fn append_event_local_locked(
        &self,
        event: NewEvent<'_>,
        expected_previous: Option<&ControlCheckpoint>,
    ) -> ControlResult<()> {
        if self.custody.is_none() && self.read_pending_intent()?.is_some() {
            return Err(ControlError::Integrity(
                "local-only publication cannot bypass an unresolved custody intent".to_owned(),
            ));
        }
        self.verify()?;
        let mut connection = self.open_read_write()?;
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let event_id = event_id(&event);
        let existing = find_existing_event(&transaction, &event_id)?;
        if let Some(existing) = existing {
            if event_matches(&existing, &event) {
                self.verify()?;
                return Ok(());
            }
            return Err(ControlError::Integrity(
                "stable event ID was reused with different content".to_owned(),
            ));
        }
        let (previous_sequence, previous_hash): (i64, String) = transaction
            .query_row(
                "SELECT sequence,current_hash FROM control_events ORDER BY sequence DESC LIMIT 1",
                [],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .optional()?
            .unwrap_or((0, GENESIS_HASH.to_owned()));
        let store_id: String = transaction.query_row(
            "SELECT store_id FROM control_meta WHERE singleton=1",
            [],
            |row| row.get(0),
        )?;
        if let Some(expected) = expected_previous
            && (expected.store_id != store_id
                || expected.sequence != previous_sequence
                || expected.current_hash != previous_hash)
        {
            return Err(ControlError::Integrity(
                "control head advanced after the backup manifest checkpoint".to_owned(),
            ));
        }
        let current_hash = event_hash_versioned(
            2,
            &HashRecord {
                event_id: &event_id,
                event_type: event.event_type,
                device_id: event.device_id,
                token_hash: event.token_hash,
                erasure_id: event.erasure_id,
                secret_hash: event.secret_hash,
                snapshot_id: event.snapshot_id,
                restore_epoch: event.restore_epoch,
                occurred_at: event.occurred_at,
                deadline_at: event.deadline_at,
                previous_hash: &previous_hash,
            },
            event.evidence_digest,
            event.artifact_name,
        )?;
        let sequence = previous_sequence + 1;
        transaction.execute(
            "INSERT INTO control_events_suffix(sequence,event_id,event_type,device_id,token_hash,erasure_id,secret_hash,snapshot_id,restore_epoch,occurred_at,deadline_at,previous_hash,current_hash,hash_version,evidence_digest,artifact_name)
             VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13,2,?14,?15)",
            params![sequence,event_id,event.event_type,event.device_id,event.token_hash,event.erasure_id,event.secret_hash,event.snapshot_id,event.restore_epoch,event.occurred_at,event.deadline_at,previous_hash,current_hash,event.evidence_digest,event.artifact_name],
        )?;
        let actual_sequence: i64 = transaction.query_row(
            "SELECT sequence FROM control_events WHERE event_id=?1",
            [&event_id],
            |row| row.get(0),
        )?;
        if actual_sequence != sequence {
            return Err(ControlError::Integrity(
                "control event sequence is not contiguous".to_owned(),
            ));
        }
        transaction.commit()?;
        let mirror = serde_json::to_vec(&serde_json::json!({
            "store_id": store_id,
            "sequence": sequence,
            "event_id": event_id,
            "current_hash": current_hash,
        }))?;
        write_mirror_create_new(&self.mirror_dir, sequence, &current_hash, &mirror)?;
        write_head_atomic(
            &self.head_path,
            &ControlHead {
                store_id,
                sequence,
                current_hash,
            },
        )?;
        self.verify()
    }

    fn acquire_publication_lock(&self) -> ControlResult<File> {
        acquire_private_lock(&self.publication_lock_path, "publication", true)
    }

    fn acquire_custody_operation_lock(&self) -> ControlResult<Option<File>> {
        self.custody_lock_path
            .as_deref()
            .map(|path| acquire_private_lock(path, "custody operation", false))
            .transpose()
    }

    fn open_read_only(&self) -> ControlResult<Connection> {
        Connection::open_with_flags(&self.db_path, OpenFlags::SQLITE_OPEN_READ_ONLY)
            .map_err(ControlError::from)
    }

    fn open_preflight_read_only(&self) -> ControlResult<Connection> {
        let raw_path = self.db_path.to_str().ok_or_else(|| {
            ControlError::Invalid("control database path is not UTF-8".to_owned())
        })?;
        if !self.db_path.is_absolute() {
            return Err(ControlError::Invalid(
                "control database preflight needs an absolute path".to_owned(),
            ));
        }
        let mut uri = String::from("file:");
        for byte in raw_path.bytes() {
            if byte.is_ascii_alphanumeric() || matches!(byte, b'/' | b'-' | b'_' | b'.' | b'~') {
                uri.push(char::from(byte));
            } else {
                uri.push_str(&format!("%{byte:02X}"));
            }
        }
        uri.push_str("?immutable=1");
        Connection::open_with_flags(
            uri,
            OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_URI,
        )
        .map_err(ControlError::from)
    }

    fn open_read_write(&self) -> ControlResult<Connection> {
        let connection =
            Connection::open_with_flags(&self.db_path, OpenFlags::SQLITE_OPEN_READ_WRITE)?;
        configure(&connection)?;
        Ok(connection)
    }
}

fn configure(connection: &Connection) -> ControlResult<()> {
    connection.busy_timeout(Duration::from_secs(5))?;
    connection.execute_batch(
        "PRAGMA journal_mode=WAL;
         PRAGMA foreign_keys=ON;
         PRAGMA synchronous=FULL;
         PRAGMA secure_delete=ON;",
    )?;
    Ok(())
}

fn custody_failure(error: crate::custody::CustodyError) -> ControlError {
    ControlError::Integrity(format!(
        "independent custody is unavailable or rejected the operation: {error}"
    ))
}

fn acquire_private_lock(path: &Path, name: &str, create: bool) -> ControlResult<File> {
    #[cfg(unix)]
    use std::os::unix::fs::{MetadataExt, OpenOptionsExt, PermissionsExt};
    let mut options = OpenOptions::new();
    options
        .read(true)
        .write(true)
        .create(create)
        .truncate(false);
    #[cfg(unix)]
    {
        options.mode(0o600).custom_flags(libc::O_NOFOLLOW);
    }
    let file = options.open(path)?;
    let opened = file.metadata()?;
    if !opened.file_type().is_file() {
        return Err(ControlError::Invalid(format!(
            "{name} lock is not a regular file"
        )));
    }
    #[cfg(unix)]
    {
        if opened.nlink() != 1 {
            return Err(ControlError::Invalid(format!("{name} lock has hard links")));
        }
        file.set_permissions(fs::Permissions::from_mode(0o600))?;
        let found = fs::symlink_metadata(path)?;
        if found.file_type().is_symlink()
            || found.dev() != opened.dev()
            || found.ino() != opened.ino()
        {
            return Err(ControlError::Integrity(format!("{name} lock path changed")));
        }
    }
    FileExt::lock_exclusive(&file)?;
    Ok(file)
}

fn digest(value: &[u8]) -> String {
    hex::encode(Sha256::digest(value))
}

fn event_id(event: &NewEvent<'_>) -> String {
    digest(format!("{}\0{}", event.event_type, event.stable_key).as_bytes())
}

fn find_existing_event(
    connection: &Connection,
    event_id: &str,
) -> ControlResult<Option<ExistingEvent>> {
    connection
        .query_row(
            "SELECT event_type,device_id,token_hash,erasure_id,secret_hash,snapshot_id,restore_epoch,occurred_at,deadline_at,evidence_digest,artifact_name
             FROM control_events WHERE event_id=?1",
            [event_id],
            |row| {
                Ok((
                    row.get(0)?,
                    row.get(1)?,
                    row.get(2)?,
                    row.get(3)?,
                    row.get(4)?,
                    row.get(5)?,
                    row.get(6)?,
                    row.get(7)?,
                    row.get(8)?,
                    row.get(9)?,
                    row.get(10)?,
                ))
            },
        )
        .optional()
        .map_err(ControlError::from)
}

fn find_existing_event_for_version(
    connection: &Connection,
    event_id: &str,
    version: i64,
) -> ControlResult<Option<ExistingEvent>> {
    if version == CONTROL_SCHEMA_VERSION || version == CONTROL_SCHEMA_V2_VERSION {
        return find_existing_event(connection, event_id);
    }
    if version != LEGACY_CONTROL_SCHEMA_VERSION {
        return Err(ControlError::Invalid(
            "unsupported control event schema".to_owned(),
        ));
    }
    connection
        .query_row(
            "SELECT event_type,device_id,token_hash,erasure_id,secret_hash,snapshot_id,
                    restore_epoch,occurred_at,deadline_at,NULL,NULL
             FROM control_events WHERE event_id=?1",
            [event_id],
            |row| {
                Ok((
                    row.get(0)?,
                    row.get(1)?,
                    row.get(2)?,
                    row.get(3)?,
                    row.get(4)?,
                    row.get(5)?,
                    row.get(6)?,
                    row.get(7)?,
                    row.get(8)?,
                    row.get(9)?,
                    row.get(10)?,
                ))
            },
        )
        .optional()
        .map_err(ControlError::from)
}

fn event_matches(existing: &ExistingEvent, event: &NewEvent<'_>) -> bool {
    existing.0 == event.event_type
        && existing.1.as_deref() == event.device_id
        && existing.2.as_deref() == event.token_hash
        && existing.3.as_deref() == event.erasure_id
        && existing.4.as_deref() == event.secret_hash
        && existing.5.as_deref() == event.snapshot_id
        && existing.6.as_deref() == event.restore_epoch
        && existing.7 == event.occurred_at
        && existing.8.as_deref() == event.deadline_at
        && existing.9.as_deref() == event.evidence_digest
        && existing.10.as_deref() == event.artifact_name
}

fn load_legacy_seed_source(health: &Connection) -> ControlResult<LegacySeedSource> {
    let revocations = {
        let mut statement = health.prepare(
            "SELECT device_id,token_hash,revoked_at
             FROM devices WHERE revoked_at IS NOT NULL
             ORDER BY device_id,token_hash",
        )?;
        let rows = statement.query_map([], |row| {
            Ok(LegacyRevocation {
                device_id: row.get(0)?,
                token_hash: row.get(1)?,
                revoked_at: row.get(2)?,
            })
        })?;
        rows.collect::<rusqlite::Result<Vec<_>>>()?
    };
    let erasures = {
        let mut statement = health.prepare(
            "SELECT device_id,erasure_id,erasure_secret_hash,requested_at,
                    backup_delete_by,metrics_deleted_at,backups_expired_at
             FROM erasures ORDER BY device_id,erasure_id",
        )?;
        let rows = statement.query_map([], |row| {
            Ok(LegacyErasure {
                device_id: row.get(0)?,
                erasure_id: row.get(1)?,
                secret_hash: row.get(2)?,
                requested_at: row.get(3)?,
                deadline_at: row.get(4)?,
                metrics_deleted_at: row.get(5)?,
                backups_expired_at: row.get(6)?,
            })
        })?;
        rows.collect::<rusqlite::Result<Vec<_>>>()?
    };
    Ok(LegacySeedSource {
        format: "boaz-legacy-control-seed-v1",
        revocations,
        erasures,
    })
}

fn legacy_seed_events(source: &LegacySeedSource, fingerprint: &str) -> Vec<OwnedEvent> {
    let mut events = Vec::new();
    for revoked in &source.revocations {
        events.push(OwnedEvent {
            stable_key: revoked.token_hash.clone(),
            event_type: "credential_revoked",
            device_id: Some(revoked.device_id.clone()),
            token_hash: Some(revoked.token_hash.clone()),
            erasure_id: None,
            secret_hash: None,
            snapshot_id: None,
            restore_epoch: None,
            occurred_at: revoked.revoked_at.clone(),
            deadline_at: None,
        });
    }
    for erasure in &source.erasures {
        events.push(OwnedEvent {
            stable_key: erasure.erasure_id.clone(),
            event_type: "erasure_intent",
            device_id: Some(erasure.device_id.clone()),
            token_hash: Some(digest(format!("legacy:{}", erasure.device_id).as_bytes())),
            erasure_id: Some(erasure.erasure_id.clone()),
            secret_hash: Some(erasure.secret_hash.clone()),
            snapshot_id: None,
            restore_epoch: None,
            occurred_at: erasure.requested_at.clone(),
            deadline_at: Some(erasure.deadline_at.clone()),
        });
        events.push(OwnedEvent {
            stable_key: erasure.erasure_id.clone(),
            event_type: "health_erasure_verified",
            device_id: Some(erasure.device_id.clone()),
            token_hash: None,
            erasure_id: Some(erasure.erasure_id.clone()),
            secret_hash: None,
            snapshot_id: None,
            restore_epoch: None,
            occurred_at: erasure.requested_at.clone(),
            deadline_at: None,
        });
        if let Some(at) = &erasure.metrics_deleted_at {
            // Historical receivers already replaced device_id with its digest
            // when metrics deletion completed. This marker prevents a second
            // one-way transform during restore replay.
            events.push(OwnedEvent {
                stable_key: format!("anonymized:{}", erasure.erasure_id),
                event_type: "legacy_imported",
                device_id: Some(erasure.device_id.clone()),
                token_hash: None,
                erasure_id: Some(erasure.erasure_id.clone()),
                secret_hash: None,
                snapshot_id: None,
                restore_epoch: None,
                occurred_at: at.clone(),
                deadline_at: None,
            });
            events.push(OwnedEvent {
                stable_key: erasure.erasure_id.clone(),
                event_type: "metrics_erasure_verified",
                device_id: Some(erasure.device_id.clone()),
                token_hash: None,
                erasure_id: Some(erasure.erasure_id.clone()),
                secret_hash: None,
                snapshot_id: None,
                restore_epoch: None,
                occurred_at: at.clone(),
                deadline_at: None,
            });
        }
        if let Some(at) = &erasure.backups_expired_at {
            events.push(OwnedEvent {
                stable_key: erasure.erasure_id.clone(),
                event_type: "backups_expired_verified",
                device_id: Some(erasure.device_id.clone()),
                token_hash: None,
                erasure_id: Some(erasure.erasure_id.clone()),
                secret_hash: None,
                snapshot_id: None,
                restore_epoch: None,
                occurred_at: at.clone(),
                deadline_at: None,
            });
        }
    }
    events.push(OwnedEvent {
        stable_key: "source-fingerprint".to_owned(),
        event_type: "legacy_imported",
        device_id: None,
        token_hash: None,
        erasure_id: None,
        secret_hash: Some(fingerprint.to_owned()),
        snapshot_id: Some("boaz-legacy-control-seed-v1".to_owned()),
        restore_epoch: None,
        occurred_at: "1970-01-01T00:00:00Z".to_owned(),
        deadline_at: None,
    });
    events
}

fn validate_sha256(value: &str) -> ControlResult<()> {
    if value.len() != 64
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        return Err(ControlError::Invalid(
            "backup file SHA-256 must be 64 lowercase hexadecimal characters".to_owned(),
        ));
    }
    Ok(())
}

#[cfg(unix)]
fn backup_root_identity(root: &Path) -> ControlResult<(u64, u64)> {
    use std::os::unix::fs::MetadataExt;
    if root.canonicalize()? != root {
        return Err(ControlError::Integrity(
            "managed backup root contains a link or alias".to_owned(),
        ));
    }
    let metadata = fs::symlink_metadata(root)?;
    if !metadata.is_dir()
        || metadata.file_type().is_symlink()
        || metadata.uid() != unsafe { libc::geteuid() }
        || metadata.mode() & 0o022 != 0
    {
        return Err(ControlError::Integrity(
            "managed backup root is not a private owner-controlled directory".to_owned(),
        ));
    }
    Ok((metadata.dev(), metadata.ino()))
}

#[cfg(not(unix))]
fn backup_root_identity(_root: &Path) -> ControlResult<(u64, u64)> {
    Err(ControlError::Invalid(
        "managed backup proof requires Unix file identities".to_owned(),
    ))
}

fn require_absent_backup_entry(root: &Path, name: &str) -> ControlResult<()> {
    match fs::symlink_metadata(root.join(name)) {
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(()),
        Ok(_) => Err(ControlError::Integrity(
            "deleted backup artifact reappeared".to_owned(),
        )),
        Err(error) => Err(error.into()),
    }
}

fn require_existing_backup_entry(root: &Path, name: &str) -> ControlResult<()> {
    let metadata = fs::symlink_metadata(root.join(name))?;
    if !metadata.is_file() || metadata.file_type().is_symlink() {
        return Err(ControlError::Integrity(
            "managed backup has an orphan or linked manifest".to_owned(),
        ));
    }
    Ok(())
}

#[cfg(unix)]
fn fingerprint_backup_entry(
    root: &Path,
    name: &str,
    retain_bytes: bool,
) -> ControlResult<(BackupFileIdentity, Vec<u8>)> {
    use std::{
        ffi::CString,
        os::{
            fd::{AsRawFd, FromRawFd},
            unix::fs::{MetadataExt, OpenOptionsExt},
        },
    };
    if name.is_empty() || name.contains('/') || name.contains('\\') || name == "." || name == ".." {
        return Err(ControlError::Integrity(
            "managed backup entry is not a direct child".to_owned(),
        ));
    }
    let expected_root = backup_root_identity(root)?;
    let directory = OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_DIRECTORY | libc::O_NOFOLLOW)
        .open(root)?;
    let directory_metadata = directory.metadata()?;
    if (directory_metadata.dev(), directory_metadata.ino()) != expected_root {
        return Err(ControlError::Integrity(
            "managed backup root changed before opening an entry".to_owned(),
        ));
    }
    let name = CString::new(name)
        .map_err(|_| ControlError::Integrity("backup name contains NUL".to_owned()))?;
    let descriptor = unsafe {
        libc::openat(
            directory.as_raw_fd(),
            name.as_ptr(),
            libc::O_RDONLY | libc::O_NOFOLLOW | libc::O_CLOEXEC,
        )
    };
    if descriptor < 0 {
        return Err(io::Error::last_os_error().into());
    }
    let mut file = unsafe { File::from_raw_fd(descriptor) };
    let before = file.metadata()?;
    if !before.is_file()
        || before.nlink() != 1
        || before.mode() & 0o077 != 0
        || before.uid() != unsafe { libc::geteuid() }
    {
        return Err(ControlError::Integrity(
            "managed backup entry has unsafe type, links, owner or permissions".to_owned(),
        ));
    }
    let mut hash = Sha256::new();
    let mut bytes = Vec::new();
    let mut chunk = [0u8; 65536];
    loop {
        let count = file.read(&mut chunk)?;
        if count == 0 {
            break;
        }
        hash.update(&chunk[..count]);
        if retain_bytes {
            if bytes.len().saturating_add(count) > 65536 {
                return Err(ControlError::Integrity(
                    "managed backup manifest exceeds 64 KiB".to_owned(),
                ));
            }
            bytes.extend_from_slice(&chunk[..count]);
        }
    }
    let after = file.metadata()?;
    if before.dev() != after.dev()
        || before.ino() != after.ino()
        || before.len() != after.len()
        || before.mtime() != after.mtime()
        || before.mtime_nsec() != after.mtime_nsec()
    {
        return Err(ControlError::Integrity(
            "managed backup entry changed while hashing".to_owned(),
        ));
    }
    if backup_root_identity(root)? != expected_root {
        return Err(ControlError::Integrity(
            "managed backup root changed while hashing an entry".to_owned(),
        ));
    }
    Ok((
        BackupFileIdentity {
            device: before.dev(),
            inode: before.ino(),
            bytes: before.len(),
            sha256: format!("{:x}", hash.finalize()),
        },
        bytes,
    ))
}

#[cfg(not(unix))]
fn fingerprint_backup_entry(
    _root: &Path,
    _name: &str,
    _retain_bytes: bool,
) -> ControlResult<(BackupFileIdentity, Vec<u8>)> {
    Err(ControlError::Invalid(
        "managed backup proof requires Unix file identities".to_owned(),
    ))
}

fn validate_artifact_name(value: &str) -> ControlResult<()> {
    if !value.starts_with("boaz-health-")
        || !value.ends_with(".db")
        || value.len() > 255
        || value.bytes().any(|byte| {
            !(byte.is_ascii_alphanumeric() || byte == b'-' || byte == b'_' || byte == b'.')
        })
    {
        return Err(ControlError::Invalid(
            "backup artifact name must be a bounded managed database filename".to_owned(),
        ));
    }
    Ok(())
}

fn validate_event_semantics(
    hash_version: i64,
    event_type: &str,
    secret_hash: Option<&str>,
    evidence_digest: Option<&str>,
    artifact_name: Option<&str>,
) -> ControlResult<()> {
    if hash_version != 2 {
        return Ok(());
    }
    match event_type {
        "backup_created" => {
            validate_sha256(secret_hash.ok_or_else(|| {
                ControlError::Integrity("v2 backup creation has no file hash".to_owned())
            })?)?;
            if let Some(name) = artifact_name {
                validate_artifact_name(name)?;
            }
            if evidence_digest.is_some() {
                return Err(ControlError::Integrity(
                    "backup creation contains restore evidence".to_owned(),
                ));
            }
        }
        "restore_completed" => {
            validate_sha256(evidence_digest.ok_or_else(|| {
                ControlError::Integrity("v2 restore completion has no evidence digest".to_owned())
            })?)?;
            if artifact_name.is_some() {
                return Err(ControlError::Integrity(
                    "restore completion contains a backup artifact name".to_owned(),
                ));
            }
        }
        "backup_delete_intent" | "backup_deleted" => {
            validate_sha256(secret_hash.ok_or_else(|| {
                ControlError::Integrity("v2 backup deletion has no file hash".to_owned())
            })?)?;
            validate_artifact_name(artifact_name.ok_or_else(|| {
                ControlError::Integrity("v2 backup deletion has no artifact name".to_owned())
            })?)?;
            if evidence_digest.is_some() {
                return Err(ControlError::Integrity(
                    "backup deletion contains restore evidence".to_owned(),
                ));
            }
        }
        _ if evidence_digest.is_some() || artifact_name.is_some() => {
            return Err(ControlError::Integrity(
                "unexpected v2 event evidence fields".to_owned(),
            ));
        }
        _ => {}
    }
    Ok(())
}

fn event_hash(record: &HashRecord<'_>) -> ControlResult<String> {
    Ok(digest(&serde_json::to_vec(record)?))
}

fn event_hash_versioned(
    hash_version: i64,
    record: &HashRecord<'_>,
    evidence_digest: Option<&str>,
    artifact_name: Option<&str>,
) -> ControlResult<String> {
    match hash_version {
        1 if evidence_digest.is_none() && artifact_name.is_none() => event_hash(record),
        2 => Ok(digest(&serde_json::to_vec(&HashRecordV2 {
            hash_version,
            record,
            evidence_digest,
            artifact_name,
        })?)),
        _ => Err(ControlError::Integrity(
            "unknown hash version or v1 event has v2 fields".to_owned(),
        )),
    }
}

fn verify_control_schema(connection: &Connection, version: i64) -> ControlResult<()> {
    let reference = Connection::open_in_memory()?;
    let mut prefix_version = None;
    match version {
        LEGACY_CONTROL_SCHEMA_VERSION => reference.execute_batch(CONTROL_SCHEMA_V1)?,
        CONTROL_SCHEMA_V2_VERSION => reference.execute_batch(CONTROL_SCHEMA_V2)?,
        CONTROL_SCHEMA_VERSION => {
            let candidate: i64 = connection.query_row(
                "SELECT prefix_version FROM control_layout WHERE singleton=1",
                [],
                |row| row.get(0),
            )?;
            let count: i64 =
                connection
                    .query_row("SELECT COUNT(*) FROM control_layout", [], |row| row.get(0))?;
            if count != 1
                || ![LEGACY_CONTROL_SCHEMA_VERSION, CONTROL_SCHEMA_V2_VERSION].contains(&candidate)
            {
                return Err(ControlError::Invalid(
                    "control prefix layout is unknown".to_owned(),
                ));
            }
            reference.execute_batch(if candidate == LEGACY_CONTROL_SCHEMA_VERSION {
                CONTROL_SCHEMA_V1
            } else {
                CONTROL_SCHEMA_V2
            })?;
            create_v3_objects(&reference, candidate)?;
            prefix_version = Some(candidate);
        }
        _ => return Err(ControlError::Invalid("unknown control schema".to_owned())),
    }
    if collect_schema_sql(connection)? != collect_schema_sql(&reference)? {
        return Err(ControlError::Invalid(
            "control schema differs from the reviewed layout".to_owned(),
        ));
    }
    if let Some(prefix_version) = prefix_version {
        let prefix_columns = collect_columns(connection, "control_events_prefix")?;
        let reference_columns = collect_columns(&reference, "control_events_prefix")?;
        if prefix_columns != reference_columns
            || collect_columns(connection, "control_events")?
                != collect_columns(&reference, "control_events")?
        {
            return Err(ControlError::Invalid(format!(
                "control v{prefix_version} prefix columns differ from reviewed layout"
            )));
        }
        return Ok(());
    }
    let tables = collect_object_names(connection, "table")?;
    let indexes = collect_object_names(connection, "index")?;
    if tables != ["control_events", "control_meta"]
        || indexes
            != [
                "control_events_device",
                "control_events_erasure",
                "control_events_token_type",
                "control_events_type_sequence",
            ]
    {
        return Err(ControlError::Invalid(
            "control object inventory does not match schema".to_owned(),
        ));
    }
    let columns = collect_columns(connection, "control_events")?;
    let mut expected_columns = vec![
        "sequence",
        "event_id",
        "event_type",
        "device_id",
        "token_hash",
        "erasure_id",
        "secret_hash",
        "snapshot_id",
        "restore_epoch",
        "occurred_at",
        "deadline_at",
        "previous_hash",
        "current_hash",
    ];
    if version == CONTROL_SCHEMA_V2_VERSION {
        expected_columns.extend(["hash_version", "evidence_digest", "artifact_name"]);
    }
    if columns != expected_columns {
        return Err(ControlError::Invalid(
            "control event columns do not match schema version".to_owned(),
        ));
    }
    let event_sql: String = connection.query_row(
        "SELECT lower(sql) FROM sqlite_schema WHERE type='table' AND name='control_events'",
        [],
        |row| row.get(0),
    )?;
    let compact: String = event_sql
        .chars()
        .filter(|character| !character.is_whitespace())
        .collect();
    if !compact.contains("check(event_typein(")
        || (version == CONTROL_SCHEMA_V2_VERSION
            && (!compact.contains("'backup_delete_intent'")
                || !compact.contains("check(hash_versionin(1,2))")))
    {
        return Err(ControlError::Invalid(
            "control event type constraint is missing".to_owned(),
        ));
    }
    Ok(())
}

fn collect_schema_sql(connection: &Connection) -> ControlResult<BTreeMap<String, String>> {
    let mut statement = connection.prepare(
        "SELECT name,sql FROM sqlite_schema WHERE type IN ('table','index','view','trigger')
         AND name NOT LIKE 'sqlite_%' ORDER BY name",
    )?;
    let rows = statement.query_map([], |row| {
        let name: String = row.get(0)?;
        let sql: String = row.get(1)?;
        Ok((name, sql.chars().filter(|ch| !ch.is_whitespace()).collect()))
    })?;
    Ok(rows.collect::<rusqlite::Result<BTreeMap<_, _>>>()?)
}

fn collect_object_names(connection: &Connection, kind: &str) -> ControlResult<Vec<String>> {
    let mut statement = connection.prepare(
        "SELECT name FROM sqlite_schema WHERE type=?1 AND name NOT LIKE 'sqlite_%' ORDER BY name",
    )?;
    let rows = statement.query_map([kind], |row| row.get::<_, String>(0))?;
    Ok(rows.collect::<rusqlite::Result<Vec<_>>>()?)
}

fn collect_columns(connection: &Connection, table: &str) -> ControlResult<Vec<String>> {
    let mut statement = connection.prepare(&format!("PRAGMA table_info('{table}')"))?;
    let rows = statement.query_map([], |row| row.get::<_, String>(1))?;
    Ok(rows.collect::<rusqlite::Result<Vec<_>>>()?)
}

fn reject_link_or_non_file(path: &Path) -> ControlResult<()> {
    let metadata = fs::symlink_metadata(path)?;
    if metadata.file_type().is_symlink() || !metadata.file_type().is_file() {
        return Err(ControlError::Invalid(
            "control authority must be a regular non-symlink file".to_owned(),
        ));
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        if metadata.nlink() != 1 {
            return Err(ControlError::Invalid(
                "control authority may not have hard links".to_owned(),
            ));
        }
    }
    Ok(())
}

fn reject_link_or_non_directory(path: &Path) -> ControlResult<()> {
    let metadata = fs::symlink_metadata(path)?;
    if metadata.file_type().is_symlink() || !metadata.file_type().is_dir() {
        return Err(ControlError::Invalid(
            "control authority directory must not be a symlink".to_owned(),
        ));
    }
    Ok(())
}

fn validate_control_authority_paths(store: &ControlStore) -> ControlResult<()> {
    let parent = store.db_path.parent().ok_or_else(|| {
        ControlError::Invalid("control database has no parent directory".to_owned())
    })?;
    reject_link_or_non_directory(parent)?;
    let mirror_parent = store.mirror_dir.parent().ok_or_else(|| {
        ControlError::Invalid("control mirror has no parent directory".to_owned())
    })?;
    reject_link_or_non_directory(mirror_parent)?;
    reject_link_or_non_directory(&store.mirror_dir)?;
    reject_link_or_non_file(&store.db_path)?;
    reject_link_or_non_file(&store.head_path)?;
    Ok(())
}

fn validate_preflight_sidecars(db_path: &Path) -> ControlResult<()> {
    let filename = db_path
        .file_name()
        .and_then(|name| name.to_str())
        .ok_or_else(|| ControlError::Invalid("control database filename is invalid".to_owned()))?;
    for suffix in ["-wal", "-shm", "-journal"] {
        let sidecar = db_path.with_file_name(format!("{filename}{suffix}"));
        match fs::symlink_metadata(&sidecar) {
            Ok(_) => {
                return Err(ControlError::Invalid(
                    "control migration preflight requires explicit offline WAL/journal recovery"
                        .to_owned(),
                ));
            }
            Err(error) if error.kind() == io::ErrorKind::NotFound => {}
            Err(error) => return Err(error.into()),
        }
    }
    Ok(())
}

fn set_private_file(path: &Path) -> ControlResult<()> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(path, fs::Permissions::from_mode(0o600))?;
    }
    Ok(())
}

fn mirror_path(directory: &Path, sequence: i64, hash: &str) -> PathBuf {
    directory.join(format!("{sequence:020}-{hash}.json"))
}

fn write_mirror_create_new(
    directory: &Path,
    sequence: i64,
    hash: &str,
    bytes: &[u8],
) -> ControlResult<()> {
    let path = mirror_path(directory, sequence, hash);
    match OpenOptions::new().write(true).create_new(true).open(&path) {
        Ok(mut file) => {
            file.write_all(bytes)?;
            file.sync_all()?;
            set_private_file(&path)?;
            let mirror_directory = OpenOptions::new().read(true).open(directory)?;
            mirror_directory.sync_all()?;
        }
        Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {
            if fs::read(&path)? != bytes {
                return Err(ControlError::Integrity(
                    "existing mirror record has different content".to_owned(),
                ));
            }
        }
        Err(error) => return Err(error.into()),
    }
    Ok(())
}

fn verify_mirror(
    directory: &Path,
    store_id: &str,
    sequence: i64,
    hash: &str,
    event_id: &str,
) -> ControlResult<()> {
    let path = mirror_path(directory, sequence, hash);
    reject_link_or_non_file(&path)?;
    let value: serde_json::Value = serde_json::from_slice(&fs::read(path)?)?;
    if value.get("store_id").and_then(serde_json::Value::as_str) != Some(store_id)
        || value.get("sequence").and_then(serde_json::Value::as_i64) != Some(sequence)
        || value.get("event_id").and_then(serde_json::Value::as_str) != Some(event_id)
        || value
            .get("current_hash")
            .and_then(serde_json::Value::as_str)
            != Some(hash)
    {
        return Err(ControlError::Integrity(
            "mirror record does not match the database event".to_owned(),
        ));
    }
    Ok(())
}

fn write_head_atomic(path: &Path, head: &ControlHead) -> ControlResult<()> {
    let parent = path
        .parent()
        .ok_or_else(|| ControlError::Invalid("head has no parent".to_owned()))?;
    let temp = parent.join(format!(".control-head-{}.tmp", uuid::Uuid::new_v4()));
    let result = (|| -> ControlResult<()> {
        let mut file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&temp)?;
        file.write_all(&serde_json::to_vec(head)?)?;
        file.sync_all()?;
        set_private_file(&temp)?;
        fs::rename(&temp, path)?;
        let directory = OpenOptions::new().read(true).open(parent)?;
        directory.sync_all()?;
        Ok(())
    })();
    if result.is_err() {
        let _ = fs::remove_file(temp);
    }
    result
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::custody::{CustodyError, CustodyResult};
    use crate::database::{initialize_health_database, open_health_database};
    use std::sync::{Arc, Barrier, Mutex};
    use tempfile::TempDir;

    fn store() -> (TempDir, ControlStore) {
        let directory = TempDir::new().unwrap();
        let root = directory.path().join("control");
        let mirror = root.join("mirror");
        fs::create_dir_all(&mirror).unwrap();
        let store = ControlStore::initialize(root.join("control.db"), mirror).unwrap();
        (directory, store)
    }

    fn checkpoint_control_for_preflight(store: &ControlStore) {
        let connection = store.open_read_write().unwrap();
        let (busy, _, _): (i64, i64, i64) = connection
            .query_row("PRAGMA wal_checkpoint(TRUNCATE)", [], |row| {
                Ok((row.get(0)?, row.get(1)?, row.get(2)?))
            })
            .unwrap();
        assert_eq!(busy, 0);
        drop(connection);
    }

    struct SyntheticCustody {
        inner: Mutex<SyntheticCustodyState>,
    }

    struct SyntheticCustodyState {
        current: CustodyState,
        pending: Option<CustodyReservationV2>,
        lose_reserve_reply: bool,
        reject_cas_once: bool,
        lose_cas_reply: bool,
        false_cas_success_once: bool,
    }

    impl SyntheticCustody {
        fn new(checkpoint: ControlCheckpoint) -> Self {
            Self {
                inner: Mutex::new(SyntheticCustodyState {
                    current: CustodyState {
                        format: 2,
                        revision: 0,
                        control: checkpoint,
                        ack: None,
                        baseline_sha256: None,
                    },
                    pending: None,
                    lose_reserve_reply: false,
                    reject_cas_once: false,
                    lose_cas_reply: false,
                    false_cas_success_once: false,
                }),
            }
        }
    }

    impl CustodyClient for SyntheticCustody {
        fn read_v2(&self, store_id: &str) -> CustodyResult<CustodyState> {
            let inner = self.inner.lock().unwrap();
            if inner.current.control.store_id != store_id || inner.pending.is_some() {
                return Err(CustodyError::Protocol(
                    "synthetic custody is pending or foreign".to_owned(),
                ));
            }
            Ok(inner.current.clone())
        }

        fn reserve_v2(
            &self,
            predecessor: &CustodyState,
            operation_id: &str,
            intent_sha256: &str,
        ) -> CustodyResult<CustodyReservationV2> {
            let mut inner = self.inner.lock().unwrap();
            if inner.current != *predecessor {
                return Err(CustodyError::Protocol(
                    "synthetic predecessor mismatch".to_owned(),
                ));
            }
            let reservation = if let Some(existing) = &inner.pending {
                if existing.predecessor != *predecessor
                    || existing.operation_id != operation_id
                    || existing.intent_sha256 != intent_sha256
                {
                    return Err(CustodyError::Protocol(
                        "synthetic reservation conflict".to_owned(),
                    ));
                }
                existing.clone()
            } else {
                let created = CustodyReservationV2 {
                    reservation_id: uuid::Uuid::new_v4().to_string(),
                    predecessor: predecessor.clone(),
                    operation_id: operation_id.to_owned(),
                    intent_sha256: intent_sha256.to_owned(),
                };
                inner.pending = Some(created.clone());
                created
            };
            if inner.lose_reserve_reply {
                inner.lose_reserve_reply = false;
                return Err(CustodyError::Protocol(
                    "synthetic lost reserve reply".to_owned(),
                ));
            }
            Ok(reservation)
        }

        fn compare_and_swap_v2(
            &self,
            reservation: &CustodyReservationV2,
            successor: &CustodyState,
        ) -> CustodyResult<CustodyState> {
            let mut inner = self.inner.lock().unwrap();
            if inner.pending.as_ref() != Some(reservation)
                || inner.current != reservation.predecessor
            {
                return Err(CustodyError::Protocol(
                    "synthetic CAS reservation mismatch".to_owned(),
                ));
            }
            if inner.reject_cas_once {
                inner.reject_cas_once = false;
                return Err(CustodyError::Protocol(
                    "synthetic CAS interrupted".to_owned(),
                ));
            }
            if inner.false_cas_success_once {
                inner.false_cas_success_once = false;
                // An untrusted transport can claim the requested state while
                // the independently held state remains at its predecessor.
                return Ok(successor.clone());
            }
            inner.current = successor.clone();
            inner.pending = None;
            if inner.lose_cas_reply {
                inner.lose_cas_reply = false;
                return Err(CustodyError::Protocol(
                    "synthetic lost CAS reply".to_owned(),
                ));
            }
            Ok(successor.clone())
        }
    }

    fn custodied_store() -> (TempDir, ControlStore, Arc<SyntheticCustody>) {
        let (directory, plain) = store();
        let custody = Arc::new(SyntheticCustody::new(plain.checkpoint().unwrap()));
        let lock_path = directory.path().join(".custody-operation.lock");
        OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&lock_path)
            .unwrap();
        let store = plain.with_custody(custody.clone(), lock_path).unwrap();
        (directory, store, custody)
    }

    #[test]
    fn custodied_control_append_advances_only_after_matching_remote_cas() {
        let (_directory, store, custody) = custodied_store();
        store
            .append_credential_revoked("phone-a", "token-a", "2026-09-19T00:00:00Z")
            .unwrap();
        let local = store.checkpoint().unwrap();
        assert_eq!(custody.read_v2(&local.store_id).unwrap().control, local);
        assert!(!store.pending_intent_path.exists());
        store
            .append_credential_revoked("phone-a", "token-a", "2026-09-19T00:00:00Z")
            .unwrap();
        assert_eq!(store.checkpoint().unwrap().sequence, 1);
    }

    #[test]
    fn matching_control_head_does_not_authenticate_other_custody_authorities() {
        let (_directory, store, custody) = custodied_store();
        let original = custody.inner.lock().unwrap().current.clone();
        let mismatches = [
            CustodyState {
                revision: 1,
                ..original.clone()
            },
            CustodyState {
                baseline_sha256: Some("a".repeat(64)),
                ..original.clone()
            },
            CustodyState {
                ack: Some(crate::custody::AckCheckpoint {
                    journal_id: uuid::Uuid::new_v4().to_string(),
                    sequence: 0,
                    confirmation_sequence: 0,
                    pairing_confirmation_sequence: 0,
                    head_sha256: "b".repeat(64),
                }),
                baseline_sha256: Some("a".repeat(64)),
                revision: 1,
                ..original.clone()
            },
        ];
        for remote in mismatches {
            custody.inner.lock().unwrap().current = remote;
            assert!(
                store.verify_custody().is_err(),
                "matching control head alone must not authenticate a custody state"
            );
        }
    }

    #[test]
    fn adopted_control_publication_requires_the_complete_local_custody_tuple() {
        let (directory, store, custody, _db, snapshot_id, hash) = synthetic_managed_backup();
        let journal_root = directory.path().join("ack-journal");
        fs::create_dir(&journal_root).unwrap();
        AckJournal::initialize(&journal_root).unwrap();
        let journal = Arc::new(AckJournal::open(&journal_root).unwrap());
        let store = store.with_ack_journal(Arc::clone(&journal)).unwrap();
        let prior = store.checkpoint().unwrap();
        store
            .append_backup_created_with_artifact(
                &snapshot_id,
                &hash,
                "boaz-health-synthetic.db",
                "2026-09-19T00:00:00Z",
                &prior,
            )
            .unwrap();
        let backup_head = store.checkpoint().unwrap();
        journal
            .bind_baseline(&crate::ack_journal::Baseline {
                snapshot_id,
                snapshot_sha256: hash,
                receipt_inventory_sha256: "a".repeat(64),
                control_store_id: backup_head.store_id.clone(),
                control_head_sequence: backup_head.sequence,
                control_head_hash: backup_head.current_hash.clone(),
            })
            .unwrap();
        let mut sealed = custody.inner.lock().unwrap().current.clone();
        sealed.revision += 1;
        sealed.baseline_sha256 = journal.baseline_sha256().unwrap();
        sealed.ack = Some(journal.checkpoint().unwrap());
        custody.inner.lock().unwrap().current = sealed.clone();
        store.verify_custody().unwrap();

        let mismatches = [
            CustodyState {
                revision: sealed.revision + 1,
                ..sealed.clone()
            },
            CustodyState {
                baseline_sha256: Some("b".repeat(64)),
                ..sealed.clone()
            },
            CustodyState {
                ack: Some(crate::custody::AckCheckpoint {
                    head_sha256: "c".repeat(64),
                    ..sealed.ack.clone().unwrap()
                }),
                ..sealed.clone()
            },
        ];
        for remote in mismatches {
            custody.inner.lock().unwrap().current = remote;
            assert!(store.verify_custody().is_err());
            assert!(
                store
                    .append_credential_revoked("phone-a", "token-a", "2026-09-19T00:01:00Z")
                    .is_err()
            );
            assert_eq!(store.checkpoint().unwrap(), backup_head);
            assert!(!store.pending_intent_path.exists());
        }
    }

    #[test]
    fn custodied_backup_cannot_publish_without_replayable_artifact_evidence() {
        let (_directory, store, custody) = custodied_store();
        let before = store.checkpoint().unwrap();
        let result = store.append_backup_created(
            "snapshot-without-file",
            &"a".repeat(64),
            "2026-09-19T00:00:00Z",
            &before,
        );
        assert!(
            result.is_err(),
            "a hash supplied by the caller is not a backup"
        );
        assert_eq!(store.checkpoint().unwrap(), before);
        assert_eq!(custody.read_v2(&before.store_id).unwrap().control, before);
    }

    fn synthetic_managed_backup() -> (
        TempDir,
        ControlStore,
        Arc<SyntheticCustody>,
        PathBuf,
        String,
        String,
    ) {
        let (directory, store, custody) = custodied_store();
        let root = directory.path().join("backups");
        fs::create_dir(&root).unwrap();
        let root = root.canonicalize().unwrap();
        let store = store.with_managed_backup_root(root.clone()).unwrap();
        let name = "boaz-health-synthetic.db";
        let snapshot_id = "synthetic-snapshot".to_owned();
        let bytes = b"synthetic encrypted-volume fixture, not health data";
        let file_hash = digest(bytes);
        let db = root.join(name);
        fs::write(&db, bytes).unwrap();
        set_private_file(&db).unwrap();
        let manifest = root.join(format!("{name}.meta.json"));
        fs::write(
            &manifest,
            serde_json::to_vec(&serde_json::json!({
                "snapshot_id": snapshot_id,
                "file_sha256": file_hash,
                "control_checkpoint": store.checkpoint().unwrap(),
                "source_schema_version": 2,
                "source_commit_sequence": null,
                "snapshot_started_at": "2026-09-19T00:00:00Z"
            }))
            .unwrap(),
        )
        .unwrap();
        set_private_file(&manifest).unwrap();
        (directory, store, custody, db, snapshot_id, file_hash)
    }

    #[test]
    fn reserved_backup_replays_only_when_file_and_manifest_still_match() {
        let (_directory, store, custody, db, snapshot_id, hash) = synthetic_managed_backup();
        let prior = store.checkpoint().unwrap();
        custody.inner.lock().unwrap().lose_reserve_reply = true;
        assert!(
            store
                .append_backup_created_with_artifact(
                    &snapshot_id,
                    &hash,
                    "boaz-health-synthetic.db",
                    "2026-09-19T00:00:00Z",
                    &prior,
                )
                .is_err()
        );
        assert_eq!(store.checkpoint().unwrap(), prior);
        store.resume_pending_custody().unwrap();
        assert_eq!(store.checkpoint().unwrap().sequence, 1);
        assert!(!store.pending_intent_path.exists());

        let (_directory, store, custody, db2, snapshot_id, hash) = synthetic_managed_backup();
        let prior = store.checkpoint().unwrap();
        custody.inner.lock().unwrap().lose_reserve_reply = true;
        assert!(
            store
                .append_backup_created_with_artifact(
                    &snapshot_id,
                    &hash,
                    "boaz-health-synthetic.db",
                    "2026-09-19T00:00:00Z",
                    &prior,
                )
                .is_err()
        );
        fs::remove_file(&db2).unwrap();
        assert!(store.resume_pending_custody().is_err());
        assert_eq!(store.checkpoint().unwrap(), prior);
        assert!(store.pending_intent_path.exists());
        assert!(custody.inner.lock().unwrap().pending.is_some());
        assert!(db.exists());
    }

    #[test]
    fn committed_backup_cannot_be_custodied_after_manifest_changes() {
        let (_directory, store, custody, db, snapshot_id, hash) = synthetic_managed_backup();
        let prior = store.checkpoint().unwrap();
        custody.inner.lock().unwrap().reject_cas_once = true;
        assert!(
            store
                .append_backup_created_with_artifact(
                    &snapshot_id,
                    &hash,
                    "boaz-health-synthetic.db",
                    "2026-09-19T00:00:00Z",
                    &prior,
                )
                .is_err()
        );
        assert_eq!(store.checkpoint().unwrap().sequence, 1);
        let manifest = db.with_extension("db.meta.json");
        fs::write(&manifest, b"{\"tampered\":true}").unwrap();
        assert!(store.resume_pending_custody().is_err());
        assert!(store.pending_intent_path.exists());
        assert!(custody.inner.lock().unwrap().pending.is_some());
    }

    #[test]
    fn reserved_backup_rejects_missing_replaced_or_changed_artifacts() {
        for case in [
            "missing-manifest",
            "changed-bytes",
            "replaced-same-bytes",
            "hard-linked-database",
            "linked-manifest",
        ] {
            let (_directory, store, custody, db, snapshot_id, hash) = synthetic_managed_backup();
            let before = store.checkpoint().unwrap();
            custody.inner.lock().unwrap().lose_reserve_reply = true;
            assert!(
                store
                    .append_backup_created_with_artifact(
                        &snapshot_id,
                        &hash,
                        "boaz-health-synthetic.db",
                        "2026-09-19T00:00:00Z",
                        &before,
                    )
                    .is_err()
            );
            match case {
                "missing-manifest" => fs::remove_file(db.with_extension("db.meta.json")).unwrap(),
                "changed-bytes" => fs::write(&db, b"tampered fixture").unwrap(),
                "replaced-same-bytes" => {
                    let replacement = db.with_extension("replacement");
                    fs::write(&replacement, fs::read(&db).unwrap()).unwrap();
                    set_private_file(&replacement).unwrap();
                    fs::rename(&replacement, &db).unwrap();
                }
                "hard-linked-database" => {
                    fs::hard_link(&db, db.with_extension("hardlink")).unwrap();
                }
                "linked-manifest" => {
                    let manifest = db.with_extension("db.meta.json");
                    let original = db.with_extension("manifest-original");
                    fs::rename(&manifest, &original).unwrap();
                    std::os::unix::fs::symlink(&original, &manifest).unwrap();
                }
                _ => unreachable!(),
            }
            assert!(store.resume_pending_custody().is_err(), "{case}");
            assert_eq!(store.checkpoint().unwrap(), before, "{case}");
            assert!(custody.inner.lock().unwrap().pending.is_some(), "{case}");
        }
    }

    #[test]
    fn committed_backup_deletion_refuses_reappeared_artifact_before_cas() {
        let (_directory, store, custody, db, snapshot_id, hash) = synthetic_managed_backup();
        store
            .append_backup_created_with_artifact(
                &snapshot_id,
                &hash,
                "boaz-health-synthetic.db",
                "2026-09-19T00:00:00Z",
                &store.checkpoint().unwrap(),
            )
            .unwrap();
        store
            .append_backup_delete_intent(
                &snapshot_id,
                &hash,
                "boaz-health-synthetic.db",
                "2026-09-19T00:01:00Z",
            )
            .unwrap();
        fs::remove_file(&db).unwrap();
        fs::remove_file(db.with_extension("db.meta.json")).unwrap();
        custody.inner.lock().unwrap().reject_cas_once = true;
        assert!(
            store
                .append_backup_deleted_with_hash(
                    &snapshot_id,
                    &hash,
                    "boaz-health-synthetic.db",
                    "2026-09-19T00:02:00Z",
                )
                .is_err()
        );
        assert_eq!(store.checkpoint().unwrap().sequence, 3);
        fs::write(&db, b"reappeared backup").unwrap();
        set_private_file(&db).unwrap();
        assert!(store.resume_pending_custody().is_err());
        assert!(store.pending_intent_path.exists());
        assert!(custody.inner.lock().unwrap().pending.is_some());
    }

    #[test]
    fn inventory_dependent_completion_does_not_auto_resume_without_new_evidence() {
        let (_directory, store, _custody) = custodied_store();
        let prior = store.checkpoint().unwrap();
        assert!(
            store
                .append_backups_expired_verified("phone-a", "erase-a", "2026-09-19T00:00:00Z")
                .is_err()
        );
        assert_eq!(store.checkpoint().unwrap(), prior);
        assert!(!store.pending_intent_path.exists());
    }

    #[test]
    fn backup_expiry_reservation_rechecks_the_complete_physical_inventory() {
        let (directory, store, custody) = custodied_store();
        let root = directory.path().join("backups");
        fs::create_dir(&root).unwrap();
        let store = store
            .with_managed_backup_root(root.canonicalize().unwrap())
            .unwrap();
        store
            .append_erasure_intent(
                "phone-a",
                "token-a",
                "erase-a",
                "secret-a",
                "2026-09-19T00:00:00Z",
                "2026-10-19T00:00:00Z",
            )
            .unwrap();
        let prior = store.checkpoint().unwrap();
        custody.inner.lock().unwrap().lose_reserve_reply = true;
        assert!(
            store
                .append_backups_expired_verified("phone-a", "erase-a", "2026-09-19T00:01:00Z")
                .is_err()
        );
        let rogue = root.join("boaz-health-rogue.db");
        fs::write(&rogue, b"unexpected old backup").unwrap();
        set_private_file(&rogue).unwrap();
        assert!(store.resume_pending_custody().is_err());
        assert_eq!(store.checkpoint().unwrap(), prior);
        assert!(custody.inner.lock().unwrap().pending.is_some());
        fs::remove_file(&rogue).unwrap();
        store.resume_pending_custody().unwrap();
        assert_eq!(store.checkpoint().unwrap().sequence, prior.sequence + 1);
    }

    #[test]
    fn custodied_backup_rejects_manifest_with_missing_schema_before_reserve() {
        let (_directory, store, custody, db, snapshot_id, hash) = synthetic_managed_backup();
        let manifest = db.with_extension("db.meta.json");
        let mut value: serde_json::Value =
            serde_json::from_slice(&fs::read(&manifest).unwrap()).unwrap();
        value
            .as_object_mut()
            .unwrap()
            .remove("source_schema_version");
        fs::write(&manifest, serde_json::to_vec(&value).unwrap()).unwrap();
        let prior = store.checkpoint().unwrap();
        assert!(
            store
                .append_backup_created_with_artifact(
                    &snapshot_id,
                    &hash,
                    "boaz-health-synthetic.db",
                    "2026-09-19T00:00:00Z",
                    &prior,
                )
                .is_err()
        );
        assert_eq!(store.checkpoint().unwrap(), prior);
        assert!(custody.inner.lock().unwrap().pending.is_none());
    }

    #[test]
    fn reserved_control_intent_retries_same_event_after_lost_reserve_reply() {
        let (_directory, store, custody) = custodied_store();
        custody.inner.lock().unwrap().lose_reserve_reply = true;
        assert!(
            store
                .append_erasure_intent(
                    "phone-a",
                    "token-a",
                    "erase-a",
                    "secret-a",
                    "2026-09-19T00:00:00Z",
                    "2026-10-19T00:00:00Z",
                )
                .is_err()
        );
        assert_eq!(store.checkpoint().unwrap().sequence, 0);
        assert!(store.pending_intent_path.exists());
        store.resume_pending_custody().unwrap();
        assert_eq!(store.checkpoint().unwrap().sequence, 1);
        assert!(!store.pending_intent_path.exists());
    }

    #[test]
    fn lost_control_cas_reply_is_confirmed_only_by_exact_remote_readback() {
        let (_directory, store, custody) = custodied_store();
        custody.inner.lock().unwrap().lose_cas_reply = true;
        store
            .append_credential_revoked("phone-a", "token-a", "2026-09-19T00:00:00Z")
            .unwrap();
        assert_eq!(store.checkpoint().unwrap().sequence, 1);
        assert!(!store.pending_intent_path.exists());
    }

    #[test]
    fn false_control_cas_success_keeps_the_intent_until_independent_readback() {
        let (_directory, store, custody) = custodied_store();
        custody.inner.lock().unwrap().false_cas_success_once = true;
        assert!(
            store
                .append_credential_revoked("phone-a", "token-a", "2026-09-19T00:00:00Z")
                .is_err()
        );
        assert_eq!(store.checkpoint().unwrap().sequence, 1);
        assert!(store.pending_intent_path.exists());
        assert!(store.verify_custody().is_err());
        store.resume_pending_custody().unwrap();
        assert!(!store.pending_intent_path.exists());
        store.verify_custody().unwrap();
    }

    #[test]
    fn committed_control_tail_repairs_only_under_its_original_reservation() {
        let (_directory, store, custody) = custodied_store();
        custody.inner.lock().unwrap().reject_cas_once = true;
        assert!(
            store
                .append_credential_revoked("phone-a", "token-a", "2026-09-19T00:00:00Z",)
                .is_err()
        );
        let tip = store.checkpoint().unwrap();
        assert_eq!(tip.sequence, 1);
        assert!(store.pending_intent_path.exists());
        assert!(store.verify_custody().is_err());
        let mirror = mirror_path(&store.mirror_dir, tip.sequence, &tip.current_hash);
        fs::remove_file(&mirror).unwrap();
        write_head_atomic(
            &store.head_path,
            &ControlHead {
                store_id: tip.store_id.clone(),
                sequence: 0,
                current_hash: GENESIS_HASH.to_owned(),
            },
        )
        .unwrap();
        store.resume_pending_custody().unwrap();
        assert!(mirror.exists());
        assert!(!store.pending_intent_path.exists());
        store.verify_custody().unwrap();
    }

    #[test]
    fn missing_custody_lock_does_not_create_it_in_request_path() {
        let (_directory, store, _custody) = custodied_store();
        let lock = store.custody_lock_path.as_ref().unwrap();
        fs::remove_file(lock).unwrap();
        assert!(
            store
                .append_credential_revoked("phone-a", "token-a", "2026-09-19T00:00:00Z")
                .is_err()
        );
        assert!(!lock.exists());
        assert_eq!(store.checkpoint().unwrap().sequence, 0);
    }

    #[test]
    fn tombstones_are_idempotent_and_hash_chain_is_verified() {
        let (_directory, store) = store();
        store
            .append_credential_revoked("phone-1", "token-hash", "2026-09-19T00:00:00Z")
            .unwrap();
        store
            .append_credential_revoked("phone-1", "token-hash", "2026-09-19T00:00:00Z")
            .unwrap();
        assert!(store.token_tombstoned("token-hash").unwrap());
        store.verify().unwrap();
        assert!(matches!(
            store.append_credential_revoked("phone-1", "token-hash", "2026-09-19T00:00:01Z"),
            Err(ControlError::Integrity(_))
        ));
    }

    #[test]
    fn concurrent_appends_publish_one_contiguous_chain() {
        const WRITERS: usize = 12;
        let (_directory, store) = store();
        let barrier = Arc::new(Barrier::new(WRITERS));
        let mut writers = Vec::new();
        for index in 0..WRITERS {
            let store = store.clone();
            let barrier = Arc::clone(&barrier);
            writers.push(std::thread::spawn(move || {
                let device_id = format!("phone-{index}");
                let token_hash = format!("token-{index}");
                barrier.wait();
                store.append_credential_revoked(&device_id, &token_hash, "2026-09-19T00:00:00Z")
            }));
        }
        for writer in writers {
            writer.join().unwrap().unwrap();
        }
        store.verify().unwrap();
        assert_eq!(store.checkpoint().unwrap().sequence, WRITERS as i64);
        assert_eq!(fs::read_dir(store.mirror_dir()).unwrap().count(), WRITERS);
        for index in 0..WRITERS {
            assert!(store.token_tombstoned(&format!("token-{index}")).unwrap());
        }
    }

    #[test]
    fn erasure_is_pending_until_both_health_and_metrics_are_verified() {
        let (_directory, store) = store();
        store
            .append_erasure_intent(
                "phone-1",
                "token-hash",
                "erase-1",
                "secret-hash",
                "2026-09-19T00:00:00Z",
                "2026-10-19T00:00:00Z",
            )
            .unwrap();
        assert!(store.pending_metric_erasures().unwrap().is_empty());
        store
            .append_health_erasure_verified("phone-1", "erase-1", "2026-09-19T00:01:00Z")
            .unwrap();
        assert_eq!(
            store.pending_metric_erasures().unwrap(),
            vec![PendingErasure {
                device_id: "phone-1".to_owned(),
                erasure_id: "erase-1".to_owned()
            }]
        );
        store
            .append_metrics_erasure_verified("phone-1", "erase-1", "2026-09-19T00:02:00Z")
            .unwrap();
        assert!(store.pending_metric_erasures().unwrap().is_empty());
    }

    #[test]
    fn head_tampering_fails_closed() {
        let (_directory, store) = store();
        store
            .append_credential_revoked("phone-1", "token-hash", "2026-09-19T00:00:00Z")
            .unwrap();
        fs::write(
            &store.head_path,
            br#"{"store_id":"wrong","sequence":1,"current_hash":"wrong"}"#,
        )
        .unwrap();
        assert!(matches!(store.verify(), Err(ControlError::Integrity(_))));
    }

    #[test]
    fn hot_path_tail_check_rejects_missing_latest_mirror() {
        let (_directory, store) = store();
        store
            .append_credential_revoked("phone-1", "token-hash", "2026-09-19T00:00:00Z")
            .unwrap();
        let checkpoint = store.checkpoint().unwrap();
        fs::remove_file(mirror_path(
            &store.mirror_dir,
            checkpoint.sequence,
            &checkpoint.current_hash,
        ))
        .unwrap();
        assert!(store.token_tombstoned("token-hash").is_err());
        assert!(store.device_erasure_tombstoned("phone-1").is_err());
    }

    #[test]
    fn extra_or_foreign_mirror_record_fails_closed() {
        let (_directory, store) = store();
        store
            .append_credential_revoked("phone-1", "token-hash", "2026-09-19T00:00:00Z")
            .unwrap();

        let extra = store
            .mirror_dir
            .join(format!("{:020}-{}.json", 2, "0".repeat(64)));
        fs::write(&extra, b"{}").unwrap();
        assert!(matches!(store.verify(), Err(ControlError::Integrity(_))));
        fs::remove_file(extra).unwrap();

        let mirror = fs::read_dir(&store.mirror_dir)
            .unwrap()
            .next()
            .unwrap()
            .unwrap()
            .path();
        let mut value: serde_json::Value =
            serde_json::from_slice(&fs::read(&mirror).unwrap()).unwrap();
        value["store_id"] = serde_json::Value::String("foreign-store".to_owned());
        fs::write(&mirror, serde_json::to_vec(&value).unwrap()).unwrap();
        assert!(matches!(store.verify(), Err(ControlError::Integrity(_))));
    }

    #[test]
    fn reconciliation_replays_erasure_and_verified_control_state() {
        let (directory, store) = store();
        let health_path = directory.path().join("health.db");
        initialize_health_database(&health_path, &store.store_id().unwrap()).unwrap();
        let mut health = open_health_database(&health_path).unwrap();
        health
            .execute(
                "INSERT INTO devices(device_id,token_hash,created_at) VALUES ('phone-1','token-hash','2026-09-19T00:00:00Z')",
                [],
            )
            .unwrap();
        store
            .append_erasure_intent(
                "phone-1",
                "token-hash",
                "erase-1",
                "secret-hash",
                "2026-09-19T00:00:00Z",
                "2026-10-19T00:00:00Z",
            )
            .unwrap();
        assert_eq!(store.reconcile_health(&mut health).unwrap(), 1);
        let devices: i64 = health
            .query_row("SELECT count(*) FROM devices", [], |row| row.get(0))
            .unwrap();
        assert_eq!(devices, 0);
        store
            .append_metrics_erasure_verified("phone-1", "erase-1", "2026-09-19T00:01:00Z")
            .unwrap();
        store
            .append_backups_expired_verified("phone-1", "erase-1", "2026-09-19T00:02:00Z")
            .unwrap();
        store.reconcile_health(&mut health).unwrap();
        let state: (String, Option<String>, Option<String>) = health
            .query_row(
                "SELECT device_id,metrics_deleted_at,backups_expired_at FROM erasures WHERE erasure_id='erase-1'",
                [],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .unwrap();
        assert_eq!(state.0, digest(b"phone-1"));
        assert_eq!(state.1.as_deref(), Some("2026-09-19T00:01:00Z"));
        assert_eq!(state.2.as_deref(), Some("2026-09-19T00:02:00Z"));
        assert_eq!(store.reconcile_health(&mut health).unwrap(), 0);
        assert!(store.pending_metric_erasures().unwrap().is_empty());
    }

    #[test]
    fn seed_legacy_preserves_verified_erasure_timestamps() {
        let (directory, store) = store();
        let health_path = directory.path().join("health.db");
        initialize_health_database(&health_path, &store.store_id().unwrap()).unwrap();
        let mut health = open_health_database(&health_path).unwrap();
        let legacy_device_id = digest(b"original-legacy-device");
        health
            .execute(
                "INSERT INTO erasures(
                    device_id,erasure_id,erasure_secret_hash,requested_at,
                    metrics_deleted_at,backups_expired_at,backup_delete_by
                 ) VALUES (
                    ?1,'legacy-erasure','legacy-secret','2026-08-01T00:00:00Z',
                    '2026-08-02T00:00:00Z','2026-08-03T00:00:00Z','2026-09-01T00:00:00Z'
                 )",
                [&legacy_device_id],
            )
            .unwrap();

        store.seed_legacy(&health).unwrap();
        store.seed_legacy(&health).unwrap();
        assert!(store.verify_legacy_seed(&health).unwrap());
        assert_eq!(
            ControlStore::legacy_seed_fingerprint(&health)
                .unwrap()
                .len(),
            64
        );
        let control = store.open_read_only().unwrap();
        let facts: Vec<(String, String)> = {
            let mut statement = control
                .prepare(
                    "SELECT event_type,occurred_at FROM control_events
                     WHERE erasure_id='legacy-erasure'
                     ORDER BY sequence",
                )
                .unwrap();
            statement
                .query_map([], |row| Ok((row.get(0)?, row.get(1)?)))
                .unwrap()
                .collect::<rusqlite::Result<Vec<_>>>()
                .unwrap()
        };
        assert_eq!(
            facts,
            vec![
                (
                    "erasure_intent".to_owned(),
                    "2026-08-01T00:00:00Z".to_owned()
                ),
                (
                    "health_erasure_verified".to_owned(),
                    "2026-08-01T00:00:00Z".to_owned()
                ),
                (
                    "legacy_imported".to_owned(),
                    "2026-08-02T00:00:00Z".to_owned()
                ),
                (
                    "metrics_erasure_verified".to_owned(),
                    "2026-08-02T00:00:00Z".to_owned()
                ),
                (
                    "backups_expired_verified".to_owned(),
                    "2026-08-03T00:00:00Z".to_owned()
                ),
            ]
        );
        store
            .replay_all_tombstones_for_restore(&mut health)
            .unwrap();
        let restored_device_id: String = health
            .query_row(
                "SELECT device_id FROM erasures WHERE erasure_id='legacy-erasure'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(restored_device_id, legacy_device_id);
    }

    #[test]
    fn legacy_seed_resumes_only_an_exact_prefix_and_ends_with_fingerprint() {
        let (directory, store) = store();
        let health_path = directory.path().join("health.db");
        initialize_health_database(&health_path, &store.store_id().unwrap()).unwrap();
        let health = open_health_database(&health_path).unwrap();
        assert!(store.has_incomplete_legacy_seed(&health).unwrap());
        health
            .execute(
                "INSERT INTO devices(device_id,token_hash,created_at,revoked_at)
                 VALUES ('legacy-phone','legacy-token','2026-01-01T00:00:00Z','2026-02-01T00:00:00Z')",
                [],
            )
            .unwrap();
        store
            .append_credential_revoked("legacy-phone", "legacy-token", "2026-02-01T00:00:00Z")
            .unwrap();
        checkpoint_control_for_preflight(&store);
        store.preflight_legacy_seed_prefix(&health).unwrap();
        assert!(store.has_incomplete_legacy_seed(&health).unwrap());
        let fingerprint = store.seed_legacy_for_migration(&health).unwrap();
        assert_eq!(
            fingerprint,
            ControlStore::legacy_seed_fingerprint(&health).unwrap()
        );
        assert!(store.verify_legacy_seed(&health).unwrap());
        checkpoint_control_for_preflight(&store);
        store.preflight_legacy_seed_prefix(&health).unwrap();
        assert!(!store.has_incomplete_legacy_seed(&health).unwrap());

        store
            .append_credential_revoked("later-phone", "later-token", "2026-03-01T00:00:00Z")
            .unwrap();
        assert!(!store.verify_legacy_seed(&health).unwrap());
        checkpoint_control_for_preflight(&store);
        assert!(matches!(
            store.preflight_legacy_seed_prefix(&health),
            Err(ControlError::Integrity(_))
        ));
        assert!(!store.has_incomplete_legacy_seed(&health).unwrap());

        let (other_directory, other_store) = self::store();
        let other_health_path = other_directory.path().join("health.db");
        initialize_health_database(&other_health_path, &other_store.store_id().unwrap()).unwrap();
        let other_health = open_health_database(&other_health_path).unwrap();
        other_health
            .execute(
                "INSERT INTO devices(device_id,token_hash,created_at,revoked_at)
                 VALUES ('legacy-phone','legacy-token','2026-01-01T00:00:00Z','2026-02-01T00:00:00Z')",
                [],
            )
            .unwrap();
        other_store
            .append_credential_revoked("unrelated-phone", "unrelated-token", "2026-01-15T00:00:00Z")
            .unwrap();
        checkpoint_control_for_preflight(&other_store);
        assert!(matches!(
            other_store.preflight_legacy_seed_prefix(&other_health),
            Err(ControlError::Integrity(_))
        ));
        assert!(matches!(
            other_store.seed_legacy_for_migration(&other_health),
            Err(ControlError::Integrity(_))
        ));
    }

    #[test]
    fn restore_replay_prevents_old_snapshot_from_resurrecting_erased_identity() {
        let (directory, store) = store();
        let health_path = directory.path().join("health.db");
        initialize_health_database(&health_path, &store.store_id().unwrap()).unwrap();
        let mut health = open_health_database(&health_path).unwrap();
        health
            .execute_batch(
                "INSERT INTO pairing_codes(code_hash,expires_at) VALUES ('old-code','2099-01-01T00:00:00Z');
                 INSERT INTO devices(device_id,token_hash,created_at)
                    VALUES ('erased-phone','erased-token','2026-01-01T00:00:00Z');
                 INSERT INTO devices(device_id,token_hash,created_at)
                    VALUES ('revoked-phone','revoked-token','2026-01-01T00:00:00Z');
                 INSERT INTO events(
                    device_id,event_id,revision,operation,kind,health_type,
                    payload_json,payload_hash,updated_at
                 ) VALUES (
                    'erased-phone','event-1',1,'upsert','quantity','heartRate',
                    '{}','payload-hash','2026-01-01T00:00:00Z'
                 );
                 INSERT INTO receipts(
                    batch_id,device_id,content_hash,accepted_events,changed_events,
                    requires_projection,received_at
                 ) VALUES ('batch-1','erased-phone','content-hash',1,1,0,'2026-01-01T00:00:00Z');
                 INSERT INTO outbox(device_id,batch_id,metric_name,created_at)
                    VALUES ('erased-phone','batch-1','metric','2026-01-01T00:00:00Z');
                 INSERT INTO audit(device_id,action,batch_id,at)
                    VALUES ('erased-phone','ingest','batch-1','2026-01-01T00:00:00Z');",
            )
            .unwrap();
        store
            .append_credential_revoked("revoked-phone", "revoked-token", "2026-02-01T00:00:00Z")
            .unwrap();
        store
            .append_erasure_intent(
                "erased-phone",
                "erased-token",
                "erase-old",
                "erase-secret",
                "2026-03-01T00:00:00Z",
                "2026-04-01T00:00:00Z",
            )
            .unwrap();
        store
            .append_health_erasure_verified("erased-phone", "erase-old", "2026-03-01T00:01:00Z")
            .unwrap();
        store
            .append_metrics_erasure_verified("erased-phone", "erase-old", "2026-03-01T00:02:00Z")
            .unwrap();
        store
            .append_backups_expired_verified("erased-phone", "erase-old", "2026-03-15T00:00:00Z")
            .unwrap();

        let report = store
            .replay_all_tombstones_for_restore(&mut health)
            .unwrap();
        assert_eq!(report.revoked_devices, 1);
        assert_eq!(report.erased_devices, 1);
        assert_eq!(report.cleared_pairing_codes, 1);
        for table in ["pairing_codes", "events", "receipts", "outbox", "audit"] {
            let count: i64 = health
                .query_row(&format!("SELECT count(*) FROM {table}"), [], |row| {
                    row.get(0)
                })
                .unwrap();
            assert_eq!(count, 0, "{table} retained restored authority");
        }
        let erased_device: i64 = health
            .query_row(
                "SELECT count(*) FROM devices WHERE device_id='erased-phone'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(erased_device, 0);
        let revoked_at: String = health
            .query_row(
                "SELECT revoked_at FROM devices WHERE device_id='revoked-phone'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(revoked_at, "2026-02-01T00:00:00Z");
        let erasure: (String, Option<String>, Option<String>) = health
            .query_row(
                "SELECT device_id,metrics_deleted_at,backups_expired_at
                 FROM erasures WHERE erasure_id='erase-old'",
                [],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .unwrap();
        assert_eq!(erasure.0, digest(b"erased-phone"));
        assert_eq!(erasure.1.as_deref(), Some("2026-03-01T00:02:00Z"));
        assert_eq!(erasure.2.as_deref(), Some("2026-03-15T00:00:00Z"));
        assert!(
            store
                .matching_erasure_intent("erase-old", "erase-secret")
                .unwrap()
                .is_some()
        );

        let second = store
            .replay_all_tombstones_for_restore(&mut health)
            .unwrap();
        assert_eq!(second.erased_devices, 1);
        assert_eq!(second.cleared_pairing_codes, 0);
        store.verify().unwrap();
    }

    #[test]
    fn active_backup_inventory_binds_hash_and_prior_checkpoint() {
        let (_directory, store) = store();
        let hash = "a".repeat(64);
        let prior = store.checkpoint().unwrap();
        store
            .append_backup_created("snapshot-1", &hash, "2026-09-19T00:00:00Z", &prior)
            .unwrap();
        store
            .append_backup_created("snapshot-1", &hash, "2026-09-19T00:00:00Z", &prior)
            .unwrap();
        let inventory = store.active_backup_inventory().unwrap();
        assert_eq!(inventory.len(), 1);
        assert_eq!(inventory[0].snapshot_id, "snapshot-1");
        assert_eq!(inventory[0].file_sha256, hash);
        assert_eq!(inventory[0].prior_checkpoint.sequence, 0);
        assert_eq!(inventory[0].prior_checkpoint.current_hash, GENESIS_HASH);
        assert!(
            store
                .verify_backup_artifact("snapshot-1", &"a".repeat(64))
                .unwrap()
        );
        assert!(
            !store
                .verify_backup_artifact("snapshot-1", &"b".repeat(64))
                .unwrap()
        );
        assert!(matches!(
            store.append_backup_created(
                "snapshot-1",
                &"b".repeat(64),
                "2026-09-19T00:00:00Z",
                &prior,
            ),
            Err(ControlError::Integrity(_))
        ));
        assert!(matches!(
            store.append_backup_deleted("snapshot-1", "2026-09-20T00:00:00Z"),
            Err(ControlError::Integrity(_))
        ));
        store
            .append_backup_delete_intent(
                "snapshot-1",
                &hash,
                "boaz-health-snapshot-1.db",
                "2026-09-20T00:00:00Z",
            )
            .unwrap();
        assert_eq!(store.pending_backup_delete_intents().unwrap().len(), 1);
        store
            .append_backup_deleted("snapshot-1", "2026-09-20T00:00:00Z")
            .unwrap();
        assert!(store.active_backup_inventory().unwrap().is_empty());
    }

    #[test]
    fn backup_creation_rejects_an_advanced_manifest_checkpoint() {
        let (_directory, store) = store();
        let stale = store.checkpoint().unwrap();
        store
            .append_credential_revoked("phone-1", "token-1", "2026-09-19T00:00:00Z")
            .unwrap();
        assert!(matches!(
            store.append_backup_created(
                "snapshot-stale",
                &"a".repeat(64),
                "2026-09-19T00:01:00Z",
                &stale,
            ),
            Err(ControlError::Integrity(_))
        ));
        assert!(store.active_backup_inventory().unwrap().is_empty());
    }

    #[test]
    fn v1_migration_preserves_original_event_hash_and_rejects_unknown_version() {
        let directory = TempDir::new().unwrap();
        let root = directory.path().join("control");
        let mirror = root.join("mirror");
        fs::create_dir_all(&mirror).unwrap();
        let db = root.join("control.db");
        let connection = Connection::open(&db).unwrap();
        configure(&connection).unwrap();
        connection.execute_batch(CONTROL_SCHEMA_V1).unwrap();
        connection.execute(
            "INSERT INTO control_meta(singleton,store_id,created_at) VALUES (1,'v1-store','2026-09-19T00:00:00Z')",
            [],
        ).unwrap();
        let event = NewEvent {
            stable_key: "old-token",
            event_type: "credential_revoked",
            device_id: Some("old-phone"),
            token_hash: Some("old-token"),
            erasure_id: None,
            secret_hash: None,
            snapshot_id: None,
            restore_epoch: None,
            occurred_at: "2026-09-19T00:01:00Z",
            deadline_at: None,
            evidence_digest: None,
            artifact_name: None,
        };
        let id = event_id(&event);
        let old_hash = event_hash(&HashRecord {
            event_id: &id,
            event_type: event.event_type,
            device_id: event.device_id,
            token_hash: event.token_hash,
            erasure_id: None,
            secret_hash: None,
            snapshot_id: None,
            restore_epoch: None,
            occurred_at: event.occurred_at,
            deadline_at: None,
            previous_hash: GENESIS_HASH,
        })
        .unwrap();
        connection.execute(
            "INSERT INTO control_events(event_id,event_type,device_id,token_hash,occurred_at,previous_hash,current_hash)
             VALUES (?1,'credential_revoked','old-phone','old-token','2026-09-19T00:01:00Z',?2,?3)",
            params![id,GENESIS_HASH,old_hash],
        ).unwrap();
        connection
            .execute_batch(&format!(
                "PRAGMA application_id={CONTROL_APPLICATION_ID}; PRAGMA user_version=1;"
            ))
            .unwrap();
        drop(connection);
        write_mirror_create_new(
            &mirror,
            1,
            &old_hash,
            &serde_json::to_vec(&serde_json::json!({
                "store_id":"v1-store", "sequence":1, "event_id":id, "current_hash":old_hash,
            }))
            .unwrap(),
        )
        .unwrap();
        write_head_atomic(
            &root.join("control.head.json"),
            &ControlHead {
                store_id: "v1-store".to_owned(),
                sequence: 1,
                current_hash: old_hash.clone(),
            },
        )
        .unwrap();
        let store = ControlStore::new(db, mirror).unwrap();
        assert_eq!(store.preflight_migration().unwrap(), "v1-store");
        let health_path = directory.path().join("health.db");
        initialize_health_database(&health_path, "v1-store").unwrap();
        let health = open_health_database(&health_path).unwrap();
        health
            .execute(
                "INSERT INTO devices(device_id,token_hash,created_at,revoked_at)
             VALUES ('old-phone','old-token','2026-09-19T00:00:00Z','2026-09-19T00:01:00Z')",
                [],
            )
            .unwrap();
        store.preflight_legacy_seed_prefix(&health).unwrap();
        health
            .execute(
                "UPDATE devices SET revoked_at='2026-09-19T00:02:00Z' WHERE device_id='old-phone'",
                [],
            )
            .unwrap();
        assert!(matches!(
            store.preflight_legacy_seed_prefix(&health),
            Err(ControlError::Integrity(_))
        ));
        health
            .execute(
                "UPDATE devices SET revoked_at='2026-09-19T00:01:00Z' WHERE device_id='old-phone'",
                [],
            )
            .unwrap();
        assert!(!store.publication_lock_path.exists());
        assert!(matches!(
            store.migrate_to_v3_inner(true),
            Err(ControlError::Integrity(_))
        ));
        assert_eq!(store.preflight_migration().unwrap(), "v1-store");
        let version_after_fault: i64 = store
            .open_preflight_read_only()
            .unwrap()
            .query_row("PRAGMA user_version", [], |row| row.get(0))
            .unwrap();
        assert_eq!(version_after_fault, 1);
        store.migrate_v1_to_v2().unwrap();
        store.migrate_v1_to_v2().unwrap();
        assert_eq!(store.checkpoint().unwrap().current_hash, old_hash);
        assert!(store.token_tombstoned("old-token").unwrap());
        let connection = store.open_read_only().unwrap();
        let hash_version: i64 = connection
            .query_row(
                "SELECT hash_version FROM control_events WHERE sequence=1",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(hash_version, 1);
        drop(connection);
        store
            .append_credential_revoked("new-phone", "new-token", "2026-09-19T00:02:00Z")
            .unwrap();
        store.verify().unwrap();
        let connection = store.open_read_only().unwrap();
        let new_hash_version: i64 = connection
            .query_row(
                "SELECT hash_version FROM control_events WHERE sequence=2",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(new_hash_version, 2);
        drop(connection);
        let connection = store.open_read_write().unwrap();
        connection.execute_batch("PRAGMA user_version=4").unwrap();
        drop(connection);
        assert!(matches!(
            store.migrate_v1_to_v2(),
            Err(ControlError::Invalid(_))
        ));
        let connection = store.open_read_only().unwrap();
        let version: i64 = connection
            .query_row("PRAGMA user_version", [], |row| row.get(0))
            .unwrap();
        assert_eq!(version, 4);
    }

    #[test]
    fn restore_completion_requires_valid_bound_evidence_and_is_idempotent() {
        let (_directory, store) = store();
        assert!(matches!(
            store.append_restore_completed("epoch-1", "snapshot-1", "2026-09-19T00:00:00Z"),
            Err(ControlError::Invalid(_))
        ));
        let proof = "a".repeat(64);
        assert!(matches!(
            store.append_restore_completed_with_evidence(
                "epoch-1",
                "snapshot-1",
                "2026-09-19T00:00:00Z",
                &proof,
            ),
            Err(ControlError::Integrity(_))
        ));
        store
            .append_restore_started("epoch-1", "snapshot-1", "2026-09-19T00:00:00Z")
            .unwrap();
        store
            .append_restore_replayed("epoch-1", "snapshot-1", "2026-09-19T00:00:01Z")
            .unwrap();
        store
            .append_restore_completed_with_evidence(
                "epoch-1",
                "snapshot-1",
                "2026-09-19T00:00:00Z",
                &proof,
            )
            .unwrap();
        let first = store.checkpoint().unwrap();
        store
            .append_restore_completed_with_evidence(
                "epoch-1",
                "snapshot-1",
                "2026-09-19T00:00:00Z",
                &proof,
            )
            .unwrap();
        assert_eq!(store.checkpoint().unwrap(), first);
        assert!(matches!(
            store.append_restore_completed_with_evidence(
                "epoch-1",
                "snapshot-1",
                "2026-09-19T00:00:00Z",
                &"b".repeat(64),
            ),
            Err(ControlError::Integrity(_))
        ));
    }

    #[test]
    fn backup_delete_intent_binds_name_and_hash() {
        let (_directory, store) = store();
        let hash = "a".repeat(64);
        let prior = store.checkpoint().unwrap();
        store
            .append_backup_created("snapshot-1", &hash, "2026-09-19T00:00:00Z", &prior)
            .unwrap();
        assert!(matches!(
            store.append_backup_delete_intent(
                "snapshot-1",
                &hash,
                "../bad.db",
                "2026-09-20T00:00:00Z",
            ),
            Err(ControlError::Invalid(_))
        ));
        store
            .append_backup_delete_intent(
                "snapshot-1",
                &hash,
                "boaz-health-snapshot-1.db",
                "2026-09-20T00:00:00Z",
            )
            .unwrap();
        assert!(matches!(
            store.append_backup_deleted_with_hash(
                "snapshot-1",
                &"b".repeat(64),
                "boaz-health-snapshot-1.db",
                "2026-09-20T00:01:00Z",
            ),
            Err(ControlError::Integrity(_))
        ));
        assert_eq!(store.pending_backup_delete_intents().unwrap().len(), 1);
        store
            .append_backup_deleted_with_hash(
                "snapshot-1",
                &hash,
                "boaz-health-snapshot-1.db",
                "2026-09-20T00:01:00Z",
            )
            .unwrap();
        assert!(store.pending_backup_delete_intents().unwrap().is_empty());
        store.verify().unwrap();
    }

    #[test]
    fn publication_reconciliation_requires_independent_exact_tip() {
        let (_directory, store) = store();
        let prior = store.checkpoint().unwrap();
        store
            .append_credential_revoked("phone-1", "token-1", "2026-09-19T00:00:00Z")
            .unwrap();
        let tip = store.checkpoint().unwrap();
        let latest_mirror = mirror_path(store.mirror_dir(), tip.sequence, &tip.current_hash);
        fs::remove_file(&latest_mirror).unwrap();
        write_head_atomic(
            store.head_path(),
            &ControlHead {
                store_id: prior.store_id.clone(),
                sequence: prior.sequence,
                current_hash: prior.current_hash.clone(),
            },
        )
        .unwrap();
        assert!(matches!(
            store.reconcile_publication(&prior),
            Err(ControlError::Integrity(_))
        ));
        assert!(!latest_mirror.exists());
        store.reconcile_publication(&tip).unwrap();
        assert!(latest_mirror.exists());
        assert_eq!(store.checkpoint().unwrap(), tip);
    }

    #[test]
    fn publication_reconciliation_rejects_missing_historical_mirror() {
        let (_directory, store) = store();
        store
            .append_credential_revoked("phone-1", "token-1", "2026-09-19T00:00:00Z")
            .unwrap();
        let first = store.checkpoint().unwrap();
        store
            .append_credential_revoked("phone-2", "token-2", "2026-09-19T00:01:00Z")
            .unwrap();
        let tip = store.checkpoint().unwrap();
        fs::remove_file(mirror_path(
            store.mirror_dir(),
            first.sequence,
            &first.current_hash,
        ))
        .unwrap();
        assert!(matches!(
            store.reconcile_publication(&tip),
            Err(ControlError::Integrity(_))
        ));
    }

    #[test]
    fn migration_preflight_rejects_unknown_version_without_creating_lock_or_sidecars() {
        let (_directory, store) = store();
        let parent = store.db_path().parent().unwrap();
        let names = || {
            fs::read_dir(parent)
                .unwrap()
                .map(|entry| entry.unwrap().file_name().into_string().unwrap())
                .collect::<BTreeSet<_>>()
        };
        let before = names();
        assert!(!store.preflight_migration().unwrap().is_empty());
        assert_eq!(names(), before);
        assert!(!store.publication_lock_path.exists());
        let connection = store.open_read_write().unwrap();
        connection.execute_batch("PRAGMA user_version=99").unwrap();
        drop(connection);
        let invalid_before = names();
        assert!(matches!(
            store.preflight_migration(),
            Err(ControlError::Invalid(_))
        ));
        assert!(matches!(
            store.migrate_v1_to_v2(),
            Err(ControlError::Invalid(_))
        ));
        assert_eq!(names(), invalid_before);
        assert!(!store.publication_lock_path.exists());
    }

    #[test]
    fn migration_preflight_rejects_partial_schema_without_creating_lock() {
        let (_directory, store) = store();
        let connection = store.open_read_write().unwrap();
        connection
            .execute_batch("CREATE TABLE unexpected(value INTEGER)")
            .unwrap();
        drop(connection);
        checkpoint_control_for_preflight(&store);
        let parent = store.db_path().parent().unwrap();
        let before = fs::read_dir(parent)
            .unwrap()
            .map(|entry| entry.unwrap().file_name().into_string().unwrap())
            .collect::<BTreeSet<_>>();
        let failure = store.preflight_migration().unwrap_err();
        assert!(failure.to_string().contains("reviewed layout"));
        assert!(matches!(
            store.migrate_v1_to_v2(),
            Err(ControlError::Invalid(_))
        ));
        let after = fs::read_dir(parent)
            .unwrap()
            .map(|entry| entry.unwrap().file_name().into_string().unwrap())
            .collect::<BTreeSet<_>>();
        assert_eq!(after, before);
        assert!(!store.publication_lock_path.exists());
    }

    fn v2_store_with_prefix_event() -> (TempDir, ControlStore, String) {
        let directory = TempDir::new().unwrap();
        let root = directory.path().join("control");
        let mirror = root.join("mirror");
        fs::create_dir_all(&mirror).unwrap();
        let db = root.join("control.db");
        let connection = Connection::open(&db).unwrap();
        configure(&connection).unwrap();
        connection.execute_batch(CONTROL_SCHEMA_V2).unwrap();
        connection.execute(
            "INSERT INTO control_meta(singleton,store_id,created_at) VALUES (1,'v2-store','2026-09-19T00:00:00Z')",
            [],
        ).unwrap();
        let old = NewEvent {
            stable_key: "old-token",
            event_type: "credential_revoked",
            device_id: Some("old-phone"),
            token_hash: Some("old-token"),
            erasure_id: None,
            secret_hash: None,
            snapshot_id: None,
            restore_epoch: None,
            occurred_at: "2026-09-19T00:00:01Z",
            deadline_at: None,
            evidence_digest: None,
            artifact_name: None,
        };
        let id = event_id(&old);
        let hash = event_hash_versioned(
            2,
            &HashRecord {
                event_id: &id,
                event_type: old.event_type,
                device_id: old.device_id,
                token_hash: old.token_hash,
                erasure_id: None,
                secret_hash: None,
                snapshot_id: None,
                restore_epoch: None,
                occurred_at: old.occurred_at,
                deadline_at: None,
                previous_hash: GENESIS_HASH,
            },
            None,
            None,
        )
        .unwrap();
        connection.execute(
            "INSERT INTO control_events(event_id,event_type,device_id,token_hash,occurred_at,previous_hash,current_hash,hash_version)
             VALUES (?1,'credential_revoked','old-phone','old-token','2026-09-19T00:00:01Z',?2,?3,2)",
            params![id, GENESIS_HASH, hash],
        ).unwrap();
        connection
            .execute_batch(&format!(
                "PRAGMA application_id={CONTROL_APPLICATION_ID}; PRAGMA user_version=2;"
            ))
            .unwrap();
        drop(connection);
        write_mirror_create_new(
            &mirror,
            1,
            &hash,
            &serde_json::to_vec(&serde_json::json!({
                "store_id":"v2-store", "sequence":1, "event_id":id, "current_hash":hash,
            }))
            .unwrap(),
        )
        .unwrap();
        write_head_atomic(
            &root.join("control.head.json"),
            &ControlHead {
                store_id: "v2-store".to_owned(),
                sequence: 1,
                current_hash: hash.clone(),
            },
        )
        .unwrap();
        let store = ControlStore::new(db, mirror).unwrap();
        store.preflight_migration().unwrap();
        (directory, store, hash)
    }

    #[test]
    fn v2_migration_retains_original_table_and_hashes_without_drop() {
        let (_directory, store, old_hash) = v2_store_with_prefix_event();
        store.migrate_to_v3().unwrap();
        let connection = store.open_read_only().unwrap();
        let prefix: String = connection
            .query_row(
                "SELECT type FROM sqlite_schema WHERE name='control_events_prefix'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(prefix, "table");
        let version: i64 = connection
            .query_row("PRAGMA user_version", [], |row| row.get(0))
            .unwrap();
        assert_eq!(version, 3);
        let persisted_hash: String = connection
            .query_row(
                "SELECT current_hash FROM control_events_prefix WHERE sequence=1",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(persisted_hash, old_hash);
        drop(connection);
        store
            .append_credential_revoked("new-phone", "new-token", "2026-09-19T00:01:00Z")
            .unwrap();
        assert_eq!(store.checkpoint().unwrap().sequence, 2);
    }

    #[test]
    fn v3_rejects_event_id_reused_across_prefix_and_suffix() {
        let (_directory, store, old_hash) = v2_store_with_prefix_event();
        store.migrate_to_v3().unwrap();
        let connection = store.open_read_write().unwrap();
        let existing_id: String = connection
            .query_row(
                "SELECT event_id FROM control_events WHERE sequence=1",
                [],
                |row| row.get(0),
            )
            .unwrap();
        let duplicated = connection.execute(
            "INSERT INTO control_events_suffix(sequence,event_id,event_type,device_id,token_hash,occurred_at,previous_hash,current_hash,hash_version)
             VALUES (2,?1,'credential_revoked','phone-2','token-2','2026-09-19T00:01:00Z',?2,?3,2)",
            params![existing_id, old_hash, "1".repeat(64)],
        );
        assert!(duplicated.is_err());
    }
}
