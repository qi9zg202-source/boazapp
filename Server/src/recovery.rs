use crate::{
    ack_journal::{AckJournal, ConfirmedBatch, PairingRecoveryRecord},
    control::{ControlCheckpoint, ControlError, ControlStore},
    database::{
        HEALTH_APPLICATION_ID, HEALTH_SCHEMA_VERSION, StorageError, open_health_database,
        verify_health_database,
    },
};
use chrono::{DateTime, Utc};
use rusqlite::{Connection, OpenFlags, OptionalExtension, TransactionBehavior, params};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
#[cfg(target_os = "linux")]
use std::process::{Command, Stdio};
use std::{
    collections::{BTreeMap, BTreeSet},
    fmt,
    fs::{self, File, OpenOptions},
    io::{self, Read, Write},
    path::{Component, Path, PathBuf},
    process::Child,
    time::{Duration, Instant},
};

const CONTROL_BACKUP_FORMAT_VERSION: u8 = 1;
const HEALTH_BACKUP_FORMAT_VERSION: u8 = 1;
const RESTORE_SESSION_FORMAT_VERSION: u8 = 1;
const CONTROL_DATABASE_NAME: &str = "control.db";
const CONTROL_HEAD_NAME: &str = "control.head.json";
const CONTROL_MIRROR_DIRECTORY: &str = "mirror";
const CONTROL_MANIFEST_NAME: &str = "control-backup.manifest.json";
const RESTORE_SESSION_NAME: &str = "restore-session.json";
const RESTORE_COMPLETION_INTENT_NAME: &str = "restore-completion-intent.json";
const RESTORE_PROJECTION_EVIDENCE_NAME: &str = "projection-evidence.json";
const RESTORE_COVERAGE_EVIDENCE_NAME: &str = "ack-coverage-evidence.json";

#[derive(Debug)]
pub enum RecoveryError {
    InvalidPath(String),
    InvalidArtifact(String),
    Integrity(String),
    Io(io::Error),
    Json(serde_json::Error),
    Sql(rusqlite::Error),
    Control(ControlError),
    Storage(StorageError),
}

impl fmt::Display for RecoveryError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidPath(message) => write!(formatter, "recovery path rejected: {message}"),
            Self::InvalidArtifact(message) => {
                write!(formatter, "recovery artifact rejected: {message}")
            }
            Self::Integrity(message) => write!(formatter, "recovery integrity failed: {message}"),
            Self::Io(error) => write!(formatter, "recovery I/O failed: {error}"),
            Self::Json(error) => write!(formatter, "recovery JSON is invalid: {error}"),
            Self::Sql(error) => write!(formatter, "recovery SQLite operation failed: {error}"),
            Self::Control(error) => write!(formatter, "{error}"),
            Self::Storage(error) => write!(formatter, "{error}"),
        }
    }
}

impl std::error::Error for RecoveryError {}

impl From<io::Error> for RecoveryError {
    fn from(value: io::Error) -> Self {
        Self::Io(value)
    }
}

impl From<serde_json::Error> for RecoveryError {
    fn from(value: serde_json::Error) -> Self {
        Self::Json(value)
    }
}

impl From<rusqlite::Error> for RecoveryError {
    fn from(value: rusqlite::Error) -> Self {
        Self::Sql(value)
    }
}

impl From<ControlError> for RecoveryError {
    fn from(value: ControlError) -> Self {
        Self::Control(value)
    }
}

impl From<StorageError> for RecoveryError {
    fn from(value: StorageError) -> Self {
        Self::Storage(value)
    }
}

pub type RecoveryResult<T> = Result<T, RecoveryError>;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ArtifactFile {
    pub relative_path: String,
    pub size_bytes: u64,
    pub sha256: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ControlBackupManifest {
    pub format_version: u8,
    pub snapshot_id: String,
    pub created_at: String,
    pub checkpoint: ControlCheckpoint,
    pub database: ArtifactFile,
    pub head: ArtifactFile,
    pub mirrors: Vec<ArtifactFile>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct HealthBackupManifest {
    #[serde(default = "health_backup_format_version")]
    pub format_version: u8,
    pub snapshot_id: String,
    pub snapshot_started_at: String,
    pub source_commit_sequence: Option<i64>,
    pub source_schema_version: i64,
    pub file_sha256: String,
    pub control_checkpoint: ControlCheckpoint,
}

fn health_backup_format_version() -> u8 {
    HEALTH_BACKUP_FORMAT_VERSION
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
enum RestorePhase {
    ControlStaged,
    HealthCopied,
    RestoreStarted,
    ReplayPrepared,
    ProjectionRebuildRequired,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
#[serde(deny_unknown_fields)]
struct ReplaySummary {
    revoked_devices: usize,
    erased_devices: usize,
    cleared_pairing_codes: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
struct RestoreSession {
    format_version: u8,
    expected_control_checkpoint: ControlCheckpoint,
    phase: RestorePhase,
    restore_epoch: Option<String>,
    health_snapshot_id: Option<String>,
    health_backup_sha256: Option<String>,
    restore_started_at: Option<String>,
    restore_replayed_at: Option<String>,
    replay_summary: Option<ReplaySummary>,
    replayed_health_sha256: Option<String>,
    #[serde(default)]
    ack_replayed_batches: Option<usize>,
}

#[derive(Debug, Clone)]
pub struct StagedControl {
    pub staging_root: PathBuf,
    pub store: ControlStore,
}

/// Owns only the native *staging* VM child started by this process. The target
/// is fixed to the isolated 18428 loopback listener; neither a live VM child
/// nor a pre-existing listener can be adopted by this handle.
pub struct NativeStagingVm {
    child: Child,
    target: crate::projection::VmTarget,
}

impl NativeStagingVm {
    pub fn target(&self) -> &crate::projection::VmTarget {
        &self.target
    }

    /// Launch is accepted only inside an isolated Linux network namespace on
    /// an encrypted candidate mount. A full oracle/readback and later live
    /// restart/readback remain separate gates.
    pub fn launch(
        live: &crate::projection::VmConfig,
        candidate: crate::projection::VmConfig,
        retention_period: &str,
    ) -> RecoveryResult<Self> {
        Self::launch_inner(live, candidate, retention_period, false)
    }

    /// Restarts an already-created isolated candidate generation. The
    /// generation marker, encrypted mount, storage identity, loopback-only
    /// namespace and vacant staging port must all still match. The caller
    /// must then perform a fresh complete export before trusting its content.
    pub fn resume(
        live: &crate::projection::VmConfig,
        candidate: crate::projection::VmConfig,
        retention_period: &str,
    ) -> RecoveryResult<Self> {
        Self::launch_inner(live, candidate, retention_period, true)
    }

    fn launch_inner(
        live: &crate::projection::VmConfig,
        candidate: crate::projection::VmConfig,
        retention_period: &str,
        resume_existing: bool,
    ) -> RecoveryResult<Self> {
        #[cfg(not(target_os = "linux"))]
        {
            let _ = (live, candidate, retention_period, resume_existing);
            Err(RecoveryError::Integrity(
                "native staging VictoriaMetrics requires isolated Linux".to_owned(),
            ))
        }
        #[cfg(target_os = "linux")]
        {
            use std::net::TcpListener;
            if !crate::projection::staging_network_isolated() {
                return Err(RecoveryError::Integrity(
                    "staging VM launch requires an isolated loopback-only Linux network namespace"
                        .to_owned(),
                ));
            }
            if !valid_retention_period(retention_period) {
                return Err(RecoveryError::InvalidArtifact(
                    "staging VM retention period is invalid".to_owned(),
                ));
            }
            validate_regular_single_link(&candidate.binary)?;
            reject_symlink_or_non_directory(&candidate.storage)?;
            if !crate::projection::encrypted_mount_verified(&candidate.storage) {
                return Err(RecoveryError::Integrity(
                    "staging VM storage is not on a verified encrypted mount".to_owned(),
                ));
            }
            let unbound_target = crate::projection::VmTarget::staging(live, candidate.clone())
                .map_err(RecoveryError::Integrity)?;
            if resume_existing {
                if !crate::projection::staging_generation_resume_verified(&unbound_target) {
                    return Err(RecoveryError::Integrity(
                        "staging VM generation is not safe to resume".to_owned(),
                    ));
                }
            } else if fs::read_dir(&candidate.storage)?
                .next()
                .transpose()?
                .is_some()
            {
                return Err(RecoveryError::InvalidPath(
                    "staging VM storage must start as an empty directory".to_owned(),
                ));
            }
            let socket_probe = TcpListener::bind("127.0.0.1:18428").map_err(|_| {
                RecoveryError::Integrity("isolated staging VM port is already occupied".to_owned())
            })?;
            drop(socket_probe);
            let mut child = Command::new(&candidate.binary)
                .arg(format!("-storageDataPath={}", candidate.storage.display()))
                .arg("-httpListenAddr=127.0.0.1:18428")
                .arg(format!("-retentionPeriod={retention_period}"))
                .stdin(Stdio::null())
                .stdout(Stdio::null())
                .stderr(Stdio::null())
                .env_clear()
                .spawn()?;
            let target = match unbound_target.bind_native_child(child.id()) {
                Ok(target) => target,
                Err(error) => {
                    let _ = child.kill();
                    let _ = child.wait();
                    return Err(RecoveryError::Integrity(error));
                }
            };
            let deadline = Instant::now() + Duration::from_secs(15);
            loop {
                if crate::projection::native_staging_vm_verified(&target) {
                    return Ok(Self { child, target });
                }
                if let Some(status) = child.try_wait()? {
                    return Err(RecoveryError::Integrity(format!(
                        "native staging VM exited before verified readiness: {status}"
                    )));
                }
                if Instant::now() >= deadline {
                    let _ = child.kill();
                    let _ = child.wait();
                    return Err(RecoveryError::Integrity(
                        "native staging VM failed verified readiness within 15 seconds".to_owned(),
                    ));
                }
                std::thread::sleep(Duration::from_millis(100));
            }
        }
    }

    /// A graceful stop is required before final storage verification; Drop
    /// remains a fail-safe kill/wait if an error path exits unexpectedly.
    pub fn stop(&mut self) -> RecoveryResult<()> {
        if self.child.try_wait()?.is_some() {
            return Ok(());
        }
        #[cfg(unix)]
        {
            let pid = self.child.id();
            let result = unsafe { libc::kill(pid as libc::pid_t, libc::SIGTERM) };
            if result != 0 {
                return Err(io::Error::last_os_error().into());
            }
        }
        #[cfg(not(unix))]
        self.child.kill()?;
        let deadline = Instant::now() + Duration::from_secs(10);
        while self.child.try_wait()?.is_none() {
            if Instant::now() >= deadline {
                self.child.kill()?;
                self.child.wait()?;
                return Err(RecoveryError::Integrity(
                    "staging VM required forced termination".to_owned(),
                ));
            }
            std::thread::sleep(Duration::from_millis(100));
        }
        Ok(())
    }
}

impl Drop for NativeStagingVm {
    fn drop(&mut self) {
        if self.child.try_wait().ok().flatten().is_none() {
            let _ = self.child.kill();
            let _ = self.child.wait();
        }
    }
}

#[cfg(target_os = "linux")]
fn valid_retention_period(value: &str) -> bool {
    let digits = value.trim_end_matches(|byte: char| byte.is_ascii_alphabetic());
    !digits.is_empty()
        && digits.len() <= 3
        && digits.bytes().all(|byte| byte.is_ascii_digit())
        && digits.parse::<u16>().is_ok_and(|number| number > 0)
        && matches!(&value[digits.len()..], "d" | "w" | "M" | "y")
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "status", rename_all = "snake_case")]
pub enum HealthRestoreState {
    ProjectionRebuildRequired {
        restore_epoch: String,
        snapshot_id: String,
        health_db: PathBuf,
        revoked_devices: usize,
        erased_devices: usize,
        cleared_pairing_codes: usize,
        control_checkpoint: ControlCheckpoint,
    },
}

/// Persisted before appending `restore_completed`, so an interrupted append
/// retries with the same timestamp and evidence digest. This is a proposed
/// completion, not evidence that the control event or cutover completed.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct RestoreCompletionIntent {
    pub format_version: u8,
    pub restore_epoch: String,
    pub snapshot_id: String,
    pub completed_at: String,
    pub health_sha256: String,
    pub projection_evidence_sha256: String,
    #[serde(default)]
    pub ack_coverage_evidence_sha256: Option<String>,
    pub control_checkpoint_before_completion: ControlCheckpoint,
}

#[derive(Debug, Clone)]
pub struct MaterializedCandidate {
    pub health_db: PathBuf,
    pub control_store: ControlStore,
    pub health_sha256: String,
    pub control_checkpoint: ControlCheckpoint,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct AckCoverageEvidence {
    pub format_version: u8,
    pub snapshot_id: String,
    pub snapshot_sha256: String,
    pub receipt_inventory_sha256: String,
    pub journal_id: String,
    pub adoption_head_sha256: String,
    pub confirmed_batch_count: usize,
    pub replayed_batch_count: usize,
    pub highest_confirmed_commit_sequence: i64,
    pub staged_health_sha256: String,
}

struct AckReplayPlan {
    journal_id: String,
    adoption_head_sha256: String,
    receipt_inventory_sha256: String,
    baseline_sequence: i64,
    batches: Vec<ConfirmedBatch>,
    pairings: Vec<PairingRecoveryRecord>,
}

/// The receipt inventory deliberately excludes mutable projection state. It
/// binds the snapshot to the exact set of transactions that had been accepted
/// before the snapshot, not merely to SQLite's allocation high-water mark.
pub fn receipt_inventory_sha256(connection: &Connection) -> RecoveryResult<String> {
    let mut statement = connection.prepare(
        "SELECT commit_sequence,batch_id,device_id,content_hash,accepted_events,changed_events,requires_projection,received_at
         FROM receipts ORDER BY commit_sequence",
    )?;
    let mut rows = statement.query([])?;
    let mut hasher = Sha256::new();
    hasher.update(b"boaz-receipt-inventory-v1\0");
    while let Some(row) = rows.next()? {
        let fields = [
            row.get::<_, i64>(0)?.to_string(),
            row.get::<_, String>(1)?,
            row.get::<_, String>(2)?,
            row.get::<_, String>(3)?,
            row.get::<_, i64>(4)?.to_string(),
            row.get::<_, i64>(5)?.to_string(),
            row.get::<_, i64>(6)?.to_string(),
            row.get::<_, String>(7)?,
        ];
        for field in fields {
            hasher.update((field.len() as u64).to_be_bytes());
            hasher.update(field.as_bytes());
        }
    }
    Ok(hex::encode(hasher.finalize()))
}

fn verified_ack_replay_plan(
    health_backup: &Path,
    manifest: &HealthBackupManifest,
    control: &ControlStore,
    journal: &AckJournal,
) -> RecoveryResult<AckReplayPlan> {
    // The adoption snapshot is the only admitted baseline. A later snapshot
    // needs its own independently bound inventory checkpoint; guessing from a
    // maximum sequence would silently lose acknowledged legal writes.
    let anchor = journal.baseline()?.ok_or_else(|| {
        RecoveryError::Integrity(
            "ack journal has no independently bound adoption baseline".to_owned(),
        )
    })?;
    if anchor.baseline.snapshot_id != manifest.snapshot_id
        || anchor.baseline.snapshot_sha256 != manifest.file_sha256
        || anchor.baseline.control_store_id != control.store_id()?
    {
        return Err(RecoveryError::Integrity(
            "selected health backup is not the journal's exact adopted baseline".to_owned(),
        ));
    }
    let anchor_checkpoint = ControlCheckpoint {
        store_id: anchor.baseline.control_store_id.clone(),
        sequence: anchor.baseline.control_head_sequence,
        current_hash: anchor.baseline.control_head_hash.clone(),
    };
    if !control.contains_checkpoint(&anchor_checkpoint)?
        || anchor_checkpoint.sequence < manifest.control_checkpoint.sequence
    {
        return Err(RecoveryError::Integrity(
            "journal adoption head is not in the selected control chain".to_owned(),
        ));
    }
    let baseline_sequence = manifest.source_commit_sequence.ok_or_else(|| {
        RecoveryError::Integrity("adoption snapshot lacks a durable receipt sequence".to_owned())
    })?;
    if baseline_sequence < 0 {
        return Err(RecoveryError::Integrity(
            "adoption snapshot receipt sequence is invalid".to_owned(),
        ));
    }
    validate_health_backup(health_backup, manifest, control)?;
    let snapshot = Connection::open_with_flags(health_backup, OpenFlags::SQLITE_OPEN_READ_ONLY)?;
    let inventory = receipt_inventory_sha256(&snapshot)?;
    if inventory != anchor.baseline.receipt_inventory_sha256 {
        return Err(RecoveryError::Integrity(
            "adoption snapshot receipt inventory differs from independent journal anchor"
                .to_owned(),
        ));
    }
    let batches = journal.confirmed_batches()?;
    let pairings = journal.pairing_records()?;
    let mut last_commit = baseline_sequence;
    let mut seen_batch_ids = BTreeSet::new();
    for confirmed in &batches {
        if confirmed.receipt.commit_sequence <= last_commit
            || !seen_batch_ids.insert(&confirmed.prepared.batch_id)
            || confirmed.receipt.accepted_events <= 0
            || confirmed.receipt.changed_events < 0
            || confirmed.receipt.changed_events > confirmed.receipt.accepted_events
        {
            return Err(RecoveryError::Integrity(
                "ack journal conflicts with the adopted receipt sequence".to_owned(),
            ));
        }
        let batch: crate::Batch = serde_json::from_slice(&confirmed.raw).map_err(|_| {
            RecoveryError::Integrity("ack journal contains invalid raw batch JSON".to_owned())
        })?;
        crate::validate_batch(&batch).map_err(|_| {
            RecoveryError::Integrity("ack journal contains an invalid batch".to_owned())
        })?;
        if batch.batch_id != confirmed.prepared.batch_id
            || batch.device_id != confirmed.prepared.device_id
            || hex::encode(Sha256::digest(&confirmed.raw)) != confirmed.prepared.content_hash
            || i64::try_from(batch.events.len()).ok() != Some(confirmed.receipt.accepted_events)
        {
            return Err(RecoveryError::Integrity(
                "ack journal raw bytes do not match their confirmed receipt".to_owned(),
            ));
        }
        last_commit = confirmed.receipt.commit_sequence;
    }
    Ok(AckReplayPlan {
        journal_id: anchor.journal_id,
        adoption_head_sha256: anchor.adoption_head_sha256,
        receipt_inventory_sha256: inventory,
        baseline_sequence,
        batches,
        pairings,
    })
}

fn replay_confirmed_batches(
    health: &mut Connection,
    control: &ControlStore,
    plan: &AckReplayPlan,
) -> RecoveryResult<usize> {
    let mut confirmed_pairings = BTreeMap::new();
    for record in &plan.pairings {
        if !record.confirmed {
            continue;
        }
        let id = &record.prepared.device_id;
        if confirmed_pairings
            .insert(id.clone(), &record.prepared)
            .is_some()
        {
            return Err(RecoveryError::Integrity(format!(
                "device {id} has multiple pairing identities; automatic replay is unsafe"
            )));
        }
    }
    let tx = health.transaction_with_behavior(TransactionBehavior::Immediate)?;
    for (device_id, pairing) in &confirmed_pairings {
        if control.device_erasure_tombstoned(device_id)? {
            continue;
        }
        let existing: Option<(String, String)> = tx
            .query_row(
                "SELECT token_hash,created_at FROM devices WHERE device_id=?1",
                [device_id],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .optional()?;
        match existing {
            Some((token_hash, created_at))
                if token_hash == pairing.token_hash && created_at == pairing.paired_at => {}
            Some(_) => {
                return Err(RecoveryError::Integrity(format!(
                    "confirmed pairing for device {device_id} conflicts with the snapshot"
                )));
            }
            None => {
                tx.execute(
                    "INSERT INTO devices(device_id,token_hash,created_at) VALUES (?1,?2,?3)",
                    params![device_id, pairing.token_hash, pairing.paired_at],
                )?;
            }
        }
    }
    let mut replayed = 0_usize;
    for confirmed in &plan.batches {
        let device_id = &confirmed.prepared.device_id;
        if control.device_erasure_tombstoned(device_id)? {
            // The control chain outranks older acknowledgements. Tombstone
            // replay below also removes any copy present in the snapshot.
            continue;
        }
        let original: Option<(String, String, i64, i64, i64, i64, String)> = tx
            .query_row(
                "SELECT device_id,content_hash,commit_sequence,accepted_events,changed_events,requires_projection,received_at
                 FROM receipts WHERE batch_id=?1",
                [&confirmed.prepared.batch_id],
                |row| {
                    Ok((
                        row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?,
                        row.get(4)?, row.get(5)?, row.get(6)?,
                    ))
                },
            )
            .optional()?;
        if let Some((device, hash, sequence, accepted, changed, projection, received_at)) = original
        {
            if device != *device_id
                || hash != confirmed.prepared.content_hash
                || sequence != confirmed.receipt.commit_sequence
                || accepted != confirmed.receipt.accepted_events
                || changed != confirmed.receipt.changed_events
                || (projection != 0) != confirmed.receipt.requires_projection
                || received_at != confirmed.receipt.received_at
            {
                return Err(RecoveryError::Integrity(format!(
                    "confirmed batch {} conflicts with staged receipt",
                    confirmed.prepared.batch_id
                )));
            }
            continue;
        }
        let device: Option<String> = tx
            .query_row(
                "SELECT token_hash FROM devices WHERE device_id=?1",
                [device_id],
                |row| row.get(0),
            )
            .optional()?;
        if let Some(pairing) = confirmed_pairings.get(device_id) {
            if device
                .as_ref()
                .is_some_and(|hash| hash != &pairing.token_hash)
            {
                return Err(RecoveryError::Integrity(format!(
                    "confirmed pairing for device {device_id} conflicts with the snapshot"
                )));
            }
            if device.is_none() {
                tx.execute(
                    "INSERT INTO devices(device_id,token_hash,created_at) VALUES (?1,?2,?3)",
                    params![device_id, pairing.token_hash, pairing.paired_at],
                )?;
            }
        } else if device.is_none() {
            return Err(RecoveryError::Integrity(format!(
                "confirmed batch {} has no recoverable pairing identity",
                confirmed.prepared.batch_id
            )));
        }
        let batch: crate::Batch = serde_json::from_slice(&confirmed.raw)?;
        let mut changed = 0_i64;
        let mut metrics = BTreeSet::new();
        for event in &batch.events {
            let (saved, touched) =
                crate::save_event(&tx, device_id, event, &confirmed.receipt.received_at).map_err(
                    |_| {
                        RecoveryError::Integrity(format!(
                            "confirmed batch {} conflicts with staged event revision",
                            confirmed.prepared.batch_id
                        ))
                    },
                )?;
            if saved {
                changed += 1;
            }
            metrics.extend(touched);
        }
        if changed != confirmed.receipt.changed_events
            || metrics.is_empty() == confirmed.receipt.requires_projection
        {
            return Err(RecoveryError::Integrity(format!(
                "confirmed batch {} has different revision or projection semantics",
                confirmed.prepared.batch_id
            )));
        }
        let projected_at = (!confirmed.receipt.requires_projection)
            .then_some(confirmed.receipt.received_at.as_str());
        tx.execute(
            "INSERT INTO receipts(commit_sequence,batch_id,device_id,content_hash,accepted_events,changed_events,requires_projection,received_at,projected_at)
             VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9)",
            params![
                confirmed.receipt.commit_sequence,
                confirmed.prepared.batch_id,
                device_id,
                confirmed.prepared.content_hash,
                confirmed.receipt.accepted_events,
                confirmed.receipt.changed_events,
                confirmed.receipt.requires_projection as i64,
                confirmed.receipt.received_at,
                projected_at,
            ],
        )?;
        for metric in metrics {
            tx.execute(
                "INSERT INTO outbox(device_id,batch_id,metric_name,created_at) VALUES (?1,?2,?3,?4)",
                params![device_id, confirmed.prepared.batch_id, metric, confirmed.receipt.received_at],
            )?;
        }
        tx.execute(
            "INSERT INTO audit(device_id,action,batch_id,at,detail) VALUES (?1,'batch_committed',?2,?3,?4)",
            params![
                device_id,
                confirmed.prepared.batch_id,
                confirmed.receipt.received_at,
                format!(
                    "accepted={};changed={changed}",
                    confirmed.receipt.accepted_events
                ),
            ],
        )?;
        replayed += 1;
    }
    tx.commit()?;
    Ok(replayed)
}

pub fn verify_ack_coverage_evidence(
    staged: &StagedControl,
    health_backup: &Path,
    manifest: &HealthBackupManifest,
    journal: &AckJournal,
    expected_health_sha256: &str,
) -> RecoveryResult<AckCoverageEvidence> {
    let plan = verified_ack_replay_plan(health_backup, manifest, &staged.store, journal)?;
    let path = staged
        .staging_root
        .join("control")
        .join(RESTORE_COVERAGE_EVIDENCE_NAME);
    validate_regular_single_link(&path)?;
    let evidence: AckCoverageEvidence = serde_json::from_slice(&fs::read(&path)?)?;
    let health_path = staged.staging_root.join("data/health.db");
    verify_health_database(&health_path, Some(&staged.store.store_id()?))?;
    let health_sha = stable_staged_health_hash(&staged.staging_root, &health_path)?;
    if evidence.format_version != 1
        || evidence.snapshot_id != manifest.snapshot_id
        || evidence.snapshot_sha256 != manifest.file_sha256
        || evidence.receipt_inventory_sha256 != plan.receipt_inventory_sha256
        || evidence.journal_id != plan.journal_id
        || evidence.adoption_head_sha256 != plan.adoption_head_sha256
        || evidence.confirmed_batch_count != plan.batches.len()
        || evidence.highest_confirmed_commit_sequence
            != plan.batches.last().map_or(plan.baseline_sequence, |batch| {
                batch.receipt.commit_sequence
            })
        || evidence.staged_health_sha256 != health_sha
        || evidence.staged_health_sha256 != expected_health_sha256
    {
        return Err(RecoveryError::Integrity(
            "durable ack coverage does not bind current snapshot, journal and staged health"
                .to_owned(),
        ));
    }
    let health = Connection::open_with_flags(&health_path, OpenFlags::SQLITE_OPEN_READ_ONLY)?;
    for confirmed in &plan.batches {
        let erased = staged
            .store
            .device_erasure_tombstoned(&confirmed.prepared.device_id)?;
        let row: Option<(String, String, i64, i64, i64, i64, String)> = health
            .query_row(
                "SELECT device_id,content_hash,commit_sequence,accepted_events,changed_events,requires_projection,received_at
                 FROM receipts WHERE batch_id=?1",
                [&confirmed.prepared.batch_id],
                |row| {
                    Ok((
                        row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?,
                        row.get(4)?, row.get(5)?, row.get(6)?,
                    ))
                },
            )
            .optional()?;
        if erased {
            if row.is_some() {
                return Err(RecoveryError::Integrity(format!(
                    "erased device receipt {} revived in candidate",
                    confirmed.prepared.batch_id
                )));
            }
            continue;
        }
        let expected = (
            confirmed.prepared.device_id.clone(),
            confirmed.prepared.content_hash.clone(),
            confirmed.receipt.commit_sequence,
            confirmed.receipt.accepted_events,
            confirmed.receipt.changed_events,
            confirmed.receipt.requires_projection as i64,
            confirmed.receipt.received_at.clone(),
        );
        if row != Some(expected) {
            return Err(RecoveryError::Integrity(format!(
                "confirmed receipt {} is missing or differs from journal",
                confirmed.prepared.batch_id
            )));
        }
    }
    verify_candidate_record_inventory(health_backup, &health, &staged.store, &plan)?;
    Ok(evidence)
}

fn verify_candidate_record_inventory(
    snapshot_path: &Path,
    candidate: &Connection,
    control: &ControlStore,
    plan: &AckReplayPlan,
) -> RecoveryResult<()> {
    let snapshot = Connection::open_with_flags(snapshot_path, OpenFlags::SQLITE_OPEN_READ_ONLY)?;
    let mut erasures = BTreeMap::<String, bool>::new();
    let mut erased = |device: &str| -> RecoveryResult<bool> {
        if let Some(value) = erasures.get(device) {
            return Ok(*value);
        }
        let value = control.device_erasure_tombstoned(device)?;
        erasures.insert(device.to_owned(), value);
        Ok(value)
    };
    let mut expected_receipts =
        BTreeMap::<String, (String, String, i64, i64, i64, i64, String)>::new();
    {
        let mut statement = snapshot.prepare(
            "SELECT batch_id,device_id,content_hash,commit_sequence,accepted_events,changed_events,requires_projection,received_at FROM receipts",
        )?;
        let mut rows = statement.query([])?;
        while let Some(row) = rows.next()? {
            let batch_id: String = row.get(0)?;
            let device_id: String = row.get(1)?;
            if !erased(&device_id)? {
                expected_receipts.insert(
                    batch_id,
                    (
                        device_id,
                        row.get(2)?,
                        row.get(3)?,
                        row.get(4)?,
                        row.get(5)?,
                        row.get(6)?,
                        row.get(7)?,
                    ),
                );
            }
        }
    }
    for confirmed in &plan.batches {
        if erased(&confirmed.prepared.device_id)? {
            continue;
        }
        if expected_receipts
            .insert(
                confirmed.prepared.batch_id.clone(),
                (
                    confirmed.prepared.device_id.clone(),
                    confirmed.prepared.content_hash.clone(),
                    confirmed.receipt.commit_sequence,
                    confirmed.receipt.accepted_events,
                    confirmed.receipt.changed_events,
                    confirmed.receipt.requires_projection as i64,
                    confirmed.receipt.received_at.clone(),
                ),
            )
            .is_some()
        {
            return Err(RecoveryError::Integrity(
                "ack journal reused a baseline snapshot batch ID".to_owned(),
            ));
        }
    }
    let mut actual_receipts = BTreeMap::new();
    {
        let mut statement = candidate.prepare(
            "SELECT batch_id,device_id,content_hash,commit_sequence,accepted_events,changed_events,requires_projection,received_at FROM receipts",
        )?;
        let mut rows = statement.query([])?;
        while let Some(row) = rows.next()? {
            actual_receipts.insert(
                row.get::<_, String>(0)?,
                (
                    row.get(1)?,
                    row.get(2)?,
                    row.get(3)?,
                    row.get(4)?,
                    row.get(5)?,
                    row.get(6)?,
                    row.get(7)?,
                ),
            );
        }
    }
    if actual_receipts != expected_receipts {
        return Err(RecoveryError::Integrity(
            "candidate receipt inventory differs from snapshot plus confirmed journal batches"
                .to_owned(),
        ));
    }
    let mut expected_events = BTreeMap::<(String, String), (i64, String, String)>::new();
    {
        let mut statement = snapshot
            .prepare("SELECT device_id,event_id,revision,payload_hash,payload_json FROM events")?;
        let mut rows = statement.query([])?;
        while let Some(row) = rows.next()? {
            let device_id: String = row.get(0)?;
            if !erased(&device_id)? {
                expected_events.insert(
                    (device_id, row.get(1)?),
                    (row.get(2)?, row.get(3)?, row.get(4)?),
                );
            }
        }
    }
    for confirmed in &plan.batches {
        if erased(&confirmed.prepared.device_id)? {
            continue;
        }
        let batch: crate::Batch = serde_json::from_slice(&confirmed.raw)?;
        for event in &batch.events {
            let bytes = serde_json::to_vec(event)?;
            let hash = hex::encode(Sha256::digest(&bytes));
            let key = (batch.device_id.clone(), event.event_id.clone());
            if let Some((revision, previous_hash, _)) = expected_events.get(&key) {
                if event.revision < *revision {
                    continue;
                }
                if event.revision == *revision {
                    if hash != *previous_hash {
                        return Err(RecoveryError::Integrity(
                            "confirmed event revision conflicts with baseline".to_owned(),
                        ));
                    }
                    continue;
                }
            }
            expected_events.insert(
                key,
                (
                    event.revision,
                    hash,
                    String::from_utf8(bytes).map_err(|_| {
                        RecoveryError::Integrity("event JSON is not UTF-8".to_owned())
                    })?,
                ),
            );
        }
    }
    let mut actual_events = BTreeMap::new();
    {
        let mut statement = candidate
            .prepare("SELECT device_id,event_id,revision,payload_hash,payload_json FROM events")?;
        let mut rows = statement.query([])?;
        while let Some(row) = rows.next()? {
            actual_events.insert(
                (row.get::<_, String>(0)?, row.get::<_, String>(1)?),
                (row.get(2)?, row.get(3)?, row.get(4)?),
            );
        }
    }
    if actual_events != expected_events {
        return Err(RecoveryError::Integrity(
            "candidate event revisions differ from snapshot plus confirmed journal batches"
                .to_owned(),
        ));
    }
    let mut expected_devices = BTreeMap::<String, (String, String)>::new();
    {
        let mut statement =
            snapshot.prepare("SELECT device_id,token_hash,created_at FROM devices")?;
        let mut rows = statement.query([])?;
        while let Some(row) = rows.next()? {
            let device_id: String = row.get(0)?;
            if !erased(&device_id)? {
                expected_devices.insert(device_id, (row.get(1)?, row.get(2)?));
            }
        }
    }
    for record in &plan.pairings {
        if !record.confirmed || erased(&record.prepared.device_id)? {
            continue;
        }
        let expected = (
            record.prepared.token_hash.clone(),
            record.prepared.paired_at.clone(),
        );
        match expected_devices.get(&record.prepared.device_id) {
            Some(existing) if existing != &expected => {
                return Err(RecoveryError::Integrity(
                    "confirmed pairing conflicts with baseline device identity".to_owned(),
                ));
            }
            Some(_) => {}
            None => {
                expected_devices.insert(record.prepared.device_id.clone(), expected);
            }
        }
    }
    let mut actual_devices = BTreeMap::new();
    {
        let mut statement =
            candidate.prepare("SELECT device_id,token_hash,created_at,revoked_at FROM devices")?;
        let mut rows = statement.query([])?;
        while let Some(row) = rows.next()? {
            let id: String = row.get(0)?;
            let token_hash: String = row.get(1)?;
            let created_at: String = row.get(2)?;
            let revoked_at: Option<String> = row.get(3)?;
            if control.token_tombstoned(&token_hash)? && revoked_at.is_none() {
                return Err(RecoveryError::Integrity(
                    "revoked token is active in the candidate".to_owned(),
                ));
            }
            actual_devices.insert(id, (token_hash, created_at));
        }
    }
    if actual_devices != expected_devices {
        return Err(RecoveryError::Integrity(
            "candidate device identities differ from snapshot plus confirmed pairings".to_owned(),
        ));
    }
    Ok(())
}

/// Copies already verified staging data to *new* final-volume generation
/// paths. Neither destination may be a running database. A partial copy is
/// inert and can be retried only if every pre-existing artifact still matches.
/// This does not publish an active pointer, start VM, or mark restore complete.
#[allow(clippy::too_many_arguments)]
pub fn materialize_final_candidate(
    staged: &StagedControl,
    health_manifest: &HealthBackupManifest,
    completion: &RestoreCompletionIntent,
    required_confirmed_commit_sequence: i64,
    expected_control_checkpoint: &ControlCheckpoint,
    final_health_db: &Path,
    final_control_dir: &Path,
    coordinator_dir: &Path,
) -> RecoveryResult<MaterializedCandidate> {
    let _ = (
        staged,
        health_manifest,
        completion,
        required_confirmed_commit_sequence,
        expected_control_checkpoint,
        final_health_db,
        final_control_dir,
        coordinator_dir,
    );
    Err(RecoveryError::Integrity(
        "sequence-only candidate materialization is disabled; use independent ack coverage"
            .to_owned(),
    ))
}

/// Materialization is admitted only after re-reading the independent journal
/// and the original adoption snapshot. Neither a caller-supplied sequence nor
/// a JSON file in staging is accepted as a coverage proof by itself.
#[allow(clippy::too_many_arguments)]
pub fn materialize_final_candidate_with_ack(
    staged: &StagedControl,
    health_backup: &Path,
    health_manifest: &HealthBackupManifest,
    journal: &AckJournal,
    completion: &RestoreCompletionIntent,
    expected_control_checkpoint: &ControlCheckpoint,
    final_health_db: &Path,
    final_control_dir: &Path,
    coordinator_dir: &Path,
) -> RecoveryResult<MaterializedCandidate> {
    verify_ack_coverage_evidence(
        staged,
        health_backup,
        health_manifest,
        journal,
        &completion.health_sha256,
    )?;
    let coverage_path = staged
        .staging_root
        .join("control")
        .join(RESTORE_COVERAGE_EVIDENCE_NAME);
    if completion.ack_coverage_evidence_sha256.as_deref()
        != Some(file_sha256(&coverage_path)?.as_str())
    {
        return Err(RecoveryError::Integrity(
            "completion intent does not bind acknowledged-write coverage".to_owned(),
        ));
    }
    if completion.format_version != RESTORE_SESSION_FORMAT_VERSION
        || completion.snapshot_id != health_manifest.snapshot_id
    {
        return Err(RecoveryError::Integrity(
            "completion intent does not match the selected health snapshot".to_owned(),
        ));
    }
    validate_sha256(&completion.health_sha256, "completed health SHA-256")?;
    let completion_path = staged
        .staging_root
        .join("control")
        .join(RESTORE_COMPLETION_INTENT_NAME);
    validate_regular_single_link(&completion_path)?;
    let persisted: RestoreCompletionIntent = serde_json::from_slice(&fs::read(completion_path)?)?;
    if &persisted != completion {
        return Err(RecoveryError::Integrity(
            "completion intent differs from its durable staging record".to_owned(),
        ));
    }
    let _persisted_projection = staged_projection_evidence(staged)?;
    if file_sha256(
        &staged
            .staging_root
            .join("control")
            .join(RESTORE_PROJECTION_EVIDENCE_NAME),
    )? != completion.projection_evidence_sha256
    {
        return Err(RecoveryError::Integrity(
            "projection evidence artifact differs from its completion intent".to_owned(),
        ));
    }
    staged.store.verify()?;
    if staged.store.checkpoint()? != *expected_control_checkpoint
        || !staged
            .store
            .contains_checkpoint(&completion.control_checkpoint_before_completion)?
    {
        return Err(RecoveryError::Integrity(
            "staged control head differs from independently expected completion head".to_owned(),
        ));
    }
    let staged_health = staged.staging_root.join("data/health.db");
    verify_health_database(&staged_health, Some(&staged.store.store_id()?))?;
    if stable_staged_health_hash(&staged.staging_root, &staged_health)? != completion.health_sha256
    {
        return Err(RecoveryError::Integrity(
            "staged health bytes changed after projection completion intent".to_owned(),
        ));
    }
    reject_sqlite_sidecars(&staged_health)?;
    validate_final_candidate_paths(
        &staged.staging_root,
        final_health_db,
        final_control_dir,
        coordinator_dir,
    )?;
    let health_parent = final_health_db.parent().ok_or_else(|| {
        RecoveryError::InvalidPath("final health candidate has no parent".to_owned())
    })?;
    if fs::symlink_metadata(final_health_db).is_ok() {
        validate_regular_single_link(final_health_db)?;
        reject_sqlite_sidecars(final_health_db)?;
        if file_sha256(final_health_db)? != completion.health_sha256 {
            return Err(RecoveryError::Integrity(
                "pre-existing final health candidate differs from staged evidence".to_owned(),
            ));
        }
    } else {
        let temp = health_parent.join(format!(".health-candidate-{}.tmp", uuid::Uuid::new_v4()));
        copy_regular_file(&staged_health, &temp)?;
        if file_sha256(&temp)? != completion.health_sha256 {
            return Err(RecoveryError::Integrity(
                "copied final health candidate differs from staged evidence".to_owned(),
            ));
        }
        fs::rename(&temp, final_health_db)?;
        sync_directory(health_parent)?;
    }
    let control_parent = final_control_dir.parent().ok_or_else(|| {
        RecoveryError::InvalidPath("final control candidate has no parent".to_owned())
    })?;
    if fs::symlink_metadata(final_control_dir).is_err() {
        let temp = control_parent.join(format!(".control-candidate-{}.tmp", uuid::Uuid::new_v4()));
        create_private_directory(&temp)?;
        let copied = (|| -> RecoveryResult<()> {
            let mirror = temp.join(CONTROL_MIRROR_DIRECTORY);
            create_private_directory(&mirror)?;
            sqlite_snapshot(staged.store.db_path(), &temp.join(CONTROL_DATABASE_NAME))?;
            copy_regular_file(staged.store.head_path(), &temp.join(CONTROL_HEAD_NAME))?;
            let mirrors = exact_source_mirror_paths(&staged.store, expected_control_checkpoint)?;
            for source in mirrors {
                let name = source.file_name().ok_or_else(|| {
                    RecoveryError::InvalidArtifact("control mirror has no filename".to_owned())
                })?;
                copy_regular_file(&source, &mirror.join(name))?;
            }
            sync_directory(&mirror)?;
            sync_directory(&temp)?;
            let copied_store = ControlStore::new(temp.join(CONTROL_DATABASE_NAME), mirror)?;
            copied_store.verify()?;
            if copied_store.checkpoint()? != *expected_control_checkpoint {
                return Err(RecoveryError::Integrity(
                    "copied control candidate has a different head".to_owned(),
                ));
            }
            Ok(())
        })();
        if let Err(error) = copied {
            // This temp was created by this invocation and never published.
            let _ = fs::remove_dir_all(&temp);
            return Err(error);
        }
        fs::rename(&temp, final_control_dir)?;
        sync_directory(control_parent)?;
    }
    reject_symlink_or_non_directory(final_control_dir)?;
    let final_store = ControlStore::new(
        final_control_dir.join(CONTROL_DATABASE_NAME),
        final_control_dir.join(CONTROL_MIRROR_DIRECTORY),
    )?;
    final_store.verify()?;
    if final_store.checkpoint()? != *expected_control_checkpoint {
        return Err(RecoveryError::Integrity(
            "final control candidate changed during materialization".to_owned(),
        ));
    }
    verify_health_database(final_health_db, Some(&final_store.store_id()?))?;
    if file_sha256(final_health_db)? != completion.health_sha256 {
        return Err(RecoveryError::Integrity(
            "final health candidate changed during verification".to_owned(),
        ));
    }
    staged.store.verify()?;
    if staged.store.checkpoint()? != *expected_control_checkpoint {
        return Err(RecoveryError::Integrity(
            "staged control head advanced during candidate materialization".to_owned(),
        ));
    }
    Ok(MaterializedCandidate {
        health_db: final_health_db.to_path_buf(),
        control_store: final_store,
        health_sha256: completion.health_sha256.clone(),
        control_checkpoint: expected_control_checkpoint.clone(),
    })
}

/// Refuses to persist a completion intent until this process has independently
/// rebuilt the oracle from the staged health database and read back the whole
/// isolated native VM namespace. The caller cannot turn an arbitrary digest
/// into completed recovery evidence.
pub async fn prepare_restore_completion(
    staged: &StagedControl,
    target: &crate::projection::VmTarget,
    projection_evidence: &crate::projection::ProjectionEvidence,
    expected_health_sha256: &str,
) -> RecoveryResult<RestoreCompletionIntent> {
    let _ = (staged, target, projection_evidence, expected_health_sha256);
    Err(RecoveryError::Integrity(
        "completion without independent ack coverage is disabled".to_owned(),
    ))
}

pub async fn prepare_restore_completion_with_ack(
    staged: &StagedControl,
    health_backup: &Path,
    health_manifest: &HealthBackupManifest,
    journal: &AckJournal,
    target: &crate::projection::VmTarget,
    projection_evidence: &crate::projection::ProjectionEvidence,
    expected_health_sha256: &str,
) -> RecoveryResult<RestoreCompletionIntent> {
    verify_ack_coverage_evidence(
        staged,
        health_backup,
        health_manifest,
        journal,
        expected_health_sha256,
    )?;
    let evidence_path = staged
        .staging_root
        .join("control")
        .join(RESTORE_PROJECTION_EVIDENCE_NAME);
    let effective_evidence = match fs::symlink_metadata(&evidence_path) {
        Ok(_) => {
            let persisted = staged_projection_evidence(staged)?;
            let mut same_content = persisted.clone();
            same_content.verified_at = projection_evidence.verified_at.clone();
            if same_content != *projection_evidence {
                return Err(RecoveryError::Integrity(
                    "retry projection evidence differs from its durable artifact".to_owned(),
                ));
            }
            persisted
        }
        Err(error) if error.kind() == io::ErrorKind::NotFound => projection_evidence.clone(),
        Err(error) => return Err(error.into()),
    };
    let health_db = staged.staging_root.join("data/health.db");
    verify_health_database(&health_db, Some(&staged.store.store_id()?))?;
    let connection = open_health_database(&health_db)?;
    crate::projection::verify_staging_readback(&connection, target, &effective_evidence)
        .await
        .map_err(RecoveryError::Integrity)?;
    drop(connection);
    persist_restore_completion_intent(staged, &effective_evidence, expected_health_sha256)
}

pub fn staged_projection_evidence(
    staged: &StagedControl,
) -> RecoveryResult<crate::projection::ProjectionEvidence> {
    let control_dir = staged.staging_root.join("control");
    reject_symlink_or_non_directory(&control_dir)?;
    if staged.store.db_path() != control_dir.join(CONTROL_DATABASE_NAME) {
        return Err(RecoveryError::InvalidPath(
            "projection evidence control store is outside the staging root".to_owned(),
        ));
    }
    let path = control_dir.join(RESTORE_PROJECTION_EVIDENCE_NAME);
    validate_regular_single_link(&path)?;
    Ok(serde_json::from_slice(&fs::read(path)?)?)
}

fn persist_restore_completion_intent(
    staged: &StagedControl,
    projection_evidence: &crate::projection::ProjectionEvidence,
    expected_health_sha256: &str,
) -> RecoveryResult<RestoreCompletionIntent> {
    validate_sha256(expected_health_sha256, "completed health SHA-256")?;
    validate_sha256(
        &projection_evidence.oracle_sha256,
        "projection oracle SHA-256",
    )?;
    validate_sha256(
        &projection_evidence.binary_sha256,
        "native VM binary SHA-256",
    )?;
    validate_sha256(
        &projection_evidence.full_readback_sha256,
        "native VM full export SHA-256",
    )?;
    if projection_evidence.full_readback_sha256 != projection_evidence.oracle_sha256 {
        return Err(RecoveryError::Integrity(
            "full VM readback digest differs from the staged health oracle".to_owned(),
        ));
    }
    validate_uuid(&projection_evidence.generation_id, "VM generation ID")?;
    validate_rfc3339(
        &projection_evidence.verified_at,
        "projection verification time",
    )?;
    if projection_evidence.mapping_version <= 0 {
        return Err(RecoveryError::InvalidArtifact(
            "projection mapping version is invalid".to_owned(),
        ));
    }
    let control_dir = staged.staging_root.join("control");
    reject_symlink_or_non_directory(&control_dir)?;
    if staged.store.db_path() != control_dir.join(CONTROL_DATABASE_NAME) {
        return Err(RecoveryError::InvalidPath(
            "completion control store is not inside the selected staging root".to_owned(),
        ));
    }
    staged.store.verify()?;
    let session = read_restore_session(&session_path(&staged.staging_root))?;
    if session.format_version != RESTORE_SESSION_FORMAT_VERSION
        || session.phase != RestorePhase::ProjectionRebuildRequired
    {
        return Err(RecoveryError::Integrity(
            "restore has not reached the projection verification boundary".to_owned(),
        ));
    }
    let restore_epoch = session
        .restore_epoch
        .ok_or_else(|| RecoveryError::Integrity("restore session lacks its epoch".to_owned()))?;
    let snapshot_id = session.health_snapshot_id.ok_or_else(|| {
        RecoveryError::Integrity("restore session lacks its snapshot ID".to_owned())
    })?;
    let health_db = staged.staging_root.join("data/health.db");
    verify_health_database(&health_db, Some(&staged.store.store_id()?))?;
    if stable_staged_health_hash(&staged.staging_root, &health_db)? != expected_health_sha256 {
        return Err(RecoveryError::Integrity(
            "completed health bytes differ from the supplied readback binding".to_owned(),
        ));
    }
    let evidence_bytes = serde_json::to_vec(projection_evidence)?;
    let evidence_sha256 = hex::encode(Sha256::digest(&evidence_bytes));
    let coverage_path = control_dir.join(RESTORE_COVERAGE_EVIDENCE_NAME);
    let coverage_sha256 = match fs::symlink_metadata(&coverage_path) {
        Ok(_) => {
            validate_regular_single_link(&coverage_path)?;
            Some(file_sha256(&coverage_path)?)
        }
        Err(error) if error.kind() == io::ErrorKind::NotFound => None,
        Err(error) => return Err(error.into()),
    };
    let evidence_path = control_dir.join(RESTORE_PROJECTION_EVIDENCE_NAME);
    match fs::symlink_metadata(&evidence_path) {
        Ok(_) => {
            validate_regular_single_link(&evidence_path)?;
            if fs::read(&evidence_path)? != evidence_bytes {
                return Err(RecoveryError::Integrity(
                    "durable projection evidence differs from verified readback".to_owned(),
                ));
            }
        }
        Err(error) if error.kind() == io::ErrorKind::NotFound => {
            write_new_json(&evidence_path, projection_evidence)?;
            sync_directory(&control_dir)?;
        }
        Err(error) => return Err(error.into()),
    }
    let path = control_dir.join(RESTORE_COMPLETION_INTENT_NAME);
    match fs::symlink_metadata(&path) {
        Ok(_) => {
            validate_regular_single_link(&path)?;
            let existing: RestoreCompletionIntent = serde_json::from_slice(&fs::read(path)?)?;
            if existing.format_version != RESTORE_SESSION_FORMAT_VERSION
                || existing.restore_epoch != restore_epoch
                || existing.snapshot_id != snapshot_id
                || existing.health_sha256 != expected_health_sha256
                || existing.projection_evidence_sha256 != evidence_sha256
                || existing.ack_coverage_evidence_sha256 != coverage_sha256
                || !staged
                    .store
                    .contains_checkpoint(&existing.control_checkpoint_before_completion)?
            {
                return Err(RecoveryError::Integrity(
                    "existing restore completion intent differs from verified evidence".to_owned(),
                ));
            }
            Ok(existing)
        }
        Err(error) if error.kind() == io::ErrorKind::NotFound => {
            let intent = RestoreCompletionIntent {
                format_version: RESTORE_SESSION_FORMAT_VERSION,
                restore_epoch,
                snapshot_id,
                completed_at: Utc::now().to_rfc3339(),
                health_sha256: expected_health_sha256.to_owned(),
                projection_evidence_sha256: evidence_sha256,
                ack_coverage_evidence_sha256: coverage_sha256,
                control_checkpoint_before_completion: staged.store.checkpoint()?,
            };
            write_new_json(&path, &intent)?;
            sync_directory(&control_dir)?;
            Ok(intent)
        }
        Err(error) => Err(error.into()),
    }
}

/// Creates an immutable, self-verifying control bundle in a new destination
/// directory. The caller remains responsible for placing the destination on
/// the approved independent encrypted recovery domain.
pub fn backup_control(
    store: &ControlStore,
    destination: &Path,
) -> RecoveryResult<ControlBackupManifest> {
    validate_new_destination(destination)?;
    reject_destination_near_control_authority(store, destination)?;
    store.verify()?;
    let checkpoint_before = store.checkpoint()?;
    let source_mirrors = exact_source_mirror_paths(store, &checkpoint_before)?;
    let parent = destination.parent().ok_or_else(|| {
        RecoveryError::InvalidPath("control backup destination has no parent".to_owned())
    })?;
    let temp = parent.join(format!(".boaz-control-backup-{}.tmp", uuid::Uuid::new_v4()));
    create_private_directory(&temp)?;
    let result = (|| -> RecoveryResult<ControlBackupManifest> {
        let mirror_destination = temp.join(CONTROL_MIRROR_DIRECTORY);
        create_private_directory(&mirror_destination)?;

        let database_destination = temp.join(CONTROL_DATABASE_NAME);
        sqlite_snapshot(store.db_path(), &database_destination)?;
        let head_destination = temp.join(CONTROL_HEAD_NAME);
        copy_regular_file(store.head_path(), &head_destination)?;

        let mut mirror_artifacts = Vec::with_capacity(source_mirrors.len());
        for source in source_mirrors {
            let name = source.file_name().ok_or_else(|| {
                RecoveryError::InvalidArtifact("control mirror has no filename".to_owned())
            })?;
            let target = mirror_destination.join(name);
            copy_regular_file(&source, &target)?;
            mirror_artifacts.push(artifact_for(&temp, &target)?);
        }
        mirror_artifacts.sort_by(|left, right| left.relative_path.cmp(&right.relative_path));

        let copied_store = ControlStore::new(database_destination.clone(), mirror_destination)?;
        copied_store.verify()?;
        if copied_store.checkpoint()? != checkpoint_before {
            return Err(RecoveryError::Integrity(
                "copied control tail differs from the source checkpoint".to_owned(),
            ));
        }
        let manifest = ControlBackupManifest {
            format_version: CONTROL_BACKUP_FORMAT_VERSION,
            snapshot_id: uuid::Uuid::new_v4().to_string(),
            created_at: Utc::now().to_rfc3339(),
            checkpoint: checkpoint_before.clone(),
            database: artifact_for(&temp, &database_destination)?,
            head: artifact_for(&temp, &head_destination)?,
            mirrors: mirror_artifacts,
        };
        validate_control_manifest_shape(&manifest)?;
        write_new_json(&temp.join(CONTROL_MANIFEST_NAME), &manifest)?;
        sync_directory(&temp)?;

        // A concurrent control append is safe only if it starts after this
        // second checkpoint. A change during the SQLite/head/mirror snapshot
        // invalidates the attempt rather than publishing a mixed generation.
        store.verify()?;
        if store.checkpoint()? != checkpoint_before {
            return Err(RecoveryError::Integrity(
                "control authority changed while its backup was being built".to_owned(),
            ));
        }
        validate_control_bundle(&temp, Some(&checkpoint_before))?;
        fs::rename(&temp, destination)?;
        sync_directory(parent)?;
        Ok(manifest)
    })();
    if result.is_err() {
        let _ = fs::remove_dir_all(&temp);
    }
    result
}

/// Restores a verified control bundle only into a new or empty staging root.
/// `expected_head` must come from an independently supplied authority; this
/// function never substitutes the bundle's own head as the expected value.
pub fn restore_control_to_staging(
    bundle: &Path,
    staging_root: &Path,
    expected_head: &ControlCheckpoint,
    live_paths: &[PathBuf],
) -> RecoveryResult<StagedControl> {
    let mut forbidden = live_paths.to_vec();
    forbidden.push(bundle.to_path_buf());
    validate_staging_root(staging_root, &forbidden, true)?;
    let manifest = validate_control_bundle(bundle, Some(expected_head))?;
    if &manifest.checkpoint != expected_head {
        return Err(RecoveryError::Integrity(
            "external expected control head does not exactly match the bundle tail".to_owned(),
        ));
    }
    ensure_empty_staging_root(staging_root)?;
    let temp = staging_root.join(format!(".control-restore-{}.tmp", uuid::Uuid::new_v4()));
    create_private_directory(&temp)?;
    let result = (|| -> RecoveryResult<StagedControl> {
        let mirror = temp.join(CONTROL_MIRROR_DIRECTORY);
        create_private_directory(&mirror)?;
        copy_regular_file(
            &bundle.join(CONTROL_DATABASE_NAME),
            &temp.join(CONTROL_DATABASE_NAME),
        )?;
        copy_regular_file(
            &bundle.join(CONTROL_HEAD_NAME),
            &temp.join(CONTROL_HEAD_NAME),
        )?;
        for artifact in &manifest.mirrors {
            let source = safe_artifact_path(bundle, &artifact.relative_path)?;
            let target = safe_artifact_path(&temp, &artifact.relative_path)?;
            copy_regular_file(&source, &target)?;
        }
        for artifact in std::iter::once(&manifest.database)
            .chain(std::iter::once(&manifest.head))
            .chain(manifest.mirrors.iter())
        {
            verify_artifact(&temp, artifact)?;
        }
        let staged_store = ControlStore::new(temp.join(CONTROL_DATABASE_NAME), mirror)?;
        staged_store.verify()?;
        if staged_store.checkpoint()? != *expected_head {
            return Err(RecoveryError::Integrity(
                "staged control tail does not match the external expected head".to_owned(),
            ));
        }
        let session = RestoreSession {
            format_version: RESTORE_SESSION_FORMAT_VERSION,
            expected_control_checkpoint: expected_head.clone(),
            phase: RestorePhase::ControlStaged,
            restore_epoch: None,
            health_snapshot_id: None,
            health_backup_sha256: None,
            restore_started_at: None,
            restore_replayed_at: None,
            replay_summary: None,
            replayed_health_sha256: None,
            ack_replayed_batches: None,
        };
        write_new_json(&temp.join(RESTORE_SESSION_NAME), &session)?;
        sync_directory(&temp)?;
        let control_destination = staging_root.join("control");
        if control_destination.exists() {
            return Err(RecoveryError::InvalidPath(
                "staging control destination already exists".to_owned(),
            ));
        }
        fs::rename(&temp, &control_destination)?;
        sync_directory(staging_root)?;
        let store = ControlStore::new(
            control_destination.join(CONTROL_DATABASE_NAME),
            control_destination.join(CONTROL_MIRROR_DIRECTORY),
        )?;
        store.verify()?;
        Ok(StagedControl {
            staging_root: staging_root.to_path_buf(),
            store,
        })
    })();
    if result.is_err() {
        let _ = fs::remove_dir_all(&temp);
    }
    result
}

/// Copies and sanitizes a health backup in staging. It deliberately stops at
/// `ProjectionRebuildRequired`; only an isolated native metrics rebuild and
/// readback may later append `restore_completed`.
pub fn restore_health_to_staging(
    health_backup: &Path,
    health_manifest: &Path,
    staged: &StagedControl,
    live_paths: &[PathBuf],
) -> RecoveryResult<HealthRestoreState> {
    restore_health_to_staging_inner(health_backup, health_manifest, staged, live_paths, None)
}

/// The only staging entry point that can produce an acknowledged-write
/// coverage artifact. The legacy entry point above may inspect/replay control
/// facts, but it cannot support candidate materialization or activation.
pub fn restore_health_to_staging_with_ack(
    health_backup: &Path,
    health_manifest: &Path,
    staged: &StagedControl,
    live_paths: &[PathBuf],
    journal: &AckJournal,
) -> RecoveryResult<HealthRestoreState> {
    restore_health_to_staging_inner(
        health_backup,
        health_manifest,
        staged,
        live_paths,
        Some(journal),
    )
}

fn restore_health_to_staging_inner(
    health_backup: &Path,
    health_manifest: &Path,
    staged: &StagedControl,
    live_paths: &[PathBuf],
    journal: Option<&AckJournal>,
) -> RecoveryResult<HealthRestoreState> {
    validate_staging_root(&staged.staging_root, live_paths, false)?;
    let control_directory = staged.staging_root.join("control");
    reject_symlink_or_non_directory(&control_directory)?;
    if staged.store.db_path() != control_directory.join(CONTROL_DATABASE_NAME)
        || staged.store.head_path() != control_directory.join(CONTROL_HEAD_NAME)
        || staged.store.mirror_dir() != control_directory.join(CONTROL_MIRROR_DIRECTORY)
    {
        return Err(RecoveryError::InvalidPath(
            "staged control paths do not belong to the selected staging root".to_owned(),
        ));
    }
    reject_path_relationship(&staged.staging_root, health_backup)?;
    reject_path_relationship(&staged.staging_root, health_manifest)?;
    let top_level = directory_entry_names(&staged.staging_root)?;
    let allowed: BTreeSet<String> = ["control", "data"].into_iter().map(str::to_owned).collect();
    if !top_level.is_subset(&allowed) || !top_level.contains("control") {
        return Err(RecoveryError::InvalidPath(
            "staging root contains unexpected recovery entries".to_owned(),
        ));
    }
    staged.store.verify()?;
    let session_path = session_path(&staged.staging_root);
    let mut session = read_restore_session(&session_path)?;
    if session.format_version != RESTORE_SESSION_FORMAT_VERSION {
        return Err(RecoveryError::InvalidArtifact(
            "restore session format is unsupported".to_owned(),
        ));
    }
    if !staged
        .store
        .contains_checkpoint(&session.expected_control_checkpoint)?
    {
        return Err(RecoveryError::Integrity(
            "staged control no longer contains its externally expected base head".to_owned(),
        ));
    }
    let manifest = read_health_manifest(health_manifest)?;
    validate_health_backup(health_backup, &manifest, &staged.store)?;
    let ack_plan = journal
        .map(|journal| verified_ack_replay_plan(health_backup, &manifest, &staged.store, journal))
        .transpose()?;
    bind_or_verify_session_health(&mut session, &manifest)?;

    let data_destination = staged.staging_root.join("data");
    let health_destination = data_destination.join("health.db");
    if data_destination.exists() {
        reject_symlink_or_non_directory(&data_destination)?;
    }
    if !data_destination.exists() {
        let temp = staged
            .staging_root
            .join(format!(".health-restore-{}.tmp", uuid::Uuid::new_v4()));
        create_private_directory(&temp)?;
        let result = (|| -> RecoveryResult<()> {
            let target = temp.join("health.db");
            copy_regular_file(health_backup, &target)?;
            if file_sha256(&target)? != manifest.file_sha256 {
                return Err(RecoveryError::Integrity(
                    "copied health database hash differs from its manifest".to_owned(),
                ));
            }
            verify_health_database(&target, Some(&staged.store.store_id()?))?;
            sync_directory(&temp)?;
            fs::rename(&temp, &data_destination)?;
            sync_directory(&staged.staging_root)?;
            Ok(())
        })();
        if result.is_err() {
            let _ = fs::remove_dir_all(&temp);
        }
        result?;
        session.phase = RestorePhase::HealthCopied;
        write_replace_json(&session_path, &session)?;
    } else if matches!(session.phase, RestorePhase::ControlStaged) {
        validate_staged_health_file(&staged.staging_root, &health_destination)?;
        if file_sha256(&health_destination)? != manifest.file_sha256 {
            return Err(RecoveryError::Integrity(
                "pre-existing staged health database is not the selected backup".to_owned(),
            ));
        }
        session.phase = RestorePhase::HealthCopied;
        write_replace_json(&session_path, &session)?;
    }

    // A previous attempt may have left `data/health.db` behind. Never follow
    // an operator-supplied link (including a hard link to the backup) into a
    // file outside staging, regardless of the persisted restore phase.
    validate_staged_health_file(&staged.staging_root, &health_destination)?;

    let restore_epoch = session.restore_epoch.clone().ok_or_else(|| {
        RecoveryError::InvalidArtifact("restore session lacks its epoch".to_owned())
    })?;
    let started_at = session.restore_started_at.clone().ok_or_else(|| {
        RecoveryError::InvalidArtifact("restore session lacks its start time".to_owned())
    })?;
    if matches!(session.phase, RestorePhase::HealthCopied) {
        staged
            .store
            .append_restore_started(&restore_epoch, &manifest.snapshot_id, &started_at)?;
        session.phase = RestorePhase::RestoreStarted;
        write_replace_json(&session_path, &session)?;
    }

    if matches!(session.phase, RestorePhase::RestoreStarted) {
        let mut health = open_health_database(&health_destination)?;
        if let Some(plan) = &ack_plan {
            session.ack_replayed_batches =
                Some(replay_confirmed_batches(&mut health, &staged.store, plan)?);
        }
        let replay = staged
            .store
            .replay_all_tombstones_for_restore(&mut health)?;
        session.replay_summary = Some(ReplaySummary {
            revoked_devices: replay.revoked_devices,
            erased_devices: replay.erased_devices,
            cleared_pairing_codes: replay.cleared_pairing_codes,
        });
        health.execute_batch(
            "PRAGMA wal_checkpoint(TRUNCATE);
             VACUUM;
             PRAGMA wal_checkpoint(TRUNCATE);",
        )?;
        drop(health);
        verify_health_database(&health_destination, Some(&staged.store.store_id()?))?;
        session.replayed_health_sha256 = Some(stable_staged_health_hash(
            &staged.staging_root,
            &health_destination,
        )?);
        session.restore_replayed_at = Some(Utc::now().to_rfc3339());
        session.phase = RestorePhase::ReplayPrepared;
        write_replace_json(&session_path, &session)?;
    }
    if matches!(session.phase, RestorePhase::ReplayPrepared) {
        let replayed_at = session.restore_replayed_at.clone().ok_or_else(|| {
            RecoveryError::InvalidArtifact("restore session lacks its replay time".to_owned())
        })?;
        staged.store.append_restore_replayed(
            &restore_epoch,
            &manifest.snapshot_id,
            &replayed_at,
        )?;
        session.phase = RestorePhase::ProjectionRebuildRequired;
        write_replace_json(&session_path, &session)?;
    }
    if !matches!(session.phase, RestorePhase::ProjectionRebuildRequired) {
        return Err(RecoveryError::Integrity(
            "restore did not reach the projection rebuild boundary".to_owned(),
        ));
    }
    let replayed_hash = session.replayed_health_sha256.as_deref().ok_or_else(|| {
        RecoveryError::Integrity("restore session lacks its replayed health hash".to_owned())
    })?;
    verify_health_database(&health_destination, Some(&staged.store.store_id()?))?;
    if stable_staged_health_hash(&staged.staging_root, &health_destination)? != replayed_hash {
        return Err(RecoveryError::Integrity(
            "staged health database differs from its completed tombstone replay".to_owned(),
        ));
    }
    if let Some(plan) = &ack_plan {
        let replayed = session.ack_replayed_batches.ok_or_else(|| {
            RecoveryError::Integrity(
                "restore reached projection stage without acknowledged-write replay".to_owned(),
            )
        })?;
        let evidence = AckCoverageEvidence {
            format_version: 1,
            snapshot_id: manifest.snapshot_id.clone(),
            snapshot_sha256: manifest.file_sha256.clone(),
            receipt_inventory_sha256: plan.receipt_inventory_sha256.clone(),
            journal_id: plan.journal_id.clone(),
            adoption_head_sha256: plan.adoption_head_sha256.clone(),
            confirmed_batch_count: plan.batches.len(),
            replayed_batch_count: replayed,
            highest_confirmed_commit_sequence: plan
                .batches
                .last()
                .map_or(plan.baseline_sequence, |batch| {
                    batch.receipt.commit_sequence
                }),
            staged_health_sha256: replayed_hash.to_owned(),
        };
        let path = staged
            .staging_root
            .join("control")
            .join(RESTORE_COVERAGE_EVIDENCE_NAME);
        match fs::symlink_metadata(&path) {
            Ok(_) => {
                validate_regular_single_link(&path)?;
                let existing: AckCoverageEvidence = serde_json::from_slice(&fs::read(&path)?)?;
                if existing != evidence {
                    return Err(RecoveryError::Integrity(
                        "acknowledged-write coverage changed across restore attempts".to_owned(),
                    ));
                }
            }
            Err(error) if error.kind() == io::ErrorKind::NotFound => {
                write_new_json(&path, &evidence)?;
                sync_directory(path.parent().ok_or_else(|| {
                    RecoveryError::InvalidPath("coverage evidence has no parent".to_owned())
                })?)?;
            }
            Err(error) => return Err(error.into()),
        }
    }
    let checkpoint = staged.store.checkpoint()?;
    let replay = session.replay_summary.unwrap_or_default();
    Ok(HealthRestoreState::ProjectionRebuildRequired {
        restore_epoch,
        snapshot_id: manifest.snapshot_id,
        health_db: health_destination,
        revoked_devices: replay.revoked_devices,
        erased_devices: replay.erased_devices,
        cleared_pairing_codes: replay.cleared_pairing_codes,
        control_checkpoint: checkpoint,
    })
}

fn bind_or_verify_session_health(
    session: &mut RestoreSession,
    manifest: &HealthBackupManifest,
) -> RecoveryResult<()> {
    match (&session.health_snapshot_id, &session.health_backup_sha256) {
        (None, None) => {
            session.health_snapshot_id = Some(manifest.snapshot_id.clone());
            session.health_backup_sha256 = Some(manifest.file_sha256.clone());
            session.restore_epoch = Some(hex::encode(Sha256::digest(
                format!(
                    "boaz-restore-epoch-v1\0{}\0{}\0{}\0{}",
                    manifest.snapshot_id,
                    session.expected_control_checkpoint.store_id,
                    session.expected_control_checkpoint.sequence,
                    session.expected_control_checkpoint.current_hash
                )
                .as_bytes(),
            )));
            session.restore_started_at = Some(Utc::now().to_rfc3339());
        }
        (Some(snapshot_id), Some(file_hash))
            if snapshot_id == &manifest.snapshot_id && file_hash == &manifest.file_sha256 => {}
        _ => {
            return Err(RecoveryError::Integrity(
                "restore session is already bound to a different health snapshot".to_owned(),
            ));
        }
    }
    Ok(())
}

fn validate_health_backup(
    health_backup: &Path,
    manifest: &HealthBackupManifest,
    control: &ControlStore,
) -> RecoveryResult<()> {
    validate_regular_single_link(health_backup)?;
    if manifest.format_version != HEALTH_BACKUP_FORMAT_VERSION {
        return Err(RecoveryError::InvalidArtifact(
            "health backup manifest format is unsupported".to_owned(),
        ));
    }
    validate_uuid(&manifest.snapshot_id, "health snapshot ID")?;
    validate_rfc3339(&manifest.snapshot_started_at, "health snapshot time")?;
    validate_sha256(&manifest.file_sha256, "health backup SHA-256")?;
    if manifest.source_schema_version != HEALTH_SCHEMA_VERSION {
        return Err(RecoveryError::InvalidArtifact(format!(
            "health snapshot schema {} is not supported schema {}",
            manifest.source_schema_version, HEALTH_SCHEMA_VERSION
        )));
    }
    if file_sha256(health_backup)? != manifest.file_sha256 {
        return Err(RecoveryError::Integrity(
            "health backup bytes do not match the manifest".to_owned(),
        ));
    }
    let connection = Connection::open_with_flags(health_backup, OpenFlags::SQLITE_OPEN_READ_ONLY)?;
    let application_id: i64 =
        connection.query_row("PRAGMA application_id", [], |row| row.get(0))?;
    let user_version: i64 = connection.query_row("PRAGMA user_version", [], |row| row.get(0))?;
    if application_id != HEALTH_APPLICATION_ID || user_version != HEALTH_SCHEMA_VERSION {
        return Err(RecoveryError::InvalidArtifact(
            "health backup identity or schema version is incompatible".to_owned(),
        ));
    }
    let durable_sequence: i64 = connection
        .query_row(
            "SELECT seq FROM sqlite_sequence WHERE name='receipts'",
            [],
            |row| row.get(0),
        )
        .optional()?
        .unwrap_or(0);
    if durable_sequence < 0
        || manifest
            .source_commit_sequence
            .is_some_and(|declared| declared != durable_sequence)
    {
        return Err(RecoveryError::Integrity(
            "health snapshot manifest commit sequence differs from SQLite's durable receipt high-water mark"
                .to_owned(),
        ));
    }
    drop(connection);
    let store_id = control.store_id()?;
    verify_health_database(health_backup, Some(&store_id))?;
    if !control.contains_checkpoint(&manifest.control_checkpoint)? {
        return Err(RecoveryError::Integrity(
            "health backup control checkpoint is not an ancestor of the staged control head"
                .to_owned(),
        ));
    }
    if !control.verify_backup_artifact(&manifest.snapshot_id, &manifest.file_sha256)? {
        return Err(RecoveryError::Integrity(
            "health backup lacks matching active backup-created control evidence".to_owned(),
        ));
    }
    let evidence_matches = control
        .active_backup_inventory()?
        .into_iter()
        .any(|backup| {
            backup.snapshot_id == manifest.snapshot_id
                && backup.file_sha256 == manifest.file_sha256
                && backup.occurred_at == manifest.snapshot_started_at
                && backup.prior_checkpoint == manifest.control_checkpoint
        });
    if !evidence_matches {
        return Err(RecoveryError::Integrity(
            "health snapshot ID, bytes, time, and prior control checkpoint are not bound by one active control event"
                .to_owned(),
        ));
    }
    Ok(())
}

fn read_health_manifest(path: &Path) -> RecoveryResult<HealthBackupManifest> {
    validate_regular_single_link(path)?;
    let manifest: HealthBackupManifest = serde_json::from_slice(&fs::read(path)?)?;
    Ok(manifest)
}

fn validate_control_bundle(
    bundle: &Path,
    expected_head: Option<&ControlCheckpoint>,
) -> RecoveryResult<ControlBackupManifest> {
    reject_symlink_or_non_directory(bundle)?;
    let expected_top_level: BTreeSet<String> = [
        CONTROL_DATABASE_NAME,
        CONTROL_HEAD_NAME,
        CONTROL_MANIFEST_NAME,
        CONTROL_MIRROR_DIRECTORY,
    ]
    .into_iter()
    .map(str::to_owned)
    .collect();
    let actual_top_level = directory_entry_names(bundle)?;
    if actual_top_level != expected_top_level {
        return Err(RecoveryError::InvalidArtifact(
            "control bundle has missing or unexpected top-level entries".to_owned(),
        ));
    }
    let manifest_path = bundle.join(CONTROL_MANIFEST_NAME);
    validate_regular_single_link(&manifest_path)?;
    let manifest: ControlBackupManifest = serde_json::from_slice(&fs::read(manifest_path)?)?;
    validate_control_manifest_shape(&manifest)?;
    if expected_head.is_some_and(|expected| expected != &manifest.checkpoint) {
        return Err(RecoveryError::Integrity(
            "control bundle manifest differs from the external expected head".to_owned(),
        ));
    }
    let mut declared = BTreeSet::new();
    for artifact in std::iter::once(&manifest.database)
        .chain(std::iter::once(&manifest.head))
        .chain(manifest.mirrors.iter())
    {
        if !declared.insert(artifact.relative_path.clone()) {
            return Err(RecoveryError::InvalidArtifact(
                "control bundle manifest contains a duplicate artifact path".to_owned(),
            ));
        }
        verify_artifact(bundle, artifact)?;
    }
    let mirror_directory = bundle.join(CONTROL_MIRROR_DIRECTORY);
    reject_symlink_or_non_directory(&mirror_directory)?;
    let actual_mirrors = directory_entry_names(&mirror_directory)?;
    let declared_mirrors: BTreeSet<String> = manifest
        .mirrors
        .iter()
        .map(|artifact| {
            Path::new(&artifact.relative_path)
                .file_name()
                .and_then(|name| name.to_str())
                .map(str::to_owned)
                .ok_or_else(|| {
                    RecoveryError::InvalidArtifact(
                        "control mirror manifest path is invalid".to_owned(),
                    )
                })
        })
        .collect::<RecoveryResult<_>>()?;
    if actual_mirrors != declared_mirrors {
        return Err(RecoveryError::InvalidArtifact(
            "control mirror inventory differs from its manifest".to_owned(),
        ));
    }
    let store = ControlStore::new(bundle.join(CONTROL_DATABASE_NAME), mirror_directory)?;
    store.verify()?;
    if store.checkpoint()? != manifest.checkpoint {
        return Err(RecoveryError::Integrity(
            "control bundle database/head tail differs from its manifest".to_owned(),
        ));
    }
    Ok(manifest)
}

fn validate_control_manifest_shape(manifest: &ControlBackupManifest) -> RecoveryResult<()> {
    if manifest.format_version != CONTROL_BACKUP_FORMAT_VERSION {
        return Err(RecoveryError::InvalidArtifact(
            "control backup manifest format is unsupported".to_owned(),
        ));
    }
    validate_uuid(&manifest.snapshot_id, "control snapshot ID")?;
    validate_rfc3339(&manifest.created_at, "control snapshot time")?;
    if manifest.database.relative_path != CONTROL_DATABASE_NAME
        || manifest.head.relative_path != CONTROL_HEAD_NAME
    {
        return Err(RecoveryError::InvalidArtifact(
            "control database or head manifest path is invalid".to_owned(),
        ));
    }
    validate_sha256(&manifest.checkpoint.current_hash, "control checkpoint hash")?;
    if manifest.checkpoint.store_id.is_empty() || manifest.checkpoint.sequence < 0 {
        return Err(RecoveryError::InvalidArtifact(
            "control checkpoint fields are invalid".to_owned(),
        ));
    }
    for artifact in std::iter::once(&manifest.database)
        .chain(std::iter::once(&manifest.head))
        .chain(manifest.mirrors.iter())
    {
        validate_sha256(&artifact.sha256, "control artifact SHA-256")?;
        validate_relative_artifact_path(&artifact.relative_path)?;
    }
    if manifest
        .mirrors
        .iter()
        .any(|artifact| !artifact.relative_path.starts_with("mirror/"))
    {
        return Err(RecoveryError::InvalidArtifact(
            "control mirror artifact is outside the mirror directory".to_owned(),
        ));
    }
    Ok(())
}

fn exact_source_mirror_paths(
    store: &ControlStore,
    checkpoint: &ControlCheckpoint,
) -> RecoveryResult<Vec<PathBuf>> {
    reject_symlink_or_non_directory(store.mirror_dir())?;
    let connection =
        Connection::open_with_flags(store.db_path(), OpenFlags::SQLITE_OPEN_READ_ONLY)?;
    let mut statement =
        connection.prepare("SELECT sequence,current_hash FROM control_events ORDER BY sequence")?;
    let rows = statement.query_map([], |row| {
        Ok((row.get::<_, i64>(0)?, row.get::<_, String>(1)?))
    })?;
    let mut expected = Vec::new();
    for row in rows {
        let (sequence, hash) = row?;
        expected.push(format!("{sequence:020}-{hash}.json"));
    }
    if i64::try_from(expected.len()).ok() != Some(checkpoint.sequence) {
        return Err(RecoveryError::Integrity(
            "control event count differs from its head sequence".to_owned(),
        ));
    }
    let actual = directory_entry_names(store.mirror_dir())?;
    let expected_set: BTreeSet<String> = expected.iter().cloned().collect();
    if actual != expected_set {
        return Err(RecoveryError::Integrity(
            "control mirror directory contains missing, extra, or forked records".to_owned(),
        ));
    }
    expected
        .into_iter()
        .map(|name| {
            let path = store.mirror_dir().join(name);
            validate_regular_single_link(&path)?;
            Ok(path)
        })
        .collect()
}

fn reject_destination_near_control_authority(
    store: &ControlStore,
    destination: &Path,
) -> RecoveryResult<()> {
    let control_parent = store
        .db_path()
        .parent()
        .ok_or_else(|| RecoveryError::InvalidPath("control database has no parent".to_owned()))?;
    let planned = planned_canonical_path(destination)?;
    for forbidden in [control_parent, store.mirror_dir()] {
        let forbidden = forbidden.canonicalize()?;
        if planned.starts_with(&forbidden) || forbidden.starts_with(&planned) {
            return Err(RecoveryError::InvalidPath(
                "control backup may not be inside or contain the live control authority".to_owned(),
            ));
        }
    }
    Ok(())
}

fn reject_path_relationship(staging_root: &Path, source: &Path) -> RecoveryResult<()> {
    let staging = staging_root.canonicalize()?;
    let source = source.canonicalize()?;
    if source.starts_with(&staging) || staging.starts_with(&source) || same_file(&staging, &source)?
    {
        return Err(RecoveryError::InvalidPath(
            "recovery input and staging output may not alias or contain one another".to_owned(),
        ));
    }
    Ok(())
}

fn validate_staging_root(
    staging_root: &Path,
    live_paths: &[PathBuf],
    require_empty: bool,
) -> RecoveryResult<()> {
    validate_absolute_lexical(staging_root)?;
    reject_symlink_components(staging_root)?;
    let planned = planned_canonical_path(staging_root)?;
    for live in live_paths {
        validate_absolute_lexical(live)?;
        let canonical = match live.canonicalize() {
            Ok(value) => value,
            Err(error) if error.kind() == io::ErrorKind::NotFound => continue,
            Err(error) => return Err(error.into()),
        };
        if planned.starts_with(&canonical) || canonical.starts_with(&planned) {
            return Err(RecoveryError::InvalidPath(
                "staging path aliases, contains, or is contained by a live path".to_owned(),
            ));
        }
        if staging_root.exists() && same_file(staging_root, &canonical)? {
            return Err(RecoveryError::InvalidPath(
                "staging path has the same device/inode identity as a live path".to_owned(),
            ));
        }
    }
    if staging_root.exists() {
        reject_symlink_or_non_directory(staging_root)?;
        if require_empty && fs::read_dir(staging_root)?.next().transpose()?.is_some() {
            return Err(RecoveryError::InvalidPath(
                "staging root must be empty".to_owned(),
            ));
        }
    }
    Ok(())
}

fn ensure_empty_staging_root(path: &Path) -> RecoveryResult<()> {
    if path.exists() {
        reject_symlink_or_non_directory(path)?;
        if fs::read_dir(path)?.next().transpose()?.is_some() {
            return Err(RecoveryError::InvalidPath(
                "staging root must be empty".to_owned(),
            ));
        }
        set_private_directory(path)?;
        return Ok(());
    }
    let parent = path
        .parent()
        .ok_or_else(|| RecoveryError::InvalidPath("staging root has no parent".to_owned()))?;
    reject_symlink_or_non_directory(parent)?;
    create_private_directory(path)?;
    sync_directory(parent)?;
    Ok(())
}

fn validate_new_destination(path: &Path) -> RecoveryResult<()> {
    validate_absolute_lexical(path)?;
    reject_symlink_components(path)?;
    if path.exists() {
        return Err(RecoveryError::InvalidPath(
            "destination must not already exist".to_owned(),
        ));
    }
    let parent = path
        .parent()
        .ok_or_else(|| RecoveryError::InvalidPath("destination has no parent".to_owned()))?;
    reject_symlink_or_non_directory(parent)
}

fn validate_absolute_lexical(path: &Path) -> RecoveryResult<()> {
    if !path.is_absolute() {
        return Err(RecoveryError::InvalidPath(
            "recovery paths must be absolute".to_owned(),
        ));
    }
    if path.components().any(|component| {
        matches!(
            component,
            Component::ParentDir | Component::CurDir | Component::Prefix(_)
        )
    }) {
        return Err(RecoveryError::InvalidPath(
            "recovery paths may not contain dot components".to_owned(),
        ));
    }
    Ok(())
}

fn reject_symlink_components(path: &Path) -> RecoveryResult<()> {
    // The configured authority entry and its immediate parent are controlled.
    // Platform aliases above that boundary (for example macOS `/var`) are not
    // treated as user-selected recovery links; canonical-path/live-alias
    // checks below still compare the resolved destination.
    for current in [Some(path), path.parent()].into_iter().flatten() {
        match fs::symlink_metadata(current) {
            Ok(metadata) if metadata.file_type().is_symlink() => {
                return Err(RecoveryError::InvalidPath(
                    "recovery path contains a symbolic-link component".to_owned(),
                ));
            }
            Ok(_) => {}
            Err(error) if error.kind() == io::ErrorKind::NotFound => break,
            Err(error) => return Err(error.into()),
        }
    }
    Ok(())
}

fn planned_canonical_path(path: &Path) -> RecoveryResult<PathBuf> {
    if path.exists() {
        return Ok(path.canonicalize()?);
    }
    let parent = path
        .parent()
        .ok_or_else(|| RecoveryError::InvalidPath("planned path has no parent".to_owned()))?;
    let name = path
        .file_name()
        .ok_or_else(|| RecoveryError::InvalidPath("planned path has no filename".to_owned()))?;
    Ok(parent.canonicalize()?.join(name))
}

fn reject_symlink_or_non_directory(path: &Path) -> RecoveryResult<()> {
    let metadata = fs::symlink_metadata(path)?;
    if metadata.file_type().is_symlink() || !metadata.file_type().is_dir() {
        return Err(RecoveryError::InvalidPath(
            "expected a real directory, not a link or another file type".to_owned(),
        ));
    }
    Ok(())
}

fn validate_regular_single_link(path: &Path) -> RecoveryResult<()> {
    let metadata = fs::symlink_metadata(path)?;
    if metadata.file_type().is_symlink() || !metadata.file_type().is_file() {
        return Err(RecoveryError::InvalidArtifact(
            "artifact must be a regular non-symlink file".to_owned(),
        ));
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        if metadata.nlink() != 1 {
            return Err(RecoveryError::InvalidArtifact(
                "artifact files may not have hard links".to_owned(),
            ));
        }
    }
    Ok(())
}

fn validate_staged_health_file(staging_root: &Path, health: &Path) -> RecoveryResult<()> {
    let data = staging_root.join("data");
    reject_symlink_or_non_directory(&data)?;
    if health.parent() != Some(data.as_path()) {
        return Err(RecoveryError::InvalidPath(
            "staged health database is not a direct child of staging/data".to_owned(),
        ));
    }
    validate_regular_single_link(health)?;
    if health.canonicalize()?.parent() != Some(data.canonicalize()?.as_path()) {
        return Err(RecoveryError::InvalidPath(
            "staged health database escapes its data directory".to_owned(),
        ));
    }
    Ok(())
}

fn validate_final_candidate_paths(
    staging_root: &Path,
    health_db: &Path,
    control_dir: &Path,
    coordinator_dir: &Path,
) -> RecoveryResult<()> {
    for path in [health_db, control_dir, coordinator_dir] {
        validate_absolute_lexical(path)?;
        reject_symlink_components(path)?;
    }
    let health_parent = health_db.parent().ok_or_else(|| {
        RecoveryError::InvalidPath("final health candidate has no parent".to_owned())
    })?;
    let control_parent = control_dir.parent().ok_or_else(|| {
        RecoveryError::InvalidPath("final control candidate has no parent".to_owned())
    })?;
    for path in [health_parent, control_parent, coordinator_dir] {
        reject_symlink_or_non_directory(path)?;
        require_private_directory(path)?;
    }
    let staged = staging_root.canonicalize()?;
    let health = planned_canonical_path(health_db)?;
    let control = planned_canonical_path(control_dir)?;
    let coordinator = coordinator_dir.canonicalize()?;
    if health == control
        || health.starts_with(&staged)
        || control.starts_with(&staged)
        || staged.starts_with(&health)
        || staged.starts_with(&control)
        || health.starts_with(&coordinator)
        || control.starts_with(&coordinator)
        || coordinator.starts_with(&health)
        || coordinator.starts_with(&control)
        || health.starts_with(&control)
        || control.starts_with(&health)
    {
        return Err(RecoveryError::InvalidPath(
            "final generation paths alias staging, coordination or each other".to_owned(),
        ));
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        let health_device = fs::metadata(health_parent)?.dev();
        let control_device = fs::metadata(control_parent)?.dev();
        let coordinator_device = fs::metadata(coordinator_dir)?.dev();
        let staging_device = fs::metadata(staging_root)?.dev();
        if health_device == control_device
            || health_device == staging_device
            || control_device == staging_device
            || control_device != coordinator_device
        {
            return Err(RecoveryError::InvalidPath(
                "final health, control and staging candidates lack independent volume identities"
                    .to_owned(),
            ));
        }
    }
    Ok(())
}

fn require_private_directory(path: &Path) -> RecoveryResult<()> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::{MetadataExt, PermissionsExt};
        let metadata = fs::symlink_metadata(path)?;
        if metadata.permissions().mode() & 0o077 != 0
            || metadata.uid() != unsafe { libc::geteuid() }
        {
            return Err(RecoveryError::InvalidPath(
                "final generation directory must be private and owned by this process".to_owned(),
            ));
        }
    }
    Ok(())
}

fn reject_sqlite_sidecars(path: &Path) -> RecoveryResult<()> {
    let filename = path
        .file_name()
        .ok_or_else(|| RecoveryError::InvalidPath("SQLite candidate has no filename".to_owned()))?;
    let parent = path
        .parent()
        .ok_or_else(|| RecoveryError::InvalidPath("SQLite candidate has no parent".to_owned()))?;
    for suffix in ["-wal", "-shm", "-journal"] {
        let sidecar = parent.join(format!("{}{}", filename.to_string_lossy(), suffix));
        match fs::symlink_metadata(sidecar) {
            Ok(_) => {
                return Err(RecoveryError::Integrity(
                    "quiescent SQLite candidate has a WAL, SHM or journal sidecar".to_owned(),
                ));
            }
            Err(error) if error.kind() == io::ErrorKind::NotFound => {}
            Err(error) => return Err(error.into()),
        }
    }
    Ok(())
}

fn stable_staged_health_hash(staging_root: &Path, health: &Path) -> RecoveryResult<String> {
    validate_staged_health_file(staging_root, health)?;
    // The SQLite main-file digest alone does not cover an uncheckpointed WAL.
    // Checkpoint before comparing with the replayed state, so a later write
    // through SQLite cannot be hidden in a sidecar during a resumed restore.
    let connection = open_health_database(health)?;
    connection.execute_batch("PRAGMA wal_checkpoint(TRUNCATE);")?;
    drop(connection);
    validate_staged_health_file(staging_root, health)?;
    file_sha256(health)
}

fn same_file(first: &Path, second: &Path) -> RecoveryResult<bool> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        let first = fs::metadata(first)?;
        let second = fs::metadata(second)?;
        Ok(first.dev() == second.dev() && first.ino() == second.ino())
    }
    #[cfg(not(unix))]
    {
        Ok(first.canonicalize()? == second.canonicalize()?)
    }
}

fn directory_entry_names(path: &Path) -> RecoveryResult<BTreeSet<String>> {
    let mut names = BTreeSet::new();
    for entry in fs::read_dir(path)? {
        let entry = entry?;
        let name = entry.file_name().into_string().map_err(|_| {
            RecoveryError::InvalidArtifact("artifact filename is not valid UTF-8".to_owned())
        })?;
        names.insert(name);
    }
    Ok(names)
}

fn sqlite_snapshot(source: &Path, destination: &Path) -> RecoveryResult<()> {
    let source = Connection::open_with_flags(source, OpenFlags::SQLITE_OPEN_READ_ONLY)?;
    let mut destination_connection = Connection::open(destination)?;
    {
        let backup = rusqlite::backup::Backup::new(&source, &mut destination_connection)?;
        backup.run_to_completion(100, Duration::from_millis(25), None)?;
    }
    // The authority runs in WAL mode, but a portable bundle must be complete
    // in one database file and may not grow read-created WAL/SHM sidecars while
    // it is being hashed and inventoried.
    destination_connection.execute_batch(
        "PRAGMA wal_checkpoint(TRUNCATE);
         PRAGMA journal_mode=DELETE;",
    )?;
    let quick: String =
        destination_connection.query_row("PRAGMA quick_check", [], |row| row.get(0))?;
    if quick != "ok" {
        return Err(RecoveryError::Integrity(
            "copied SQLite control database failed quick_check".to_owned(),
        ));
    }
    drop(destination_connection);
    set_private_file(destination)?;
    File::open(destination)?.sync_all()?;
    Ok(())
}

fn copy_regular_file(source: &Path, destination: &Path) -> RecoveryResult<()> {
    validate_regular_single_link(source)?;
    let mut input = File::open(source)?;
    let mut output = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(destination)?;
    io::copy(&mut input, &mut output)?;
    output.sync_all()?;
    set_private_file(destination)?;
    Ok(())
}

fn artifact_for(root: &Path, path: &Path) -> RecoveryResult<ArtifactFile> {
    validate_regular_single_link(path)?;
    let relative = path
        .strip_prefix(root)
        .map_err(|_| RecoveryError::InvalidArtifact("artifact is outside its bundle".to_owned()))?;
    let relative_path = relative
        .components()
        .map(|component| component.as_os_str().to_string_lossy())
        .collect::<Vec<_>>()
        .join("/");
    Ok(ArtifactFile {
        relative_path,
        size_bytes: fs::metadata(path)?.len(),
        sha256: file_sha256(path)?,
    })
}

fn verify_artifact(root: &Path, artifact: &ArtifactFile) -> RecoveryResult<()> {
    let path = safe_artifact_path(root, &artifact.relative_path)?;
    validate_regular_single_link(&path)?;
    let metadata = fs::metadata(&path)?;
    if metadata.len() != artifact.size_bytes || file_sha256(&path)? != artifact.sha256 {
        return Err(RecoveryError::Integrity(format!(
            "artifact {} does not match its size/hash manifest",
            artifact.relative_path
        )));
    }
    Ok(())
}

fn safe_artifact_path(root: &Path, relative: &str) -> RecoveryResult<PathBuf> {
    validate_relative_artifact_path(relative)?;
    Ok(root.join(relative))
}

fn validate_relative_artifact_path(relative: &str) -> RecoveryResult<()> {
    let path = Path::new(relative);
    if relative.is_empty()
        || path.is_absolute()
        || path
            .components()
            .any(|component| !matches!(component, Component::Normal(_)))
    {
        return Err(RecoveryError::InvalidArtifact(
            "artifact manifest path is not a safe relative path".to_owned(),
        ));
    }
    Ok(())
}

fn file_sha256(path: &Path) -> RecoveryResult<String> {
    let mut file = File::open(path)?;
    let mut digest = Sha256::new();
    let mut buffer = [0_u8; 64 * 1024];
    loop {
        let count = file.read(&mut buffer)?;
        if count == 0 {
            break;
        }
        digest.update(&buffer[..count]);
    }
    Ok(hex::encode(digest.finalize()))
}

fn validate_sha256(value: &str, label: &str) -> RecoveryResult<()> {
    if value.len() != 64
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        return Err(RecoveryError::InvalidArtifact(format!(
            "{label} must be 64 lowercase hexadecimal characters"
        )));
    }
    Ok(())
}

fn validate_uuid(value: &str, label: &str) -> RecoveryResult<()> {
    if uuid::Uuid::parse_str(value).is_err() {
        return Err(RecoveryError::InvalidArtifact(format!(
            "{label} is not a UUID"
        )));
    }
    Ok(())
}

fn validate_rfc3339(value: &str, label: &str) -> RecoveryResult<()> {
    if DateTime::parse_from_rfc3339(value).is_err() {
        return Err(RecoveryError::InvalidArtifact(format!(
            "{label} is not RFC 3339"
        )));
    }
    Ok(())
}

fn session_path(staging_root: &Path) -> PathBuf {
    staging_root.join("control").join(RESTORE_SESSION_NAME)
}

fn read_restore_session(path: &Path) -> RecoveryResult<RestoreSession> {
    validate_regular_single_link(path)?;
    Ok(serde_json::from_slice(&fs::read(path)?)?)
}

fn write_new_json<T: Serialize>(path: &Path, value: &T) -> RecoveryResult<()> {
    let bytes = serde_json::to_vec(value)?;
    let mut file = OpenOptions::new().write(true).create_new(true).open(path)?;
    file.write_all(&bytes)?;
    file.sync_all()?;
    set_private_file(path)?;
    Ok(())
}

fn write_replace_json<T: Serialize>(path: &Path, value: &T) -> RecoveryResult<()> {
    let parent = path
        .parent()
        .ok_or_else(|| RecoveryError::InvalidPath("JSON state file has no parent".to_owned()))?;
    let temp = parent.join(format!(".restore-state-{}.tmp", uuid::Uuid::new_v4()));
    let result = (|| -> RecoveryResult<()> {
        write_new_json(&temp, value)?;
        fs::rename(&temp, path)?;
        sync_directory(parent)
    })();
    if result.is_err() {
        let _ = fs::remove_file(temp);
    }
    result
}

fn create_private_directory(path: &Path) -> RecoveryResult<()> {
    fs::create_dir(path)?;
    set_private_directory(path)
}

fn set_private_directory(path: &Path) -> RecoveryResult<()> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(path, fs::Permissions::from_mode(0o700))?;
    }
    Ok(())
}

fn set_private_file(path: &Path) -> RecoveryResult<()> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(path, fs::Permissions::from_mode(0o600))?;
    }
    Ok(())
}

fn sync_directory(path: &Path) -> RecoveryResult<()> {
    OpenOptions::new().read(true).open(path)?.sync_all()?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::database::initialize_health_database;
    use tempfile::TempDir;

    fn control_store(root: &Path) -> ControlStore {
        let control = root.join("control-live");
        let mirror = control.join("mirror");
        fs::create_dir_all(&mirror).unwrap();
        ControlStore::initialize(control.join("control.db"), mirror).unwrap()
    }

    fn live_paths(store: &ControlStore, live_health: &Path) -> Vec<PathBuf> {
        vec![
            store.db_path().parent().unwrap().to_path_buf(),
            live_health.to_path_buf(),
        ]
    }

    fn snapshot_health(source: &Path, destination: &Path) {
        let source = Connection::open_with_flags(source, OpenFlags::SQLITE_OPEN_READ_ONLY).unwrap();
        let mut destination = Connection::open(destination).unwrap();
        let backup = rusqlite::backup::Backup::new(&source, &mut destination).unwrap();
        backup
            .run_to_completion(100, Duration::from_millis(1), None)
            .unwrap();
    }

    fn erased_staging_fixture() -> (
        TempDir,
        ControlStore,
        PathBuf,
        PathBuf,
        PathBuf,
        StagedControl,
    ) {
        let directory = TempDir::new().unwrap();
        let store = control_store(directory.path());
        let live_health = directory.path().join("live-health.db");
        initialize_health_database(&live_health, &store.store_id().unwrap()).unwrap();
        let health = open_health_database(&live_health).unwrap();
        health
            .execute(
                "INSERT INTO devices(device_id,token_hash,created_at) VALUES ('phone-old','token-old','2026-09-19T00:00:00Z')",
                [],
            )
            .unwrap();
        health
            .execute(
                "INSERT INTO events(device_id,event_id,revision,operation,kind,health_type,payload_json,payload_hash,updated_at)
                 VALUES ('phone-old','event-old',1,'upsert','quantity','HKQuantityTypeIdentifierBodyMass','{}','payload-hash','2026-09-19T00:00:00Z')",
                [],
            )
            .unwrap();
        drop(health);

        let backup = directory.path().join("health-backup.db");
        snapshot_health(&live_health, &backup);
        let snapshot_id = uuid::Uuid::new_v4().to_string();
        let backup_hash = file_sha256(&backup).unwrap();
        let checkpoint = store.checkpoint().unwrap();
        store
            .append_backup_created(
                &snapshot_id,
                &backup_hash,
                "2026-09-19T00:01:00Z",
                &checkpoint,
            )
            .unwrap();
        store
            .append_erasure_intent(
                "phone-old",
                "token-old",
                "erase-old",
                "secret-old",
                "2026-09-19T00:02:00Z",
                "2026-10-19T00:02:00Z",
            )
            .unwrap();
        store
            .append_health_erasure_verified("phone-old", "erase-old", "2026-09-19T00:03:00Z")
            .unwrap();

        let manifest = directory.path().join("health-backup.db.meta.json");
        write_new_json(
            &manifest,
            &HealthBackupManifest {
                format_version: HEALTH_BACKUP_FORMAT_VERSION,
                snapshot_id,
                snapshot_started_at: "2026-09-19T00:01:00Z".to_owned(),
                source_commit_sequence: None,
                source_schema_version: HEALTH_SCHEMA_VERSION,
                file_sha256: backup_hash,
                control_checkpoint: checkpoint,
            },
        )
        .unwrap();
        let bundle = directory.path().join("control-bundle");
        let control_manifest = backup_control(&store, &bundle).unwrap();
        let staging_root = directory.path().join("restore-staging");
        let staged = restore_control_to_staging(
            &bundle,
            &staging_root,
            &control_manifest.checkpoint,
            &live_paths(&store, &live_health),
        )
        .unwrap();
        (directory, store, live_health, backup, manifest, staged)
    }

    fn adoption_journal(
        root: &Path,
        backup: &Path,
        manifest_path: &Path,
        store: &ControlStore,
    ) -> AckJournal {
        let mut manifest = read_health_manifest(manifest_path).unwrap();
        manifest.source_commit_sequence = Some(0);
        write_replace_json(manifest_path, &manifest).unwrap();
        let journal_root = root.join("ack-journal");
        fs::create_dir(&journal_root).unwrap();
        AckJournal::initialize(&journal_root).unwrap();
        let journal = AckJournal::open(&journal_root).unwrap();
        let backup = Connection::open_with_flags(backup, OpenFlags::SQLITE_OPEN_READ_ONLY).unwrap();
        let checkpoint = store.checkpoint().unwrap();
        journal
            .bind_baseline(&crate::ack_journal::Baseline {
                snapshot_id: manifest.snapshot_id,
                snapshot_sha256: manifest.file_sha256,
                receipt_inventory_sha256: receipt_inventory_sha256(&backup).unwrap(),
                control_store_id: checkpoint.store_id,
                control_head_sequence: checkpoint.sequence,
                control_head_hash: checkpoint.current_hash,
            })
            .unwrap();
        journal
    }

    #[test]
    fn empty_genesis_adoption_snapshot_enters_exact_ack_replay_plan() {
        let directory = TempDir::new_in(std::env::temp_dir().canonicalize().unwrap()).unwrap();
        let coordinator = directory.path().join("coord");
        crate::activation::initialize_coordinator(&coordinator).unwrap();
        let guard = crate::activation::lock_coordinator(&coordinator, true, true).unwrap();
        let paths = crate::database::StoragePaths::new(
            directory.path().join("data"),
            directory.path().join("data/health.db"),
            directory.path().join("control/control.db"),
            directory.path().join("control/mirror"),
            directory.path().join("backups"),
        );
        paths.prepare_empty_layout().unwrap();
        let control =
            ControlStore::initialize(paths.control_db.clone(), paths.control_mirror_dir.clone())
                .unwrap();
        initialize_health_database(&paths.health_db, &control.store_id().unwrap()).unwrap();
        let backup = paths.backup_dir.join("boaz-health-genesis.db");
        snapshot_health(&paths.health_db, &backup);
        let snapshot_id = uuid::Uuid::new_v4().to_string();
        let snapshot_hash = file_sha256(&backup).unwrap();
        let snapshot_time = "2026-09-19T00:00:00Z";
        let predecessor = control.checkpoint().unwrap();
        let manifest = HealthBackupManifest {
            format_version: HEALTH_BACKUP_FORMAT_VERSION,
            snapshot_id: snapshot_id.clone(),
            snapshot_started_at: snapshot_time.into(),
            source_commit_sequence: Some(0),
            source_schema_version: HEALTH_SCHEMA_VERSION,
            file_sha256: snapshot_hash.clone(),
            control_checkpoint: predecessor.clone(),
        };
        let manifest_path = backup.with_extension("db.meta.json");
        write_new_json(&manifest_path, &manifest).unwrap();
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            fs::set_permissions(&backup, fs::Permissions::from_mode(0o600)).unwrap();
            fs::set_permissions(&manifest_path, fs::Permissions::from_mode(0o600)).unwrap();
        }
        control
            .append_backup_created(&snapshot_id, &snapshot_hash, snapshot_time, &predecessor)
            .unwrap();
        let journal_root = directory.path().join("ack-journal");
        fs::create_dir(&journal_root).unwrap();
        AckJournal::initialize(&journal_root).unwrap();
        let journal = AckJournal::open(&journal_root).unwrap();

        let baseline = crate::adoption::verify_generation_zero_snapshot(
            &guard, &paths, &backup, &control, &journal,
        )
        .unwrap();
        assert_eq!(baseline.snapshot_id, snapshot_id);
        journal.bind_baseline(&baseline).unwrap();

        let plan = verified_ack_replay_plan(&backup, &manifest, &control, &journal).unwrap();
        assert_eq!(plan.baseline_sequence, 0);
        assert!(plan.batches.is_empty());
        assert!(plan.pairings.is_empty());
        let mut unproven = manifest.clone();
        unproven.source_commit_sequence = None;
        assert!(verified_ack_replay_plan(&backup, &unproven, &control, &journal).is_err());
    }

    #[test]
    fn unanchored_ack_journal_cannot_stage_health_or_claim_coverage() {
        let (directory, store, live_health, backup, manifest_path, staged) =
            erased_staging_fixture();
        let journal_root = directory.path().join("unanchored-journal");
        fs::create_dir(&journal_root).unwrap();
        AckJournal::initialize(&journal_root).unwrap();
        let journal = AckJournal::open(&journal_root).unwrap();
        let error = restore_health_to_staging_with_ack(
            &backup,
            &manifest_path,
            &staged,
            &live_paths(&store, &live_health),
            &journal,
        )
        .unwrap_err();
        assert!(matches!(error, RecoveryError::Integrity(_)), "{error}");
        assert!(!staged.staging_root.join("data").exists());
    }

    #[test]
    fn adopted_snapshot_replays_confirmed_survivor_and_keeps_erased_identity_retired() {
        let (directory, store, live_health, backup, manifest_path, staged) =
            erased_staging_fixture();
        let journal = adoption_journal(directory.path(), &backup, &manifest_path, &store);
        let pairing = journal
            .prepare_pairing(
                "phone-new",
                &hex::encode(Sha256::digest(b"synthetic-code")),
                &hex::encode(Sha256::digest(b"synthetic-token")),
                "2026-09-19T00:04:00Z",
            )
            .unwrap();
        journal.confirm_pairing(&pairing).unwrap();
        let raw = serde_json::to_vec(&serde_json::json!({
            "schema_version": 1,
            "batch_id": "batch-new-1",
            "device_id": "phone-new",
            "events": [{
                "event_id": "event-new-1", "revision": 1, "operation": "upsert",
                "kind": "quantity", "type": "HKQuantityTypeIdentifierHeartRate",
                "source": {"bundle_id":"synthetic.test","name":"Synthetic"},
                "start_utc":"2026-09-19T00:04:00Z",
                "end_utc":"2026-09-19T00:04:01Z",
                "value":72.0,"unit":"count/min","metadata":{}
            }]
        }))
        .unwrap();
        let prepared = journal
            .prepare_batch("batch-new-1", "phone-new", &raw)
            .unwrap();
        journal
            .confirm_batch(
                &prepared,
                &crate::ack_journal::AckReceipt {
                    commit_sequence: 1,
                    received_at: "2026-09-19T00:04:02Z".to_owned(),
                    accepted_events: 1,
                    changed_events: 1,
                    requires_projection: true,
                },
            )
            .unwrap();
        let state = restore_health_to_staging_with_ack(
            &backup,
            &manifest_path,
            &staged,
            &live_paths(&store, &live_health),
            &journal,
        )
        .unwrap();
        assert!(matches!(
            state,
            HealthRestoreState::ProjectionRebuildRequired { .. }
        ));
        let staged_db = staged.staging_root.join("data/health.db");
        let manifest = read_health_manifest(&manifest_path).unwrap();
        let health_sha = stable_staged_health_hash(&staged.staging_root, &staged_db).unwrap();
        let evidence =
            verify_ack_coverage_evidence(&staged, &backup, &manifest, &journal, &health_sha)
                .unwrap();
        assert_eq!(evidence.replayed_batch_count, 1);
        let health = open_health_database(&staged_db).unwrap();
        let survivor: i64 = health
            .query_row(
                "SELECT count(*) FROM events WHERE device_id='phone-new'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        let retired: i64 = health
            .query_row(
                "SELECT count(*) FROM events WHERE device_id='phone-old'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!((survivor, retired), (1, 0));
    }

    #[test]
    fn confirmed_batch_without_recoverable_pairing_fails_closed() {
        let (directory, store, live_health, backup, manifest_path, staged) =
            erased_staging_fixture();
        let journal = adoption_journal(directory.path(), &backup, &manifest_path, &store);
        let raw = serde_json::to_vec(&serde_json::json!({
            "schema_version":1,"batch_id":"batch-orphan","device_id":"phone-orphan",
            "events":[{"event_id":"orphan-1","revision":1,"operation":"delete",
                "kind":"quantity","type":"HKQuantityTypeIdentifierHeartRate","metadata":{}}]
        }))
        .unwrap();
        let prepared = journal
            .prepare_batch("batch-orphan", "phone-orphan", &raw)
            .unwrap();
        journal
            .confirm_batch(
                &prepared,
                &crate::ack_journal::AckReceipt {
                    commit_sequence: 1,
                    received_at: "2026-09-19T00:04:02Z".to_owned(),
                    accepted_events: 1,
                    changed_events: 1,
                    requires_projection: true,
                },
            )
            .unwrap();
        let error = restore_health_to_staging_with_ack(
            &backup,
            &manifest_path,
            &staged,
            &live_paths(&store, &live_health),
            &journal,
        )
        .unwrap_err();
        assert!(matches!(error, RecoveryError::Integrity(_)), "{error}");
        assert!(
            !staged
                .staging_root
                .join("control")
                .join(RESTORE_COVERAGE_EVIDENCE_NAME)
                .exists()
        );
        let live = open_health_database(&live_health).unwrap();
        let old_count: i64 = live
            .query_row(
                "SELECT count(*) FROM events WHERE device_id='phone-old'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(old_count, 1);
    }

    #[cfg(unix)]
    #[test]
    fn linked_staged_health_file_cannot_alias_backup() {
        use std::os::unix::fs::symlink;

        let (_directory, store, live_health, backup, manifest, staged) = erased_staging_fixture();
        let data = staged.staging_root.join("data");
        fs::create_dir(&data).unwrap();
        let destination = data.join("health.db");
        let backup_hash = file_sha256(&backup).unwrap();

        symlink(&backup, &destination).unwrap();
        let symlink_error = restore_health_to_staging(
            &backup,
            &manifest,
            &staged,
            &live_paths(&store, &live_health),
        )
        .unwrap_err();
        assert!(
            matches!(symlink_error, RecoveryError::InvalidArtifact(_)),
            "a symlink must be rejected as an invalid staged artifact: {symlink_error}"
        );
        assert_eq!(file_sha256(&backup).unwrap(), backup_hash);
        fs::remove_file(&destination).unwrap();

        fs::hard_link(&backup, &destination).unwrap();
        let hardlink_error = restore_health_to_staging(
            &backup,
            &manifest,
            &staged,
            &live_paths(&store, &live_health),
        )
        .unwrap_err();
        assert!(
            matches!(hardlink_error, RecoveryError::InvalidArtifact(_)),
            "a hard link must be rejected as an invalid staged artifact: {hardlink_error}"
        );
        assert_eq!(file_sha256(&backup).unwrap(), backup_hash);
        assert_eq!(
            fs::metadata(&destination).unwrap().len(),
            fs::metadata(&backup).unwrap().len()
        );
    }

    #[test]
    fn replacing_health_after_replay_cannot_reuse_projection_ready_state() {
        let (_directory, store, live_health, backup, manifest, staged) = erased_staging_fixture();
        let first = restore_health_to_staging(
            &backup,
            &manifest,
            &staged,
            &live_paths(&store, &live_health),
        )
        .unwrap();
        assert!(matches!(
            first,
            HealthRestoreState::ProjectionRebuildRequired { .. }
        ));
        let destination = staged.staging_root.join("data/health.db");
        let restored = open_health_database(&destination).unwrap();
        let erased_events: i64 = restored
            .query_row("SELECT count(*) FROM events", [], |row| row.get(0))
            .unwrap();
        assert_eq!(erased_events, 0);
        drop(restored);

        // The selected backup is structurally valid and bound to this store,
        // but it predates the completed erasure. Replacing the staged file
        // after replay must not reuse the recorded replay state.
        fs::copy(&backup, &destination).unwrap();
        let replacement_error = restore_health_to_staging(
            &backup,
            &manifest,
            &staged,
            &live_paths(&store, &live_health),
        )
        .unwrap_err();
        assert!(
            matches!(replacement_error, RecoveryError::Integrity(_)),
            "a valid old snapshot must fail replay integrity, not be reported ready: {replacement_error}"
        );
    }

    #[test]
    fn wal_only_mutation_after_replay_cannot_reuse_projection_ready_state() {
        let (_directory, store, live_health, backup, manifest, staged) = erased_staging_fixture();
        restore_health_to_staging(
            &backup,
            &manifest,
            &staged,
            &live_paths(&store, &live_health),
        )
        .unwrap();
        let destination = staged.staging_root.join("data/health.db");
        let replayed_file_hash = file_sha256(&destination).unwrap();
        let writer = open_health_database(&destination).unwrap();
        writer
            .execute(
                "INSERT INTO pairing_codes(code_hash,expires_at) VALUES ('resurrected-pair','2026-10-19T00:10:00Z')",
                [],
            )
            .unwrap();
        assert_eq!(
            file_sha256(&destination).unwrap(),
            replayed_file_hash,
            "fixture must keep the added row only in SQLite WAL"
        );
        let wal_error = restore_health_to_staging(
            &backup,
            &manifest,
            &staged,
            &live_paths(&store, &live_health),
        )
        .unwrap_err();
        assert!(
            matches!(wal_error, RecoveryError::Integrity(_)),
            "a post-replay WAL write must fail replay integrity: {wal_error}"
        );
        drop(writer);
    }

    #[test]
    fn completion_intent_reuses_exact_evidence_and_timestamp() {
        let (_directory, store, live_health, backup, manifest, staged) = erased_staging_fixture();
        restore_health_to_staging(
            &backup,
            &manifest,
            &staged,
            &live_paths(&store, &live_health),
        )
        .unwrap();
        let health_db = staged.staging_root.join("data/health.db");
        let health_sha256 = stable_staged_health_hash(&staged.staging_root, &health_db).unwrap();
        let projection = crate::projection::ProjectionEvidence {
            generation_id: uuid::Uuid::new_v4().to_string(),
            mapping_version: 1,
            storage_path: staged.staging_root.join("not-used-by-this-binding-test"),
            storage_identity: "synthetic-storage".to_owned(),
            binary_sha256: hex::encode(Sha256::digest(b"synthetic-binary")),
            oracle_sha256: hex::encode(Sha256::digest(b"synthetic-oracle")),
            full_readback_sha256: hex::encode(Sha256::digest(b"synthetic-oracle")),
            verified_at: "2026-09-19T00:04:00Z".to_owned(),
            series_count: 0,
            sample_count: 0,
            collision_count: 0,
        };
        let first =
            persist_restore_completion_intent(&staged, &projection, &health_sha256).unwrap();
        let second =
            persist_restore_completion_intent(&staged, &projection, &health_sha256).unwrap();
        assert_eq!(first, second);
        let evidence_path = staged
            .staging_root
            .join("control")
            .join(RESTORE_PROJECTION_EVIDENCE_NAME);
        assert_eq!(
            file_sha256(&evidence_path).unwrap(),
            first.projection_evidence_sha256
        );
        let mut contradictory = projection.clone();
        contradictory.oracle_sha256 = hex::encode(Sha256::digest(b"different-oracle"));
        assert!(
            persist_restore_completion_intent(&staged, &contradictory, &health_sha256).is_err()
        );
        fs::write(&evidence_path, b"{}").unwrap();
        assert!(staged_projection_evidence(&staged).is_err());
        assert!(persist_restore_completion_intent(&staged, &projection, &health_sha256).is_err());
    }

    #[tokio::test]
    async fn public_completion_rejects_caller_forged_vm_digest_without_native_readback() {
        let (directory, store, live_health, backup, manifest, staged) = erased_staging_fixture();
        restore_health_to_staging(
            &backup,
            &manifest,
            &staged,
            &live_paths(&store, &live_health),
        )
        .unwrap();
        let health_db = staged.staging_root.join("data/health.db");
        let health_sha256 = stable_staged_health_hash(&staged.staging_root, &health_db).unwrap();
        let synthetic_root = directory.path().canonicalize().unwrap();
        let binary = synthetic_root.join("synthetic-vm-binary");
        fs::write(&binary, b"synthetic-binary").unwrap();
        let live_storage = synthetic_root.join("live-vm-storage");
        let staging_storage = synthetic_root.join("staging-vm-storage");
        fs::create_dir(&live_storage).unwrap();
        fs::create_dir(&staging_storage).unwrap();
        let target = crate::projection::VmTarget::staging(
            &crate::projection::VmConfig {
                binary: binary.clone(),
                storage: live_storage,
            },
            crate::projection::VmConfig {
                binary,
                storage: staging_storage.clone(),
            },
        )
        .unwrap();
        let forged = crate::projection::ProjectionEvidence {
            generation_id: uuid::Uuid::new_v4().to_string(),
            mapping_version: 1,
            storage_path: staging_storage,
            storage_identity: "synthetic-storage".to_owned(),
            binary_sha256: hex::encode(Sha256::digest(b"synthetic-binary")),
            oracle_sha256: hex::encode(Sha256::digest(b"[]")),
            full_readback_sha256: hex::encode(Sha256::digest(b"[]")),
            verified_at: "2026-09-19T00:04:00Z".to_owned(),
            series_count: 0,
            sample_count: 0,
            collision_count: 0,
        };
        assert!(
            prepare_restore_completion(&staged, &target, &forged, &health_sha256)
                .await
                .is_err()
        );
        assert!(
            !staged
                .staging_root
                .join("control/restore-completion-intent.json")
                .exists()
        );
    }

    #[test]
    fn tampered_snapshot_commit_watermark_rejects_staging_without_mutation() {
        let (_directory, store, live_health, backup, manifest_path, staged) =
            erased_staging_fixture();
        let mut manifest = read_health_manifest(&manifest_path).unwrap();
        manifest.source_commit_sequence = Some(1);
        write_replace_json(&manifest_path, &manifest).unwrap();
        let error = restore_health_to_staging(
            &backup,
            &manifest_path,
            &staged,
            &live_paths(&store, &live_health),
        )
        .unwrap_err();
        assert!(matches!(error, RecoveryError::Integrity(_)), "{error}");
        assert!(!staged.staging_root.join("data").exists());
    }

    #[test]
    fn final_volume_candidate_rejects_a_matching_watermark_without_replay_proof() {
        let (directory, store, live_health, backup, manifest_path, staged) =
            erased_staging_fixture();
        restore_health_to_staging(
            &backup,
            &manifest_path,
            &staged,
            &live_paths(&store, &live_health),
        )
        .unwrap();
        let health_db = staged.staging_root.join("data/health.db");
        let health_sha256 = stable_staged_health_hash(&staged.staging_root, &health_db).unwrap();
        let evidence = crate::projection::ProjectionEvidence {
            generation_id: uuid::Uuid::new_v4().to_string(),
            mapping_version: 1,
            storage_path: directory.path().join("synthetic-vm"),
            storage_identity: "synthetic-storage".to_owned(),
            binary_sha256: hex::encode(Sha256::digest(b"synthetic-binary")),
            oracle_sha256: hex::encode(Sha256::digest(b"synthetic-oracle")),
            full_readback_sha256: hex::encode(Sha256::digest(b"synthetic-oracle")),
            verified_at: "2026-09-19T00:04:00Z".to_owned(),
            series_count: 0,
            sample_count: 0,
            collision_count: 0,
        };
        let completion =
            persist_restore_completion_intent(&staged, &evidence, &health_sha256).unwrap();
        let mut manifest = read_health_manifest(&manifest_path).unwrap();
        // A matching SQLite allocation watermark is not an independent
        // inventory of the acknowledgements that must survive recovery.
        manifest.source_commit_sequence = Some(0);
        let final_health = directory.path().join("final-health.db");
        let final_control = directory.path().join("final-control");
        let error = materialize_final_candidate(
            &staged,
            &manifest,
            &completion,
            0,
            &staged.store.checkpoint().unwrap(),
            &final_health,
            &final_control,
            directory.path(),
        )
        .unwrap_err();
        assert!(matches!(error, RecoveryError::Integrity(_)), "{error}");
        assert!(!final_health.exists());
        assert!(!final_control.exists());
    }

    #[cfg(not(target_os = "linux"))]
    #[test]
    fn native_staging_vm_refuses_non_linux_before_spawning() {
        let directory = TempDir::new().unwrap();
        let candidate_storage = directory.path().join("candidate-vm");
        let live = crate::projection::VmConfig {
            binary: directory.path().join("missing-vm-binary"),
            storage: directory.path().join("live-vm"),
        };
        let candidate = crate::projection::VmConfig {
            binary: live.binary.clone(),
            storage: candidate_storage.clone(),
        };
        assert!(NativeStagingVm::launch(&live, candidate.clone(), "30y").is_err());
        assert!(NativeStagingVm::resume(&live, candidate, "30y").is_err());
        assert!(!candidate_storage.exists());
    }

    #[test]
    fn expected_head_mismatch_and_artifact_tampering_fail_closed() {
        let directory = TempDir::new().unwrap();
        let store = control_store(directory.path());
        store
            .append_credential_revoked("phone-1", "token-hash", "2026-09-19T00:00:00Z")
            .unwrap();
        let bundle = directory.path().join("control-bundle");
        let manifest = backup_control(&store, &bundle).unwrap();
        let wrong = ControlCheckpoint {
            store_id: manifest.checkpoint.store_id.clone(),
            sequence: manifest.checkpoint.sequence,
            current_hash: "f".repeat(64),
        };
        let staging = directory.path().join("staging-wrong");
        assert!(
            restore_control_to_staging(&bundle, &staging, &wrong, &[]).is_err(),
            "bundle must never supply its own expected head"
        );
        assert!(!staging.exists());

        let head = bundle.join(CONTROL_HEAD_NAME);
        fs::write(&head, b"tampered").unwrap();
        let tampered_staging = directory.path().join("staging-tampered");
        assert!(
            restore_control_to_staging(&bundle, &tampered_staging, &manifest.checkpoint, &[])
                .is_err()
        );
        assert!(!tampered_staging.exists());
    }

    #[test]
    fn nonempty_and_live_alias_staging_are_rejected_without_live_mutation() {
        let directory = TempDir::new().unwrap();
        let store = control_store(directory.path());
        let bundle = directory.path().join("control-bundle");
        let manifest = backup_control(&store, &bundle).unwrap();
        let live_head_before = fs::read(store.head_path()).unwrap();
        let live_db_before = file_sha256(store.db_path()).unwrap();

        let nonempty = directory.path().join("nonempty");
        fs::create_dir(&nonempty).unwrap();
        fs::write(nonempty.join("keep"), b"user-owned").unwrap();
        assert!(restore_control_to_staging(&bundle, &nonempty, &manifest.checkpoint, &[]).is_err());
        assert_eq!(fs::read(nonempty.join("keep")).unwrap(), b"user-owned");

        let live_parent = store.db_path().parent().unwrap().to_path_buf();
        assert!(
            restore_control_to_staging(
                &bundle,
                &live_parent,
                &manifest.checkpoint,
                std::slice::from_ref(&live_parent)
            )
            .is_err()
        );
        assert_eq!(fs::read(store.head_path()).unwrap(), live_head_before);
        assert_eq!(file_sha256(store.db_path()).unwrap(), live_db_before);
    }

    #[test]
    fn hard_linked_bundle_artifact_is_rejected() {
        let directory = TempDir::new().unwrap();
        let store = control_store(directory.path());
        let bundle = directory.path().join("control-bundle");
        let manifest = backup_control(&store, &bundle).unwrap();
        fs::hard_link(
            bundle.join(CONTROL_HEAD_NAME),
            directory.path().join("linked-control-head.json"),
        )
        .unwrap();
        let staging = directory.path().join("restore-staging");
        assert!(restore_control_to_staging(&bundle, &staging, &manifest.checkpoint, &[]).is_err());
        assert!(!staging.exists());
    }

    #[cfg(unix)]
    #[test]
    fn symlink_staging_is_rejected() {
        use std::os::unix::fs::symlink;
        let directory = TempDir::new().unwrap();
        let store = control_store(directory.path());
        let bundle = directory.path().join("control-bundle");
        let manifest = backup_control(&store, &bundle).unwrap();
        let real = directory.path().join("real-staging");
        fs::create_dir(&real).unwrap();
        let linked = directory.path().join("linked-staging");
        symlink(&real, &linked).unwrap();
        assert!(restore_control_to_staging(&bundle, &linked, &manifest.checkpoint, &[]).is_err());
        assert!(fs::read_dir(real).unwrap().next().is_none());
    }

    #[test]
    fn old_snapshot_replays_completed_erasure_and_clears_pairing_codes() {
        let directory = TempDir::new().unwrap();
        let store = control_store(directory.path());
        let live_health = directory.path().join("live-health.db");
        initialize_health_database(&live_health, &store.store_id().unwrap()).unwrap();
        let health = open_health_database(&live_health).unwrap();
        health
            .execute(
                "INSERT INTO pairing_codes(code_hash,expires_at) VALUES ('pair-hash','2026-09-19T00:10:00Z')",
                [],
            )
            .unwrap();
        health
            .execute(
                "INSERT INTO devices(device_id,token_hash,created_at) VALUES ('phone-old','token-old','2026-09-19T00:00:00Z')",
                [],
            )
            .unwrap();
        health
            .execute(
                "INSERT INTO events(device_id,event_id,revision,operation,kind,health_type,payload_json,payload_hash,updated_at)
                 VALUES ('phone-old','event-old',1,'upsert','quantity','HKQuantityTypeIdentifierBodyMass','{}','payload-hash','2026-09-19T00:00:00Z')",
                [],
            )
            .unwrap();
        drop(health);

        let backup = directory.path().join("boaz-health-synthetic.db");
        snapshot_health(&live_health, &backup);
        let snapshot_id = uuid::Uuid::new_v4().to_string();
        let backup_hash = file_sha256(&backup).unwrap();
        let backup_time = "2026-09-19T00:01:00Z";
        let backup_checkpoint = store.checkpoint().unwrap();
        store
            .append_backup_created(&snapshot_id, &backup_hash, backup_time, &backup_checkpoint)
            .unwrap();
        store
            .append_erasure_intent(
                "phone-old",
                "token-old",
                "erase-old",
                "secret-old",
                "2026-09-19T00:02:00Z",
                "2026-10-19T00:02:00Z",
            )
            .unwrap();
        store
            .append_health_erasure_verified("phone-old", "erase-old", "2026-09-19T00:03:00Z")
            .unwrap();

        let health_manifest = directory.path().join("boaz-health-synthetic.db.meta.json");
        write_new_json(
            &health_manifest,
            &HealthBackupManifest {
                format_version: HEALTH_BACKUP_FORMAT_VERSION,
                snapshot_id: snapshot_id.clone(),
                snapshot_started_at: backup_time.to_owned(),
                source_commit_sequence: None,
                source_schema_version: HEALTH_SCHEMA_VERSION,
                file_sha256: backup_hash,
                control_checkpoint: backup_checkpoint,
            },
        )
        .unwrap();

        let control_bundle = directory.path().join("control-bundle");
        let control_manifest = backup_control(&store, &control_bundle).unwrap();
        let staging_root = directory.path().join("restore-staging");
        let staged = restore_control_to_staging(
            &control_bundle,
            &staging_root,
            &control_manifest.checkpoint,
            &live_paths(&store, &live_health),
        )
        .unwrap();
        let state = restore_health_to_staging(
            &backup,
            &health_manifest,
            &staged,
            &live_paths(&store, &live_health),
        )
        .unwrap();
        assert!(matches!(
            &state,
            HealthRestoreState::ProjectionRebuildRequired { .. }
        ));
        let repeated = restore_health_to_staging(
            &backup,
            &health_manifest,
            &staged,
            &live_paths(&store, &live_health),
        )
        .unwrap();
        assert_eq!(
            state, repeated,
            "restore retry must retain the same epoch/state"
        );
        let control =
            Connection::open_with_flags(staged.store.db_path(), OpenFlags::SQLITE_OPEN_READ_ONLY)
                .unwrap();
        let completed: i64 = control
            .query_row(
                "SELECT count(*) FROM control_events WHERE event_type='restore_completed'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(completed, 0, "staging must not fake projection completion");
        drop(control);
        let restored = open_health_database(&staging_root.join("data/health.db")).unwrap();
        for table in [
            "pairing_codes",
            "devices",
            "events",
            "receipts",
            "outbox",
            "audit",
        ] {
            let count: i64 = restored
                .query_row(&format!("SELECT count(*) FROM {table}"), [], |row| {
                    row.get(0)
                })
                .unwrap();
            assert_eq!(count, 0, "{table} was resurrected by restore");
        }
        let erasure: i64 = restored
            .query_row(
                "SELECT count(*) FROM erasures WHERE erasure_id='erase-old'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(erasure, 1);
        drop(restored);

        // Recovery operates only on staged copies; the live fixture remains as
        // it was before the simulated disaster.
        let live = open_health_database(&live_health).unwrap();
        let live_events: i64 = live
            .query_row("SELECT count(*) FROM events", [], |row| row.get(0))
            .unwrap();
        let live_pairings: i64 = live
            .query_row("SELECT count(*) FROM pairing_codes", [], |row| row.get(0))
            .unwrap();
        assert_eq!((live_events, live_pairings), (1, 1));
    }

    #[test]
    fn health_artifact_tamper_is_rejected_before_staging_copy() {
        let directory = TempDir::new().unwrap();
        let store = control_store(directory.path());
        let live_health = directory.path().join("live-health.db");
        initialize_health_database(&live_health, &store.store_id().unwrap()).unwrap();
        let backup = directory.path().join("health-backup.db");
        snapshot_health(&live_health, &backup);
        let snapshot_id = uuid::Uuid::new_v4().to_string();
        let hash = file_sha256(&backup).unwrap();
        let checkpoint = store.checkpoint().unwrap();
        store
            .append_backup_created(&snapshot_id, &hash, "2026-09-19T00:00:00Z", &checkpoint)
            .unwrap();
        let manifest_path = directory.path().join("health-backup.db.meta.json");
        write_new_json(
            &manifest_path,
            &HealthBackupManifest {
                format_version: HEALTH_BACKUP_FORMAT_VERSION,
                snapshot_id,
                snapshot_started_at: "2026-09-19T00:00:00Z".to_owned(),
                source_commit_sequence: None,
                source_schema_version: HEALTH_SCHEMA_VERSION,
                file_sha256: hash,
                control_checkpoint: checkpoint,
            },
        )
        .unwrap();
        let control_bundle = directory.path().join("control-bundle");
        let control_manifest = backup_control(&store, &control_bundle).unwrap();
        let staging_root = directory.path().join("restore-staging");
        let staged = restore_control_to_staging(
            &control_bundle,
            &staging_root,
            &control_manifest.checkpoint,
            &live_paths(&store, &live_health),
        )
        .unwrap();
        let mismatched_manifest_path = directory.path().join("health-backup-wrong-checkpoint.json");
        let mut mismatched: HealthBackupManifest =
            serde_json::from_slice(&fs::read(&manifest_path).unwrap()).unwrap();
        mismatched.control_checkpoint = control_manifest.checkpoint.clone();
        write_new_json(&mismatched_manifest_path, &mismatched).unwrap();
        assert!(
            restore_health_to_staging(
                &backup,
                &mismatched_manifest_path,
                &staged,
                &live_paths(&store, &live_health),
            )
            .is_err(),
            "a merely valid ancestor/tail is not the checkpoint bound to this snapshot"
        );
        assert!(!staging_root.join("data").exists());

        let mut file = OpenOptions::new().append(true).open(&backup).unwrap();
        file.write_all(b"tamper").unwrap();
        file.sync_all().unwrap();
        assert!(
            restore_health_to_staging(
                &backup,
                &manifest_path,
                &staged,
                &live_paths(&store, &live_health),
            )
            .is_err()
        );
        assert!(!staging_root.join("data").exists());
    }
}
