use axum::{
    Router,
    body::Bytes,
    extract::{Path, State},
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
    routing::{get, post},
};
use chrono::{DateTime, Duration, Utc};
use fs2::FileExt;
use rusqlite::{Connection, OptionalExtension, Transaction, TransactionBehavior, params};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::collections::HashSet;
use std::{
    fs::{File, OpenOptions},
    path::{Path as FsPath, PathBuf},
    sync::Arc,
};
use tokio::sync::Notify;

pub mod ack_journal;
pub mod activation;
pub mod adoption;
pub mod control;
pub mod custody;
pub mod database;
pub mod projection;
pub mod recovery;

use control::ControlStore;

const MAX_BATCH_BYTES: usize = 128 * 1024;
const MAX_EVENTS: usize = 200;

#[derive(Clone)]
pub struct ServerState {
    pub db_path: PathBuf,
    pub control_store: Option<ControlStore>,
    /// Required for every newly acknowledged upload. Must reside outside the
    /// health snapshot's failure domain and be opened by the operator first.
    pub ack_journal: Option<Arc<ack_journal::AckJournal>>,
    /// Independently administered off-host custody. No local-file fallback is
    /// accepted for a production acknowledgement.
    pub custody: Option<Arc<dyn custody::CustodyClient>>,
    pub custody_lock_path: Option<PathBuf>,
    pub projection_notify: Arc<Notify>,
    pub upload_enabled: bool,
    pub runtime_guard: Option<RuntimeGuard>,
}

#[derive(Clone)]
pub struct RuntimeGuard {
    pub vm: projection::VmConfig,
    pub data_volume: PathBuf,
    pub control_volume: PathBuf,
    pub backup_volume: PathBuf,
}

impl RuntimeGuard {
    pub fn verified(&self) -> bool {
        projection::native_vm_verified(&self.vm)
            && projection::encrypted_mount_verified(&self.data_volume)
            && projection::encrypted_mount_verified(&self.control_volume)
            && projection::encrypted_mount_verified(&self.backup_volume)
            && projection::encrypted_mount_verified(&self.vm.storage)
            && separate_recovery_domains(&self.data_volume, &self.control_volume)
    }
}

#[cfg(unix)]
fn separate_recovery_domains(first: &FsPath, second: &FsPath) -> bool {
    use std::os::unix::fs::MetadataExt;
    let Ok(first) = std::fs::metadata(first) else {
        return false;
    };
    let Ok(second) = std::fs::metadata(second) else {
        return false;
    };
    first.dev() != second.dev()
}

#[cfg(not(unix))]
fn separate_recovery_domains(_first: &FsPath, _second: &FsPath) -> bool {
    false
}

impl ServerState {
    fn accepts_upload(&self) -> bool {
        self.upload_enabled
            && self
                .ack_journal
                .as_ref()
                .is_some_and(|journal| journal.baseline().ok().flatten().is_some())
            && self.custody.is_some()
            && self.custody_lock_path.is_some()
            && self
                .runtime_guard
                .as_ref()
                .is_none_or(RuntimeGuard::verified)
    }
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Batch {
    pub schema_version: u8,
    pub batch_id: String,
    pub device_id: String,
    pub events: Vec<HealthEvent>,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct HealthEvent {
    pub event_id: String,
    pub revision: i64,
    pub operation: Operation,
    pub kind: EventKind,
    #[serde(rename = "type")]
    pub health_type: String,
    pub source: Option<Source>,
    pub start_utc: Option<String>,
    pub end_utc: Option<String>,
    pub value: Option<f64>,
    pub unit: Option<String>,
    #[serde(default)]
    pub metadata: Value,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Source {
    pub bundle_id: String,
    pub name: String,
}

#[derive(Debug, Clone, Copy, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum Operation {
    Upsert,
    Delete,
}

#[derive(Debug, Clone, Copy, Deserialize, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum EventKind {
    Quantity,
    Category,
    Workout,
    Activity,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Receipt {
    pub batch_id: String,
    pub status: String,
    pub accepted_events: usize,
    pub changed_events: usize,
    pub content_hash: String,
    pub commit_sequence: i64,
    pub received_at: String,
    pub projected_at: Option<String>,
}

#[derive(Debug)]
pub struct ApiError {
    pub status: StatusCode,
    pub code: &'static str,
    pub message: &'static str,
}

impl ApiError {
    fn new(status: StatusCode, code: &'static str, message: &'static str) -> Self {
        Self {
            status,
            code,
            message,
        }
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        (
            self.status,
            axum::Json(json!({"error": self.code, "message": self.message})),
        )
            .into_response()
    }
}

fn bad(message: &'static str) -> ApiError {
    ApiError::new(StatusCode::BAD_REQUEST, "invalid_request", message)
}
fn unauthorized() -> ApiError {
    ApiError::new(
        StatusCode::UNAUTHORIZED,
        "unauthorized",
        "Valid device token required",
    )
}
fn disabled() -> ApiError {
    ApiError::new(
        StatusCode::SERVICE_UNAVAILABLE,
        "upload_disabled",
        "Upload gate is closed",
    )
}
fn internal() -> ApiError {
    ApiError::new(
        StatusCode::INTERNAL_SERVER_ERROR,
        "internal_error",
        "Request could not be completed",
    )
}

fn journal_unavailable() -> ApiError {
    ApiError::new(
        StatusCode::SERVICE_UNAVAILABLE,
        "recovery_journal_unavailable",
        "Durable recovery journal is unavailable",
    )
}

fn custody_unavailable() -> ApiError {
    ApiError::new(
        StatusCode::SERVICE_UNAVAILABLE,
        "custody_unavailable",
        "Independent acknowledgement custody is unavailable",
    )
}

fn read_checked_custody(
    state: &ServerState,
    journal: &ack_journal::AckJournal,
) -> Result<custody::CustodyState, ApiError> {
    let store = state
        .control_store
        .as_ref()
        .ok_or_else(custody_unavailable)?;
    store
        .verify_custody_while_locked()
        .map_err(|_| custody_unavailable())?;
    let local_control = store.checkpoint().map_err(|_| custody_unavailable())?;
    let remote = state
        .custody
        .as_ref()
        .ok_or_else(custody_unavailable)?
        .read_v2(&local_control.store_id)
        .map_err(|_| custody_unavailable())?;
    if remote.control != local_control
        || remote.baseline_sha256
            != journal
                .baseline_sha256()
                .map_err(|_| custody_unavailable())?
        || !remote
            .ack
            .as_ref()
            .is_some_and(|head| journal.contains_checkpoint(head).unwrap_or(false))
    {
        return Err(custody_unavailable());
    }
    Ok(remote)
}

fn settled_custody(
    state: &ServerState,
    journal: &ack_journal::AckJournal,
) -> Result<custody::CustodyState, ApiError> {
    if journal
        .pending_custody_intent()
        .map_err(|_| custody_unavailable())?
        .is_some()
    {
        return Err(custody_unavailable());
    }
    let remote = read_checked_custody(state, journal)?;
    let local = journal.checkpoint().map_err(|_| custody_unavailable())?;
    let anchored = remote.ack.as_ref().ok_or_else(custody_unavailable)?;
    if anchored.confirmation_sequence != local.confirmation_sequence
        || anchored.pairing_confirmation_sequence != local.pairing_confirmation_sequence
    {
        return Err(custody_unavailable());
    }
    Ok(remote)
}

/// Re-enter exactly one durable pending confirmation. This is called only
/// while holding the common health-operation and custody-operation locks.
/// No receipt or plaintext token is returned until the off-host state is the
/// exact intended successor.
fn reconcile_ack_custody(
    state: &ServerState,
    journal: &ack_journal::AckJournal,
    connection: &Connection,
) -> Result<custody::CustodyState, ApiError> {
    let Some(pending) = journal
        .pending_custody_intent()
        .map_err(|_| custody_unavailable())?
    else {
        return settled_custody(state, journal);
    };
    let client = state.custody.as_ref().ok_or_else(custody_unavailable)?;
    // The off-host owner refuses a plain read while a reservation is still
    // pending. In that case retry the *same* immutable intent rather than
    // interpreting the transport error as a new empty reservation.
    let remote = client.read_v2(&pending.predecessor.control.store_id).ok();
    if remote
        .as_ref()
        .is_some_and(|head| head != &pending.predecessor && head != &pending.successor)
    {
        return Err(custody_unavailable());
    }
    match &pending.confirmation {
        ack_journal::AckCustodyKind::Batch { prepared, receipt } => {
            let stored = read_receipt(connection, &prepared.device_id, &prepared.batch_id)
                .map_err(|_| custody_unavailable())?
                .ok_or_else(custody_unavailable)?;
            if stored.content_hash != prepared.content_hash
                || durable_receipt(connection, &prepared.device_id, &stored)
                    .map_err(|_| custody_unavailable())?
                    != *receipt
                || journal
                    .raw_batch(&prepared.batch_id)
                    .map_err(|_| custody_unavailable())?
                    .is_none_or(|raw| digest(&raw) != prepared.content_hash)
            {
                return Err(custody_unavailable());
            }
        }
        ack_journal::AckCustodyKind::Pairing { pairing } => {
            let match_count: i64 = connection
                .query_row(
                    "SELECT count(*) FROM devices WHERE device_id=?1 AND token_hash=?2 AND created_at=?3 AND revoked_at IS NULL",
                    params![pairing.device_id, pairing.token_hash, pairing.paired_at],
                    |row| row.get(0),
                )
                .map_err(|_| custody_unavailable())?;
            if match_count != 1 {
                return Err(custody_unavailable());
            }
        }
    }
    if remote.as_ref() != Some(&pending.successor) {
        let reservation = client
            .reserve_v2(
                &pending.predecessor,
                &pending.operation_id,
                &pending.intent_sha256().map_err(|_| custody_unavailable())?,
            )
            .map_err(|_| custody_unavailable())?;
        match &pending.confirmation {
            ack_journal::AckCustodyKind::Batch { prepared, receipt } => journal
                .confirm_batch(prepared, receipt)
                .map_err(|_| custody_unavailable())?,
            ack_journal::AckCustodyKind::Pairing { pairing } => journal
                .confirm_pairing(pairing)
                .map_err(|_| custody_unavailable())?,
        }
        if journal
            .checkpoint()
            .map_err(|_| custody_unavailable())?
            .head_sha256
            != pending
                .successor
                .ack
                .as_ref()
                .ok_or_else(custody_unavailable)?
                .head_sha256
        {
            return Err(custody_unavailable());
        }
        match client.compare_and_swap_v2(&reservation, &pending.successor) {
            Ok(confirmed) if confirmed == pending.successor => {}
            _ if client
                .read_v2(&pending.predecessor.control.store_id)
                .map_err(|_| custody_unavailable())?
                == pending.successor => {}
            _ => return Err(custody_unavailable()),
        }
    } else if journal.checkpoint().map_err(|_| custody_unavailable())?
        != *pending
            .successor
            .ack
            .as_ref()
            .ok_or_else(custody_unavailable)?
    {
        return Err(custody_unavailable());
    }
    // A CAS reply is not a durability receipt. Keep the original intent until
    // an independent read proves that the custodian stored the exact successor;
    // otherwise a false success would make an interrupted confirmation
    // impossible to re-enter with its original operation ID.
    if client
        .read_v2(&pending.predecessor.control.store_id)
        .map_err(|_| custody_unavailable())?
        != pending.successor
    {
        return Err(custody_unavailable());
    }
    journal
        .finish_custody_intent(&pending, &pending.successor)
        .map_err(|_| custody_unavailable())?;
    settled_custody(state, journal)
}

pub fn router(state: ServerState) -> Router {
    Router::new()
        .route("/v1/health/pairings", post(pair))
        .route("/v1/health/batches", post(ingest))
        .route("/v1/health/batches/{batch_id}/receipt", get(receipt))
        .route("/v1/health/status", get(status))
        .route("/v1/health/revoke", post(revoke))
        .route("/v1/health/erase", post(erase))
        .route("/v1/health/erasures/{device_id}", get(erasure_status))
        .with_state(Arc::new(state))
}

pub fn open_db(path: &FsPath) -> database::StorageResult<Connection> {
    database::open_health_database(path)
}

pub fn operation_lock(path: &FsPath, try_only: bool) -> std::io::Result<File> {
    let parent = path.parent().ok_or_else(|| {
        std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "Database path has no parent",
        )
    })?;
    let lock_path = parent.join("health-operations.lock");
    let mut options = OpenOptions::new();
    options.read(true).write(true).create(true).truncate(false);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600).custom_flags(libc::O_NOFOLLOW);
    }
    let file = options.open(&lock_path)?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::{MetadataExt, PermissionsExt};
        let opened = file.metadata()?;
        let named = std::fs::symlink_metadata(&lock_path)?;
        if !opened.file_type().is_file()
            || opened.nlink() != 1
            || named.file_type().is_symlink()
            || opened.dev() != named.dev()
            || opened.ino() != named.ino()
        {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "Health operation lock path is not a unique regular file",
            ));
        }
        file.set_permissions(std::fs::Permissions::from_mode(0o600))?;
    }
    if try_only {
        file.try_lock_exclusive()?;
    } else {
        file.lock_exclusive()?;
    }
    Ok(file)
}

/// This lock is stable across health/control generations. It must be created
/// by the offline coordinator bootstrap; a missing path never gets repaired
/// by an HTTP request.
pub fn custody_operation_lock(path: &FsPath) -> std::io::Result<File> {
    let mut options = OpenOptions::new();
    options.read(true).write(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.custom_flags(libc::O_NOFOLLOW);
    }
    let file = options.open(path)?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::{MetadataExt, PermissionsExt};
        let opened = file.metadata()?;
        let named = std::fs::symlink_metadata(path)?;
        if !opened.file_type().is_file()
            || opened.nlink() != 1
            || named.file_type().is_symlink()
            || opened.dev() != named.dev()
            || opened.ino() != named.ino()
            || opened.permissions().mode() & 0o077 != 0
        {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "Custody-operation lock identity or permissions are invalid",
            ));
        }
    }
    file.lock_exclusive()?;
    Ok(file)
}

/// Coordinates the lifetime of the running receiver with offline storage
/// operations. `serve` holds a shared lock for its whole lifetime; restore or
/// migration tooling must take the exclusive form before touching staged or
/// live storage. The caller owns the returned file and therefore the lock.
pub fn lifecycle_lock(path: &FsPath, exclusive: bool, try_only: bool) -> std::io::Result<File> {
    let parent = path.parent().ok_or_else(|| {
        std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "Database path has no parent",
        )
    })?;
    let lock_path = parent.join("storage-lifecycle.lock");
    let mut options = OpenOptions::new();
    options.read(true).write(true).create(true).truncate(false);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600).custom_flags(libc::O_NOFOLLOW);
    }
    let file = options.open(&lock_path)?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::{MetadataExt, PermissionsExt};
        let opened = file.metadata()?;
        let named = std::fs::symlink_metadata(&lock_path)?;
        if !opened.file_type().is_file()
            || opened.nlink() != 1
            || named.file_type().is_symlink()
            || opened.dev() != named.dev()
            || opened.ino() != named.ino()
        {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "Storage lifecycle lock path is not a unique regular file",
            ));
        }
        file.set_permissions(std::fs::Permissions::from_mode(0o600))?;
    }
    match (exclusive, try_only) {
        (true, true) => file.try_lock_exclusive()?,
        (true, false) => file.lock_exclusive()?,
        (false, true) => file.try_lock_shared()?,
        (false, false) => file.lock_shared()?,
    }
    Ok(file)
}

fn digest(value: &[u8]) -> String {
    hex::encode(Sha256::digest(value))
}

fn random_secret() -> String {
    let mut bytes = [0_u8; 32];
    bytes[..16].copy_from_slice(uuid::Uuid::new_v4().as_bytes());
    bytes[16..].copy_from_slice(uuid::Uuid::new_v4().as_bytes());
    hex::encode(bytes)
}

fn valid_id(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 128
        && value
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b"-_.:".contains(&b))
}

pub fn revoke_device(connection: &mut Connection, device_id: &str) -> rusqlite::Result<bool> {
    if !valid_id(device_id) {
        return Ok(false);
    }
    let transaction = connection.transaction()?;
    let changed = transaction.execute(
        "UPDATE devices SET revoked_at=?2 WHERE device_id=?1 AND revoked_at IS NULL",
        params![device_id, Utc::now().to_rfc3339()],
    )?;
    if changed > 0 {
        transaction.execute(
            "INSERT INTO audit(device_id,action,at) VALUES (?1,'credential_revoked',?2)",
            params![device_id, Utc::now().to_rfc3339()],
        )?;
    }
    transaction.commit()?;
    Ok(changed > 0)
}

fn valid_type(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 96
        && value
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b"._".contains(&b))
}

fn parse_time(value: &str) -> Result<DateTime<Utc>, ApiError> {
    DateTime::parse_from_rfc3339(value)
        .map(|time| time.with_timezone(&Utc))
        .map_err(|_| bad("Times must be RFC3339 UTC timestamps"))
}

pub(crate) fn validate_batch(batch: &Batch) -> Result<(), ApiError> {
    if batch.schema_version != 1 || !valid_id(&batch.batch_id) || !valid_id(&batch.device_id) {
        return Err(bad("Unsupported schema or invalid batch/device ID"));
    }
    if batch.events.is_empty() || batch.events.len() > MAX_EVENTS {
        return Err(bad("Batch must contain 1 to 200 events"));
    }
    let mut seen = HashSet::new();
    for event in &batch.events {
        if !valid_id(&event.event_id) || !valid_type(&event.health_type) || event.revision < 0 {
            return Err(bad("Invalid event ID, type, or revision"));
        }
        if !seen.insert(&event.event_id) {
            return Err(bad("Duplicate event ID in batch"));
        }
        if let Some(source) = &event.source
            && (source.bundle_id.len() > 256 || source.name.len() > 256)
        {
            return Err(bad("Source is too long"));
        }
        if event.unit.as_ref().is_some_and(|unit| unit.len() > 64) {
            return Err(bad("Unit is too long"));
        }
        if event.value.is_some_and(|value| !value.is_finite()) {
            return Err(bad("Non-finite value"));
        }
        if !event.metadata.is_object() && !event.metadata.is_null() {
            return Err(bad("Metadata must be an object"));
        }
        if event.operation == Operation::Upsert {
            if event.source.is_none() || event.start_utc.is_none() || event.end_utc.is_none() {
                return Err(bad("Upserts require source and UTC times"));
            }
            let start = parse_time(event.start_utc.as_deref().unwrap_or_default())?;
            let end = parse_time(event.end_utc.as_deref().unwrap_or_default())?;
            if start > end {
                return Err(bad("Event end precedes start"));
            }
            if matches!(event.kind, EventKind::Quantity | EventKind::Activity)
                && (event.value.is_none() || event.unit.is_none())
            {
                return Err(bad("Quantity/activity upserts require value and unit"));
            }
            if event.health_type == "boaz.sleep.deep_minutes" {
                let valid_derived = matches!(event.kind, EventKind::Quantity)
                    && event.unit.as_deref() == Some("min")
                    && event
                        .value
                        .is_some_and(|value| (0.0..=1440.0).contains(&value))
                    && event
                        .source
                        .as_ref()
                        .is_some_and(|source| source.bundle_id == "boazapp")
                    && event
                        .metadata
                        .get("derivation_version")
                        .is_some_and(|version| {
                            version.as_u64() == Some(1) || version.as_str() == Some("1")
                        });
                if !valid_derived {
                    return Err(bad("Invalid versioned sleep derivation"));
                }
            }
        }
    }
    Ok(())
}

pub(crate) fn metric_for(event: &HealthEvent) -> Option<&'static str> {
    if event.operation == Operation::Delete {
        return metric_for_type(&event.health_type, None);
    }
    metric_for_type(&event.health_type, event.unit.as_deref())
}

fn metric_for_type(health_type: &str, unit: Option<&str>) -> Option<&'static str> {
    match (health_type, unit) {
        ("HKQuantityTypeIdentifierHeartRate" | "heartRate", Some("count/min" | "beats/min")) => {
            Some("boaz_health_v1_heart_rate_bpm")
        }
        (
            "HKQuantityTypeIdentifierRestingHeartRate" | "restingHeartRate",
            Some("count/min" | "beats/min"),
        ) => Some("boaz_health_v1_resting_heart_rate_bpm"),
        ("HKQuantityTypeIdentifierBodyMass" | "bodyMass", Some("kg")) => {
            Some("boaz_health_v1_weight_kg")
        }
        ("HKQuantityTypeIdentifierOxygenSaturation" | "oxygenSaturation", Some("%")) => {
            Some("boaz_health_v1_spo2_percent")
        }
        ("HKQuantityTypeIdentifierStepCount" | "stepCount", Some("count")) => {
            Some("boaz_health_v1_step_count")
        }
        ("HKQuantityTypeIdentifierFlightsClimbed" | "flightsClimbed", Some("count")) => {
            Some("boaz_health_v1_flights_climbed_count")
        }
        ("HKQuantityTypeIdentifierActiveEnergyBurned" | "activeEnergyBurned", Some("kcal")) => {
            Some("boaz_health_v1_active_energy_kcal")
        }
        ("HKQuantityTypeIdentifierAppleExerciseTime" | "appleExerciseTime", Some("min")) => {
            Some("boaz_health_v1_exercise_time_min")
        }
        ("HKQuantityTypeIdentifierAppleStandTime" | "appleStandTime", Some("min")) => {
            Some("boaz_health_v1_stand_time_min")
        }
        (
            "HKQuantityTypeIdentifierAppleSleepingWristTemperature"
            | "appleSleepingWristTemperature",
            Some("Cel" | "degC"),
        ) => Some("boaz_health_v1_wrist_temperature_celsius"),
        (
            "HKQuantityTypeIdentifierRespiratoryRate" | "respiratoryRate",
            Some("count/min" | "breaths/min"),
        ) => Some("boaz_health_v1_respiratory_rate_per_min"),
        (
            "HKQuantityTypeIdentifierHeartRateVariabilitySDNN" | "heartRateVariabilitySDNN",
            Some("ms"),
        ) => Some("boaz_health_v1_hrv_sdnn_ms"),
        (
            "HKQuantityTypeIdentifierBloodPressureSystolic" | "bloodPressureSystolic",
            Some("mmHg"),
        ) => Some("boaz_health_v1_blood_pressure_systolic_mmhg"),
        (
            "HKQuantityTypeIdentifierBloodPressureDiastolic" | "bloodPressureDiastolic",
            Some("mmHg"),
        ) => Some("boaz_health_v1_blood_pressure_diastolic_mmhg"),
        ("HKQuantityTypeIdentifierBodyFatPercentage" | "bodyFatPercentage", Some("%")) => {
            Some("boaz_health_v1_body_fat_percent")
        }
        ("HKQuantityTypeIdentifierBodyMassIndex" | "bodyMassIndex", Some("count")) => {
            Some("boaz_health_v1_bmi")
        }
        (
            "HKQuantityTypeIdentifierDistanceWalkingRunning" | "distanceWalkingRunning",
            Some("m"),
        ) => Some("boaz_health_v1_walk_run_distance_m"),
        ("HKQuantityTypeIdentifierDistanceCycling" | "distanceCycling", Some("m")) => {
            Some("boaz_health_v1_cycle_distance_m")
        }
        ("HKQuantityTypeIdentifierDistanceSwimming" | "distanceSwimming", Some("m")) => {
            Some("boaz_health_v1_swim_distance_m")
        }
        ("HKActivitySummaryMove" | "activity.move", Some("kcal")) => {
            Some("boaz_health_v1_activity_move_kcal")
        }
        ("HKActivitySummaryExercise" | "activity.exercise", Some("min")) => {
            Some("boaz_health_v1_activity_exercise_min")
        }
        ("HKActivitySummaryStand" | "activity.stand", Some("h" | "hours")) => {
            Some("boaz_health_v1_activity_stand_hours")
        }
        ("boaz.sleep.deep_minutes", Some("min")) => Some("boaz_health_v1_deep_sleep_min"),
        ("HKQuantityTypeIdentifierHeartRate" | "heartRate", None) => {
            Some("boaz_health_v1_heart_rate_bpm")
        }
        ("HKQuantityTypeIdentifierRestingHeartRate" | "restingHeartRate", None) => {
            Some("boaz_health_v1_resting_heart_rate_bpm")
        }
        ("HKQuantityTypeIdentifierBodyMass" | "bodyMass", None) => Some("boaz_health_v1_weight_kg"),
        ("HKQuantityTypeIdentifierOxygenSaturation" | "oxygenSaturation", None) => {
            Some("boaz_health_v1_spo2_percent")
        }
        ("HKQuantityTypeIdentifierStepCount" | "stepCount", None) => {
            Some("boaz_health_v1_step_count")
        }
        ("HKQuantityTypeIdentifierFlightsClimbed" | "flightsClimbed", None) => {
            Some("boaz_health_v1_flights_climbed_count")
        }
        ("HKQuantityTypeIdentifierActiveEnergyBurned" | "activeEnergyBurned", None) => {
            Some("boaz_health_v1_active_energy_kcal")
        }
        ("HKQuantityTypeIdentifierAppleExerciseTime" | "appleExerciseTime", None) => {
            Some("boaz_health_v1_exercise_time_min")
        }
        ("HKQuantityTypeIdentifierAppleStandTime" | "appleStandTime", None) => {
            Some("boaz_health_v1_stand_time_min")
        }
        (
            "HKQuantityTypeIdentifierAppleSleepingWristTemperature"
            | "appleSleepingWristTemperature",
            None,
        ) => Some("boaz_health_v1_wrist_temperature_celsius"),
        ("HKQuantityTypeIdentifierRespiratoryRate" | "respiratoryRate", None) => {
            Some("boaz_health_v1_respiratory_rate_per_min")
        }
        ("HKQuantityTypeIdentifierHeartRateVariabilitySDNN" | "heartRateVariabilitySDNN", None) => {
            Some("boaz_health_v1_hrv_sdnn_ms")
        }
        ("HKQuantityTypeIdentifierBloodPressureSystolic" | "bloodPressureSystolic", None) => {
            Some("boaz_health_v1_blood_pressure_systolic_mmhg")
        }
        ("HKQuantityTypeIdentifierBloodPressureDiastolic" | "bloodPressureDiastolic", None) => {
            Some("boaz_health_v1_blood_pressure_diastolic_mmhg")
        }
        ("HKQuantityTypeIdentifierBodyFatPercentage" | "bodyFatPercentage", None) => {
            Some("boaz_health_v1_body_fat_percent")
        }
        ("HKQuantityTypeIdentifierBodyMassIndex" | "bodyMassIndex", None) => {
            Some("boaz_health_v1_bmi")
        }
        ("HKQuantityTypeIdentifierDistanceWalkingRunning" | "distanceWalkingRunning", None) => {
            Some("boaz_health_v1_walk_run_distance_m")
        }
        ("HKQuantityTypeIdentifierDistanceCycling" | "distanceCycling", None) => {
            Some("boaz_health_v1_cycle_distance_m")
        }
        ("HKQuantityTypeIdentifierDistanceSwimming" | "distanceSwimming", None) => {
            Some("boaz_health_v1_swim_distance_m")
        }
        ("HKActivitySummaryMove" | "activity.move", None) => {
            Some("boaz_health_v1_activity_move_kcal")
        }
        ("HKActivitySummaryExercise" | "activity.exercise", None) => {
            Some("boaz_health_v1_activity_exercise_min")
        }
        ("HKActivitySummaryStand" | "activity.stand", None) => {
            Some("boaz_health_v1_activity_stand_hours")
        }
        ("boaz.sleep.deep_minutes", None) => Some("boaz_health_v1_deep_sleep_min"),
        _ => None,
    }
}

#[derive(Debug)]
struct AuthenticatedDevice {
    device_id: String,
    token_hash: String,
}

fn auth_device(
    state: &ServerState,
    connection: &Connection,
    headers: &HeaderMap,
) -> Result<AuthenticatedDevice, ApiError> {
    let token = headers
        .get("authorization")
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.strip_prefix("Bearer "))
        .filter(|token| token.len() == 64 && token.bytes().all(|byte| byte.is_ascii_hexdigit()))
        .ok_or_else(unauthorized)?;
    let token_hash = digest(token.as_bytes());
    if state
        .control_store
        .as_ref()
        .is_some_and(|store| store.token_tombstoned(&token_hash).unwrap_or(true))
    {
        return Err(unauthorized());
    }
    let device_id = connection
        .query_row(
            "SELECT device_id FROM devices WHERE token_hash=?1 AND revoked_at IS NULL",
            params![&token_hash],
            |row| row.get(0),
        )
        .optional()
        .map_err(|_| internal())?
        .ok_or_else(unauthorized)?;
    Ok(AuthenticatedDevice {
        device_id,
        token_hash,
    })
}

pub fn create_pairing_code(connection: &Connection) -> rusqlite::Result<String> {
    let code = random_secret();
    let expires = (Utc::now() + Duration::minutes(10)).to_rfc3339();
    connection.execute(
        "INSERT INTO pairing_codes(code_hash, expires_at) VALUES (?1, ?2)",
        params![digest(code.as_bytes()), expires],
    )?;
    Ok(code)
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct PairRequest {
    code: String,
    device_id: String,
}

async fn pair(
    State(state): State<Arc<ServerState>>,
    body: Bytes,
) -> Result<impl IntoResponse, ApiError> {
    if !state.accepts_upload() {
        return Err(disabled());
    }
    if body.len() > 2048 {
        return Err(bad("Pairing request too large"));
    }
    let request: PairRequest =
        serde_json::from_slice(&body).map_err(|_| bad("Invalid pairing JSON"))?;
    if !valid_id(&request.device_id)
        || request.code.len() != 64
        || !request.code.bytes().all(|b| b.is_ascii_hexdigit())
    {
        return Err(bad("Invalid pairing credentials"));
    }
    // Pairing and erasure must serialize on the same health-ledger lifecycle
    // lock. Otherwise an erasure intent could retire the device in the control
    // store after this handler checks it but before the new credential commits.
    let _operation_guard = operation_lock(&state.db_path, false).map_err(|_| {
        ApiError::new(
            StatusCode::SERVICE_UNAVAILABLE,
            "operation_busy",
            "Backup, pairing, or erasure operation is active",
        )
    })?;
    let journal = state.ack_journal.as_ref().ok_or_else(journal_unavailable)?;
    let _custody_guard = custody_operation_lock(
        state
            .custody_lock_path
            .as_deref()
            .ok_or_else(custody_unavailable)?,
    )
    .map_err(|_| custody_unavailable())?;
    if state.control_store.as_ref().is_some_and(|store| {
        store
            .device_erasure_tombstoned(&request.device_id)
            .unwrap_or(true)
    }) {
        return Err(ApiError::new(
            StatusCode::CONFLICT,
            "erasure_identity_retired",
            "Erased device identity cannot be reused",
        ));
    }
    let mut connection = open_db(&state.db_path).map_err(|_| internal())?;
    let custody_predecessor = reconcile_ack_custody(&state, journal, &connection)?;
    // Acquire the write reservation before reading so simultaneous uses of one
    // pairing code serialize instead of failing a deferred read-to-write upgrade.
    let tx = connection
        .transaction_with_behavior(TransactionBehavior::Immediate)
        .map_err(|_| internal())?;
    let pending_erasure: bool = tx.query_row("SELECT EXISTS(SELECT 1 FROM erasures WHERE device_id=?1 AND metrics_deleted_at IS NULL)", params![request.device_id], |row| row.get(0)).map_err(|_| internal())?;
    if pending_erasure {
        return Err(ApiError::new(
            StatusCode::CONFLICT,
            "erasure_pending",
            "Device erasure is pending",
        ));
    }
    let used = tx.execute(
        "UPDATE pairing_codes SET used_at=?1 WHERE code_hash=?2 AND used_at IS NULL AND expires_at>?1",
        params![Utc::now().to_rfc3339(), digest(request.code.as_bytes())],
    ).map_err(|_| internal())?;
    if used != 1 {
        return Err(unauthorized());
    }
    let token = random_secret();
    let token_hash = digest(token.as_bytes());
    let paired_at = Utc::now().to_rfc3339();
    let changed = tx.execute(
        "INSERT INTO devices(device_id, token_hash, created_at) VALUES (?1, ?2, ?3) ON CONFLICT(device_id) DO UPDATE SET token_hash=excluded.token_hash, created_at=excluded.created_at, revoked_at=NULL WHERE devices.revoked_at IS NOT NULL AND NOT EXISTS (SELECT 1 FROM erasures WHERE erasures.device_id=devices.device_id AND metrics_deleted_at IS NULL)",
        params![request.device_id, token_hash, paired_at],
    );
    if !matches!(changed, Ok(1)) {
        return Err(ApiError::new(
            StatusCode::CONFLICT,
            "device_exists",
            "Device already paired",
        ));
    }
    tx.execute(
        "INSERT INTO audit(device_id, action, at) VALUES (?1, 'paired', ?2)",
        params![request.device_id, Utc::now().to_rfc3339()],
    )
    .map_err(|_| internal())?;
    let pairing = journal
        .prepare_pairing(
            &request.device_id,
            &digest(request.code.as_bytes()),
            &token_hash,
            &paired_at,
        )
        .map_err(|_| journal_unavailable())?;
    tx.commit().map_err(|_| internal())?;
    // Never disclose the token if its recovery identity was not durably
    // confirmed. The client must obtain a new pairing code if interrupted.
    journal
        .start_pairing_custody_intent(&pairing, &custody_predecessor)
        .map_err(|_| custody_unavailable())?;
    reconcile_ack_custody(&state, journal, &connection)?;
    Ok((
        StatusCode::CREATED,
        axum::Json(json!({"device_id": request.device_id, "token": token})),
    ))
}

fn read_receipt(
    connection: &Connection,
    device_id: &str,
    batch_id: &str,
) -> rusqlite::Result<Option<Receipt>> {
    connection.query_row(
        "SELECT r.batch_id,r.content_hash,r.accepted_events,r.changed_events,r.commit_sequence,r.received_at,r.projected_at,r.requires_projection,r.projected_generation,r.projection_mapping_version,p.generation_id,p.mapping_version
         FROM receipts r CROSS JOIN projection_state p
         WHERE p.singleton=1 AND r.device_id=?1 AND r.batch_id=?2",
        params![device_id, batch_id],
        |row| {
            let stored_projected_at: Option<String> = row.get(6)?;
            let requires_projection: bool = row.get(7)?;
            let projected_generation: Option<String> = row.get(8)?;
            let projected_mapping_version: Option<i64> = row.get(9)?;
            let current_generation: String = row.get(10)?;
            let current_mapping_version: i64 = row.get(11)?;
            let projection_is_current = stored_projected_at.is_some()
                && (!requires_projection
                    || (projected_generation.as_deref() == Some(current_generation.as_str())
                        && projected_mapping_version == Some(current_mapping_version)));
            let projected_at = projection_is_current
                .then_some(stored_projected_at)
                .flatten();
            Ok(Receipt {
                batch_id: row.get(0)?,
                status: if projection_is_current { "metrics_current" } else { "cloud_saved" }.to_owned(),
                content_hash: row.get(1)?,
                accepted_events: row.get::<_, i64>(2)? as usize,
                changed_events: row.get::<_, i64>(3)? as usize,
                commit_sequence: row.get(4)?,
                received_at: row.get(5)?,
                projected_at,
            })
        },
    ).optional()
}

fn durable_receipt(
    connection: &Connection,
    device_id: &str,
    receipt: &Receipt,
) -> rusqlite::Result<ack_journal::AckReceipt> {
    let requires_projection: bool = connection.query_row(
        "SELECT requires_projection FROM receipts WHERE device_id=?1 AND batch_id=?2",
        params![device_id, receipt.batch_id],
        |row| row.get(0),
    )?;
    Ok(ack_journal::AckReceipt {
        commit_sequence: receipt.commit_sequence,
        received_at: receipt.received_at.clone(),
        accepted_events: receipt.accepted_events as i64,
        changed_events: receipt.changed_events as i64,
        requires_projection,
    })
}

pub(crate) fn save_event(
    tx: &Transaction<'_>,
    device_id: &str,
    event: &HealthEvent,
    now: &str,
) -> Result<(bool, Vec<&'static str>), ApiError> {
    let bytes = serde_json::to_vec(event).map_err(|_| bad("Invalid event JSON"))?;
    let payload_hash = digest(&bytes);
    let previous: Option<(i64, String, String)> = tx.query_row(
        "SELECT revision, payload_hash, payload_json FROM events WHERE device_id=?1 AND event_id=?2",
        params![device_id, event.event_id],
        |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
    ).optional().map_err(|_| internal())?;
    let mut metrics = Vec::new();
    if let Some((revision, old_hash, old_json)) = previous {
        if event.revision < revision {
            return Ok((false, metrics));
        }
        if event.revision == revision {
            if old_hash == payload_hash {
                return Ok((false, metrics));
            }
            return Err(ApiError::new(
                StatusCode::CONFLICT,
                "revision_conflict",
                "Same event revision has different content",
            ));
        }
        let old: HealthEvent = serde_json::from_str(&old_json).map_err(|_| internal())?;
        if let Some(metric) = metric_for(&old) {
            metrics.push(metric);
        }
    }
    if let Some(metric) = metric_for(event)
        && !metrics.contains(&metric)
    {
        metrics.push(metric);
    }
    tx.execute(
        "INSERT INTO events(device_id,event_id,revision,operation,kind,health_type,source_json,start_utc,end_utc,value,unit,payload_json,payload_hash,updated_at) VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13,?14) ON CONFLICT(device_id,event_id) DO UPDATE SET revision=excluded.revision,operation=excluded.operation,kind=excluded.kind,health_type=excluded.health_type,source_json=excluded.source_json,start_utc=excluded.start_utc,end_utc=excluded.end_utc,value=excluded.value,unit=excluded.unit,payload_json=excluded.payload_json,payload_hash=excluded.payload_hash,updated_at=excluded.updated_at",
        params![device_id, event.event_id, event.revision, format!("{:?}", event.operation).to_lowercase(), format!("{:?}", event.kind).to_lowercase(), event.health_type,
            event.source.as_ref().map(|source| serde_json::to_string(source).unwrap_or_default()), event.start_utc, event.end_utc, event.value, event.unit,
            String::from_utf8(bytes).map_err(|_| internal())?, payload_hash, now],
    ).map_err(|_| internal())?;
    Ok((true, metrics))
}

async fn ingest(
    State(state): State<Arc<ServerState>>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<impl IntoResponse, ApiError> {
    if !state.accepts_upload() {
        return Err(disabled());
    }
    if body.len() > MAX_BATCH_BYTES {
        return Err(ApiError::new(
            StatusCode::PAYLOAD_TOO_LARGE,
            "batch_too_large",
            "Maximum batch size is 128 KiB",
        ));
    }
    let batch: Batch = serde_json::from_slice(&body).map_err(|_| bad("Invalid batch JSON"))?;
    validate_batch(&batch)?;
    let content_hash = digest(&body);
    // An erasure publishes its control tombstone while holding this same lock.
    // Keep the authorization check and SQLite commit on one side of that
    // boundary so an in-flight old token cannot commit after retirement.
    let _operation_guard = operation_lock(&state.db_path, false).map_err(|_| internal())?;
    let journal = state.ack_journal.as_ref().ok_or_else(journal_unavailable)?;
    let _custody_guard = custody_operation_lock(
        state
            .custody_lock_path
            .as_deref()
            .ok_or_else(custody_unavailable)?,
    )
    .map_err(|_| custody_unavailable())?;
    let mut connection = open_db(&state.db_path).map_err(|_| internal())?;
    let custody_predecessor = reconcile_ack_custody(&state, journal, &connection)?;
    let authenticated = auth_device(&state, &connection, &headers)?;
    let device_id = authenticated.device_id;
    if device_id != batch.device_id {
        return Err(unauthorized());
    }
    let tx = connection
        .transaction_with_behavior(TransactionBehavior::Immediate)
        .map_err(|_| internal())?;
    if auth_device(&state, &tx, &headers)?.device_id != device_id {
        return Err(unauthorized());
    }
    if let Some(receipt) = read_receipt(&tx, &device_id, &batch.batch_id).map_err(|_| internal())? {
        if receipt.content_hash == content_hash {
            // A legacy or only-half-committed receipt is not an acknowledged
            // recovery fact. A retry may finish a prepared commit only when
            // the exact request bytes are present in the independent journal.
            let prepared = journal
                .prepared_batch(&batch.batch_id, &device_id, &body)
                .map_err(|_| journal_unavailable())?
                .ok_or_else(journal_unavailable)?;
            let ack_receipt = durable_receipt(&tx, &device_id, &receipt).map_err(|_| internal())?;
            drop(tx);
            if !journal
                .receipt_matches(&batch.batch_id, &device_id, &content_hash, &ack_receipt)
                .map_err(|_| custody_unavailable())?
            {
                journal
                    .start_batch_custody_intent(&prepared, &ack_receipt, &custody_predecessor)
                    .map_err(|_| custody_unavailable())?;
                reconcile_ack_custody(&state, journal, &connection)?;
            }
            return Ok((StatusCode::OK, axum::Json(receipt)));
        }
        return Err(ApiError::new(
            StatusCode::CONFLICT,
            "batch_conflict",
            "Batch ID already used with different content",
        ));
    }
    // Batch IDs are globally unique. Detect another device's collision before
    // writing events, without disclosing its receipt or device identity.
    let batch_id_exists: bool = tx
        .query_row(
            "SELECT EXISTS(SELECT 1 FROM receipts WHERE batch_id=?1)",
            params![batch.batch_id],
            |row| row.get(0),
        )
        .map_err(|_| internal())?;
    if batch_id_exists {
        return Err(ApiError::new(
            StatusCode::CONFLICT,
            "batch_conflict",
            "Batch ID already used with different content",
        ));
    }
    let now = Utc::now().to_rfc3339();
    let mut changed = 0_usize;
    let mut metrics = HashSet::new();
    for event in &batch.events {
        let (saved, touched) = save_event(&tx, &device_id, event, &now)?;
        if saved {
            changed += 1;
        }
        for metric in touched {
            metrics.insert(metric);
        }
    }
    // The deterministic revision validation above may reject a batch. Only
    // after it succeeds do we reserve immutable recovery bytes, but always
    // before the SQLite receipt transaction can commit.
    let prepared = journal
        .prepare_batch(&batch.batch_id, &device_id, &body)
        .map_err(|_| journal_unavailable())?;
    let requires_projection = !metrics.is_empty();
    let projected_at = if requires_projection {
        None
    } else {
        Some(now.as_str())
    };
    tx.execute(
        "INSERT INTO receipts(batch_id,device_id,content_hash,accepted_events,changed_events,requires_projection,received_at,projected_at) VALUES (?1,?2,?3,?4,?5,?6,?7,?8)",
        params![batch.batch_id, device_id, content_hash, batch.events.len() as i64, changed as i64, requires_projection as i64, now, projected_at],
    ).map_err(|_| internal())?;
    for metric in metrics {
        tx.execute(
            "INSERT INTO outbox(device_id,batch_id,metric_name,created_at) VALUES (?1,?2,?3,?4)",
            params![device_id, batch.batch_id, metric, now],
        )
        .map_err(|_| internal())?;
    }
    tx.execute("INSERT INTO audit(device_id,action,batch_id,at,detail) VALUES (?1,'batch_committed',?2,?3,?4)", params![device_id,batch.batch_id,now,format!("accepted={};changed={changed}",batch.events.len())]).map_err(|_| internal())?;
    tx.commit().map_err(|_| internal())?;
    let receipt = read_receipt(&connection, &device_id, &batch.batch_id)
        .map_err(|_| internal())?
        .ok_or_else(internal)?;
    journal
        .start_batch_custody_intent(
            &prepared,
            &durable_receipt(&connection, &device_id, &receipt).map_err(|_| internal())?,
            &custody_predecessor,
        )
        .map_err(|_| custody_unavailable())?;
    reconcile_ack_custody(&state, journal, &connection)?;
    state.projection_notify.notify_one();
    Ok((StatusCode::CREATED, axum::Json(receipt)))
}

async fn receipt(
    State(state): State<Arc<ServerState>>,
    Path(batch_id): Path<String>,
    headers: HeaderMap,
) -> Result<impl IntoResponse, ApiError> {
    let journal = state.ack_journal.as_ref().ok_or_else(journal_unavailable)?;
    let _operation_guard =
        operation_lock(&state.db_path, false).map_err(|_| custody_unavailable())?;
    let _custody_guard = custody_operation_lock(
        state
            .custody_lock_path
            .as_deref()
            .ok_or_else(custody_unavailable)?,
    )
    .map_err(|_| custody_unavailable())?;
    settled_custody(&state, journal)?;
    let connection = open_db(&state.db_path).map_err(|_| internal())?;
    let device_id = auth_device(&state, &connection, &headers)?.device_id;
    let receipt = read_receipt(&connection, &device_id, &batch_id)
        .map_err(|_| internal())?
        .ok_or_else(|| ApiError::new(StatusCode::NOT_FOUND, "not_found", "Receipt not found"))?;
    if !journal
        .receipt_matches(
            &batch_id,
            &device_id,
            &receipt.content_hash,
            &durable_receipt(&connection, &device_id, &receipt).map_err(|_| internal())?,
        )
        .map_err(|_| journal_unavailable())?
    {
        return Err(journal_unavailable());
    }
    Ok(axum::Json(receipt))
}

async fn status(
    State(state): State<Arc<ServerState>>,
    headers: HeaderMap,
) -> Result<impl IntoResponse, ApiError> {
    let _operation_guard =
        operation_lock(&state.db_path, false).map_err(|_| custody_unavailable())?;
    let _custody_guard = custody_operation_lock(
        state
            .custody_lock_path
            .as_deref()
            .ok_or_else(custody_unavailable)?,
    )
    .map_err(|_| custody_unavailable())?;
    settled_custody(
        &state,
        state.ack_journal.as_ref().ok_or_else(journal_unavailable)?,
    )?;
    let connection = open_db(&state.db_path).map_err(|_| internal())?;
    let device_id = auth_device(&state, &connection, &headers)?.device_id;
    let sample_count: i64 = connection
        .query_row(
            "SELECT count(*) FROM events WHERE device_id=?1 AND operation='upsert'",
            params![device_id],
            |row| row.get(0),
        )
        .map_err(|_| internal())?;
    let pending_projection_jobs: i64 = connection
        .query_row(
            "SELECT count(*) FROM outbox WHERE device_id=?1 AND processed_at IS NULL",
            params![device_id],
            |row| row.get(0),
        )
        .map_err(|_| internal())?;
    let latest_commit_sequence: Option<i64> = connection
        .query_row(
            "SELECT max(commit_sequence) FROM receipts WHERE device_id=?1",
            params![device_id],
            |row| row.get(0),
        )
        .map_err(|_| internal())?;
    Ok(axum::Json(
        json!({"schema_version":1,"device_id":device_id,"upload_enabled":state.accepts_upload(),"sample_count":sample_count,"pending_projection_jobs":pending_projection_jobs,"latest_commit_sequence":latest_commit_sequence}),
    ))
}

async fn revoke(
    State(state): State<Arc<ServerState>>,
    headers: HeaderMap,
) -> Result<impl IntoResponse, ApiError> {
    if state.control_store.is_none() {
        return Err(custody_unavailable());
    }
    let _operation_guard = operation_lock(&state.db_path, false).map_err(|_| internal())?;
    let mut connection = open_db(&state.db_path).map_err(|_| internal())?;
    let authenticated = auth_device(&state, &connection, &headers)?;
    let device_id = authenticated.device_id;
    if let Some(store) = &state.control_store {
        store
            .append_credential_revoked(
                &device_id,
                &authenticated.token_hash,
                &Utc::now().to_rfc3339(),
            )
            .map_err(|_| internal())?;
    }
    revoke_device(&mut connection, &device_id).map_err(|_| internal())?;
    Ok(axum::Json(json!({"device_id":device_id,"revoked":true})))
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct EraseRequest {
    confirmation: String,
    erasure_id: String,
    erasure_secret: String,
}

fn erasure_read(
    connection: &Connection,
    erasure_id: &str,
    secret: &str,
) -> Result<Option<Value>, ApiError> {
    connection.query_row(
        "SELECT requested_at,metrics_deleted_at,backups_expired_at,backup_delete_by FROM erasures WHERE erasure_id=?1 AND erasure_secret_hash=?2",
        params![erasure_id,digest(secret.as_bytes())],
        |row| {
            let requested_at: String = row.get(0)?;
            let metrics_deleted_at: Option<String> = row.get(1)?;
            let backups_expired_at: Option<String> = row.get(2)?;
            let backup_delete_by: String = row.get(3)?;
            Ok(json!({"erasure_id":erasure_id,"status":if metrics_deleted_at.is_some() && backups_expired_at.is_some() {"complete"} else if metrics_deleted_at.is_some() {"pending_backup_expiry"} else {"pending_metrics"},"requested_at":requested_at,"metrics_deleted_at":metrics_deleted_at,"backups_expired_at":backups_expired_at,"backup_delete_by":backup_delete_by}))
        },
    ).optional().map_err(|_| internal())
}

fn certified_erasure_read(
    state: &ServerState,
    connection: &Connection,
    erasure_id: &str,
    secret: &str,
) -> Result<Option<Value>, ApiError> {
    let Some(receipt) = erasure_read(connection, erasure_id, secret)? else {
        return Ok(None);
    };
    let facts = state
        .control_store
        .as_ref()
        .ok_or_else(custody_unavailable)?
        .certified_erasure_facts(erasure_id, &digest(secret.as_bytes()))
        .map_err(|_| custody_unavailable())?
        .ok_or_else(custody_unavailable)?;
    if receipt["requested_at"].as_str() != Some(facts.requested_at.as_str())
        || receipt["backup_delete_by"].as_str() != Some(facts.backup_delete_by.as_str())
        || receipt["metrics_deleted_at"].as_str() != facts.metrics_deleted_at.as_deref()
        || receipt["backups_expired_at"].as_str() != facts.backups_expired_at.as_deref()
    {
        return Err(custody_unavailable());
    }
    Ok(Some(receipt))
}

async fn erase(
    State(state): State<Arc<ServerState>>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<impl IntoResponse, ApiError> {
    if state.control_store.is_none() {
        return Err(custody_unavailable());
    }
    if body.len() > 768 {
        return Err(bad("Erasure request too large"));
    }
    let request: EraseRequest =
        serde_json::from_slice(&body).map_err(|_| bad("Invalid erasure JSON"))?;
    if request.confirmation != "ERASE_CLOUD_HEALTH_DATA" {
        return Err(bad("Explicit erasure confirmation required"));
    }
    if !valid_id(&request.erasure_id)
        || request.erasure_secret.len() != 64
        || !request
            .erasure_secret
            .bytes()
            .all(|byte| byte.is_ascii_hexdigit())
    {
        return Err(bad("Invalid erasure credentials"));
    }
    let secret_hash = digest(request.erasure_secret.as_bytes());
    let bearer = headers
        .get("authorization")
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.strip_prefix("Bearer "))
        .ok_or_else(unauthorized)?;
    let mut connection = open_db(&state.db_path).map_err(|_| internal())?;
    if let Some(store) = &state.control_store
        && let Some(intent) = store
            .certified_erasure_facts(&request.erasure_id, &secret_hash)
            .map_err(|_| custody_unavailable())?
    {
        if bearer != request.erasure_secret && digest(bearer.as_bytes()) != intent.auth.token_hash {
            return Err(unauthorized());
        }
        let _operation_guard = operation_lock(&state.db_path, true).map_err(|_| {
            ApiError::new(
                StatusCode::SERVICE_UNAVAILABLE,
                "operation_busy",
                "Backup or erasure operation is active",
            )
        })?;
        store
            .reconcile_health(&mut connection)
            .map_err(|_| internal())?;
        state.projection_notify.notify_one();
        let existing = certified_erasure_read(
            &state,
            &connection,
            &request.erasure_id,
            &request.erasure_secret,
        )?
        .ok_or_else(internal)?;
        return Ok(axum::Json(existing));
    }
    if erasure_read(&connection, &request.erasure_id, &request.erasure_secret)?.is_some() {
        if bearer == request.erasure_secret {
            let certified = certified_erasure_read(
                &state,
                &connection,
                &request.erasure_id,
                &request.erasure_secret,
            )?
            .ok_or_else(custody_unavailable)?;
            return Ok(axum::Json(certified));
        }
        return Err(unauthorized());
    }
    let collision: bool = connection
        .query_row(
            "SELECT EXISTS(SELECT 1 FROM erasures WHERE erasure_id=?1)",
            params![request.erasure_id],
            |row| row.get(0),
        )
        .map_err(|_| internal())?;
    if collision {
        return Err(unauthorized());
    }
    let authenticated = auth_device(&state, &connection, &headers)?;
    let device_id = authenticated.device_id;
    let _operation_guard = operation_lock(&state.db_path, true).map_err(|_| {
        ApiError::new(
            StatusCode::SERVICE_UNAVAILABLE,
            "operation_busy",
            "Backup or erasure operation is active",
        )
    })?;
    let now = Utc::now();
    let requested_at = now.to_rfc3339();
    let backup_delete_by = (now + Duration::days(30)).to_rfc3339();
    if let Some(store) = &state.control_store {
        store
            .append_erasure_intent(
                &device_id,
                &authenticated.token_hash,
                &request.erasure_id,
                &secret_hash,
                &requested_at,
                &backup_delete_by,
            )
            .map_err(|_| internal())?;
        store
            .reconcile_health(&mut connection)
            .map_err(|_| internal())?;
        state.projection_notify.notify_one();
        let result = certified_erasure_read(
            &state,
            &connection,
            &request.erasure_id,
            &request.erasure_secret,
        )?
        .ok_or_else(internal)?;
        return Ok(axum::Json(result));
    }
    let tx = connection.transaction().map_err(|_| internal())?;
    tx.execute("DELETE FROM outbox WHERE device_id=?1", params![device_id])
        .map_err(|_| internal())?;
    tx.execute(
        "DELETE FROM receipts WHERE device_id=?1",
        params![device_id],
    )
    .map_err(|_| internal())?;
    tx.execute("DELETE FROM events WHERE device_id=?1", params![device_id])
        .map_err(|_| internal())?;
    tx.execute("DELETE FROM audit WHERE device_id=?1", params![device_id])
        .map_err(|_| internal())?;
    tx.execute("DELETE FROM devices WHERE device_id=?1", params![device_id])
        .map_err(|_| internal())?;
    tx.execute("INSERT INTO erasures(device_id,erasure_id,erasure_secret_hash,requested_at,backup_delete_by) VALUES (?1,?2,?3,?4,?5) ON CONFLICT(device_id) DO UPDATE SET erasure_id=excluded.erasure_id,erasure_secret_hash=excluded.erasure_secret_hash,requested_at=excluded.requested_at,metrics_deleted_at=NULL,backups_expired_at=NULL,backup_delete_by=excluded.backup_delete_by,last_error=NULL",
        params![device_id,request.erasure_id,secret_hash,requested_at,backup_delete_by]).map_err(|_| internal())?;
    tx.commit().map_err(|_| internal())?;
    state.projection_notify.notify_one();
    Ok(axum::Json(
        json!({"erasure_id":request.erasure_id,"status":"pending_metrics","requested_at":requested_at,"metrics_deleted_at":null,"backups_expired_at":null,"backup_delete_by":backup_delete_by}),
    ))
}

async fn erasure_status(
    State(state): State<Arc<ServerState>>,
    Path(erasure_id): Path<String>,
    headers: HeaderMap,
) -> Result<impl IntoResponse, ApiError> {
    let token = headers
        .get("authorization")
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.strip_prefix("Bearer "))
        .ok_or_else(unauthorized)?;
    let connection = open_db(&state.db_path).map_err(|_| internal())?;
    let result = certified_erasure_read(&state, &connection, &erasure_id, token)?
        .ok_or_else(unauthorized)?;
    Ok(axum::Json(result))
}
