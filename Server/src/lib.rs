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
    time::Duration as StdDuration,
};

pub mod projection;

const MAX_BATCH_BYTES: usize = 128 * 1024;
const MAX_EVENTS: usize = 200;

#[derive(Clone)]
pub struct ServerState {
    pub db_path: PathBuf,
    pub upload_enabled: bool,
    pub runtime_guard: Option<RuntimeGuard>,
}

#[derive(Clone)]
pub struct RuntimeGuard {
    pub vm: projection::VmConfig,
    pub data_volume: PathBuf,
    pub backup_volume: PathBuf,
}

impl RuntimeGuard {
    pub fn verified(&self) -> bool {
        projection::native_vm_verified(&self.vm)
            && projection::encrypted_mount_verified(&self.data_volume)
            && projection::encrypted_mount_verified(&self.backup_volume)
            && projection::encrypted_mount_verified(&self.vm.storage)
    }
}

impl ServerState {
    fn accepts_upload(&self) -> bool {
        self.upload_enabled
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

pub fn open_db(path: &PathBuf) -> rusqlite::Result<Connection> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)
            .map_err(|_| rusqlite::Error::InvalidPath(parent.to_path_buf()))?;
    }
    let connection = Connection::open(path)?;
    connection.busy_timeout(StdDuration::from_secs(5))?;
    connection.execute_batch("PRAGMA journal_mode=WAL; PRAGMA foreign_keys=ON; PRAGMA synchronous=FULL; PRAGMA secure_delete=ON;")?;
    connection.execute_batch(include_str!("schema.sql"))?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600))
            .map_err(|_| rusqlite::Error::InvalidPath(path.clone()))?;
    }
    Ok(connection)
}

pub fn operation_lock(path: &FsPath, try_only: bool) -> std::io::Result<File> {
    let parent = path.parent().ok_or_else(|| {
        std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "Database path has no parent",
        )
    })?;
    let lock_path = parent.join("health-operations.lock");
    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(lock_path)?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        file.set_permissions(std::fs::Permissions::from_mode(0o600))?;
    }
    if try_only {
        file.try_lock_exclusive()?;
    } else {
        file.lock_exclusive()?;
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

fn validate_batch(batch: &Batch) -> Result<(), ApiError> {
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

fn metric_for(event: &HealthEvent) -> Option<&'static str> {
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

fn auth_device(connection: &Connection, headers: &HeaderMap) -> Result<String, ApiError> {
    let token = headers
        .get("authorization")
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.strip_prefix("Bearer "))
        .filter(|token| token.len() == 64 && token.bytes().all(|byte| byte.is_ascii_hexdigit()))
        .ok_or_else(unauthorized)?;
    let token_hash = digest(token.as_bytes());
    connection
        .query_row(
            "SELECT device_id FROM devices WHERE token_hash=?1 AND revoked_at IS NULL",
            params![token_hash],
            |row| row.get(0),
        )
        .optional()
        .map_err(|_| internal())?
        .ok_or_else(unauthorized)
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
    let mut connection = open_db(&state.db_path).map_err(|_| internal())?;
    let tx = connection.transaction().map_err(|_| internal())?;
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
    let changed = tx.execute(
        "INSERT INTO devices(device_id, token_hash, created_at) VALUES (?1, ?2, ?3) ON CONFLICT(device_id) DO UPDATE SET token_hash=excluded.token_hash, created_at=excluded.created_at, revoked_at=NULL WHERE devices.revoked_at IS NOT NULL AND NOT EXISTS (SELECT 1 FROM erasures WHERE erasures.device_id=devices.device_id AND metrics_deleted_at IS NULL)",
        params![request.device_id, digest(token.as_bytes()), Utc::now().to_rfc3339()],
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
    tx.commit().map_err(|_| internal())?;
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
        "SELECT batch_id, content_hash, accepted_events, changed_events, commit_sequence, received_at, projected_at FROM receipts WHERE device_id=?1 AND batch_id=?2",
        params![device_id, batch_id],
        |row| {
            let projected_at: Option<String> = row.get(6)?;
            Ok(Receipt {
                batch_id: row.get(0)?,
                status: if projected_at.is_some() { "metrics_current" } else { "cloud_saved" }.to_owned(),
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

fn save_event(
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
    let mut connection = open_db(&state.db_path).map_err(|_| internal())?;
    let device_id = auth_device(&connection, &headers)?;
    if device_id != batch.device_id {
        return Err(unauthorized());
    }
    let tx = connection
        .transaction_with_behavior(TransactionBehavior::Immediate)
        .map_err(|_| internal())?;
    if auth_device(&tx, &headers)? != device_id {
        return Err(unauthorized());
    }
    if let Some(receipt) = read_receipt(&tx, &device_id, &batch.batch_id).map_err(|_| internal())? {
        if receipt.content_hash == content_hash {
            return Ok((StatusCode::OK, axum::Json(receipt)));
        }
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
    Ok((StatusCode::CREATED, axum::Json(receipt)))
}

async fn receipt(
    State(state): State<Arc<ServerState>>,
    Path(batch_id): Path<String>,
    headers: HeaderMap,
) -> Result<impl IntoResponse, ApiError> {
    let connection = open_db(&state.db_path).map_err(|_| internal())?;
    let device_id = auth_device(&connection, &headers)?;
    let receipt = read_receipt(&connection, &device_id, &batch_id)
        .map_err(|_| internal())?
        .ok_or_else(|| ApiError::new(StatusCode::NOT_FOUND, "not_found", "Receipt not found"))?;
    Ok(axum::Json(receipt))
}

async fn status(
    State(state): State<Arc<ServerState>>,
    headers: HeaderMap,
) -> Result<impl IntoResponse, ApiError> {
    let connection = open_db(&state.db_path).map_err(|_| internal())?;
    let device_id = auth_device(&connection, &headers)?;
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
    let mut connection = open_db(&state.db_path).map_err(|_| internal())?;
    let device_id = auth_device(&connection, &headers)?;
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

async fn erase(
    State(state): State<Arc<ServerState>>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<impl IntoResponse, ApiError> {
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
    let mut connection = open_db(&state.db_path).map_err(|_| internal())?;
    if let Some(existing) = erasure_read(&connection, &request.erasure_id, &request.erasure_secret)?
    {
        let bearer = headers
            .get("authorization")
            .and_then(|value| value.to_str().ok())
            .and_then(|value| value.strip_prefix("Bearer "))
            .ok_or_else(unauthorized)?;
        if bearer == request.erasure_secret {
            return Ok(axum::Json(existing));
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
    let device_id = auth_device(&connection, &headers)?;
    let _operation_guard = operation_lock(&state.db_path, true).map_err(|_| {
        ApiError::new(
            StatusCode::SERVICE_UNAVAILABLE,
            "operation_busy",
            "Backup or erasure operation is active",
        )
    })?;
    let now = Utc::now();
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
        params![device_id,request.erasure_id,digest(request.erasure_secret.as_bytes()),now.to_rfc3339(),(now + Duration::days(30)).to_rfc3339()]).map_err(|_| internal())?;
    tx.commit().map_err(|_| internal())?;
    Ok(axum::Json(
        json!({"erasure_id":request.erasure_id,"status":"pending_metrics","requested_at":now.to_rfc3339(),"metrics_deleted_at":null,"backups_expired_at":null,"backup_delete_by":(now + Duration::days(30)).to_rfc3339()}),
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
    let result = erasure_read(&connection, &erasure_id, token)?.ok_or_else(unauthorized)?;
    Ok(axum::Json(result))
}
