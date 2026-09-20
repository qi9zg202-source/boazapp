use axum::{
    body::{Body, to_bytes},
    http::{Request, StatusCode},
};
use boaz_health_receiver::projection::{VmConfig, project_once};
use boaz_health_receiver::{
    RuntimeGuard, ServerState, create_pairing_code, open_db, operation_lock, router,
};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::{path::PathBuf, process::Command};
use tempfile::TempDir;
use tower::ServiceExt;

struct Fixture {
    _dir: TempDir,
    state: ServerState,
    token: String,
}

async fn request(
    state: &ServerState,
    method: &str,
    uri: &str,
    token: Option<&str>,
    payload: Option<Value>,
) -> (StatusCode, Value) {
    let body = payload
        .map(|value| serde_json::to_vec(&value).unwrap())
        .unwrap_or_default();
    let mut builder = Request::builder()
        .method(method)
        .uri(uri)
        .header("content-type", "application/json");
    if let Some(token) = token {
        builder = builder.header("authorization", format!("Bearer {token}"));
    }
    let response = router(state.clone())
        .oneshot(builder.body(Body::from(body)).unwrap())
        .await
        .unwrap();
    let status = response.status();
    let bytes = to_bytes(response.into_body(), 256 * 1024).await.unwrap();
    (status, serde_json::from_slice(&bytes).unwrap())
}

async fn fixture() -> Fixture {
    let dir = TempDir::new().unwrap();
    let state = ServerState {
        db_path: dir.path().join("health.db"),
        upload_enabled: true,
        runtime_guard: None,
    };
    let connection = open_db(&state.db_path).unwrap();
    let code = create_pairing_code(&connection).unwrap();
    let (status, response) = request(
        &state,
        "POST",
        "/v1/health/pairings",
        None,
        Some(json!({"code":code,"device_id":"phone-1"})),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED);
    Fixture {
        _dir: dir,
        state,
        token: response["token"].as_str().unwrap().to_owned(),
    }
}

fn quantity(event_id: &str, revision: i64, value: f64) -> Value {
    json!({"event_id":event_id,"revision":revision,"operation":"upsert","kind":"quantity","type":"HKQuantityTypeIdentifierHeartRate","source":{"bundle_id":"com.apple.health","name":"Apple Watch"},"start_utc":"2026-09-18T00:00:00Z","end_utc":"2026-09-18T00:01:00Z","value":value,"unit":"count/min","metadata":{}})
}

fn batch(batch_id: &str, events: Vec<Value>) -> Value {
    json!({"schema_version":1,"batch_id":batch_id,"device_id":"phone-1","events":events})
}

#[tokio::test]
async fn lost_response_retry_returns_same_receipt_and_one_event() {
    let fixture = fixture().await;
    let payload = batch("batch-1", vec![quantity("sample-1", 1, 62.0)]);
    let (first_status, first) = request(
        &fixture.state,
        "POST",
        "/v1/health/batches",
        Some(&fixture.token),
        Some(payload.clone()),
    )
    .await;
    assert_eq!(first_status, StatusCode::CREATED);
    assert_eq!(first["status"], "cloud_saved");
    assert_eq!(first["accepted_events"], 1);
    let raw_body = serde_json::to_vec(&payload).unwrap();
    assert_eq!(first["content_hash"], hex::encode(Sha256::digest(raw_body)));
    let (retry_status, retry) = request(
        &fixture.state,
        "POST",
        "/v1/health/batches",
        Some(&fixture.token),
        Some(payload),
    )
    .await;
    assert_eq!(retry_status, StatusCode::OK);
    assert_eq!(first, retry);
    let (lookup_status, lookup) = request(
        &fixture.state,
        "GET",
        "/v1/health/batches/batch-1/receipt",
        Some(&fixture.token),
        None,
    )
    .await;
    assert_eq!(lookup_status, StatusCode::OK);
    assert_eq!(lookup, first);
    let connection = open_db(&fixture.state.db_path).unwrap();
    let count: i64 = connection
        .query_row("SELECT count(*) FROM events", [], |row| row.get(0))
        .unwrap();
    let jobs: i64 = connection
        .query_row("SELECT count(*) FROM outbox", [], |row| row.get(0))
        .unwrap();
    assert_eq!(count, 1);
    assert_eq!(jobs, 1);
}

#[tokio::test]
async fn batch_id_and_revision_conflicts_fail_closed() {
    let fixture = fixture().await;
    let (status, _) = request(
        &fixture.state,
        "POST",
        "/v1/health/batches",
        Some(&fixture.token),
        Some(batch("batch-1", vec![quantity("sample-1", 1, 62.0)])),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED);
    let (status, body) = request(
        &fixture.state,
        "POST",
        "/v1/health/batches",
        Some(&fixture.token),
        Some(batch("batch-1", vec![quantity("sample-1", 1, 63.0)])),
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT);
    assert_eq!(body["error"], "batch_conflict");
    let (status, body) = request(
        &fixture.state,
        "POST",
        "/v1/health/batches",
        Some(&fixture.token),
        Some(batch("batch-2", vec![quantity("sample-1", 1, 63.0)])),
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT);
    assert_eq!(body["error"], "revision_conflict");
    let connection = open_db(&fixture.state.db_path).unwrap();
    let value: f64 = connection
        .query_row(
            "SELECT value FROM events WHERE event_id='sample-1'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(value, 62.0);
}

#[tokio::test]
async fn tombstone_prevents_old_addition_and_preserves_new_revision() {
    let fixture = fixture().await;
    let deletion = json!({"event_id":"sample-1","revision":2,"operation":"delete","kind":"quantity","type":"HKQuantityTypeIdentifierHeartRate","source":null,"start_utc":null,"end_utc":null,"value":null,"unit":null,"metadata":{}});
    let (status, _) = request(
        &fixture.state,
        "POST",
        "/v1/health/batches",
        Some(&fixture.token),
        Some(batch("delete-first", vec![deletion])),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED);
    let (status, receipt) = request(
        &fixture.state,
        "POST",
        "/v1/health/batches",
        Some(&fixture.token),
        Some(batch("late-old-add", vec![quantity("sample-1", 1, 62.0)])),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED);
    assert_eq!(receipt["accepted_events"], 1);
    assert_eq!(receipt["changed_events"], 0);
    let (status, status_body) = request(
        &fixture.state,
        "GET",
        "/v1/health/status",
        Some(&fixture.token),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(status_body["sample_count"], 0);
    let (status, _) = request(
        &fixture.state,
        "POST",
        "/v1/health/batches",
        Some(&fixture.token),
        Some(batch("new-add", vec![quantity("sample-1", 3, 64.0)])),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED);
    let (status, status_body) = request(
        &fixture.state,
        "GET",
        "/v1/health/status",
        Some(&fixture.token),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(status_body["sample_count"], 1);
}

#[tokio::test]
async fn auth_pairing_one_time_and_erasure_revoke() {
    let fixture = fixture().await;
    let (status, _) = request(
        &fixture.state,
        "POST",
        "/v1/health/batches",
        None,
        Some(batch("batch-1", vec![quantity("sample-1", 1, 62.0)])),
    )
    .await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
    let connection = open_db(&fixture.state.db_path).unwrap();
    let code = create_pairing_code(&connection).unwrap();
    let (status, _) = request(
        &fixture.state,
        "POST",
        "/v1/health/pairings",
        None,
        Some(json!({"code":code,"device_id":"phone-2"})),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED);
    let (status, _) = request(
        &fixture.state,
        "POST",
        "/v1/health/pairings",
        None,
        Some(json!({"code":code,"device_id":"phone-3"})),
    )
    .await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
    let (status, _) = request(
        &fixture.state,
        "POST",
        "/v1/health/batches",
        Some(&fixture.token),
        Some(batch("batch-1", vec![quantity("sample-1", 1, 62.0)])),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED);
    let erase_request = json!({"confirmation":"ERASE_CLOUD_HEALTH_DATA","erasure_id":"erase-1","erasure_secret":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"});
    let (status, erase) = request(
        &fixture.state,
        "POST",
        "/v1/health/erase",
        Some(&fixture.token),
        Some(erase_request.clone()),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(erase["status"], "pending_metrics");
    let (status, _) = request(
        &fixture.state,
        "GET",
        "/v1/health/status",
        Some(&fixture.token),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
    let erasure_secret = erase_request["erasure_secret"].as_str().unwrap().to_owned();
    let (retry_status, retry) = request(
        &fixture.state,
        "POST",
        "/v1/health/erase",
        Some(&erasure_secret),
        Some(erase_request),
    )
    .await;
    assert_eq!(retry_status, StatusCode::OK);
    assert_eq!(retry, erase);
    let (status, erasure) = request(
        &fixture.state,
        "GET",
        "/v1/health/erasures/erase-1",
        Some(&erasure_secret),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(erasure["status"], "pending_metrics");
    let remaining: i64 = connection
        .query_row("SELECT count(*) FROM events", [], |row| row.get(0))
        .unwrap();
    assert_eq!(remaining, 0);
}

#[tokio::test]
async fn limits_and_closed_upload_gate() {
    let fixture = fixture().await;
    let oversized = batch(
        "oversized",
        (0..201)
            .map(|index| quantity(&format!("sample-{index}"), 1, 62.0))
            .collect(),
    );
    let (status, _) = request(
        &fixture.state,
        "POST",
        "/v1/health/batches",
        Some(&fixture.token),
        Some(oversized),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    let disabled = ServerState {
        upload_enabled: false,
        ..fixture.state.clone()
    };
    let (status, response) = request(
        &disabled,
        "POST",
        "/v1/health/batches",
        Some(&fixture.token),
        Some(batch("batch-1", vec![quantity("sample-1", 1, 62.0)])),
    )
    .await;
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(response["error"], "upload_disabled");
}

#[tokio::test]
async fn app_wrist_and_activity_units_create_projection_jobs() {
    let fixture = fixture().await;
    let wrist = json!({"event_id":"wrist-1","revision":1,"operation":"upsert","kind":"quantity","type":"HKQuantityTypeIdentifierAppleSleepingWristTemperature","source":{"bundle_id":"com.apple.health","name":"Apple Watch"},"start_utc":"2026-09-17T22:00:00Z","end_utc":"2026-09-18T06:00:00Z","value":36.4,"unit":"degC","metadata":{}});
    let stand = json!({"event_id":"activity:2026-09-18:stand","revision":1,"operation":"upsert","kind":"activity","type":"activity.stand","source":{"bundle_id":"com.apple.health","name":"Apple Health"},"start_utc":"2026-09-17T16:00:00Z","end_utc":"2026-09-18T16:00:00Z","value":9.0,"unit":"hours","metadata":{"timezone":"Asia/Shanghai"}});
    let (status, receipt) = request(
        &fixture.state,
        "POST",
        "/v1/health/batches",
        Some(&fixture.token),
        Some(batch("app-activity", vec![wrist, stand])),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED);
    assert_eq!(receipt["status"], "cloud_saved");
    let connection = open_db(&fixture.state.db_path).unwrap();
    let mut statement = connection
        .prepare("SELECT metric_name FROM outbox ORDER BY metric_name")
        .unwrap();
    let jobs: Vec<String> = statement
        .query_map([], |row| row.get(0))
        .unwrap()
        .map(Result::unwrap)
        .collect();
    assert_eq!(
        jobs,
        vec![
            "boaz_health_v1_activity_stand_hours",
            "boaz_health_v1_wrist_temperature_celsius"
        ]
    );
}

#[tokio::test]
async fn versioned_sleep_derivation_is_the_only_deep_sleep_projection() {
    let fixture = fixture().await;
    let raw = json!({"event_id":"raw-sleep-1","revision":1,"operation":"upsert","kind":"category","type":"HKCategoryTypeIdentifierSleepAnalysis","source":{"bundle_id":"com.apple.health","name":"Apple Watch"},"start_utc":"2026-09-17T22:00:00Z","end_utc":"2026-09-17T23:00:00Z","value":4,"unit":null,"metadata":{"stage":"asleepDeep"}});
    let (status, raw_receipt) = request(
        &fixture.state,
        "POST",
        "/v1/health/batches",
        Some(&fixture.token),
        Some(batch("raw-sleep", vec![raw])),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED);
    assert_eq!(raw_receipt["status"], "metrics_current");
    assert!(raw_receipt["projected_at"].is_string());
    let derived = json!({"event_id":"sleep-day:2026-09-18","revision":1,"operation":"upsert","kind":"quantity","type":"boaz.sleep.deep_minutes","source":{"bundle_id":"boazapp","name":"Boaz SleepAnalyzer"},"start_utc":"2026-09-17T22:00:00Z","end_utc":"2026-09-18T06:00:00Z","value":65.0,"unit":"min","metadata":{"derivation_version":"1","time_zone":"Asia/Shanghai"}});
    let (status, receipt) = request(
        &fixture.state,
        "POST",
        "/v1/health/batches",
        Some(&fixture.token),
        Some(batch("derived-sleep", vec![derived])),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED);
    assert_eq!(receipt["status"], "cloud_saved");
    let connection = open_db(&fixture.state.db_path).unwrap();
    let metric: String = connection
        .query_row(
            "SELECT metric_name FROM outbox WHERE batch_id='derived-sleep'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(metric, "boaz_health_v1_deep_sleep_min");
}

#[tokio::test]
async fn token_can_be_revoked_without_deleting_health_history() {
    let fixture = fixture().await;
    let (status, _) = request(
        &fixture.state,
        "POST",
        "/v1/health/batches",
        Some(&fixture.token),
        Some(batch("batch-1", vec![quantity("sample-1", 1, 62.0)])),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED);
    let (status, result) = request(
        &fixture.state,
        "POST",
        "/v1/health/revoke",
        Some(&fixture.token),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(result["revoked"], true);
    let (status, _) = request(
        &fixture.state,
        "GET",
        "/v1/health/status",
        Some(&fixture.token),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
    let connection = open_db(&fixture.state.db_path).unwrap();
    let remaining: i64 = connection
        .query_row("SELECT count(*) FROM events", [], |row| row.get(0))
        .unwrap();
    assert_eq!(remaining, 1);
}

#[tokio::test]
async fn metrics_outage_retains_durable_outbox_and_cloud_saved_receipt() {
    let fixture = fixture().await;
    let (status, _) = request(
        &fixture.state,
        "POST",
        "/v1/health/batches",
        Some(&fixture.token),
        Some(batch("metric-outage", vec![quantity("sample-1", 1, 62.0)])),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED);
    let config = VmConfig {
        binary: PathBuf::from("/nonexistent/native-vm"),
        storage: PathBuf::from("/nonexistent/vm-storage"),
    };
    assert_eq!(
        project_once(&fixture.state, &config).await.unwrap_err(),
        "native_metrics_identity_unverified"
    );
    let (status, receipt) = request(
        &fixture.state,
        "GET",
        "/v1/health/batches/metric-outage/receipt",
        Some(&fixture.token),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(receipt["status"], "cloud_saved");
    let connection = open_db(&fixture.state.db_path).unwrap();
    let pending: i64 = connection
        .query_row(
            "SELECT count(*) FROM outbox WHERE processed_at IS NULL",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(pending, 1);
}

#[tokio::test]
async fn bounded_pages_cover_large_history_without_record_loss() {
    let fixture = fixture().await;
    for page in 0..8 {
        let events = (0..200)
            .map(|index| quantity(&format!("sample-{page}-{index}"), 1, 60.0 + index as f64))
            .collect();
        let (status, receipt) = request(
            &fixture.state,
            "POST",
            "/v1/health/batches",
            Some(&fixture.token),
            Some(batch(&format!("page-{page}"), events)),
        )
        .await;
        assert_eq!(status, StatusCode::CREATED);
        assert_eq!(receipt["accepted_events"], 200);
    }
    let connection = open_db(&fixture.state.db_path).unwrap();
    let count: i64 = connection
        .query_row("SELECT count(*) FROM events", [], |row| row.get(0))
        .unwrap();
    let commits: i64 = connection
        .query_row("SELECT count(*) FROM receipts", [], |row| row.get(0))
        .unwrap();
    assert_eq!(count, 1600);
    assert_eq!(commits, 8);
}

#[tokio::test]
async fn wal_backup_is_consistent_and_encrypted_volume_gate_is_required() {
    let fixture = fixture().await;
    let (status, _) = request(
        &fixture.state,
        "POST",
        "/v1/health/batches",
        Some(&fixture.token),
        Some(batch("batch-1", vec![quantity("sample-1", 1, 62.0)])),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED);
    let backup_dir = fixture._dir.path().join("backups");
    std::fs::create_dir(&backup_dir).unwrap();
    let backup = backup_dir.join("boaz-health-test.db");
    let binary = env!("CARGO_BIN_EXE_boaz-health-receiver");
    let denied = Command::new(binary)
        .arg("backup")
        .arg(&backup)
        .env("BOAZ_HEALTH_DB", &fixture.state.db_path)
        .env_remove("BOAZ_HEALTH_BACKUP_VOLUME_ENCRYPTED")
        .status()
        .unwrap();
    assert!(!denied.success());
    assert!(!backup.exists());
    let allowed = Command::new(binary)
        .arg("backup")
        .arg(&backup)
        .env("BOAZ_HEALTH_DB", &fixture.state.db_path)
        .env("BOAZ_HEALTH_BACKUP_VOLUME_ENCRYPTED", "1")
        .status()
        .unwrap();
    assert!(allowed.success());
    assert!(backup.with_extension("db.meta.json").exists());
    let copied = open_db(&backup).unwrap();
    let integrity: String = copied
        .query_row("PRAGMA integrity_check", [], |row| row.get(0))
        .unwrap();
    let count: i64 = copied
        .query_row("SELECT count(*) FROM events", [], |row| row.get(0))
        .unwrap();
    assert_eq!(integrity, "ok");
    assert_eq!(count, 1);
}

#[tokio::test]
async fn runtime_identity_loss_closes_ingest_without_losing_queue() {
    let fixture = fixture().await;
    let guard = RuntimeGuard {
        vm: VmConfig {
            binary: PathBuf::from("/nonexistent/native-vm"),
            storage: PathBuf::from("/nonexistent/storage"),
        },
        data_volume: fixture._dir.path().to_path_buf(),
        backup_volume: fixture._dir.path().to_path_buf(),
    };
    let closed = ServerState {
        runtime_guard: Some(guard),
        ..fixture.state.clone()
    };
    let (status, error) = request(
        &closed,
        "POST",
        "/v1/health/batches",
        Some(&fixture.token),
        Some(batch("blocked", vec![quantity("sample-1", 1, 62.0)])),
    )
    .await;
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(error["error"], "upload_disabled");
    let connection = open_db(&fixture.state.db_path).unwrap();
    let count: i64 = connection
        .query_row("SELECT count(*) FROM events", [], |row| row.get(0))
        .unwrap();
    assert_eq!(count, 0);
}

#[tokio::test]
async fn backup_lock_defers_erasure_and_erasure_removes_identity_audit() {
    let fixture = fixture().await;
    let (status, _) = request(
        &fixture.state,
        "POST",
        "/v1/health/batches",
        Some(&fixture.token),
        Some(batch("batch-1", vec![quantity("sample-1", 1, 62.0)])),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED);
    let erase_request = json!({"confirmation":"ERASE_CLOUD_HEALTH_DATA","erasure_id":"erase-locked","erasure_secret":"bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"});
    let lock = operation_lock(&fixture.state.db_path, false).unwrap();
    let (status, error) = request(
        &fixture.state,
        "POST",
        "/v1/health/erase",
        Some(&fixture.token),
        Some(erase_request.clone()),
    )
    .await;
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(error["error"], "operation_busy");
    drop(lock);
    let (status, _) = request(
        &fixture.state,
        "POST",
        "/v1/health/erase",
        Some(&fixture.token),
        Some(erase_request),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let connection = open_db(&fixture.state.db_path).unwrap();
    for table in ["events", "receipts", "outbox", "devices", "audit"] {
        let query = format!("SELECT count(*) FROM {table}");
        let count: i64 = connection.query_row(&query, [], |row| row.get(0)).unwrap();
        assert_eq!(count, 0, "{table} retains data");
    }
}

#[tokio::test]
async fn production_swift_batch_bytes_match_receipt_and_sqlite_readback() {
    let fixture = fixture().await;
    let raw = include_bytes!("fixtures/ios_batch.json");
    let request = Request::builder()
        .method("POST")
        .uri("/v1/health/batches")
        .header("content-type", "application/json")
        .header("authorization", format!("Bearer {}", fixture.token))
        .body(Body::from(raw.as_slice()))
        .unwrap();
    let response = router(fixture.state.clone())
        .oneshot(request)
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::CREATED);
    let body = to_bytes(response.into_body(), 4096).await.unwrap();
    let receipt: Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(receipt["batch_id"], "d418aa43-35e5-433f-b1d7-29297ad2b525");
    assert_eq!(receipt["accepted_events"], 2);
    assert_eq!(receipt["changed_events"], 2);
    assert_eq!(receipt["status"], "cloud_saved");
    assert_eq!(receipt["content_hash"], hex::encode(Sha256::digest(raw)));
    let connection = open_db(&fixture.state.db_path).unwrap();
    let mut statement = connection.prepare("SELECT event_id,value,unit,source_json FROM events WHERE device_id='phone-1' ORDER BY event_id").unwrap();
    let rows: Vec<(String, f64, String, String)> = statement
        .query_map([], |row| {
            Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?))
        })
        .unwrap()
        .map(Result::unwrap)
        .collect();
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].0, "swift-heart-1");
    assert_eq!(rows[0].1, 72.0);
    assert_eq!(rows[0].2, "count/min");
    assert_eq!(rows[1].0, "swift-heart-2");
    assert_eq!(rows[1].1, 74.0);
    assert_eq!(rows[1].2, "count/min");
    let source: Value = serde_json::from_str(&rows[0].3).unwrap();
    assert_eq!(source["bundle_id"], "test.watch");
}
