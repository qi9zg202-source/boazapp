use axum::{
    body::{Body, to_bytes},
    http::{Request, StatusCode},
};
use boaz_health_receiver::projection::{VmConfig, project_once};
use boaz_health_receiver::{
    RuntimeGuard, ServerState,
    ack_journal::{AckJournal, Baseline},
    activation::initialize_coordinator,
    control::ControlStore,
    create_pairing_code,
    custody::{CustodyClient, CustodyError, CustodyReservationV2, CustodyResult, CustodyState},
    database::{HealthLayout, StoragePaths, classify_health_database, initialize_health_database},
    open_db, operation_lock, router,
};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::{
    path::PathBuf,
    process::Command,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicU8, Ordering},
    },
};
use tempfile::TempDir;
use tokio::sync::Notify;
use tower::ServiceExt;

// The production CLI has no test override for its physical dm-crypt gate.
// Compile its private core into this synthetic-only test binary to retain
// backup/prune behavior coverage on hosts without a separate encrypted mount.
#[allow(dead_code)]
#[path = "../src/main.rs"]
mod receiver_cli;

struct Fixture {
    _dir: TempDir,
    state: ServerState,
    custody: Arc<SyntheticCustody>,
    token: String,
}

/// This in-memory custodian exists only in the synthetic integration binary.
/// The released receiver constructs the authenticated SSH implementation.
struct SyntheticCustody {
    state: Mutex<(CustodyState, Option<CustodyReservationV2>)>,
    fail_next_cas: AtomicBool,
    cas_reply_mode: AtomicU8,
    fail_next_read: AtomicBool,
}

impl SyntheticCustody {
    fn new(state: CustodyState) -> Self {
        Self {
            state: Mutex::new((state, None)),
            fail_next_cas: AtomicBool::new(false),
            cas_reply_mode: AtomicU8::new(0),
            fail_next_read: AtomicBool::new(false),
        }
    }
}

impl CustodyClient for SyntheticCustody {
    fn read_v2(&self, store_id: &str) -> CustodyResult<CustodyState> {
        if self.fail_next_read.swap(false, Ordering::SeqCst) {
            return Err(CustodyError::Protocol("injected readback outage".into()));
        }
        let state = self.state.lock().unwrap();
        if state.0.control.store_id != store_id {
            return Err(CustodyError::Protocol("synthetic identity mismatch".into()));
        }
        if state.1.is_some() {
            return Err(CustodyError::Protocol(
                "synthetic reservation pending".into(),
            ));
        }
        Ok(state.0.clone())
    }

    fn reserve_v2(
        &self,
        predecessor: &CustodyState,
        operation_id: &str,
        intent_sha256: &str,
    ) -> CustodyResult<CustodyReservationV2> {
        let mut state = self.state.lock().unwrap();
        if state.0 != *predecessor {
            return Err(CustodyError::Protocol("synthetic stale predecessor".into()));
        }
        if let Some(existing) = &state.1 {
            if existing.predecessor == *predecessor
                && existing.operation_id == operation_id
                && existing.intent_sha256 == intent_sha256
            {
                return Ok(existing.clone());
            }
            return Err(CustodyError::Protocol(
                "synthetic reservation conflict".into(),
            ));
        }
        let reservation = CustodyReservationV2 {
            reservation_id: uuid::Uuid::new_v4().to_string(),
            predecessor: predecessor.clone(),
            operation_id: operation_id.to_owned(),
            intent_sha256: intent_sha256.to_owned(),
        };
        state.1 = Some(reservation.clone());
        Ok(reservation)
    }

    fn compare_and_swap_v2(
        &self,
        reservation: &CustodyReservationV2,
        successor: &CustodyState,
    ) -> CustodyResult<CustodyState> {
        if self.fail_next_cas.swap(false, Ordering::SeqCst) {
            return Err(CustodyError::Protocol("injected CAS outage".into()));
        }
        let mut state = self.state.lock().unwrap();
        if state.1.as_ref() != Some(reservation)
            || state.0 != reservation.predecessor
            || successor.revision != state.0.revision + 1
        {
            return Err(CustodyError::Protocol("synthetic CAS conflict".into()));
        }
        let reply_mode = self.cas_reply_mode.swap(0, Ordering::SeqCst);
        if reply_mode == 3 {
            // A lying or malfunctioning remote reported success without
            // committing its reservation. The route must verify readback
            // before discarding the original durable intent.
            return Ok(successor.clone());
        }
        state.0 = successor.clone();
        state.1 = None;
        match reply_mode {
            1 => Err(CustodyError::Protocol("injected lost CAS reply".into())),
            2 => {
                self.fail_next_read.store(true, Ordering::SeqCst);
                Err(CustodyError::Protocol(
                    "injected lost CAS reply and first readback".into(),
                ))
            }
            _ => Ok(state.0.clone()),
        }
    }
}

#[tokio::test]
async fn synthetic_loopback_http_contracts() {
    let fixture = fixture().await;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let state = fixture.state.clone();
    let task = tokio::spawn(async move {
        axum::serve(listener, router(state)).await.unwrap();
    });
    // The test-only route exercises an actual TCP listener. It does not call
    // the production serve CLI or bypass its activation gate.
    let client = reqwest::Client::new();
    let base = format!("http://{address}");
    let token = fixture.token.clone();
    let code = create_pairing_code(&open_db(&fixture.state.db_path).unwrap()).unwrap();
    let pair_response = client
        .post(format!("{base}/v1/health/pairings"))
        .header("content-type", "application/json")
        .body(serde_json::to_vec(&json!({"code":code,"device_id":"loopback-phone-2"})).unwrap())
        .send()
        .await
        .unwrap();
    assert_eq!(pair_response.status(), StatusCode::CREATED);
    let pair: Value = serde_json::from_slice(&pair_response.bytes().await.unwrap()).unwrap();
    let second_token = pair["token"].as_str().unwrap().to_owned();
    let saved = client
        .post(format!("{base}/v1/health/batches"))
        .bearer_auth(&token)
        .header("content-type", "application/json")
        .body(
            serde_json::to_vec(&batch(
                "loopback-batch",
                vec![quantity("loopback-event", 1, 71.0)],
            ))
            .unwrap(),
        )
        .send()
        .await
        .unwrap();
    assert_eq!(saved.status(), StatusCode::CREATED);
    let saved: Value = serde_json::from_slice(&saved.bytes().await.unwrap()).unwrap();
    let readback = client
        .get(format!("{base}/v1/health/batches/loopback-batch/receipt"))
        .bearer_auth(&token)
        .send()
        .await
        .unwrap();
    assert_eq!(readback.status(), StatusCode::OK);
    let readback: Value = serde_json::from_slice(&readback.bytes().await.unwrap()).unwrap();
    assert_eq!(readback, saved);
    let revoked = client
        .post(format!("{base}/v1/health/revoke"))
        .bearer_auth(&token)
        .send()
        .await
        .unwrap();
    assert_eq!(revoked.status(), StatusCode::OK);
    let erased = client
        .post(format!("{base}/v1/health/erase"))
        .bearer_auth(&second_token)
        .header("content-type", "application/json")
        .body(
            serde_json::to_vec(&json!({
                "confirmation":"ERASE_CLOUD_HEALTH_DATA",
                "erasure_id":"loopback-erasure",
                "erasure_secret":"c".repeat(64)
            }))
            .unwrap(),
        )
        .send()
        .await
        .unwrap();
    assert_eq!(erased.status(), StatusCode::OK);
    let erased: Value = serde_json::from_slice(&erased.bytes().await.unwrap()).unwrap();
    assert_eq!(erased["status"], "pending_metrics");
    task.abort();
}

#[tokio::test]
async fn custody_cas_outage_never_returns_a_locally_confirmed_receipt() {
    let fixture = fixture().await;
    fixture.custody.fail_next_cas.store(true, Ordering::SeqCst);
    let payload = batch("custody-retry", vec![quantity("custody-event", 1, 72.0)]);
    let (status, error) = request(
        &fixture.state,
        "POST",
        "/v1/health/batches",
        Some(&fixture.token),
        Some(payload.clone()),
    )
    .await;
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(error["error"], "custody_unavailable");
    assert_eq!(ledger_counts(&fixture.state)[0], 1);
    let (status, _) = request(
        &fixture.state,
        "GET",
        "/v1/health/batches/custody-retry/receipt",
        Some(&fixture.token),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
    let (status, _) = request(
        &fixture.state,
        "GET",
        "/v1/health/status",
        Some(&fixture.token),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
    let (status, recovered) = request(
        &fixture.state,
        "POST",
        "/v1/health/batches",
        Some(&fixture.token),
        Some(payload),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(recovered["batch_id"], "custody-retry");
    assert_eq!(ledger_counts(&fixture.state)[0], 1);
    let (status, readback) = request(
        &fixture.state,
        "GET",
        "/v1/health/batches/custody-retry/receipt",
        Some(&fixture.token),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(readback, recovered);
}

#[tokio::test]
async fn committed_cas_with_lost_reply_and_readback_requires_exact_batch_retry() {
    let fixture = fixture().await;
    let journal = fixture.state.ack_journal.as_ref().unwrap();
    let raw = serde_json::to_vec(&batch(
        "lost-cas-reply-batch",
        vec![quantity("lost-cas-reply-event", 1, 73.0)],
    ))
    .unwrap();
    fixture.custody.cas_reply_mode.store(2, Ordering::SeqCst);
    let (status, error) = request_bytes(
        &fixture.state,
        "POST",
        "/v1/health/batches",
        Some(&fixture.token),
        raw.clone(),
    )
    .await;
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(error["error"], "custody_unavailable");
    let pending = journal.pending_custody_intent().unwrap().unwrap();
    assert!(matches!(
        pending.confirmation,
        boaz_health_receiver::ack_journal::AckCustodyKind::Batch { .. }
    ));
    assert_eq!(fixture.custody.state.lock().unwrap().0, pending.successor);
    assert_eq!(&ledger_counts(&fixture.state)[..2], &[1, 1]);
    let (status, _) = request(
        &fixture.state,
        "GET",
        "/v1/health/batches/lost-cas-reply-batch/receipt",
        Some(&fixture.token),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);

    let altered = serde_json::to_vec(&batch(
        "lost-cas-reply-batch",
        vec![quantity("lost-cas-reply-event", 1, 79.0)],
    ))
    .unwrap();
    let (status, conflict) = request_bytes(
        &fixture.state,
        "POST",
        "/v1/health/batches",
        Some(&fixture.token),
        altered,
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT);
    assert_eq!(conflict["error"], "batch_conflict");
    assert!(journal.pending_custody_intent().unwrap().is_none());
    assert_eq!(&ledger_counts(&fixture.state)[..2], &[1, 1]);

    let (status, receipt) = request_bytes(
        &fixture.state,
        "POST",
        "/v1/health/batches",
        Some(&fixture.token),
        raw,
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(receipt["batch_id"], "lost-cas-reply-batch");
    assert!(journal.pending_custody_intent().unwrap().is_none());
    assert_eq!(&ledger_counts(&fixture.state)[..2], &[1, 1]);
    assert_eq!(fixture.custody.state.lock().unwrap().0, pending.successor);
}

#[tokio::test]
async fn false_cas_success_must_keep_original_intent_until_remote_readback() {
    let fixture = fixture().await;
    let journal = fixture.state.ack_journal.as_ref().unwrap();
    let predecessor = fixture.custody.state.lock().unwrap().0.clone();
    let raw = serde_json::to_vec(&batch(
        "false-cas-success-batch",
        vec![quantity("false-cas-success-event", 1, 75.0)],
    ))
    .unwrap();
    fixture.custody.cas_reply_mode.store(3, Ordering::SeqCst);
    let (status, error) = request_bytes(
        &fixture.state,
        "POST",
        "/v1/health/batches",
        Some(&fixture.token),
        raw.clone(),
    )
    .await;
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(error["error"], "custody_unavailable");
    assert_eq!(fixture.custody.state.lock().unwrap().0, predecessor);
    assert!(journal.pending_custody_intent().unwrap().is_some());

    let (status, receipt) = request_bytes(
        &fixture.state,
        "POST",
        "/v1/health/batches",
        Some(&fixture.token),
        raw,
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(receipt["batch_id"], "false-cas-success-batch");
    assert!(journal.pending_custody_intent().unwrap().is_none());
    assert_eq!(&ledger_counts(&fixture.state)[..2], &[1, 1]);
}

#[tokio::test]
async fn lost_pairing_cas_reply_never_returns_plaintext_token() {
    let fixture = fixture().await;
    let journal = fixture.state.ack_journal.as_ref().unwrap();
    let code = create_pairing_code(&open_db(&fixture.state.db_path).unwrap()).unwrap();
    fixture.custody.cas_reply_mode.store(2, Ordering::SeqCst);
    let body = json!({"code": code, "device_id": "lost-pairing-reply-device"});
    let (status, error) = request(
        &fixture.state,
        "POST",
        "/v1/health/pairings",
        None,
        Some(body.clone()),
    )
    .await;
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(error["error"], "custody_unavailable");
    assert!(error.get("token").is_none());
    let pending = journal.pending_custody_intent().unwrap().unwrap();
    assert!(matches!(
        pending.confirmation,
        boaz_health_receiver::ack_journal::AckCustodyKind::Pairing { .. }
    ));
    assert_eq!(fixture.custody.state.lock().unwrap().0, pending.successor);

    let (status, retry) = request(
        &fixture.state,
        "POST",
        "/v1/health/pairings",
        None,
        Some(body),
    )
    .await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
    assert!(retry.get("token").is_none());
    assert!(journal.pending_custody_intent().unwrap().is_none());
    let new_code = create_pairing_code(&open_db(&fixture.state.db_path).unwrap()).unwrap();
    let (status, paired) = request(
        &fixture.state,
        "POST",
        "/v1/health/pairings",
        None,
        Some(json!({"code": new_code, "device_id": "new-after-lost-pairing-device"})),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED);
    assert!(paired["token"].as_str().is_some());
}

#[tokio::test]
async fn conflicting_reservation_or_divergent_head_cannot_confirm_new_batch() {
    let fixture = fixture().await;
    let original = fixture.custody.state.lock().unwrap().0.clone();
    let body = batch(
        "conflicted-custody-batch",
        vec![quantity("conflicted-custody-event", 1, 74.0)],
    );
    fixture.custody.state.lock().unwrap().1 = Some(CustodyReservationV2 {
        reservation_id: uuid::Uuid::new_v4().to_string(),
        predecessor: original.clone(),
        operation_id: "another-operation".into(),
        intent_sha256: "a".repeat(64),
    });
    let (status, _) = request(
        &fixture.state,
        "POST",
        "/v1/health/batches",
        Some(&fixture.token),
        Some(body.clone()),
    )
    .await;
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(&ledger_counts(&fixture.state)[..2], &[0, 0]);

    {
        let mut remote = fixture.custody.state.lock().unwrap();
        remote.1 = None;
        remote.0.revision += 1;
    }
    let (status, _) = request(
        &fixture.state,
        "POST",
        "/v1/health/batches",
        Some(&fixture.token),
        Some(body.clone()),
    )
    .await;
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(&ledger_counts(&fixture.state)[..2], &[0, 0]);

    fixture.custody.state.lock().unwrap().0 = original;
    let (status, receipt) = request(
        &fixture.state,
        "POST",
        "/v1/health/batches",
        Some(&fixture.token),
        Some(body),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED);
    assert_eq!(receipt["batch_id"], "conflicted-custody-batch");
    assert_eq!(&ledger_counts(&fixture.state)[..2], &[1, 1]);
}

fn canonical_temp_dir() -> TempDir {
    TempDir::new_in(std::env::temp_dir().canonicalize().unwrap()).unwrap()
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
    request_bytes(state, method, uri, token, body).await
}

async fn request_bytes(
    state: &ServerState,
    method: &str,
    uri: &str,
    token: Option<&str>,
    body: Vec<u8>,
) -> (StatusCode, Value) {
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
    let dir = canonical_temp_dir();
    initialize_coordinator(&dir.path().join("coord")).unwrap();
    let data_root = dir.path().join("data");
    let control_db = dir.path().join("control/control.db");
    let control_mirror = dir.path().join("control/mirror");
    let storage = StoragePaths::new(
        data_root.clone(),
        data_root.join("health.db"),
        control_db.clone(),
        control_mirror.clone(),
        dir.path().join("storage-backups"),
    );
    storage.prepare_empty_layout().unwrap();
    let control_store = ControlStore::initialize(control_db, control_mirror).unwrap();
    initialize_health_database(&storage.health_db, &control_store.store_id().unwrap()).unwrap();
    let journal_root = dir.path().join("synthetic-ack-journal");
    std::fs::create_dir(&journal_root).unwrap();
    AckJournal::initialize(&journal_root).unwrap();
    let journal = Arc::new(AckJournal::open(&journal_root).unwrap());
    // Route-only synthetic fixture: one harmless tombstone supplies the
    // sequence-1 control prefix required by custody's generation-zero shape.
    // This is not a verified adoption backup or production restore evidence.
    control_store
        .append_credential_revoked(
            "fixture-retired-device",
            &"f".repeat(64),
            "2026-09-19T00:00:00Z",
        )
        .unwrap();
    let checkpoint = control_store.checkpoint().unwrap();
    // This is a synthetic API fixture, not an attested production backup.
    journal
        .bind_baseline(&Baseline {
            snapshot_id: "00000000-0000-4000-8000-000000000001".to_owned(),
            snapshot_sha256: hex::encode(Sha256::digest(
                std::fs::read(&storage.health_db).unwrap(),
            )),
            receipt_inventory_sha256: hex::encode(Sha256::digest(b"[]")),
            control_store_id: checkpoint.store_id,
            control_head_sequence: checkpoint.sequence,
            control_head_hash: checkpoint.current_hash,
        })
        .unwrap();
    let custody = Arc::new(SyntheticCustody::new(CustodyState {
        format: 2,
        revision: 2,
        control: control_store.checkpoint().unwrap(),
        ack: Some(journal.checkpoint().unwrap()),
        baseline_sha256: journal.baseline_sha256().unwrap(),
    }));
    let custody_lock_path = dir.path().join("coord/.custody-operation.lock");
    let control_store = control_store
        .with_custody(custody.clone(), custody_lock_path.clone())
        .unwrap()
        .with_ack_journal(Arc::clone(&journal))
        .unwrap();
    control_store.verify_custody().unwrap();
    let state = ServerState {
        db_path: storage.health_db,
        control_store: Some(control_store),
        ack_journal: Some(journal),
        custody: Some(custody.clone()),
        custody_lock_path: Some(custody_lock_path),
        projection_notify: Arc::new(Notify::new()),
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
    assert_eq!(status, StatusCode::CREATED, "pairing response: {response}");
    Fixture {
        _dir: dir,
        state,
        custody,
        token: response["token"].as_str().unwrap().to_owned(),
    }
}

fn quantity(event_id: &str, revision: i64, value: f64) -> Value {
    json!({"event_id":event_id,"revision":revision,"operation":"upsert","kind":"quantity","type":"HKQuantityTypeIdentifierHeartRate","source":{"bundle_id":"com.apple.health","name":"Apple Watch"},"start_utc":"2026-09-18T00:00:00Z","end_utc":"2026-09-18T00:01:00Z","value":value,"unit":"count/min","metadata":{}})
}

fn batch(batch_id: &str, events: Vec<Value>) -> Value {
    json!({"schema_version":1,"batch_id":batch_id,"device_id":"phone-1","events":events})
}

fn ledger_counts(state: &ServerState) -> Vec<i64> {
    let connection = open_db(&state.db_path).unwrap();
    ["events", "receipts", "outbox", "audit", "devices"]
        .iter()
        .map(|table| {
            connection
                .query_row(&format!("SELECT count(*) FROM {table}"), [], |row| {
                    row.get(0)
                })
                .unwrap()
        })
        .collect()
}

fn receiver_command(state: &ServerState) -> Command {
    let mut command = Command::new(env!("CARGO_BIN_EXE_boaz-health-receiver"));
    command.env(
        "BOAZ_HEALTH_COORD_DIR",
        state
            .db_path
            .parent()
            .unwrap()
            .parent()
            .unwrap()
            .join("coord"),
    );
    command.env("BOAZ_HEALTH_DB", &state.db_path);
    command.env("BOAZ_HEALTH_DATA_ROOT", state.db_path.parent().unwrap());
    let control = state.control_store.as_ref().unwrap();
    command.env("BOAZ_HEALTH_CONTROL_DB", control.db_path());
    command.env("BOAZ_HEALTH_CONTROL_MIRROR_DIR", control.mirror_dir());
    command.env(
        "BOAZ_HEALTH_BACKUP_DIR",
        state.db_path.parent().unwrap().join("cli-managed-backups"),
    );
    command.env_remove("BOAZ_HEALTH_BACKUP_VOLUME_ENCRYPTED");
    command
}

fn synthetic_coordinator(directory: &std::path::Path) -> PathBuf {
    let root = directory.join("coord");
    initialize_coordinator(&root).unwrap();
    root
}

fn managed_paths(fixture: &Fixture, directory: &std::path::Path) -> StoragePaths {
    let control = fixture.state.control_store.as_ref().unwrap();
    StoragePaths::new(
        fixture.state.db_path.parent().unwrap().to_path_buf(),
        fixture.state.db_path.clone(),
        control.db_path().to_path_buf(),
        control.mirror_dir().to_path_buf(),
        directory.to_path_buf(),
    )
}

fn synthetic_backup(fixture: &Fixture, destination: &std::path::Path) {
    let paths = managed_paths(fixture, destination.parent().unwrap());
    let control = fixture
        .state
        .control_store
        .as_ref()
        .unwrap()
        .clone()
        .with_managed_backup_root(destination.parent().unwrap().canonicalize().unwrap())
        .unwrap();
    receiver_cli::backup_synthetic_test(&paths, destination, &control).unwrap();
}

fn synthetic_backup_at(
    fixture: &Fixture,
    destination: &std::path::Path,
    at: chrono::DateTime<chrono::Utc>,
) {
    let paths = managed_paths(fixture, destination.parent().unwrap());
    let control = fixture
        .state
        .control_store
        .as_ref()
        .unwrap()
        .clone()
        .with_managed_backup_root(destination.parent().unwrap().canonicalize().unwrap())
        .unwrap();
    receiver_cli::backup_synthetic_test_at(&paths, destination, &control, at).unwrap();
}

fn synthetic_prune(
    fixture: &Fixture,
    configured_directory: &std::path::Path,
    requested_directory: &std::path::Path,
) -> Result<(), Box<dyn std::error::Error>> {
    let control = fixture
        .state
        .control_store
        .as_ref()
        .unwrap()
        .clone()
        .with_managed_backup_root(configured_directory.canonicalize()?)?;
    receiver_cli::prune_backups_synthetic_test(
        &managed_paths(fixture, configured_directory),
        requested_directory,
        &control,
    )
}

fn synthetic_prune_at(
    fixture: &Fixture,
    directory: &std::path::Path,
    at: chrono::DateTime<chrono::Utc>,
) -> Result<(), Box<dyn std::error::Error>> {
    let control = fixture
        .state
        .control_store
        .as_ref()
        .unwrap()
        .clone()
        .with_managed_backup_root(directory.canonicalize()?)?;
    receiver_cli::prune_backups_synthetic_test_at(
        &managed_paths(fixture, directory),
        directory,
        &control,
        at,
    )
}

fn create_managed_backup(fixture: &Fixture, directory: &std::path::Path, name: &str) -> PathBuf {
    std::fs::create_dir_all(directory).unwrap();
    let destination = directory.join(name);
    synthetic_backup(fixture, &destination);
    destination
}

fn checkpoint_control_for_offline_migration(path: &std::path::Path) {
    // A migration must not inspect a possibly stale main file while control
    // transactions still live in WAL. This synthetic fixture performs the
    // explicit offline checkpoint required before the zero-write preflight.
    let connection = rusqlite::Connection::open(path).unwrap();
    let (busy, _, _): (i64, i64, i64) = connection
        .query_row("PRAGMA wal_checkpoint(TRUNCATE)", [], |row| {
            Ok((row.get(0)?, row.get(1)?, row.get(2)?))
        })
        .unwrap();
    assert_eq!(busy, 0);
    drop(connection);
    assert!(!path.with_extension("db-wal").exists());
    assert!(!path.with_extension("db-shm").exists());
}

fn synthetic_empty_v1_control(path: &std::path::Path, store_id: &str) {
    let connection = rusqlite::Connection::open(path).unwrap();
    connection
        .execute_batch(&format!(
            "PRAGMA application_id={}; PRAGMA user_version=1;
             CREATE TABLE control_meta (
                 singleton INTEGER PRIMARY KEY CHECK(singleton = 1),
                 store_id TEXT NOT NULL UNIQUE, created_at TEXT NOT NULL
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
                 device_id TEXT, token_hash TEXT, erasure_id TEXT, secret_hash TEXT,
                 snapshot_id TEXT, restore_epoch TEXT, occurred_at TEXT NOT NULL,
                 deadline_at TEXT, previous_hash TEXT NOT NULL,
                 current_hash TEXT NOT NULL UNIQUE
             );
             CREATE INDEX control_events_device ON control_events(device_id, sequence);
             CREATE INDEX control_events_erasure ON control_events(erasure_id, sequence);
             CREATE INDEX control_events_token_type ON control_events(token_hash, event_type);
             CREATE INDEX control_events_type_sequence ON control_events(event_type, sequence);",
            boaz_health_receiver::control::CONTROL_APPLICATION_ID
        ))
        .unwrap();
    connection
        .execute(
            "INSERT INTO control_meta(singleton,store_id,created_at) VALUES (1,?1,'2026-09-19T00:00:00Z')",
            [store_id],
        )
        .unwrap();
    drop(connection);
    std::fs::write(
        path.parent().unwrap().join("control.head.json"),
        serde_json::to_vec(&json!({
            "store_id":store_id,
            "sequence":0,
            "current_hash":"0".repeat(64)
        }))
        .unwrap(),
    )
    .unwrap();
}

#[test]
fn cli_migrates_compatible_control_v1_but_rejects_mismatched_health_without_writes() {
    for mismatched in [false, true] {
        let directory = canonical_temp_dir();
        let coordinator = synthetic_coordinator(directory.path());
        let data_root = directory.path().join("data");
        let health_db = data_root.join("health.db");
        let control_db = directory.path().join("control/control.db");
        let mirror = directory.path().join("control/mirror");
        let backups = directory.path().join("backups");
        let paths = StoragePaths::new(
            data_root.clone(),
            health_db.clone(),
            control_db.clone(),
            mirror.clone(),
            backups.clone(),
        );
        paths.prepare_empty_layout().unwrap();
        initialize_health_database(
            &health_db,
            if mismatched {
                "different-store"
            } else {
                "v1-store"
            },
        )
        .unwrap();
        synthetic_empty_v1_control(&control_db, "v1-store");
        let store = ControlStore::new(control_db.clone(), mirror).unwrap();
        assert_eq!(store.preflight_migration().unwrap(), "v1-store");
        let before_control = std::fs::read(&control_db).unwrap();
        let before_health = std::fs::read(&health_db).unwrap();
        let output = Command::new(env!("CARGO_BIN_EXE_boaz-health-receiver"))
            .arg("migrate-storage")
            .env("BOAZ_HEALTH_DATA_ROOT", &data_root)
            .env("BOAZ_HEALTH_DB", &health_db)
            .env("BOAZ_HEALTH_CONTROL_DB", &control_db)
            .env("BOAZ_HEALTH_CONTROL_MIRROR_DIR", store.mirror_dir())
            .env("BOAZ_HEALTH_BACKUP_DIR", &backups)
            .env("BOAZ_HEALTH_COORD_DIR", &coordinator)
            .env("BOAZ_HEALTH_BOOTSTRAP", "1")
            .env_remove("BOAZ_HEALTH_UPLOAD_ENABLED")
            .output()
            .unwrap();
        if mismatched {
            assert!(!output.status.success());
            assert_eq!(std::fs::read(&control_db).unwrap(), before_control);
            assert_eq!(std::fs::read(&health_db).unwrap(), before_health);
            assert!(!data_root.join("storage-lifecycle.lock").exists());
            assert!(!control_db.with_extension("db-wal").exists());
            assert!(!control_db.with_extension("db-shm").exists());
        } else {
            assert!(
                output.status.success(),
                "{}",
                String::from_utf8_lossy(&output.stderr)
            );
            store.verify().unwrap();
            assert_eq!(store.store_id().unwrap(), "v1-store");
            let connection = rusqlite::Connection::open(&control_db).unwrap();
            let version: i64 = connection
                .query_row("PRAGMA user_version", [], |row| row.get(0))
                .unwrap();
            assert_eq!(version, 3);
            boaz_health_receiver::database::verify_health_database(&health_db, Some("v1-store"))
                .unwrap();
        }
    }
}

fn assert_unsafe_migration_creates_no_files(setup: impl FnOnce(&std::path::Path)) {
    let directory = canonical_temp_dir();
    let coordinator = synthetic_coordinator(directory.path());
    let data_root = directory.path().join("data");
    std::fs::create_dir(&data_root).unwrap();
    let health_db = data_root.join("health.db");
    setup(&health_db);
    let control_db = directory.path().join("control/control.db");
    let control_mirror = directory.path().join("control/mirror");
    let backup_dir = directory.path().join("backups");
    let output = Command::new(env!("CARGO_BIN_EXE_boaz-health-receiver"))
        .arg("migrate-storage")
        .env("BOAZ_HEALTH_DATA_ROOT", &data_root)
        .env("BOAZ_HEALTH_DB", &health_db)
        .env("BOAZ_HEALTH_CONTROL_DB", &control_db)
        .env("BOAZ_HEALTH_CONTROL_MIRROR_DIR", &control_mirror)
        .env("BOAZ_HEALTH_BACKUP_DIR", &backup_dir)
        .env("BOAZ_HEALTH_COORD_DIR", &coordinator)
        .env("BOAZ_HEALTH_BOOTSTRAP", "1")
        .env_remove("BOAZ_HEALTH_UPLOAD_ENABLED")
        .output()
        .unwrap();
    assert!(
        !output.status.success(),
        "unsafe database was accepted: {}",
        String::from_utf8_lossy(&output.stdout)
    );
    let entries: Vec<String> = std::fs::read_dir(&data_root)
        .unwrap()
        .map(|entry| entry.unwrap().file_name().into_string().unwrap())
        .collect();
    assert_eq!(entries, vec!["health.db"]);
    assert!(!directory.path().join("control").exists());
    assert!(!backup_dir.exists());
    assert!(!data_root.join("health-operations.lock").exists());
    assert!(!data_root.join("health-migration.lock").exists());
    assert!(!health_db.with_extension("db-wal").exists());
    assert!(!health_db.with_extension("db-shm").exists());
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
async fn unbound_health_erasure_row_cannot_claim_complete() {
    let fixture = fixture().await;
    let erasure_id = "unbound-complete-erasure";
    let secret = "e".repeat(64);
    let secret_hash = hex::encode(Sha256::digest(secret.as_bytes()));
    let connection = open_db(&fixture.state.db_path).unwrap();
    connection.execute(
        "INSERT INTO erasures(device_id,erasure_id,erasure_secret_hash,requested_at,metrics_deleted_at,backups_expired_at,backup_delete_by)
         VALUES (?1,?2,?3,?4,?5,?6,?7)",
        rusqlite::params![
            "unbound-phone",
            erasure_id,
            secret_hash,
            "2026-09-19T00:00:00Z",
            "2026-09-19T00:01:00Z",
            "2026-10-19T00:00:00Z",
            "2026-10-19T00:00:00Z"
        ],
    ).unwrap();
    let checkpoint = fixture
        .state
        .control_store
        .as_ref()
        .unwrap()
        .checkpoint()
        .unwrap();
    let (get_status, get_body) = request(
        &fixture.state,
        "GET",
        "/v1/health/erasures/unbound-complete-erasure",
        Some(&secret),
        None,
    )
    .await;
    let (post_status, post_body) = request(
        &fixture.state,
        "POST",
        "/v1/health/erase",
        Some(&secret),
        Some(json!({
            "confirmation":"ERASE_CLOUD_HEALTH_DATA",
            "erasure_id":erasure_id,
            "erasure_secret":secret
        })),
    )
    .await;
    assert_eq!(
        fixture
            .state
            .control_store
            .as_ref()
            .unwrap()
            .checkpoint()
            .unwrap(),
        checkpoint
    );
    assert!(
        get_status != StatusCode::OK && post_status != StatusCode::OK,
        "unbound health-row completion leaked: GET {get_status} {get_body}, POST {post_status} {post_body}"
    );
}

#[tokio::test]
async fn control_intent_without_metric_or_backup_verification_cannot_claim_complete() {
    let fixture = fixture().await;
    let erasure_id = "premature-complete-erasure";
    let secret = "f".repeat(64);
    let secret_hash = hex::encode(Sha256::digest(secret.as_bytes()));
    let token_hash = hex::encode(Sha256::digest(fixture.token.as_bytes()));
    let control = fixture.state.control_store.as_ref().unwrap();
    control
        .append_erasure_intent(
            "phone-1",
            &token_hash,
            erasure_id,
            &secret_hash,
            "2026-09-19T00:00:00Z",
            "2026-10-19T00:00:00Z",
        )
        .unwrap();
    control
        .append_health_erasure_verified("phone-1", erasure_id, "2026-09-19T00:01:00Z")
        .unwrap();
    let connection = open_db(&fixture.state.db_path).unwrap();
    connection.execute(
        "INSERT INTO erasures(device_id,erasure_id,erasure_secret_hash,requested_at,metrics_deleted_at,backups_expired_at,backup_delete_by)
         VALUES (?1,?2,?3,?4,?5,?6,?7)",
        rusqlite::params![
            "phone-1",
            erasure_id,
            secret_hash,
            "2026-09-19T00:00:00Z",
            "2026-09-19T00:02:00Z",
            "2026-10-19T00:00:00Z",
            "2026-10-19T00:00:00Z"
        ],
    ).unwrap();
    let checkpoint = control.checkpoint().unwrap();
    let (get_status, get_body) = request(
        &fixture.state,
        "GET",
        "/v1/health/erasures/premature-complete-erasure",
        Some(&secret),
        None,
    )
    .await;
    let (post_status, post_body) = request(
        &fixture.state,
        "POST",
        "/v1/health/erase",
        Some(&secret),
        Some(json!({
            "confirmation":"ERASE_CLOUD_HEALTH_DATA",
            "erasure_id":erasure_id,
            "erasure_secret":secret
        })),
    )
    .await;
    assert_eq!(control.checkpoint().unwrap(), checkpoint);
    assert!(
        get_body["status"] != "complete" && post_body["status"] != "complete",
        "unverified completion leaked: GET {get_status} {get_body}, POST {post_status} {post_body}"
    );
}

#[test]
fn projection_worker_cannot_converge_erasure_during_backup_operation_lock() {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let fixture = runtime.block_on(fixture());
    let control = fixture.state.control_store.as_ref().unwrap();
    let token_hash = hex::encode(Sha256::digest(fixture.token.as_bytes()));
    control
        .append_erasure_intent(
            "phone-1",
            &token_hash,
            "worker-lock-erasure",
            &"d".repeat(64),
            "2026-09-19T00:00:00Z",
            "2026-10-19T00:00:00Z",
        )
        .unwrap();
    let prior = control.checkpoint().unwrap();
    let backup_guard = operation_lock(&fixture.state.db_path, false).unwrap();
    let metrics = fixture._dir.path().join("worker-lock-metrics");
    std::fs::create_dir(&metrics).unwrap();
    let config = VmConfig {
        binary: fixture._dir.path().join("absent-native-vm"),
        storage: metrics,
    };
    let state = fixture.state.clone();
    let (ready_tx, ready_rx) = std::sync::mpsc::channel();
    let (done_tx, done_rx) = std::sync::mpsc::channel();
    let worker = std::thread::spawn(move || {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        ready_tx.send(()).unwrap();
        done_tx
            .send(runtime.block_on(project_once(&state, &config)))
            .unwrap();
    });
    ready_rx.recv().unwrap();
    let _ = done_rx.recv_timeout(std::time::Duration::from_millis(500));
    let health = open_db(&fixture.state.db_path).unwrap();
    let device_count: i64 = health
        .query_row(
            "SELECT count(*) FROM devices WHERE device_id='phone-1'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    let head_while_frozen = control.checkpoint().unwrap();
    drop(backup_guard);
    worker.join().unwrap();
    assert_eq!(
        device_count, 1,
        "worker changed the health ledger during backup freeze"
    );
    assert_eq!(
        head_while_frozen, prior,
        "worker advanced control during backup freeze"
    );
}

#[tokio::test]
async fn committed_erasure_intent_can_resume_with_the_original_token() {
    let fixture = fixture().await;
    let erasure_secret = "edededededededededededededededededededededededededededededededed";
    let token_hash = hex::encode(Sha256::digest(fixture.token.as_bytes()));
    let secret_hash = hex::encode(Sha256::digest(erasure_secret.as_bytes()));
    fixture
        .state
        .control_store
        .as_ref()
        .unwrap()
        .append_erasure_intent(
            "phone-1",
            &token_hash,
            "resume-erasure",
            &secret_hash,
            "2026-09-19T00:00:00Z",
            "2026-10-19T00:00:00Z",
        )
        .unwrap();
    let (status, response) = request(
        &fixture.state,
        "POST",
        "/v1/health/erase",
        Some(&fixture.token),
        Some(json!({
            "confirmation":"ERASE_CLOUD_HEALTH_DATA",
            "erasure_id":"resume-erasure",
            "erasure_secret":erasure_secret
        })),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(response["status"], "pending_metrics");
    let connection = open_db(&fixture.state.db_path).unwrap();
    let devices: i64 = connection
        .query_row("SELECT count(*) FROM devices", [], |row| row.get(0))
        .unwrap();
    assert_eq!(devices, 0);
}

#[tokio::test]
async fn control_tombstone_blocks_auth_before_health_convergence() {
    let fixture = fixture().await;
    let token_hash = hex::encode(Sha256::digest(fixture.token.as_bytes()));
    fixture
        .state
        .control_store
        .as_ref()
        .unwrap()
        .append_credential_revoked("phone-1", &token_hash, "2026-09-19T00:00:00Z")
        .unwrap();
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
    let revoked_at: Option<String> = connection
        .query_row(
            "SELECT revoked_at FROM devices WHERE device_id='phone-1'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert!(revoked_at.is_none());
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
    let denied = receiver_command(&fixture.state)
        .arg("backup")
        .arg(&backup)
        .env("BOAZ_HEALTH_BACKUP_DIR", &backup_dir)
        .env_remove("BOAZ_HEALTH_BACKUP_VOLUME_ENCRYPTED")
        .status()
        .unwrap();
    assert!(!denied.success());
    assert!(!backup.exists());
    let flag_only = receiver_command(&fixture.state)
        .arg("backup")
        .arg(&backup)
        .env("BOAZ_HEALTH_BACKUP_DIR", &backup_dir)
        .env("BOAZ_HEALTH_BACKUP_VOLUME_ENCRYPTED", "1")
        .output()
        .unwrap();
    assert!(!flag_only.status.success());
    let reason = String::from_utf8_lossy(&flag_only.stderr);
    assert!(
        reason.contains("physically verified dm-crypt")
            || reason.contains("independent encrypted device")
            || reason.contains("Adopted recovery coordinator is incomplete")
    );
    assert!(!backup.exists());
    synthetic_backup(&fixture, &backup);
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
        control_volume: fixture._dir.path().to_path_buf(),
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

#[tokio::test]
async fn devices_cannot_read_write_or_erase_each_others_records() {
    let fixture = fixture().await;
    let code = create_pairing_code(&open_db(&fixture.state.db_path).unwrap()).unwrap();
    let (status, paired) = request(
        &fixture.state,
        "POST",
        "/v1/health/pairings",
        None,
        Some(json!({"code": code, "device_id": "phone-2"})),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED);
    let second_token = paired["token"].as_str().unwrap();
    let first_payload = batch("first-batch", vec![quantity("shared-sample", 1, 61.0)]);
    let (status, _) = request(
        &fixture.state,
        "POST",
        "/v1/health/batches",
        Some(&fixture.token),
        Some(first_payload.clone()),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED);
    let before = ledger_counts(&fixture.state);
    let (status, _) = request(
        &fixture.state,
        "POST",
        "/v1/health/batches",
        Some(second_token),
        Some(first_payload),
    )
    .await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
    assert_eq!(ledger_counts(&fixture.state), before);
    let (status, _) = request(
        &fixture.state,
        "GET",
        "/v1/health/batches/first-batch/receipt",
        Some(second_token),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    let mut second_payload = batch("second-batch", vec![quantity("shared-sample", 1, 92.0)]);
    second_payload["device_id"] = json!("phone-2");
    let (status, second_receipt) = request(
        &fixture.state,
        "POST",
        "/v1/health/batches",
        Some(second_token),
        Some(second_payload),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED);
    let (status, _) = request(&fixture.state, "POST", "/v1/health/erase", Some(&fixture.token), Some(json!({"confirmation":"ERASE_CLOUD_HEALTH_DATA","erasure_id":"first-erasure","erasure_secret":"cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"}))).await;
    assert_eq!(status, StatusCode::OK);
    let (status, receipt) = request(
        &fixture.state,
        "GET",
        "/v1/health/batches/second-batch/receipt",
        Some(second_token),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(receipt, second_receipt);
    let (status, remaining) = request(
        &fixture.state,
        "GET",
        "/v1/health/status",
        Some(second_token),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(remaining["sample_count"], 1);
    let connection = open_db(&fixture.state.db_path).unwrap();
    let record: (String, f64) = connection
        .query_row("SELECT device_id,value FROM events", [], |row| {
            Ok((row.get(0)?, row.get(1)?))
        })
        .unwrap();
    assert_eq!(record, ("phone-2".to_owned(), 92.0));
}

#[tokio::test]
async fn cross_device_batch_id_collision_is_a_conflict_and_does_not_write() {
    let fixture = fixture().await;
    let connection = open_db(&fixture.state.db_path).unwrap();
    let code = create_pairing_code(&connection).unwrap();
    let (status, paired) = request(
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
        "/v1/health/batches",
        Some(&fixture.token),
        Some(batch("collision", vec![quantity("first-sample", 1, 61.0)])),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED);
    let before = ledger_counts(&fixture.state);
    let mut collision = batch("collision", vec![quantity("second-sample", 1, 90.0)]);
    collision["device_id"] = json!("phone-2");
    let (status, response) = request(
        &fixture.state,
        "POST",
        "/v1/health/batches",
        paired["token"].as_str(),
        Some(collision),
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT);
    assert_eq!(response["error"], "batch_conflict");
    assert_eq!(ledger_counts(&fixture.state), before);
}

#[tokio::test]
async fn expired_pairing_is_rejected_and_failed_pairing_does_not_consume_code() {
    let fixture = fixture().await;
    let connection = open_db(&fixture.state.db_path).unwrap();
    let expired = create_pairing_code(&connection).unwrap();
    connection
        .execute(
            "UPDATE pairing_codes SET expires_at='2000-01-01T00:00:00Z' WHERE code_hash=?1",
            [hex::encode(Sha256::digest(expired.as_bytes()))],
        )
        .unwrap();
    let before = ledger_counts(&fixture.state);
    let (status, _) = request(
        &fixture.state,
        "POST",
        "/v1/health/pairings",
        None,
        Some(json!({"code":expired,"device_id":"phone-2"})),
    )
    .await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
    assert_eq!(ledger_counts(&fixture.state), before);
    let used: Option<String> = connection
        .query_row(
            "SELECT used_at FROM pairing_codes WHERE code_hash=?1",
            [hex::encode(Sha256::digest(expired.as_bytes()))],
            |row| row.get(0),
        )
        .unwrap();
    assert!(used.is_none());
    let fresh = create_pairing_code(&connection).unwrap();
    let (status, response) = request(
        &fixture.state,
        "POST",
        "/v1/health/pairings",
        None,
        Some(json!({"code":fresh,"device_id":"phone-1"})),
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT);
    assert_eq!(response["error"], "device_exists");
    assert_eq!(ledger_counts(&fixture.state), before);
    let (status, _) = request(
        &fixture.state,
        "POST",
        "/v1/health/pairings",
        None,
        Some(json!({"code":fresh,"device_id":"phone-2"})),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED);
}

#[tokio::test]
async fn late_revision_conflict_rolls_back_every_record_receipt_job_and_audit() {
    let fixture = fixture().await;
    let (status, _) = request(
        &fixture.state,
        "POST",
        "/v1/health/batches",
        Some(&fixture.token),
        Some(batch("seed", vec![quantity("existing", 1, 62.0)])),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED);
    let before = ledger_counts(&fixture.state);
    let (status, response) = request(
        &fixture.state,
        "POST",
        "/v1/health/batches",
        Some(&fixture.token),
        Some(batch(
            "atomic-failure",
            vec![
                quantity("new-first", 1, 72.0),
                quantity("existing", 1, 99.0),
            ],
        )),
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT);
    assert_eq!(response["error"], "revision_conflict");
    assert_eq!(ledger_counts(&fixture.state), before);
    let connection = open_db(&fixture.state.db_path).unwrap();
    let value: f64 = connection
        .query_row(
            "SELECT value FROM events WHERE event_id='existing'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(value, 62.0);
    let (status, _) = request(
        &fixture.state,
        "GET",
        "/v1/health/batches/atomic-failure/receipt",
        Some(&fixture.token),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    let (status, receipt) = request(
        &fixture.state,
        "POST",
        "/v1/health/batches",
        Some(&fixture.token),
        Some(batch(
            "atomic-failure",
            vec![
                quantity("new-first", 1, 72.0),
                quantity("existing", 2, 99.0),
            ],
        )),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED);
    assert_eq!(receipt["changed_events"], 2);
}

#[tokio::test]
async fn invalid_schema_and_event_fields_never_mutate_the_ledger() {
    let fixture = fixture().await;
    let before = ledger_counts(&fixture.state);
    let base = batch("invalid", vec![quantity("sample", 1, 62.0)]);
    let mut cases = Vec::new();
    for (path, replacement) in [
        ("/schema_version", json!(2)),
        ("/batch_id", json!("../escape")),
        ("/device_id", json!("")),
        ("/events", json!([])),
        ("/events/0/revision", json!(-1)),
        ("/events/0/event_id", json!("has whitespace")),
        ("/events/0/type", json!("bad/type")),
        ("/events/0/operation", json!("replace")),
        ("/events/0/source", Value::Null),
        ("/events/0/start_utc", json!("yesterday")),
        ("/events/0/end_utc", json!("2026-09-17T00:00:00Z")),
        ("/events/0/value", Value::Null),
        ("/events/0/unit", Value::Null),
        ("/events/0/metadata", json!(["unexpected-array"])),
        ("/events/0/source/name", json!("n".repeat(257))),
        ("/events/0/unit", json!("u".repeat(65))),
    ] {
        let mut invalid = base.clone();
        *invalid.pointer_mut(path).unwrap() = replacement;
        cases.push((path.to_owned(), invalid));
    }
    let mut unknown = base.clone();
    unknown["unexpected_field"] = json!(true);
    cases.push(("unknown batch field".to_owned(), unknown));
    let mut duplicate = base.clone();
    duplicate["events"] = json!([quantity("sample", 1, 62.0), quantity("sample", 2, 63.0)]);
    cases.push(("duplicate event".to_owned(), duplicate));
    for (label, invalid) in cases {
        let (status, response) = request(
            &fixture.state,
            "POST",
            "/v1/health/batches",
            Some(&fixture.token),
            Some(invalid),
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{label}: {response}");
        assert_eq!(response["error"], "invalid_request", "{label}");
        assert_eq!(
            ledger_counts(&fixture.state),
            before,
            "{label} mutated state"
        );
    }
    for raw in [
        b"{broken".as_slice(),
        b"{\"schema_version\":1,\"schema_version\":1}".as_slice(),
    ] {
        let response = router(fixture.state.clone())
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/health/batches")
                    .header("authorization", format!("Bearer {}", fixture.token))
                    .body(Body::from(raw))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        assert_eq!(ledger_counts(&fixture.state), before);
    }
    let mut oversized = base;
    oversized["events"][0]["metadata"] = json!({"padding":"x".repeat(128 * 1024)});
    let (status, response) = request(
        &fixture.state,
        "POST",
        "/v1/health/batches",
        Some(&fixture.token),
        Some(oversized),
    )
    .await;
    assert_eq!(status, StatusCode::PAYLOAD_TOO_LARGE);
    assert_eq!(response["error"], "batch_too_large");
    assert_eq!(ledger_counts(&fixture.state), before);
}

#[tokio::test]
async fn backup_restores_receipts_credentials_and_pending_work_in_separate_database() {
    let fixture = fixture().await;
    let payload = batch("restore-batch", vec![quantity("restore-sample", 1, 67.0)]);
    let (status, receipt) = request(
        &fixture.state,
        "POST",
        "/v1/health/batches",
        Some(&fixture.token),
        Some(payload.clone()),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED);
    let backup = fixture._dir.path().join("boaz-health-restore.db");
    synthetic_backup(&fixture, &backup);
    let manifest: Value =
        serde_json::from_slice(&std::fs::read(backup.with_extension("db.meta.json")).unwrap())
            .unwrap();
    assert_eq!(
        manifest["source_commit_sequence"],
        receipt["commit_sequence"]
    );
    assert!(
        chrono::DateTime::parse_from_rfc3339(manifest["snapshot_started_at"].as_str().unwrap())
            .is_ok()
    );
    let restored_path = fixture._dir.path().join("restored/health.db");
    std::fs::create_dir(restored_path.parent().unwrap()).unwrap();
    std::fs::copy(&backup, &restored_path).unwrap();
    let restored = ServerState {
        db_path: restored_path,
        ..fixture.state.clone()
    };
    assert_eq!(ledger_counts(&restored), ledger_counts(&fixture.state));
    let (status, readback) = request(
        &restored,
        "GET",
        "/v1/health/batches/restore-batch/receipt",
        Some(&fixture.token),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(readback, receipt);
    let (status, replay) = request(
        &restored,
        "POST",
        "/v1/health/batches",
        Some(&fixture.token),
        Some(payload),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(replay, receipt);
    let connection = open_db(&restored.db_path).unwrap();
    let integrity: String = connection
        .query_row("PRAGMA integrity_check", [], |row| row.get(0))
        .unwrap();
    assert_eq!(integrity, "ok");
    let pending: i64 = connection
        .query_row(
            "SELECT count(*) FROM outbox WHERE processed_at IS NULL",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(pending, 1);
    let value: f64 = connection
        .query_row(
            "SELECT value FROM events WHERE event_id='restore-sample'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(value, 67.0);
}

#[tokio::test]
async fn empty_receipt_backup_records_explicit_genesis_replay_watermark() {
    let fixture = fixture().await;
    let backup = fixture._dir.path().join("boaz-health-empty-receipts.db");
    synthetic_backup(&fixture, &backup);
    let manifest: Value =
        serde_json::from_slice(&std::fs::read(backup.with_extension("db.meta.json")).unwrap())
            .unwrap();
    let snapshot = rusqlite::Connection::open(&backup).unwrap();
    let receipt_count: i64 = snapshot
        .query_row("SELECT count(*) FROM receipts", [], |row| row.get(0))
        .unwrap();
    assert_eq!(receipt_count, 0);
    assert_eq!(manifest["source_commit_sequence"].as_i64(), Some(0));
}

#[tokio::test]
async fn backup_commit_watermark_does_not_regress_when_receipts_are_deleted() {
    let fixture = fixture().await;
    let (status, receipt) = request(
        &fixture.state,
        "POST",
        "/v1/health/batches",
        Some(&fixture.token),
        Some(batch(
            "watermark-batch",
            vec![quantity("watermark-sample", 1, 67.0)],
        )),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED);
    let original_sequence = receipt["commit_sequence"].as_i64().unwrap();
    let connection = rusqlite::Connection::open(&fixture.state.db_path).unwrap();
    connection.execute("DELETE FROM outbox", []).unwrap();
    connection.execute("DELETE FROM receipts", []).unwrap();
    drop(connection);
    let backup = fixture._dir.path().join("boaz-health-watermark.db");
    synthetic_backup(&fixture, &backup);
    let manifest: Value =
        serde_json::from_slice(&std::fs::read(backup.with_extension("db.meta.json")).unwrap())
            .unwrap();
    assert_eq!(
        manifest["source_commit_sequence"].as_i64(),
        Some(original_sequence)
    );
}

#[tokio::test]
async fn backup_after_control_erasure_intent_reconciles_before_snapshot() {
    let fixture = fixture().await;
    let (status, _) = request(
        &fixture.state,
        "POST",
        "/v1/health/batches",
        Some(&fixture.token),
        Some(batch(
            "pre-erasure-backup-batch",
            vec![quantity("pre-erasure-backup-event", 1, 68.0)],
        )),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED);
    let health = open_db(&fixture.state.db_path).unwrap();
    let token_hash: String = health
        .query_row(
            "SELECT token_hash FROM devices WHERE device_id='phone-1'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    drop(health);
    let control = fixture.state.control_store.as_ref().unwrap();
    control
        .append_erasure_intent(
            "phone-1",
            &token_hash,
            "erase-before-backup",
            &"a".repeat(64),
            "2026-09-19T00:00:00Z",
            "2026-10-19T00:00:00Z",
        )
        .unwrap();

    let directory = fixture._dir.path().join("managed-after-erasure-intent");
    std::fs::create_dir(&directory).unwrap();
    let snapshot = directory.join("boaz-health-after-erasure-intent.db");
    synthetic_backup(&fixture, &snapshot);
    let backup = rusqlite::Connection::open(&snapshot).unwrap();
    for table in ["events", "receipts", "outbox", "audit", "devices"] {
        let count: i64 = backup
            .query_row(
                &format!("SELECT count(*) FROM {table} WHERE device_id='phone-1'"),
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(count, 0, "old device survived in snapshot table {table}");
    }
    let verified: i64 = backup
        .query_row(
            "SELECT count(*) FROM erasures WHERE erasure_id='erase-before-backup'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(verified, 1);
}

#[tokio::test]
async fn backup_expiry_retains_recent_copies_until_pre_erasure_snapshots_are_gone() {
    let fixture = fixture().await;
    let backup_dir = fixture._dir.path().join("managed-backups");
    std::fs::create_dir(&backup_dir).unwrap();
    let old = backup_dir.join("boaz-health-old.db");
    let recent = backup_dir.join("boaz-health-recent.db");
    let clock = chrono::Utc::now();
    synthetic_backup_at(&fixture, &old, clock - chrono::Duration::days(30));
    synthetic_backup_at(&fixture, &recent, clock - chrono::Duration::days(28));
    let unrelated = backup_dir.join("unrelated.db");
    std::fs::write(
        &unrelated,
        b"unmanaged backup is not part of receiver retention",
    )
    .unwrap();
    let erasure_secret = "dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd";
    let (status, _) = request(&fixture.state, "POST", "/v1/health/erase", Some(&fixture.token), Some(json!({"confirmation":"ERASE_CLOUD_HEALTH_DATA","erasure_id":"expiry-erasure","erasure_secret":erasure_secret}))).await;
    assert_eq!(status, StatusCode::OK);
    let denied = receiver_command(&fixture.state)
        .arg("prune-backups")
        .arg(&backup_dir)
        .env("BOAZ_HEALTH_BACKUP_DIR", &backup_dir)
        .output()
        .unwrap();
    assert!(!denied.status.success());
    assert!(old.exists());
    let flag_only = receiver_command(&fixture.state)
        .arg("prune-backups")
        .arg(&backup_dir)
        .env("BOAZ_HEALTH_BACKUP_DIR", &backup_dir)
        .env("BOAZ_HEALTH_BACKUP_VOLUME_ENCRYPTED", "1")
        .output()
        .unwrap();
    assert!(!flag_only.status.success());
    let reason = String::from_utf8_lossy(&flag_only.stderr);
    assert!(
        reason.contains("physically verified dm-crypt")
            || reason.contains("independent encrypted device")
            || reason.contains("Adopted recovery coordinator is incomplete")
    );
    assert!(old.exists());
    synthetic_prune_at(&fixture, &backup_dir, clock).unwrap();
    assert!(!old.exists());
    assert!(!old.with_extension("db.meta.json").exists());
    assert!(recent.exists());
    assert!(unrelated.exists());
    let (status, pending) = request(
        &fixture.state,
        "GET",
        "/v1/health/erasures/expiry-erasure",
        Some(erasure_secret),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert!(pending["backups_expired_at"].is_null());
    std::fs::rename(&unrelated, fixture._dir.path().join("unrelated.db")).unwrap();
    synthetic_prune_at(&fixture, &backup_dir, clock + chrono::Duration::days(2)).unwrap();
    assert!(!recent.exists());
    let (status, expired) = request(
        &fixture.state,
        "GET",
        "/v1/health/erasures/expiry-erasure",
        Some(erasure_secret),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert!(expired["backups_expired_at"].is_string());
    assert_eq!(expired["status"], "pending_metrics");
}

#[tokio::test]
async fn backup_cli_rejects_unsafe_destinations_and_the_mac_sync_database_path() {
    let fixture = fixture().await;
    let wrong_name = fixture._dir.path().join("unmanaged.db");
    let rejected = receiver_command(&fixture.state)
        .arg("backup")
        .arg(&wrong_name)
        .env("BOAZ_HEALTH_BACKUP_VOLUME_ENCRYPTED", "1")
        .output()
        .unwrap();
    assert!(!rejected.status.success());
    assert!(!wrong_name.exists());
    let paths = managed_paths(&fixture, fixture._dir.path());
    assert!(
        receiver_cli::backup_synthetic_test(
            &paths,
            &wrong_name,
            fixture.state.control_store.as_ref().unwrap(),
        )
        .is_err()
    );
    let existing = fixture._dir.path().join("boaz-health-existing.db");
    std::fs::write(&existing, b"preserve this file").unwrap();
    let rejected = receiver_command(&fixture.state)
        .arg("backup")
        .arg(&existing)
        .env("BOAZ_HEALTH_BACKUP_VOLUME_ENCRYPTED", "1")
        .output()
        .unwrap();
    assert!(!rejected.status.success());
    assert_eq!(std::fs::read(&existing).unwrap(), b"preserve this file");
    assert!(
        receiver_cli::backup_synthetic_test(
            &paths,
            &existing,
            fixture.state.control_store.as_ref().unwrap(),
        )
        .is_err()
    );
    let blocked = receiver_command(&fixture.state)
        .arg("pair-code")
        .env("BOAZ_HEALTH_DB", "/opt/boaz/data/health.db")
        .env("BOAZ_HEALTH_DATA_ROOT", "/opt/boaz/data")
        .output()
        .unwrap();
    assert!(!blocked.status.success());
    assert!(
        String::from_utf8_lossy(&blocked.stderr).contains("outside the Mac sync replacement path")
            || String::from_utf8_lossy(&blocked.stderr)
                .contains("Adopted recovery coordinator is incomplete")
    );
}

#[tokio::test]
async fn backup_and_prune_reject_paths_outside_the_validated_managed_directory() {
    let fixture = fixture().await;
    let managed = fixture._dir.path().join("managed");
    let outside = fixture._dir.path().join("outside");
    std::fs::create_dir(&managed).unwrap();
    std::fs::create_dir(&outside).unwrap();
    let outside_backup = outside.join("boaz-health-outside.db");
    let backup = receiver_command(&fixture.state)
        .arg("backup")
        .arg(&outside_backup)
        .env("BOAZ_HEALTH_BACKUP_DIR", &managed)
        .env("BOAZ_HEALTH_BACKUP_VOLUME_ENCRYPTED", "1")
        .output()
        .unwrap();
    assert!(!backup.status.success());
    assert!(!outside_backup.exists());

    let marker = outside.join("preserve.txt");
    std::fs::write(&marker, b"preserve").unwrap();
    let prune = synthetic_prune(&fixture, &managed, &outside);
    assert!(prune.is_err());
    assert_eq!(std::fs::read(marker).unwrap(), b"preserve");
}

#[tokio::test]
async fn prune_fails_before_mutation_when_any_managed_backup_artifact_is_missing() {
    for missing_database in [true, false] {
        let fixture = fixture().await;
        let directory = fixture._dir.path().join("managed");
        let backup = create_managed_backup(
            &fixture,
            &directory,
            if missing_database {
                "boaz-health-missing-file.db"
            } else {
                "boaz-health-missing-manifest.db"
            },
        );
        let manifest = backup.with_extension("db.meta.json");
        if missing_database {
            std::fs::remove_file(&backup).unwrap();
        } else {
            std::fs::remove_file(&manifest).unwrap();
        }
        let output = synthetic_prune(&fixture, &directory, &directory);
        assert!(output.is_err());
        if missing_database {
            assert!(manifest.exists());
        } else {
            assert!(backup.exists());
        }
    }
}

#[tokio::test]
async fn prune_validates_entire_inventory_before_deleting_an_old_backup() {
    let fixture = fixture().await;
    let directory = fixture._dir.path().join("managed");
    let old = create_managed_backup(&fixture, &directory, "boaz-health-old.db");
    let missing = create_managed_backup(&fixture, &directory, "boaz-health-missing.db");
    let old_manifest = old.with_extension("db.meta.json");
    let mut value: Value = serde_json::from_slice(&std::fs::read(&old_manifest).unwrap()).unwrap();
    value["snapshot_started_at"] =
        json!((chrono::Utc::now() - chrono::Duration::days(30)).to_rfc3339());
    std::fs::write(&old_manifest, serde_json::to_vec(&value).unwrap()).unwrap();
    std::fs::remove_file(&missing).unwrap();

    let before = fixture
        .state
        .control_store
        .as_ref()
        .unwrap()
        .checkpoint()
        .unwrap();
    let output = synthetic_prune(&fixture, &directory, &directory);
    assert!(output.is_err());
    assert!(old.exists());
    assert!(old_manifest.exists());
    assert_eq!(
        fixture
            .state
            .control_store
            .as_ref()
            .unwrap()
            .checkpoint()
            .unwrap(),
        before
    );
}

#[tokio::test]
async fn prune_fails_before_mutation_for_missing_control_event_or_hash_mismatch() {
    {
        let fixture = fixture().await;
        let directory = fixture._dir.path().join("managed");
        let backup = create_managed_backup(&fixture, &directory, "boaz-health-no-control-event.db");
        let manifest = backup.with_extension("db.meta.json");
        std::fs::create_dir_all(fixture._dir.path().join("unrelated-control/mirror")).unwrap();
        let unrelated = ControlStore::initialize(
            fixture._dir.path().join("unrelated-control/control.db"),
            fixture._dir.path().join("unrelated-control/mirror"),
        )
        .unwrap();
        let output = receiver_cli::prune_backups_synthetic_test(
            &managed_paths(&fixture, &directory),
            &directory,
            &unrelated,
        );
        assert!(output.is_err());
        assert!(backup.exists());
        assert!(manifest.exists());
    }

    {
        let fixture = fixture().await;
        let directory = fixture._dir.path().join("managed");
        let backup = create_managed_backup(&fixture, &directory, "boaz-health-bad-hash.db");
        let manifest = backup.with_extension("db.meta.json");
        use std::io::Write;
        std::fs::OpenOptions::new()
            .append(true)
            .open(&backup)
            .unwrap()
            .write_all(b"tamper")
            .unwrap();
        let mut value: Value = serde_json::from_slice(&std::fs::read(&manifest).unwrap()).unwrap();
        value["file_sha256"] = json!(hex::encode(Sha256::digest(std::fs::read(&backup).unwrap())));
        std::fs::write(&manifest, serde_json::to_vec(&value).unwrap()).unwrap();
        let output = synthetic_prune(&fixture, &directory, &directory);
        assert!(output.is_err());
        assert!(backup.exists());
        assert!(manifest.exists());
    }
}

#[tokio::test]
async fn prune_resumes_after_database_removal_only_when_delete_intent_matches() {
    let fixture = fixture().await;
    let directory = fixture._dir.path().join("managed");
    let database = create_managed_backup(&fixture, &directory, "boaz-health-interrupted.db");
    let manifest = database.with_extension("db.meta.json");
    let value: Value = serde_json::from_slice(&std::fs::read(&manifest).unwrap()).unwrap();
    let snapshot_id = value["snapshot_id"].as_str().unwrap();
    let file_hash = value["file_sha256"].as_str().unwrap();
    let control = fixture
        .state
        .control_store
        .as_ref()
        .unwrap()
        .clone()
        .with_managed_backup_root(directory.canonicalize().unwrap())
        .unwrap();
    control
        .append_backup_delete_intent(
            snapshot_id,
            file_hash,
            "boaz-health-interrupted.db",
            &chrono::Utc::now().to_rfc3339(),
        )
        .unwrap();
    std::fs::remove_file(&database).unwrap();

    synthetic_prune(&fixture, &directory, &directory).unwrap();
    assert!(!manifest.exists());
    assert!(control.pending_backup_delete_intents().unwrap().is_empty());
    assert!(control.active_backup_inventory().unwrap().is_empty());
}

#[tokio::test]
async fn prune_refuses_to_reconcile_an_intent_with_changed_manifest() {
    let fixture = fixture().await;
    let directory = fixture._dir.path().join("managed");
    let database = create_managed_backup(&fixture, &directory, "boaz-health-diverged.db");
    let manifest = database.with_extension("db.meta.json");
    let mut value: Value = serde_json::from_slice(&std::fs::read(&manifest).unwrap()).unwrap();
    let snapshot_id = value["snapshot_id"].as_str().unwrap().to_owned();
    let file_hash = value["file_sha256"].as_str().unwrap().to_owned();
    let control = fixture
        .state
        .control_store
        .as_ref()
        .unwrap()
        .clone()
        .with_managed_backup_root(directory.canonicalize().unwrap())
        .unwrap();
    control
        .append_backup_delete_intent(
            &snapshot_id,
            &file_hash,
            "boaz-health-diverged.db",
            &chrono::Utc::now().to_rfc3339(),
        )
        .unwrap();
    value["file_sha256"] = serde_json::json!("0".repeat(64));
    std::fs::write(&manifest, serde_json::to_vec(&value).unwrap()).unwrap();

    assert!(synthetic_prune(&fixture, &directory, &directory).is_err());
    assert!(database.exists());
    assert!(manifest.exists());
    assert_eq!(control.pending_backup_delete_intents().unwrap().len(), 1);
}

#[cfg(unix)]
#[tokio::test]
async fn prune_does_not_confirm_deletion_after_validated_file_is_replaced() {
    use std::os::unix::fs::symlink;

    let fixture = fixture().await;
    let directory = fixture._dir.path().join("managed-prune-race");
    let database = create_managed_backup(&fixture, &directory, "boaz-health-raced.db");
    let manifest: Value =
        serde_json::from_slice(&std::fs::read(database.with_extension("db.meta.json")).unwrap())
            .unwrap();
    let snapshot_id = manifest["snapshot_id"].as_str().unwrap();
    let file_hash = manifest["file_sha256"].as_str().unwrap();
    let control = fixture
        .state
        .control_store
        .as_ref()
        .unwrap()
        .clone()
        .with_managed_backup_root(directory.canonicalize().unwrap())
        .unwrap();
    control
        .append_backup_delete_intent(
            snapshot_id,
            file_hash,
            "boaz-health-raced.db",
            &chrono::Utc::now().to_rfc3339(),
        )
        .unwrap();
    let escaped = fixture._dir.path().join("escaped-original-backup.db");
    let before_hash = file_hash.to_owned();
    let result = receiver_cli::reconcile_pending_backup_deletes_synthetic_test(
        &directory,
        &control,
        |path| {
            std::fs::rename(path, &escaped).unwrap();
            symlink(&escaped, path).unwrap();
        },
    );
    assert!(
        result.is_err(),
        "replacement must not be confirmed as deletion"
    );
    assert_eq!(
        hex::encode(Sha256::digest(std::fs::read(&escaped).unwrap())),
        before_hash
    );
    assert_eq!(control.pending_backup_delete_intents().unwrap().len(), 1);
}

#[tokio::test]
async fn exact_body_limit_is_accepted_and_one_extra_byte_is_rejected() {
    let fixture = fixture().await;
    let mut body = serde_json::to_vec(&batch(
        "exact-limit",
        vec![quantity("boundary-sample", 1, 68.0)],
    ))
    .unwrap();
    body.resize(128 * 1024, b' ');
    let (status, receipt) = request_bytes(
        &fixture.state,
        "POST",
        "/v1/health/batches",
        Some(&fixture.token),
        body.clone(),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED);
    assert_eq!(receipt["accepted_events"], 1);
    assert_eq!(receipt["content_hash"], hex::encode(Sha256::digest(&body)));
    let before = ledger_counts(&fixture.state);
    body.push(b' ');
    let (status, error) = request_bytes(
        &fixture.state,
        "POST",
        "/v1/health/batches",
        Some(&fixture.token),
        body,
    )
    .await;
    assert_eq!(status, StatusCode::PAYLOAD_TOO_LARGE);
    assert_eq!(error["error"], "batch_too_large");
    assert_eq!(ledger_counts(&fixture.state), before);
    let (status, readback) = request(
        &fixture.state,
        "GET",
        "/v1/health/batches/exact-limit/receipt",
        Some(&fixture.token),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(readback, receipt);
}

#[tokio::test]
async fn batch_id_reuse_with_only_json_whitespace_changed_is_a_conflict() {
    let fixture = fixture().await;
    let body = serde_json::to_vec(&batch(
        "whitespace-identity",
        vec![quantity("whitespace-sample", 1, 68.0)],
    ))
    .unwrap();
    let (status, first) = request_bytes(
        &fixture.state,
        "POST",
        "/v1/health/batches",
        Some(&fixture.token),
        body.clone(),
    )
    .await;
    assert_eq!(status, StatusCode::CREATED);
    let before = ledger_counts(&fixture.state);
    let mut reformatted = body.clone();
    reformatted.extend_from_slice(b" \n\t");
    assert_eq!(
        serde_json::from_slice::<Value>(&body).unwrap(),
        serde_json::from_slice::<Value>(&reformatted).unwrap()
    );
    let (status, error) = request_bytes(
        &fixture.state,
        "POST",
        "/v1/health/batches",
        Some(&fixture.token),
        reformatted,
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT);
    assert_eq!(error["error"], "batch_conflict");
    assert_eq!(ledger_counts(&fixture.state), before);
    let (status, original_retry) = request_bytes(
        &fixture.state,
        "POST",
        "/v1/health/batches",
        Some(&fixture.token),
        body,
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(original_retry, first);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_identical_batches_create_one_receipt_event_and_projection_job() {
    let fixture = fixture().await;
    let attempts = 8;
    let start = std::sync::Arc::new(tokio::sync::Barrier::new(attempts + 1));
    let mut tasks = tokio::task::JoinSet::new();
    for _ in 0..attempts {
        let state = fixture.state.clone();
        let token = fixture.token.clone();
        let barrier = start.clone();
        tasks.spawn(async move {
            barrier.wait().await;
            request(
                &state,
                "POST",
                "/v1/health/batches",
                Some(&token),
                Some(batch(
                    "concurrent-batch",
                    vec![quantity("concurrent-sample", 1, 68.0)],
                )),
            )
            .await
        });
    }
    start.wait().await;
    let mut created = 0;
    let mut receipts = Vec::new();
    while let Some(result) = tasks.join_next().await {
        let (status, receipt) = result.unwrap();
        assert!(status == StatusCode::CREATED || status == StatusCode::OK);
        created += usize::from(status == StatusCode::CREATED);
        receipts.push(receipt);
    }
    assert_eq!(created, 1);
    assert_eq!(receipts.len(), attempts);
    assert!(receipts.iter().all(|receipt| receipt == &receipts[0]));
    assert_eq!(ledger_counts(&fixture.state), vec![1, 1, 1, 2, 1]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_pairing_uses_one_code_for_exactly_one_new_device() {
    let fixture = fixture().await;
    let code = create_pairing_code(&open_db(&fixture.state.db_path).unwrap()).unwrap();
    let attempts = 8;
    let start = std::sync::Arc::new(tokio::sync::Barrier::new(attempts + 1));
    let mut tasks = tokio::task::JoinSet::new();
    for index in 0..attempts {
        let state = fixture.state.clone();
        let pairing_code = code.clone();
        let barrier = start.clone();
        tasks.spawn(async move {
            barrier.wait().await;
            request(
                &state,
                "POST",
                "/v1/health/pairings",
                None,
                Some(json!({"code":pairing_code,"device_id":format!("racing-device-{index}")})),
            )
            .await
            .0
        });
    }
    start.wait().await;
    let mut created = 0;
    let mut unauthorized = 0;
    while let Some(result) = tasks.join_next().await {
        let status = result.unwrap();
        assert!(
            status == StatusCode::CREATED || status == StatusCode::UNAUTHORIZED,
            "Unexpected pairing status: {status}"
        );
        created += usize::from(status == StatusCode::CREATED);
        unauthorized += usize::from(status == StatusCode::UNAUTHORIZED);
    }
    assert_eq!(created, 1);
    assert_eq!(unauthorized, attempts - 1);
    assert_eq!(ledger_counts(&fixture.state), vec![0, 0, 0, 2, 2]);
}

#[test]
fn storage_cli_requires_explicit_init_and_verify_is_read_only() {
    let directory = canonical_temp_dir();
    let coordinator = synthetic_coordinator(directory.path());
    let data_root = directory.path().join("data");
    let health_db = data_root.join("health.db");
    let control_db = directory.path().join("control/control.db");
    let control_mirror = directory.path().join("control/mirror");
    let backup_dir = directory.path().join("backups");
    let command = |subcommand: &str| {
        let mut process = Command::new(env!("CARGO_BIN_EXE_boaz-health-receiver"));
        process
            .arg(subcommand)
            .env("BOAZ_HEALTH_DATA_ROOT", &data_root)
            .env("BOAZ_HEALTH_DB", &health_db)
            .env("BOAZ_HEALTH_CONTROL_DB", &control_db)
            .env("BOAZ_HEALTH_CONTROL_MIRROR_DIR", &control_mirror)
            .env("BOAZ_HEALTH_BACKUP_DIR", &backup_dir)
            .env("BOAZ_HEALTH_COORD_DIR", &coordinator)
            .env("BOAZ_HEALTH_BOOTSTRAP", "1")
            .env_remove("BOAZ_HEALTH_UPLOAD_ENABLED");
        process.output().unwrap()
    };
    assert!(!command("verify-storage").status.success());
    assert!(!health_db.exists());
    assert!(command("init-storage").status.success());
    let health_modified = std::fs::metadata(&health_db).unwrap().modified().unwrap();
    let control_modified = std::fs::metadata(&control_db).unwrap().modified().unwrap();
    assert!(command("verify-storage").status.success());
    assert_eq!(
        std::fs::metadata(&health_db).unwrap().modified().unwrap(),
        health_modified
    );
    assert_eq!(
        std::fs::metadata(&control_db).unwrap().modified().unwrap(),
        control_modified
    );
    assert!(!command("init-storage").status.success());
}

#[test]
fn recovery_cli_requires_external_head_separate_encrypted_staging_and_upload_off() {
    let directory = canonical_temp_dir();
    let coordinator = synthetic_coordinator(directory.path());
    let data_root = directory.path().join("data");
    let health_db = data_root.join("health.db");
    let control_db = directory.path().join("control/control.db");
    let control_mirror = directory.path().join("control/mirror");
    let backup_dir = directory.path().join("backups");
    let command = |arguments: &[&str], upload_enabled: bool| {
        let mut process = Command::new(env!("CARGO_BIN_EXE_boaz-health-receiver"));
        process
            .args(arguments)
            .env("BOAZ_HEALTH_DATA_ROOT", &data_root)
            .env("BOAZ_HEALTH_DB", &health_db)
            .env("BOAZ_HEALTH_CONTROL_DB", &control_db)
            .env("BOAZ_HEALTH_CONTROL_MIRROR_DIR", &control_mirror)
            .env("BOAZ_HEALTH_BACKUP_DIR", &backup_dir)
            .env("BOAZ_HEALTH_COORD_DIR", &coordinator)
            .env("BOAZ_HEALTH_BOOTSTRAP", "1")
            .env(
                "BOAZ_HEALTH_UPLOAD_ENABLED",
                if upload_enabled { "1" } else { "0" },
            );
        process.output().unwrap()
    };
    assert!(command(&["init-storage"], false).status.success());
    let source = backup_dir.join("control-bundle");
    let staged = directory.path().join("staging");
    let source_text = source.to_str().unwrap();
    let staged_text = staged.to_str().unwrap();

    let missing_head = command(
        &[
            "restore-control",
            source_text,
            "--staging-path",
            staged_text,
        ],
        false,
    );
    assert!(!missing_head.status.success());
    assert!(String::from_utf8_lossy(&missing_head.stderr).contains("--expected-head"));
    let upload_on = command(
        &["restore-health", source_text, "--staging-path", staged_text],
        true,
    );
    assert!(!upload_on.status.success());
    assert!(String::from_utf8_lossy(&upload_on.stderr).contains("UPLOAD_ENABLED=0"));
    let unencrypted = command(&["backup-control", source_text], false);
    assert!(!unencrypted.status.success());
    assert!(
        String::from_utf8_lossy(&unencrypted.stderr).contains("dm-crypt")
            || String::from_utf8_lossy(&unencrypted.stderr)
                .contains("Adopted recovery coordinator is incomplete")
    );
    assert!(!source.exists());
    assert!(!staged.exists());
    assert!(!data_root.join("storage-lifecycle.lock").exists());
}

#[test]
fn unsafe_migrate_classification_is_read_only_for_zero_partial_unknown_and_corrupt_files() {
    assert_unsafe_migration_creates_no_files(|path| {
        std::fs::File::create(path).unwrap();
    });
    assert_unsafe_migration_creates_no_files(|path| {
        let connection = rusqlite::Connection::open(path).unwrap();
        connection
            .execute_batch("CREATE TABLE devices(device_id TEXT PRIMARY KEY);")
            .unwrap();
    });
    assert_unsafe_migration_creates_no_files(|path| {
        let connection = rusqlite::Connection::open(path).unwrap();
        connection
            .execute_batch(
                "CREATE TABLE foreign_layout(value TEXT); PRAGMA application_id=12345; PRAGMA user_version=1;",
            )
            .unwrap();
    });
    assert_unsafe_migration_creates_no_files(|path| {
        std::fs::write(path, b"not a sqlite database").unwrap();
    });
}

#[test]
fn legacy_migration_rejects_a_nonempty_existing_control_store_before_locking_or_writing() {
    let directory = canonical_temp_dir();
    let coordinator = synthetic_coordinator(directory.path());
    let data_root = directory.path().join("data");
    let health_db = data_root.join("health.db");
    let control_db = directory.path().join("control/control.db");
    let control_mirror = directory.path().join("control/mirror");
    let backup_dir = directory.path().join("backups");
    let mut init = Command::new(env!("CARGO_BIN_EXE_boaz-health-receiver"));
    init.arg("init-storage")
        .env("BOAZ_HEALTH_DATA_ROOT", &data_root)
        .env("BOAZ_HEALTH_DB", &health_db)
        .env("BOAZ_HEALTH_CONTROL_DB", &control_db)
        .env("BOAZ_HEALTH_CONTROL_MIRROR_DIR", &control_mirror)
        .env("BOAZ_HEALTH_BACKUP_DIR", &backup_dir)
        .env("BOAZ_HEALTH_COORD_DIR", &coordinator)
        .env("BOAZ_HEALTH_BOOTSTRAP", "1")
        .env_remove("BOAZ_HEALTH_UPLOAD_ENABLED");
    assert!(init.status().unwrap().success());

    let store = ControlStore::new(control_db.clone(), control_mirror.clone()).unwrap();
    store
        .append_credential_revoked("legacy-phone", "legacy-token-hash", "2026-09-19T00:00:00Z")
        .unwrap();
    let connection = rusqlite::Connection::open(&health_db).unwrap();
    connection
        .execute_batch(
            "DROP TABLE projection_state;
             DROP TABLE storage_meta;
             ALTER TABLE receipts DROP COLUMN projected_generation;
             ALTER TABLE receipts DROP COLUMN projection_mapping_version;
             PRAGMA application_id=0;
             PRAGMA user_version=0;",
        )
        .unwrap();
    drop(connection);
    assert_eq!(
        classify_health_database(&health_db).unwrap(),
        HealthLayout::LegacyV0
    );
    let before_hash = hex::encode(Sha256::digest(std::fs::read(&health_db).unwrap()));
    let before_checkpoint = store.checkpoint().unwrap();
    checkpoint_control_for_offline_migration(&control_db);

    let output = Command::new(env!("CARGO_BIN_EXE_boaz-health-receiver"))
        .arg("migrate-storage")
        .env("BOAZ_HEALTH_DATA_ROOT", &data_root)
        .env("BOAZ_HEALTH_DB", &health_db)
        .env("BOAZ_HEALTH_CONTROL_DB", &control_db)
        .env("BOAZ_HEALTH_CONTROL_MIRROR_DIR", &control_mirror)
        .env("BOAZ_HEALTH_BACKUP_DIR", &backup_dir)
        .env("BOAZ_HEALTH_COORD_DIR", &coordinator)
        .env("BOAZ_HEALTH_BOOTSTRAP", "1")
        .env_remove("BOAZ_HEALTH_UPLOAD_ENABLED")
        .output()
        .unwrap();
    assert!(!output.status.success());
    assert!(String::from_utf8_lossy(&output.stderr).contains("unrelated to the legacy seed"));
    assert_eq!(
        classify_health_database(&health_db).unwrap(),
        HealthLayout::LegacyV0
    );
    assert_eq!(
        hex::encode(Sha256::digest(std::fs::read(&health_db).unwrap())),
        before_hash
    );
    assert_eq!(store.checkpoint().unwrap(), before_checkpoint);
    assert!(!data_root.join("health-operations.lock").exists());
    assert!(!data_root.join("health-migration.lock").exists());
}

#[test]
fn legacy_migration_resumes_after_the_control_seed_was_durably_completed() {
    let directory = canonical_temp_dir();
    let coordinator = synthetic_coordinator(directory.path());
    let data_root = directory.path().join("data");
    let health_db = data_root.join("health.db");
    let control_db = directory.path().join("control/control.db");
    let control_mirror = directory.path().join("control/mirror");
    let backup_dir = directory.path().join("backups");
    let configured = |subcommand: &str| {
        let mut command = Command::new(env!("CARGO_BIN_EXE_boaz-health-receiver"));
        command
            .arg(subcommand)
            .env("BOAZ_HEALTH_DATA_ROOT", &data_root)
            .env("BOAZ_HEALTH_DB", &health_db)
            .env("BOAZ_HEALTH_CONTROL_DB", &control_db)
            .env("BOAZ_HEALTH_CONTROL_MIRROR_DIR", &control_mirror)
            .env("BOAZ_HEALTH_BACKUP_DIR", &backup_dir)
            .env("BOAZ_HEALTH_COORD_DIR", &coordinator)
            .env("BOAZ_HEALTH_BOOTSTRAP", "1")
            .env_remove("BOAZ_HEALTH_UPLOAD_ENABLED");
        command.output().unwrap()
    };
    assert!(configured("init-storage").status.success());
    let connection = rusqlite::Connection::open(&health_db).unwrap();
    connection
        .execute(
            "INSERT INTO devices(device_id,token_hash,created_at,revoked_at) VALUES (?1,?2,?3,?4)",
            rusqlite::params![
                "legacy-phone",
                "legacy-token-hash",
                "2026-09-18T00:00:00Z",
                "2026-09-19T00:00:00Z"
            ],
        )
        .unwrap();
    connection
        .execute_batch(
            "DROP TABLE projection_state;
             DROP TABLE storage_meta;
             ALTER TABLE receipts DROP COLUMN projected_generation;
             ALTER TABLE receipts DROP COLUMN projection_mapping_version;
             PRAGMA application_id=0;
             PRAGMA user_version=0;",
        )
        .unwrap();
    drop(connection);
    let store = ControlStore::new(control_db.clone(), control_mirror.clone()).unwrap();
    let legacy = rusqlite::Connection::open_with_flags(
        &health_db,
        rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY,
    )
    .unwrap();
    store.seed_legacy_for_migration(&legacy).unwrap();
    assert!(store.verify_legacy_seed(&legacy).unwrap());
    drop(legacy);
    checkpoint_control_for_offline_migration(&control_db);

    let output = configured("migrate-storage");
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert_eq!(
        classify_health_database(&health_db).unwrap(),
        HealthLayout::CurrentV2
    );
    let health = open_db(&health_db).unwrap();
    let revoked_at: String = health
        .query_row(
            "SELECT revoked_at FROM devices WHERE device_id='legacy-phone'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(revoked_at, "2026-09-19T00:00:00Z");
}
