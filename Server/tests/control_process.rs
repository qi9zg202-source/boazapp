//! Local process-death fixture. The forced-command custodian runs in a
//! separate process but on this same test host: this is not off-host acceptance.
use boaz_health_receiver::{
    control::{ControlCheckpoint, ControlStore},
    custody::{
        CustodyClient, CustodyError, CustodyReservationV2, CustodyResult, CustodyState,
        initialize_off_host_v2,
    },
};
use serde::Serialize;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::{
    fs::{self, OpenOptions},
    io::Write,
    path::{Path, PathBuf},
    process::{Command, Stdio},
    sync::Arc,
    thread,
    time::{Duration, Instant},
};
use uuid::Uuid;

const NAME: &str = "boaz-health-process-fixture.db";
const SNAPSHOT: &str = "process-fixture-snapshot";
const TIME: &str = "2026-09-19T00:00:00Z";

#[derive(Serialize)]
struct CommittedV2Fixture {
    reservation: CustodyReservationV2,
    successor: CustodyState,
}

#[derive(Serialize)]
struct HistorySealFixture {
    format: u8,
    revision: u64,
    head_sha256: String,
    latest: Option<CommittedV2Fixture>,
}

#[derive(Clone)]
struct LocalForcedCustody {
    root: PathBuf,
    pause: Option<&'static str>,
    marker: PathBuf,
}

impl LocalForcedCustody {
    fn call(&self, request: Value) -> CustodyResult<Value> {
        let mut child = Command::new(env!("CARGO_BIN_EXE_boaz-health-receiver"))
            .arg("custody-protocol")
            .env("SSH_ORIGINAL_COMMAND", "boaz-health-custody-protocol")
            .env("BOAZ_HEALTH_CUSTODY_ROOT", &self.root)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()?;
        child
            .stdin
            .take()
            .unwrap()
            .write_all(&serde_json::to_vec(&request)?)?;
        let output = child.wait_with_output()?;
        if !output.status.success() {
            return Err(CustodyError::Protocol(format!(
                "local synthetic forced command failed: {}",
                String::from_utf8_lossy(&output.stderr)
            )));
        }
        Ok(serde_json::from_slice(&output.stdout)?)
    }

    fn pause_at(&self, point: &'static str) {
        if self.pause == Some(point) {
            fs::write(&self.marker, point).unwrap();
            loop {
                thread::sleep(Duration::from_secs(1));
            }
        }
    }
}

impl CustodyClient for LocalForcedCustody {
    fn read_v2(&self, store_id: &str) -> CustodyResult<CustodyState> {
        Ok(serde_json::from_value(
            self.call(json!({"op":"read_v2","store_id":store_id}))?["state"].clone(),
        )?)
    }

    fn reserve_v2(
        &self,
        predecessor: &CustodyState,
        operation_id: &str,
        intent_sha256: &str,
    ) -> CustodyResult<CustodyReservationV2> {
        let response = self.call(json!({
            "op":"reserve_v2",
            "predecessor":predecessor,
            "operation_id":operation_id,
            "intent_sha256":intent_sha256
        }))?;
        self.pause_at("after_reserve");
        Ok(serde_json::from_value(response["reservation"].clone())?)
    }

    fn compare_and_swap_v2(
        &self,
        reservation: &CustodyReservationV2,
        successor: &CustodyState,
    ) -> CustodyResult<CustodyState> {
        self.pause_at("before_cas");
        let response = self.call(json!({
            "op":"compare_and_swap_v2",
            "reservation":reservation,
            "successor":successor
        }))?;
        self.pause_at("after_cas");
        Ok(serde_json::from_value(response["state"].clone())?)
    }
}

fn fixture_store(root: &Path, pause: Option<&'static str>) -> ControlStore {
    let custody = Arc::new(LocalForcedCustody {
        root: root.join("custodian"),
        pause,
        marker: root.join("paused"),
    });
    ControlStore::new(root.join("control/control.db"), root.join("control/mirror"))
        .unwrap()
        .with_custody(custody, root.join("coord/.custody-operation.lock"))
        .unwrap()
        .with_managed_backup_root(root.join("backups"))
        .unwrap()
}

fn setup(root: &Path) -> (ControlCheckpoint, String) {
    for directory in ["control/mirror", "backups", "coord", "custodian"] {
        fs::create_dir_all(root.join(directory)).unwrap();
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(root.join("custodian"), fs::Permissions::from_mode(0o700)).unwrap();
    }
    OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(root.join("coord/.custody-operation.lock"))
        .unwrap();
    let store =
        ControlStore::initialize(root.join("control/control.db"), root.join("control/mirror"))
            .unwrap();
    let checkpoint = store.checkpoint().unwrap();
    initialize_off_host_v2(&root.join("custodian"), &checkpoint).unwrap();
    let bytes = b"process-level synthetic bytes, never health records";
    let hash = format!("{:x}", Sha256::digest(bytes));
    let database = root.join("backups").join(NAME);
    fs::write(&database, bytes).unwrap();
    let manifest = root.join("backups").join(format!("{NAME}.meta.json"));
    fs::write(
        &manifest,
        serde_json::to_vec(&json!({
            "snapshot_id": SNAPSHOT,
            "file_sha256": hash,
            "control_checkpoint": checkpoint,
            "source_schema_version": 2,
            "source_commit_sequence": null,
            "snapshot_started_at": TIME
        }))
        .unwrap(),
    )
    .unwrap();
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(&database, fs::Permissions::from_mode(0o600)).unwrap();
        fs::set_permissions(&manifest, fs::Permissions::from_mode(0o600)).unwrap();
    }
    (checkpoint, hash)
}

fn sync_synthetic_file(path: &Path, bytes: &[u8]) {
    use std::os::unix::fs::OpenOptionsExt;
    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(path)
        .unwrap();
    file.write_all(bytes).unwrap();
    file.sync_all().unwrap();
    fs::File::open(path.parent().unwrap())
        .unwrap()
        .sync_all()
        .unwrap();
}

fn custody_cas_fixture(
    root: &Path,
) -> (
    LocalForcedCustody,
    CustodyState,
    CustodyState,
    CustodyReservationV2,
    Vec<u8>,
    Vec<u8>,
) {
    let custody_root = root.join("custodian");
    fs::create_dir(&custody_root).unwrap();
    use std::os::unix::fs::PermissionsExt;
    fs::set_permissions(&custody_root, fs::Permissions::from_mode(0o700)).unwrap();
    let control = ControlCheckpoint {
        store_id: "synthetic-process-store".into(),
        sequence: 0,
        current_hash: "a".repeat(64),
    };
    initialize_off_host_v2(&custody_root, &control).unwrap();
    let client = LocalForcedCustody {
        root: custody_root.clone(),
        pause: None,
        marker: root.join("paused"),
    };
    let genesis = client.read_v2(&control.store_id).unwrap();
    let reservation = client
        .reserve_v2(&genesis, "control-1", &"c".repeat(64))
        .unwrap();
    let mut successor = genesis.clone();
    successor.revision = 1;
    successor.control.sequence = 1;
    successor.control.current_hash = "b".repeat(64);
    let record = CommittedV2Fixture {
        reservation: reservation.clone(),
        successor: successor.clone(),
    };
    let record_bytes = serde_json::to_vec(&record).unwrap();
    let mut hash = Sha256::new();
    hash.update(b"boaz-health-custody-v2-history-genesis\0");
    hash.update(fs::read(custody_root.join("state-v2.anchor.json")).unwrap());
    let genesis_hash = hash.finalize();
    let mut hash = Sha256::new();
    hash.update(b"boaz-health-custody-v2-history-record\0");
    hash.update(genesis_hash);
    hash.update(&record_bytes);
    let seal_bytes = serde_json::to_vec(&HistorySealFixture {
        format: 1,
        revision: 1,
        head_sha256: hex::encode(hash.finalize()),
        latest: Some(record),
    })
    .unwrap();
    (
        client,
        genesis,
        successor,
        reservation,
        record_bytes,
        seal_bytes,
    )
}

#[test]
fn child_custody_temporary_publish() {
    let Ok(root) = std::env::var("BOAZ_SYNTHETIC_CUSTODY_TEMP_ROOT") else {
        return;
    };
    let root = PathBuf::from(root);
    let stage = std::env::var("BOAZ_SYNTHETIC_CUSTODY_TEMP_STAGE").unwrap();
    let custody_root = root.join("custodian");
    let destination = match stage.as_str() {
        "mirror_linked" => custody_root
            .join("state-v2.history")
            .join("00000000000000000001.json"),
        "seal_temp" | "wrong_seal_temp" => custody_root.join("state-v2.history-seal.json"),
        "head_temp" => custody_root.join("state-v2.head.json"),
        _ => panic!("unknown synthetic custody stage"),
    };
    let name = destination.file_name().unwrap().to_str().unwrap();
    let temporary = destination
        .parent()
        .unwrap()
        .join(format!(".pending-{name}-{}", Uuid::new_v4()));
    sync_synthetic_file(&temporary, &fs::read(root.join("payload.bin")).unwrap());
    if stage == "mirror_linked" {
        fs::hard_link(&temporary, &destination).unwrap();
        fs::File::open(destination.parent().unwrap())
            .unwrap()
            .sync_all()
            .unwrap();
    }
    fs::write(root.join("paused"), stage).unwrap();
    loop {
        thread::sleep(Duration::from_secs(1));
    }
}

#[test]
fn child_control_backup_publish() {
    let Ok(root) = std::env::var("BOAZ_SYNTHETIC_PROCESS_ROOT") else {
        return;
    };
    let point = std::env::var("BOAZ_SYNTHETIC_PAUSE").unwrap();
    let pause = match point.as_str() {
        "after_reserve" => Some("after_reserve"),
        "before_cas" => Some("before_cas"),
        "after_cas" => Some("after_cas"),
        _ => panic!("unknown synthetic pause"),
    };
    let root = PathBuf::from(root);
    let store = fixture_store(&root, pause);
    let hash = format!(
        "{:x}",
        Sha256::digest(fs::read(root.join("backups").join(NAME)).unwrap())
    );
    let prior = store.checkpoint().unwrap();
    store
        .append_backup_created_with_artifact(SNAPSHOT, &hash, NAME, TIME, &prior)
        .unwrap();
    panic!("synthetic child unexpectedly passed the pause");
}

#[test]
fn child_control_tombstone_publish() {
    let Ok(root) = std::env::var("BOAZ_SYNTHETIC_TOMBSTONE_ROOT") else {
        return;
    };
    let point = std::env::var("BOAZ_SYNTHETIC_TOMBSTONE_PAUSE").unwrap();
    let event = std::env::var("BOAZ_SYNTHETIC_TOMBSTONE_EVENT").unwrap();
    let pause = match point.as_str() {
        "after_reserve" => Some("after_reserve"),
        "before_cas" => Some("before_cas"),
        "after_cas" => Some("after_cas"),
        _ => panic!("unknown synthetic pause"),
    };
    let store = fixture_store(Path::new(&root), pause);
    match event.as_str() {
        "revoke" => store
            .append_credential_revoked("phone-1", &"a".repeat(64), TIME)
            .unwrap(),
        "erase" => store
            .append_erasure_intent(
                "phone-1",
                &"a".repeat(64),
                "synthetic-erasure-1",
                &"b".repeat(64),
                TIME,
                "2026-10-19T00:00:00Z",
            )
            .unwrap(),
        _ => panic!("unknown synthetic control event"),
    }
    panic!("synthetic child unexpectedly passed the pause");
}

#[test]
fn kill_nine_revoke_and_erasure_intents_never_lose_tombstones() {
    use std::os::unix::process::ExitStatusExt;
    for event in ["revoke", "erase"] {
        for point in ["after_reserve", "before_cas", "after_cas"] {
            let directory = tempfile::tempdir().unwrap();
            let root = directory.path().canonicalize().unwrap();
            let (prior, _) = setup(&root);
            let mut child = Command::new(std::env::current_exe().unwrap())
                .args(["--exact", "child_control_tombstone_publish", "--nocapture"])
                .env("BOAZ_SYNTHETIC_TOMBSTONE_ROOT", &root)
                .env("BOAZ_SYNTHETIC_TOMBSTONE_PAUSE", point)
                .env("BOAZ_SYNTHETIC_TOMBSTONE_EVENT", event)
                .stdout(Stdio::piped())
                .stderr(Stdio::piped())
                .spawn()
                .unwrap();
            let marker = root.join("paused");
            let start = Instant::now();
            while !marker.exists() {
                assert!(
                    start.elapsed() < Duration::from_secs(15),
                    "{event}/{point} timed out"
                );
                assert!(
                    child.try_wait().unwrap().is_none(),
                    "{event}/{point} child exited"
                );
                thread::sleep(Duration::from_millis(20));
            }
            child.kill().unwrap();
            let output = child.wait_with_output().unwrap();
            assert_eq!(
                output.status.signal(),
                Some(libc::SIGKILL),
                "{event}/{point}"
            );
            let store = fixture_store(&root, None);
            store.resume_pending_custody().unwrap();
            assert_eq!(store.checkpoint().unwrap().sequence, prior.sequence + 1);
            assert!(store.token_tombstoned(&"a".repeat(64)).unwrap());
            assert!(store.verify_custody().is_ok());
            assert!(!root.join("control/control.pending-intent.json").exists());
        }
    }
}

#[test]
fn kill_nine_at_reserve_and_cas_boundaries_replays_or_stays_closed() {
    use std::os::unix::process::ExitStatusExt;
    for (point, tamper) in [
        ("after_reserve", false),
        ("before_cas", false),
        ("after_cas", false),
        ("before_cas", true),
    ] {
        let directory = tempfile::tempdir().unwrap();
        let root = directory.path().canonicalize().unwrap();
        let (prior, hash) = setup(&root);
        let mut child = Command::new(std::env::current_exe().unwrap())
            .args(["--exact", "child_control_backup_publish", "--nocapture"])
            .env("BOAZ_SYNTHETIC_PROCESS_ROOT", &root)
            .env("BOAZ_SYNTHETIC_PAUSE", point)
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .unwrap();
        let marker = root.join("paused");
        let start = Instant::now();
        while !marker.exists() {
            assert!(
                start.elapsed() < Duration::from_secs(15),
                "{point} timed out"
            );
            assert!(
                child.try_wait().unwrap().is_none(),
                "{point} child exited early"
            );
            thread::sleep(Duration::from_millis(20));
        }
        child.kill().unwrap(); // SIGKILL on Unix, not a graceful test exit.
        let output = child.wait_with_output().unwrap();
        assert_eq!(output.status.signal(), Some(libc::SIGKILL), "{point}");
        let store = fixture_store(&root, None);
        if tamper {
            fs::write(
                root.join("backups").join(format!("{NAME}.meta.json")),
                b"{\"tampered\":true}",
            )
            .unwrap();
            assert!(store.resume_pending_custody().is_err());
            assert!(root.join("control/control.pending-intent.json").exists());
            assert_eq!(store.checkpoint().unwrap().sequence, 1);
        } else {
            store.resume_pending_custody().unwrap();
            assert_eq!(store.checkpoint().unwrap().sequence, prior.sequence + 1);
            assert!(store.verify_custody().is_ok());
            assert!(!root.join("control/control.pending-intent.json").exists());
            assert!(store.verify_backup_artifact(SNAPSHOT, &hash).unwrap());
        }
    }
}

#[test]
fn kill_nine_with_custody_temporary_files_retries_exact_cas_or_closes() {
    use std::os::unix::process::ExitStatusExt;
    for (stage, should_resume) in [
        ("mirror_linked", true),
        ("seal_temp", true),
        ("head_temp", true),
        ("wrong_seal_temp", false),
    ] {
        let directory = tempfile::tempdir().unwrap();
        let root = directory.path().canonicalize().unwrap();
        let (client, genesis, successor, reservation, record_bytes, seal_bytes) =
            custody_cas_fixture(&root);
        let custody_root = root.join("custodian");
        let mirror = custody_root
            .join("state-v2.history")
            .join("00000000000000000001.json");
        if stage != "mirror_linked" {
            sync_synthetic_file(&mirror, &record_bytes);
        }
        if stage == "head_temp" {
            fs::remove_file(custody_root.join("state-v2.history-seal.json")).unwrap();
            sync_synthetic_file(
                &custody_root.join("state-v2.history-seal.json"),
                &seal_bytes,
            );
        }
        let payload = match stage {
            "mirror_linked" => record_bytes.clone(),
            "seal_temp" => seal_bytes.clone(),
            "head_temp" => serde_json::to_vec(&successor).unwrap(),
            "wrong_seal_temp" => b"{}".to_vec(),
            _ => unreachable!(),
        };
        fs::write(root.join("payload.bin"), payload).unwrap();

        let mut child = Command::new(std::env::current_exe().unwrap())
            .args(["--exact", "child_custody_temporary_publish", "--nocapture"])
            .env("BOAZ_SYNTHETIC_CUSTODY_TEMP_ROOT", &root)
            .env("BOAZ_SYNTHETIC_CUSTODY_TEMP_STAGE", stage)
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .unwrap();
        let marker = root.join("paused");
        let start = Instant::now();
        while !marker.exists() {
            assert!(
                start.elapsed() < Duration::from_secs(15),
                "{stage} timed out"
            );
            assert!(child.try_wait().unwrap().is_none(), "{stage} child exited");
            thread::sleep(Duration::from_millis(20));
        }
        child.kill().unwrap();
        let output = child.wait_with_output().unwrap();
        assert_eq!(output.status.signal(), Some(libc::SIGKILL), "{stage}");
        let result = client.compare_and_swap_v2(&reservation, &successor);
        if should_resume {
            assert_eq!(result.unwrap(), successor, "{stage}");
            assert_eq!(
                client.read_v2(&genesis.control.store_id).unwrap(),
                successor
            );
            let leftovers = fs::read_dir(&custody_root)
                .unwrap()
                .filter_map(Result::ok)
                .any(|item| item.file_name().to_string_lossy().starts_with(".pending-"));
            assert!(!leftovers, "{stage} left a root temporary");
        } else {
            assert!(result.is_err(), "{stage} must fail closed");
            assert!(client.read_v2(&genesis.control.store_id).is_err());
            assert_eq!(
                serde_json::from_slice::<CustodyState>(
                    &fs::read(custody_root.join("state-v2.head.json")).unwrap()
                )
                .unwrap(),
                genesis
            );
        }
    }
}
