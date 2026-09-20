use boaz_health_receiver::{
    RuntimeGuard, ServerState,
    activation::{self, CoordinatorGuard, RecoveryJournal},
    adoption,
    control::{ControlCheckpoint, ControlStore},
    create_pairing_code,
    custody::{AckCheckpoint, CustodyClient, CustodyState, SshCustody},
    database::{
        HealthLayout, StoragePaths, classify_health_database, initialize_health_database,
        migrate_health_database, verify_health_database,
    },
    lifecycle_lock, open_db, operation_lock,
    projection::{self, VmConfig, run_worker},
    recovery::{self, StagedControl},
    revoke_device, router,
};
use chrono::{DateTime, Utc};
use rusqlite::OptionalExtension;
use sha2::{Digest, Sha256};
use std::{
    collections::{BTreeMap, BTreeSet},
    env,
    fs::{File, Metadata},
    future::IntoFuture,
    io::Read,
    net::{IpAddr, Ipv4Addr, SocketAddr},
    path::{Component, Path, PathBuf},
    sync::Arc,
    time::Duration,
};
use tokio::sync::Notify;

fn env_path(name: &str, default: &str) -> PathBuf {
    env::var_os(name)
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from(default))
}

fn configured_custody() -> Result<Arc<dyn CustodyClient>, Box<dyn std::error::Error>> {
    let target = env::var("BOAZ_HEALTH_CUSTODY_SSH_TARGET")
        .map_err(|_| "BOAZ_HEALTH_CUSTODY_SSH_TARGET is required for independent custody")?;
    let known_hosts = PathBuf::from(
        env::var_os("BOAZ_HEALTH_CUSTODY_KNOWN_HOSTS")
            .ok_or("BOAZ_HEALTH_CUSTODY_KNOWN_HOSTS is required")?,
    );
    let identity = PathBuf::from(
        env::var_os("BOAZ_HEALTH_CUSTODY_IDENTITY_FILE")
            .ok_or("BOAZ_HEALTH_CUSTODY_IDENTITY_FILE is required")?,
    );
    Ok(Arc::new(SshCustody::new(target, known_hosts, identity)?))
}

fn attested(name: &str) -> bool {
    env::var(name).ok().as_deref() == Some("1")
}

fn coordinator_lock_for_command(
    root: &Path,
    command: &str,
) -> Result<CoordinatorGuard, Box<dyn std::error::Error>> {
    // No command that can open storage may silently fall back to fixed legacy
    // paths. Only the explicit bootstrap command creates this directory.
    let exclusive = command != "verify-storage";
    let guard = activation::lock_coordinator(root, exclusive, true)?;
    let journal_present = match std::fs::symlink_metadata(root.join("current-restore.json")) {
        Ok(_) => true,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => false,
        Err(error) => return Err(error.into()),
    };
    let active_present = match std::fs::symlink_metadata(root.join("active-set.json")) {
        Ok(_) => true,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => false,
        Err(error) => return Err(error.into()),
    };
    let adoption_present = match std::fs::symlink_metadata(root.join("adopted-unactivated.json")) {
        Ok(_) => true,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => false,
        Err(error) => return Err(error.into()),
    };
    if command == "verify-storage" && !active_present && !journal_present {
        return Ok(guard);
    }
    if matches!(command, "init-storage" | "migrate-storage")
        && attested("BOAZ_HEALTH_BOOTSTRAP")
        && !active_present
        && !journal_present
        && !adoption_present
    {
        return Ok(guard);
    }
    if matches!(command, "init-storage" | "migrate-storage") {
        return Err("Storage initialization or migration is permitted only before adoption under explicit offline bootstrap".into());
    }
    if command == "init-ack-journal"
        && attested("BOAZ_HEALTH_BOOTSTRAP")
        && !active_present
        && !journal_present
        && !adoption_present
    {
        return Ok(guard);
    }
    if command == "init-ack-journal" {
        return Err("Ack journal initialization is permitted only before adoption under explicit offline bootstrap".into());
    }
    if command == "adopt-active-set"
        && attested("BOAZ_HEALTH_BOOTSTRAP")
        && !active_present
        && !journal_present
    {
        if adoption_present {
            let _ = activation::read_adopted_unactivated(&guard)?;
        }
        return Ok(guard);
    }
    if matches!(command, "restore-control" | "restore-health") {
        if journal_present {
            let _ = RecoveryJournal::load(&guard)?;
        }
        return Ok(guard);
    }
    if !active_present || !journal_present {
        return Err(
            "Adopted recovery coordinator is incomplete; refusing to open writable storage".into(),
        );
    }
    let _ = activation::read_active_candidate(&guard)?;
    let journal = RecoveryJournal::load(&guard)?;
    if journal.current.phase != activation::RecoveryPhase::ActiveVerified {
        return Err("Recovery remains in progress; receiver and writers must stay stopped".into());
    }
    // A completed local journal and pointer are still insufficient to serve:
    // the off-host control head and the activated native VM's complete export
    // must be rechecked on every launch. Until that launch path is wired to a
    // real custody provider, refusing all operational commands is deliberate.
    Err("Active generation has no off-host custody and native VM launch readback proof; refusing to serve or mutate".into())
}

fn storage_paths() -> Result<StoragePaths, Box<dyn std::error::Error>> {
    let default_db = PathBuf::from("/opt/boaz-health/data/health.db");
    let health_db = env_path("BOAZ_HEALTH_DB", default_db.to_str().unwrap());
    let data_root = env::var_os("BOAZ_HEALTH_DATA_ROOT")
        .map(PathBuf::from)
        .or_else(|| health_db.parent().map(PathBuf::from))
        .ok_or("Health database path has no parent")?;
    Ok(StoragePaths::new(
        data_root,
        health_db,
        env_path(
            "BOAZ_HEALTH_CONTROL_DB",
            "/opt/boaz-health/control/control.db",
        ),
        env_path(
            "BOAZ_HEALTH_CONTROL_MIRROR_DIR",
            "/opt/boaz-health/control/mirror",
        ),
        env_path("BOAZ_HEALTH_BACKUP_DIR", "/opt/boaz-health/backups"),
    ))
}

fn control_store(paths: &StoragePaths) -> Result<ControlStore, Box<dyn std::error::Error>> {
    let store = ControlStore::new(paths.control_db.clone(), paths.control_mirror_dir.clone())?;
    store.verify()?;
    Ok(store)
}

fn verify_storage_read_only(
    paths: &StoragePaths,
) -> Result<ControlStore, Box<dyn std::error::Error>> {
    paths.validate_no_write()?;
    let store = control_store(paths)?;
    let store_id = store.store_id()?;
    verify_health_database(&paths.health_db, Some(&store_id))?;
    Ok(store)
}

fn governed_control_store(
    paths: &StoragePaths,
    guard: &CoordinatorGuard,
    custody: Arc<dyn CustodyClient>,
) -> Result<ControlStore, Box<dyn std::error::Error>> {
    paths.validate_no_write()?;
    let journal_root = PathBuf::from(
        env::var_os("BOAZ_HEALTH_ACK_JOURNAL_DIR")
            .ok_or("BOAZ_HEALTH_ACK_JOURNAL_DIR is required for governed control writes")?,
    );
    require_recovery_storage(&journal_root, paths)?;
    let journal = Arc::new(boaz_health_receiver::ack_journal::AckJournal::open(
        &journal_root,
    )?);
    let store = ControlStore::new(paths.control_db.clone(), paths.control_mirror_dir.clone())?
        .with_custody(custody, activation::custody_operation_lock_path(guard)?)?
        .with_ack_journal(journal)?
        .with_managed_backup_root(paths.backup_dir.clone())?;
    // A prior crash may have committed SQLite before publishing its mirror,
    // local head, or independent CAS. Resume only its frozen intent first;
    // ordinary full verification before this step could never repair it.
    store.resume_pending_custody()?;
    store.verify()?;
    verify_health_database(&paths.health_db, Some(&store.store_id()?))?;
    Ok(store)
}

/// Only the offline generation-zero command may reach this path. A crash can
/// leave an already verified local baseline ahead of the independent head,
/// or leave its exact reservation pending. Operational full-tuple verification
/// must still reject that condition; this path verifies the entire local
/// control chain and refuses any unresolved control intent before retrying the
/// one deterministic baseline transition below.
fn adoption_retry_control_store(
    paths: &StoragePaths,
    guard: &CoordinatorGuard,
    custody: Arc<dyn CustodyClient>,
    journal: Arc<boaz_health_receiver::ack_journal::AckJournal>,
) -> Result<ControlStore, Box<dyn std::error::Error>> {
    paths.validate_no_write()?;
    let store = ControlStore::new(paths.control_db.clone(), paths.control_mirror_dir.clone())?
        .with_custody(custody, activation::custody_operation_lock_path(guard)?)?
        .with_ack_journal(journal)?
        .with_managed_backup_root(paths.backup_dir.clone())?;
    store.verify_tail()?;
    store.verify()?;
    verify_health_database(&paths.health_db, Some(&store.store_id()?))?;
    Ok(store)
}

/// The expected predecessor is fixed by the one-event genesis backup. Calling
/// reserve with its stable operation ID and byte digest works both before and
/// after a crash in reserve/CAS, including when read_v2 intentionally refuses
/// to expose a pending reservation. Any other remote state is rejected by the
/// custodian; only an authenticated exact successor readback is confirmation.
fn confirm_adoption_baseline(
    custody: &dyn CustodyClient,
    control: &ControlCheckpoint,
    snapshot_id: &str,
    baseline_sha: &str,
    ack_head: &AckCheckpoint,
) -> Result<CustodyState, Box<dyn std::error::Error>> {
    if control.sequence != 1
        || ack_head.sequence != 0
        || ack_head.confirmation_sequence != 0
        || ack_head.pairing_confirmation_sequence != 0
    {
        return Err("Adoption baseline requires one genesis backup and an empty journal".into());
    }
    let predecessor = CustodyState {
        format: 2,
        revision: 1,
        control: control.clone(),
        ack: None,
        baseline_sha256: None,
    };
    let mut successor = predecessor.clone();
    successor.revision = 2;
    successor.ack = Some(ack_head.clone());
    successor.baseline_sha256 = Some(baseline_sha.to_owned());
    match custody.read_v2(&control.store_id) {
        Ok(remote) if remote == successor => return Ok(remote),
        Ok(remote) if remote != predecessor => {
            return Err("Independent custody baseline state diverged".into());
        }
        // A pending exact reservation makes read_v2 refuse service. Only the
        // deterministic reserve below may distinguish that from disconnect or
        // a conflicting reservation; both other cases remain fail-closed.
        _ => {}
    }
    let operation_id = format!("baseline-{snapshot_id}");
    let reservation = custody.reserve_v2(&predecessor, &operation_id, baseline_sha)?;
    match custody.compare_and_swap_v2(&reservation, &successor) {
        Ok(state) if state == successor => {}
        Ok(_) => return Err("Custody baseline CAS response conflicts with successor".into()),
        Err(_) => {
            // The CAS reply may have been lost after a durable commit. Do not
            // claim success yet: the authenticated read below is mandatory.
        }
    }
    let confirmed = custody.read_v2(&control.store_id)?;
    if confirmed != successor {
        return Err("Independent custody did not read back the exact baseline successor".into());
    }
    Ok(confirmed)
}

fn open_operational_storage(
    paths: &StoragePaths,
) -> Result<(ControlStore, rusqlite::Connection), Box<dyn std::error::Error>> {
    let store = verify_storage_read_only(paths)?;
    let store_id = store.store_id()?;
    let mut health = open_db(&paths.health_db)?;
    store.reconcile_health(&mut health)?;
    verify_health_database(&paths.health_db, Some(&store_id))?;
    Ok((store, health))
}

fn init_storage(paths: &StoragePaths) -> Result<(), Box<dyn std::error::Error>> {
    paths.prepare_empty_layout()?;
    let store =
        ControlStore::initialize(paths.control_db.clone(), paths.control_mirror_dir.clone())?;
    let store_id = store.store_id()?;
    if let Err(error) = initialize_health_database(&paths.health_db, &store_id) {
        let _ = std::fs::remove_file(&paths.control_db);
        let _ = std::fs::remove_file(paths.control_db.with_extension("db-wal"));
        let _ = std::fs::remove_file(paths.control_db.with_extension("db-shm"));
        if let Some(parent) = paths.control_db.parent() {
            let _ = std::fs::remove_file(parent.join("control.head.json"));
        }
        return Err(error.into());
    }
    verify_health_database(&paths.health_db, Some(&store_id))?;
    println!("storage_initialized=true");
    Ok(())
}

fn migrate_storage(paths: &StoragePaths) -> Result<(), Box<dyn std::error::Error>> {
    if attested("BOAZ_HEALTH_UPLOAD_ENABLED") {
        return Err("migrate-storage requires upload to be disabled".into());
    }
    // Classification and control-layout checks are deliberately read-only.
    // An incompatible health database must not cause even a lock or directory
    // to be created alongside it.
    let layout = classify_health_database(&paths.health_db)?;
    paths.validate_control_layout_for_migration_no_write()?;
    // Reject an incompatible pair or an unrelated legacy seed before even
    // creating the legacy lifecycle lock. The same checks are repeated under
    // the lock below to close the classification-to-mutation interval.
    if paths.control_db.exists() {
        let candidate =
            ControlStore::new(paths.control_db.clone(), paths.control_mirror_dir.clone())?;
        let linked_store_id = candidate.preflight_migration()?;
        if layout == HealthLayout::CurrentV2 {
            verify_health_database(&paths.health_db, Some(&linked_store_id))?;
        } else {
            let legacy = rusqlite::Connection::open_with_flags(
                &paths.health_db,
                rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY,
            )?;
            candidate.preflight_legacy_seed_prefix(&legacy)?;
        }
    }
    let _lifecycle_guard = lifecycle_lock(&paths.health_db, true, true)?;
    if layout == HealthLayout::CurrentV2 {
        // A valid v1 control ledger cannot pass the v2 operational verifier.
        // Migrate it explicitly before that verifier, using the migration's
        // read-only v1 schema/chain preflight and transactional copy checks.
        let store = ControlStore::new(paths.control_db.clone(), paths.control_mirror_dir.clone())?;
        // Reject a mismatched health/control pair before the control schema
        // migration is allowed to touch either on-disk authority.
        let linked_store_id = store.preflight_migration()?;
        verify_health_database(&paths.health_db, Some(&linked_store_id))?;
        store.migrate_v1_to_v2()?;
        let store = verify_storage_read_only(paths)?;
        let health = rusqlite::Connection::open_with_flags(
            &paths.health_db,
            rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY,
        )?;
        let incomplete_seed = store.has_incomplete_legacy_seed(&health)?;
        let source_safety_facts: i64 = health.query_row(
            "SELECT (SELECT count(*) FROM devices WHERE revoked_at IS NOT NULL)
                  + (SELECT count(*) FROM erasures)",
            [],
            |row| row.get(0),
        )?;
        // A fresh v2 ledger with no historical safety facts legitimately has
        // an empty control chain. A partial legacy seed, or any missing
        // revoked/erased fact, must still fail closed.
        if incomplete_seed && (store.checkpoint()?.sequence != 0 || source_safety_facts != 0) {
            return Err(
                "current health database has an incomplete legacy control import; recovery is required"
                    .into(),
            );
        }
        drop(health);
        // Control v1 is classified and verified read-only before this explicit,
        // forward-only migration. Old event hashes remain byte-for-byte valid.
        let _ = open_operational_storage(paths)?;
        println!("storage_migrated=false storage_already_current=true");
        return Ok(());
    }
    if paths.control_db.exists() {
        let existing =
            ControlStore::new(paths.control_db.clone(), paths.control_mirror_dir.clone())?;
        let _ = existing.preflight_migration()?;
        let legacy = rusqlite::Connection::open_with_flags(
            &paths.health_db,
            rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY,
        )?;
        existing.preflight_legacy_seed_prefix(&legacy)?;
        drop(legacy);
        existing.migrate_v1_to_v2()?;
    }
    let _operation_guard = operation_lock(&paths.health_db, false)?;
    paths.prepare_control_layout_for_migration()?;
    let created_control = !paths.control_db.exists();
    let store = if !created_control {
        control_store(paths)?
    } else {
        ControlStore::initialize(paths.control_db.clone(), paths.control_mirror_dir.clone())?
    };
    let store_id = store.store_id()?;
    let migration = (|| -> Result<(), Box<dyn std::error::Error>> {
        // Import the append-only safety facts before binding/mutating the
        // health ledger. A crash can therefore never produce a v2 health DB
        // whose legacy tombstones were not already durably published.
        let legacy = rusqlite::Connection::open_with_flags(
            &paths.health_db,
            rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY,
        )?;
        store.seed_legacy_for_migration(&legacy)?;
        drop(legacy);
        migrate_health_database(&paths.health_db, &store_id)?;
        Ok(())
    })();
    if let Err(error) = migration {
        if created_control {
            // The SQLite schema transaction may have committed before a
            // post-commit permission/readback error. Never discard the
            // control authority unless the health layout is still legacy.
            if matches!(
                classify_health_database(&paths.health_db),
                Ok(HealthLayout::LegacyV0 | HealthLayout::ClaimedV1)
            ) {
                cleanup_new_control_store(paths)?;
            }
        }
        return Err(error);
    }
    let mut health = open_db(&paths.health_db)?;
    store.reconcile_health(&mut health)?;
    verify_health_database(&paths.health_db, Some(&store_id))?;
    println!("storage_migrated=true");
    Ok(())
}

fn cleanup_new_control_store(paths: &StoragePaths) -> Result<(), Box<dyn std::error::Error>> {
    for path in [
        paths.control_db.clone(),
        paths.control_db.with_extension("db-wal"),
        paths.control_db.with_extension("db-shm"),
    ] {
        match std::fs::remove_file(path) {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(error.into()),
        }
    }
    if let Some(parent) = paths.control_db.parent() {
        for name in ["control.head.json", "control.publish.lock"] {
            match std::fs::remove_file(parent.join(name)) {
                Ok(()) => {}
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                Err(error) => return Err(error.into()),
            }
        }
    }
    if paths.control_mirror_dir.is_dir() {
        for entry in std::fs::read_dir(&paths.control_mirror_dir)? {
            let entry = entry?;
            if !entry.file_type()?.is_file() {
                return Err("New control mirror contains an unexpected non-file artifact".into());
            }
            std::fs::remove_file(entry.path())?;
        }
    }
    Ok(())
}

fn file_sha256(path: &Path) -> Result<String, Box<dyn std::error::Error>> {
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

fn sync_file(path: &Path) -> Result<(), Box<dyn std::error::Error>> {
    File::open(path)?.sync_all()?;
    Ok(())
}

fn sync_directory(path: &Path) -> Result<(), Box<dyn std::error::Error>> {
    File::open(path)?.sync_all()?;
    Ok(())
}

#[derive(Debug)]
struct ManagedBackup {
    database_path: PathBuf,
    snapshot_id: String,
    file_sha256: String,
    snapshot_started_at: DateTime<Utc>,
}

fn require_private_regular_file(
    path: &Path,
    metadata: &Metadata,
) -> Result<(), Box<dyn std::error::Error>> {
    if !metadata.file_type().is_file() {
        return Err(format!(
            "Managed backup artifact is not a regular file: {}",
            path.display()
        )
        .into());
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        if metadata.nlink() != 1 {
            return Err(
                format!("Managed backup artifact has hard links: {}", path.display()).into(),
            );
        }
    }
    Ok(())
}

fn is_managed_database_name(name: &str) -> bool {
    name.starts_with("boaz-health-") && name.ends_with(".db")
}

fn is_managed_manifest_name(name: &str) -> bool {
    name.starts_with("boaz-health-") && name.ends_with(".db.meta.json")
}

fn canonical_sha256(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

/// Reconciles all three authorities before pruning or confirming expiry:
/// control events, manifests, and physical SQLite snapshot files.
fn validate_managed_backup_inventory(
    directory: &Path,
    control: &ControlStore,
) -> Result<Vec<ManagedBackup>, Box<dyn std::error::Error>> {
    let mut databases = BTreeMap::new();
    let mut manifests = BTreeMap::new();
    for entry in std::fs::read_dir(directory)? {
        let entry = entry?;
        let name = entry
            .file_name()
            .into_string()
            .map_err(|_| "Managed backup directory contains a non-UTF-8 name")?;
        if is_managed_database_name(&name) {
            let metadata = std::fs::symlink_metadata(entry.path())?;
            require_private_regular_file(&entry.path(), &metadata)?;
            if databases.insert(name, entry.path()).is_some() {
                return Err("Managed backup inventory contains a duplicate database".into());
            }
        } else if is_managed_manifest_name(&name) {
            let metadata = std::fs::symlink_metadata(entry.path())?;
            require_private_regular_file(&entry.path(), &metadata)?;
            let database_name = name
                .strip_suffix(".meta.json")
                .ok_or("Managed backup manifest filename is invalid")?
                .to_owned();
            if manifests.insert(database_name, entry.path()).is_some() {
                return Err("Managed backup inventory contains a duplicate manifest".into());
            }
        } else if name.starts_with("boaz-health-") || name.starts_with(".boaz-health-backup-") {
            return Err(format!("Unexpected managed backup artifact: {name}").into());
        }
    }
    if databases.keys().collect::<BTreeSet<_>>() != manifests.keys().collect::<BTreeSet<_>>() {
        return Err(
            "Managed backup inventory requires exactly one database and one manifest per snapshot"
                .into(),
        );
    }

    let active = control.active_backup_inventory()?;
    let mut active_by_snapshot = BTreeMap::new();
    for event in active {
        if active_by_snapshot
            .insert(event.snapshot_id.clone(), event)
            .is_some()
        {
            return Err("Control store contains duplicate active backup events".into());
        }
    }

    let mut seen_snapshots = BTreeSet::new();
    let mut verified = Vec::new();
    for (name, database_path) in databases {
        let manifest_path = manifests
            .remove(&name)
            .ok_or("Managed backup manifest is missing")?;
        let manifest: serde_json::Value = serde_json::from_slice(&std::fs::read(&manifest_path)?)?;
        let snapshot_id = manifest
            .get("snapshot_id")
            .and_then(serde_json::Value::as_str)
            .filter(|value| !value.is_empty())
            .ok_or("Backup manifest lacks snapshot ID")?
            .to_owned();
        if !seen_snapshots.insert(snapshot_id.clone()) {
            return Err("Managed backup manifests contain a duplicate snapshot ID".into());
        }
        let expected_hash = manifest
            .get("file_sha256")
            .and_then(serde_json::Value::as_str)
            .filter(|value| canonical_sha256(value))
            .ok_or("Backup manifest lacks a canonical file hash")?;
        let source_schema_version = manifest
            .get("source_schema_version")
            .and_then(serde_json::Value::as_i64)
            .ok_or("Backup manifest lacks a health schema version")?;
        if source_schema_version != boaz_health_receiver::database::HEALTH_SCHEMA_VERSION {
            return Err("Managed backup was created from an unsupported health schema".into());
        }
        if file_sha256(&database_path)? != expected_hash {
            return Err("Managed backup hash does not match its manifest".into());
        }
        let checkpoint: ControlCheckpoint = serde_json::from_value(
            manifest
                .get("control_checkpoint")
                .cloned()
                .ok_or("Backup manifest lacks control checkpoint")?,
        )?;
        let event = active_by_snapshot
            .remove(&snapshot_id)
            .ok_or("Managed backup has no active backup_created control event")?;
        if event.file_sha256 != expected_hash {
            return Err("Managed backup hash does not match its control event".into());
        }
        if event.prior_checkpoint != checkpoint {
            return Err(
                "Managed backup manifest is bound to a different control checkpoint".into(),
            );
        }
        let started_text = manifest
            .get("snapshot_started_at")
            .and_then(serde_json::Value::as_str)
            .ok_or("Backup manifest lacks snapshot time")?;
        let snapshot_started_at = DateTime::parse_from_rfc3339(started_text)?.with_timezone(&Utc);
        verified.push(ManagedBackup {
            database_path,
            snapshot_id,
            file_sha256: expected_hash.to_owned(),
            snapshot_started_at,
        });
    }
    if !active_by_snapshot.is_empty() {
        return Err("Active backup_created control event has no manifest and database".into());
    }
    Ok(verified)
}

#[cfg(unix)]
struct PinnedBackupDirectory {
    path: PathBuf,
    file: File,
    device: u64,
    inode: u64,
}

#[cfg(unix)]
impl PinnedBackupDirectory {
    fn open(path: &Path) -> Result<Self, Box<dyn std::error::Error>> {
        use std::os::unix::fs::{MetadataExt, OpenOptionsExt};
        let file = std::fs::OpenOptions::new()
            .read(true)
            .custom_flags(libc::O_DIRECTORY | libc::O_NOFOLLOW | libc::O_CLOEXEC)
            .open(path)?;
        let metadata = file.metadata()?;
        if !metadata.is_dir()
            || metadata.uid() != unsafe { libc::geteuid() }
            || metadata.mode() & 0o022 != 0
        {
            return Err("Managed backup directory has unsafe ownership or permissions".into());
        }
        let pinned = Self {
            path: path.to_path_buf(),
            file,
            device: metadata.dev(),
            inode: metadata.ino(),
        };
        pinned.verify_path()?;
        Ok(pinned)
    }

    fn verify_path(&self) -> Result<(), Box<dyn std::error::Error>> {
        use std::os::unix::fs::MetadataExt;
        let current = std::fs::symlink_metadata(&self.path)?;
        if !current.is_dir()
            || current.file_type().is_symlink()
            || current.dev() != self.device
            || current.ino() != self.inode
        {
            return Err("Managed backup directory was replaced".into());
        }
        Ok(())
    }

    fn open_entry(&self, name: &str) -> Result<Option<File>, Box<dyn std::error::Error>> {
        use std::{
            ffi::CString,
            os::{
                fd::{AsRawFd, FromRawFd},
                unix::fs::MetadataExt,
            },
        };
        if name.is_empty() || name.contains('/') || name == "." || name == ".." {
            return Err("Backup entry is not a direct child".into());
        }
        self.verify_path()?;
        let name = CString::new(name)?;
        let descriptor = unsafe {
            libc::openat(
                self.file.as_raw_fd(),
                name.as_ptr(),
                libc::O_RDONLY | libc::O_NOFOLLOW | libc::O_CLOEXEC,
            )
        };
        if descriptor < 0 {
            let error = std::io::Error::last_os_error();
            if error.kind() == std::io::ErrorKind::NotFound {
                return Ok(None);
            }
            return Err(error.into());
        }
        let file = unsafe { File::from_raw_fd(descriptor) };
        let metadata = file.metadata()?;
        if !metadata.is_file()
            || metadata.nlink() != 1
            || metadata.uid() != unsafe { libc::geteuid() }
            || metadata.mode() & 0o077 != 0
        {
            return Err("Managed backup entry has unsafe type, links, owner or permissions".into());
        }
        self.assert_entry_identity(&name, &file)?;
        Ok(Some(file))
    }

    fn assert_entry_identity(
        &self,
        name: &std::ffi::CString,
        file: &File,
    ) -> Result<(), Box<dyn std::error::Error>> {
        use std::os::{fd::AsRawFd, unix::fs::MetadataExt};
        self.verify_path()?;
        let opened = file.metadata()?;
        let mut current = std::mem::MaybeUninit::<libc::stat>::uninit();
        let result = unsafe {
            libc::fstatat(
                self.file.as_raw_fd(),
                name.as_ptr(),
                current.as_mut_ptr(),
                libc::AT_SYMLINK_NOFOLLOW,
            )
        };
        if result < 0 {
            return Err(std::io::Error::last_os_error().into());
        }
        let current = unsafe { current.assume_init() };
        if current.st_dev as u64 != opened.dev()
            || current.st_ino != opened.ino()
            || opened.nlink() != 1
            || current.st_mode & libc::S_IFMT != libc::S_IFREG
        {
            return Err("Managed backup entry changed after verification".into());
        }
        Ok(())
    }

    fn unlink_verified(&self, name: &str, file: &File) -> Result<(), Box<dyn std::error::Error>> {
        use std::{
            ffi::CString,
            os::{fd::AsRawFd, unix::fs::MetadataExt},
        };
        let name = CString::new(name)?;
        self.assert_entry_identity(&name, file)?;
        let result = unsafe { libc::unlinkat(self.file.as_raw_fd(), name.as_ptr(), 0) };
        if result < 0 {
            return Err(std::io::Error::last_os_error().into());
        }
        self.file.sync_all()?;
        self.verify_path()?;
        if file.metadata()?.nlink() != 0 || self.open_entry(name.to_str()?)?.is_some() {
            return Err("Managed backup unlink did not remove the pinned file".into());
        }
        Ok(())
    }
}

#[cfg(unix)]
fn hash_open_backup(file: &mut File) -> Result<String, Box<dyn std::error::Error>> {
    use std::os::unix::fs::MetadataExt;
    let before = file.metadata()?;
    let mut hash = Sha256::new();
    let mut buffer = [0u8; 65536];
    loop {
        let count = file.read(&mut buffer)?;
        if count == 0 {
            break;
        }
        hash.update(&buffer[..count]);
    }
    let after = file.metadata()?;
    if before.dev() != after.dev()
        || before.ino() != after.ino()
        || before.len() != after.len()
        || before.mtime() != after.mtime()
        || before.mtime_nsec() != after.mtime_nsec()
    {
        return Err("Managed backup entry changed while hashing".into());
    }
    Ok(hex::encode(hash.finalize()))
}

#[cfg(unix)]
fn verify_delete_manifest(
    file: &mut File,
    snapshot_id: &str,
    file_sha256: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut bytes = Vec::new();
    file.take(65537).read_to_end(&mut bytes)?;
    if bytes.len() > 65536 {
        return Err("Pending backup deletion manifest exceeds 64 KiB".into());
    }
    let manifest: serde_json::Value = serde_json::from_slice(&bytes)?;
    if manifest
        .get("snapshot_id")
        .and_then(serde_json::Value::as_str)
        != Some(snapshot_id)
        || manifest
            .get("file_sha256")
            .and_then(serde_json::Value::as_str)
            != Some(file_sha256)
    {
        return Err("Pending backup deletion manifest differs from control intent".into());
    }
    Ok(())
}

/// An intent is durable before either file can be removed. Only this exact
/// two-file deletion order is resumable: complete pair, manifest-only, absent.
/// A database without its manifest or any unexpected alias requires review.
fn reconcile_pending_backup_deletes(
    directory: &Path,
    control: &ControlStore,
    verified_at: DateTime<Utc>,
) -> Result<(), Box<dyn std::error::Error>> {
    reconcile_pending_backup_deletes_with_hook(directory, control, verified_at, |_| {})
}

#[cfg(unix)]
fn reconcile_pending_backup_deletes_with_hook(
    directory: &Path,
    control: &ControlStore,
    verified_at: DateTime<Utc>,
    mut before_database_delete: impl FnMut(&Path),
) -> Result<(), Box<dyn std::error::Error>> {
    let pinned = PinnedBackupDirectory::open(directory)?;
    for intent in control.pending_backup_delete_intents()? {
        if !is_managed_database_name(&intent.artifact_name)
            || Path::new(&intent.artifact_name)
                .file_name()
                .and_then(|v| v.to_str())
                != Some(intent.artifact_name.as_str())
        {
            return Err("Pending backup deletion has an invalid artifact name".into());
        }
        let database_path = directory.join(&intent.artifact_name);
        let manifest_name = format!("{}.meta.json", intent.artifact_name);
        let mut database = pinned.open_entry(&intent.artifact_name)?;
        let mut manifest = pinned.open_entry(&manifest_name)?;
        if database.is_some() && manifest.is_none() {
            return Err("Pending backup deletion has a database without its manifest".into());
        }
        if let Some(file) = database.as_mut()
            && hash_open_backup(file)? != intent.file_sha256
        {
            return Err("Pending backup deletion database hash differs from control intent".into());
        }
        if let Some(file) = manifest.as_mut() {
            verify_delete_manifest(file, &intent.snapshot_id, &intent.file_sha256)?;
        }
        if let Some(file) = database.as_ref() {
            before_database_delete(&database_path);
            pinned.unlink_verified(&intent.artifact_name, file)?;
        }
        if let Some(file) = manifest.as_ref() {
            pinned.unlink_verified(&manifest_name, file)?;
        }
        // Do not claim deletion if another managed filename advertises the
        // same snapshot. Different legitimate snapshots can have equal bytes.
        for entry in std::fs::read_dir(directory)? {
            let entry = entry?;
            let name = entry
                .file_name()
                .into_string()
                .map_err(|_| "Non-UTF-8 backup artifact")?;
            if is_managed_manifest_name(&name) {
                let file = pinned
                    .open_entry(&name)?
                    .ok_or("Managed backup manifest vanished during deletion audit")?;
                let mut bytes = Vec::new();
                file.take(65537).read_to_end(&mut bytes)?;
                if bytes.len() > 65536 {
                    return Err("Managed backup manifest exceeds 64 KiB".into());
                }
                let value: serde_json::Value = serde_json::from_slice(&bytes)?;
                if value.get("snapshot_id").and_then(serde_json::Value::as_str)
                    == Some(intent.snapshot_id.as_str())
                {
                    return Err("Duplicate snapshot manifest remains after deletion".into());
                }
            }
        }
        pinned.verify_path()?;
        control.append_backup_deleted_with_hash(
            &intent.snapshot_id,
            &intent.file_sha256,
            &intent.artifact_name,
            &verified_at.to_rfc3339(),
        )?;
    }
    Ok(())
}

#[cfg(not(unix))]
fn reconcile_pending_backup_deletes_with_hook(
    _directory: &Path,
    _control: &ControlStore,
    _verified_at: DateTime<Utc>,
    _before_database_delete: impl FnMut(&Path),
) -> Result<(), Box<dyn std::error::Error>> {
    Err("Verified managed backup deletion requires Unix directory handles".into())
}

#[cfg(all(test, unix))]
#[allow(dead_code)]
pub(crate) fn reconcile_pending_backup_deletes_synthetic_test(
    directory: &Path,
    control: &ControlStore,
    before_database_delete: impl FnMut(&Path),
) -> Result<(), Box<dyn std::error::Error>> {
    reconcile_pending_backup_deletes_with_hook(
        directory,
        control,
        Utc::now(),
        before_database_delete,
    )
}

#[cfg(test)]
fn backup(
    paths: &StoragePaths,
    destination: &Path,
    control: &ControlStore,
) -> Result<(), Box<dyn std::error::Error>> {
    let _operation_guard = operation_lock(&paths.health_db, false)?;
    backup_locked(paths, destination, control, None)
}

fn backup_locked(
    paths: &StoragePaths,
    destination: &Path,
    control: &ControlStore,
    snapshot_time: Option<DateTime<Utc>>,
) -> Result<(), Box<dyn std::error::Error>> {
    paths.validate_backup_destination(destination)?;
    if destination.exists() {
        return Err("Backup destination already exists".into());
    }
    let filename = destination
        .file_name()
        .and_then(|name| name.to_str())
        .ok_or("Invalid backup filename")?;
    if !filename.starts_with("boaz-health-") || !filename.ends_with(".db") {
        return Err("Backup filename must match boaz-health-*.db".into());
    }
    let parent = destination.parent().ok_or("Invalid backup destination")?;
    validate_managed_backup_inventory(parent, control)?;
    let manifest = destination.with_extension("db.meta.json");
    if manifest.exists() {
        return Err("Backup manifest already exists".into());
    }
    let temp = parent.join(format!(".boaz-health-backup-{}.tmp", uuid::Uuid::new_v4()));
    let temp_manifest = parent.join(format!(
        ".boaz-health-backup-{}.meta.tmp",
        uuid::Uuid::new_v4()
    ));
    let result = (|| -> Result<(), Box<dyn std::error::Error>> {
        let mut source = open_db(&paths.health_db)?;
        // The control intent can be durable even when a crash interrupted
        // health erasure. Never timestamp a snapshot after that intent while
        // still copying the old device's rows.
        control.reconcile_health(&mut source)?;
        let snapshot_started_at = snapshot_time.unwrap_or_else(Utc::now).to_rfc3339();
        let snapshot_id = uuid::Uuid::new_v4().to_string();
        let control_checkpoint = control.checkpoint()?;
        let mut target = rusqlite::Connection::open(&temp)?;
        {
            let snapshot = rusqlite::backup::Backup::new(&source, &mut target)?;
            snapshot.run_to_completion(100, Duration::from_millis(100), None)?;
        }
        let integrity: String = target.query_row("PRAGMA integrity_check", [], |row| row.get(0))?;
        if integrity != "ok" {
            return Err("Backup integrity check failed".into());
        }
        // Bind the watermark to the completed snapshot, not a separate read
        // of the source before SQLite's backup transaction. The first empty
        // generation has no sqlite_sequence row; encode its proven zero
        // receipts as Some(0), which recovery requires for replay planning.
        let source_commit_sequence = snapshot_receipt_commit_sequence(&target)?;
        drop(target);
        let file_sha256 = file_sha256(&temp)?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(&temp, std::fs::Permissions::from_mode(0o600))?;
        }
        sync_file(&temp)?;
        std::fs::write(
            &temp_manifest,
            serde_json::to_vec(&serde_json::json!({
                "snapshot_id":snapshot_id,
                "snapshot_started_at":snapshot_started_at,
                "source_commit_sequence":source_commit_sequence,
                "source_schema_version":boaz_health_receiver::database::HEALTH_SCHEMA_VERSION,
                "file_sha256":file_sha256,
                "control_checkpoint":control_checkpoint
            }))?,
        )?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(&temp_manifest, std::fs::Permissions::from_mode(0o600))?;
        }
        sync_file(&temp_manifest)?;
        std::fs::rename(&temp, destination)?;
        if let Err(error) = std::fs::rename(&temp_manifest, &manifest) {
            let _ = std::fs::remove_file(destination);
            let _ = sync_directory(parent);
            return Err(error.into());
        }
        // Publish the names durably before claiming the snapshot in the
        // append-only control chain.
        if let Err(error) = sync_directory(parent) {
            let _ = std::fs::remove_file(destination);
            let _ = std::fs::remove_file(&manifest);
            let _ = sync_directory(parent);
            return Err(error);
        }
        if let Err(error) = control.append_backup_created_with_artifact(
            &snapshot_id,
            &file_sha256,
            filename,
            &snapshot_started_at,
            &control_checkpoint,
        ) {
            // The independent custodian may already hold a durable
            // reservation even when the local control event is not yet
            // published. Preserve both artifacts for exact-intent recovery;
            // deleting them here would make the reservation impossible to
            // complete safely.
            return Err(error.into());
        }
        Ok(())
    })();
    if result.is_err() {
        let _ = std::fs::remove_file(&temp);
        let _ = std::fs::remove_file(&temp_manifest);
    }
    result?;
    println!("Backup written to {}", destination.display());
    Ok(())
}

fn snapshot_receipt_commit_sequence(
    snapshot: &rusqlite::Connection,
) -> Result<i64, Box<dyn std::error::Error>> {
    let sequence: Option<i64> = snapshot
        .query_row(
            "SELECT seq FROM sqlite_sequence WHERE name='receipts'",
            [],
            |row| row.get(0),
        )
        .optional()?;
    match sequence {
        Some(value) if value >= 0 => Ok(value),
        Some(_) => Err("Snapshot receipt high-water mark is negative".into()),
        None => {
            let count: i64 =
                snapshot.query_row("SELECT count(*) FROM receipts", [], |row| row.get(0))?;
            if count != 0 {
                return Err("Snapshot contains receipts without a durable high-water mark".into());
            }
            Ok(0)
        }
    }
}

fn adopt_active_set(
    guard: &CoordinatorGuard,
    paths: &StoragePaths,
    destination: &Path,
) -> Result<(), Box<dyn std::error::Error>> {
    // This command seals a genesis baseline, not an active generation. A
    // candidate still needs the separate P0-R2.2 VM rebuild and cutover.
    require_upload_off_for_restore()?;
    let directory = destination
        .parent()
        .ok_or("Adoption backup has no parent")?;
    require_managed_backup_storage(paths, directory)?;
    if !projection::encrypted_mount_verified(&guard.root().to_path_buf()) {
        return Err("Coordinator must be on a verified encrypted control volume".into());
    }
    let journal_root = PathBuf::from(
        env::var_os("BOAZ_HEALTH_ACK_JOURNAL_DIR")
            .ok_or("BOAZ_HEALTH_ACK_JOURNAL_DIR is required for adoption")?,
    );
    require_recovery_storage(&journal_root, paths)?;
    let custody = configured_custody()?;
    let _ = activation::provision_custody_operation_lock_before_adoption(guard)?;
    let _lifecycle_guard = lifecycle_lock(&paths.health_db, true, true)?;
    let _operation_guard = operation_lock(&paths.health_db, false)?;
    let journal = Arc::new(boaz_health_receiver::ack_journal::AckJournal::open(
        &journal_root,
    )?);
    let baseline_was_bound = journal.baseline()?.is_some();
    let control = if baseline_was_bound {
        // The local baseline can legitimately precede reserve/CAS. The
        // ordinary governed constructor rejects that mismatch, so this one
        // offline retry checks local integrity and the absent control intent;
        // the exact remote predecessor is enforced by reserve_v2 below.
        adoption_retry_control_store(paths, guard, Arc::clone(&custody), Arc::clone(&journal))?
    } else {
        governed_control_store(paths, guard, Arc::clone(&custody))?
    };
    let initial = control.checkpoint()?;
    if !baseline_was_bound {
        let off_host = custody.read_v2(&initial.store_id)?;
        if off_host.control != initial {
            return Err(
                "Independent custody control head differs from the frozen local chain".into(),
            );
        }
        if initial.sequence == 0 && (off_host.ack.is_some() || off_host.baseline_sha256.is_some()) {
            return Err("Independent custody already contains an adoption baseline".into());
        }
    }
    if initial.sequence == 0 {
        if baseline_was_bound {
            return Err("Bound baseline has no genesis backup event".into());
        }
        if destination.exists() {
            return Err("Unclaimed adoption backup exists; preserve it and reconcile the original control intent".into());
        }
        backup_locked(paths, destination, &control, None)?;
    } else if initial.sequence != 1 || !destination.exists() {
        return Err(
            "Only a fresh genesis store or its exact one-event adoption retry is supported".into(),
        );
    }
    let baseline =
        adoption::verify_generation_zero_snapshot(guard, paths, destination, &control, &journal)?;
    let _anchor = journal.bind_baseline(&baseline)?;
    let baseline_sha = file_sha256(&journal_root.join("baseline.json"))?;
    let ack_head = journal.checkpoint()?;
    let confirmed = confirm_adoption_baseline(
        custody.as_ref(),
        &control.checkpoint()?,
        &baseline.snapshot_id,
        &baseline_sha,
        &ack_head,
    )?;
    let record = activation::AdoptionRecord {
        format_version: 1,
        snapshot_id: baseline.snapshot_id,
        snapshot_sha256: baseline.snapshot_sha256,
        receipt_inventory_sha256: baseline.receipt_inventory_sha256,
        control_checkpoint: control.checkpoint()?,
        baseline_sha256: baseline_sha,
        custody_revision: confirmed.revision,
    };
    activation::publish_adopted_unactivated(guard, &record)?;
    println!(
        "adopted_unactivated=true snapshot_id={}",
        record.snapshot_id
    );
    Ok(())
}

#[cfg(test)]
fn prune_backups(
    paths: &StoragePaths,
    directory: &Path,
    control: &ControlStore,
) -> Result<(), Box<dyn std::error::Error>> {
    let _operation_guard = operation_lock(&paths.health_db, false)?;
    prune_backups_locked(paths, directory, control, Utc::now())
}

fn prune_backups_locked(
    paths: &StoragePaths,
    directory: &Path,
    control: &ControlStore,
    now: DateTime<Utc>,
) -> Result<(), Box<dyn std::error::Error>> {
    paths.validate_backup_directory(directory)?;
    let retention = chrono::Duration::days(29);
    reconcile_pending_backup_deletes(directory, control, now)?;
    let inventory = validate_managed_backup_inventory(directory, control)?;
    let mut deleted = 0;
    for backup in inventory {
        if now.signed_duration_since(backup.snapshot_started_at) >= retention {
            let artifact_name = backup
                .database_path
                .file_name()
                .and_then(|value| value.to_str())
                .ok_or("Managed backup has no UTF-8 artifact name")?;
            control.append_backup_delete_intent(
                &backup.snapshot_id,
                &backup.file_sha256,
                artifact_name,
                &now.to_rfc3339(),
            )?;
            reconcile_pending_backup_deletes(directory, control, now)?;
            deleted += 1;
        }
    }
    // Reconcile again after deletion events. No erasure may be confirmed from
    // an inventory that is only partially reflected in the control ledger.
    let retained: Vec<DateTime<Utc>> = validate_managed_backup_inventory(directory, control)?
        .into_iter()
        .map(|backup| backup.snapshot_started_at)
        .collect();
    let mut connection = open_db(&paths.health_db)?;
    let pending = {
        let mut statement = connection.prepare("SELECT device_id,erasure_id,requested_at,backup_delete_by FROM erasures WHERE backups_expired_at IS NULL")?;
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
    let mut confirmed = Vec::new();
    let mut overdue = 0;
    for (device_id, erasure_id, requested_at, deadline) in pending {
        let requested: DateTime<Utc> =
            DateTime::parse_from_rfc3339(&requested_at)?.with_timezone(&Utc);
        let old_copy_exists = retained.iter().any(|started| *started <= requested);
        if !old_copy_exists {
            let verified_at = now.to_rfc3339();
            control.append_backups_expired_verified(&device_id, &erasure_id, &verified_at)?;
            confirmed.push((device_id, verified_at));
        } else if now > DateTime::parse_from_rfc3339(&deadline)?.with_timezone(&Utc) {
            overdue += 1;
        }
    }
    let transaction = connection.transaction()?;
    for (device_id, verified_at) in confirmed {
        transaction.execute(
            "UPDATE erasures SET backups_expired_at=?2 WHERE device_id=?1",
            rusqlite::params![device_id, verified_at],
        )?;
    }
    transaction.commit()?;
    println!("managed_backups_deleted={deleted} overdue_erasures={overdue}");
    if overdue > 0 {
        return Err("Backup expiry deadline missed".into());
    }
    Ok(())
}

/// A configuration flag is an operator request, never evidence of encryption.
/// Both managed-backup commands must pass this gate before opening or changing
/// any health/control ledger state. On non-Linux systems the physical dm-crypt
/// check fails closed, while the core logic remains testable with synthetic data.
fn require_managed_backup_storage(
    paths: &StoragePaths,
    directory: &Path,
) -> Result<(), Box<dyn std::error::Error>> {
    paths.validate_backup_directory(directory)?;
    if !attested("BOAZ_HEALTH_BACKUP_VOLUME_ENCRYPTED") {
        return Err("Encrypted backup volume must be verified first".into());
    }
    if !projection::encrypted_mount_verified(&directory.to_path_buf()) {
        return Err("Managed backup storage must be physically verified dm-crypt".into());
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        let backup_device = std::fs::metadata(directory)?.dev();
        let control_directory = paths
            .control_db
            .parent()
            .ok_or("Control database path has no parent")?;
        if backup_device == std::fs::metadata(&paths.data_root)?.dev()
            || backup_device == std::fs::metadata(control_directory)?.dev()
        {
            return Err("Managed backups require an independent encrypted device".into());
        }
    }
    Ok(())
}

// Integration tests compile this module into the test binary and call the
// deterministic backup/prune core with synthetic SQLite fixtures. The actual
// CLI always goes through the physical storage gate above; no bypass is
// compiled into the production binary.
#[cfg(test)]
#[allow(dead_code)]
pub(crate) fn backup_synthetic_test(
    paths: &StoragePaths,
    destination: &Path,
    control: &ControlStore,
) -> Result<(), Box<dyn std::error::Error>> {
    backup(paths, destination, control)
}

#[cfg(test)]
#[allow(dead_code)]
pub(crate) fn backup_synthetic_test_at(
    paths: &StoragePaths,
    destination: &Path,
    control: &ControlStore,
    snapshot_time: DateTime<Utc>,
) -> Result<(), Box<dyn std::error::Error>> {
    let _operation_guard = operation_lock(&paths.health_db, false)?;
    backup_locked(paths, destination, control, Some(snapshot_time))
}

#[cfg(test)]
#[allow(dead_code)]
pub(crate) fn prune_backups_synthetic_test(
    paths: &StoragePaths,
    directory: &Path,
    control: &ControlStore,
) -> Result<(), Box<dyn std::error::Error>> {
    prune_backups(paths, directory, control)
}

#[cfg(test)]
#[allow(dead_code)]
pub(crate) fn prune_backups_synthetic_test_at(
    paths: &StoragePaths,
    directory: &Path,
    control: &ControlStore,
    now: DateTime<Utc>,
) -> Result<(), Box<dyn std::error::Error>> {
    let _operation_guard = operation_lock(&paths.health_db, false)?;
    prune_backups_locked(paths, directory, control, now)
}

fn require_recovery_storage(
    path: &Path,
    paths: &StoragePaths,
) -> Result<(), Box<dyn std::error::Error>> {
    let directory = if path.is_dir() {
        path.to_path_buf()
    } else {
        path.parent()
            .ok_or("Recovery path has no parent directory")?
            .to_path_buf()
    };
    if !projection::encrypted_mount_verified(&directory) {
        return Err("Recovery storage must be verified dm-crypt, not a configuration flag".into());
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        let recovery_device = std::fs::metadata(&directory)?.dev();
        for live in [
            &paths.data_root,
            paths
                .control_db
                .parent()
                .ok_or("Control path has no parent")?,
        ] {
            if std::fs::metadata(live)?.dev() == recovery_device {
                return Err("Recovery storage must use an independent encrypted device".into());
            }
        }
    }
    Ok(())
}

fn require_upload_off_for_restore() -> Result<(), Box<dyn std::error::Error>> {
    if attested("BOAZ_HEALTH_UPLOAD_ENABLED") {
        return Err("Restore requires BOAZ_HEALTH_UPLOAD_ENABLED=0".into());
    }
    Ok(())
}

fn restore_options(
    arguments: &[String],
    expected_head: bool,
) -> Result<(PathBuf, Option<PathBuf>), Box<dyn std::error::Error>> {
    let mut staging = None;
    let mut head = None;
    let mut index = 0;
    while index < arguments.len() {
        let value = arguments
            .get(index + 1)
            .ok_or("Restore option needs a path")?;
        match arguments[index].as_str() {
            "--staging-path" if staging.is_none() => staging = Some(PathBuf::from(value)),
            "--expected-head" if expected_head && head.is_none() => {
                head = Some(PathBuf::from(value));
            }
            _ => return Err("Unknown or repeated restore option".into()),
        }
        index += 2;
    }
    let staging = staging.ok_or("Restore requires --staging-path ROOT")?;
    if expected_head && head.is_none() {
        return Err("Restore requires --expected-head FILE from independent custody".into());
    }
    Ok((staging, head))
}

fn recovery_live_paths(paths: &StoragePaths) -> Vec<PathBuf> {
    vec![
        paths.data_root.clone(),
        paths.control_db.parent().unwrap().to_path_buf(),
        paths.backup_dir.clone(),
        env_path(
            "BOAZ_HEALTH_VM_STORAGE",
            "/opt/boaz-health/victoria-metrics-data",
        ),
        PathBuf::from("/opt/boaz/data"),
    ]
}

fn read_external_expected_head(
    path: &Path,
    bundle: &Path,
) -> Result<ControlCheckpoint, Box<dyn std::error::Error>> {
    if !path.is_absolute()
        || path
            .components()
            .any(|component| matches!(component, Component::ParentDir | Component::CurDir))
    {
        return Err("Expected head needs an absolute path without dot components".into());
    }
    let metadata = std::fs::symlink_metadata(path)?;
    if !metadata.file_type().is_file() || metadata.len() > 4096 {
        return Err("Expected head must be a small, regular independent file".into());
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        if metadata.nlink() != 1 {
            return Err("Expected head may not be hard-linked to another artifact".into());
        }
    }
    if path.canonicalize()?.starts_with(bundle.canonicalize()?) {
        return Err("Expected head cannot be read from the backup bundle".into());
    }
    Ok(serde_json::from_slice(&std::fs::read(path)?)?)
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    #[cfg(unix)]
    unsafe {
        libc::umask(0o077);
    }
    let command = env::args().nth(1).unwrap_or_else(|| "serve".to_owned());
    if command == "custody-protocol" {
        // This entry point is installed only on the independently administered
        // custody host as an authorized_keys forced command. It must not open
        // the receiver's local health or control storage.
        let root = PathBuf::from(
            env::var_os("BOAZ_HEALTH_CUSTODY_ROOT")
                .ok_or("BOAZ_HEALTH_CUSTODY_ROOT is required on the custody host")?,
        );
        boaz_health_receiver::custody::run_forced_command(&root)?;
        return Ok(());
    }
    let coordinator_root = env_path("BOAZ_HEALTH_COORD_DIR", "/opt/boaz-health/control/coord");
    if command == "bootstrap-coordinator" {
        require_upload_off_for_restore()?;
        if !attested("BOAZ_HEALTH_BOOTSTRAP") {
            return Err("Explicit BOAZ_HEALTH_BOOTSTRAP=1 is required".into());
        }
        activation::initialize_coordinator(&coordinator_root)?;
        println!("coordinator_initialized=true adopted=false");
        return Ok(());
    }
    let coordinator_guard = coordinator_lock_for_command(&coordinator_root, &command)?;
    let paths = storage_paths()?;
    paths.validate_no_write()?;
    if !matches!(
        command.as_str(),
        "init-storage"
            | "migrate-storage"
            | "init-ack-journal"
            | "verify-storage"
            | "restore-control"
            | "restore-health"
            | "adopt-active-set"
    ) {
        let active = activation::read_active_candidate(&coordinator_guard)?;
        let configured_vm = env_path(
            "BOAZ_HEALTH_VM_STORAGE",
            "/opt/boaz-health/victoria-metrics-data",
        );
        if active.health.path != paths.health_db
            || active.control.path != paths.control_db
            || active.vm_storage.path != configured_vm
        {
            return Err(
                "Configured health, control, or VM path differs from the adopted active generation"
                    .into(),
            );
        }
    }
    let db_path = paths.health_db.clone();
    match command.as_str() {
        "init-ack-journal" => {
            require_upload_off_for_restore()?;
            let root = PathBuf::from(env::var_os("BOAZ_HEALTH_ACK_JOURNAL_DIR")
                .ok_or("BOAZ_HEALTH_ACK_JOURNAL_DIR is required")?);
            require_recovery_storage(&root, &paths)?;
            boaz_health_receiver::ack_journal::AckJournal::initialize(&root)?;
            println!("ack_journal_initialized=true confirmed_batches=0");
            return Ok(());
        }
        "init-storage" => {
            init_storage(&paths)?;
            return Ok(());
        }
        "migrate-storage" => {
            migrate_storage(&paths)?;
            return Ok(());
        }
        "verify-storage" => {
            let _ = verify_storage_read_only(&paths)?;
            println!("storage_verified=true");
            return Ok(());
        }
        "adopt-active-set" => {
            let destination = PathBuf::from(env::args().nth(2).ok_or(
                "Usage: boaz-health-receiver adopt-active-set MANAGED_BACKUP_PATH",
            )?);
            if env::args().count() != 3 {
                return Err("adopt-active-set accepts exactly one managed backup path".into());
            }
            adopt_active_set(&coordinator_guard, &paths, &destination)?;
            return Ok(());
        }
        "pair-code" => {
            let _lifecycle_guard = lifecycle_lock(&db_path, false, true)?;
            let _operation_guard = operation_lock(&db_path, false)?;
            let _ = governed_control_store(
                &paths,
                &coordinator_guard,
                configured_custody()?,
            )?;
            let connection = open_db(&db_path)?;
            let code = create_pairing_code(&connection)?;
            println!("{code}");
            return Ok(());
        }
        "backup" => {
            let destination = env::args().nth(2).ok_or("Usage: boaz-health-receiver backup /encrypted/path/boaz-health-YYYYMMDD.db")?;
            let destination = PathBuf::from(destination);
            let directory = destination.parent().ok_or("Backup destination has no parent")?;
            require_managed_backup_storage(&paths, directory)?;
            let _lifecycle_guard = lifecycle_lock(&db_path, false, true)?;
            let _operation_guard = operation_lock(&db_path, false)?;
            let store = governed_control_store(
                &paths,
                &coordinator_guard,
                configured_custody()?,
            )?;
            backup_locked(&paths, &destination, &store, None)?;
            return Ok(());
        }
        "backup-control" => {
            let destination = PathBuf::from(env::args().nth(2).ok_or(
                "Usage: boaz-health-receiver backup-control NEW_ENCRYPTED_DIRECTORY",
            )?);
            if env::args().count() != 3 {
                return Err("backup-control accepts exactly one destination".into());
            }
            require_recovery_storage(&destination, &paths)?;
            let _lifecycle_guard = lifecycle_lock(&db_path, false, true)?;
            let _operation_guard = operation_lock(&db_path, false)?;
            let store = governed_control_store(
                &paths,
                &coordinator_guard,
                configured_custody()?,
            )?;
            let manifest = recovery::backup_control(&store, &destination)?;
            println!(
                "control_backup_created=true snapshot_id={} head_sequence={}",
                manifest.snapshot_id, manifest.checkpoint.sequence
            );
            return Ok(());
        }
        "restore-control" => {
            require_upload_off_for_restore()?;
            let arguments: Vec<String> = env::args().skip(2).collect();
            let source = PathBuf::from(arguments.first().ok_or(
                "Usage: boaz-health-receiver restore-control SOURCE --staging-path ROOT --expected-head FILE",
            )?);
            let (staging, expected_file) = restore_options(&arguments[1..], true)?;
            let expected_file = expected_file.ok_or("External expected head is required")?;
            require_recovery_storage(&source, &paths)?;
            require_recovery_storage(&staging, &paths)?;
            // Disaster recovery must still be able to stage a known-good
            // bundle when the live health/control database is corrupt. Path
            // validation and the offline lifecycle lock remain mandatory.
            let _lifecycle_guard = lifecycle_lock(&db_path, true, true)?;
            let expected = read_external_expected_head(&expected_file, &source)?;
            let staged = recovery::restore_control_to_staging(
                &source,
                &staging,
                &expected,
                &recovery_live_paths(&paths),
            )?;
            println!(
                "control_staged=true head_sequence={} path={}",
                expected.sequence,
                staged.staging_root.display()
            );
            return Ok(());
        }
        "restore-health" => {
            require_upload_off_for_restore()?;
            let arguments: Vec<String> = env::args().skip(2).collect();
            let source = PathBuf::from(arguments.first().ok_or(
                "Usage: boaz-health-receiver restore-health BACKUP --staging-path ROOT",
            )?);
            let (staging, _) = restore_options(&arguments[1..], false)?;
            require_recovery_storage(&source, &paths)?;
            require_recovery_storage(&staging, &paths)?;
            let journal_root = PathBuf::from(
                env::var_os("BOAZ_HEALTH_ACK_JOURNAL_DIR")
                    .ok_or("BOAZ_HEALTH_ACK_JOURNAL_DIR is required for recovery")?,
            );
            require_recovery_storage(&journal_root, &paths)?;
            let ack_journal = boaz_health_receiver::ack_journal::AckJournal::open(&journal_root)?;
            if ack_journal.baseline()?.is_none() {
                return Err("Recovery journal has no independently verified adoption baseline".into());
            }
            let _lifecycle_guard = lifecycle_lock(&db_path, true, true)?;
            let staged = StagedControl {
                staging_root: staging.clone(),
                store: ControlStore::new(
                    staging.join("control/control.db"),
                    staging.join("control/mirror"),
                )?,
            };
            let state = recovery::restore_health_to_staging_with_ack(
                &source,
                &source.with_extension("db.meta.json"),
                &staged,
                &recovery_live_paths(&paths),
                &ack_journal,
            )?;
            println!("{}", serde_json::to_string(&state)?);
            return Err(
                "Staged health replay is not a completed restore: isolated VictoriaMetrics rebuild/readback and operator cutover remain required"
                    .into(),
            );
        }
        "revoke-device" => {
            let device_id = env::args().nth(2).ok_or("Usage: boaz-health-receiver revoke-device DEVICE_ID")?;
            let _lifecycle_guard = lifecycle_lock(&db_path, false, true)?;
            let _operation_guard = operation_lock(&db_path, false)?;
            let store = governed_control_store(
                &paths,
                &coordinator_guard,
                configured_custody()?,
            )?;
            let mut connection = open_db(&db_path)?;
            let token_hash: Option<String> = connection.query_row(
                "SELECT token_hash FROM devices WHERE device_id=?1",
                [&device_id],
                |row| row.get(0),
            ).optional()?;
            if let Some(token_hash) = token_hash
                && !store.token_tombstoned(&token_hash)?
            {
                store.append_credential_revoked(&device_id, &token_hash, &Utc::now().to_rfc3339())?;
            }
            println!("revoked={}", revoke_device(&mut connection, &device_id)?);
            return Ok(());
        }
        "prune-backups" => {
            let directory = env::args().nth(2).ok_or("Usage: boaz-health-receiver prune-backups ENCRYPTED_BACKUP_DIRECTORY")?;
            let directory = PathBuf::from(directory);
            require_managed_backup_storage(&paths, &directory)?;
            let _lifecycle_guard = lifecycle_lock(&db_path, false, true)?;
            let _operation_guard = operation_lock(&db_path, false)?;
            let store = governed_control_store(
                &paths,
                &coordinator_guard,
                configured_custody()?,
            )?;
            prune_backups_locked(&paths, &directory, &store, Utc::now())?;
            return Ok(());
        }
        "serve" => {},
        _ => return Err("Usage: boaz-health-receiver [bootstrap-coordinator|init-ack-journal|init-storage|migrate-storage|verify-storage|adopt-active-set MANAGED_BACKUP_PATH|serve|pair-code|backup PATH|backup-control DIRECTORY|restore-control SOURCE --staging-path ROOT --expected-head FILE|restore-health BACKUP --staging-path ROOT|prune-backups DIRECTORY|revoke-device DEVICE_ID]".into()),
    }
    let _lifecycle_guard = lifecycle_lock(&db_path, false, true)?;
    let _operation_guard = operation_lock(&db_path, false)?;
    let custody = configured_custody()?;
    let custody_lock_path = activation::custody_operation_lock_path(&coordinator_guard)?;
    let control_store = governed_control_store(&paths, &coordinator_guard, Arc::clone(&custody))?;
    let mut health = open_db(&db_path)?;
    control_store.reconcile_health(&mut health)?;
    drop(_operation_guard);
    let vm_config = VmConfig {
        binary: env_path(
            "BOAZ_HEALTH_VM_BINARY",
            "/opt/boaz-health/bin/victoria-metrics-prod",
        ),
        storage: env_path(
            "BOAZ_HEALTH_VM_STORAGE",
            "/opt/boaz-health/victoria-metrics-data",
        ),
    };
    let guard = RuntimeGuard {
        vm: vm_config.clone(),
        data_volume: db_path
            .parent()
            .ok_or("Invalid database directory")?
            .to_path_buf(),
        control_volume: paths
            .control_db
            .parent()
            .ok_or("Invalid control database directory")?
            .to_path_buf(),
        backup_volume: env_path("BOAZ_HEALTH_BACKUP_DIR", "/opt/boaz-health/backups"),
    };
    let requested_upload = attested("BOAZ_HEALTH_UPLOAD_ENABLED");
    let ack_journal = match env::var_os("BOAZ_HEALTH_ACK_JOURNAL_DIR") {
        Some(value) => {
            let root = PathBuf::from(value);
            require_recovery_storage(&root, &paths)?;
            Some(Arc::new(
                boaz_health_receiver::ack_journal::AckJournal::open(&root)?,
            ))
        }
        None => None,
    };
    let verified = attested("BOAZ_HEALTH_DATA_VOLUME_ENCRYPTED")
        && attested("BOAZ_HEALTH_CONTROL_VOLUME_ENCRYPTED")
        && attested("BOAZ_HEALTH_BACKUP_RESTORE_VERIFIED")
        && attested("BOAZ_HEALTH_TAILSCALE_PRIVATE_VERIFIED")
        && guard.verified();
    let upload_enabled = requested_upload && verified;
    if requested_upload && !verified {
        eprintln!(
            "Upload gate remains closed: separate encrypted health/control storage, backup restore, Tailscale privacy, or native VictoriaMetrics identity is unverified"
        );
    }
    let state = ServerState {
        db_path,
        control_store: Some(control_store),
        ack_journal,
        custody: Some(custody),
        custody_lock_path: Some(custody_lock_path),
        projection_notify: Arc::new(Notify::new()),
        upload_enabled,
        runtime_guard: Some(guard),
    };
    let mut worker = tokio::spawn(run_worker(state.clone(), vm_config));
    let port: u16 = env::var("BOAZ_HEALTH_PORT")
        .ok()
        .as_deref()
        .unwrap_or("8787")
        .parse()?;
    let address = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), port);
    let listener = tokio::net::TcpListener::bind(address).await?;
    println!("Boaz Health receiver listening on {address}; upload_enabled={upload_enabled}");
    let server = axum::serve(listener, router(state)).into_future();
    tokio::pin!(server);
    tokio::select! {
        result = &mut server => {
            worker.abort();
            result?;
        }
        result = &mut worker => {
            return Err(format!("projection worker terminated unexpectedly: {result:?}").into());
        }
    }
    Ok(())
}

#[cfg(test)]
mod coordinator_gate_tests {
    use super::*;
    use boaz_health_receiver::custody::{
        AckCheckpoint, CustodyError, CustodyReservationV2, CustodyResult, CustodyState,
    };
    use std::sync::Mutex;

    struct PendingBaselineCustody {
        state: Mutex<CustodyState>,
        pending: Mutex<bool>,
        wrong_readback: bool,
    }

    impl CustodyClient for PendingBaselineCustody {
        fn read_v2(&self, store_id: &str) -> CustodyResult<CustodyState> {
            let state = self.state.lock().unwrap();
            if state.control.store_id != store_id || *self.pending.lock().unwrap() {
                return Err(CustodyError::Protocol("reservation unresolved".into()));
            }
            let mut result = state.clone();
            if self.wrong_readback && result.baseline_sha256.is_some() {
                result.baseline_sha256 = Some("f".repeat(64));
            }
            Ok(result)
        }

        fn reserve_v2(
            &self,
            predecessor: &CustodyState,
            operation_id: &str,
            intent_sha256: &str,
        ) -> CustodyResult<CustodyReservationV2> {
            if *self.state.lock().unwrap() != *predecessor
                || operation_id != "baseline-synthetic-snapshot"
                || intent_sha256 != "a".repeat(64)
            {
                return Err(CustodyError::Protocol("stale or divergent baseline".into()));
            }
            *self.pending.lock().unwrap() = true;
            Ok(CustodyReservationV2 {
                reservation_id: "12345678-1234-1234-1234-123456789abc".into(),
                predecessor: predecessor.clone(),
                operation_id: operation_id.into(),
                intent_sha256: intent_sha256.into(),
            })
        }

        fn compare_and_swap_v2(
            &self,
            reservation: &CustodyReservationV2,
            successor: &CustodyState,
        ) -> CustodyResult<CustodyState> {
            let mut state = self.state.lock().unwrap();
            let mut pending = self.pending.lock().unwrap();
            if !*pending || *state != reservation.predecessor {
                return Err(CustodyError::Protocol("reservation changed".into()));
            }
            *state = successor.clone();
            *pending = false;
            Ok(successor.clone())
        }
    }

    fn synthetic_baseline_fixture(
        wrong_readback: bool,
    ) -> (PendingBaselineCustody, ControlCheckpoint, AckCheckpoint) {
        let control = ControlCheckpoint {
            store_id: "synthetic-store".into(),
            sequence: 1,
            current_hash: "b".repeat(64),
        };
        let ack = AckCheckpoint {
            journal_id: "synthetic-journal".into(),
            sequence: 0,
            confirmation_sequence: 0,
            pairing_confirmation_sequence: 0,
            head_sha256: "c".repeat(64),
        };
        let custody = PendingBaselineCustody {
            state: Mutex::new(CustodyState {
                format: 2,
                revision: 1,
                control: control.clone(),
                ack: None,
                baseline_sha256: None,
            }),
            pending: Mutex::new(true),
            wrong_readback,
        };
        (custody, control, ack)
    }

    #[test]
    fn empty_snapshot_receipt_inventory_has_explicit_zero_watermark() {
        let connection = rusqlite::Connection::open_in_memory().unwrap();
        connection
            .execute_batch("CREATE TABLE receipts(id INTEGER PRIMARY KEY AUTOINCREMENT)")
            .unwrap();
        assert_eq!(snapshot_receipt_commit_sequence(&connection).unwrap(), 0);
        connection
            .execute("INSERT INTO receipts DEFAULT VALUES", [])
            .unwrap();
        connection.execute("DELETE FROM receipts", []).unwrap();
        assert_eq!(snapshot_receipt_commit_sequence(&connection).unwrap(), 1);
    }

    #[test]
    fn populated_snapshot_without_receipt_sequence_is_not_inferred_as_zero() {
        let connection = rusqlite::Connection::open_in_memory().unwrap();
        connection
            .execute_batch(
                "CREATE TABLE other(id INTEGER PRIMARY KEY AUTOINCREMENT);
                 CREATE TABLE receipts(id INTEGER PRIMARY KEY);
                 INSERT INTO receipts VALUES (1)",
            )
            .unwrap();
        assert!(snapshot_receipt_commit_sequence(&connection).is_err());
    }

    #[test]
    fn pending_exact_baseline_retries_without_preliminary_read() {
        let (custody, control, ack) = synthetic_baseline_fixture(false);
        assert!(custody.read_v2(&control.store_id).is_err());
        let confirmed = confirm_adoption_baseline(
            &custody,
            &control,
            "synthetic-snapshot",
            &"a".repeat(64),
            &ack,
        )
        .unwrap();
        assert_eq!(confirmed.revision, 2);
        assert_eq!(confirmed.ack, Some(ack));
        assert_eq!(
            confirmed.baseline_sha256.as_deref(),
            Some("a".repeat(64).as_str())
        );
    }

    #[test]
    fn bound_baseline_without_reservation_can_start_the_same_transition() {
        let (custody, control, ack) = synthetic_baseline_fixture(false);
        *custody.pending.lock().unwrap() = false;
        let confirmed = confirm_adoption_baseline(
            &custody,
            &control,
            "synthetic-snapshot",
            &"a".repeat(64),
            &ack,
        )
        .unwrap();
        assert_eq!(confirmed.revision, 2);
        assert_eq!(confirmed.control, control);
    }

    #[test]
    fn committed_baseline_with_lost_local_completion_marker_is_idempotent() {
        let (custody, control, ack) = synthetic_baseline_fixture(false);
        *custody.pending.lock().unwrap() = false;
        {
            let mut remote = custody.state.lock().unwrap();
            remote.revision = 2;
            remote.ack = Some(ack.clone());
            remote.baseline_sha256 = Some("a".repeat(64));
        }
        let confirmed = confirm_adoption_baseline(
            &custody,
            &control,
            "synthetic-snapshot",
            &"a".repeat(64),
            &ack,
        )
        .unwrap();
        assert_eq!(confirmed.revision, 2);
        assert_eq!(confirmed.ack, Some(ack));
    }

    #[test]
    fn divergent_remote_control_head_cannot_be_adopted() {
        let (custody, control, ack) = synthetic_baseline_fixture(false);
        custody.state.lock().unwrap().control.current_hash = "d".repeat(64);
        assert!(
            confirm_adoption_baseline(
                &custody,
                &control,
                "synthetic-snapshot",
                &"a".repeat(64),
                &ack,
            )
            .is_err()
        );
    }

    #[test]
    fn baseline_cas_response_without_exact_readback_is_not_confirmed() {
        let (custody, control, ack) = synthetic_baseline_fixture(true);
        assert!(
            confirm_adoption_baseline(
                &custody,
                &control,
                "synthetic-snapshot",
                &"a".repeat(64),
                &ack,
            )
            .is_err()
        );
    }

    #[test]
    fn unfinished_restore_blocks_legacy_serve_and_writes() {
        let directory = tempfile::tempdir_in(std::env::temp_dir().canonicalize().unwrap()).unwrap();
        let root = directory.path().join("coord");
        activation::initialize_coordinator(&root).unwrap();
        // A coordinator without an adopted generation must never serve.
        assert!(coordinator_lock_for_command(&root, "serve").is_err());
        std::fs::write(root.join("current-restore.json"), b"interrupted").unwrap();
        assert!(coordinator_lock_for_command(&root, "serve").is_err());
        assert!(coordinator_lock_for_command(&root, "pair-code").is_err());
        assert!(coordinator_lock_for_command(&root, "backup").is_err());
    }

    #[test]
    fn active_pointer_without_verified_cutover_blocks_legacy_serve() {
        let directory = tempfile::tempdir_in(std::env::temp_dir().canonicalize().unwrap()).unwrap();
        let root = directory.path().join("coord");
        activation::initialize_coordinator(&root).unwrap();
        std::fs::write(root.join("active-set.json"), b"not-verified").unwrap();
        assert!(coordinator_lock_for_command(&root, "serve").is_err());
    }

    #[test]
    fn coordinator_lock_is_shared_by_mutating_commands() {
        let directory = tempfile::tempdir_in(std::env::temp_dir().canonicalize().unwrap()).unwrap();
        let root = directory.path().join("coord");
        activation::initialize_coordinator(&root).unwrap();
        let _first = coordinator_lock_for_command(&root, "verify-storage").unwrap();
        assert!(coordinator_lock_for_command(&root, "revoke-device").is_err());
    }

    #[test]
    fn missing_coordinator_never_falls_back_to_legacy_paths() {
        let directory = tempfile::tempdir_in(std::env::temp_dir().canonicalize().unwrap()).unwrap();
        let root = directory.path().join("absent-coord");
        for command in [
            "serve",
            "pair-code",
            "backup",
            "revoke-device",
            "prune-backups",
        ] {
            assert!(
                coordinator_lock_for_command(&root, command).is_err(),
                "{command}"
            );
        }
    }
}
