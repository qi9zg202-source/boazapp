//! Read-only proof for the one admitted generation-zero health snapshot.
//!
//! Adoption is deliberately narrower than restoring a historical ledger. A
//! store with pre-adoption acknowledged writes is rejected unless an independent
//! source is designed and verified in a later protocol version.

use crate::{
    ack_journal::{AckJournal, Baseline},
    activation::CoordinatorGuard,
    control::ControlStore,
    database::{HEALTH_SCHEMA_VERSION, StoragePaths, verify_health_database},
    recovery::{HealthBackupManifest, receipt_inventory_sha256},
};
use rusqlite::{Connection, OpenFlags};
use sha2::{Digest, Sha256};
use std::{error::Error, fs, io::Read, path::Path};

pub type AdoptionResult<T> = Result<T, Box<dyn Error>>;

fn file_sha256(path: &Path) -> AdoptionResult<String> {
    let mut file = fs::File::open(path)?;
    let mut digest = Sha256::new();
    let mut chunk = [0u8; 64 * 1024];
    loop {
        let count = file.read(&mut chunk)?;
        if count == 0 {
            return Ok(hex::encode(digest.finalize()));
        }
        digest.update(&chunk[..count]);
    }
}

fn read_only(path: &Path) -> AdoptionResult<Connection> {
    Ok(Connection::open_with_flags(
        path,
        OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX,
    )?)
}

fn require_private_single_link_file(path: &Path) -> AdoptionResult<()> {
    let metadata = fs::symlink_metadata(path)?;
    if !metadata.file_type().is_file() {
        return Err("adoption artifact is not a regular file".into());
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::{MetadataExt, PermissionsExt};
        if metadata.nlink() != 1 || metadata.permissions().mode() & 0o077 != 0 {
            return Err("adoption artifact is linked or has broad permissions".into());
        }
    }
    Ok(())
}

fn count(connection: &Connection, table: &str) -> AdoptionResult<i64> {
    // The names are fixed constants below, never supplied by the caller.
    Ok(
        connection.query_row(&format!("SELECT count(*) FROM {table}"), [], |row| {
            row.get(0)
        })?,
    )
}

/// Must be called while the coordinator, lifecycle, and health operation locks
/// are held; it performs no mutation and never infers missing historical
/// receipts from sqlite_sequence or a supplied integer.
pub fn verify_generation_zero_snapshot(
    guard: &CoordinatorGuard,
    paths: &StoragePaths,
    backup_path: &Path,
    control: &ControlStore,
    journal: &AckJournal,
) -> AdoptionResult<Baseline> {
    if !guard.is_exclusive() {
        return Err("generation-zero verification requires exclusive coordinator lock".into());
    }
    paths.validate_no_write()?;
    paths.validate_backup_destination(backup_path)?;
    let manifest_path = backup_path.with_extension("db.meta.json");
    require_private_single_link_file(backup_path)?;
    require_private_single_link_file(&manifest_path)?;
    let manifest: HealthBackupManifest = serde_json::from_slice(&fs::read(&manifest_path)?)?;
    let store_id = control.store_id()?;
    if !journal.confirmed_batches()?.is_empty() || !journal.pairing_records()?.is_empty() {
        return Err("adoption journal is not empty or was already bound".into());
    }
    let checkpoint = control.checkpoint()?;
    if checkpoint.sequence != 1
        || manifest.control_checkpoint.sequence != 0
        || manifest.control_checkpoint.store_id != checkpoint.store_id
        || manifest.source_schema_version != HEALTH_SCHEMA_VERSION
        || !control.verify_backup_artifact(&manifest.snapshot_id, &manifest.file_sha256)?
    {
        return Err(
            "generation-zero backup lacks one verified, custody-bound creation event".into(),
        );
    }
    if file_sha256(backup_path)? != manifest.file_sha256 {
        return Err("generation-zero snapshot differs from its manifest".into());
    }
    verify_health_database(&paths.health_db, Some(&store_id))?;
    verify_health_database(backup_path, Some(&store_id))?;
    let source = read_only(&paths.health_db)?;
    let backup = read_only(backup_path)?;
    for database in [&source, &backup] {
        let integrity: String =
            database.query_row("PRAGMA integrity_check", [], |row| row.get(0))?;
        if integrity != "ok" {
            return Err("generation-zero SQLite integrity check failed".into());
        }
        // There is no independently attested pre-journal history in this
        // protocol. Refuse to turn an existing ledger into a false RPO=0 claim.
        for table in [
            "receipts",
            "events",
            "devices",
            "erasures",
            "pairing_codes",
            "audit",
            "outbox",
            // AUTOINCREMENT rows survive deletion of their business rows.
            // A prior receipt, audit, or projection task therefore cannot be
            // laundered into an apparently empty generation-zero ledger.
            "sqlite_sequence",
        ] {
            if count(database, table)? != 0 {
                return Err(
                    "pre-adoption health history has no independent acknowledgement source".into(),
                );
            }
        }
    }
    let source_inventory = receipt_inventory_sha256(&source)?;
    let backup_inventory = receipt_inventory_sha256(&backup)?;
    if source_inventory != backup_inventory {
        return Err("frozen source and managed snapshot receipt inventories differ".into());
    }
    let baseline = Baseline {
        snapshot_id: manifest.snapshot_id,
        snapshot_sha256: manifest.file_sha256,
        receipt_inventory_sha256: backup_inventory,
        control_store_id: checkpoint.store_id,
        control_head_sequence: checkpoint.sequence,
        control_head_hash: checkpoint.current_hash,
    };
    if let Some(existing) = journal.baseline()?
        && existing.baseline != baseline
    {
        return Err("existing adoption baseline differs from the verified snapshot".into());
    }
    Ok(baseline)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        activation::{initialize_coordinator, lock_coordinator},
        control::ControlStore,
        database::initialize_health_database,
    };
    use tempfile::TempDir;

    #[test]
    fn old_or_missing_snapshot_cannot_be_adopted() {
        let root = TempDir::new_in(std::env::temp_dir().canonicalize().unwrap()).unwrap();
        let coord = root.path().join("coord");
        initialize_coordinator(&coord).unwrap();
        let guard = lock_coordinator(&coord, true, true).unwrap();
        let paths = StoragePaths::new(
            root.path().join("data"),
            root.path().join("data/health.db"),
            root.path().join("control/control.db"),
            root.path().join("control/mirror"),
            root.path().join("backups"),
        );
        paths.prepare_empty_layout().unwrap();
        let control =
            ControlStore::initialize(paths.control_db.clone(), paths.control_mirror_dir.clone())
                .unwrap();
        initialize_health_database(&paths.health_db, &control.store_id().unwrap()).unwrap();
        let journal_root = root.path().join("journal");
        fs::create_dir(&journal_root).unwrap();
        AckJournal::initialize(&journal_root).unwrap();
        let journal = AckJournal::open(&journal_root).unwrap();
        assert!(
            verify_generation_zero_snapshot(
                &guard,
                &paths,
                &paths.backup_dir.join("missing.db"),
                &control,
                &journal,
            )
            .is_err()
        );

        let backup_path = paths.backup_dir.join("boaz-health-genesis.db");
        let source = Connection::open(&paths.health_db).unwrap();
        let mut target = Connection::open(&backup_path).unwrap();
        rusqlite::backup::Backup::new(&source, &mut target)
            .unwrap()
            .run_to_completion(100, std::time::Duration::from_millis(1), None)
            .unwrap();
        drop(target);
        drop(source);
        let snapshot_id = uuid::Uuid::new_v4().to_string();
        let snapshot_hash = file_sha256(&backup_path).unwrap();
        let predecessor = control.checkpoint().unwrap();
        let started = "2026-09-19T00:00:00Z";
        fs::write(
            backup_path.with_extension("db.meta.json"),
            serde_json::to_vec(&serde_json::json!({
                "snapshot_id": snapshot_id,
                "snapshot_started_at": started,
                "source_commit_sequence": null,
                "source_schema_version": HEALTH_SCHEMA_VERSION,
                "file_sha256": snapshot_hash,
                "control_checkpoint": predecessor
            }))
            .unwrap(),
        )
        .unwrap();
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            fs::set_permissions(&backup_path, fs::Permissions::from_mode(0o600)).unwrap();
            fs::set_permissions(
                backup_path.with_extension("db.meta.json"),
                fs::Permissions::from_mode(0o600),
            )
            .unwrap();
        }
        control
            .append_backup_created(&snapshot_id, &snapshot_hash, started, &predecessor)
            .unwrap();
        let baseline =
            verify_generation_zero_snapshot(&guard, &paths, &backup_path, &control, &journal)
                .unwrap();
        assert_eq!(baseline.snapshot_id, snapshot_id);
        assert_eq!(baseline.snapshot_sha256, snapshot_hash);
        journal.bind_baseline(&baseline).unwrap();
        assert_eq!(
            verify_generation_zero_snapshot(&guard, &paths, &backup_path, &control, &journal)
                .unwrap(),
            baseline
        );
        fs::write(&backup_path, b"tampered").unwrap();
        assert!(
            verify_generation_zero_snapshot(&guard, &paths, &backup_path, &control, &journal,)
                .is_err()
        );
    }

    #[test]
    fn emptied_ledger_with_durable_autoincrement_history_is_not_genesis() {
        let root = TempDir::new_in(std::env::temp_dir().canonicalize().unwrap()).unwrap();
        let coord = root.path().join("coord");
        initialize_coordinator(&coord).unwrap();
        let guard = lock_coordinator(&coord, true, true).unwrap();
        let paths = StoragePaths::new(
            root.path().join("data"),
            root.path().join("data/health.db"),
            root.path().join("control/control.db"),
            root.path().join("control/mirror"),
            root.path().join("backups"),
        );
        paths.prepare_empty_layout().unwrap();
        let control =
            ControlStore::initialize(paths.control_db.clone(), paths.control_mirror_dir.clone())
                .unwrap();
        initialize_health_database(&paths.health_db, &control.store_id().unwrap()).unwrap();
        let journal_root = root.path().join("journal");
        fs::create_dir(&journal_root).unwrap();
        AckJournal::initialize(&journal_root).unwrap();
        let journal = AckJournal::open(&journal_root).unwrap();

        let source = Connection::open(&paths.health_db).unwrap();
        source.execute_batch(
            "INSERT INTO devices(device_id,token_hash,created_at) VALUES('old-device','old-token','2026-09-19T00:00:00Z');
             INSERT INTO receipts(batch_id,device_id,content_hash,accepted_events,changed_events,received_at)
             VALUES('old-batch','old-device','old-hash',1,1,'2026-09-19T00:00:00Z');
             INSERT INTO audit(device_id,action,at) VALUES('old-device','ingest','2026-09-19T00:00:00Z');
             INSERT INTO outbox(device_id,batch_id,metric_name,created_at)
             VALUES('old-device','old-batch','heart_rate','2026-09-19T00:00:00Z');
             DELETE FROM outbox;
             DELETE FROM audit;
             DELETE FROM receipts;
             DELETE FROM devices;",
        )
        .unwrap();
        for table in ["receipts", "audit", "outbox"] {
            let sequence: i64 = source
                .query_row(
                    "SELECT seq FROM sqlite_sequence WHERE name=?1",
                    [table],
                    |row| row.get(0),
                )
                .unwrap();
            assert!(sequence > 0);
        }
        let backup_path = paths.backup_dir.join("boaz-health-genesis.db");
        let mut target = Connection::open(&backup_path).unwrap();
        rusqlite::backup::Backup::new(&source, &mut target)
            .unwrap()
            .run_to_completion(100, std::time::Duration::from_millis(1), None)
            .unwrap();
        drop(target);
        drop(source);
        let snapshot_id = uuid::Uuid::new_v4().to_string();
        let snapshot_hash = file_sha256(&backup_path).unwrap();
        let predecessor = control.checkpoint().unwrap();
        let started = "2026-09-19T00:00:00Z";
        fs::write(
            backup_path.with_extension("db.meta.json"),
            serde_json::to_vec(&serde_json::json!({
                "snapshot_id": snapshot_id,
                "snapshot_started_at": started,
                "source_commit_sequence": 1,
                "source_schema_version": HEALTH_SCHEMA_VERSION,
                "file_sha256": snapshot_hash,
                "control_checkpoint": predecessor
            }))
            .unwrap(),
        )
        .unwrap();
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            fs::set_permissions(&backup_path, fs::Permissions::from_mode(0o600)).unwrap();
            fs::set_permissions(
                backup_path.with_extension("db.meta.json"),
                fs::Permissions::from_mode(0o600),
            )
            .unwrap();
        }
        control
            .append_backup_created(&snapshot_id, &snapshot_hash, started, &predecessor)
            .unwrap();
        assert!(
            verify_generation_zero_snapshot(&guard, &paths, &backup_path, &control, &journal)
                .is_err(),
            "a ledger with deleted acknowledgements is not an independently proven genesis"
        );
    }
}
