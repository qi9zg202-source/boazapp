use boaz_health_receiver::{
    RuntimeGuard, ServerState, create_pairing_code, open_db, operation_lock,
    projection::{VmConfig, run_worker},
    revoke_device, router,
};
use chrono::{DateTime, Utc};
use std::{
    env,
    net::{IpAddr, Ipv4Addr, SocketAddr},
    path::PathBuf,
    time::Duration,
};

fn env_path(name: &str, default: &str) -> PathBuf {
    env::var_os(name)
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from(default))
}

fn attested(name: &str) -> bool {
    env::var(name).ok().as_deref() == Some("1")
}

fn backup(db_path: &PathBuf, destination: &PathBuf) -> Result<(), Box<dyn std::error::Error>> {
    if !attested("BOAZ_HEALTH_BACKUP_VOLUME_ENCRYPTED") {
        return Err("Encrypted backup volume must be verified first".into());
    }
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
    if !parent.is_dir() {
        return Err("Backup destination directory is missing".into());
    }
    let _operation_guard = operation_lock(db_path, false)?;
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
        let source = open_db(db_path)?;
        let snapshot_started_at = Utc::now().to_rfc3339();
        let source_commit_sequence: Option<i64> =
            source.query_row("SELECT max(commit_sequence) FROM receipts", [], |row| {
                row.get(0)
            })?;
        let mut target = rusqlite::Connection::open(&temp)?;
        {
            let snapshot = rusqlite::backup::Backup::new(&source, &mut target)?;
            snapshot.run_to_completion(100, Duration::from_millis(100), None)?;
        }
        let integrity: String = target.query_row("PRAGMA integrity_check", [], |row| row.get(0))?;
        if integrity != "ok" {
            return Err("Backup integrity check failed".into());
        }
        drop(target);
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(&temp, std::fs::Permissions::from_mode(0o600))?;
        }
        std::fs::write(
            &temp_manifest,
            serde_json::to_vec(
                &serde_json::json!({"snapshot_started_at":snapshot_started_at,"source_commit_sequence":source_commit_sequence}),
            )?,
        )?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(&temp_manifest, std::fs::Permissions::from_mode(0o600))?;
        }
        std::fs::rename(&temp, destination)?;
        if let Err(error) = std::fs::rename(&temp_manifest, &manifest) {
            let _ = std::fs::remove_file(destination);
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

fn prune_backups(db_path: &PathBuf, directory: &PathBuf) -> Result<(), Box<dyn std::error::Error>> {
    if !attested("BOAZ_HEALTH_BACKUP_VOLUME_ENCRYPTED") {
        return Err("Encrypted backup volume must be verified first".into());
    }
    if !directory.is_dir() {
        return Err("Managed backup directory is missing".into());
    }
    let _operation_guard = operation_lock(db_path, false)?;
    let now = Utc::now();
    let retention = chrono::Duration::days(29);
    let mut retained = Vec::new();
    let mut deleted = 0;
    for entry in std::fs::read_dir(directory)? {
        let entry = entry?;
        if !entry.file_type()?.is_file() {
            continue;
        }
        let name = entry.file_name();
        let name = name.to_string_lossy();
        if !name.starts_with("boaz-health-") || !name.ends_with(".db") {
            continue;
        }
        let manifest_path = entry.path().with_extension("db.meta.json");
        let manifest: serde_json::Value = serde_json::from_slice(&std::fs::read(&manifest_path)?)?;
        let started_text = manifest
            .get("snapshot_started_at")
            .and_then(serde_json::Value::as_str)
            .ok_or("Backup manifest lacks snapshot time")?;
        let started = DateTime::parse_from_rfc3339(started_text)?.with_timezone(&Utc);
        if now.signed_duration_since(started) >= retention {
            std::fs::remove_file(entry.path())?;
            std::fs::remove_file(manifest_path)?;
            deleted += 1;
        } else {
            retained.push(started);
        }
    }
    let mut connection = open_db(db_path)?;
    let pending = {
        let mut statement = connection.prepare("SELECT device_id,requested_at,backup_delete_by FROM erasures WHERE backups_expired_at IS NULL")?;
        let rows = statement.query_map([], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
            ))
        })?;
        rows.collect::<rusqlite::Result<Vec<_>>>()?
    };
    let transaction = connection.transaction()?;
    let mut overdue = 0;
    for (device_id, requested_at, deadline) in pending {
        let requested: DateTime<Utc> =
            DateTime::parse_from_rfc3339(&requested_at)?.with_timezone(&Utc);
        let old_copy_exists = retained.iter().any(|started| *started <= requested);
        if !old_copy_exists {
            transaction.execute(
                "UPDATE erasures SET backups_expired_at=?2 WHERE device_id=?1",
                rusqlite::params![device_id, Utc::now().to_rfc3339()],
            )?;
        } else if Utc::now() > DateTime::parse_from_rfc3339(&deadline)?.with_timezone(&Utc) {
            overdue += 1;
        }
    }
    transaction.commit()?;
    println!("managed_backups_deleted={deleted} overdue_erasures={overdue}");
    if overdue > 0 {
        return Err("Backup expiry deadline missed".into());
    }
    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    #[cfg(unix)]
    unsafe {
        libc::umask(0o077);
    }
    let db_path = env_path("BOAZ_HEALTH_DB", "/opt/boaz-health/data/health.db");
    if db_path.starts_with("/opt/boaz/data") {
        return Err(
            "Health receiver database must be outside the Mac sync replacement path".into(),
        );
    }
    let command = env::args().nth(1).unwrap_or_else(|| "serve".to_owned());
    match command.as_str() {
        "pair-code" => {
            let connection = open_db(&db_path)?;
            let code = create_pairing_code(&connection)?;
            println!("{code}");
            return Ok(());
        }
        "backup" => {
            let destination = env::args().nth(2).ok_or("Usage: boaz-health-receiver backup /encrypted/path/boaz-health-YYYYMMDD.db")?;
            backup(&db_path, &PathBuf::from(destination))?;
            return Ok(());
        }
        "revoke-device" => {
            let device_id = env::args().nth(2).ok_or("Usage: boaz-health-receiver revoke-device DEVICE_ID")?;
            let mut connection = open_db(&db_path)?;
            println!("revoked={}", revoke_device(&mut connection, &device_id)?);
            return Ok(());
        }
        "prune-backups" => {
            let directory = env::args().nth(2).ok_or("Usage: boaz-health-receiver prune-backups ENCRYPTED_BACKUP_DIRECTORY")?;
            prune_backups(&db_path, &PathBuf::from(directory))?;
            return Ok(());
        }
        "serve" => {},
        _ => return Err("Usage: boaz-health-receiver [serve|pair-code|backup PATH|prune-backups DIRECTORY|revoke-device DEVICE_ID]".into()),
    }
    let _ = open_db(&db_path)?;
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
        backup_volume: env_path("BOAZ_HEALTH_BACKUP_DIR", "/opt/boaz-health/backups"),
    };
    let requested_upload = attested("BOAZ_HEALTH_UPLOAD_ENABLED");
    let verified = attested("BOAZ_HEALTH_DATA_VOLUME_ENCRYPTED")
        && attested("BOAZ_HEALTH_BACKUP_RESTORE_VERIFIED")
        && attested("BOAZ_HEALTH_TAILSCALE_PRIVATE_VERIFIED")
        && guard.verified();
    let upload_enabled = requested_upload && verified;
    if requested_upload && !verified {
        eprintln!(
            "Upload gate remains closed: encrypted storage, backup restore, Tailscale privacy, or native VictoriaMetrics identity is unverified"
        );
    }
    let state = ServerState {
        db_path,
        upload_enabled,
        runtime_guard: Some(guard),
    };
    tokio::spawn(run_worker(state.clone(), vm_config));
    let port: u16 = env::var("BOAZ_HEALTH_PORT")
        .ok()
        .as_deref()
        .unwrap_or("8787")
        .parse()?;
    let address = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), port);
    let listener = tokio::net::TcpListener::bind(address).await?;
    println!("Boaz Health receiver listening on {address}; upload_enabled={upload_enabled}");
    axum::serve(listener, router(state)).await?;
    Ok(())
}
