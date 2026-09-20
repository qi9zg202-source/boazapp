use crate::{
    HealthEvent, ServerState, database::DEFAULT_MAPPING_VERSION, digest, metric_for, open_db,
};
use chrono::{DateTime, Utc};
use reqwest::Client;
use rusqlite::{Connection, OptionalExtension, TransactionBehavior, params};
use serde::{Deserialize, Serialize};
use std::{
    collections::{BTreeMap, HashMap},
    fs::{self, OpenOptions},
    io::Write,
    path::{Path, PathBuf},
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

const LIVE_VM_PORT: u16 = 8428;
const STAGING_VM_PORT: u16 = 18428;
const BOAZ_NAMESPACE: &str = "{__name__=~\"boaz_health_v1_.*\"}";
const ALL_METRICS: &str = "{__name__=~\".+\"}";
const DELETE_MATCH: &str = "{__name__=~\"boaz_health_v1_.*\",device_id=\"";
const GENERATION_MARKER: &str = ".boaz-projection-generation-v1.json";
const IDENTITY_CACHE_TTL: Duration = Duration::from_secs(60);

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
struct GenerationMarker {
    schema_version: u8,
    generation_id: String,
    storage_identity: String,
}

#[derive(Default)]
struct RuntimeIdentityCache {
    valid_until: Option<Instant>,
}

impl RuntimeIdentityCache {
    fn is_current(&self) -> bool {
        self.valid_until
            .is_some_and(|deadline| deadline > Instant::now())
    }

    fn mark_verified(&mut self) {
        self.valid_until = Some(Instant::now() + IDENTITY_CACHE_TTL);
    }

    fn invalidate(&mut self) {
        self.valid_until = None;
    }
}

#[derive(Clone)]
pub struct VmConfig {
    pub binary: PathBuf,
    pub storage: PathBuf,
}

/// A staging endpoint can only be constructed for a separate native VM process.
/// The live worker always uses the fixed production loopback port.
#[derive(Clone)]
pub struct VmTarget {
    config: VmConfig,
    port: u16,
    live_storage: Option<PathBuf>,
    staging_storage_identity: Option<String>,
    #[cfg_attr(not(target_os = "linux"), allow(dead_code))]
    native_child_identity: Option<(u32, u64)>,
}

impl VmTarget {
    fn live(config: &VmConfig) -> Self {
        Self {
            config: config.clone(),
            port: LIVE_VM_PORT,
            live_storage: None,
            staging_storage_identity: None,
            native_child_identity: None,
        }
    }

    pub fn staging(live: &VmConfig, candidate: VmConfig) -> Result<Self, String> {
        if !path_components_private(&candidate.storage) {
            return Err("staging_metrics_storage_invalid".to_owned());
        }
        let live_binary = live
            .binary
            .canonicalize()
            .map_err(|_| "native_metrics_binary_unavailable".to_owned())?;
        let candidate_binary = candidate
            .binary
            .canonicalize()
            .map_err(|_| "native_metrics_binary_unavailable".to_owned())?;
        if live_binary != candidate_binary {
            return Err("staging_metrics_binary_mismatch".to_owned());
        }
        let live_storage = live
            .storage
            .canonicalize()
            .map_err(|_| "native_metrics_storage_unavailable".to_owned())?;
        let candidate_storage = candidate
            .storage
            .canonicalize()
            .map_err(|_| "native_metrics_storage_unavailable".to_owned())?;
        if candidate_storage == live_storage
            || candidate_storage.starts_with(&live_storage)
            || live_storage.starts_with(&candidate_storage)
        {
            return Err("staging_metrics_storage_aliases_live".to_owned());
        }
        if !fs::symlink_metadata(&candidate.storage)
            .map_err(|_| "native_metrics_storage_unavailable".to_owned())?
            .file_type()
            .is_dir()
        {
            return Err("staging_metrics_storage_invalid".to_owned());
        }
        Ok(Self {
            staging_storage_identity: Some(storage_identity(&candidate.storage)?),
            config: candidate,
            port: STAGING_VM_PORT,
            live_storage: Some(live_storage),
            native_child_identity: None,
        })
    }

    /// A staging target is usable only for the exact child spawned by the
    /// recovery launcher. PID plus Linux start time prevents PID reuse from
    /// silently adopting another native VM process on the same socket.
    #[cfg(target_os = "linux")]
    pub(crate) fn bind_native_child(mut self, pid: u32) -> Result<Self, String> {
        if self.port != STAGING_VM_PORT || self.native_child_identity.is_some() {
            return Err("staging_native_child_identity_invalid".to_owned());
        }
        self.native_child_identity = Some((pid, process_start_time(pid)?));
        Ok(self)
    }

    fn url(&self) -> String {
        format!("http://127.0.0.1:{}", self.port)
    }
}

#[cfg(target_os = "linux")]
pub fn encrypted_mount_verified(path: &PathBuf) -> bool {
    let target = match path.canonicalize() {
        Ok(value) => value,
        Err(_) => return false,
    };
    let output = match std::process::Command::new("findmnt")
        .args(["-n", "-T"])
        .arg(&target)
        .args(["-o", "SOURCE"])
        .output()
    {
        Ok(value) if value.status.success() => value,
        _ => return false,
    };
    let source = match String::from_utf8(output.stdout) {
        Ok(value) => value.trim().to_owned(),
        Err(_) => return false,
    };
    if !source.starts_with("/dev/") {
        return false;
    }
    let device = match PathBuf::from(source).canonicalize() {
        Ok(value) => value,
        Err(_) => return false,
    };
    let Some(name) = device.file_name() else {
        return false;
    };
    let uuid_path = PathBuf::from("/sys/class/block").join(name).join("dm/uuid");
    std::fs::read_to_string(uuid_path)
        .ok()
        .is_some_and(|value| value.starts_with("CRYPT-"))
}

#[cfg(not(target_os = "linux"))]
pub fn encrypted_mount_verified(_path: &PathBuf) -> bool {
    false
}

fn error_message(error: impl std::fmt::Display) -> String {
    let _ = error;
    "native_metrics_unavailable_or_projection_failed".to_owned()
}

#[cfg(target_os = "linux")]
fn process_start_time(pid: u32) -> Result<u64, String> {
    let stat = fs::read_to_string(format!("/proc/{pid}/stat"))
        .map_err(|_| "native_metrics_process_identity_unavailable".to_owned())?;
    let fields = stat
        .rsplit_once(") ")
        .ok_or_else(|| "native_metrics_process_identity_unavailable".to_owned())?
        .1;
    // After the comm field, field 3 (state) is index 0; starttime is field 22.
    fields
        .split_whitespace()
        .nth(19)
        .and_then(|value| value.parse().ok())
        .ok_or_else(|| "native_metrics_process_identity_unavailable".to_owned())
}

#[cfg(target_os = "linux")]
pub fn native_vm_verified(config: &VmConfig) -> bool {
    native_vm_target_verified(&VmTarget::live(config))
}

#[cfg(target_os = "linux")]
fn native_vm_target_verified(target: &VmTarget) -> bool {
    if target.port == STAGING_VM_PORT && target.native_child_identity.is_none() {
        return false;
    }
    let expected_binary = match target.config.binary.canonicalize() {
        Ok(path) => path,
        Err(_) => return false,
    };
    let expected_storage = match target.config.storage.canonicalize() {
        Ok(path) => path,
        Err(_) => return false,
    };
    let tcp = match std::fs::read_to_string("/proc/net/tcp") {
        Ok(value) => value,
        Err(_) => return false,
    };
    let listener_inodes: Vec<String> = tcp
        .lines()
        .skip(1)
        .filter_map(|line| {
            let parts: Vec<_> = line.split_whitespace().collect();
            if parts.len() > 9
                && parts[1] == format!("0100007F:{:04X}", target.port)
                && parts[3] == "0A"
            {
                Some(parts[9].to_owned())
            } else {
                None
            }
        })
        .collect();
    if listener_inodes.is_empty() {
        return false;
    }
    let entries = match std::fs::read_dir("/proc") {
        Ok(value) => value,
        Err(_) => return false,
    };
    for entry in entries.flatten() {
        let name = entry.file_name();
        if !name
            .to_string_lossy()
            .bytes()
            .all(|byte| byte.is_ascii_digit())
        {
            continue;
        }
        if let Some((pid, start_time)) = target.native_child_identity {
            if name.to_string_lossy() != pid.to_string()
                || process_start_time(pid).ok() != Some(start_time)
            {
                continue;
            }
        }
        let proc_path = entry.path();
        if std::fs::read_link(proc_path.join("exe")).ok() != Some(expected_binary.clone()) {
            continue;
        }
        let cmdline = match std::fs::read(proc_path.join("cmdline")) {
            Ok(value) => value,
            Err(_) => continue,
        };
        let args: Vec<_> = cmdline
            .split(|byte| *byte == 0)
            .filter(|part| !part.is_empty())
            .map(|part| String::from_utf8_lossy(part).into_owned())
            .collect();
        let storage_ok = args.iter().any(|arg| {
            arg.strip_prefix("-storageDataPath=")
                .and_then(|path| PathBuf::from(path).canonicalize().ok())
                .as_ref()
                == Some(&expected_storage)
        });
        let listen_ok = args
            .iter()
            .any(|arg| arg == &format!("-httpListenAddr=127.0.0.1:{}", target.port));
        if !storage_ok || !listen_ok {
            continue;
        }
        let fds = match std::fs::read_dir(proc_path.join("fd")) {
            Ok(value) => value,
            Err(_) => continue,
        };
        for fd in fds.flatten() {
            if let Ok(target) = std::fs::read_link(fd.path()) {
                let target = target.to_string_lossy();
                if listener_inodes
                    .iter()
                    .any(|inode| target == format!("socket:[{inode}]"))
                {
                    return true;
                }
            }
        }
    }
    false
}

#[cfg(not(target_os = "linux"))]
pub fn native_vm_verified(_config: &VmConfig) -> bool {
    false
}

#[cfg(not(target_os = "linux"))]
fn native_vm_target_verified(_target: &VmTarget) -> bool {
    false
}

pub fn native_staging_vm_verified(target: &VmTarget) -> bool {
    target.port == STAGING_VM_PORT
        && staging_network_isolated()
        && staging_storage_disjoint(target)
        && encrypted_mount_verified(&target.config.storage)
        && native_vm_target_verified(target)
}

/// A stopped staging generation may be restarted only on the same isolated,
/// encrypted candidate storage with its original generation marker intact.
/// This does not attest the stored metric content; full export must follow.
#[cfg(target_os = "linux")]
pub(crate) fn staging_generation_resume_verified(target: &VmTarget) -> bool {
    target.port == STAGING_VM_PORT
        && staging_network_isolated()
        && staging_storage_disjoint(target)
        && encrypted_mount_verified(&target.config.storage)
        && read_marker(&target.config)
            .ok()
            .flatten()
            .is_some_and(|marker| {
                storage_identity(&target.config.storage).ok().as_deref()
                    == Some(marker.storage_identity.as_str())
            })
}

#[cfg(target_os = "linux")]
pub fn staging_network_isolated() -> bool {
    let (Ok(current), Ok(host)) = (
        fs::read_link("/proc/self/ns/net"),
        fs::read_link("/proc/1/ns/net"),
    ) else {
        return false;
    };
    if current == host {
        return false;
    }
    // A loopback-only namespace makes a listener swap unable to route a
    // staging request to the host's 8428 VM. An unexpected interface or a
    // local 8428 listener closes this gate until the namespace is repaired.
    let Ok(interfaces) = fs::read_dir("/sys/class/net") else {
        return false;
    };
    let mut found_loopback = false;
    for interface in interfaces {
        let Ok(interface) = interface else {
            return false;
        };
        if interface.file_name() != "lo" {
            return false;
        }
        found_loopback = true;
    }
    if !found_loopback {
        return false;
    }
    for table in ["/proc/net/tcp", "/proc/net/tcp6"] {
        let Ok(contents) = fs::read_to_string(table) else {
            return false;
        };
        for line in contents.lines().skip(1) {
            let columns: Vec<_> = line.split_whitespace().collect();
            if columns.len() < 4 {
                return false;
            }
            if columns[3] == "0A"
                && columns[1]
                    .rsplit_once(':')
                    .is_some_and(|(_, port)| port == "20EC")
            {
                return false;
            }
        }
    }
    true
}

#[cfg(not(target_os = "linux"))]
pub fn staging_network_isolated() -> bool {
    false
}

fn staging_storage_disjoint(target: &VmTarget) -> bool {
    let (Some(live_storage), Some(identity)) =
        (&target.live_storage, &target.staging_storage_identity)
    else {
        return false;
    };
    if !path_components_private(&target.config.storage)
        || !staging_tree_private(&target.config.storage)
        || storage_identity(&target.config.storage).ok().as_ref() != Some(identity)
    {
        return false;
    }
    let Ok(candidate) = target.config.storage.canonicalize() else {
        return false;
    };
    if candidate == *live_storage
        || candidate.starts_with(live_storage)
        || live_storage.starts_with(&candidate)
        || !fs::symlink_metadata(&target.config.storage)
            .ok()
            .is_some_and(|meta| meta.file_type().is_dir())
    {
        return false;
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        let (Ok(live), Ok(staging)) = (fs::metadata(live_storage), fs::metadata(&candidate)) else {
            return false;
        };
        if live.dev() == staging.dev() && live.ino() == staging.ino() {
            return false;
        }
    }
    true
}

fn path_components_private(path: &Path) -> bool {
    if !path.is_absolute() {
        return false;
    }
    for component in path.ancestors() {
        let Ok(metadata) = fs::symlink_metadata(component) else {
            return false;
        };
        if metadata.file_type().is_symlink() {
            return false;
        }
    }
    true
}

fn staging_tree_private(root: &Path) -> bool {
    let mut pending = vec![root.to_path_buf()];
    let mut entries = 0_usize;
    while let Some(path) = pending.pop() {
        entries += 1;
        if entries > 1_000_000 {
            return false;
        }
        let Ok(metadata) = fs::symlink_metadata(&path) else {
            return false;
        };
        if metadata.file_type().is_symlink() {
            return false;
        }
        if metadata.is_dir() {
            let Ok(children) = fs::read_dir(&path) else {
                return false;
            };
            for child in children {
                let Ok(child) = child else {
                    return false;
                };
                pending.push(child.path());
            }
        } else if metadata.is_file() {
            #[cfg(unix)]
            {
                use std::os::unix::fs::MetadataExt;
                if metadata.nlink() != 1 {
                    return false;
                }
            }
        } else {
            return false;
        }
    }
    true
}

fn storage_identity(path: &Path) -> Result<String, String> {
    let canonical = path
        .canonicalize()
        .map_err(|_| "metrics_storage_identity_unavailable".to_owned())?;
    let metadata =
        fs::metadata(&canonical).map_err(|_| "metrics_storage_identity_unavailable".to_owned())?;
    if !metadata.is_dir() {
        return Err("metrics_storage_identity_unavailable".to_owned());
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        Ok(digest(
            format!(
                "boaz-vm-storage-v1\0{}\0{}\0{}",
                canonical.display(),
                metadata.dev(),
                metadata.ino()
            )
            .as_bytes(),
        ))
    }
    #[cfg(not(unix))]
    {
        Ok(digest(
            format!("boaz-vm-storage-v1\0{}", canonical.display()).as_bytes(),
        ))
    }
}

fn marker_path(config: &VmConfig) -> PathBuf {
    config.storage.join(GENERATION_MARKER)
}

fn read_marker(config: &VmConfig) -> Result<Option<GenerationMarker>, String> {
    let path = marker_path(config);
    let metadata = match fs::symlink_metadata(&path) {
        Ok(value) => value,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(_) => return Err("projection_generation_marker_unavailable".to_owned()),
    };
    if metadata.file_type().is_symlink() || !metadata.file_type().is_file() {
        return Err("projection_generation_marker_invalid".to_owned());
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        if metadata.nlink() != 1 {
            return Err("projection_generation_marker_invalid".to_owned());
        }
    }
    let marker: GenerationMarker = serde_json::from_slice(
        &fs::read(path).map_err(|_| "projection_generation_marker_unavailable".to_owned())?,
    )
    .map_err(|_| "projection_generation_marker_invalid".to_owned())?;
    if marker.schema_version != 1
        || uuid::Uuid::parse_str(&marker.generation_id).is_err()
        || marker.storage_identity.len() != 64
        || !marker
            .storage_identity
            .bytes()
            .all(|byte| byte.is_ascii_hexdigit())
    {
        return Err("projection_generation_marker_invalid".to_owned());
    }
    Ok(Some(marker))
}

fn write_marker_atomic(path: &Path, marker: &GenerationMarker) -> Result<(), String> {
    let parent = path
        .parent()
        .ok_or_else(|| "projection_generation_marker_unavailable".to_owned())?;
    let temporary = parent.join(format!(
        ".boaz-projection-generation-{}.tmp",
        uuid::Uuid::new_v4()
    ));
    let result = (|| -> Result<(), String> {
        let mut file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&temporary)
            .map_err(|_| "projection_generation_marker_unavailable".to_owned())?;
        file.write_all(
            &serde_json::to_vec(marker)
                .map_err(|_| "projection_generation_marker_invalid".to_owned())?,
        )
        .map_err(|_| "projection_generation_marker_unavailable".to_owned())?;
        file.sync_all()
            .map_err(|_| "projection_generation_marker_unavailable".to_owned())?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            file.set_permissions(fs::Permissions::from_mode(0o600))
                .map_err(|_| "projection_generation_marker_unavailable".to_owned())?;
        }
        fs::rename(&temporary, path)
            .map_err(|_| "projection_generation_marker_unavailable".to_owned())?;
        let directory = OpenOptions::new()
            .read(true)
            .open(parent)
            .map_err(|_| "projection_generation_marker_unavailable".to_owned())?;
        directory
            .sync_all()
            .map_err(|_| "projection_generation_marker_unavailable".to_owned())?;
        Ok(())
    })();
    if result.is_err() {
        let _ = fs::remove_file(temporary);
    }
    result
}

fn ensure_generation_marker(config: &VmConfig) -> Result<GenerationMarker, String> {
    let identity = storage_identity(&config.storage)?;
    if let Some(existing) = read_marker(config)?
        && existing.storage_identity == identity
    {
        return Ok(existing);
    }
    let marker = GenerationMarker {
        schema_version: 1,
        generation_id: uuid::Uuid::new_v4().to_string(),
        storage_identity: identity,
    };
    write_marker_atomic(&marker_path(config), &marker)?;
    Ok(marker)
}

fn projection_state_needs_reconciliation(
    connection: &Connection,
    config: &VmConfig,
) -> Result<bool, String> {
    let current: (String, i64, String) = connection
        .query_row(
            "SELECT generation_id,mapping_version,storage_identity FROM projection_state WHERE singleton=1",
            [],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .map_err(error_message)?;
    let Some(marker) = read_marker(config)? else {
        return Ok(true);
    };
    let actual_storage_identity = storage_identity(&config.storage)?;
    Ok(marker.storage_identity != actual_storage_identity
        || current.0 != marker.generation_id
        || current.1 != DEFAULT_MAPPING_VERSION
        || current.2 != marker.storage_identity)
}

fn reconcile_projection_state(
    connection: &mut Connection,
    marker: &GenerationMarker,
) -> Result<bool, String> {
    let current: (String, i64, String) = connection
        .query_row(
            "SELECT generation_id,mapping_version,storage_identity FROM projection_state WHERE singleton=1",
            [],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .map_err(error_message)?;
    if current.0 == marker.generation_id
        && current.1 == DEFAULT_MAPPING_VERSION
        && current.2 == marker.storage_identity
    {
        return Ok(false);
    }
    let transaction = connection
        .transaction_with_behavior(TransactionBehavior::Immediate)
        .map_err(error_message)?;
    let now = Utc::now().to_rfc3339();
    transaction
        .execute(
            "UPDATE projection_state SET generation_id=?1,mapping_version=?2,storage_identity=?3,updated_at=?4 WHERE singleton=1",
            params![
                marker.generation_id,
                DEFAULT_MAPPING_VERSION,
                marker.storage_identity,
                now
            ],
        )
        .map_err(error_message)?;
    transaction
        .execute(
            "UPDATE receipts SET projected_at=NULL,projected_generation=NULL,projection_mapping_version=NULL WHERE requires_projection=1",
            [],
        )
        .map_err(error_message)?;
    transaction
        .execute(
            "UPDATE outbox SET processed_at=NULL,last_error=NULL WHERE processed_at IS NOT NULL",
            [],
        )
        .map_err(error_message)?;
    transaction.commit().map_err(error_message)?;
    Ok(true)
}

fn runtime_identity_verified(state: &ServerState, config: &VmConfig) -> bool {
    state
        .runtime_guard
        .as_ref()
        .map_or_else(|| native_vm_verified(config), |guard| guard.verified())
}

fn selector(device_id: &str, metric_name: &str) -> String {
    format!("{{__name__=\"{metric_name}\",device_id=\"{device_id}\"}}")
}

async fn delete_series(client: &Client, target: &VmTarget, selector: &str) -> Result<(), String> {
    let response = client
        .post(format!("{}/api/v1/admin/tsdb/delete_series", target.url()))
        .query(&[("match[]", selector)])
        .send()
        .await
        .map_err(error_message)?;
    if !response.status().is_success() {
        return Err("metrics_delete_failed".to_owned());
    }
    Ok(())
}

async fn import_lines(client: &Client, target: &VmTarget, lines: &[String]) -> Result<(), String> {
    let mut block = String::new();
    for line in lines {
        if block.len() + line.len() > 128 * 1024 && !block.is_empty() {
            let response = client
                .post(format!("{}/api/v1/import/prometheus", target.url()))
                .body(std::mem::take(&mut block))
                .send()
                .await
                .map_err(error_message)?;
            if !response.status().is_success() {
                return Err("metrics_import_failed".to_owned());
            }
        }
        block.push_str(line);
    }
    if !block.is_empty() {
        let response = client
            .post(format!("{}/api/v1/import/prometheus", target.url()))
            .body(block)
            .send()
            .await
            .map_err(error_message)?;
        if !response.status().is_success() {
            return Err("metrics_import_failed".to_owned());
        }
    }
    Ok(())
}

#[derive(Deserialize)]
struct ExportLine {
    metric: HashMap<String, String>,
    values: Vec<f64>,
    timestamps: Vec<i64>,
}

fn compare_export_line(
    line: &[u8],
    device_id: &str,
    metric_name: Option<&str>,
    outstanding: &mut BTreeMap<i64, f64>,
) -> bool {
    let Ok(export) = serde_json::from_slice::<ExportLine>(line) else {
        return false;
    };
    if export.metric.len() != 2
        || export.metric.get("device_id").map(String::as_str) != Some(device_id)
        || !export
            .metric
            .get("__name__")
            .is_some_and(|name| name.starts_with("boaz_health_v1_"))
        || metric_name
            .is_some_and(|name| export.metric.get("__name__").map(String::as_str) != Some(name))
        || export.values.len() != export.timestamps.len()
    {
        return false;
    }
    for (timestamp, value) in export.timestamps.into_iter().zip(export.values) {
        if outstanding.remove(&timestamp) != Some(value) {
            return false;
        }
    }
    true
}

async fn export_matches(
    client: &Client,
    target: &VmTarget,
    selector: &str,
    device_id: &str,
    metric_name: Option<&str>,
    expected: &BTreeMap<i64, f64>,
) -> Result<bool, String> {
    let mut response = client
        .get(format!("{}/api/v1/export", target.url()))
        .query(&[("match[]", selector), ("max_rows_per_line", "1000")])
        .send()
        .await
        .map_err(error_message)?;
    if !response.status().is_success() {
        return Err("metrics_readback_failed".to_owned());
    }
    let mut outstanding = expected.clone();
    let mut buffer = Vec::new();
    while let Some(chunk) = response.chunk().await.map_err(error_message)? {
        buffer.extend_from_slice(&chunk);
        while let Some(newline) = buffer.iter().position(|byte| *byte == b'\n') {
            let line = buffer.drain(..=newline).collect::<Vec<_>>();
            if line.len() > 1024 * 1024 {
                return Err("metrics_readback_line_too_large".to_owned());
            }
            if line.iter().all(|byte| byte.is_ascii_whitespace()) {
                continue;
            }
            if !compare_export_line(&line, device_id, metric_name, &mut outstanding) {
                return Ok(false);
            }
        }
        if buffer.len() > 1024 * 1024 {
            return Err("metrics_readback_line_too_large".to_owned());
        }
    }
    if !buffer.iter().all(|byte| byte.is_ascii_whitespace())
        && !compare_export_line(&buffer, device_id, metric_name, &mut outstanding)
    {
        return Ok(false);
    }
    Ok(outstanding.is_empty())
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize)]
struct SeriesKey {
    device_id: String,
    metric_name: String,
}

#[derive(Debug, Clone)]
pub struct ProjectionOracle {
    series: BTreeMap<SeriesKey, BTreeMap<i64, f64>>,
    pub collision_count: usize,
    pub oracle_sha256: String,
}

impl ProjectionOracle {
    pub fn series_count(&self) -> usize {
        self.series.len()
    }

    pub fn sample_count(&self) -> usize {
        self.series.values().map(BTreeMap::len).sum()
    }

    fn prometheus_lines(&self) -> Vec<String> {
        self.series
            .iter()
            .flat_map(|(key, samples)| {
                samples.iter().map(move |(timestamp, value)| {
                    format!(
                        "{}{{device_id=\"{}\"}} {} {}\n",
                        key.metric_name, key.device_id, value, timestamp
                    )
                })
            })
            .collect()
    }
}

fn canonical_series_sha256(
    series: &BTreeMap<SeriesKey, BTreeMap<i64, f64>>,
) -> Result<String, String> {
    let canonical = series
        .iter()
        .map(|(key, points)| {
            (
                key,
                points
                    .iter()
                    .map(|(timestamp, value)| (*timestamp, *value))
                    .collect::<Vec<_>>(),
            )
        })
        .collect::<Vec<_>>();
    Ok(digest(
        &serde_json::to_vec(&canonical).map_err(error_message)?,
    ))
}

pub fn build_projection_oracle(connection: &Connection) -> Result<ProjectionOracle, String> {
    // Match the live worker's event_id order so a same-millisecond collision
    // has one deterministic winner. Revisions have already converged in events.
    let mut statement = connection
        .prepare("SELECT device_id,payload_json FROM events WHERE operation='upsert' ORDER BY device_id,event_id")
        .map_err(error_message)?;
    let rows = statement
        .query_map([], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
        })
        .map_err(error_message)?;
    let mut series: BTreeMap<SeriesKey, BTreeMap<i64, f64>> = BTreeMap::new();
    let mut collision_count = 0;
    for row in rows {
        let (device_id, payload) = row.map_err(error_message)?;
        if device_id.is_empty()
            || device_id.len() > 128
            || !device_id
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_'))
        {
            return Err("projection_oracle_invalid_device_id".to_owned());
        }
        let event: HealthEvent = serde_json::from_str(&payload).map_err(error_message)?;
        if event.operation != crate::Operation::Upsert {
            return Err("projection_oracle_event_mismatch".to_owned());
        }
        let Some(metric_name) = metric_for(&event) else {
            continue;
        };
        let (Some(value), Some(end)) = (event.value, event.end_utc.as_deref()) else {
            continue;
        };
        if !value.is_finite() {
            return Err("projection_oracle_invalid_value".to_owned());
        }
        // VM may normalize IEEE -0.0 to 0.0 on import/export. They are one
        // numeric sample, so the persisted canonical digest must agree too.
        let value = if value == 0.0 { 0.0 } else { value };
        let timestamp = DateTime::parse_from_rfc3339(end)
            .map_err(error_message)?
            .timestamp_millis();
        let key = SeriesKey {
            device_id,
            metric_name: metric_name.to_owned(),
        };
        if series
            .entry(key)
            .or_default()
            .insert(timestamp, value)
            .is_some()
        {
            collision_count += 1;
        }
    }
    let oracle_sha256 = canonical_series_sha256(&series)?;
    Ok(ProjectionOracle {
        series,
        collision_count,
        oracle_sha256,
    })
}

async fn full_export_digest(
    client: &Client,
    target: &VmTarget,
    oracle: &ProjectionOracle,
) -> Result<Option<String>, String> {
    let mut response = client
        .get(format!("{}/api/v1/export", target.url()))
        .query(&[("match[]", ALL_METRICS), ("max_rows_per_line", "1000")])
        .send()
        .await
        .map_err(error_message)?;
    if !response.status().is_success() {
        return Err("metrics_readback_failed".to_owned());
    }
    let mut actual = BTreeMap::new();
    let mut buffer = Vec::new();
    while let Some(chunk) = response.chunk().await.map_err(error_message)? {
        buffer.extend_from_slice(&chunk);
        while let Some(newline) = buffer.iter().position(|byte| *byte == b'\n') {
            let line = buffer.drain(..=newline).collect::<Vec<_>>();
            if line.len() > 1024 * 1024 {
                return Err("metrics_readback_line_too_large".to_owned());
            }
            if !line.iter().all(|byte| byte.is_ascii_whitespace())
                && !consume_full_export_line(&line, &oracle.series, &mut actual)
            {
                return Ok(None);
            }
        }
        if buffer.len() > 1024 * 1024 {
            return Err("metrics_readback_line_too_large".to_owned());
        }
    }
    if !buffer.iter().all(|byte| byte.is_ascii_whitespace())
        && !consume_full_export_line(&buffer, &oracle.series, &mut actual)
    {
        return Ok(None);
    }
    if actual != oracle.series {
        return Ok(None);
    }
    Ok(Some(canonical_series_sha256(&actual)?))
}

fn consume_full_export_line(
    line: &[u8],
    expected: &BTreeMap<SeriesKey, BTreeMap<i64, f64>>,
    actual: &mut BTreeMap<SeriesKey, BTreeMap<i64, f64>>,
) -> bool {
    let Ok(export) = serde_json::from_slice::<ExportLine>(line) else {
        return false;
    };
    if export.metric.len() != 2
        || export.values.is_empty()
        || export.values.len() != export.timestamps.len()
    {
        return false;
    }
    let (Some(device_id), Some(metric_name)) = (
        export.metric.get("device_id"),
        export.metric.get("__name__"),
    ) else {
        return false;
    };
    if !metric_name.starts_with("boaz_health_v1_") || device_id.is_empty() {
        return false;
    }
    let key = SeriesKey {
        device_id: device_id.clone(),
        metric_name: metric_name.clone(),
    };
    let Some(expected_samples) = expected.get(&key) else {
        return false;
    };
    let samples = actual.entry(key).or_default();
    for (timestamp, value) in export.timestamps.into_iter().zip(export.values) {
        let value = if value == 0.0 { 0.0 } else { value };
        if !value.is_finite()
            || expected_samples.get(&timestamp) != Some(&value)
            || samples.insert(timestamp, value).is_some()
        {
            return false;
        }
    }
    true
}

#[cfg(test)]
fn compare_full_export_line(
    line: &[u8],
    outstanding: &mut BTreeMap<SeriesKey, BTreeMap<i64, f64>>,
) -> bool {
    let Ok(export) = serde_json::from_slice::<ExportLine>(line) else {
        return false;
    };
    if export.metric.len() != 2
        || export.values.is_empty()
        || export.values.len() != export.timestamps.len()
    {
        return false;
    }
    let (Some(device_id), Some(metric_name)) = (
        export.metric.get("device_id"),
        export.metric.get("__name__"),
    ) else {
        return false;
    };
    if !metric_name.starts_with("boaz_health_v1_") {
        return false;
    }
    let key = SeriesKey {
        device_id: device_id.clone(),
        metric_name: metric_name.clone(),
    };
    let Some(samples) = outstanding.get_mut(&key) else {
        return false;
    };
    for (timestamp, value) in export.timestamps.into_iter().zip(export.values) {
        if !value.is_finite() || samples.get(&timestamp) != Some(&value) {
            return false;
        }
        samples.remove(&timestamp);
    }
    true
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ProjectionEvidence {
    pub generation_id: String,
    pub mapping_version: i64,
    pub storage_path: PathBuf,
    pub storage_identity: String,
    pub binary_sha256: String,
    pub oracle_sha256: String,
    /// SHA-256 of canonical samples parsed from an actual complete VM export.
    /// It is recomputed on each staging or activated verification, never
    /// accepted merely because it has the shape of a digest.
    pub full_readback_sha256: String,
    pub verified_at: String,
    pub series_count: usize,
    pub sample_count: usize,
    pub collision_count: usize,
}

fn verified_marker(target: &VmTarget) -> Result<GenerationMarker, String> {
    let identity_ok = match target.port {
        STAGING_VM_PORT => native_staging_vm_verified(target),
        LIVE_VM_PORT => {
            encrypted_mount_verified(&target.config.storage) && native_vm_target_verified(target)
        }
        _ => false,
    };
    if !identity_ok {
        return Err("native_metrics_identity_unverified".to_owned());
    }
    let marker = read_marker(&target.config)?
        .ok_or_else(|| "staging_projection_generation_missing".to_owned())?;
    if marker.storage_identity != storage_identity(&target.config.storage)? {
        return Err("staging_projection_storage_changed".to_owned());
    }
    Ok(marker)
}

pub async fn rebuild_and_verify_staging(
    connection: &mut Connection,
    target: &VmTarget,
) -> Result<ProjectionEvidence, String> {
    if !native_staging_vm_verified(target) {
        return Err("staging_native_metrics_identity_unverified".to_owned());
    }
    let oracle = build_projection_oracle(connection)?;
    let marker = ensure_generation_marker(&target.config)?;
    reconcile_projection_state(connection, &marker)?;
    let client = Client::builder()
        .no_proxy()
        .timeout(Duration::from_secs(30))
        .build()
        .map_err(error_message)?;

    // This target cannot name the live socket or storage. Clearing only the
    // Boaz namespace allows an interrupted staging import to be retried.
    delete_series(&client, target, BOAZ_NAMESPACE).await?;
    let empty = ProjectionOracle {
        series: BTreeMap::new(),
        collision_count: 0,
        oracle_sha256: digest(b"[]"),
    };
    let mut empty_confirmed = false;
    for attempt in 0..5 {
        if full_export_digest(&client, target, &empty).await?.is_some() {
            empty_confirmed = true;
            break;
        }
        if attempt < 4 {
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
    }
    if !empty_confirmed {
        return Err("staging_metrics_namespace_not_empty".to_owned());
    }
    import_lines(&client, target, &oracle.prometheus_lines()).await?;
    let mut readback_sha256 = None;
    for attempt in 0..5 {
        if let Some(digest) = full_export_digest(&client, target, &oracle).await? {
            readback_sha256 = Some(digest);
            break;
        }
        if attempt < 4 {
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
    }
    let readback_sha256 =
        readback_sha256.ok_or_else(|| "staging_metrics_readback_mismatch".to_owned())?;
    let current_marker = verified_marker(target)?;
    if current_marker != marker {
        return Err("staging_projection_generation_changed".to_owned());
    }
    let storage_path = target
        .config
        .storage
        .canonicalize()
        .map_err(|_| "staging_projection_storage_changed".to_owned())?;
    let binary_sha256 = digest(
        &fs::read(&target.config.binary)
            .map_err(|_| "native_metrics_binary_unavailable".to_owned())?,
    );
    Ok(ProjectionEvidence {
        generation_id: marker.generation_id,
        mapping_version: DEFAULT_MAPPING_VERSION,
        storage_path,
        storage_identity: marker.storage_identity,
        binary_sha256,
        oracle_sha256: oracle.oracle_sha256.clone(),
        full_readback_sha256: readback_sha256,
        verified_at: Utc::now().to_rfc3339(),
        series_count: oracle.series_count(),
        sample_count: oracle.sample_count(),
        collision_count: oracle.collision_count,
    })
}

pub async fn verify_staging_readback(
    connection: &Connection,
    target: &VmTarget,
    evidence: &ProjectionEvidence,
) -> Result<(), String> {
    if target.port != STAGING_VM_PORT {
        return Err("staging_metrics_target_required".to_owned());
    }
    verify_readback_with_target(connection, target, evidence).await
}

pub async fn verify_activated_readback(
    connection: &Connection,
    active: &VmConfig,
    evidence: &ProjectionEvidence,
) -> Result<(), String> {
    verify_readback_with_target(connection, &VmTarget::live(active), evidence).await
}

async fn verify_readback_with_target(
    connection: &Connection,
    target: &VmTarget,
    evidence: &ProjectionEvidence,
) -> Result<(), String> {
    let marker = verified_marker(target)?;
    let oracle = build_projection_oracle(connection)?;
    let projection_state: (String, i64, String) = connection
        .query_row(
            "SELECT generation_id,mapping_version,storage_identity FROM projection_state WHERE singleton=1",
            [],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .map_err(error_message)?;
    if marker.generation_id != evidence.generation_id
        || marker.storage_identity != evidence.storage_identity
        || evidence.mapping_version != DEFAULT_MAPPING_VERSION
        || projection_state
            != (
                evidence.generation_id.clone(),
                evidence.mapping_version,
                evidence.storage_identity.clone(),
            )
        || target.config.storage.canonicalize().ok().as_ref() != Some(&evidence.storage_path)
        || digest(
            &fs::read(&target.config.binary)
                .map_err(|_| "native_metrics_binary_unavailable".to_owned())?,
        ) != evidence.binary_sha256
        || oracle.oracle_sha256 != evidence.oracle_sha256
        || oracle.series_count() != evidence.series_count
        || oracle.sample_count() != evidence.sample_count
        || oracle.collision_count != evidence.collision_count
    {
        return Err("staging_projection_evidence_mismatch".to_owned());
    }
    let client = Client::builder()
        .no_proxy()
        .timeout(Duration::from_secs(30))
        .build()
        .map_err(error_message)?;
    if full_export_digest(&client, target, &oracle)
        .await?
        .as_deref()
        != Some(evidence.full_readback_sha256.as_str())
    {
        return Err("staging_metrics_readback_mismatch".to_owned());
    }
    let after_marker = verified_marker(target)?;
    if after_marker != marker {
        return Err("staging_projection_generation_changed".to_owned());
    }
    Ok(())
}

pub async fn settle_staging_projection(
    connection: &mut Connection,
    target: &VmTarget,
    evidence: &ProjectionEvidence,
) -> Result<(), String> {
    verify_staging_readback(connection, target, evidence).await?;
    commit_staging_projection_success(connection, evidence)
}

fn commit_staging_projection_success(
    connection: &mut Connection,
    evidence: &ProjectionEvidence,
) -> Result<(), String> {
    let current: (String, i64, String) = connection
        .query_row(
            "SELECT generation_id,mapping_version,storage_identity FROM projection_state WHERE singleton=1",
            [],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .map_err(error_message)?;
    if current
        != (
            evidence.generation_id.clone(),
            evidence.mapping_version,
            evidence.storage_identity.clone(),
        )
    {
        return Err("staging_projection_state_changed".to_owned());
    }
    let transaction = connection
        .transaction_with_behavior(TransactionBehavior::Immediate)
        .map_err(error_message)?;
    transaction
        .execute(
            "UPDATE outbox SET processed_at=?1,last_error=NULL WHERE processed_at IS NULL",
            params![evidence.verified_at],
        )
        .map_err(error_message)?;
    transaction
        .execute(
            "UPDATE receipts SET projected_at=?1,projected_generation=?2,projection_mapping_version=?3
             WHERE requires_projection=1 AND NOT EXISTS
               (SELECT 1 FROM outbox WHERE outbox.batch_id=receipts.batch_id AND outbox.processed_at IS NULL)",
            params![evidence.verified_at, evidence.generation_id, evidence.mapping_version],
        )
        .map_err(error_message)?;
    transaction.commit().map_err(error_message)
}

async fn confirmed_readback(
    client: &Client,
    target: &VmTarget,
    selector: &str,
    device_id: &str,
    metric_name: Option<&str>,
    expected: &BTreeMap<i64, f64>,
) -> Result<(), String> {
    for attempt in 0..5 {
        if export_matches(client, target, selector, device_id, metric_name, expected).await? {
            return Ok(());
        }
        if attempt < 4 {
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
    }
    Err("metrics_readback_mismatch".to_owned())
}

#[derive(Debug, Clone)]
struct PendingErasureJob {
    device_id: String,
    erasure_id: String,
    control_authoritative: bool,
}

fn pending_erasure_job(
    state: &ServerState,
    connection: &Connection,
) -> Result<Option<PendingErasureJob>, String> {
    if let Some(store) = &state.control_store {
        return store
            .pending_metric_erasures()
            .map_err(error_message)
            .map(|jobs| {
                jobs.into_iter().next().map(|job| PendingErasureJob {
                    device_id: job.device_id,
                    erasure_id: job.erasure_id,
                    control_authoritative: true,
                })
            });
    }
    connection
        .query_row(
            "SELECT device_id,erasure_id FROM erasures WHERE metrics_deleted_at IS NULL ORDER BY requested_at LIMIT 1",
            [],
            |row| {
                Ok(PendingErasureJob {
                    device_id: row.get(0)?,
                    erasure_id: row.get(1)?,
                    control_authoritative: false,
                })
            },
        )
        .optional()
        .map_err(error_message)
}

fn pending_projection_job(
    connection: &Connection,
) -> Result<Option<(String, String, i64)>, String> {
    connection
        .query_row(
            "SELECT device_id,metric_name,max(id) FROM outbox WHERE processed_at IS NULL GROUP BY device_id,metric_name ORDER BY min(id) LIMIT 1",
            [],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .optional()
        .map_err(error_message)
}

fn commit_projection_success(
    connection: &mut Connection,
    device_id: &str,
    metric_name: &str,
    max_id: i64,
    marker: &GenerationMarker,
    projected_at: &str,
) -> Result<(), String> {
    let transaction = connection
        .transaction_with_behavior(TransactionBehavior::Immediate)
        .map_err(error_message)?;
    transaction
        .execute(
            "UPDATE outbox SET processed_at=?1,last_error=NULL WHERE device_id=?2 AND metric_name=?3 AND id<=?4 AND processed_at IS NULL",
            params![projected_at, device_id, metric_name, max_id],
        )
        .map_err(error_message)?;
    transaction
        .execute(
            "UPDATE receipts
             SET projected_at=?1,projected_generation=?3,projection_mapping_version=?4
             WHERE device_id=?2 AND requires_projection=1
               AND NOT EXISTS (
                   SELECT 1 FROM outbox
                   WHERE outbox.batch_id=receipts.batch_id AND outbox.processed_at IS NULL
               )",
            params![
                projected_at,
                device_id,
                marker.generation_id,
                DEFAULT_MAPPING_VERSION
            ],
        )
        .map_err(error_message)?;
    transaction.commit().map_err(error_message)
}

async fn project_once_with_client(
    state: &ServerState,
    config: &VmConfig,
    client: &Client,
    identity_cache: &mut RuntimeIdentityCache,
) -> Result<bool, String> {
    // Backup, ingest and erasure freeze health mutations with this same lock.
    // Acquire it before opening either ledger or touching VM, and keep it for
    // the entire pass so control publication and SQLite convergence cannot
    // cross a backup's snapshot/checkpoint boundary. Try-only avoids blocking
    // a Tokio worker thread behind a long-running backup.
    let _operation_guard = crate::operation_lock(&state.db_path, true)
        .map_err(|error| format!("health_operation_lock_unavailable: {error}"))?;
    let target = VmTarget::live(config);
    let mut connection = open_db(&state.db_path).map_err(error_message)?;

    // A control event is the durable authority. If the process crashed after
    // committing that event but before updating the health ledger, converge
    // the local projection before deciding that the worker is idle. The
    // control store performs its full chain/head/mirror verification first,
    // so a divergent authority fails closed instead of being projected.
    if let Some(store) = &state.control_store {
        store
            .reconcile_health(&mut connection)
            .map_err(error_message)?;
    }

    // Check durable work and the cheap marker state before inspecting processes,
    // sockets, or encrypted mounts.
    let has_erasure = pending_erasure_job(state, &connection)?.is_some();
    let has_projection = pending_projection_job(&connection)?.is_some();
    let generation_changed = projection_state_needs_reconciliation(&connection, config)?;
    if !has_erasure && !has_projection && !generation_changed {
        return Ok(false);
    }

    if !identity_cache.is_current() {
        if !runtime_identity_verified(state, config) {
            identity_cache.invalidate();
            return Err("native_metrics_identity_unverified".to_owned());
        }
        identity_cache.mark_verified();
    }

    let marker = ensure_generation_marker(config)?;
    // The same pass immediately begins draining any deterministic rebuild.
    reconcile_projection_state(&mut connection, &marker)?;

    if let Some(erasure) = pending_erasure_job(state, &connection)? {
        let match_selector = format!("{DELETE_MATCH}{}\"}}", erasure.device_id);
        let result = async {
            delete_series(client, &target, &match_selector).await?;
            confirmed_readback(
                client,
                &target,
                &match_selector,
                &erasure.device_id,
                None,
                &BTreeMap::new(),
            )
            .await
        }
        .await;
        match result {
            Ok(()) => {
                let now = Utc::now().to_rfc3339();
                if erasure.control_authoritative {
                    state
                        .control_store
                        .as_ref()
                        .ok_or_else(|| "control_store_unavailable".to_owned())?
                        .append_metrics_erasure_verified(
                            &erasure.device_id,
                            &erasure.erasure_id,
                            &now,
                        )
                        .map_err(error_message)?;
                }
                let transaction = connection
                    .transaction_with_behavior(TransactionBehavior::Immediate)
                    .map_err(error_message)?;
                transaction
                    .execute(
                        "UPDATE erasures
                         SET device_id=?2,metrics_deleted_at=?3,last_error=NULL
                         WHERE erasure_id=?1 AND metrics_deleted_at IS NULL",
                        params![
                            erasure.erasure_id,
                            digest(erasure.device_id.as_bytes()),
                            now
                        ],
                    )
                    .map_err(error_message)?;
                transaction.commit().map_err(error_message)?;
            }
            Err(error) => {
                identity_cache.invalidate();
                connection
                    .execute(
                        "UPDATE erasures SET last_error=?2 WHERE erasure_id=?1",
                        params![erasure.erasure_id, error],
                    )
                    .map_err(error_message)?;
                return Err(error);
            }
        }
        return Ok(true);
    }

    let Some((device_id, metric_name, max_id)) = pending_projection_job(&connection)? else {
        return Ok(false);
    };
    let expected = {
        let mut statement = connection
            .prepare("SELECT payload_json FROM events WHERE device_id=?1 AND operation='upsert' ORDER BY event_id")
            .map_err(error_message)?;
        let serialized = statement
            .query_map(params![device_id], |row| row.get::<_, String>(0))
            .map_err(error_message)?;
        let mut values = BTreeMap::new();
        for serialized in serialized {
            let event: HealthEvent =
                serde_json::from_str(&serialized.map_err(error_message)?).map_err(error_message)?;
            if metric_for(&event) != Some(metric_name.as_str()) {
                continue;
            }
            let (Some(value), Some(end)) = (event.value, event.end_utc.as_deref()) else {
                continue;
            };
            let timestamp = DateTime::parse_from_rfc3339(end)
                .map_err(error_message)?
                .timestamp_millis();
            values.insert(timestamp, value);
        }
        values
    };
    let lines: Vec<String> = expected
        .iter()
        .map(|(timestamp, value)| {
            format!("{metric_name}{{device_id=\"{device_id}\"}} {value} {timestamp}\n")
        })
        .collect();
    let match_selector = selector(&device_id, &metric_name);
    let result = async {
        delete_series(client, &target, &match_selector).await?;
        import_lines(client, &target, &lines).await?;
        confirmed_readback(
            client,
            &target,
            &match_selector,
            &device_id,
            Some(&metric_name),
            &expected,
        )
        .await
    }
    .await;
    match result {
        Ok(()) => {
            let now = Utc::now().to_rfc3339();
            commit_projection_success(
                &mut connection,
                &device_id,
                &metric_name,
                max_id,
                &marker,
                &now,
            )?;
            Ok(true)
        }
        Err(error) => {
            identity_cache.invalidate();
            connection.execute("UPDATE outbox SET attempts=attempts+1,last_error=?1 WHERE device_id=?2 AND metric_name=?3 AND id<=?4 AND processed_at IS NULL", params![error,device_id,metric_name,max_id]).map_err(error_message)?;
            Err(error)
        }
    }
}

pub async fn project_once(state: &ServerState, config: &VmConfig) -> Result<bool, String> {
    let client = Client::builder()
        .timeout(Duration::from_secs(5))
        .build()
        .map_err(error_message)?;
    let mut identity_cache = RuntimeIdentityCache::default();
    project_once_with_client(state, config, &client, &mut identity_cache).await
}

fn retry_delay(attempt: u32) -> Duration {
    let exponent = attempt.min(6);
    let base_seconds = 1_u64 << exponent;
    let base_seconds = base_seconds.min(60);
    let jitter_millis = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| u64::from(duration.subsec_nanos()) % 251)
        .unwrap_or(0);
    Duration::from_secs(base_seconds) + Duration::from_millis(jitter_millis)
}

pub async fn run_worker(state: ServerState, config: VmConfig) {
    let client = match Client::builder().timeout(Duration::from_secs(5)).build() {
        Ok(client) => client,
        Err(_) => panic!("projection HTTP client initialization failed"),
    };
    let mut identity_cache = RuntimeIdentityCache::default();
    let mut failure_attempt = 0_u32;
    loop {
        match project_once_with_client(&state, &config, &client, &mut identity_cache).await {
            Ok(true) => {
                failure_attempt = 0;
                continue;
            }
            Ok(false) => {
                failure_attempt = 0;
                tokio::select! {
                    _ = state.projection_notify.notified() => {},
                    _ = tokio::time::sleep(Duration::from_secs(60)) => {},
                }
            }
            Err(error) => {
                eprintln!("projection worker retryable failure: {error}");
                let delay = retry_delay(failure_attempt);
                failure_attempt = failure_attempt.saturating_add(1);
                tokio::time::sleep(delay).await;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::database::initialize_health_database;
    use tempfile::TempDir;

    fn fixture() -> (TempDir, PathBuf, VmConfig) {
        let directory = TempDir::new_in(std::env::temp_dir().canonicalize().unwrap()).unwrap();
        let data = directory.path().join("data");
        let metrics = directory.path().join("metrics");
        fs::create_dir_all(&data).unwrap();
        fs::create_dir_all(&metrics).unwrap();
        let database = data.join("health.db");
        initialize_health_database(&database, "control-test").unwrap();
        let config = VmConfig {
            binary: directory.path().join("victoria-metrics-prod"),
            storage: metrics,
        };
        (directory, database, config)
    }

    fn insert_projected_job(connection: &Connection) {
        let old_generation: String = connection
            .query_row(
                "SELECT generation_id FROM projection_state WHERE singleton=1",
                [],
                |row| row.get(0),
            )
            .unwrap();
        connection
            .execute(
                "INSERT INTO devices(device_id,token_hash,created_at) VALUES ('phone-1','token-hash','2026-09-19T00:00:00Z')",
                [],
            )
            .unwrap();
        connection
            .execute(
                "INSERT INTO receipts(batch_id,device_id,content_hash,accepted_events,changed_events,requires_projection,received_at,projected_at,projected_generation,projection_mapping_version)
                 VALUES ('batch-1','phone-1','content-hash',1,1,1,'2026-09-19T00:00:00Z','2026-09-19T00:01:00Z',?1,?2)",
                params![old_generation, DEFAULT_MAPPING_VERSION],
            )
            .unwrap();
        connection
            .execute(
                "INSERT INTO outbox(device_id,batch_id,metric_name,created_at,processed_at)
                 VALUES ('phone-1','batch-1','boaz_health_v1_heart_rate_bpm','2026-09-19T00:00:00Z','2026-09-19T00:01:00Z')",
                [],
            )
            .unwrap();
    }

    #[test]
    fn generation_marker_change_demotes_receipt_and_requeues_work() {
        let (_directory, database, config) = fixture();
        let mut connection = open_db(&database).unwrap();
        insert_projected_job(&connection);

        let marker = ensure_generation_marker(&config).unwrap();
        assert!(reconcile_projection_state(&mut connection, &marker).unwrap());
        let receipt: (Option<String>, Option<String>, Option<i64>) = connection
            .query_row(
                "SELECT projected_at,projected_generation,projection_mapping_version FROM receipts WHERE batch_id='batch-1'",
                [],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .unwrap();
        assert_eq!(receipt, (None, None, None));
        let processed: Option<String> = connection
            .query_row(
                "SELECT processed_at FROM outbox WHERE batch_id='batch-1'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert!(processed.is_none());
        assert!(!reconcile_projection_state(&mut connection, &marker).unwrap());
    }

    #[test]
    fn outbox_and_receipt_completion_roll_back_together() {
        let (_directory, database, config) = fixture();
        let mut connection = open_db(&database).unwrap();
        insert_projected_job(&connection);
        let marker = ensure_generation_marker(&config).unwrap();
        reconcile_projection_state(&mut connection, &marker).unwrap();
        connection
            .execute_batch(
                "CREATE TEMP TRIGGER synthetic_receipt_failure
                 BEFORE UPDATE OF projected_at ON receipts
                 BEGIN
                   SELECT RAISE(ABORT, 'synthetic receipt failure');
                 END;",
            )
            .unwrap();
        assert!(
            commit_projection_success(
                &mut connection,
                "phone-1",
                "boaz_health_v1_heart_rate_bpm",
                1,
                &marker,
                "2026-09-19T00:02:00Z",
            )
            .is_err()
        );
        let processed: Option<String> = connection
            .query_row(
                "SELECT processed_at FROM outbox WHERE batch_id='batch-1'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        let projected: Option<String> = connection
            .query_row(
                "SELECT projected_at FROM receipts WHERE batch_id='batch-1'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert!(processed.is_none());
        assert!(projected.is_none());
    }

    #[test]
    fn corrupt_generation_marker_fails_closed() {
        let (_directory, _database, config) = fixture();
        fs::write(marker_path(&config), b"not-json").unwrap();
        assert_eq!(
            read_marker(&config).unwrap_err(),
            "projection_generation_marker_invalid"
        );
    }

    #[test]
    fn retry_delay_is_bounded_and_increases() {
        assert!(retry_delay(0) >= Duration::from_secs(1));
        assert!(retry_delay(1) >= Duration::from_secs(2));
        assert!(retry_delay(20) < Duration::from_secs(61));
    }

    #[test]
    fn staging_target_rejects_live_storage_alias_and_nested_path() {
        let (directory, _database, live) = fixture();
        fs::write(&live.binary, b"synthetic-native-binary").unwrap();
        let alias = VmConfig {
            binary: live.binary.clone(),
            storage: live.storage.clone(),
        };
        assert_eq!(
            VmTarget::staging(&live, alias).err().unwrap(),
            "staging_metrics_storage_aliases_live"
        );
        let nested = live.storage.join("nested");
        fs::create_dir(&nested).unwrap();
        assert_eq!(
            VmTarget::staging(
                &live,
                VmConfig {
                    binary: live.binary.clone(),
                    storage: nested,
                },
            )
            .err()
            .unwrap(),
            "staging_metrics_storage_aliases_live"
        );
        let separate = directory.path().join("staging-metrics");
        fs::create_dir(&separate).unwrap();
        let staging = VmTarget::staging(
            &live,
            VmConfig {
                binary: live.binary.clone(),
                storage: separate,
            },
        )
        .unwrap();
        assert_eq!(staging.url(), "http://127.0.0.1:18428");
        assert!(!native_staging_vm_verified(&staging));
        #[cfg(unix)]
        {
            fs::remove_dir(&staging.config.storage).unwrap();
            std::os::unix::fs::symlink(&live.storage, &staging.config.storage).unwrap();
            assert!(!staging_storage_disjoint(&staging));
        }
    }

    fn insert_event(
        connection: &Connection,
        event_id: &str,
        operation: &str,
        health_type: &str,
        value: Option<f64>,
        unit: &str,
    ) {
        let payload = serde_json::json!({
            "event_id": event_id,
            "revision": 1,
            "operation": operation,
            "kind": "quantity",
            "type": health_type,
            "source": null,
            "start_utc": null,
            "end_utc": "2026-09-19T00:00:00.123Z",
            "value": value,
            "unit": unit,
            "metadata": {}
        });
        connection
            .execute(
                "INSERT INTO events(device_id,event_id,revision,operation,kind,health_type,end_utc,value,unit,payload_json,payload_hash,updated_at)
                 VALUES ('phone-1',?1,1,?2,'quantity',?3,'2026-09-19T00:00:00.123Z',?4,?5,?6,'synthetic-hash','2026-09-19T00:01:00Z')",
                params![event_id, operation, health_type, value, unit, payload.to_string()],
            )
            .unwrap();
    }

    #[test]
    fn oracle_matches_live_collision_order_and_excludes_deletion_and_bad_unit() {
        let (_directory, database, _config) = fixture();
        let connection = open_db(&database).unwrap();
        connection
            .execute(
                "INSERT INTO devices(device_id,token_hash,created_at) VALUES ('phone-1','token-hash','2026-09-19T00:00:00Z')",
                [],
            )
            .unwrap();
        insert_event(
            &connection,
            "a",
            "upsert",
            "heartRate",
            Some(60.0),
            "count/min",
        );
        insert_event(
            &connection,
            "b",
            "upsert",
            "heartRate",
            Some(70.0),
            "count/min",
        );
        insert_event(&connection, "c", "delete", "heartRate", None, "count/min");
        insert_event(&connection, "d", "upsert", "heartRate", Some(80.0), "kg");
        let oracle = build_projection_oracle(&connection).unwrap();
        assert_eq!(oracle.series_count(), 1);
        assert_eq!(oracle.sample_count(), 1);
        assert_eq!(oracle.collision_count, 1);
        assert!(oracle.prometheus_lines()[0].contains(" 70 1789776000123"));
    }

    #[test]
    fn full_export_rejects_extra_labels_series_points_and_wrong_values() {
        let key = SeriesKey {
            device_id: "phone-1".to_owned(),
            metric_name: "boaz_health_v1_heart_rate_bpm".to_owned(),
        };
        let mut expected = BTreeMap::from([(key, BTreeMap::from([(1000, 70.0)]))]);
        let extra_label = br#"{"metric":{"__name__":"boaz_health_v1_heart_rate_bpm","device_id":"phone-1","source":"extra"},"values":[70],"timestamps":[1000]}"#;
        assert!(!compare_full_export_line(extra_label, &mut expected));
        let extra_series = br#"{"metric":{"__name__":"boaz_health_v1_weight_kg","device_id":"phone-1"},"values":[70],"timestamps":[1000]}"#;
        assert!(!compare_full_export_line(extra_series, &mut expected));
        let wrong_value = br#"{"metric":{"__name__":"boaz_health_v1_heart_rate_bpm","device_id":"phone-1"},"values":[71],"timestamps":[1000]}"#;
        assert!(!compare_full_export_line(wrong_value, &mut expected));
        let right = br#"{"metric":{"__name__":"boaz_health_v1_heart_rate_bpm","device_id":"phone-1"},"values":[70],"timestamps":[1000]}"#;
        assert!(compare_full_export_line(right, &mut expected));
        assert!(expected.values().all(BTreeMap::is_empty));
        assert!(!compare_full_export_line(right, &mut expected));
    }

    #[test]
    fn export_digest_is_canonical_and_rejects_unexpected_or_repeated_points() {
        let first = SeriesKey {
            device_id: "phone-1".to_owned(),
            metric_name: "boaz_health_v1_heart_rate_bpm".to_owned(),
        };
        let second = SeriesKey {
            device_id: "phone-2".to_owned(),
            metric_name: "boaz_health_v1_weight_kg".to_owned(),
        };
        let expected = BTreeMap::from([
            (first, BTreeMap::from([(1000, 70.0), (2000, 71.0)])),
            (second, BTreeMap::from([(1500, 80.0)])),
        ]);
        let oracle_hash = canonical_series_sha256(&expected).unwrap();
        let mut actual = BTreeMap::new();
        let weight = br#"{"metric":{"device_id":"phone-2","__name__":"boaz_health_v1_weight_kg"},"values":[80],"timestamps":[1500]}"#;
        let heart = br#"{"timestamps":[2000,1000],"values":[71,70],"metric":{"__name__":"boaz_health_v1_heart_rate_bpm","device_id":"phone-1"}}"#;
        assert!(consume_full_export_line(weight, &expected, &mut actual));
        assert!(consume_full_export_line(heart, &expected, &mut actual));
        assert_eq!(actual, expected);
        assert_eq!(canonical_series_sha256(&actual).unwrap(), oracle_hash);
        assert!(!consume_full_export_line(heart, &expected, &mut actual));
        let extra = br#"{"metric":{"__name__":"boaz_health_v1_weight_kg","device_id":"erased-phone"},"values":[80],"timestamps":[1500]}"#;
        assert!(!consume_full_export_line(extra, &expected, &mut actual));
        assert_eq!(actual, expected);
    }
}
