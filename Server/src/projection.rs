use crate::{HealthEvent, ServerState, metric_for, open_db};
use chrono::{DateTime, Utc};
use reqwest::Client;
use rusqlite::{OptionalExtension, params};
use serde::Deserialize;
use std::{
    collections::{BTreeMap, HashMap},
    path::PathBuf,
    time::Duration,
};

const VM_URL: &str = "http://127.0.0.1:8428";
const DELETE_MATCH: &str = "{__name__=~\"boaz_health_v1_.*\",device_id=\"";

#[derive(Clone)]
pub struct VmConfig {
    pub binary: PathBuf,
    pub storage: PathBuf,
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
pub fn native_vm_verified(config: &VmConfig) -> bool {
    let expected_binary = match config.binary.canonicalize() {
        Ok(path) => path,
        Err(_) => return false,
    };
    let expected_storage = match config.storage.canonicalize() {
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
            if parts.len() > 9 && parts[1] == "0100007F:20EC" && parts[3] == "0A" {
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
            .any(|arg| arg == "-httpListenAddr=127.0.0.1:8428");
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

fn selector(device_id: &str, metric_name: &str) -> String {
    format!("{{__name__=\"{metric_name}\",device_id=\"{device_id}\"}}")
}

async fn delete_series(client: &Client, selector: &str) -> Result<(), String> {
    let response = client
        .post(format!("{VM_URL}/api/v1/admin/tsdb/delete_series"))
        .query(&[("match[]", selector)])
        .send()
        .await
        .map_err(error_message)?;
    if !response.status().is_success() {
        return Err("metrics_delete_failed".to_owned());
    }
    Ok(())
}

async fn import_lines(client: &Client, lines: &[String]) -> Result<(), String> {
    let mut block = String::new();
    for line in lines {
        if block.len() + line.len() > 128 * 1024 && !block.is_empty() {
            let response = client
                .post(format!("{VM_URL}/api/v1/import/prometheus"))
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
            .post(format!("{VM_URL}/api/v1/import/prometheus"))
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
    if export.metric.get("device_id").map(String::as_str) != Some(device_id)
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
    selector: &str,
    device_id: &str,
    metric_name: Option<&str>,
    expected: &BTreeMap<i64, f64>,
) -> Result<bool, String> {
    let mut response = client
        .get(format!("{VM_URL}/api/v1/export"))
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

async fn confirmed_readback(
    client: &Client,
    selector: &str,
    device_id: &str,
    metric_name: Option<&str>,
    expected: &BTreeMap<i64, f64>,
) -> Result<(), String> {
    for attempt in 0..5 {
        if export_matches(client, selector, device_id, metric_name, expected).await? {
            return Ok(());
        }
        if attempt < 4 {
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
    }
    Err("metrics_readback_mismatch".to_owned())
}

pub async fn project_once(state: &ServerState, config: &VmConfig) -> Result<bool, String> {
    if state
        .runtime_guard
        .as_ref()
        .is_some_and(|guard| !guard.verified())
    {
        return Err("runtime_gate_closed".to_owned());
    }
    if !native_vm_verified(config) {
        return Err("native_metrics_identity_unverified".to_owned());
    }
    let connection = open_db(&state.db_path).map_err(error_message)?;
    let client = Client::builder()
        .timeout(Duration::from_secs(5))
        .build()
        .map_err(error_message)?;
    let pending_erasure: Option<String> = connection.query_row(
        "SELECT device_id FROM erasures WHERE metrics_deleted_at IS NULL ORDER BY requested_at LIMIT 1",
        [], |row| row.get(0),
    ).optional().map_err(error_message)?;
    if let Some(device_id) = pending_erasure {
        let match_selector = format!("{DELETE_MATCH}{device_id}\"}}");
        let result = async {
            delete_series(&client, &match_selector).await?;
            confirmed_readback(&client, &match_selector, &device_id, None, &BTreeMap::new()).await
        }
        .await;
        match result {
            Ok(()) => {
                connection.execute("UPDATE erasures SET device_id=?2,metrics_deleted_at=?3,last_error=NULL WHERE device_id=?1", params![device_id,crate::digest(device_id.as_bytes()),Utc::now().to_rfc3339()]).map_err(error_message)?;
            }
            Err(error) => {
                connection
                    .execute(
                        "UPDATE erasures SET last_error=?2 WHERE device_id=?1",
                        params![device_id, error],
                    )
                    .map_err(error_message)?;
                return Err(error);
            }
        }
        return Ok(true);
    }
    let job: Option<(String, String, i64)> = connection.query_row(
        "SELECT device_id,metric_name,max(id) FROM outbox WHERE processed_at IS NULL GROUP BY device_id,metric_name ORDER BY min(id) LIMIT 1",
        [], |row| Ok((row.get(0)?,row.get(1)?,row.get(2)?)),
    ).optional().map_err(error_message)?;
    let Some((device_id, metric_name, max_id)) = job else {
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
        delete_series(&client, &match_selector).await?;
        import_lines(&client, &lines).await?;
        confirmed_readback(
            &client,
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
            connection.execute("UPDATE outbox SET processed_at=?1,last_error=NULL WHERE device_id=?2 AND metric_name=?3 AND id<=?4 AND processed_at IS NULL", params![now,device_id,metric_name,max_id]).map_err(error_message)?;
            connection.execute("UPDATE receipts SET projected_at=?1 WHERE device_id=?2 AND requires_projection=1 AND projected_at IS NULL AND NOT EXISTS (SELECT 1 FROM outbox WHERE outbox.batch_id=receipts.batch_id AND outbox.processed_at IS NULL)", params![now,device_id]).map_err(error_message)?;
            Ok(true)
        }
        Err(error) => {
            connection.execute("UPDATE outbox SET attempts=attempts+1,last_error=?1 WHERE device_id=?2 AND metric_name=?3 AND id<=?4 AND processed_at IS NULL", params![error,device_id,metric_name,max_id]).map_err(error_message)?;
            Err(error)
        }
    }
}

pub async fn run_worker(state: ServerState, config: VmConfig) {
    loop {
        let _ = project_once(&state, &config).await;
        tokio::time::sleep(Duration::from_secs(5)).await;
    }
}
