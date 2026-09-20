//! An authenticated, off-host control-head boundary.
//!
//! A local JSON file is not a custody provider. This module intentionally has
//! no fallback provider: callers must configure a separately administered SSH
//! endpoint before relying on a head for serving or activating a generation.

use crate::control::ControlCheckpoint;
use fs2::FileExt;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{
    collections::HashSet,
    error::Error,
    fmt, fs,
    io::{self, Read, Write},
    os::fd::AsRawFd,
    path::{Component, Path, PathBuf},
    process::{Command, Stdio},
    thread,
    time::{Duration, Instant},
};
use uuid::Uuid;

#[derive(Debug)]
pub enum CustodyError {
    Invalid(&'static str),
    Io(std::io::Error),
    Protocol(String),
    Json(serde_json::Error),
}

impl fmt::Display for CustodyError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Invalid(message) => write!(f, "invalid custody configuration: {message}"),
            Self::Io(error) => write!(f, "custody transport failed: {error}"),
            Self::Protocol(message) => write!(f, "custody rejected request: {message}"),
            Self::Json(error) => write!(f, "custody response is invalid: {error}"),
        }
    }
}

impl Error for CustodyError {}

impl From<std::io::Error> for CustodyError {
    fn from(value: std::io::Error) -> Self {
        Self::Io(value)
    }
}

impl From<serde_json::Error> for CustodyError {
    fn from(value: serde_json::Error) -> Self {
        Self::Json(value)
    }
}

pub type CustodyResult<T> = Result<T, CustodyError>;

/// A checkpoint of the *entire* acknowledgement journal head, not just its
/// largest confirmed batch. The off-host owner stores only a digest and
/// counters; raw health requests remain in the separate encrypted journal.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct AckCheckpoint {
    pub journal_id: String,
    pub sequence: u64,
    pub confirmation_sequence: u64,
    pub pairing_confirmation_sequence: u64,
    pub head_sha256: String,
}

/// Version 2 is one monotonic custody revision for both independent ledgers.
/// `None` is permitted only before the one-time generation-0 baseline seal.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct CustodyState {
    pub format: u8,
    pub revision: u64,
    pub control: ControlCheckpoint,
    pub ack: Option<AckCheckpoint>,
    pub baseline_sha256: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct CustodyReservationV2 {
    pub reservation_id: String,
    pub predecessor: CustodyState,
    pub operation_id: String,
    pub intent_sha256: String,
}

/// Production uses `SshCustody`. A synthetic implementation belongs only in
/// test code; callers must not silently substitute a local JSON fixture.
pub trait CustodyClient: Send + Sync {
    fn read_v2(&self, store_id: &str) -> CustodyResult<CustodyState>;
    fn reserve_v2(
        &self,
        predecessor: &CustodyState,
        operation_id: &str,
        intent_sha256: &str,
    ) -> CustodyResult<CustodyReservationV2>;
    fn compare_and_swap_v2(
        &self,
        reservation: &CustodyReservationV2,
        successor: &CustodyState,
    ) -> CustodyResult<CustodyState>;
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct Reservation {
    pub reservation_id: String,
    pub predecessor: ControlCheckpoint,
    pub event_id: String,
    pub intent_sha256: String,
}

#[derive(Serialize)]
#[serde(tag = "op", rename_all = "snake_case")]
enum Request<'a> {
    Read {
        store_id: &'a str,
    },
    Reserve {
        predecessor: &'a ControlCheckpoint,
        event_id: &'a str,
        intent_sha256: &'a str,
    },
    CompareAndSwap {
        reservation: &'a Reservation,
        successor: &'a ControlCheckpoint,
    },
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Response {
    checkpoint: ControlCheckpoint,
    reservation: Option<Reservation>,
}

#[derive(Serialize)]
#[serde(tag = "op", rename_all = "snake_case")]
enum RequestV2<'a> {
    ReadV2 {
        store_id: &'a str,
    },
    ReserveV2 {
        predecessor: &'a CustodyState,
        operation_id: &'a str,
        intent_sha256: &'a str,
    },
    CompareAndSwapV2 {
        reservation: &'a CustodyReservationV2,
        successor: &'a CustodyState,
    },
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ResponseV2 {
    state: CustodyState,
    reservation: Option<CustodyReservationV2>,
}

#[derive(Debug, Clone)]
pub struct SshCustody {
    host: String,
    pinned_known_hosts: PathBuf,
    identity_file: PathBuf,
}

impl SshCustody {
    pub fn new(
        host: String,
        pinned_known_hosts: PathBuf,
        identity_file: PathBuf,
    ) -> CustodyResult<Self> {
        if host.split('@').count() != 2
            || host.split('@').any(str::is_empty)
            || host.starts_with('-')
            || !host
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || b"@.-_".contains(&byte))
        {
            return Err(CustodyError::Invalid(
                "SSH target must be a fixed user@host without options",
            ));
        }
        validate_ssh_executable()?;
        validate_private_file(&pinned_known_hosts)?;
        validate_private_file(&identity_file)?;
        Ok(Self {
            host,
            pinned_known_hosts,
            identity_file,
        })
    }

    fn invoke<I: Serialize, O: DeserializeOwned>(&self, request: &I) -> CustodyResult<O> {
        // The remote account must restrict this key to one audited forced
        // command. No shell interpolation is used for local arguments.
        let input = serde_json::to_vec(request)?;
        if input.len() > 4096 {
            return Err(CustodyError::Invalid("custody request is too large"));
        }
        // The receiver must not let PATH select a different program. The
        // deployment also has to make this executable and all credential
        // ancestors root-controlled; a root compromise is outside this check.
        validate_ssh_executable()?;
        validate_private_file(&self.pinned_known_hosts)?;
        validate_private_file(&self.identity_file)?;
        let mut command = self.ssh_command();
        let output = run_custody_command(&mut command, &input, Duration::from_secs(30))?;
        Ok(serde_json::from_slice(&output)?)
    }

    fn ssh_command(&self) -> Command {
        let mut command = Command::new(ssh_executable());
        // Do not inherit loader hooks, SSH_AUTH_SOCK, proxy variables or PATH
        // from the receiver's environment. The audited forced-command key is
        // the only client identity used for this protocol.
        command.env_clear();
        command
            .arg("-T")
            .arg("-F")
            .arg("/dev/null")
            .arg("-oBatchMode=yes")
            .arg("-oIdentitiesOnly=yes")
            .arg("-oIdentityAgent=none")
            .arg("-oStrictHostKeyChecking=yes")
            .arg("-oConnectTimeout=10")
            .arg("-oConnectionAttempts=1")
            .arg("-oServerAliveInterval=10")
            .arg("-oServerAliveCountMax=2")
            .arg(format!(
                "-oUserKnownHostsFile={}",
                self.pinned_known_hosts.display()
            ))
            .arg("-i")
            .arg(&self.identity_file)
            .arg("--")
            .arg(&self.host)
            .arg("boaz-health-custody-protocol");
        command
    }

    pub fn read(&self, store_id: &str) -> CustodyResult<ControlCheckpoint> {
        let response: Response = self.invoke(&Request::Read { store_id })?;
        if response.checkpoint.store_id != store_id || response.reservation.is_some() {
            return Err(CustodyError::Protocol("head identity mismatch".to_owned()));
        }
        validate_checkpoint(&response.checkpoint)?;
        Ok(response.checkpoint)
    }

    pub fn reserve(
        &self,
        predecessor: &ControlCheckpoint,
        event_id: &str,
        intent_sha256: &str,
    ) -> CustodyResult<Reservation> {
        validate_checkpoint(predecessor)?;
        validate_hash(intent_sha256)?;
        validate_identifier(event_id)?;
        let response: Response = self.invoke(&Request::Reserve {
            predecessor,
            event_id,
            intent_sha256,
        })?;
        let reservation = response
            .reservation
            .ok_or(CustodyError::Protocol("reservation missing".to_owned()))?;
        if response.checkpoint != *predecessor
            || reservation.predecessor != *predecessor
            || reservation.event_id != event_id
            || reservation.intent_sha256 != intent_sha256
        {
            return Err(CustodyError::Protocol(
                "reservation differs from request".to_owned(),
            ));
        }
        Ok(reservation)
    }

    pub fn compare_and_swap(
        &self,
        reservation: &Reservation,
        successor: &ControlCheckpoint,
    ) -> CustodyResult<ControlCheckpoint> {
        validate_checkpoint(successor)?;
        if successor.store_id != reservation.predecessor.store_id
            || reservation.predecessor.sequence.checked_add(1) != Some(successor.sequence)
            || successor.current_hash == reservation.predecessor.current_hash
        {
            return Err(CustodyError::Invalid(
                "successor does not advance the reserved head",
            ));
        }
        let response: Response = self.invoke(&Request::CompareAndSwap {
            reservation,
            successor,
        })?;
        if response.checkpoint != *successor || response.reservation.is_some() {
            return Err(CustodyError::Protocol(
                "CAS acknowledgement differs from successor".to_owned(),
            ));
        }
        Ok(response.checkpoint)
    }
}

fn run_custody_command(
    command: &mut Command,
    input: &[u8],
    deadline: Duration,
) -> CustodyResult<Vec<u8>> {
    // Begin the budget before spawn. spawn(2) and kernel D-state waits cannot
    // themselves be interrupted here; an external service watchdog is still
    // required for an absolute wall-clock bound.
    let stop_at = Instant::now()
        .checked_add(deadline)
        .ok_or(CustodyError::Invalid("custody deadline is invalid"))?;
    #[cfg(unix)]
    {
        use std::os::unix::process::CommandExt;
        command.process_group(0);
    }
    let mut child = command
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()?;
    let mut leader_reaped = false;
    let result = communicate_with_deadline(&mut child, input, stop_at, &mut leader_reaped);
    if result.is_err() {
        // Kill the process group as an SSH wrapper may have left descendants
        // holding stdout open. Never signal a process group by a PID whose
        // leader was already reaped: that identifier may have been recycled.
        if !leader_reaped {
            #[cfg(unix)]
            unsafe {
                libc::kill(
                    -i32::try_from(child.id()).unwrap_or(i32::MAX),
                    libc::SIGKILL,
                );
            }
            let _ = child.kill();
        }
        let cleanup_until = Instant::now() + Duration::from_millis(200);
        loop {
            match child.try_wait() {
                Ok(Some(_)) => break,
                Ok(None) if Instant::now() < cleanup_until => {
                    thread::sleep(Duration::from_millis(10));
                }
                _ => {
                    // The request is already failed closed. A background
                    // reaper avoids blocking this caller if waitpid stalls.
                    thread::spawn(move || {
                        let _ = child.wait();
                    });
                    return result;
                }
            }
        }
    }
    result
}

fn communicate_with_deadline(
    child: &mut std::process::Child,
    input: &[u8],
    deadline: Instant,
    leader_reaped: &mut bool,
) -> CustodyResult<Vec<u8>> {
    let mut stdin = Some(
        child
            .stdin
            .take()
            .ok_or(CustodyError::Invalid("custody stdin unavailable"))?,
    );
    let mut stdout = child
        .stdout
        .take()
        .ok_or(CustodyError::Invalid("custody stdout unavailable"))?;
    set_nonblocking(stdin.as_ref().unwrap().as_raw_fd())?;
    set_nonblocking(stdout.as_raw_fd())?;
    let mut written = 0;
    let mut output = Vec::new();
    let mut stdout_closed = false;
    let mut exit_status = None;

    loop {
        if Instant::now() >= deadline {
            return Err(CustodyError::Io(io::Error::new(
                io::ErrorKind::TimedOut,
                "custody command exceeded its overall deadline",
            )));
        }

        if let Some(pipe) = stdin.as_mut() {
            if written == input.len() {
                stdin = None;
            } else {
                match pipe.write(&input[written..]) {
                    Ok(0) => {
                        return Err(CustodyError::Io(io::Error::new(
                            io::ErrorKind::WriteZero,
                            "custody command stopped accepting input",
                        )));
                    }
                    Ok(count) => written += count,
                    Err(error) if error.kind() == io::ErrorKind::WouldBlock => {}
                    Err(error) if error.kind() == io::ErrorKind::Interrupted => continue,
                    Err(error) => return Err(CustodyError::Io(error)),
                }
                if written == input.len() {
                    stdin = None;
                }
            }
        }

        if !stdout_closed {
            let mut chunk = [0_u8; 1024];
            loop {
                let available = (4097 - output.len()).min(chunk.len());
                match stdout.read(&mut chunk[..available]) {
                    Ok(0) => {
                        stdout_closed = true;
                        break;
                    }
                    Ok(count) => {
                        output.extend_from_slice(&chunk[..count]);
                        if output.len() > 4096 {
                            return Err(CustodyError::Protocol("oversized response".to_owned()));
                        }
                    }
                    Err(error) if error.kind() == io::ErrorKind::WouldBlock => break,
                    Err(error) if error.kind() == io::ErrorKind::Interrupted => continue,
                    Err(error) => return Err(CustodyError::Io(error)),
                }
            }
        }

        if stdout_closed && exit_status.is_none() {
            exit_status = child.try_wait()?;
            *leader_reaped = exit_status.is_some();
        }
        if stdout_closed && let Some(status) = exit_status {
            if !status.success() {
                return Err(CustodyError::Protocol("off-host command failed".to_owned()));
            }
            return Ok(output);
        }

        let remaining = deadline.saturating_duration_since(Instant::now());
        let wait_ms = remaining.as_millis().clamp(1, 100) as i32;
        let mut descriptors = Vec::with_capacity(2);
        if let Some(pipe) = stdin.as_ref() {
            descriptors.push(libc::pollfd {
                fd: pipe.as_raw_fd(),
                events: libc::POLLOUT,
                revents: 0,
            });
        }
        if !stdout_closed {
            descriptors.push(libc::pollfd {
                fd: stdout.as_raw_fd(),
                events: libc::POLLIN,
                revents: 0,
            });
        }
        // Both pipes are nonblocking; poll only bounds idle time. The absolute
        // deadline also covers an SSH peer that keeps sending tiny fragments.
        let status = unsafe {
            libc::poll(
                descriptors.as_mut_ptr(),
                descriptors.len() as libc::nfds_t,
                wait_ms,
            )
        };
        if status < 0 {
            let error = io::Error::last_os_error();
            if error.kind() != io::ErrorKind::Interrupted {
                return Err(CustodyError::Io(error));
            }
        }
    }
}

fn set_nonblocking(fd: libc::c_int) -> io::Result<()> {
    let flags = unsafe { libc::fcntl(fd, libc::F_GETFL) };
    if flags < 0 {
        return Err(io::Error::last_os_error());
    }
    if unsafe { libc::fcntl(fd, libc::F_SETFL, flags | libc::O_NONBLOCK) } < 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(())
}

impl CustodyClient for SshCustody {
    fn read_v2(&self, store_id: &str) -> CustodyResult<CustodyState> {
        validate_identifier(store_id)?;
        let response: ResponseV2 = self.invoke(&RequestV2::ReadV2 { store_id })?;
        validate_state_v2(&response.state)?;
        if response.state.control.store_id != store_id || response.reservation.is_some() {
            return Err(CustodyError::Protocol("v2 state identity mismatch".into()));
        }
        Ok(response.state)
    }

    fn reserve_v2(
        &self,
        predecessor: &CustodyState,
        operation_id: &str,
        intent_sha256: &str,
    ) -> CustodyResult<CustodyReservationV2> {
        validate_state_v2(predecessor)?;
        validate_identifier(operation_id)?;
        validate_hash(intent_sha256)?;
        let response: ResponseV2 = self.invoke(&RequestV2::ReserveV2 {
            predecessor,
            operation_id,
            intent_sha256,
        })?;
        let reservation = response
            .reservation
            .ok_or(CustodyError::Protocol("v2 reservation missing".into()))?;
        if response.state != *predecessor
            || reservation.predecessor != *predecessor
            || reservation.operation_id != operation_id
            || reservation.intent_sha256 != intent_sha256
        {
            return Err(CustodyError::Protocol("v2 reservation differs".into()));
        }
        validate_reservation_v2(&reservation)?;
        Ok(reservation)
    }

    fn compare_and_swap_v2(
        &self,
        reservation: &CustodyReservationV2,
        successor: &CustodyState,
    ) -> CustodyResult<CustodyState> {
        validate_advance_v2(reservation, successor)?;
        let response: ResponseV2 = self.invoke(&RequestV2::CompareAndSwapV2 {
            reservation,
            successor,
        })?;
        if response.state != *successor || response.reservation.is_some() {
            return Err(CustodyError::Protocol(
                "v2 CAS acknowledgement differs".into(),
            ));
        }
        Ok(response.state)
    }
}

fn validate_hash(value: &str) -> CustodyResult<()> {
    if value.len() != 64 || !value.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        return Err(CustodyError::Invalid("SHA-256 must be 64 hex characters"));
    }
    Ok(())
}

fn validate_checkpoint(checkpoint: &ControlCheckpoint) -> CustodyResult<()> {
    if validate_identifier(&checkpoint.store_id).is_err() || checkpoint.sequence < 0 {
        return Err(CustodyError::Invalid("control head identity is invalid"));
    }
    validate_hash(&checkpoint.current_hash)
}

fn ssh_executable() -> &'static Path {
    Path::new("/usr/bin/ssh")
}

fn validate_ssh_executable() -> CustodyResult<()> {
    validate_root_controlled_file(ssh_executable(), true)
}

fn validate_private_file(path: &Path) -> CustodyResult<()> {
    validate_root_controlled_file(path, false)
}

fn validate_root_controlled_file(path: &Path, executable: bool) -> CustodyResult<()> {
    if !path.is_absolute()
        || path
            .components()
            .any(|component| matches!(component, Component::ParentDir | Component::CurDir))
    {
        return Err(CustodyError::Invalid(
            "custody key paths must be absolute and normalized",
        ));
    }
    // A receiver-UID attacker can replace even a mode-0400 leaf when its
    // directory is writable. Check every ancestor, including `/`, and require
    // a separate privileged owner. This is an operational permission contract,
    // not a defense against privileged replacement after this check.
    let mut parent = path.parent();
    while let Some(directory) = parent {
        let metadata = fs::symlink_metadata(directory)?;
        if !metadata.file_type().is_dir() {
            return Err(CustodyError::Invalid(
                "custody path has a symlink or non-directory ancestor",
            ));
        }
        #[cfg(unix)]
        {
            use std::os::unix::fs::{MetadataExt, PermissionsExt};
            if metadata.uid() != 0 || metadata.permissions().mode() & 0o022 != 0 {
                return Err(CustodyError::Invalid(
                    "custody path ancestor is writable by the receiver",
                ));
            }
        }
        parent = directory.parent();
    }
    let metadata = fs::symlink_metadata(path)?;
    if !metadata.file_type().is_file() || metadata.len() == 0 {
        return Err(CustodyError::Invalid(
            "custody key path is not a regular nonempty file",
        ));
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::{MetadataExt, PermissionsExt};
        if metadata.uid() != 0
            || metadata.nlink() != 1
            || metadata.permissions().mode() & 0o022 != 0
            || (executable && metadata.permissions().mode() & 0o111 == 0)
        {
            return Err(CustodyError::Invalid(
                "custody executable or key is not root-controlled",
            ));
        }
    }
    Ok(())
}

// The forced-command account owns an independently administered, encrypted
// directory. A request never supplies a path. The off-host store guarantees
// ordered, durable head changes; the caller still verifies the full control
// event hash chain because this store receives only its intent digest.
#[derive(Deserialize)]
#[serde(tag = "op", rename_all = "snake_case", deny_unknown_fields)]
enum Incoming {
    Read {
        store_id: String,
    },
    Reserve {
        predecessor: ControlCheckpoint,
        event_id: String,
        intent_sha256: String,
    },
    CompareAndSwap {
        reservation: Reservation,
        successor: ControlCheckpoint,
    },
    ReadV2 {
        store_id: String,
    },
    ReserveV2 {
        predecessor: CustodyState,
        operation_id: String,
        intent_sha256: String,
    },
    CompareAndSwapV2 {
        reservation: CustodyReservationV2,
        successor: CustodyState,
    },
}

impl Incoming {
    fn is_v2(&self) -> bool {
        matches!(
            self,
            Self::ReadV2 { .. } | Self::ReserveV2 { .. } | Self::CompareAndSwapV2 { .. }
        )
    }
}

#[derive(Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
struct Committed {
    reservation: Reservation,
    successor: ControlCheckpoint,
}

const MAX_MESSAGE: u64 = 4096;
const HEAD: &str = "head.json";
const ANCHOR: &str = "anchor.json";
const PENDING: &str = "reservation.json";
const LOCK: &str = "custody.lock";
const HISTORY: &str = "history";
const V2_ANCHOR: &str = "state-v2.anchor.json";
const V2_HEAD: &str = "state-v2.head.json";
const V2_HISTORY_SEAL: &str = "state-v2.history-seal.json";
const V2_PENDING: &str = "state-v2.reservation.json";
const V2_HISTORY: &str = "state-v2.history";

/// Offline, one-time initialization. This is never a request operation.
/// A nonempty or partly initialized directory is left untouched.
pub fn initialize_off_host(root: &Path, anchor: &ControlCheckpoint) -> CustodyResult<()> {
    validate_checkpoint(anchor)?;
    validate_root(root)?;
    if fs::read_dir(root)?.next().is_some() {
        return Err(CustodyError::Protocol("custody root is not empty".into()));
    }
    let history = root.join(HISTORY);
    fs::create_dir(&history)?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(&history, fs::Permissions::from_mode(0o700))?;
    }
    create_private(&root.join(LOCK), b"")?;
    let initial = serde_json::to_vec(anchor)?;
    create_private(&root.join(ANCHOR), &initial)?;
    create_private(&root.join(HEAD), &initial)?;
    sync_dir(&history)?;
    sync_dir(root)
}

/// Initialize a new dual-ledger custodian. This is an offline administrative
/// operation, not an SSH request; there is deliberately no runtime bootstrap.
pub fn initialize_off_host_v2(root: &Path, control: &ControlCheckpoint) -> CustodyResult<()> {
    validate_checkpoint(control)?;
    validate_root(root)?;
    if fs::read_dir(root)?.next().is_some() {
        return Err(CustodyError::Protocol("custody root is not empty".into()));
    }
    create_private(&root.join(LOCK), b"")?;
    create_v2_state(root, control.clone(), None).map(|_| ())
}

/// Explicit, offline migration of a *settled* v1 chain. The old files remain
/// immutable and are checked on every v2 access; any pending v1 reservation
/// or partial v2 publication fails closed, rather than being guessed away.
pub fn migrate_off_host_v1_to_v2(root: &Path) -> CustodyResult<CustodyState> {
    validate_root(root)?;
    let lock = open_private(&root.join(LOCK))?;
    lock.lock_exclusive()?;
    let result = (|| {
        if v2_present(root)? {
            return Err(CustodyError::Protocol("v2 files already exist".into()));
        }
        validate_legacy_root_inventory(root)?;
        let legacy = Store::load(root)?;
        if legacy.pending.is_some() {
            return Err(CustodyError::Protocol(
                "v1 reservation is unresolved".into(),
            ));
        }
        let old_tip = legacy.head;
        create_v2_state(root, old_tip.clone(), Some(old_tip))
    })();
    FileExt::unlock(&lock)?;
    result
}

fn create_v2_state(
    root: &Path,
    control: ControlCheckpoint,
    legacy_tip: Option<ControlCheckpoint>,
) -> CustodyResult<CustodyState> {
    let state = CustodyState {
        format: 2,
        revision: 0,
        control,
        ack: None,
        baseline_sha256: None,
    };
    let anchor = AnchorV2 {
        format: 2,
        genesis: state.clone(),
        legacy_tip,
    };
    let history = root.join(V2_HISTORY);
    fs::create_dir(&history)?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(&history, fs::Permissions::from_mode(0o700))?;
    }
    create_private(&root.join(V2_ANCHOR), &serde_json::to_vec(&anchor)?)?;
    create_private(&root.join(V2_HEAD), &serde_json::to_vec(&state)?)?;
    let seal = HistorySealV2 {
        format: 1,
        revision: 0,
        head_sha256: history_genesis_hash(&anchor)?,
        latest: None,
    };
    create_private(&root.join(V2_HISTORY_SEAL), &serde_json::to_vec(&seal)?)?;
    sync_dir(&history)?;
    sync_dir(root)?;
    Ok(state)
}

fn v2_present(root: &Path) -> CustodyResult<bool> {
    for name in [V2_ANCHOR, V2_HEAD, V2_HISTORY_SEAL, V2_PENDING, V2_HISTORY] {
        match fs::symlink_metadata(root.join(name)) {
            Ok(_) => return Ok(true),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(error.into()),
        }
    }
    Ok(false)
}

fn validate_legacy_root_inventory(root: &Path) -> CustodyResult<()> {
    for item in fs::read_dir(root)? {
        let name = item?.file_name();
        let name = name
            .to_str()
            .ok_or_else(|| CustodyError::Protocol("invalid legacy custody name".into()))?;
        if !matches!(name, LOCK | ANCHOR | HEAD | PENDING | HISTORY) {
            return Err(CustodyError::Protocol(
                "unknown legacy custody root file".into(),
            ));
        }
    }
    // The v1 wire type predates a format tag. Its exact field set is the only
    // legacy layout we recognize for a no-loss v2 migration.
    let _: StrictLegacyCheckpoint = read_json(&root.join(ANCHOR))?;
    let _: StrictLegacyCheckpoint = read_json(&root.join(HEAD))?;
    let history = root.join(HISTORY);
    validate_private_dir(&history)?;
    for entry in fs::read_dir(history)? {
        let _: StrictLegacyCommitted = read_json(&entry?.path())?;
    }
    if fs::symlink_metadata(root.join(PENDING)).is_ok() {
        let _: StrictLegacyReservation = read_json(&root.join(PENDING))?;
    }
    Ok(())
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
#[allow(dead_code)]
struct StrictLegacyCheckpoint {
    store_id: String,
    sequence: i64,
    current_hash: String,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
#[allow(dead_code)]
struct StrictLegacyReservation {
    reservation_id: String,
    predecessor: StrictLegacyCheckpoint,
    event_id: String,
    intent_sha256: String,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
#[allow(dead_code)]
struct StrictLegacyCommitted {
    reservation: StrictLegacyReservation,
    successor: StrictLegacyCheckpoint,
}

/// Entrypoint intended solely for an authorized_keys forced command.
/// The binary, SSH key, account and directory must be administered separately
/// from the receiver; a local copy of these files is not independent custody.
pub fn run_forced_command(root: &Path) -> CustodyResult<()> {
    if std::env::var("SSH_ORIGINAL_COMMAND").as_deref() != Ok("boaz-health-custody-protocol") {
        return Err(CustodyError::Protocol(
            "unexpected SSH original command".into(),
        ));
    }
    let mut input = Vec::new();
    std::io::stdin()
        .take(MAX_MESSAGE + 1)
        .read_to_end(&mut input)?;
    if input.is_empty() || input.len() as u64 > MAX_MESSAGE {
        return Err(CustodyError::Protocol("invalid request size".into()));
    }
    let request: Incoming = serde_json::from_slice(&input)?;
    let output = if request.is_v2() {
        serde_json::to_vec(&process_forced_request_v2(root, request)?)?
    } else {
        serde_json::to_vec(&process_forced_request_legacy(root, request)?)?
    };
    if output.len() as u64 > MAX_MESSAGE {
        return Err(CustodyError::Protocol("oversized response".into()));
    }
    std::io::stdout().write_all(&output)?;
    Ok(())
}

#[cfg(test)]
fn process_forced_request(root: &Path, input: &[u8]) -> CustodyResult<Response> {
    if input.is_empty() || input.len() as u64 > MAX_MESSAGE {
        return Err(CustodyError::Protocol("invalid request size".into()));
    }
    let request: Incoming = serde_json::from_slice(input)?;
    process_forced_request_legacy(root, request)
}

fn process_forced_request_legacy(root: &Path, request: Incoming) -> CustodyResult<Response> {
    if request.is_v2() {
        return Err(CustodyError::Protocol(
            "v2 operation sent to legacy entry".into(),
        ));
    }
    validate_root(root)?;
    let lock = open_private(&root.join(LOCK))?;
    lock.lock_exclusive()?;
    let result = if v2_present(root)? {
        Err(CustodyError::Protocol(
            "legacy protocol disabled after v2 adoption".into(),
        ))
    } else {
        Store::load(root).and_then(|mut store| store.apply(request))
    };
    FileExt::unlock(&lock)?;
    result
}

fn process_forced_request_v2(root: &Path, request: Incoming) -> CustodyResult<ResponseV2> {
    if !request.is_v2() {
        return Err(CustodyError::Protocol(
            "legacy operation sent to v2 entry".into(),
        ));
    }
    validate_root(root)?;
    let lock = open_private(&root.join(LOCK))?;
    lock.lock_exclusive()?;
    let result = settle_v2_temporary_for_request(root, &request)
        .and_then(|()| StoreV2::load(root))
        .and_then(|mut store| store.apply(request));
    FileExt::unlock(&lock)?;
    result
}

/// A SIGKILL can leave the fsynced temporary file from an atomic publish.
/// Only the *same* reservation/request may discard a proven, uncommitted
/// temporary and retry. Reads, unrelated operations, ambiguous names and
/// divergent bytes leave the store closed. In particular, an old anonymous
/// `.pending-<uuid>` file is deliberately not guessed into a target.
fn settle_v2_temporary_for_request(root: &Path, request: &Incoming) -> CustodyResult<()> {
    let history = root.join(V2_HISTORY);
    validate_private_dir(&history)?;
    let mut temporaries = Vec::new();
    for directory in [root, history.as_path()] {
        for entry in fs::read_dir(directory)? {
            let entry = entry?;
            let name = entry.file_name();
            let name = name
                .to_str()
                .ok_or_else(|| CustodyError::Protocol("invalid temporary name".into()))?;
            if name.starts_with(".pending-") {
                temporaries.push(entry.path());
            }
        }
    }
    if temporaries.is_empty() {
        return Ok(());
    }
    if temporaries.len() != 1 {
        return Err(CustodyError::Protocol(
            "ambiguous custody temporary files".into(),
        ));
    }
    let temporary = &temporaries[0];
    let name = temporary
        .file_name()
        .and_then(|name| name.to_str())
        .ok_or_else(|| CustodyError::Protocol("invalid temporary name".into()))?;
    let expected = match request {
        Incoming::ReserveV2 {
            predecessor,
            operation_id,
            intent_sha256,
        } if temporary.parent() == Some(root) && temporary_name_matches(name, V2_PENDING) => {
            validate_state_v2(predecessor)?;
            validate_identifier(operation_id)?;
            validate_hash(intent_sha256)?;
            if fs::symlink_metadata(root.join(V2_PENDING)).is_ok()
                || read_json::<CustodyState>(&root.join(V2_HEAD))? != *predecessor
            {
                return Err(CustodyError::Protocol(
                    "v2 reservation temporary conflicts".into(),
                ));
            }
            let bytes = read_temporary_bytes(temporary, None)?;
            let reservation: CustodyReservationV2 = serde_json::from_slice(&bytes)?;
            validate_reservation_v2(&reservation)?;
            if reservation.predecessor != *predecessor
                || reservation.operation_id != *operation_id
                || reservation.intent_sha256 != *intent_sha256
                || bytes != serde_json::to_vec(&reservation)?
            {
                return Err(CustodyError::Protocol(
                    "v2 reservation temporary differs".into(),
                ));
            }
            bytes
        }
        Incoming::CompareAndSwapV2 {
            reservation,
            successor,
        } => {
            validate_advance_v2(reservation, successor)?;
            if read_json::<CustodyReservationV2>(&root.join(V2_PENDING))? != *reservation {
                return Err(CustodyError::Protocol(
                    "v2 CAS temporary lacks its reservation".into(),
                ));
            }
            let record = CommittedV2 {
                reservation: reservation.clone(),
                successor: successor.clone(),
            };
            let mirror_name = format!("{:020}.json", successor.revision);
            let mirror = history.join(&mirror_name);
            if temporary.parent() == Some(history.as_path())
                && temporary_name_matches(name, &mirror_name)
            {
                let bytes = read_temporary_bytes(temporary, Some(&mirror))?;
                if bytes != serde_json::to_vec(&record)? {
                    return Err(CustodyError::Protocol("v2 mirror temporary differs".into()));
                }
                bytes
            } else if temporary.parent() == Some(root)
                && temporary_name_matches(name, V2_HISTORY_SEAL)
            {
                let prior: HistorySealV2 = read_json(&root.join(V2_HISTORY_SEAL))?;
                if prior.revision.checked_add(1) != Some(successor.revision) {
                    return Err(CustodyError::Protocol("v2 seal temporary is stale".into()));
                }
                let next = HistorySealV2 {
                    format: 1,
                    revision: successor.revision,
                    head_sha256: history_next_hash(&prior.head_sha256, &record)?,
                    latest: Some(record),
                };
                let bytes = read_temporary_bytes(temporary, None)?;
                if bytes != serde_json::to_vec(&next)? {
                    return Err(CustodyError::Protocol("v2 seal temporary differs".into()));
                }
                bytes
            } else if temporary.parent() == Some(root) && temporary_name_matches(name, V2_HEAD) {
                let bytes = read_temporary_bytes(temporary, None)?;
                if bytes != serde_json::to_vec(successor)? {
                    return Err(CustodyError::Protocol("v2 head temporary differs".into()));
                }
                bytes
            } else {
                return Err(CustodyError::Protocol("unknown v2 temporary".into()));
            }
        }
        _ => return Err(CustodyError::Protocol("unknown v2 temporary".into())),
    };
    if expected.is_empty() {
        return Err(CustodyError::Protocol("empty v2 temporary".into()));
    }
    fs::remove_file(temporary)?;
    sync_dir(
        temporary
            .parent()
            .ok_or(CustodyError::Invalid("missing parent"))?,
    )
}

fn temporary_name_matches(name: &str, target: &str) -> bool {
    name.strip_prefix(&format!(".pending-{target}-"))
        .is_some_and(|suffix| Uuid::parse_str(suffix).is_ok())
}

fn read_temporary_bytes(path: &Path, linked_target: Option<&Path>) -> CustodyResult<Vec<u8>> {
    let metadata = fs::symlink_metadata(path)?;
    if !metadata.file_type().is_file() {
        return Err(CustodyError::Protocol(
            "custody temporary is not regular".into(),
        ));
    }
    #[cfg(unix)]
    let file = {
        use std::os::unix::fs::{MetadataExt, OpenOptionsExt, PermissionsExt};
        if metadata.uid() != unsafe { libc::geteuid() }
            || metadata.permissions().mode() & 0o077 != 0
            || !matches!(metadata.nlink(), 1 | 2)
        {
            return Err(CustodyError::Protocol(
                "custody temporary is not private".into(),
            ));
        }
        if metadata.nlink() == 2 {
            let target = linked_target.ok_or_else(|| {
                CustodyError::Protocol("unexpected custody temporary hardlink".into())
            })?;
            let linked = fs::symlink_metadata(target)?;
            if !linked.file_type().is_file()
                || linked.dev() != metadata.dev()
                || linked.ino() != metadata.ino()
                || linked.nlink() != 2
            {
                return Err(CustodyError::Protocol("custody hardlink differs".into()));
            }
        } else if linked_target.is_some_and(|target| fs::symlink_metadata(target).is_ok()) {
            return Err(CustodyError::Protocol(
                "custody mirror already exists".into(),
            ));
        }
        let file = fs::OpenOptions::new()
            .read(true)
            .custom_flags(libc::O_NOFOLLOW)
            .open(path)?;
        let opened = file.metadata()?;
        if opened.dev() != metadata.dev()
            || opened.ino() != metadata.ino()
            || opened.nlink() != metadata.nlink()
            || opened.uid() != metadata.uid()
            || opened.permissions().mode() != metadata.permissions().mode()
        {
            return Err(CustodyError::Protocol("custody temporary changed".into()));
        }
        file
    };
    #[cfg(not(unix))]
    let file = {
        if linked_target.is_some() {
            return Err(CustodyError::Protocol(
                "custody hardlink recovery requires Unix".into(),
            ));
        }
        fs::File::open(path)?
    };
    let mut bytes = Vec::new();
    file.take(MAX_MESSAGE + 1).read_to_end(&mut bytes)?;
    if bytes.is_empty() || bytes.len() as u64 > MAX_MESSAGE {
        return Err(CustodyError::Protocol(
            "custody temporary size invalid".into(),
        ));
    }
    Ok(bytes)
}

struct Store<'a> {
    root: &'a Path,
    anchor: ControlCheckpoint,
    head: ControlCheckpoint,
    latest: Option<Committed>,
    pending: Option<Reservation>,
    event_ids: HashSet<String>,
}

impl<'a> Store<'a> {
    fn load(root: &'a Path) -> CustodyResult<Self> {
        let anchor: ControlCheckpoint = read_json(&root.join(ANCHOR))?;
        let mut head: ControlCheckpoint = read_json(&root.join(HEAD))?;
        validate_checkpoint(&anchor)?;
        validate_checkpoint(&head)?;
        if head.store_id != anchor.store_id || head.sequence < anchor.sequence {
            return Err(CustodyError::Protocol("head predates anchor".into()));
        }
        let history = root.join(HISTORY);
        validate_private_dir(&history)?;
        let mut names = fs::read_dir(&history)?
            .map(|item| item.map(|item| item.file_name()))
            .collect::<Result<Vec<_>, _>>()?;
        names.sort();
        let mut tip = anchor.clone();
        let mut ids = HashSet::new();
        let mut latest = None;
        for name in names {
            let name = name
                .to_str()
                .ok_or_else(|| CustodyError::Protocol("invalid history name".into()))?;
            let next = tip
                .sequence
                .checked_add(1)
                .ok_or(CustodyError::Invalid("custody sequence is exhausted"))?;
            if name != format!("{next:020}.json") {
                return Err(CustodyError::Protocol("history gap or unknown file".into()));
            }
            let record: Committed = read_json(&history.join(name))?;
            validate_reservation(&record.reservation)?;
            validate_advance(&record.reservation, &record.successor)?;
            if record.reservation.predecessor != tip
                || !ids.insert(record.reservation.event_id.clone())
            {
                return Err(CustodyError::Protocol(
                    "history fork or duplicate event".into(),
                ));
            }
            tip = record.successor.clone();
            latest = Some(record);
        }
        if tip.sequence == head.sequence && tip != head {
            return Err(CustodyError::Protocol("head conflicts with history".into()));
        }
        if tip.sequence < head.sequence
            || head
                .sequence
                .checked_add(1)
                .is_some_and(|next| tip.sequence > next)
        {
            return Err(CustodyError::Protocol("head/history gap".into()));
        }
        if tip.sequence == head.sequence + 1 {
            if latest
                .as_ref()
                .is_none_or(|entry: &Committed| entry.reservation.predecessor != head)
            {
                return Err(CustodyError::Protocol("unmatched CAS recovery".into()));
            }
            replace_private(&root.join(HEAD), &serde_json::to_vec(&tip)?)?;
            head = tip;
        }
        let pending = match fs::symlink_metadata(root.join(PENDING)) {
            Ok(_) => Some(read_json::<Reservation>(&root.join(PENDING))?),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => None,
            Err(error) => return Err(error.into()),
        };
        let pending = if let Some(ref reservation) = pending {
            validate_reservation(reservation)?;
            if reservation.predecessor != head {
                if latest.as_ref().is_some_and(|entry| {
                    entry.reservation == *reservation && entry.successor == head
                }) {
                    fs::remove_file(root.join(PENDING))?;
                    sync_dir(root)?;
                    None
                } else {
                    return Err(CustodyError::Protocol(
                        "reservation conflicts with head".into(),
                    ));
                }
            } else {
                pending
            }
        } else {
            None
        };
        Ok(Self {
            root,
            anchor,
            head,
            latest,
            pending,
            event_ids: ids,
        })
    }

    fn apply(&mut self, request: Incoming) -> CustodyResult<Response> {
        if self.anchor.store_id != self.head.store_id {
            return Err(CustodyError::Protocol("store identity changed".into()));
        }
        match request {
            Incoming::Read { store_id } => {
                validate_identifier(&store_id)?;
                if store_id != self.head.store_id {
                    return Err(CustodyError::Protocol("store ID mismatch".into()));
                }
                Ok(Response {
                    checkpoint: self.head.clone(),
                    reservation: None,
                })
            }
            Incoming::Reserve {
                predecessor,
                event_id,
                intent_sha256,
            } => {
                validate_checkpoint(&predecessor)?;
                validate_identifier(&event_id)?;
                validate_hash(&intent_sha256)?;
                if predecessor != self.head {
                    if let Some(ref latest) = self.latest
                        && latest.reservation.predecessor == predecessor
                        && latest.reservation.event_id == event_id
                        && latest.reservation.intent_sha256 == intent_sha256
                    {
                        return Ok(Response {
                            checkpoint: predecessor,
                            reservation: Some(latest.reservation.clone()),
                        });
                    }
                    return Err(CustodyError::Protocol("stale predecessor".into()));
                }
                if self.event_ids.contains(&event_id) {
                    return Err(CustodyError::Protocol(
                        "event ID was already committed".into(),
                    ));
                }
                if let Some(ref existing) = self.pending {
                    if existing.predecessor == predecessor
                        && existing.event_id == event_id
                        && existing.intent_sha256 == intent_sha256
                    {
                        return Ok(Response {
                            checkpoint: predecessor,
                            reservation: Some(existing.clone()),
                        });
                    }
                    return Err(CustodyError::Protocol("head already reserved".into()));
                }
                let reservation = Reservation {
                    reservation_id: Uuid::new_v4().to_string(),
                    predecessor: predecessor.clone(),
                    event_id,
                    intent_sha256,
                };
                replace_private(&self.root.join(PENDING), &serde_json::to_vec(&reservation)?)?;
                self.pending = Some(reservation.clone());
                Ok(Response {
                    checkpoint: predecessor,
                    reservation: Some(reservation),
                })
            }
            Incoming::CompareAndSwap {
                reservation,
                successor,
            } => {
                validate_reservation(&reservation)?;
                validate_advance(&reservation, &successor)?;
                if self.head == successor {
                    if self.latest.as_ref().is_some_and(|entry| {
                        entry.reservation == reservation && entry.successor == successor
                    }) {
                        return Ok(Response {
                            checkpoint: successor,
                            reservation: None,
                        });
                    }
                    return Err(CustodyError::Protocol("CAS conflicts with head".into()));
                }
                if self.head != reservation.predecessor
                    || self.pending.as_ref() != Some(&reservation)
                {
                    return Err(CustodyError::Protocol(
                        "CAS has no matching reservation".into(),
                    ));
                }
                let record = Committed {
                    reservation,
                    successor: successor.clone(),
                };
                let mirror = self
                    .root
                    .join(HISTORY)
                    .join(format!("{:020}.json", successor.sequence));
                if fs::symlink_metadata(&mirror).is_ok() {
                    return Err(CustodyError::Protocol("CAS history already exists".into()));
                }
                publish_new_private(&mirror, &serde_json::to_vec(&record)?)?;
                replace_private(&self.root.join(HEAD), &serde_json::to_vec(&successor)?)?;
                fs::remove_file(self.root.join(PENDING))?;
                sync_dir(self.root)?;
                self.head = successor.clone();
                self.event_ids.insert(record.reservation.event_id.clone());
                self.latest = Some(record);
                self.pending = None;
                Ok(Response {
                    checkpoint: successor,
                    reservation: None,
                })
            }
            _ => Err(CustodyError::Protocol("v2 operation in v1 store".into())),
        }
    }
}

#[derive(Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
struct AnchorV2 {
    format: u8,
    genesis: CustodyState,
    legacy_tip: Option<ControlCheckpoint>,
}

#[derive(Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
struct CommittedV2 {
    reservation: CustodyReservationV2,
    successor: CustodyState,
}

/// Published after its history mirror and before the public head. The digest
/// binds the ordered history, including reservation IDs and intent hashes.
/// An unsealed mirror can only be completed by retrying the exact same CAS.
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct HistorySealV2 {
    format: u8,
    revision: u64,
    head_sha256: String,
    latest: Option<CommittedV2>,
}

fn history_genesis_hash(anchor: &AnchorV2) -> CustodyResult<String> {
    let mut hash = Sha256::new();
    hash.update(b"boaz-health-custody-v2-history-genesis\0");
    hash.update(serde_json::to_vec(anchor)?);
    Ok(hex::encode(hash.finalize()))
}

fn history_next_hash(previous: &str, record: &CommittedV2) -> CustodyResult<String> {
    validate_hash(previous)?;
    let mut hash = Sha256::new();
    hash.update(b"boaz-health-custody-v2-history-record\0");
    hash.update(hex::decode(previous).map_err(|_| CustodyError::Invalid("invalid history hash"))?);
    hash.update(serde_json::to_vec(record)?);
    Ok(hex::encode(hash.finalize()))
}

struct StoreV2<'a> {
    root: &'a Path,
    head: CustodyState,
    latest: Option<CommittedV2>,
    pending: Option<CustodyReservationV2>,
    operation_ids: HashSet<String>,
    history_seal: HistorySealV2,
    unsealed_tail: Option<CommittedV2>,
}

impl<'a> StoreV2<'a> {
    fn load(root: &'a Path) -> CustodyResult<Self> {
        let anchor: AnchorV2 = read_json(&root.join(V2_ANCHOR))?;
        let mut head: CustodyState = read_json(&root.join(V2_HEAD))?;
        if anchor.format != 2 || anchor.genesis.revision != 0 {
            return Err(CustodyError::Protocol("unknown v2 anchor".into()));
        }
        validate_state_v2(&anchor.genesis)?;
        validate_state_v2(&head)?;
        validate_v2_root_inventory(root, anchor.legacy_tip.is_some())?;
        if let Some(ref legacy_tip) = anchor.legacy_tip {
            validate_checkpoint(legacy_tip)?;
            if anchor.genesis.control != *legacy_tip {
                return Err(CustodyError::Protocol("v1 migration anchor changed".into()));
            }
            let legacy = Store::load(root)?;
            if legacy.head != *legacy_tip || legacy.pending.is_some() {
                return Err(CustodyError::Protocol(
                    "v1 chain moved after migration".into(),
                ));
            }
        } else if fs::symlink_metadata(root.join(ANCHOR)).is_ok() {
            return Err(CustodyError::Protocol(
                "unexpected v1 files in v2 store".into(),
            ));
        }
        let history = root.join(V2_HISTORY);
        validate_private_dir(&history)?;
        // A pre-seal v2 history is not self-authenticating. Never invent a
        // seal for nonempty old history on a request path.
        let history_seal: HistorySealV2 = read_json(&root.join(V2_HISTORY_SEAL))?;
        if history_seal.format != 1 {
            return Err(CustodyError::Protocol("unknown v2 history seal".into()));
        }
        validate_hash(&history_seal.head_sha256)?;
        let mut names = fs::read_dir(&history)?
            .map(|entry| entry.map(|entry| entry.file_name()))
            .collect::<Result<Vec<_>, _>>()?;
        names.sort();
        let mut history_hash = history_genesis_hash(&anchor)?;
        let mut sealed_hash = (history_seal.revision == 0).then(|| history_hash.clone());
        let mut sealed_latest = None;
        let mut tip = anchor.genesis;
        let mut ids = HashSet::new();
        let mut latest = None;
        for name in names {
            let name = name
                .to_str()
                .ok_or_else(|| CustodyError::Protocol("invalid v2 history name".into()))?;
            let next = tip
                .revision
                .checked_add(1)
                .ok_or(CustodyError::Invalid("custody revision exhausted"))?;
            if name != format!("{next:020}.json") {
                return Err(CustodyError::Protocol(
                    "v2 history gap or unknown file".into(),
                ));
            }
            let record: CommittedV2 = read_json(&history.join(name))?;
            validate_advance_v2(&record.reservation, &record.successor)?;
            if record.reservation.predecessor != tip
                || !ids.insert(record.reservation.operation_id.clone())
            {
                return Err(CustodyError::Protocol(
                    "v2 history fork or duplicate ID".into(),
                ));
            }
            history_hash = history_next_hash(&history_hash, &record)?;
            if record.successor.revision == history_seal.revision {
                sealed_hash = Some(history_hash.clone());
                sealed_latest = Some(record.clone());
            }
            tip = record.successor.clone();
            latest = Some(record);
        }
        let pending = match fs::symlink_metadata(root.join(V2_PENDING)) {
            Ok(_) => Some(read_json::<CustodyReservationV2>(&root.join(V2_PENDING))?),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => None,
            Err(error) => return Err(error.into()),
        };
        if let Some(ref reservation) = pending {
            validate_reservation_v2(reservation)?;
        }
        if sealed_hash.as_ref() != Some(&history_seal.head_sha256)
            || sealed_latest != history_seal.latest
        {
            return Err(CustodyError::Protocol("v2 history seal differs".into()));
        }
        let unsealed_tail = if history_seal.revision == tip.revision {
            None
        } else if history_seal.revision.checked_add(1) == Some(tip.revision) {
            let record = latest
                .as_ref()
                .ok_or_else(|| CustodyError::Protocol("v2 unsealed tail missing".into()))?;
            if head != record.reservation.predecessor
                || pending.as_ref() != Some(&record.reservation)
            {
                return Err(CustodyError::Protocol("unmatched v2 unsealed CAS".into()));
            }
            Some(record.clone())
        } else {
            return Err(CustodyError::Protocol(
                "v2 history seal revision differs".into(),
            ));
        };
        if tip.revision == head.revision && tip != head {
            return Err(CustodyError::Protocol(
                "v2 head conflicts with history".into(),
            ));
        }
        if tip.revision < head.revision || tip.revision > head.revision.saturating_add(1) {
            return Err(CustodyError::Protocol("v2 head/history gap".into()));
        }
        if tip.revision == head.revision + 1 && unsealed_tail.is_none() {
            if latest
                .as_ref()
                .is_none_or(|entry: &CommittedV2| entry.reservation.predecessor != head)
            {
                return Err(CustodyError::Protocol("unmatched v2 CAS recovery".into()));
            }
            replace_private(&root.join(V2_HEAD), &serde_json::to_vec(&tip)?)?;
            head = tip;
        }
        let pending = if let Some(ref reservation) = pending {
            if reservation.predecessor != head {
                if latest.as_ref().is_some_and(|entry| {
                    entry.reservation == *reservation && entry.successor == head
                }) {
                    fs::remove_file(root.join(V2_PENDING))?;
                    sync_dir(root)?;
                    None
                } else {
                    return Err(CustodyError::Protocol("v2 reservation conflicts".into()));
                }
            } else {
                pending
            }
        } else {
            None
        };
        Ok(Self {
            root,
            head,
            latest,
            pending,
            operation_ids: ids,
            history_seal,
            unsealed_tail,
        })
    }

    fn apply(&mut self, request: Incoming) -> CustodyResult<ResponseV2> {
        match request {
            Incoming::ReadV2 { store_id } => {
                validate_identifier(&store_id)?;
                if store_id != self.head.control.store_id {
                    return Err(CustodyError::Protocol("v2 store ID mismatch".into()));
                }
                if self.pending.is_some() {
                    return Err(CustodyError::Protocol("v2 reservation unresolved".into()));
                }
                Ok(ResponseV2 {
                    state: self.head.clone(),
                    reservation: None,
                })
            }
            Incoming::ReserveV2 {
                predecessor,
                operation_id,
                intent_sha256,
            } => {
                validate_state_v2(&predecessor)?;
                validate_identifier(&operation_id)?;
                validate_hash(&intent_sha256)?;
                if predecessor != self.head {
                    if let Some(ref latest) = self.latest
                        && latest.reservation.predecessor == predecessor
                        && latest.reservation.operation_id == operation_id
                        && latest.reservation.intent_sha256 == intent_sha256
                    {
                        return Ok(ResponseV2 {
                            state: predecessor,
                            reservation: Some(latest.reservation.clone()),
                        });
                    }
                    return Err(CustodyError::Protocol("stale v2 predecessor".into()));
                }
                if let Some(ref pending) = self.pending {
                    if pending.predecessor == predecessor
                        && pending.operation_id == operation_id
                        && pending.intent_sha256 == intent_sha256
                    {
                        return Ok(ResponseV2 {
                            state: predecessor,
                            reservation: Some(pending.clone()),
                        });
                    }
                    return Err(CustodyError::Protocol("v2 head already reserved".into()));
                }
                if self.operation_ids.contains(&operation_id) {
                    return Err(CustodyError::Protocol("v2 operation ID reused".into()));
                }
                let reservation = CustodyReservationV2 {
                    reservation_id: Uuid::new_v4().to_string(),
                    predecessor: predecessor.clone(),
                    operation_id,
                    intent_sha256,
                };
                replace_private(
                    &self.root.join(V2_PENDING),
                    &serde_json::to_vec(&reservation)?,
                )?;
                self.pending = Some(reservation.clone());
                Ok(ResponseV2 {
                    state: predecessor,
                    reservation: Some(reservation),
                })
            }
            Incoming::CompareAndSwapV2 {
                reservation,
                successor,
            } => {
                validate_advance_v2(&reservation, &successor)?;
                if self.head == successor {
                    if self.latest.as_ref().is_some_and(|entry| {
                        entry.reservation == reservation && entry.successor == successor
                    }) {
                        return Ok(ResponseV2 {
                            state: successor,
                            reservation: None,
                        });
                    }
                    return Err(CustodyError::Protocol("v2 CAS conflicts with head".into()));
                }
                if self.head != reservation.predecessor
                    || self.pending.as_ref() != Some(&reservation)
                {
                    return Err(CustodyError::Protocol("v2 CAS has no reservation".into()));
                }
                let record = CommittedV2 {
                    reservation,
                    successor: successor.clone(),
                };
                let next_seal = HistorySealV2 {
                    format: 1,
                    revision: successor.revision,
                    head_sha256: history_next_hash(&self.history_seal.head_sha256, &record)?,
                    latest: Some(record.clone()),
                };
                let mirror = self
                    .root
                    .join(V2_HISTORY)
                    .join(format!("{:020}.json", successor.revision));
                if let Some(ref unsealed) = self.unsealed_tail {
                    if *unsealed != record {
                        return Err(CustodyError::Protocol(
                            "v2 unsealed CAS retry differs".into(),
                        ));
                    }
                } else {
                    if fs::symlink_metadata(&mirror).is_ok() {
                        return Err(CustodyError::Protocol("v2 CAS history exists".into()));
                    }
                    publish_new_private(&mirror, &serde_json::to_vec(&record)?)?;
                }
                // A mirror by itself cannot authorize a successor. The exact
                // retry seals it, or an already sealed mirror lets a restart
                // publish the public head after validating both witnesses.
                replace_private(
                    &self.root.join(V2_HISTORY_SEAL),
                    &serde_json::to_vec(&next_seal)?,
                )?;
                replace_private(&self.root.join(V2_HEAD), &serde_json::to_vec(&successor)?)?;
                fs::remove_file(self.root.join(V2_PENDING))?;
                sync_dir(self.root)?;
                self.head = successor.clone();
                self.operation_ids
                    .insert(record.reservation.operation_id.clone());
                self.latest = Some(record);
                self.pending = None;
                self.history_seal = next_seal;
                self.unsealed_tail = None;
                Ok(ResponseV2 {
                    state: successor,
                    reservation: None,
                })
            }
            _ => Err(CustodyError::Protocol("v1 operation in v2 store".into())),
        }
    }
}

fn validate_v2_root_inventory(root: &Path, migrated: bool) -> CustodyResult<()> {
    for item in fs::read_dir(root)? {
        let name = item?.file_name();
        let name = name
            .to_str()
            .ok_or_else(|| CustodyError::Protocol("invalid custody root name".into()))?;
        let v2_name = matches!(
            name,
            LOCK | V2_ANCHOR | V2_HEAD | V2_HISTORY_SEAL | V2_PENDING | V2_HISTORY
        );
        let v1_name = migrated && matches!(name, ANCHOR | HEAD | HISTORY);
        if !v2_name && !v1_name {
            return Err(CustodyError::Protocol("unknown custody root file".into()));
        }
    }
    Ok(())
}

fn validate_identifier(value: &str) -> CustodyResult<()> {
    if value.is_empty()
        || value.len() > 128
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || b"-_.".contains(&byte))
    {
        return Err(CustodyError::Invalid("custody identifier is invalid"));
    }
    Ok(())
}

fn validate_reservation(value: &Reservation) -> CustodyResult<()> {
    validate_checkpoint(&value.predecessor)?;
    validate_identifier(&value.event_id)?;
    validate_hash(&value.intent_sha256)?;
    if Uuid::parse_str(&value.reservation_id).is_err() {
        return Err(CustodyError::Invalid("reservation ID is invalid"));
    }
    Ok(())
}

fn validate_advance(value: &Reservation, successor: &ControlCheckpoint) -> CustodyResult<()> {
    validate_checkpoint(successor)?;
    if successor.store_id != value.predecessor.store_id
        || value.predecessor.sequence.checked_add(1) != Some(successor.sequence)
        || successor.current_hash == value.predecessor.current_hash
    {
        return Err(CustodyError::Invalid(
            "CAS does not advance the reserved head",
        ));
    }
    Ok(())
}

fn validate_state_v2(state: &CustodyState) -> CustodyResult<()> {
    if state.format != 2 {
        return Err(CustodyError::Protocol("unknown custody format".into()));
    }
    validate_checkpoint(&state.control)?;
    if state.ack.is_some() != state.baseline_sha256.is_some() {
        return Err(CustodyError::Protocol(
            "baseline and journal binding differ".into(),
        ));
    }
    if let Some(ref hash) = state.baseline_sha256 {
        validate_hash(hash)?;
    }
    if let Some(ref ack) = state.ack {
        validate_identifier(&ack.journal_id)?;
        validate_hash(&ack.head_sha256)?;
        if ack.confirmation_sequence > ack.sequence {
            return Err(CustodyError::Protocol(
                "journal confirmation exceeds prepared count".into(),
            ));
        }
    }
    Ok(())
}

fn validate_reservation_v2(value: &CustodyReservationV2) -> CustodyResult<()> {
    validate_state_v2(&value.predecessor)?;
    validate_identifier(&value.operation_id)?;
    validate_hash(&value.intent_sha256)?;
    if Uuid::parse_str(&value.reservation_id).is_err() {
        return Err(CustodyError::Invalid("v2 reservation ID invalid"));
    }
    Ok(())
}

fn validate_advance_v2(
    reservation: &CustodyReservationV2,
    successor: &CustodyState,
) -> CustodyResult<()> {
    validate_reservation_v2(reservation)?;
    validate_state_v2(successor)?;
    let prior = &reservation.predecessor;
    if prior.revision.checked_add(1) != Some(successor.revision)
        || prior.control.store_id != successor.control.store_id
    {
        return Err(CustodyError::Protocol(
            "v2 revision or store identity changed".into(),
        ));
    }
    let control_changed = prior.control != successor.control;
    let baseline_changed = prior.baseline_sha256 != successor.baseline_sha256;
    let ack_changed = prior.ack != successor.ack;
    if u8::from(control_changed) + u8::from(baseline_changed) + u8::from(ack_changed) == 0 {
        return Err(CustodyError::Protocol(
            "v2 CAS did not advance an authority".into(),
        ));
    }
    if control_changed {
        if baseline_changed || ack_changed {
            return Err(CustodyError::Protocol(
                "v2 control CAS must stand alone".into(),
            ));
        }
        if prior.control.sequence.checked_add(1) != Some(successor.control.sequence)
            || prior.control.current_hash == successor.control.current_hash
        {
            return Err(CustodyError::Protocol(
                "v2 control head is not sequential".into(),
            ));
        }
        return Ok(());
    }
    if baseline_changed {
        if prior.baseline_sha256.is_some() || prior.ack.is_some() {
            return Err(CustodyError::Protocol(
                "v2 baseline cannot be replaced".into(),
            ));
        }
        let ack = successor.ack.as_ref().ok_or(CustodyError::Protocol(
            "baseline requires journal head".into(),
        ))?;
        if ack.sequence != 0
            || ack.confirmation_sequence != 0
            || ack.pairing_confirmation_sequence != 0
        {
            return Err(CustodyError::Protocol(
                "baseline journal is not empty".into(),
            ));
        }
        return Ok(());
    }
    if !ack_changed {
        return Err(CustodyError::Protocol(
            "v2 CAS has no journal advance".into(),
        ));
    }
    let (Some(old), Some(new)) = (&prior.ack, &successor.ack) else {
        return Err(CustodyError::Protocol("v2 journal is not bound".into()));
    };
    if old.journal_id != new.journal_id
        || new.sequence < old.sequence
        || old.head_sha256 == new.head_sha256
    {
        return Err(CustodyError::Protocol("v2 journal head regressed".into()));
    }
    let batch_advanced = old.confirmation_sequence.checked_add(1)
        == Some(new.confirmation_sequence)
        && old.pairing_confirmation_sequence == new.pairing_confirmation_sequence;
    let pairing_advanced = old.pairing_confirmation_sequence.checked_add(1)
        == Some(new.pairing_confirmation_sequence)
        && old.confirmation_sequence == new.confirmation_sequence;
    if !batch_advanced && !pairing_advanced {
        return Err(CustodyError::Protocol(
            "v2 confirmation is not sequential".into(),
        ));
    }
    Ok(())
}

fn validate_root(root: &Path) -> CustodyResult<()> {
    if !root.is_absolute()
        || root
            .components()
            .any(|component| matches!(component, Component::ParentDir | Component::CurDir))
    {
        return Err(CustodyError::Invalid(
            "custody root must be absolute and normalized",
        ));
    }
    let mut prefix = PathBuf::new();
    for component in root.components() {
        prefix.push(component.as_os_str());
        if !fs::symlink_metadata(&prefix)?.file_type().is_dir() {
            return Err(CustodyError::Invalid(
                "custody path contains a symlink or non-directory",
            ));
        }
    }
    validate_private_dir(root)
}

fn validate_private_dir(path: &Path) -> CustodyResult<()> {
    let metadata = fs::symlink_metadata(path)?;
    if !metadata.file_type().is_dir() {
        return Err(CustodyError::Invalid("custody directory is not real"));
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::{MetadataExt, PermissionsExt};
        if metadata.uid() != unsafe { libc::geteuid() }
            || metadata.permissions().mode() & 0o077 != 0
        {
            return Err(CustodyError::Invalid("custody directory is not private"));
        }
    }
    Ok(())
}

fn open_private(path: &Path) -> CustodyResult<fs::File> {
    let metadata = fs::symlink_metadata(path)?;
    if !metadata.file_type().is_file() {
        return Err(CustodyError::Invalid("custody file is not regular"));
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::{MetadataExt, OpenOptionsExt, PermissionsExt};
        if metadata.nlink() != 1
            || metadata.uid() != unsafe { libc::geteuid() }
            || metadata.permissions().mode() & 0o077 != 0
        {
            return Err(CustodyError::Invalid("custody file is not private"));
        }
        let file = fs::OpenOptions::new()
            .read(true)
            .write(true)
            .custom_flags(libc::O_NOFOLLOW)
            .open(path)?;
        let opened = file.metadata()?;
        if opened.dev() != metadata.dev()
            || opened.ino() != metadata.ino()
            || opened.nlink() != 1
            || opened.uid() != unsafe { libc::geteuid() }
            || opened.permissions().mode() & 0o077 != 0
        {
            return Err(CustodyError::Invalid("custody file changed while opening"));
        }
        Ok(file)
    }
    #[cfg(not(unix))]
    {
        Ok(fs::OpenOptions::new().read(true).write(true).open(path)?)
    }
}

fn read_json<T: for<'de> Deserialize<'de>>(path: &Path) -> CustodyResult<T> {
    let mut bytes = Vec::new();
    open_private(path)?
        .take(MAX_MESSAGE + 1)
        .read_to_end(&mut bytes)?;
    if bytes.is_empty() || bytes.len() as u64 > MAX_MESSAGE {
        return Err(CustodyError::Protocol(
            "custody file size is invalid".into(),
        ));
    }
    Ok(serde_json::from_slice(&bytes)?)
}

fn create_private(path: &Path, bytes: &[u8]) -> CustodyResult<()> {
    if bytes.len() as u64 > MAX_MESSAGE {
        return Err(CustodyError::Invalid("custody record is too large"));
    }
    let mut options = fs::OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600).custom_flags(libc::O_NOFOLLOW);
    }
    let mut file = options.open(path)?;
    file.write_all(bytes)?;
    file.sync_all()?;
    sync_dir(
        path.parent()
            .ok_or(CustodyError::Invalid("missing parent"))?,
    )
}

fn replace_private(path: &Path, bytes: &[u8]) -> CustodyResult<()> {
    let parent = path
        .parent()
        .ok_or(CustodyError::Invalid("missing parent"))?;
    let temporary = temporary_for(path)?;
    create_private(&temporary, bytes)?;
    fs::rename(&temporary, path)?;
    sync_dir(parent)
}

fn publish_new_private(path: &Path, bytes: &[u8]) -> CustodyResult<()> {
    let parent = path
        .parent()
        .ok_or(CustodyError::Invalid("missing parent"))?;
    let temporary = temporary_for(path)?;
    create_private(&temporary, bytes)?;
    // hard_link is create-new: unlike rename, it cannot replace an existing
    // mirror. The temporary name is removed before the CAS is acknowledged.
    fs::hard_link(&temporary, path)?;
    sync_dir(parent)?;
    fs::remove_file(&temporary)?;
    sync_dir(parent)
}

fn temporary_for(path: &Path) -> CustodyResult<PathBuf> {
    let parent = path
        .parent()
        .ok_or(CustodyError::Invalid("missing parent"))?;
    let name = path
        .file_name()
        .and_then(|name| name.to_str())
        .ok_or(CustodyError::Invalid("invalid custody filename"))?;
    Ok(parent.join(format!(".pending-{name}-{}", Uuid::new_v4())))
}

fn sync_dir(path: &Path) -> CustodyResult<()> {
    fs::File::open(path)?.sync_all()?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use tempfile::TempDir;

    fn fixture() -> (TempDir, ControlCheckpoint) {
        let root = TempDir::new_in(std::env::temp_dir().canonicalize().unwrap())
            .expect("isolated custody directory");
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            fs::set_permissions(root.path(), fs::Permissions::from_mode(0o700)).unwrap();
        }
        let anchor = ControlCheckpoint {
            store_id: "synthetic-store".into(),
            sequence: 0,
            current_hash: "a".repeat(64),
        };
        initialize_off_host(root.path(), &anchor).expect("initialize");
        (root, anchor)
    }

    fn request(root: &Path, input: serde_json::Value) -> CustodyResult<Response> {
        process_forced_request(root, &serde_json::to_vec(&input).expect("JSON"))
    }

    fn fixture_v2() -> (TempDir, CustodyState) {
        let root = TempDir::new_in(std::env::temp_dir().canonicalize().unwrap()).unwrap();
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            fs::set_permissions(root.path(), fs::Permissions::from_mode(0o700)).unwrap();
        }
        let control = ControlCheckpoint {
            store_id: "synthetic-store".into(),
            sequence: 0,
            current_hash: "a".repeat(64),
        };
        initialize_off_host_v2(root.path(), &control).unwrap();
        let state = request_v2(
            root.path(),
            json!({
                "op":"read_v2", "store_id":control.store_id
            }),
        )
        .unwrap()
        .state;
        (root, state)
    }

    fn request_v2(root: &Path, input: serde_json::Value) -> CustodyResult<ResponseV2> {
        let request: Incoming = serde_json::from_value(input).unwrap();
        process_forced_request_v2(root, request)
    }

    fn reserved_control_v2() -> (
        TempDir,
        CustodyState,
        CustodyState,
        CustodyReservationV2,
        CommittedV2,
    ) {
        let (root, genesis) = fixture_v2();
        let mut successor = genesis.clone();
        successor.revision = 1;
        successor.control.sequence = 1;
        successor.control.current_hash = "b".repeat(64);
        let reservation = request_v2(
            root.path(),
            json!({
                "op":"reserve_v2", "predecessor":genesis,
                "operation_id":"control-1", "intent_sha256":"c".repeat(64)
            }),
        )
        .unwrap()
        .reservation
        .unwrap();
        let record = CommittedV2 {
            reservation: reservation.clone(),
            successor: successor.clone(),
        };
        (root, genesis, successor, reservation, record)
    }

    fn commit_v2(root: &Path, prior: &CustodyState, successor: &CustodyState, operation_id: &str) {
        let reserve = json!({
            "op":"reserve_v2", "predecessor":prior,
            "operation_id":operation_id, "intent_sha256":"d".repeat(64)
        });
        let reservation = request_v2(root, reserve.clone())
            .unwrap()
            .reservation
            .unwrap();
        assert_eq!(
            request_v2(root, reserve).unwrap().reservation.unwrap(),
            reservation
        );
        assert!(
            request_v2(
                root,
                json!({"op":"read_v2", "store_id":prior.control.store_id})
            )
            .is_err()
        );
        let cas = json!({
            "op":"compare_and_swap_v2", "reservation":reservation,
            "successor":successor
        });
        assert_eq!(request_v2(root, cas.clone()).unwrap().state, *successor);
        assert_eq!(request_v2(root, cas).unwrap().state, *successor);
    }

    #[test]
    fn consistent_whole_domain_rollback_needs_an_independent_witness() {
        let (root, genesis) = fixture_v2();
        let old_head = fs::read(root.path().join(V2_HEAD)).unwrap();
        let old_seal = fs::read(root.path().join(V2_HISTORY_SEAL)).unwrap();
        let mut successor = genesis.clone();
        successor.revision = 1;
        successor.control.sequence = 1;
        successor.control.current_hash = "b".repeat(64);
        commit_v2(root.path(), &genesis, &successor, "control-1");

        // An attacker able to restore the *entire* custody failure domain can
        // present an internally consistent older generation. The current
        // same-domain chain cannot distinguish it from untouched genesis.
        replace_private(&root.path().join(V2_HEAD), &old_head).unwrap();
        replace_private(&root.path().join(V2_HISTORY_SEAL), &old_seal).unwrap();
        fs::remove_file(
            root.path()
                .join(V2_HISTORY)
                .join("00000000000000000001.json"),
        )
        .unwrap();
        assert_eq!(
            request_v2(
                root.path(),
                json!({"op":"read_v2", "store_id":"synthetic-store"}),
            )
            .unwrap()
            .state,
            genesis
        );
    }

    #[test]
    fn v2_control_baseline_and_confirmations_share_one_monotonic_revision() {
        let (root, genesis) = fixture_v2();
        let mut control = genesis.clone();
        control.revision = 1;
        control.control.sequence = 1;
        control.control.current_hash = "b".repeat(64);
        commit_v2(root.path(), &genesis, &control, "control-1");

        let mut baseline = control.clone();
        baseline.revision = 2;
        baseline.baseline_sha256 = Some("c".repeat(64));
        baseline.ack = Some(AckCheckpoint {
            journal_id: "journal-1".into(),
            sequence: 0,
            confirmation_sequence: 0,
            pairing_confirmation_sequence: 0,
            head_sha256: "e".repeat(64),
        });
        commit_v2(root.path(), &control, &baseline, "baseline-1");

        let mut batch = baseline.clone();
        batch.revision = 3;
        let ack = batch.ack.as_mut().unwrap();
        ack.sequence = 1;
        ack.confirmation_sequence = 1;
        ack.head_sha256 = "f".repeat(64);
        commit_v2(root.path(), &baseline, &batch, "batch-1");

        let mut pairing = batch.clone();
        pairing.revision = 4;
        pairing.ack.as_mut().unwrap().pairing_confirmation_sequence = 1;
        pairing.ack.as_mut().unwrap().head_sha256 = "1".repeat(64);
        commit_v2(root.path(), &batch, &pairing, "pairing-1");

        let final_state = request_v2(
            root.path(),
            json!({"op":"read_v2", "store_id":"synthetic-store"}),
        )
        .unwrap()
        .state;
        assert_eq!(final_state, pairing);
        assert!(
            request_v2(
                root.path(),
                json!({
                    "op":"reserve_v2", "predecessor":pairing,
                    "operation_id":"batch-1", "intent_sha256":"d".repeat(64)
                })
            )
            .is_err()
        );
    }

    #[test]
    fn v2_rejects_baseline_replacement_and_journal_rollback() {
        let (root, genesis) = fixture_v2();
        let mut baseline = genesis.clone();
        baseline.revision = 1;
        baseline.baseline_sha256 = Some("b".repeat(64));
        baseline.ack = Some(AckCheckpoint {
            journal_id: "journal-1".into(),
            sequence: 0,
            confirmation_sequence: 0,
            pairing_confirmation_sequence: 0,
            head_sha256: "c".repeat(64),
        });
        commit_v2(root.path(), &genesis, &baseline, "baseline-1");
        let mut replacement = baseline.clone();
        replacement.revision = 2;
        replacement.baseline_sha256 = Some("d".repeat(64));
        let reservation = CustodyReservationV2 {
            reservation_id: Uuid::new_v4().to_string(),
            predecessor: baseline.clone(),
            operation_id: "replace".into(),
            intent_sha256: "e".repeat(64),
        };
        assert!(validate_advance_v2(&reservation, &replacement).is_err());
        replacement.baseline_sha256 = baseline.baseline_sha256.clone();
        replacement.ack.as_mut().unwrap().head_sha256 = "f".repeat(64);
        assert!(validate_advance_v2(&reservation, &replacement).is_err());

        let mut corrupted = baseline.clone();
        corrupted.baseline_sha256 = Some("f".repeat(64));
        replace_private(
            &root.path().join(V2_HEAD),
            &serde_json::to_vec(&corrupted).unwrap(),
        )
        .unwrap();
        assert!(
            request_v2(
                root.path(),
                json!({"op":"read_v2", "store_id":"synthetic-store"})
            )
            .is_err()
        );
    }

    #[test]
    fn v1_migration_requires_settled_chain_and_disables_legacy_protocol() {
        let (root, genesis) = fixture();
        let reserve = json!({
            "op":"reserve", "predecessor":genesis,
            "event_id":"legacy-1", "intent_sha256":"b".repeat(64)
        });
        let reservation = request(root.path(), reserve).unwrap().reservation.unwrap();
        assert!(migrate_off_host_v1_to_v2(root.path()).is_err());
        let successor = ControlCheckpoint {
            sequence: 1,
            current_hash: "c".repeat(64),
            ..genesis
        };
        request(
            root.path(),
            json!({
                "op":"compare_and_swap", "reservation":reservation,
                "successor":successor
            }),
        )
        .unwrap();
        let adopted = migrate_off_host_v1_to_v2(root.path()).unwrap();
        assert_eq!(adopted.control, successor);
        assert_eq!(adopted.revision, 0);
        assert!(migrate_off_host_v1_to_v2(root.path()).is_err());
        assert!(
            request(
                root.path(),
                json!({
                    "op":"read", "store_id":"synthetic-store"
                })
            )
            .is_err()
        );
        assert_eq!(
            request_v2(
                root.path(),
                json!({
                    "op":"read_v2", "store_id":"synthetic-store"
                })
            )
            .unwrap()
            .state,
            adopted
        );

        let wrong = ControlCheckpoint {
            current_hash: "f".repeat(64),
            ..successor
        };
        replace_private(
            &root.path().join(HEAD),
            &serde_json::to_vec(&wrong).unwrap(),
        )
        .unwrap();
        assert!(
            request_v2(
                root.path(),
                json!({
                    "op":"read_v2", "store_id":"synthetic-store"
                })
            )
            .is_err()
        );
    }

    #[test]
    fn v2_interrupted_cas_republishes_only_its_reserved_successor() {
        for seal_published in [false, true] {
            let (root, genesis) = fixture_v2();
            let mut successor = genesis.clone();
            successor.revision = 1;
            successor.control.sequence = 1;
            successor.control.current_hash = "b".repeat(64);
            let reservation = request_v2(
                root.path(),
                json!({
                    "op":"reserve_v2", "predecessor":genesis,
                    "operation_id":"control-1", "intent_sha256":"c".repeat(64)
                }),
            )
            .unwrap()
            .reservation
            .unwrap();
            let record = CommittedV2 {
                reservation: reservation.clone(),
                successor: successor.clone(),
            };
            let initial_seal: HistorySealV2 =
                read_json(&root.path().join(V2_HISTORY_SEAL)).unwrap();
            let committed_seal = HistorySealV2 {
                format: 1,
                revision: 1,
                head_sha256: history_next_hash(&initial_seal.head_sha256, &record).unwrap(),
                latest: Some(record.clone()),
            };
            publish_new_private(
                &root
                    .path()
                    .join(V2_HISTORY)
                    .join("00000000000000000001.json"),
                &serde_json::to_vec(&record).unwrap(),
            )
            .unwrap();
            if seal_published {
                replace_private(
                    &root.path().join(V2_HISTORY_SEAL),
                    &serde_json::to_vec(&committed_seal).unwrap(),
                )
                .unwrap();
            }
            if seal_published {
                assert_eq!(
                    request_v2(
                        root.path(),
                        json!({"op":"read_v2", "store_id":"synthetic-store"})
                    )
                    .unwrap()
                    .state,
                    successor
                );
            } else {
                assert!(
                    request_v2(
                        root.path(),
                        json!({"op":"read_v2", "store_id":"synthetic-store"})
                    )
                    .is_err()
                );
                // The original reservation must be reusable while the exact
                // CAS is still missing its independently published seal.
                assert_eq!(
                    request_v2(
                        root.path(),
                        json!({
                            "op":"reserve_v2", "predecessor":genesis,
                            "operation_id":"control-1", "intent_sha256":"c".repeat(64)
                        })
                    )
                    .unwrap()
                    .reservation,
                    Some(reservation.clone())
                );
            }
            assert_eq!(
                request_v2(
                    root.path(),
                    json!({
                        "op":"compare_and_swap_v2", "reservation":reservation,
                        "successor":successor
                    })
                )
                .unwrap()
                .state,
                successor
            );
            assert!(!root.path().join(V2_PENDING).exists());
        }
    }

    #[test]
    fn v2_unsealed_history_tamper_cannot_choose_a_new_successor() {
        let (root, genesis) = fixture_v2();
        let mut successor = genesis.clone();
        successor.revision = 1;
        successor.control.sequence = 1;
        successor.control.current_hash = "b".repeat(64);
        let reservation = request_v2(
            root.path(),
            json!({
                "op":"reserve_v2", "predecessor":genesis,
                "operation_id":"control-1", "intent_sha256":"c".repeat(64)
            }),
        )
        .unwrap()
        .reservation
        .unwrap();
        let mut altered = CommittedV2 {
            reservation: reservation.clone(),
            successor: successor.clone(),
        };
        altered.successor.control.current_hash = "d".repeat(64);
        publish_new_private(
            &root
                .path()
                .join(V2_HISTORY)
                .join("00000000000000000001.json"),
            &serde_json::to_vec(&altered).unwrap(),
        )
        .unwrap();
        assert!(
            request_v2(
                root.path(),
                json!({
                    "op":"compare_and_swap_v2", "reservation":reservation,
                    "successor":successor
                })
            )
            .is_err()
        );
        assert_eq!(
            read_json::<CustodyState>(&root.path().join(V2_HEAD)).unwrap(),
            genesis
        );
    }

    #[test]
    fn v2_matching_cas_retry_recovers_a_seal_temporary_file() {
        let (root, genesis) = fixture_v2();
        let mut successor = genesis.clone();
        successor.revision = 1;
        successor.control.sequence = 1;
        successor.control.current_hash = "b".repeat(64);
        let reservation = request_v2(
            root.path(),
            json!({
                "op":"reserve_v2", "predecessor":genesis,
                "operation_id":"control-1", "intent_sha256":"c".repeat(64)
            }),
        )
        .unwrap()
        .reservation
        .unwrap();
        let record = CommittedV2 {
            reservation: reservation.clone(),
            successor: successor.clone(),
        };
        publish_new_private(
            &root
                .path()
                .join(V2_HISTORY)
                .join("00000000000000000001.json"),
            &serde_json::to_vec(&record).unwrap(),
        )
        .unwrap();
        let prior_seal: HistorySealV2 = read_json(&root.path().join(V2_HISTORY_SEAL)).unwrap();
        let next_seal = HistorySealV2 {
            format: 1,
            revision: 1,
            head_sha256: history_next_hash(&prior_seal.head_sha256, &record).unwrap(),
            latest: Some(record),
        };
        let abandoned = root
            .path()
            .join(format!(".pending-{V2_HISTORY_SEAL}-{}", Uuid::new_v4()));
        create_private(&abandoned, &serde_json::to_vec(&next_seal).unwrap()).unwrap();

        assert_eq!(
            request_v2(
                root.path(),
                json!({
                    "op":"compare_and_swap_v2", "reservation":reservation,
                    "successor":successor
                })
            )
            .unwrap()
            .state,
            successor
        );
        assert!(!abandoned.exists());
    }

    #[test]
    fn v2_matching_reserve_retry_recovers_its_temporary_file() {
        let (root, genesis) = fixture_v2();
        let unreturned = CustodyReservationV2 {
            reservation_id: Uuid::new_v4().to_string(),
            predecessor: genesis.clone(),
            operation_id: "control-1".into(),
            intent_sha256: "c".repeat(64),
        };
        let abandoned = temporary_for(&root.path().join(V2_PENDING)).unwrap();
        create_private(&abandoned, &serde_json::to_vec(&unreturned).unwrap()).unwrap();
        let returned = request_v2(
            root.path(),
            json!({
                "op":"reserve_v2", "predecessor":genesis,
                "operation_id":"control-1", "intent_sha256":"c".repeat(64)
            }),
        )
        .unwrap()
        .reservation
        .unwrap();
        assert_eq!(returned.predecessor, genesis);
        assert_eq!(returned.operation_id, "control-1");
        assert!(!abandoned.exists());
        assert_eq!(
            read_json::<CustodyReservationV2>(&root.path().join(V2_PENDING)).unwrap(),
            returned
        );
    }

    #[test]
    fn v2_matching_cas_retry_recovers_linked_mirror_temporary_file() {
        let (root, _, successor, reservation, record) = reserved_control_v2();
        let mirror = root
            .path()
            .join(V2_HISTORY)
            .join("00000000000000000001.json");
        let abandoned = temporary_for(&mirror).unwrap();
        create_private(&abandoned, &serde_json::to_vec(&record).unwrap()).unwrap();
        fs::hard_link(&abandoned, &mirror).unwrap();
        sync_dir(mirror.parent().unwrap()).unwrap();
        assert_eq!(
            request_v2(
                root.path(),
                json!({
                    "op":"compare_and_swap_v2", "reservation":reservation,
                    "successor":successor
                })
            )
            .unwrap()
            .state,
            successor
        );
        assert!(!abandoned.exists());
        assert!(read_json::<CommittedV2>(&mirror).unwrap() == record);
    }

    #[test]
    fn v2_matching_cas_retry_recovers_head_temporary_file() {
        let (root, _, successor, reservation, record) = reserved_control_v2();
        publish_new_private(
            &root
                .path()
                .join(V2_HISTORY)
                .join("00000000000000000001.json"),
            &serde_json::to_vec(&record).unwrap(),
        )
        .unwrap();
        let prior: HistorySealV2 = read_json(&root.path().join(V2_HISTORY_SEAL)).unwrap();
        let next = HistorySealV2 {
            format: 1,
            revision: 1,
            head_sha256: history_next_hash(&prior.head_sha256, &record).unwrap(),
            latest: Some(record),
        };
        replace_private(
            &root.path().join(V2_HISTORY_SEAL),
            &serde_json::to_vec(&next).unwrap(),
        )
        .unwrap();
        let abandoned = temporary_for(&root.path().join(V2_HEAD)).unwrap();
        create_private(&abandoned, &serde_json::to_vec(&successor).unwrap()).unwrap();
        assert_eq!(
            request_v2(
                root.path(),
                json!({
                    "op":"compare_and_swap_v2", "reservation":reservation,
                    "successor":successor
                })
            )
            .unwrap()
            .state,
            successor
        );
        assert!(!abandoned.exists());
    }

    #[test]
    fn v2_unknown_or_conflicting_temporary_stays_closed() {
        let (root, genesis, successor, reservation, _) = reserved_control_v2();
        let unknown = root.path().join(format!(".pending-{}", Uuid::new_v4()));
        create_private(&unknown, b"{}").unwrap();
        assert!(
            request_v2(
                root.path(),
                json!({
                    "op":"compare_and_swap_v2", "reservation":reservation,
                    "successor":successor
                })
            )
            .is_err()
        );
        assert!(unknown.exists());
        assert_eq!(
            read_json::<CustodyState>(&root.path().join(V2_HEAD)).unwrap(),
            genesis
        );

        let (root, genesis, successor, reservation, _) = reserved_control_v2();
        let conflicting = temporary_for(&root.path().join(V2_HISTORY_SEAL)).unwrap();
        create_private(&conflicting, b"{}").unwrap();
        assert!(
            request_v2(
                root.path(),
                json!({
                    "op":"compare_and_swap_v2", "reservation":reservation,
                    "successor":successor
                })
            )
            .is_err()
        );
        assert!(conflicting.exists());
        assert_eq!(
            read_json::<CustodyState>(&root.path().join(V2_HEAD)).unwrap(),
            genesis
        );
    }

    #[test]
    fn v2_unknown_or_partial_layout_does_not_migrate() {
        let (root, _) = fixture();
        create_private(&root.path().join("unknown.json"), b"{}")
            .expect("create unknown synthetic record");
        assert!(migrate_off_host_v1_to_v2(root.path()).is_err());
        assert!(!root.path().join(V2_ANCHOR).exists());
        assert!(!root.path().join(V2_HEAD).exists());

        let (root, _) = fixture_v2();
        fs::remove_file(root.path().join(V2_HEAD)).unwrap();
        assert!(
            request_v2(
                root.path(),
                json!({"op":"read_v2", "store_id":"synthetic-store"})
            )
            .is_err()
        );
        assert!(
            request(
                root.path(),
                json!({"op":"read", "store_id":"synthetic-store"})
            )
            .is_err()
        );

        // A formerly unsealed v2 history cannot be silently adopted as if
        // its old reservation bytes had been authenticated all along.
        let (root, _) = fixture_v2();
        fs::remove_file(root.path().join(V2_HISTORY_SEAL)).unwrap();
        assert!(
            request_v2(
                root.path(),
                json!({"op":"read_v2", "store_id":"synthetic-store"})
            )
            .is_err()
        );
    }

    #[test]
    fn v2_head_alias_and_unknown_history_fail_closed() {
        let (root, _) = fixture_v2();
        fs::hard_link(root.path().join(V2_HEAD), root.path().join("alias"))
            .expect("synthetic hardlink");
        assert!(
            request_v2(
                root.path(),
                json!({"op":"read_v2", "store_id":"synthetic-store"})
            )
            .is_err()
        );

        let (root, _) = fixture_v2();
        create_private(&root.path().join(V2_HISTORY).join("unexpected.json"), b"{}")
            .expect("synthetic unknown mirror");
        assert!(
            request_v2(
                root.path(),
                json!({"op":"read_v2", "store_id":"synthetic-store"})
            )
            .is_err()
        );
    }

    #[test]
    fn v2_rejects_old_history_intent_rewritten_without_changing_heads() {
        let (root, genesis) = fixture_v2();
        let mut first = genesis.clone();
        first.revision = 1;
        first.control.sequence = 1;
        first.control.current_hash = "b".repeat(64);
        commit_v2(root.path(), &genesis, &first, "control-1");

        let mut second = first.clone();
        second.revision = 2;
        second.control.sequence = 2;
        second.control.current_hash = "c".repeat(64);
        commit_v2(root.path(), &first, &second, "control-2");

        let old_record = root
            .path()
            .join(V2_HISTORY)
            .join("00000000000000000001.json");
        let mut rewritten: CommittedV2 = read_json(&old_record).unwrap();
        rewritten.reservation.operation_id = "rewritten-control-1".into();
        rewritten.reservation.intent_sha256 = "f".repeat(64);
        replace_private(&old_record, &serde_json::to_vec(&rewritten).unwrap()).unwrap();

        assert!(
            request_v2(
                root.path(),
                json!({"op":"read_v2", "store_id":"synthetic-store"})
            )
            .is_err(),
            "an old reservation rewrite must invalidate the custody chain"
        );
    }

    #[test]
    fn v2_rejects_seal_rewrite_without_matching_history() {
        let (root, genesis) = fixture_v2();
        let mut successor = genesis.clone();
        successor.revision = 1;
        successor.control.sequence = 1;
        successor.control.current_hash = "b".repeat(64);
        commit_v2(root.path(), &genesis, &successor, "control-1");

        let seal_path = root.path().join(V2_HISTORY_SEAL);
        let mut altered: HistorySealV2 = read_json(&seal_path).unwrap();
        let record = altered.latest.as_mut().unwrap();
        record.reservation.intent_sha256 = "f".repeat(64);
        altered.head_sha256 = history_next_hash(
            &history_genesis_hash(&read_json(&root.path().join(V2_ANCHOR)).unwrap()).unwrap(),
            record,
        )
        .unwrap();
        replace_private(&seal_path, &serde_json::to_vec(&altered).unwrap()).unwrap();

        assert!(
            request_v2(
                root.path(),
                json!({"op":"read_v2", "store_id":"synthetic-store"})
            )
            .is_err()
        );
    }

    #[test]
    fn forced_protocol_reserves_and_commits_exactly_once() {
        let (root, anchor) = fixture();
        let reserve = json!({
            "op": "reserve", "predecessor": anchor,
            "event_id": "event-1", "intent_sha256": "b".repeat(64)
        });
        let first = request(root.path(), reserve.clone()).unwrap();
        let reservation = first.reservation.unwrap();
        assert_eq!(
            request(root.path(), reserve.clone())
                .unwrap()
                .reservation
                .unwrap(),
            reservation
        );
        assert!(
            request(
                root.path(),
                json!({
                    "op": "reserve", "predecessor": anchor,
                    "event_id": "event-2", "intent_sha256": "b".repeat(64)
                })
            )
            .is_err()
        );
        let successor = ControlCheckpoint {
            store_id: anchor.store_id.clone(),
            sequence: 1,
            current_hash: "c".repeat(64),
        };
        let cas = json!({
            "op": "compare_and_swap", "reservation": reservation,
            "successor": successor
        });
        assert_eq!(
            request(root.path(), cas.clone()).unwrap().checkpoint,
            successor
        );
        assert_eq!(request(root.path(), cas).unwrap().checkpoint, successor);
        assert_eq!(
            request(root.path(), reserve).unwrap().reservation.unwrap(),
            reservation
        );
        assert!(
            request(
                root.path(),
                json!({
                    "op": "reserve", "predecessor": successor,
                    "event_id": "event-1", "intent_sha256": "d".repeat(64)
                })
            )
            .is_err()
        );
        assert_eq!(
            request(
                root.path(),
                json!({
                    "op": "read", "store_id": anchor.store_id
                })
            )
            .unwrap()
            .checkpoint,
            successor
        );
    }

    #[test]
    fn interrupted_cas_publishes_only_matching_immutable_mirror() {
        let (root, anchor) = fixture();
        let reservation = request(
            root.path(),
            json!({
                "op": "reserve", "predecessor": anchor,
                "event_id": "event-1", "intent_sha256": "b".repeat(64)
            }),
        )
        .unwrap()
        .reservation
        .unwrap();
        let successor = ControlCheckpoint {
            store_id: anchor.store_id.clone(),
            sequence: 1,
            current_hash: "c".repeat(64),
        };
        let committed = Committed {
            reservation: reservation.clone(),
            successor: successor.clone(),
        };
        replace_private(
            &root.path().join(HISTORY).join("00000000000000000001.json"),
            &serde_json::to_vec(&committed).unwrap(),
        )
        .unwrap();
        assert_eq!(
            request(
                root.path(),
                json!({
                    "op": "read", "store_id": anchor.store_id
                })
            )
            .unwrap()
            .checkpoint,
            successor
        );
        assert!(!root.path().join(PENDING).exists());
        assert_eq!(
            request(
                root.path(),
                json!({
                    "op": "compare_and_swap", "reservation": reservation,
                    "successor": successor
                })
            )
            .unwrap()
            .checkpoint,
            successor
        );
    }

    #[test]
    fn corrupted_history_forks_and_links_fail_closed() {
        let (root, anchor) = fixture();
        let history = root.path().join(HISTORY);
        create_private(&history.join("00000000000000000002.json"), b"{}").unwrap();
        assert!(
            request(
                root.path(),
                json!({
                    "op": "read", "store_id": anchor.store_id
                })
            )
            .is_err()
        );

        let (root, anchor) = fixture();
        let outside = TempDir::new().unwrap();
        let linked = root.path().join(HISTORY).join("00000000000000000001.json");
        #[cfg(unix)]
        {
            std::os::unix::fs::symlink(outside.path(), &linked).unwrap();
            assert!(
                request(
                    root.path(),
                    json!({
                        "op": "read", "store_id": anchor.store_id
                    })
                )
                .is_err()
            );
        }
    }

    #[test]
    fn altered_head_and_hardlink_are_rejected() {
        let (root, anchor) = fixture();
        let wrong = ControlCheckpoint {
            current_hash: "f".repeat(64),
            ..anchor.clone()
        };
        replace_private(
            &root.path().join(HEAD),
            &serde_json::to_vec(&wrong).unwrap(),
        )
        .unwrap();
        assert!(
            request(
                root.path(),
                json!({
                    "op": "read", "store_id": anchor.store_id
                })
            )
            .is_err()
        );

        let (root, anchor) = fixture();
        let alias = root.path().join("head-alias");
        fs::hard_link(root.path().join(HEAD), &alias).unwrap();
        assert!(
            request(
                root.path(),
                json!({
                    "op": "read", "store_id": anchor.store_id
                })
            )
            .is_err()
        );
    }

    #[test]
    fn rejects_unsafe_root_and_input_without_writing() {
        let (root, anchor) = fixture();
        let before = fs::read(root.path().join(HEAD)).unwrap();
        assert!(process_forced_request(root.path(), b"{}").is_err());
        assert!(
            request(
                root.path(),
                json!({
                    "op": "read", "store_id": "../other"
                })
            )
            .is_err()
        );
        assert_eq!(fs::read(root.path().join(HEAD)).unwrap(), before);
        assert!(initialize_off_host(root.path(), &anchor).is_err());
    }

    #[test]
    fn refuses_unpinned_or_option_like_ssh_target() {
        let path = PathBuf::from("/nonexistent/custody-key");
        assert!(
            SshCustody::new("-oProxyCommand=bad".to_owned(), path.clone(), path.clone()).is_err()
        );
        assert!(SshCustody::new("host with spaces".to_owned(), path.clone(), path).is_err());
    }

    #[test]
    fn ssh_transport_never_resolves_executable_from_path() {
        let client = SshCustody {
            host: "custodian@example.org".into(),
            pinned_known_hosts: PathBuf::from("/etc/boaz-health/known_hosts"),
            identity_file: PathBuf::from("/etc/boaz-health/custody-key"),
        };
        let command = client.ssh_command();
        assert_eq!(command.get_program(), std::ffi::OsStr::new("/usr/bin/ssh"));
        assert!(command.get_envs().all(|(_, value)| value.is_none()));
        assert!(validate_ssh_executable().is_ok());
    }

    #[test]
    fn ssh_identity_rejects_writable_or_linked_parent() {
        use std::os::unix::fs::PermissionsExt;

        let root = tempfile::tempdir().unwrap();
        let key = root.path().join("identity");
        let hosts = root.path().join("known_hosts");
        fs::write(&key, b"synthetic-key").unwrap();
        fs::write(&hosts, b"synthetic-host").unwrap();
        fs::set_permissions(&key, fs::Permissions::from_mode(0o400)).unwrap();
        fs::set_permissions(&hosts, fs::Permissions::from_mode(0o400)).unwrap();
        // Mode 0400 on a leaf does not prevent its owner from replacing it in
        // a directory owned by that same receiver UID.
        assert!(
            SshCustody::new("custodian@example.org".into(), hosts.clone(), key.clone()).is_err()
        );

        let alias = root.path().join("alias");
        std::os::unix::fs::symlink(root.path(), &alias).unwrap();
        assert!(validate_private_file(&alias.join("identity")).is_err());
    }

    #[test]
    fn custody_timeout_terminates_descendant_that_holds_stdout() {
        let root = tempfile::tempdir().unwrap();
        let marker = root.path().join("survivor");
        let mut command = Command::new("sh");
        command
            .env("CUSTODY_TEST_MARKER", &marker)
            .arg("-c")
            .arg("(sleep 0.5; printf alive > \"$CUSTODY_TEST_MARKER\") & wait");
        let result = run_custody_command(&mut command, b"{}", Duration::from_millis(100));
        assert!(
            matches!(result, Err(CustodyError::Io(ref error)) if error.kind() == io::ErrorKind::TimedOut)
        );
        std::thread::sleep(Duration::from_millis(650));
        assert!(
            !marker.exists(),
            "a timed-out custody process left a writer behind"
        );
    }

    #[test]
    fn stalled_custody_child_hits_one_overall_deadline() {
        let start = std::time::Instant::now();
        let mut stalled = Command::new("sh");
        stalled.arg("-c").arg("exec sleep 2");
        let result = run_custody_command(&mut stalled, b"{}", Duration::from_millis(150));
        assert!(
            matches!(result, Err(CustodyError::Io(ref error)) if error.kind() == std::io::ErrorKind::TimedOut),
            "stalled custody process must fail as a timeout: {result:?}"
        );
        assert!(
            start.elapsed() < Duration::from_secs(1),
            "custody deadline must bound the whole child invocation"
        );
    }

    #[test]
    fn custody_deadline_covers_partial_output_and_blocked_input() {
        let mut partial = Command::new("sh");
        partial.arg("-c").arg("printf '{}'; exec sleep 2");
        let result = run_custody_command(&mut partial, b"{}", Duration::from_millis(150));
        assert!(
            matches!(result, Err(CustodyError::Io(ref error)) if error.kind() == io::ErrorKind::TimedOut),
            "a partial reply is not custody acknowledgement: {result:?}"
        );

        let mut blocked_input = Command::new("sh");
        blocked_input.arg("-c").arg("exec sleep 2");
        let payload = vec![b'x'; 1_000_000];
        let result = run_custody_command(&mut blocked_input, &payload, Duration::from_millis(150));
        assert!(
            matches!(result, Err(CustodyError::Io(ref error)) if error.kind() == io::ErrorKind::TimedOut),
            "blocked child stdin must not stall indefinitely: {result:?}"
        );
    }

    #[test]
    fn custody_child_keeps_bounded_response_contract() {
        let mut complete = Command::new("sh");
        complete.arg("-c").arg("printf done");
        assert_eq!(
            run_custody_command(&mut complete, b"{}", Duration::from_secs(1)).unwrap(),
            b"done"
        );

        let mut oversized = Command::new("sh");
        oversized.arg("-c").arg("printf '%04097d' 0");
        assert!(matches!(
            run_custody_command(&mut oversized, b"{}", Duration::from_secs(1)),
            Err(CustodyError::Protocol(message)) if message == "oversized response"
        ));
    }

    #[test]
    fn successor_must_be_exactly_one_event_ahead() {
        let old = ControlCheckpoint {
            store_id: "store".to_owned(),
            sequence: 7,
            current_hash: "a".repeat(64),
        };
        let reservation = Reservation {
            reservation_id: "reservation".to_owned(),
            predecessor: old.clone(),
            event_id: "event".to_owned(),
            intent_sha256: "b".repeat(64),
        };
        let successor = ControlCheckpoint {
            sequence: 9,
            current_hash: "c".repeat(64),
            ..old
        };
        let client = SshCustody {
            host: "invalid".to_owned(),
            pinned_known_hosts: PathBuf::new(),
            identity_file: PathBuf::new(),
        };
        assert!(client.compare_and_swap(&reservation, &successor).is_err());
    }
}
