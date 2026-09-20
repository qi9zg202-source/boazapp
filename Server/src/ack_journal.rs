//! An append-only, separately placed source of exact acknowledged batch bytes.
//!
//! The operator must create and attest this directory in a recovery domain
//! independent of the health snapshot. Neither `open` nor a request handler
//! creates it. A missing, altered, or incomplete journal fails closed.

use crate::custody::{AckCheckpoint, CustodyState};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{
    collections::{BTreeMap, BTreeSet},
    fs::{self, File, OpenOptions},
    io::{self, Read, Write},
    path::{Path, PathBuf},
    sync::{
        Mutex,
        atomic::{AtomicBool, Ordering},
    },
};
use uuid::Uuid;

const FORMAT: u8 = 1;
const CUSTODY_PENDING: &str = "custody-pending.json";
const EMPTY_HASH: &str = "0000000000000000000000000000000000000000000000000000000000000000";

#[derive(Debug)]
pub struct AckJournal {
    root: PathBuf,
    root_identity: FileIdentity,
    inner: Mutex<Index>,
    poisoned: AtomicBool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PreparedBatch {
    pub sequence: u64,
    pub batch_id: String,
    pub device_id: String,
    pub content_hash: String,
    pub record_sha256: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PreparedPairing {
    pub record_id: String,
    pub device_id: String,
    pub code_hash: String,
    pub token_hash: String,
    pub paired_at: String,
    pub record_sha256: String,
}

#[derive(Debug, Clone)]
pub struct ConfirmedBatch {
    pub prepared: PreparedBatch,
    pub receipt: AckReceipt,
    pub raw: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AckReceipt {
    pub commit_sequence: i64,
    pub received_at: String,
    pub accepted_events: i64,
    pub changed_events: i64,
    pub requires_projection: bool,
}

#[derive(Debug, Clone)]
pub struct PairingRecoveryRecord {
    pub prepared: PreparedPairing,
    pub confirmed: bool,
}

/// An adoption statement, not proof of a backup by itself. The operator must
/// independently verify the named artifact, receipt inventory, and custody
/// checkpoint before binding it to this journal.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Baseline {
    pub snapshot_id: String,
    pub snapshot_sha256: String,
    pub receipt_inventory_sha256: String,
    pub control_store_id: String,
    pub control_head_sequence: i64,
    pub control_head_hash: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BaselineAnchor {
    pub format: u8,
    pub baseline: Baseline,
    pub journal_id: String,
    /// Hash of the journal head at adoption, not the mutable latest head.
    pub adoption_head_sha256: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct PairingDisk {
    format: u8,
    record_id: String,
    device_id: String,
    code_hash: String,
    token_hash: String,
    paired_at: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct PairingConfirmation {
    format: u8,
    sequence: u64,
    previous_hash: String,
    record_id: String,
    prepared_sha256: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct PreparedDisk {
    format: u8,
    sequence: u64,
    previous_hash: String,
    batch_id: String,
    device_id: String,
    content_hash: String,
    raw_hex: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ConfirmedDisk {
    format: u8,
    confirmation_sequence: u64,
    previous_confirmation_hash: String,
    sequence: u64,
    batch_id: String,
    device_id: String,
    content_hash: String,
    prepared_sha256: String,
    receipt: AckReceipt,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct HeadDisk {
    format: u8,
    journal_id: String,
    sequence: u64,
    prepared_sha256: String,
    confirmation_sequence: u64,
    confirmation_sha256: String,
    pairing_confirmation_sequence: u64,
    pairing_confirmation_sha256: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum AckCustodyKind {
    Batch {
        prepared: PreparedBatch,
        receipt: AckReceipt,
    },
    Pairing {
        pairing: PreparedPairing,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PendingAckCustody {
    pub format: u8,
    pub operation_id: String,
    pub predecessor: CustodyState,
    pub successor: CustodyState,
    pub confirmation: AckCustodyKind,
}

impl PendingAckCustody {
    pub fn intent_sha256(&self) -> io::Result<String> {
        Ok(digest(&json_bytes(self)?))
    }
}

#[derive(Debug)]
struct Index {
    head: HeadDisk,
    prepared: BTreeMap<String, PreparedBatch>,
    confirmed: BTreeMap<String, i64>,
    pairings: BTreeMap<String, PreparedPairing>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct FileIdentity {
    dev: u64,
    ino: u64,
}

fn invalid(message: impl Into<String>) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message.into())
}

fn digest(bytes: &[u8]) -> String {
    hex::encode(Sha256::digest(bytes))
}

fn json_bytes<T: Serialize>(value: &T) -> io::Result<Vec<u8>> {
    serde_json::to_vec(value).map_err(io::Error::other)
}

fn identity(metadata: &fs::Metadata) -> FileIdentity {
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        FileIdentity {
            dev: metadata.dev(),
            ino: metadata.ino(),
        }
    }
    #[cfg(not(unix))]
    {
        let _ = metadata;
        FileIdentity { dev: 0, ino: 0 }
    }
}

fn verified_dir(path: &Path) -> io::Result<FileIdentity> {
    let named = fs::symlink_metadata(path)?;
    if !named.file_type().is_dir() || named.file_type().is_symlink() {
        return Err(invalid("ack journal root must be a real directory"));
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        if named.permissions().mode() & 0o077 != 0 {
            return Err(invalid(
                "ack journal root must not be group/world accessible",
            ));
        }
    }
    Ok(identity(&named))
}

fn safe_read(path: &Path) -> io::Result<Vec<u8>> {
    let mut options = OpenOptions::new();
    options.read(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::{MetadataExt, OpenOptionsExt};
        options.custom_flags(libc::O_NOFOLLOW);
        let mut file = options.open(path)?;
        let opened = file.metadata()?;
        let named = fs::symlink_metadata(path)?;
        if !opened.is_file()
            || opened.nlink() != 1
            || opened.dev() != named.dev()
            || opened.ino() != named.ino()
        {
            return Err(invalid("ack journal file identity changed"));
        }
        if opened.len() > 1024 * 1024 {
            return Err(invalid("ack journal record is too large"));
        }
        let mut bytes = Vec::new();
        file.read_to_end(&mut bytes)?;
        Ok(bytes)
    }
    #[cfg(not(unix))]
    {
        let mut file = options.open(path)?;
        let mut bytes = Vec::new();
        file.read_to_end(&mut bytes)?;
        Ok(bytes)
    }
}

fn safe_write_new(path: &Path, bytes: &[u8]) -> io::Result<()> {
    let mut options = OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600).custom_flags(libc::O_NOFOLLOW);
    }
    let mut file = options.open(path)?;
    file.write_all(bytes)?;
    file.sync_all()?;
    let parent = path
        .parent()
        .ok_or_else(|| invalid("journal path has no parent"))?;
    File::open(parent)?.sync_all()?;
    Ok(())
}

fn prepared_name(sequence: u64) -> String {
    format!("{sequence:020}.prepared.json")
}

fn confirmed_name(sequence: u64) -> String {
    format!("{sequence:020}.confirmed.json")
}

fn pairing_prepared_name(record_id: &str) -> String {
    format!("pairing-{record_id}.prepared.json")
}

fn pairing_confirmed_name(record_id: &str) -> String {
    format!("pairing-{record_id}.confirmed.json")
}

fn parse_json<T: for<'a> Deserialize<'a>>(bytes: &[u8]) -> io::Result<T> {
    serde_json::from_slice(bytes).map_err(|error| invalid(error.to_string()))
}

impl AckJournal {
    fn initial_head(journal_id: String) -> HeadDisk {
        HeadDisk {
            format: FORMAT,
            journal_id,
            sequence: 0,
            prepared_sha256: EMPTY_HASH.to_owned(),
            confirmation_sequence: 0,
            confirmation_sha256: EMPTY_HASH.to_owned(),
            pairing_confirmation_sequence: 0,
            pairing_confirmation_sha256: EMPTY_HASH.to_owned(),
        }
    }

    fn validate_baseline(anchor: &BaselineAnchor, journal_id: &str) -> io::Result<()> {
        let baseline = &anchor.baseline;
        if anchor.format != FORMAT
            || anchor.journal_id != journal_id
            || baseline.snapshot_id.is_empty()
            || baseline.control_store_id.is_empty()
            || baseline.control_head_sequence < 0
            || !valid_sha(&baseline.snapshot_sha256)
            || !valid_sha(&baseline.receipt_inventory_sha256)
            || !valid_sha(&baseline.control_head_hash)
            || anchor.adoption_head_sha256
                != digest(&json_bytes(&Self::initial_head(journal_id.to_owned()))?)
        {
            return Err(invalid(
                "adoption baseline is invalid or bound to another journal",
            ));
        }
        Ok(())
    }

    /// Explicit bootstrap only. The caller must first attest the independent
    /// encrypted volume; this method does not create the directory.
    pub fn initialize(root: &Path) -> io::Result<()> {
        let metadata = fs::symlink_metadata(root)?;
        if !metadata.file_type().is_dir() || metadata.file_type().is_symlink() {
            return Err(invalid("ack journal root must be a real directory"));
        }
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            fs::set_permissions(root, fs::Permissions::from_mode(0o700))?;
        }
        if fs::read_dir(root)?.next().is_some() {
            return Err(invalid(
                "ack journal initialization requires an empty directory",
            ));
        }
        let head = Self::initial_head(Uuid::new_v4().to_string());
        safe_write_new(&root.join("head.json"), &json_bytes(&head)?)
    }

    pub fn open(root: &Path) -> io::Result<Self> {
        let root_identity = verified_dir(root)?;
        let head: HeadDisk = parse_json(&safe_read(&root.join("head.json"))?)?;
        if head.format != FORMAT
            || Uuid::parse_str(&head.journal_id).is_err()
            || !valid_sha(&head.prepared_sha256)
            || !valid_sha(&head.confirmation_sha256)
            || !valid_sha(&head.pairing_confirmation_sha256)
        {
            return Err(invalid("ack journal head format is invalid"));
        }
        let mut expected_names = BTreeSet::from(["head.json".to_owned()]);
        if root.join("baseline.json").exists() {
            let anchor: BaselineAnchor = parse_json(&safe_read(&root.join("baseline.json"))?)?;
            Self::validate_baseline(&anchor, &head.journal_id)?;
            expected_names.insert("baseline.json".to_owned());
        } else if head.sequence > 0 || head.pairing_confirmation_sequence > 0 {
            return Err(invalid("ack journal has writes but no adoption baseline"));
        }
        if root.join(CUSTODY_PENDING).exists() {
            let pending: PendingAckCustody = parse_json(&safe_read(&root.join(CUSTODY_PENDING))?)?;
            if pending.format != FORMAT
                || pending.operation_id.is_empty()
                || pending.predecessor.format != 2
                || pending.successor.format != 2
                || pending.successor.revision
                    != pending
                        .predecessor
                        .revision
                        .checked_add(1)
                        .ok_or_else(|| invalid("custody revision overflow"))?
                || pending.successor.control != pending.predecessor.control
                || pending.successor.baseline_sha256 != pending.predecessor.baseline_sha256
                || pending.successor.ack.is_none()
            {
                return Err(invalid("ack custody intent is invalid"));
            }
            expected_names.insert(CUSTODY_PENDING.to_owned());
        }
        let mut previous_hash = EMPTY_HASH.to_owned();
        let mut prepared = BTreeMap::new();
        let mut confirmed = BTreeMap::new();
        let mut confirmation_chain = BTreeMap::new();
        let mut pairings = BTreeMap::new();
        let mut pairing_confirmation_chain = BTreeMap::new();
        let mut seen_sequences = BTreeSet::new();
        for sequence in 1..=head.sequence {
            let name = prepared_name(sequence);
            let bytes = safe_read(&root.join(&name))?;
            let record: PreparedDisk = parse_json(&bytes)?;
            Self::validate_prepared(&record, sequence, &previous_hash)?;
            if prepared
                .insert(
                    record.batch_id.clone(),
                    PreparedBatch {
                        sequence,
                        batch_id: record.batch_id,
                        device_id: record.device_id,
                        content_hash: record.content_hash,
                        record_sha256: digest(&bytes),
                    },
                )
                .is_some()
            {
                return Err(invalid("duplicate batch ID in ack journal"));
            }
            previous_hash = digest(&bytes);
            expected_names.insert(name.clone());
            let confirmed_name = confirmed_name(sequence);
            if root.join(&confirmed_name).exists() {
                let bytes = safe_read(&root.join(&confirmed_name))?;
                let value: ConfirmedDisk = parse_json(&bytes)?;
                let entry = prepared
                    .values()
                    .find(|entry| entry.sequence == sequence)
                    .unwrap();
                Self::validate_confirmation(&value, entry, &previous_hash)?;
                if !seen_sequences.insert(value.receipt.commit_sequence) {
                    return Err(invalid("duplicate commit sequence in ack journal"));
                }
                confirmed.insert(entry.batch_id.clone(), value.receipt.commit_sequence);
                if confirmation_chain
                    .insert(value.confirmation_sequence, value)
                    .is_some()
                {
                    return Err(invalid("duplicate confirmation journal sequence"));
                }
                expected_names.insert(confirmed_name);
            }
        }
        if confirmation_chain.len() as u64 != head.confirmation_sequence {
            return Err(invalid("confirmation journal count differs from head"));
        }
        let mut confirmation_hash = EMPTY_HASH.to_owned();
        for sequence in 1..=head.confirmation_sequence {
            let value = confirmation_chain
                .get(&sequence)
                .ok_or_else(|| invalid("missing confirmation journal sequence"))?;
            if value.previous_confirmation_hash != confirmation_hash {
                return Err(invalid("confirmation journal chain differs"));
            }
            confirmation_hash = digest(&json_bytes(value)?);
        }
        if confirmation_hash != head.confirmation_sha256 {
            return Err(invalid("confirmation journal head differs"));
        }
        if previous_hash != head.prepared_sha256 {
            return Err(invalid("ack journal head does not match prepared chain"));
        }
        for item in fs::read_dir(root)? {
            let item = item?;
            let name = item.file_name().to_string_lossy().into_owned();
            let Some(record_id) = name
                .strip_prefix("pairing-")
                .and_then(|name| name.strip_suffix(".prepared.json"))
            else {
                continue;
            };
            Uuid::parse_str(record_id).map_err(|_| invalid("invalid pairing record ID"))?;
            let bytes = safe_read(&item.path())?;
            let disk: PairingDisk = parse_json(&bytes)?;
            if disk.format != FORMAT
                || disk.record_id != record_id
                || disk.device_id.is_empty()
                || !valid_sha(&disk.code_hash)
                || !valid_sha(&disk.token_hash)
                || chrono::DateTime::parse_from_rfc3339(&disk.paired_at).is_err()
            {
                return Err(invalid("invalid pairing recovery record"));
            }
            let pairing = PreparedPairing {
                record_id: record_id.to_owned(),
                device_id: disk.device_id,
                code_hash: disk.code_hash,
                token_hash: disk.token_hash,
                paired_at: disk.paired_at,
                record_sha256: digest(&bytes),
            };
            let confirmed_name = pairing_confirmed_name(record_id);
            if root.join(&confirmed_name).exists() {
                let confirmed: PairingConfirmation =
                    parse_json(&safe_read(&root.join(&confirmed_name))?)?;
                if confirmed.format != FORMAT
                    || confirmed.record_id != record_id
                    || confirmed.prepared_sha256 != pairing.record_sha256
                    || confirmed.sequence == 0
                    || !valid_sha(&confirmed.previous_hash)
                {
                    return Err(invalid("pairing confirmation is inconsistent"));
                }
                if pairing_confirmation_chain
                    .insert(confirmed.sequence, confirmed)
                    .is_some()
                {
                    return Err(invalid("duplicate pairing confirmation sequence"));
                }
                expected_names.insert(confirmed_name);
            }
            expected_names.insert(name.clone());
            pairings.insert(record_id.to_owned(), pairing);
        }
        if pairing_confirmation_chain.len() as u64 != head.pairing_confirmation_sequence {
            return Err(invalid("pairing confirmation count differs from head"));
        }
        let mut pairing_confirmation_hash = EMPTY_HASH.to_owned();
        for sequence in 1..=head.pairing_confirmation_sequence {
            let value = pairing_confirmation_chain
                .get(&sequence)
                .ok_or_else(|| invalid("missing pairing confirmation sequence"))?;
            if value.previous_hash != pairing_confirmation_hash {
                return Err(invalid("pairing confirmation chain differs"));
            }
            pairing_confirmation_hash = digest(&json_bytes(value)?);
        }
        if pairing_confirmation_hash != head.pairing_confirmation_sha256 {
            return Err(invalid("pairing confirmation head differs"));
        }
        for item in fs::read_dir(root)? {
            let item = item?;
            let name = item.file_name().to_string_lossy().into_owned();
            if !expected_names.contains(&name) {
                return Err(invalid(format!("unexpected ack journal entry: {name}")));
            }
        }
        Ok(Self {
            root: root.to_path_buf(),
            root_identity,
            inner: Mutex::new(Index {
                head,
                prepared,
                confirmed,
                pairings,
            }),
            poisoned: AtomicBool::new(false),
        })
    }

    fn check_root_and_head(&self, index: &Index) -> io::Result<()> {
        if self.poisoned.load(Ordering::SeqCst) {
            return Err(invalid("ack journal requires crash reconciliation"));
        }
        if verified_dir(&self.root)? != self.root_identity {
            return Err(invalid("ack journal directory identity changed"));
        }
        let disk: HeadDisk = parse_json(&safe_read(&self.root.join("head.json"))?)?;
        if disk.sequence != index.head.sequence
            || disk.prepared_sha256 != index.head.prepared_sha256
            || disk.confirmation_sequence != index.head.confirmation_sequence
            || disk.confirmation_sha256 != index.head.confirmation_sha256
            || disk.pairing_confirmation_sequence != index.head.pairing_confirmation_sequence
            || disk.pairing_confirmation_sha256 != index.head.pairing_confirmation_sha256
            || disk.format != FORMAT
            || disk.journal_id != index.head.journal_id
        {
            return Err(invalid("ack journal head changed unexpectedly"));
        }
        if index.head.sequence > 0 || index.head.pairing_confirmation_sequence > 0 {
            self.check_baseline(index)?;
        }
        Ok(())
    }

    fn check_baseline(&self, index: &Index) -> io::Result<()> {
        let anchor: BaselineAnchor = parse_json(&safe_read(&self.root.join("baseline.json"))?)?;
        Self::validate_baseline(&anchor, &index.head.journal_id)
    }

    fn validate_prepared(record: &PreparedDisk, sequence: u64, previous: &str) -> io::Result<()> {
        let raw = hex::decode(&record.raw_hex).map_err(|_| invalid("invalid raw batch hex"))?;
        if record.format != FORMAT
            || record.sequence != sequence
            || record.previous_hash != previous
            || record.batch_id.is_empty()
            || record.device_id.is_empty()
            || raw.len() > 128 * 1024
            || digest(&raw) != record.content_hash
        {
            return Err(invalid("prepared batch is inconsistent"));
        }
        Ok(())
    }

    fn validate_confirmation(
        value: &ConfirmedDisk,
        prepared: &PreparedBatch,
        prepared_sha256: &str,
    ) -> io::Result<()> {
        if value.format != FORMAT
            || value.confirmation_sequence == 0
            || !valid_sha(&value.previous_confirmation_hash)
            || value.sequence != prepared.sequence
            || value.batch_id != prepared.batch_id
            || value.device_id != prepared.device_id
            || value.content_hash != prepared.content_hash
            || value.prepared_sha256 != prepared_sha256
            || value.receipt.commit_sequence <= 0
            || value.receipt.accepted_events <= 0
            || value.receipt.accepted_events > 200
            || value.receipt.changed_events < 0
            || value.receipt.changed_events > value.receipt.accepted_events
            || chrono::DateTime::parse_from_rfc3339(&value.receipt.received_at).is_err()
        {
            return Err(invalid("confirmed receipt does not match prepared batch"));
        }
        Ok(())
    }

    fn read_prepared(&self, prepared: &PreparedBatch) -> io::Result<(PreparedDisk, String)> {
        let bytes = safe_read(&self.root.join(prepared_name(prepared.sequence)))?;
        let value: PreparedDisk = parse_json(&bytes)?;
        if value.sequence != prepared.sequence
            || value.batch_id != prepared.batch_id
            || value.device_id != prepared.device_id
            || value.content_hash != prepared.content_hash
            || digest(&bytes) != prepared.record_sha256
            || digest(&hex::decode(&value.raw_hex).map_err(|_| invalid("invalid batch hex"))?)
                != prepared.content_hash
        {
            return Err(invalid("prepared batch changed"));
        }
        Ok((value, digest(&bytes)))
    }

    pub fn prepare_batch(
        &self,
        batch_id: &str,
        device_id: &str,
        raw: &[u8],
    ) -> io::Result<PreparedBatch> {
        if batch_id.is_empty() || device_id.is_empty() || raw.len() > 128 * 1024 {
            return Err(invalid("invalid batch journal input"));
        }
        let content_hash = digest(raw);
        let mut index = self
            .inner
            .lock()
            .map_err(|_| invalid("ack journal lock poisoned"))?;
        self.check_root_and_head(&index)?;
        self.check_baseline(&index)?;
        if self.root.join(CUSTODY_PENDING).exists() {
            return Err(invalid("ack custody intent requires reconciliation"));
        }
        if let Some(existing) = index.prepared.get(batch_id) {
            let (record, _) = self.read_prepared(existing)?;
            if record.device_id == device_id
                && record.content_hash == content_hash
                && hex::decode(record.raw_hex).map_err(|_| invalid("invalid batch hex"))? == raw
            {
                return Ok(existing.clone());
            }
            return Err(invalid(
                "batch ID already prepared with different bytes or device",
            ));
        }
        let sequence = index
            .head
            .sequence
            .checked_add(1)
            .ok_or_else(|| invalid("ack sequence overflow"))?;
        let record = PreparedDisk {
            format: FORMAT,
            sequence,
            previous_hash: index.head.prepared_sha256.clone(),
            batch_id: batch_id.to_owned(),
            device_id: device_id.to_owned(),
            content_hash: content_hash.clone(),
            raw_hex: hex::encode(raw),
        };
        let bytes = json_bytes(&record)?;
        self.poisoned.store(true, Ordering::SeqCst);
        safe_write_new(&self.root.join(prepared_name(sequence)), &bytes)?;
        let head = HeadDisk {
            format: FORMAT,
            journal_id: index.head.journal_id.clone(),
            sequence,
            prepared_sha256: digest(&bytes),
            confirmation_sequence: index.head.confirmation_sequence,
            confirmation_sha256: index.head.confirmation_sha256.clone(),
            pairing_confirmation_sequence: index.head.pairing_confirmation_sequence,
            pairing_confirmation_sha256: index.head.pairing_confirmation_sha256.clone(),
        };
        let next = self.root.join("head.next");
        safe_write_new(&next, &json_bytes(&head)?)?;
        fs::rename(&next, self.root.join("head.json"))?;
        File::open(&self.root)?.sync_all()?;
        let prepared = PreparedBatch {
            sequence,
            batch_id: batch_id.to_owned(),
            device_id: device_id.to_owned(),
            content_hash,
            record_sha256: head.prepared_sha256.clone(),
        };
        index.head = head;
        index.prepared.insert(batch_id.to_owned(), prepared.clone());
        self.poisoned.store(false, Ordering::SeqCst);
        Ok(prepared)
    }

    /// Existing database receipts may only be completed from a PREPARED
    /// record that predates their commit. Never manufacture a new source for
    /// an old receipt after the fact.
    pub fn prepared_batch(
        &self,
        batch_id: &str,
        device_id: &str,
        raw: &[u8],
    ) -> io::Result<Option<PreparedBatch>> {
        let index = self
            .inner
            .lock()
            .map_err(|_| invalid("ack journal lock poisoned"))?;
        self.check_root_and_head(&index)?;
        let Some(prepared) = index.prepared.get(batch_id) else {
            return Ok(None);
        };
        let (disk, _) = self.read_prepared(prepared)?;
        if disk.device_id != device_id
            || hex::decode(disk.raw_hex).map_err(|_| invalid("invalid batch hex"))? != raw
        {
            return Err(invalid("prepared batch does not match retry bytes"));
        }
        Ok(Some(prepared.clone()))
    }

    pub fn confirm_batch(&self, prepared: &PreparedBatch, receipt: &AckReceipt) -> io::Result<()> {
        let mut index = self
            .inner
            .lock()
            .map_err(|_| invalid("ack journal lock poisoned"))?;
        self.check_root_and_head(&index)?;
        if index.prepared.get(&prepared.batch_id) != Some(prepared)
            || receipt.commit_sequence <= 0
            || receipt.accepted_events <= 0
            || receipt.accepted_events > 200
            || receipt.changed_events < 0
            || receipt.changed_events > receipt.accepted_events
            || chrono::DateTime::parse_from_rfc3339(&receipt.received_at).is_err()
        {
            return Err(invalid("confirmation lacks matching prepared batch"));
        }
        let (_, prepared_sha256) = self.read_prepared(prepared)?;
        let value = ConfirmedDisk {
            format: FORMAT,
            confirmation_sequence: index
                .head
                .confirmation_sequence
                .checked_add(1)
                .ok_or_else(|| invalid("confirmation sequence overflow"))?,
            previous_confirmation_hash: index.head.confirmation_sha256.clone(),
            sequence: prepared.sequence,
            batch_id: prepared.batch_id.clone(),
            device_id: prepared.device_id.clone(),
            content_hash: prepared.content_hash.clone(),
            prepared_sha256: prepared_sha256.clone(),
            receipt: receipt.clone(),
        };
        let path = self.root.join(confirmed_name(prepared.sequence));
        if let Some(existing) = index.confirmed.get(&prepared.batch_id) {
            let disk: ConfirmedDisk = parse_json(&safe_read(&path)?)?;
            Self::validate_confirmation(&disk, prepared, &prepared_sha256)?;
            if existing == &receipt.commit_sequence && disk.receipt == *receipt {
                return Ok(());
            }
            return Err(invalid("confirmation conflicts with durable record"));
        }
        if path.exists() {
            return Err(invalid("unexpected confirmation exists on disk"));
        }
        self.poisoned.store(true, Ordering::SeqCst);
        safe_write_new(&path, &json_bytes(&value)?)?;
        let head = HeadDisk {
            format: FORMAT,
            journal_id: index.head.journal_id.clone(),
            sequence: index.head.sequence,
            prepared_sha256: index.head.prepared_sha256.clone(),
            confirmation_sequence: value.confirmation_sequence,
            confirmation_sha256: digest(&json_bytes(&value)?),
            pairing_confirmation_sequence: index.head.pairing_confirmation_sequence,
            pairing_confirmation_sha256: index.head.pairing_confirmation_sha256.clone(),
        };
        let next = self.root.join("head.next");
        safe_write_new(&next, &json_bytes(&head)?)?;
        fs::rename(&next, self.root.join("head.json"))?;
        File::open(&self.root)?.sync_all()?;
        index.head = head;
        index
            .confirmed
            .insert(prepared.batch_id.clone(), receipt.commit_sequence);
        self.poisoned.store(false, Ordering::SeqCst);
        Ok(())
    }

    pub fn is_confirmed(
        &self,
        batch_id: &str,
        device_id: &str,
        content_hash: &str,
        commit_sequence: i64,
    ) -> io::Result<bool> {
        let index = self
            .inner
            .lock()
            .map_err(|_| invalid("ack journal lock poisoned"))?;
        self.check_root_and_head(&index)?;
        let Some(prepared) = index.prepared.get(batch_id) else {
            return Ok(false);
        };
        if prepared.device_id != device_id || prepared.content_hash != content_hash {
            return Err(invalid("receipt identity conflicts with ack journal"));
        }
        let (_, prepared_sha256) = self.read_prepared(prepared)?;
        let Some(confirmed_sequence) = index.confirmed.get(batch_id) else {
            if self.root.join(confirmed_name(prepared.sequence)).exists() {
                return Err(invalid("unindexed confirmation exists"));
            }
            return Ok(false);
        };
        let value: ConfirmedDisk = parse_json(&safe_read(
            &self.root.join(confirmed_name(prepared.sequence)),
        )?)?;
        Self::validate_confirmation(&value, prepared, &prepared_sha256)?;
        Ok(*confirmed_sequence == commit_sequence
            && value.receipt.commit_sequence == commit_sequence)
    }

    /// A receipt is externally visible only when every immutable receipt
    /// field still matches the independently persisted confirmation. Checking
    /// its sequence alone would permit a modified SQLite row to be returned.
    pub fn receipt_matches(
        &self,
        batch_id: &str,
        device_id: &str,
        content_hash: &str,
        receipt: &AckReceipt,
    ) -> io::Result<bool> {
        let index = self
            .inner
            .lock()
            .map_err(|_| invalid("ack journal lock poisoned"))?;
        self.check_root_and_head(&index)?;
        let Some(prepared) = index.prepared.get(batch_id) else {
            return Ok(false);
        };
        if prepared.device_id != device_id || prepared.content_hash != content_hash {
            return Err(invalid("receipt identity conflicts with ack journal"));
        }
        let (_, prepared_sha256) = self.read_prepared(prepared)?;
        let Some(confirmed_sequence) = index.confirmed.get(batch_id) else {
            if self.root.join(confirmed_name(prepared.sequence)).exists() {
                return Err(invalid("unindexed confirmation exists"));
            }
            return Ok(false);
        };
        let value: ConfirmedDisk = parse_json(&safe_read(
            &self.root.join(confirmed_name(prepared.sequence)),
        )?)?;
        Self::validate_confirmation(&value, prepared, &prepared_sha256)?;
        Ok(*confirmed_sequence == receipt.commit_sequence && value.receipt == *receipt)
    }

    pub fn raw_batch(&self, batch_id: &str) -> io::Result<Option<Vec<u8>>> {
        let index = self
            .inner
            .lock()
            .map_err(|_| invalid("ack journal lock poisoned"))?;
        self.check_root_and_head(&index)?;
        let Some(prepared) = index.prepared.get(batch_id) else {
            return Ok(None);
        };
        let (record, _) = self.read_prepared(prepared)?;
        let bytes = hex::decode(record.raw_hex).map_err(|_| invalid("invalid batch hex"))?;
        Ok(Some(bytes))
    }

    pub fn root(&self) -> &Path {
        &self.root
    }

    /// The complete, verified journal head to pin in independent custody.
    /// This includes prepared-only records: a custody checkpoint is never a
    /// substitute for checking the confirmation records themselves.
    pub fn checkpoint(&self) -> io::Result<AckCheckpoint> {
        let index = self
            .inner
            .lock()
            .map_err(|_| invalid("ack journal lock poisoned"))?;
        self.check_root_and_head(&index)?;
        let bytes = safe_read(&self.root.join("head.json"))?;
        if bytes != json_bytes(&index.head)? {
            return Err(invalid("ack journal head serialization changed"));
        }
        Ok(AckCheckpoint {
            journal_id: index.head.journal_id.clone(),
            sequence: index.head.sequence,
            confirmation_sequence: index.head.confirmation_sequence,
            pairing_confirmation_sequence: index.head.pairing_confirmation_sequence,
            head_sha256: digest(&bytes),
        })
    }

    /// A prepared-only tail may move the local head before an HTTP response
    /// exists. Verify that a custodied checkpoint is an exact historical
    /// prefix; a counter comparison alone would admit a rolled-back journal.
    pub fn contains_checkpoint(&self, checkpoint: &AckCheckpoint) -> io::Result<bool> {
        let index = self
            .inner
            .lock()
            .map_err(|_| invalid("ack journal lock poisoned"))?;
        self.check_root_and_head(&index)?;
        if checkpoint.journal_id != index.head.journal_id
            || checkpoint.sequence > index.head.sequence
            || checkpoint.confirmation_sequence > index.head.confirmation_sequence
            || checkpoint.pairing_confirmation_sequence > index.head.pairing_confirmation_sequence
            || !valid_sha(&checkpoint.head_sha256)
        {
            return Ok(false);
        }
        if checkpoint == &Self::checkpoint_from_head(&index.head)? {
            return Ok(true);
        }
        let prepared_sha256 = if checkpoint.sequence == 0 {
            EMPTY_HASH.to_owned()
        } else {
            digest(&safe_read(
                &self.root.join(prepared_name(checkpoint.sequence)),
            )?)
        };
        let mut confirmation_sha256 = EMPTY_HASH.to_owned();
        let mut confirmation_batch_sequence = 0;
        if checkpoint.confirmation_sequence > 0 {
            for prepared in index.prepared.values() {
                let path = self.root.join(confirmed_name(prepared.sequence));
                if !path.exists() {
                    continue;
                }
                let bytes = safe_read(&path)?;
                let value: ConfirmedDisk = parse_json(&bytes)?;
                if value.confirmation_sequence == checkpoint.confirmation_sequence {
                    confirmation_sha256 = digest(&bytes);
                    confirmation_batch_sequence = prepared.sequence;
                    break;
                }
            }
            if confirmation_batch_sequence == 0 || confirmation_batch_sequence > checkpoint.sequence
            {
                return Ok(false);
            }
        }
        let mut pairing_confirmation_sha256 = EMPTY_HASH.to_owned();
        if checkpoint.pairing_confirmation_sequence > 0 {
            let mut found = false;
            for pairing in index.pairings.values() {
                let path = self.root.join(pairing_confirmed_name(&pairing.record_id));
                if !path.exists() {
                    continue;
                }
                let bytes = safe_read(&path)?;
                let value: PairingConfirmation = parse_json(&bytes)?;
                if value.sequence == checkpoint.pairing_confirmation_sequence {
                    pairing_confirmation_sha256 = digest(&bytes);
                    found = true;
                    break;
                }
            }
            if !found {
                return Ok(false);
            }
        }
        let historic = HeadDisk {
            format: FORMAT,
            journal_id: index.head.journal_id.clone(),
            sequence: checkpoint.sequence,
            prepared_sha256,
            confirmation_sequence: checkpoint.confirmation_sequence,
            confirmation_sha256,
            pairing_confirmation_sequence: checkpoint.pairing_confirmation_sequence,
            pairing_confirmation_sha256,
        };
        Ok(checkpoint.head_sha256 == digest(&json_bytes(&historic)?))
    }

    fn checkpoint_from_head(head: &HeadDisk) -> io::Result<AckCheckpoint> {
        Ok(AckCheckpoint {
            journal_id: head.journal_id.clone(),
            sequence: head.sequence,
            confirmation_sequence: head.confirmation_sequence,
            pairing_confirmation_sequence: head.pairing_confirmation_sequence,
            head_sha256: digest(&json_bytes(head)?),
        })
    }

    /// The exact immutable baseline bytes, not merely the caller-supplied
    /// snapshot or receipt hash fields. Replacing baseline.json with another
    /// syntactically valid statement must therefore conflict with custody.
    pub fn baseline_sha256(&self) -> io::Result<Option<String>> {
        let index = self
            .inner
            .lock()
            .map_err(|_| invalid("ack journal lock poisoned"))?;
        self.check_root_and_head(&index)?;
        let path = self.root.join("baseline.json");
        if !path.exists() {
            return Ok(None);
        }
        let bytes = safe_read(&path)?;
        let anchor: BaselineAnchor = parse_json(&bytes)?;
        Self::validate_baseline(&anchor, &index.head.journal_id)?;
        if bytes != json_bytes(&anchor)? {
            return Err(invalid("baseline serialization changed"));
        }
        Ok(Some(digest(&bytes)))
    }

    /// Check the complete custody tuple against this opened, verified journal.
    /// A prepared-only local tail may be newer than the independently held
    /// checkpoint, but every confirmed batch and pairing must be covered.
    /// Revision arithmetic is restricted to generation-zero adoption: that
    /// path accepts only a genesis control chain followed by one backup event.
    pub fn verify_generation_zero_custody(&self, state: &CustodyState) -> io::Result<()> {
        if state.format != 2 {
            return Err(invalid("unknown custody state format"));
        }
        if self.pending_custody_intent()?.is_some() {
            return Err(invalid("ack custody intent requires reconciliation"));
        }
        let local_baseline = self.baseline_sha256()?;
        if state.baseline_sha256 != local_baseline {
            return Err(invalid("custody baseline differs from local journal"));
        }
        let local_head = self.checkpoint()?;
        let expected_revision = match (self.baseline()?, state.ack.as_ref()) {
            (None, None) => {
                if local_head.sequence != 0
                    || local_head.confirmation_sequence != 0
                    || local_head.pairing_confirmation_sequence != 0
                    || !(0..=1).contains(&state.control.sequence)
                {
                    return Err(invalid("unadopted custody state is not a fresh genesis"));
                }
                u64::try_from(state.control.sequence)
                    .map_err(|_| invalid("negative control sequence"))?
            }
            (Some(anchor), Some(ack)) => {
                if anchor.baseline.control_store_id != state.control.store_id
                    || anchor.baseline.control_head_sequence != 1
                    || state.control.sequence < anchor.baseline.control_head_sequence
                    || (state.control.sequence == 1
                        && state.control.current_hash != anchor.baseline.control_head_hash)
                    || !self.contains_checkpoint(ack)?
                    || ack.confirmation_sequence != local_head.confirmation_sequence
                    || ack.pairing_confirmation_sequence != local_head.pairing_confirmation_sequence
                {
                    return Err(invalid(
                        "custody journal head is not the confirmed local prefix",
                    ));
                }
                u64::try_from(state.control.sequence)
                    .map_err(|_| invalid("negative control sequence"))?
                    .checked_add(1)
                    .and_then(|value| value.checked_add(ack.confirmation_sequence))
                    .and_then(|value| value.checked_add(ack.pairing_confirmation_sequence))
                    .ok_or_else(|| invalid("custody revision overflow"))?
            }
            _ => return Err(invalid("custody baseline and journal binding differ")),
        };
        if state.revision != expected_revision {
            return Err(invalid(
                "custody revision differs from local authority history",
            ));
        }
        // Detect a replacement between the two journal reads above. This is
        // not a substitute for separate service-UID and directory controls.
        if self.baseline_sha256()? != local_baseline || self.checkpoint()? != local_head {
            return Err(invalid("local journal changed during custody verification"));
        }
        Ok(())
    }

    /// A durable, single-flight custody intent. The caller must hold the
    /// shared cross-process custody-operation lock from the initial read of
    /// `predecessor` through CAS and `finish_custody_intent`.
    pub fn start_batch_custody_intent(
        &self,
        prepared: &PreparedBatch,
        receipt: &AckReceipt,
        predecessor: &CustodyState,
    ) -> io::Result<PendingAckCustody> {
        let local_baseline = self
            .baseline_sha256()?
            .ok_or_else(|| invalid("ack journal baseline is missing"))?;
        if predecessor.baseline_sha256.as_deref() != Some(&local_baseline)
            || predecessor.format != 2
        {
            return Err(invalid("custody baseline differs from local journal"));
        }
        let anchored = predecessor
            .ack
            .as_ref()
            .ok_or_else(|| invalid("custody has no acknowledged journal head"))?;
        if !self.contains_checkpoint(anchored)? {
            return Err(invalid("custody journal head is not a local prefix"));
        }
        let index = self
            .inner
            .lock()
            .map_err(|_| invalid("ack journal lock poisoned"))?;
        self.check_root_and_head(&index)?;
        if self.root.join(CUSTODY_PENDING).exists() {
            return Err(invalid(
                "an ack custody intent already requires reconciliation",
            ));
        }
        if anchored.confirmation_sequence != index.head.confirmation_sequence
            || anchored.pairing_confirmation_sequence != index.head.pairing_confirmation_sequence
            || index.confirmed.contains_key(&prepared.batch_id)
            || index.prepared.get(&prepared.batch_id) != Some(prepared)
        {
            return Err(invalid("ack custody predecessor or batch is inconsistent"));
        }
        let (_, prepared_sha256) = self.read_prepared(prepared)?;
        let value = ConfirmedDisk {
            format: FORMAT,
            confirmation_sequence: index
                .head
                .confirmation_sequence
                .checked_add(1)
                .ok_or_else(|| invalid("confirmation sequence overflow"))?,
            previous_confirmation_hash: index.head.confirmation_sha256.clone(),
            sequence: prepared.sequence,
            batch_id: prepared.batch_id.clone(),
            device_id: prepared.device_id.clone(),
            content_hash: prepared.content_hash.clone(),
            prepared_sha256,
            receipt: receipt.clone(),
        };
        Self::validate_confirmation(&value, prepared, &prepared.record_sha256)?;
        let next_head = HeadDisk {
            format: FORMAT,
            journal_id: index.head.journal_id.clone(),
            sequence: index.head.sequence,
            prepared_sha256: index.head.prepared_sha256.clone(),
            confirmation_sequence: value.confirmation_sequence,
            confirmation_sha256: digest(&json_bytes(&value)?),
            pairing_confirmation_sequence: index.head.pairing_confirmation_sequence,
            pairing_confirmation_sha256: index.head.pairing_confirmation_sha256.clone(),
        };
        let mut successor = predecessor.clone();
        successor.revision = successor
            .revision
            .checked_add(1)
            .ok_or_else(|| invalid("custody revision overflow"))?;
        successor.ack = Some(Self::checkpoint_from_head(&next_head)?);
        let intent = PendingAckCustody {
            format: FORMAT,
            operation_id: format!("ack-batch-{}", digest(prepared.batch_id.as_bytes())),
            predecessor: predecessor.clone(),
            successor,
            confirmation: AckCustodyKind::Batch {
                prepared: prepared.clone(),
                receipt: receipt.clone(),
            },
        };
        safe_write_new(&self.root.join(CUSTODY_PENDING), &json_bytes(&intent)?)?;
        Ok(intent)
    }

    pub fn start_pairing_custody_intent(
        &self,
        pairing: &PreparedPairing,
        predecessor: &CustodyState,
    ) -> io::Result<PendingAckCustody> {
        let local_baseline = self
            .baseline_sha256()?
            .ok_or_else(|| invalid("ack journal baseline is missing"))?;
        if predecessor.baseline_sha256.as_deref() != Some(&local_baseline)
            || predecessor.format != 2
        {
            return Err(invalid("custody baseline differs from local journal"));
        }
        let anchored = predecessor
            .ack
            .as_ref()
            .ok_or_else(|| invalid("custody has no acknowledged journal head"))?;
        if !self.contains_checkpoint(anchored)? {
            return Err(invalid("custody journal head is not a local prefix"));
        }
        let index = self
            .inner
            .lock()
            .map_err(|_| invalid("ack journal lock poisoned"))?;
        self.check_root_and_head(&index)?;
        if self.root.join(CUSTODY_PENDING).exists() {
            return Err(invalid(
                "an ack custody intent already requires reconciliation",
            ));
        }
        if anchored.confirmation_sequence != index.head.confirmation_sequence
            || anchored.pairing_confirmation_sequence != index.head.pairing_confirmation_sequence
            || index.pairings.get(&pairing.record_id) != Some(pairing)
            || self
                .root
                .join(pairing_confirmed_name(&pairing.record_id))
                .exists()
        {
            return Err(invalid(
                "ack custody predecessor or pairing is inconsistent",
            ));
        }
        let bytes = safe_read(&self.root.join(pairing_prepared_name(&pairing.record_id)))?;
        if digest(&bytes) != pairing.record_sha256 {
            return Err(invalid("prepared pairing changed"));
        }
        let confirmation = PairingConfirmation {
            format: FORMAT,
            sequence: index
                .head
                .pairing_confirmation_sequence
                .checked_add(1)
                .ok_or_else(|| invalid("pairing confirmation sequence overflow"))?,
            previous_hash: index.head.pairing_confirmation_sha256.clone(),
            record_id: pairing.record_id.clone(),
            prepared_sha256: pairing.record_sha256.clone(),
        };
        let next_head = HeadDisk {
            format: FORMAT,
            journal_id: index.head.journal_id.clone(),
            sequence: index.head.sequence,
            prepared_sha256: index.head.prepared_sha256.clone(),
            confirmation_sequence: index.head.confirmation_sequence,
            confirmation_sha256: index.head.confirmation_sha256.clone(),
            pairing_confirmation_sequence: confirmation.sequence,
            pairing_confirmation_sha256: digest(&json_bytes(&confirmation)?),
        };
        let mut successor = predecessor.clone();
        successor.revision = successor
            .revision
            .checked_add(1)
            .ok_or_else(|| invalid("custody revision overflow"))?;
        successor.ack = Some(Self::checkpoint_from_head(&next_head)?);
        let intent = PendingAckCustody {
            format: FORMAT,
            operation_id: format!("ack-pairing-{}", pairing.record_id),
            predecessor: predecessor.clone(),
            successor,
            confirmation: AckCustodyKind::Pairing {
                pairing: pairing.clone(),
            },
        };
        safe_write_new(&self.root.join(CUSTODY_PENDING), &json_bytes(&intent)?)?;
        Ok(intent)
    }

    pub fn pending_custody_intent(&self) -> io::Result<Option<PendingAckCustody>> {
        let index = self
            .inner
            .lock()
            .map_err(|_| invalid("ack journal lock poisoned"))?;
        self.check_root_and_head(&index)?;
        let path = self.root.join(CUSTODY_PENDING);
        if !path.exists() {
            return Ok(None);
        }
        let value: PendingAckCustody = parse_json(&safe_read(&path)?)?;
        if value.format != FORMAT
            || value.predecessor.format != 2
            || value.successor.format != 2
            || value.successor.revision
                != value
                    .predecessor
                    .revision
                    .checked_add(1)
                    .ok_or_else(|| invalid("custody revision overflow"))?
            || value.successor.control != value.predecessor.control
            || value.successor.baseline_sha256 != value.predecessor.baseline_sha256
            || value.successor.ack.is_none()
        {
            return Err(invalid("ack custody intent is inconsistent"));
        }
        Ok(Some(value))
    }

    pub fn finish_custody_intent(
        &self,
        intent: &PendingAckCustody,
        confirmed: &CustodyState,
    ) -> io::Result<()> {
        let index = self
            .inner
            .lock()
            .map_err(|_| invalid("ack journal lock poisoned"))?;
        self.check_root_and_head(&index)?;
        let path = self.root.join(CUSTODY_PENDING);
        let disk: PendingAckCustody = parse_json(&safe_read(&path)?)?;
        if &disk != intent
            || confirmed != &intent.successor
            || confirmed.ack.as_ref() != Some(&Self::checkpoint_from_head(&index.head)?)
        {
            return Err(invalid(
                "custody completion differs from local journal head",
            ));
        }
        fs::remove_file(&path)?;
        File::open(&self.root)?.sync_all()?;
        Ok(())
    }

    /// Bind a verified generation-0 snapshot exactly once, before any
    /// acknowledged batch or pairing. This does not attest the snapshot;
    /// production callers must do that before invoking this method.
    pub fn bind_baseline(&self, baseline: &Baseline) -> io::Result<BaselineAnchor> {
        let index = self
            .inner
            .lock()
            .map_err(|_| invalid("ack journal lock poisoned"))?;
        self.check_root_and_head(&index)?;
        if index.head.sequence != 0
            || index.head.confirmation_sequence != 0
            || index.head.pairing_confirmation_sequence != 0
            || !index.pairings.is_empty()
        {
            return Err(invalid(
                "baseline must precede every recovery journal write",
            ));
        }
        let anchor = BaselineAnchor {
            format: FORMAT,
            baseline: baseline.clone(),
            journal_id: index.head.journal_id.clone(),
            adoption_head_sha256: digest(&json_bytes(&index.head)?),
        };
        Self::validate_baseline(&anchor, &index.head.journal_id)?;
        let path = self.root.join("baseline.json");
        if path.exists() {
            let prior: BaselineAnchor = parse_json(&safe_read(&path)?)?;
            if prior == anchor {
                return Ok(prior);
            }
            return Err(invalid("baseline is immutable"));
        }
        self.poisoned.store(true, Ordering::SeqCst);
        safe_write_new(&path, &json_bytes(&anchor)?)?;
        self.poisoned.store(false, Ordering::SeqCst);
        Ok(anchor)
    }

    pub fn baseline(&self) -> io::Result<Option<BaselineAnchor>> {
        let index = self
            .inner
            .lock()
            .map_err(|_| invalid("ack journal lock poisoned"))?;
        self.check_root_and_head(&index)?;
        let path = self.root.join("baseline.json");
        if !path.exists() {
            if index.head.sequence > 0 || index.head.pairing_confirmation_sequence > 0 {
                return Err(invalid("journal writes exist without an adoption baseline"));
            }
            return Ok(None);
        }
        let anchor: BaselineAnchor = parse_json(&safe_read(&path)?)?;
        Self::validate_baseline(&anchor, &index.head.journal_id)?;
        Ok(Some(anchor))
    }

    /// Recovery must compare this complete, ordered source with the candidate
    /// ledger; a maximum sequence or count alone is never sufficient.
    pub fn confirmed_batches(&self) -> io::Result<Vec<ConfirmedBatch>> {
        let index = self
            .inner
            .lock()
            .map_err(|_| invalid("ack journal lock poisoned"))?;
        self.check_root_and_head(&index)?;
        let mut ordered: Vec<_> = index.prepared.values().cloned().collect();
        ordered.sort_by_key(|record| record.sequence);
        let mut result = Vec::new();
        for prepared in ordered {
            let (disk, prepared_sha256) = self.read_prepared(&prepared)?;
            let Some(commit_sequence) = index.confirmed.get(&prepared.batch_id) else {
                continue;
            };
            let value: ConfirmedDisk = parse_json(&safe_read(
                &self.root.join(confirmed_name(prepared.sequence)),
            )?)?;
            Self::validate_confirmation(&value, &prepared, &prepared_sha256)?;
            if value.receipt.commit_sequence != *commit_sequence {
                return Err(invalid("confirmation index changed"));
            }
            result.push(ConfirmedBatch {
                prepared,
                receipt: value.receipt,
                raw: hex::decode(disk.raw_hex).map_err(|_| invalid("invalid batch hex"))?,
            });
        }
        Ok(result)
    }

    pub fn pairing_records(&self) -> io::Result<Vec<PairingRecoveryRecord>> {
        let index = self
            .inner
            .lock()
            .map_err(|_| invalid("ack journal lock poisoned"))?;
        self.check_root_and_head(&index)?;
        let mut result = Vec::new();
        for prepared in index.pairings.values() {
            let bytes = safe_read(&self.root.join(pairing_prepared_name(&prepared.record_id)))?;
            if digest(&bytes) != prepared.record_sha256 {
                return Err(invalid("pairing recovery record changed"));
            }
            let confirmation_path = self.root.join(pairing_confirmed_name(&prepared.record_id));
            let confirmed = if confirmation_path.exists() {
                let value: PairingConfirmation = parse_json(&safe_read(&confirmation_path)?)?;
                if value.format != FORMAT
                    || value.record_id != prepared.record_id
                    || value.prepared_sha256 != prepared.record_sha256
                {
                    return Err(invalid("pairing confirmation changed"));
                }
                true
            } else {
                false
            };
            result.push(PairingRecoveryRecord {
                prepared: prepared.clone(),
                confirmed,
            });
        }
        Ok(result)
    }

    /// Persist recovery identity without persisting a plaintext credential.
    /// A crash after the health commit but before confirmation requires an
    /// operator reconciliation and client re-pairing; the old token cannot be
    /// replayed to the client and must never be guessed.
    pub fn prepare_pairing(
        &self,
        device_id: &str,
        code_hash: &str,
        token_hash: &str,
        paired_at: &str,
    ) -> io::Result<PreparedPairing> {
        if device_id.is_empty()
            || !valid_sha(code_hash)
            || !valid_sha(token_hash)
            || chrono::DateTime::parse_from_rfc3339(paired_at).is_err()
        {
            return Err(invalid("invalid pairing recovery identity"));
        }
        let mut index = self
            .inner
            .lock()
            .map_err(|_| invalid("ack journal lock poisoned"))?;
        self.check_root_and_head(&index)?;
        self.check_baseline(&index)?;
        if self.root.join(CUSTODY_PENDING).exists() {
            return Err(invalid("ack custody intent requires reconciliation"));
        }
        let record_id = Uuid::new_v4().to_string();
        let disk = PairingDisk {
            format: FORMAT,
            record_id: record_id.clone(),
            device_id: device_id.to_owned(),
            code_hash: code_hash.to_owned(),
            token_hash: token_hash.to_owned(),
            paired_at: paired_at.to_owned(),
        };
        let bytes = json_bytes(&disk)?;
        self.poisoned.store(true, Ordering::SeqCst);
        safe_write_new(&self.root.join(pairing_prepared_name(&record_id)), &bytes)?;
        let pairing = PreparedPairing {
            record_id: record_id.clone(),
            device_id: device_id.to_owned(),
            code_hash: code_hash.to_owned(),
            token_hash: token_hash.to_owned(),
            paired_at: paired_at.to_owned(),
            record_sha256: digest(&bytes),
        };
        index.pairings.insert(record_id, pairing.clone());
        self.poisoned.store(false, Ordering::SeqCst);
        Ok(pairing)
    }

    pub fn confirm_pairing(&self, pairing: &PreparedPairing) -> io::Result<()> {
        let mut index = self
            .inner
            .lock()
            .map_err(|_| invalid("ack journal lock poisoned"))?;
        self.check_root_and_head(&index)?;
        if index.pairings.get(&pairing.record_id) != Some(pairing) {
            return Err(invalid("pairing recovery identity changed"));
        }
        let bytes = safe_read(&self.root.join(pairing_prepared_name(&pairing.record_id)))?;
        if digest(&bytes) != pairing.record_sha256 {
            return Err(invalid("prepared pairing changed"));
        }
        let path = self.root.join(pairing_confirmed_name(&pairing.record_id));
        if path.exists() {
            let existing: PairingConfirmation = parse_json(&safe_read(&path)?)?;
            if existing.format == FORMAT
                && existing.record_id == pairing.record_id
                && existing.prepared_sha256 == pairing.record_sha256
            {
                return Ok(());
            }
            return Err(invalid("pairing confirmation changed"));
        }
        let confirmation = PairingConfirmation {
            format: FORMAT,
            sequence: index
                .head
                .pairing_confirmation_sequence
                .checked_add(1)
                .ok_or_else(|| invalid("pairing confirmation sequence overflow"))?,
            previous_hash: index.head.pairing_confirmation_sha256.clone(),
            record_id: pairing.record_id.clone(),
            prepared_sha256: pairing.record_sha256.clone(),
        };
        self.poisoned.store(true, Ordering::SeqCst);
        safe_write_new(&path, &json_bytes(&confirmation)?)?;
        let head = HeadDisk {
            format: FORMAT,
            journal_id: index.head.journal_id.clone(),
            sequence: index.head.sequence,
            prepared_sha256: index.head.prepared_sha256.clone(),
            confirmation_sequence: index.head.confirmation_sequence,
            confirmation_sha256: index.head.confirmation_sha256.clone(),
            pairing_confirmation_sequence: confirmation.sequence,
            pairing_confirmation_sha256: digest(&json_bytes(&confirmation)?),
        };
        let next = self.root.join("head.next");
        safe_write_new(&next, &json_bytes(&head)?)?;
        fs::rename(&next, self.root.join("head.json"))?;
        File::open(&self.root)?.sync_all()?;
        index.head = head;
        self.poisoned.store(false, Ordering::SeqCst);
        Ok(())
    }
}

fn valid_sha(value: &str) -> bool {
    value.len() == 64 && value.bytes().all(|byte| byte.is_ascii_hexdigit())
}
