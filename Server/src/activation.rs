//! Durable, fail-closed coordination for an offline recovery cutover.
//!
//! This module never starts VictoriaMetrics, copies a health ledger, or
//! authorizes serving. The caller must hold `CoordinatorGuard`, verify the
//! independent custody head, and validate the health/control/VM evidence
//! before advancing a phase. A journal divergence is deliberately not healed
//! without an explicit proof-checking resume call.

use crate::{
    control::{ControlCheckpoint, ControlStore},
    recovery::{RecoveryError, RecoveryResult},
};
use fs2::FileExt;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{
    collections::BTreeSet,
    fs::{self, File, OpenOptions},
    io::{self, Read, Write},
    path::{Component, Path, PathBuf},
};

const FORMAT_VERSION: u8 = 1;
const LOCK_NAME: &str = ".lifecycle.lock";
const CUSTODY_LOCK_NAME: &str = ".custody-operation.lock";
const JOURNALS_DIR: &str = "restore-journals";
const CURRENT_RESTORE_NAME: &str = "current-restore.json";
const HEAD_NAME: &str = "head.json";
const ACTIVE_NAME: &str = "active-set.json";
const ADOPTED_NAME: &str = "adopted-unactivated.json";
const GENERATIONS_DIR: &str = "generations";
const ZERO_HASH: &str = "0000000000000000000000000000000000000000000000000000000000000000";

#[derive(Debug)]
pub struct CoordinatorGuard {
    root: PathBuf,
    directory_identity: (u64, u64),
    exclusive: bool,
    _lock: File,
}

impl CoordinatorGuard {
    pub fn root(&self) -> &Path {
        &self.root
    }

    pub fn is_exclusive(&self) -> bool {
        self.exclusive
    }

    fn verify_root(&self) -> RecoveryResult<()> {
        validate_private_directory(&self.root)?;
        if directory_identity(&self.root)? != self.directory_identity {
            return Err(RecoveryError::Integrity(
                "coordination directory was replaced after acquiring its lock".to_owned(),
            ));
        }
        Ok(())
    }

    fn require_exclusive(&self) -> RecoveryResult<()> {
        self.verify_root()?;
        if !self.exclusive {
            return Err(RecoveryError::Integrity(
                "recovery state mutation requires the exclusive coordinator lock".to_owned(),
            ));
        }
        Ok(())
    }
}

/// Creates only a private coordination directory. It intentionally creates
/// neither an active pointer nor a journal; initial adoption is a separate
/// verified operation and an absent pointer must not be treated as legacy OK.
pub fn initialize_coordinator(root: &Path) -> RecoveryResult<()> {
    validate_absolute(root)?;
    let parent = root.parent().ok_or_else(|| {
        RecoveryError::InvalidPath("coordination directory has no parent".to_owned())
    })?;
    validate_real_directory_components(parent)?;
    if fs::symlink_metadata(root).is_ok() {
        return Err(RecoveryError::InvalidPath(
            "coordination directory already exists; initialization is not adoption".to_owned(),
        ));
    }
    fs::create_dir(root)?;
    set_private_directory(root)?;
    let custody_lock = root.join(CUSTODY_LOCK_NAME);
    let file = open_new_nofollow(&custody_lock)?;
    file.sync_all()?;
    sync_directory(root)?;
    sync_directory(parent)?;
    validate_private_directory(root)
}

pub fn custody_operation_lock_path(guard: &CoordinatorGuard) -> RecoveryResult<PathBuf> {
    guard.require_exclusive()?;
    let path = guard.root.join(CUSTODY_LOCK_NAME);
    validate_single_link_file(&path)?;
    Ok(path)
}

/// One-time upgrade for a coordinator created before the custody-wide lock
/// existed. It is forbidden once adoption or restore state has been written.
pub fn provision_custody_operation_lock_before_adoption(
    guard: &CoordinatorGuard,
) -> RecoveryResult<PathBuf> {
    guard.require_exclusive()?;
    for name in [ADOPTED_NAME, ACTIVE_NAME, CURRENT_RESTORE_NAME] {
        if fs::symlink_metadata(guard.root.join(name)).is_ok() {
            return custody_operation_lock_path(guard);
        }
    }
    let path = guard.root.join(CUSTODY_LOCK_NAME);
    match open_new_nofollow(&path) {
        Ok(file) => {
            file.sync_all()?;
            sync_directory(&guard.root)?;
        }
        Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {}
        Err(error) => return Err(error.into()),
    }
    custody_operation_lock_path(guard)
}

/// Acquires the one persistent lock before any active-set or storage path is
/// resolved. The directory must already exist and have private permissions.
pub fn lock_coordinator(
    root: &Path,
    exclusive: bool,
    try_only: bool,
) -> RecoveryResult<CoordinatorGuard> {
    validate_private_directory(root)?;
    let before = directory_identity(root)?;
    let path = root.join(LOCK_NAME);
    let lock = match open_new_nofollow(&path) {
        Ok(file) => {
            file.sync_all()?;
            sync_directory(root)?;
            file
        }
        Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {
            open_existing_nofollow(&path)?
        }
        Err(error) => return Err(error.into()),
    };
    validate_single_link_file(&path)?;
    if file_identity(&lock)? != regular_file_identity(&path)? {
        return Err(RecoveryError::Integrity(
            "coordinator lock file changed during opening".to_owned(),
        ));
    }
    match (exclusive, try_only) {
        (true, true) => FileExt::try_lock_exclusive(&lock)?,
        (true, false) => FileExt::lock_exclusive(&lock)?,
        (false, true) => FileExt::try_lock_shared(&lock)?,
        (false, false) => FileExt::lock_shared(&lock)?,
    }
    if directory_identity(root)? != before {
        return Err(RecoveryError::Integrity(
            "coordination directory changed during lock acquisition".to_owned(),
        ));
    }
    Ok(CoordinatorGuard {
        root: root.to_path_buf(),
        directory_identity: before,
        exclusive,
        _lock: lock,
    })
}

/// A generation-zero custody receipt. This is deliberately not an active-set
/// pointer and can never authorize the receiver or projection worker to start.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct AdoptionRecord {
    pub format_version: u8,
    pub snapshot_id: String,
    pub snapshot_sha256: String,
    pub receipt_inventory_sha256: String,
    pub control_checkpoint: ControlCheckpoint,
    pub baseline_sha256: String,
    pub custody_revision: u64,
}

impl AdoptionRecord {
    fn validate(&self) -> RecoveryResult<()> {
        if self.format_version != FORMAT_VERSION || self.custody_revision == 0 {
            return Err(RecoveryError::InvalidArtifact(
                "adoption format or custody revision is invalid".to_owned(),
            ));
        }
        validate_uuid(&self.snapshot_id, "adoption snapshot ID")?;
        validate_hex(&self.snapshot_sha256, "adoption snapshot hash")?;
        validate_hex(
            &self.receipt_inventory_sha256,
            "adoption receipt inventory hash",
        )?;
        validate_hex(&self.baseline_sha256, "adoption baseline hash")?;
        validate_checkpoint(&self.control_checkpoint)
    }
}

pub fn publish_adopted_unactivated(
    guard: &CoordinatorGuard,
    record: &AdoptionRecord,
) -> RecoveryResult<()> {
    guard.require_exclusive()?;
    record.validate()?;
    if fs::symlink_metadata(guard.root.join(ACTIVE_NAME)).is_ok()
        || fs::symlink_metadata(guard.root.join(CURRENT_RESTORE_NAME)).is_ok()
    {
        return Err(RecoveryError::Integrity(
            "generation-zero adoption cannot overwrite a recovery or active generation".to_owned(),
        ));
    }
    let path = guard.root.join(ADOPTED_NAME);
    if fs::symlink_metadata(&path).is_ok() {
        validate_single_link_file(&path)?;
        let prior: AdoptionRecord = serde_json::from_slice(&fs::read(&path)?)?;
        prior.validate()?;
        if &prior == record {
            return Ok(());
        }
        return Err(RecoveryError::Integrity(
            "adoption record is immutable and differs from custody".to_owned(),
        ));
    }
    write_new_json(&path, record)?;
    sync_directory(&guard.root)
}

pub fn read_adopted_unactivated(guard: &CoordinatorGuard) -> RecoveryResult<AdoptionRecord> {
    guard.verify_root()?;
    let path = guard.root.join(ADOPTED_NAME);
    validate_single_link_file(&path)?;
    let record: AdoptionRecord = serde_json::from_slice(&fs::read(path)?)?;
    record.validate()?;
    Ok(record)
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum RecoveryPhase {
    Frozen,
    ControlReplayed,
    CandidateReady,
    MetricsVerified,
    ControlCompleted,
    CustodyAdvanced,
    Activating,
    ActiveVerified,
}

impl RecoveryPhase {
    fn successor(self) -> Option<Self> {
        Some(match self {
            Self::Frozen => Self::ControlReplayed,
            Self::ControlReplayed => Self::CandidateReady,
            Self::CandidateReady => Self::MetricsVerified,
            Self::MetricsVerified => Self::ControlCompleted,
            Self::ControlCompleted => Self::CustodyAdvanced,
            Self::CustodyAdvanced => Self::Activating,
            Self::Activating => Self::ActiveVerified,
            Self::ActiveVerified => return None,
        })
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct RecoveryIntent {
    pub restore_epoch: String,
    pub snapshot_id: String,
    pub expected_control_checkpoint: ControlCheckpoint,
    pub previous_generation_id: Option<String>,
    pub source_commit_sequence: Option<i64>,
}

impl RecoveryIntent {
    fn validate(&self) -> RecoveryResult<()> {
        validate_hex(&self.restore_epoch, "restore epoch")?;
        validate_uuid(&self.snapshot_id, "health snapshot ID")?;
        if let Some(id) = &self.previous_generation_id {
            validate_uuid(id, "previous generation ID")?;
        }
        if self
            .source_commit_sequence
            .is_some_and(|sequence| sequence < 0)
        {
            return Err(RecoveryError::InvalidArtifact(
                "source commit sequence may not be negative".to_owned(),
            ));
        }
        validate_checkpoint(&self.expected_control_checkpoint)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct RecoveryRecord {
    pub format_version: u8,
    pub sequence: u64,
    pub phase: RecoveryPhase,
    pub intent: RecoveryIntent,
    pub evidence_sha256: String,
    pub previous_hash: String,
    pub current_hash: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
struct JournalHead {
    format_version: u8,
    sequence: u64,
    current_hash: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
struct CurrentRestore {
    format_version: u8,
    restore_epoch: String,
}

#[derive(Debug, Clone)]
pub struct RecoveryJournal {
    pub current: RecoveryRecord,
}

impl RecoveryJournal {
    /// Starts a fresh restore. `frozen_evidence_sha256` must be the caller's
    /// canonical proof of stopped writes, inventory, and expected head.
    pub fn begin(
        guard: &CoordinatorGuard,
        intent: RecoveryIntent,
        frozen_evidence_sha256: &str,
    ) -> RecoveryResult<Self> {
        guard.require_exclusive()?;
        intent.validate()?;
        validate_hex(frozen_evidence_sha256, "freeze evidence digest")?;
        let journals_root = guard.root.join(JOURNALS_DIR);
        let current_path = guard.root.join(CURRENT_RESTORE_NAME);
        if fs::symlink_metadata(&current_path).is_ok() {
            let prior = Self::load(guard)?;
            if prior.current.phase != RecoveryPhase::ActiveVerified {
                return Err(RecoveryError::Integrity(
                    "a prior restore remains unfinished; it must not be bypassed".to_owned(),
                ));
            }
            if intent.previous_generation_id.is_none() {
                return Err(RecoveryError::Integrity(
                    "a second-generation restore must bind the active generation it replaces"
                        .to_owned(),
                ));
            }
        }
        if fs::symlink_metadata(&journals_root).is_err() {
            fs::create_dir(&journals_root)?;
            set_private_directory(&journals_root)?;
            sync_directory(&guard.root)?;
        }
        validate_private_directory(&journals_root)?;
        let journal_dir = journal_dir(guard, &intent.restore_epoch);
        if fs::symlink_metadata(&journal_dir).is_ok() {
            return Err(RecoveryError::Integrity(
                "restore epoch journal already exists; resume or inspect it instead".to_owned(),
            ));
        }
        check_previous_active_set(guard, intent.previous_generation_id.as_deref())?;
        fs::create_dir(&journal_dir)?;
        set_private_directory(&journal_dir)?;
        sync_directory(&journals_root)?;
        let record = make_record(
            0,
            RecoveryPhase::Frozen,
            intent,
            frozen_evidence_sha256,
            ZERO_HASH,
        )?;
        write_new_json(&journal_dir.join(record_name(0)), &record)?;
        sync_directory(&journal_dir)?;
        publish_head(&journal_dir, &record)?;
        replace_json(
            &current_path,
            &CurrentRestore {
                format_version: FORMAT_VERSION,
                restore_epoch: record.intent.restore_epoch.clone(),
            },
        )?;
        Ok(Self { current: record })
    }

    /// Fails closed if a record is missing, modified, out of order, or ahead
    /// of the published head. A single unheaded record needs explicit resume.
    pub fn load(guard: &CoordinatorGuard) -> RecoveryResult<Self> {
        guard.verify_root()?;
        let current_path = guard.root.join(CURRENT_RESTORE_NAME);
        validate_single_link_file(&current_path)?;
        let current: CurrentRestore = serde_json::from_slice(&fs::read(current_path)?)?;
        if current.format_version != FORMAT_VERSION {
            return Err(RecoveryError::InvalidArtifact(
                "current restore selector version is unsupported".to_owned(),
            ));
        }
        validate_hex(&current.restore_epoch, "restore epoch")?;
        let journals_root = guard.root.join(JOURNALS_DIR);
        validate_private_directory(&journals_root)?;
        let epochs = directory_entry_names(&journals_root)?;
        if !epochs.contains(&current.restore_epoch) {
            return Err(RecoveryError::Integrity(
                "current restore journal is missing".to_owned(),
            ));
        }
        let mut selected = None;
        for epoch in epochs {
            validate_hex(&epoch, "restore journal directory name")?;
            let journal = Self::load_epoch(guard, &epoch)?;
            if epoch == current.restore_epoch {
                selected = Some(journal);
            } else if journal.current.phase != RecoveryPhase::ActiveVerified {
                return Err(RecoveryError::Integrity(
                    "an unpublished or incomplete historical restore journal exists".to_owned(),
                ));
            }
        }
        selected.ok_or_else(|| {
            RecoveryError::Integrity("current restore journal is missing".to_owned())
        })
    }

    /// Loads a historical epoch without changing the current restore selector.
    pub fn load_epoch(guard: &CoordinatorGuard, epoch: &str) -> RecoveryResult<Self> {
        guard.verify_root()?;
        validate_hex(epoch, "restore epoch")?;
        let journal_dir = journal_dir(guard, epoch);
        validate_private_directory(&journal_dir)?;
        let head_path = journal_dir.join(HEAD_NAME);
        validate_single_link_file(&head_path)?;
        let head: JournalHead = serde_json::from_slice(&fs::read(head_path)?)?;
        if head.format_version != FORMAT_VERSION {
            return Err(RecoveryError::InvalidArtifact(
                "restore journal head version is unsupported".to_owned(),
            ));
        }
        validate_hex(&head.current_hash, "restore journal head hash")?;
        let entries = directory_entry_names(&journal_dir)?;
        let expected = (0..=head.sequence)
            .map(record_name)
            .chain(std::iter::once(HEAD_NAME.to_owned()))
            .collect::<BTreeSet<_>>();
        if entries != expected {
            return Err(RecoveryError::Integrity(
                "restore journal inventory differs from its published head".to_owned(),
            ));
        }
        let mut previous_hash = ZERO_HASH.to_owned();
        let mut previous_phase = None;
        let mut first_intent: Option<RecoveryIntent> = None;
        let mut last = None;
        for sequence in 0..=head.sequence {
            let path = journal_dir.join(record_name(sequence));
            validate_single_link_file(&path)?;
            let record: RecoveryRecord = serde_json::from_slice(&fs::read(path)?)?;
            verify_record(
                &record,
                sequence,
                &previous_hash,
                previous_phase,
                first_intent.as_ref(),
            )?;
            if first_intent.is_none() {
                first_intent = Some(record.intent.clone());
            }
            previous_hash = record.current_hash.clone();
            previous_phase = Some(record.phase);
            last = Some(record);
        }
        if previous_hash != head.current_hash {
            return Err(RecoveryError::Integrity(
                "restore journal head differs from its final record".to_owned(),
            ));
        }
        let current = last.expect("journal genesis is required");
        if current.intent.restore_epoch != epoch {
            return Err(RecoveryError::Integrity(
                "restore journal directory does not match its epoch".to_owned(),
            ));
        }
        Ok(Self { current })
    }

    /// Repairs only the crash window after a new epoch's genesis was synced
    /// but before `current-restore.json` was published. The supplied verifier
    /// must recheck the freeze, writer exclusion, expected head and inventory.
    pub fn resume_unselected_epoch<F>(
        guard: &CoordinatorGuard,
        epoch: &str,
        verify_freeze: F,
    ) -> RecoveryResult<Self>
    where
        F: FnOnce(&RecoveryRecord) -> RecoveryResult<()>,
    {
        guard.require_exclusive()?;
        validate_hex(epoch, "restore epoch")?;
        let journals_root = guard.root.join(JOURNALS_DIR);
        validate_private_directory(&journals_root)?;
        let current_path = guard.root.join(CURRENT_RESTORE_NAME);
        let selected_epoch = match fs::symlink_metadata(&current_path) {
            Ok(_) => Some(Self::load_current_selector(guard)?.restore_epoch),
            Err(error) if error.kind() == io::ErrorKind::NotFound => None,
            Err(error) => return Err(error.into()),
        };
        if selected_epoch.as_deref() == Some(epoch) {
            return Self::load(guard);
        }
        let epochs = directory_entry_names(&journals_root)?;
        if !epochs.contains(epoch) {
            return Err(RecoveryError::Integrity(
                "unselected restore epoch does not exist".to_owned(),
            ));
        }
        let mut incomplete = Vec::new();
        for candidate in epochs {
            validate_hex(&candidate, "restore journal directory name")?;
            let journal = Self::load_epoch(guard, &candidate)?;
            if journal.current.phase != RecoveryPhase::ActiveVerified {
                incomplete.push(candidate);
            }
        }
        if incomplete != [epoch.to_owned()] {
            return Err(RecoveryError::Integrity(
                "there is not exactly one unselected incomplete restore epoch".to_owned(),
            ));
        }
        if let Some(prior) = &selected_epoch
            && Self::load_epoch(guard, prior)?.current.phase != RecoveryPhase::ActiveVerified
        {
            return Err(RecoveryError::Integrity(
                "selected predecessor restore is not complete".to_owned(),
            ));
        }
        let new_journal = Self::load_epoch(guard, epoch)?;
        if new_journal.current.phase != RecoveryPhase::Frozen {
            return Err(RecoveryError::Integrity(
                "unselected restore epoch advanced beyond its freeze record".to_owned(),
            ));
        }
        check_previous_active_set(
            guard,
            new_journal.current.intent.previous_generation_id.as_deref(),
        )?;
        verify_freeze(&new_journal.current)?;
        replace_json(
            &current_path,
            &CurrentRestore {
                format_version: FORMAT_VERSION,
                restore_epoch: epoch.to_owned(),
            },
        )?;
        Self::load(guard)
    }

    /// Append one immutable phase record and atomically publish its head.
    /// Repeating the same phase with the same evidence is idempotent.
    pub fn advance(
        &mut self,
        guard: &CoordinatorGuard,
        next: RecoveryPhase,
        evidence_sha256: &str,
    ) -> RecoveryResult<()> {
        guard.require_exclusive()?;
        validate_hex(evidence_sha256, "phase evidence digest")?;
        let reloaded = Self::load(guard)?;
        if reloaded.current != self.current {
            return Err(RecoveryError::Integrity(
                "restore journal changed since it was loaded".to_owned(),
            ));
        }
        if next == self.current.phase && evidence_sha256 == self.current.evidence_sha256 {
            return Ok(());
        }
        if self.current.phase.successor() != Some(next) {
            return Err(RecoveryError::Integrity(
                "restore phase transition is not permitted".to_owned(),
            ));
        }
        let sequence = self.current.sequence.checked_add(1).ok_or_else(|| {
            RecoveryError::Integrity("restore journal sequence overflow".to_owned())
        })?;
        let record = make_record(
            sequence,
            next,
            self.current.intent.clone(),
            evidence_sha256,
            &self.current.current_hash,
        )?;
        let journal_dir = journal_dir(guard, &self.current.intent.restore_epoch);
        write_new_json(&journal_dir.join(record_name(sequence)), &record)?;
        sync_directory(&journal_dir)?;
        publish_head(&journal_dir, &record)?;
        self.current = record;
        Ok(())
    }

    /// Completes only the narrowly defined crash boundary where exactly one
    /// next record was synced but its head was not published. The verifier must
    /// independently recheck that phase's external SQLite/VM/custody proof.
    pub fn resume_unpublished<F>(guard: &CoordinatorGuard, verify: F) -> RecoveryResult<Self>
    where
        F: FnOnce(&RecoveryRecord) -> RecoveryResult<()>,
    {
        guard.require_exclusive()?;
        let current = Self::load_current_selector(guard)?;
        let journal_dir = journal_dir(guard, &current.restore_epoch);
        validate_private_directory(&journal_dir)?;
        let head_path = journal_dir.join(HEAD_NAME);
        validate_single_link_file(&head_path)?;
        let head: JournalHead = serde_json::from_slice(&fs::read(&head_path)?)?;
        if head.format_version != FORMAT_VERSION {
            return Err(RecoveryError::InvalidArtifact(
                "restore journal head version is unsupported".to_owned(),
            ));
        }
        let next_sequence = head.sequence.checked_add(1).ok_or_else(|| {
            RecoveryError::Integrity("restore journal sequence overflow".to_owned())
        })?;
        let entries = directory_entry_names(&journal_dir)?;
        let expected = (0..=next_sequence)
            .map(record_name)
            .chain(std::iter::once(HEAD_NAME.to_owned()))
            .collect::<BTreeSet<_>>();
        if entries != expected {
            return Err(RecoveryError::Integrity(
                "there is not exactly one unpublished restore record".to_owned(),
            ));
        }
        // Verify the complete published prefix without accepting the extra
        // record as a normal load.
        let mut prior_hash = ZERO_HASH.to_owned();
        let mut prior_phase = None;
        let mut intent = None;
        for sequence in 0..=head.sequence {
            let path = journal_dir.join(record_name(sequence));
            validate_single_link_file(&path)?;
            let record: RecoveryRecord = serde_json::from_slice(&fs::read(path)?)?;
            verify_record(&record, sequence, &prior_hash, prior_phase, intent.as_ref())?;
            if intent.is_none() {
                intent = Some(record.intent.clone());
            }
            prior_hash = record.current_hash;
            prior_phase = Some(record.phase);
        }
        if prior_hash != head.current_hash {
            return Err(RecoveryError::Integrity(
                "published restore journal prefix has a divergent head".to_owned(),
            ));
        }
        let next_path = journal_dir.join(record_name(next_sequence));
        validate_single_link_file(&next_path)?;
        let record: RecoveryRecord = serde_json::from_slice(&fs::read(next_path)?)?;
        verify_record(
            &record,
            next_sequence,
            &prior_hash,
            prior_phase,
            intent.as_ref(),
        )?;
        verify(&record)?;
        publish_head(&journal_dir, &record)?;
        Self::load(guard)
    }

    fn load_current_selector(guard: &CoordinatorGuard) -> RecoveryResult<CurrentRestore> {
        let path = guard.root.join(CURRENT_RESTORE_NAME);
        validate_single_link_file(&path)?;
        let current: CurrentRestore = serde_json::from_slice(&fs::read(path)?)?;
        if current.format_version != FORMAT_VERSION {
            return Err(RecoveryError::InvalidArtifact(
                "current restore selector version is unsupported".to_owned(),
            ));
        }
        validate_hex(&current.restore_epoch, "restore epoch")?;
        Ok(current)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct PathIdentity {
    pub path: PathBuf,
    pub device: u64,
    pub inode: u64,
    pub is_directory: bool,
    pub sha256: Option<String>,
}

impl PathIdentity {
    pub fn capture_file(path: &Path) -> RecoveryResult<Self> {
        validate_single_link_file(path)?;
        reject_sqlite_sidecars(path)?;
        let (device, inode) = regular_file_identity(path)?;
        Ok(Self {
            path: path.to_path_buf(),
            device,
            inode,
            is_directory: false,
            sha256: Some(file_sha256(path)?),
        })
    }

    pub fn capture_directory(path: &Path) -> RecoveryResult<Self> {
        validate_real_directory_components(path)?;
        let (device, inode) = directory_identity(path)?;
        Ok(Self {
            path: path.to_path_buf(),
            device,
            inode,
            is_directory: true,
            sha256: None,
        })
    }

    pub fn verify(&self) -> RecoveryResult<()> {
        self.verify_location()?;
        validate_absolute(&self.path)?;
        let actual = if self.is_directory {
            if self.sha256.is_some() {
                return Err(RecoveryError::InvalidArtifact(
                    "directory identity must not claim a file hash".to_owned(),
                ));
            }
            Self::capture_directory(&self.path)?
        } else {
            validate_hex(
                self.sha256.as_deref().ok_or_else(|| {
                    RecoveryError::InvalidArtifact("file identity lacks SHA-256".to_owned())
                })?,
                "file identity SHA-256",
            )?;
            Self::capture_file(&self.path)?
        };
        if &actual != self {
            return Err(RecoveryError::Integrity(
                "recovery path identity or contents changed".to_owned(),
            ));
        }
        Ok(())
    }

    /// Identity check for a database after activation. Runtime SQLite writes
    /// change its initial SHA-256, so that hash is cutover evidence only.
    pub fn verify_location(&self) -> RecoveryResult<()> {
        validate_absolute(&self.path)?;
        if self.is_directory {
            validate_real_directory_components(&self.path)?;
            if self.sha256.is_some() {
                return Err(RecoveryError::InvalidArtifact(
                    "directory identity must not claim a file hash".to_owned(),
                ));
            }
        } else {
            validate_single_link_file(&self.path)?;
            validate_hex(
                self.sha256.as_deref().ok_or_else(|| {
                    RecoveryError::InvalidArtifact("file identity lacks SHA-256".to_owned())
                })?,
                "file identity SHA-256",
            )?;
        }
        let actual = if self.is_directory {
            directory_identity(&self.path)?
        } else {
            regular_file_identity(&self.path)?
        };
        if actual != (self.device, self.inode) {
            return Err(RecoveryError::Integrity(
                "recovery path device/inode changed".to_owned(),
            ));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ActivationManifest {
    pub format_version: u8,
    pub generation_id: String,
    pub restore_epoch: String,
    pub snapshot_id: String,
    pub health: PathIdentity,
    pub control: PathIdentity,
    pub vm_storage: PathIdentity,
    pub control_checkpoint: ControlCheckpoint,
    pub vm_generation_id: String,
    pub mapping_version: i64,
    pub oracle_sha256: String,
    pub full_readback_sha256: String,
    pub verified_at: String,
}

impl ActivationManifest {
    fn validate_metadata(&self) -> RecoveryResult<()> {
        if self.format_version != FORMAT_VERSION {
            return Err(RecoveryError::InvalidArtifact(
                "activation manifest version is unsupported".to_owned(),
            ));
        }
        validate_uuid(&self.generation_id, "activation generation ID")?;
        validate_hex(&self.restore_epoch, "restore epoch")?;
        validate_uuid(&self.snapshot_id, "health snapshot ID")?;
        validate_uuid(&self.vm_generation_id, "VM generation ID")?;
        validate_hex(&self.oracle_sha256, "projection oracle SHA-256")?;
        validate_hex(&self.full_readback_sha256, "full VM readback SHA-256")?;
        if self.mapping_version <= 0 {
            return Err(RecoveryError::InvalidArtifact(
                "mapping version must be positive".to_owned(),
            ));
        }
        chrono::DateTime::parse_from_rfc3339(&self.verified_at).map_err(|_| {
            RecoveryError::InvalidArtifact("projection verification time is invalid".to_owned())
        })?;
        validate_checkpoint(&self.control_checkpoint)?;
        if self.health.is_directory || self.control.is_directory || !self.vm_storage.is_directory {
            return Err(RecoveryError::InvalidArtifact(
                "activation storage kinds are incompatible".to_owned(),
            ));
        }
        if self.health.path == self.control.path
            || self.vm_storage.path.starts_with(&self.health.path)
            || self.vm_storage.path.starts_with(&self.control.path)
            || self.health.device == self.control.device
            || self.health.device == self.vm_storage.device
            || self.control.device == self.vm_storage.device
        {
            return Err(RecoveryError::InvalidPath(
                "activation requires physically separate health, control and VM domains".to_owned(),
            ));
        }
        Ok(())
    }

    /// Cutover-time proof for quiescent, single-file SQLite candidates and a
    /// native VM directory. Runtime DB files can change only after activation.
    pub fn validate(&self) -> RecoveryResult<()> {
        self.validate_metadata()?;
        self.health.verify()?;
        self.control.verify()?;
        self.vm_storage.verify()?;
        Ok(())
    }

    pub fn sha256(&self) -> RecoveryResult<String> {
        self.validate()?;
        Ok(hex::encode(Sha256::digest(serde_json::to_vec(self)?)))
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ActiveSet {
    pub format_version: u8,
    pub generation_id: String,
    pub manifest_sha256: String,
}

/// Publishes one pointer, not three cross-volume renames. This requires a
/// completed custody phase, an explicit Activating record bound to exactly
/// this manifest, and the independently supplied current expected head.
/// The caller must have already completed native VM readback and stopped all
/// writers. No network endpoint or service is started here.
pub fn publish_active_set(
    guard: &CoordinatorGuard,
    journal: &RecoveryJournal,
    manifest: &ActivationManifest,
    independent_expected_head: &ControlCheckpoint,
) -> RecoveryResult<ActiveSet> {
    guard.require_exclusive()?;
    let reloaded = RecoveryJournal::load(guard)?;
    if reloaded.current != journal.current || journal.current.phase != RecoveryPhase::Activating {
        return Err(RecoveryError::Integrity(
            "active-set publication requires the current Activating record".to_owned(),
        ));
    }
    manifest.validate()?;
    if manifest.control.device != guard.directory_identity.0
        || manifest.health.device == guard.directory_identity.0
        || manifest.vm_storage.device == guard.directory_identity.0
    {
        return Err(RecoveryError::InvalidPath(
            "candidate databases and VM are not on their final independent volumes".to_owned(),
        ));
    }
    if manifest.restore_epoch != journal.current.intent.restore_epoch
        || manifest.snapshot_id != journal.current.intent.snapshot_id
        || &manifest.control_checkpoint != independent_expected_head
    {
        return Err(RecoveryError::Integrity(
            "activation manifest does not match the restore intent and independent head".to_owned(),
        ));
    }
    let digest = manifest.sha256()?;
    if journal.current.evidence_sha256 != digest {
        return Err(RecoveryError::Integrity(
            "Activating journal evidence does not bind this manifest".to_owned(),
        ));
    }
    let pointer_path = guard.root.join(ACTIVE_NAME);
    let already_published = if fs::symlink_metadata(&pointer_path).is_ok() {
        validate_single_link_file(&pointer_path)?;
        let existing: ActiveSet = serde_json::from_slice(&fs::read(&pointer_path)?)?;
        existing.generation_id == manifest.generation_id && existing.manifest_sha256 == digest
    } else {
        false
    };
    if !already_published {
        check_previous_active_set(
            guard,
            journal.current.intent.previous_generation_id.as_deref(),
        )?;
    }
    let generations_dir = guard.root.join(GENERATIONS_DIR);
    if fs::symlink_metadata(&generations_dir).is_err() {
        fs::create_dir(&generations_dir)?;
        set_private_directory(&generations_dir)?;
        sync_directory(&guard.root)?;
    }
    validate_private_directory(&generations_dir)?;
    let manifest_path = generations_dir.join(format!("{}.json", manifest.generation_id));
    match write_new_json(&manifest_path, manifest) {
        Ok(()) => sync_directory(&generations_dir)?,
        Err(RecoveryError::Io(error)) if error.kind() == io::ErrorKind::AlreadyExists => {
            validate_single_link_file(&manifest_path)?;
            let existing: ActivationManifest = serde_json::from_slice(&fs::read(&manifest_path)?)?;
            if existing != *manifest {
                return Err(RecoveryError::Integrity(
                    "existing generation manifest differs from this activation".to_owned(),
                ));
            }
        }
        Err(error) => return Err(error),
    }
    let active = ActiveSet {
        format_version: FORMAT_VERSION,
        generation_id: manifest.generation_id.clone(),
        manifest_sha256: digest,
    };
    if already_published {
        return Ok(active);
    }
    replace_json(&guard.root.join(ACTIVE_NAME), &active)?;
    Ok(active)
}

/// Verifies the pointer and immutable manifest. The caller must additionally
/// recheck the native VM process/listener, full export, mount encryption, and
/// independent custody source before it may open the receiver.
pub fn read_active_candidate(guard: &CoordinatorGuard) -> RecoveryResult<ActivationManifest> {
    guard.verify_root()?;
    let pointer_path = guard.root.join(ACTIVE_NAME);
    validate_single_link_file(&pointer_path)?;
    let active: ActiveSet = serde_json::from_slice(&fs::read(pointer_path)?)?;
    if active.format_version != FORMAT_VERSION {
        return Err(RecoveryError::InvalidArtifact(
            "active-set pointer version is unsupported".to_owned(),
        ));
    }
    validate_uuid(&active.generation_id, "active generation ID")?;
    validate_hex(&active.manifest_sha256, "active manifest SHA-256")?;
    let generations_dir = guard.root.join(GENERATIONS_DIR);
    validate_private_directory(&generations_dir)?;
    let manifest_path = generations_dir.join(format!("{}.json", active.generation_id));
    validate_single_link_file(&manifest_path)?;
    let manifest: ActivationManifest = serde_json::from_slice(&fs::read(manifest_path)?)?;
    manifest.validate_metadata()?;
    if manifest.generation_id != active.generation_id
        || hex::encode(Sha256::digest(serde_json::to_vec(&manifest)?)) != active.manifest_sha256
    {
        return Err(RecoveryError::Integrity(
            "active-set pointer and generation manifest diverge".to_owned(),
        ));
    }
    Ok(manifest)
}

pub fn verify_active_set(
    guard: &CoordinatorGuard,
    control: &ControlStore,
    independent_expected_head: &ControlCheckpoint,
) -> RecoveryResult<ActivationManifest> {
    guard.verify_root()?;
    let manifest = read_active_candidate(guard)?;
    manifest.health.verify_location()?;
    manifest.control.verify_location()?;
    manifest.vm_storage.verify_location()?;
    if control.db_path() != manifest.control.path {
        return Err(RecoveryError::Integrity(
            "active control path differs from the selected generation".to_owned(),
        ));
    }
    crate::database::verify_health_database(&manifest.health.path, Some(&control.store_id()?))?;
    if !control.contains_checkpoint(&manifest.control_checkpoint)? {
        return Err(RecoveryError::Integrity(
            "active generation's completed control head is absent".to_owned(),
        ));
    }
    control.verify()?;
    if control.checkpoint()? != *independent_expected_head {
        return Err(RecoveryError::Integrity(
            "control head differs from independent expected head".to_owned(),
        ));
    }
    let journal = RecoveryJournal::load(guard)?;
    if journal.current.phase != RecoveryPhase::ActiveVerified
        || journal.current.intent.restore_epoch != manifest.restore_epoch
    {
        return Err(RecoveryError::Integrity(
            "active generation lacks a completed matching restore journal".to_owned(),
        ));
    }
    let historical = RecoveryJournal::load_epoch(guard, &manifest.restore_epoch)?;
    if historical.current.phase != RecoveryPhase::ActiveVerified {
        return Err(RecoveryError::Integrity(
            "selected generation's restore journal is not completed".to_owned(),
        ));
    }
    Ok(manifest)
}

fn check_previous_active_set(
    guard: &CoordinatorGuard,
    previous_generation_id: Option<&str>,
) -> RecoveryResult<()> {
    let path = guard.root.join(ACTIVE_NAME);
    match (fs::symlink_metadata(&path), previous_generation_id) {
        (Err(error), None) if error.kind() == io::ErrorKind::NotFound => Ok(()),
        (Ok(_), Some(expected)) => {
            validate_single_link_file(&path)?;
            let pointer: ActiveSet = serde_json::from_slice(&fs::read(path)?)?;
            if pointer.generation_id == expected {
                Ok(())
            } else {
                Err(RecoveryError::Integrity(
                    "active generation changed since recovery freeze".to_owned(),
                ))
            }
        }
        (Err(error), _) if error.kind() != io::ErrorKind::NotFound => Err(error.into()),
        _ => Err(RecoveryError::Integrity(
            "active generation presence changed since recovery freeze".to_owned(),
        )),
    }
}

fn make_record(
    sequence: u64,
    phase: RecoveryPhase,
    intent: RecoveryIntent,
    evidence_sha256: &str,
    previous_hash: &str,
) -> RecoveryResult<RecoveryRecord> {
    let mut record = RecoveryRecord {
        format_version: FORMAT_VERSION,
        sequence,
        phase,
        intent,
        evidence_sha256: evidence_sha256.to_owned(),
        previous_hash: previous_hash.to_owned(),
        current_hash: String::new(),
    };
    record.current_hash = record_hash(&record)?;
    Ok(record)
}

fn record_hash(record: &RecoveryRecord) -> RecoveryResult<String> {
    let mut payload = record.clone();
    payload.current_hash.clear();
    Ok(hex::encode(Sha256::digest(serde_json::to_vec(&payload)?)))
}

fn verify_record(
    record: &RecoveryRecord,
    sequence: u64,
    prior_hash: &str,
    prior_phase: Option<RecoveryPhase>,
    intent: Option<&RecoveryIntent>,
) -> RecoveryResult<()> {
    if record.format_version != FORMAT_VERSION
        || record.sequence != sequence
        || record.previous_hash != prior_hash
        || record_hash(record)? != record.current_hash
        || record.intent.validate().is_err()
        || intent.is_some_and(|prior| prior != &record.intent)
        || prior_phase.map_or(record.phase != RecoveryPhase::Frozen, |prior| {
            prior.successor() != Some(record.phase)
        })
    {
        return Err(RecoveryError::Integrity(
            "restore journal hash, intent, or phase transition is invalid".to_owned(),
        ));
    }
    validate_hex(&record.evidence_sha256, "phase evidence digest")?;
    Ok(())
}

fn publish_head(journal_dir: &Path, record: &RecoveryRecord) -> RecoveryResult<()> {
    let head = JournalHead {
        format_version: FORMAT_VERSION,
        sequence: record.sequence,
        current_hash: record.current_hash.clone(),
    };
    replace_json(&journal_dir.join(HEAD_NAME), &head)
}

fn record_name(sequence: u64) -> String {
    format!("{sequence:020}.json")
}

fn journal_dir(guard: &CoordinatorGuard, epoch: &str) -> PathBuf {
    guard.root.join(JOURNALS_DIR).join(epoch)
}

fn validate_checkpoint(checkpoint: &ControlCheckpoint) -> RecoveryResult<()> {
    if checkpoint.store_id.is_empty() || checkpoint.sequence < 0 {
        return Err(RecoveryError::InvalidArtifact(
            "control checkpoint identity is invalid".to_owned(),
        ));
    }
    validate_hex(&checkpoint.current_hash, "control checkpoint hash")
}

fn validate_uuid(value: &str, label: &str) -> RecoveryResult<()> {
    if uuid::Uuid::parse_str(value).is_err() {
        return Err(RecoveryError::InvalidArtifact(format!(
            "{label} is not a UUID"
        )));
    }
    Ok(())
}

fn validate_hex(value: &str, label: &str) -> RecoveryResult<()> {
    if value.len() != 64
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        return Err(RecoveryError::InvalidArtifact(format!(
            "{label} must be lowercase SHA-256 hex"
        )));
    }
    Ok(())
}

fn validate_absolute(path: &Path) -> RecoveryResult<()> {
    if !path.is_absolute()
        || path.components().any(|part| {
            matches!(
                part,
                Component::CurDir | Component::ParentDir | Component::Prefix(_)
            )
        })
    {
        return Err(RecoveryError::InvalidPath(
            "coordination paths must be absolute without dot components".to_owned(),
        ));
    }
    Ok(())
}

fn validate_real_directory_components(path: &Path) -> RecoveryResult<()> {
    validate_absolute(path)?;
    let mut cursor = PathBuf::new();
    for part in path.components() {
        cursor.push(part.as_os_str());
        let metadata = fs::symlink_metadata(&cursor)?;
        if metadata.file_type().is_symlink() || !metadata.is_dir() {
            return Err(RecoveryError::InvalidPath(
                "recovery directory contains a link or non-directory component".to_owned(),
            ));
        }
    }
    Ok(())
}

fn validate_private_directory(path: &Path) -> RecoveryResult<()> {
    validate_real_directory_components(path)?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::{MetadataExt, PermissionsExt};
        let metadata = fs::symlink_metadata(path)?;
        if metadata.permissions().mode() & 0o077 != 0
            || metadata.uid() != unsafe { libc::geteuid() }
        {
            return Err(RecoveryError::InvalidPath(
                "coordination directory must be private and owned by the current process"
                    .to_owned(),
            ));
        }
    }
    Ok(())
}

fn validate_single_link_file(path: &Path) -> RecoveryResult<()> {
    validate_absolute(path)?;
    validate_real_directory_components(
        path.parent()
            .ok_or_else(|| RecoveryError::InvalidPath("recovery file has no parent".to_owned()))?,
    )?;
    let metadata = fs::symlink_metadata(path)?;
    if metadata.file_type().is_symlink() || !metadata.is_file() {
        return Err(RecoveryError::InvalidPath(
            "recovery artifact is not a real regular file".to_owned(),
        ));
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        if metadata.nlink() != 1 {
            return Err(RecoveryError::InvalidPath(
                "recovery artifact must have exactly one hard link".to_owned(),
            ));
        }
    }
    Ok(())
}

fn reject_sqlite_sidecars(path: &Path) -> RecoveryResult<()> {
    let name = path
        .file_name()
        .ok_or_else(|| RecoveryError::InvalidPath("SQLite candidate has no filename".to_owned()))?;
    let parent = path
        .parent()
        .ok_or_else(|| RecoveryError::InvalidPath("SQLite candidate has no parent".to_owned()))?;
    for suffix in ["-wal", "-shm", "-journal"] {
        let sidecar = parent.join(format!("{}{}", name.to_string_lossy(), suffix));
        match fs::symlink_metadata(sidecar) {
            Ok(_) => {
                return Err(RecoveryError::Integrity(
                    "quiescent SQLite candidate has a journal or WAL sidecar".to_owned(),
                ));
            }
            Err(error) if error.kind() == io::ErrorKind::NotFound => {}
            Err(error) => return Err(error.into()),
        }
    }
    Ok(())
}

#[cfg(unix)]
fn identity(metadata: &fs::Metadata) -> (u64, u64) {
    use std::os::unix::fs::MetadataExt;
    (metadata.dev(), metadata.ino())
}

#[cfg(not(unix))]
fn identity(_metadata: &fs::Metadata) -> (u64, u64) {
    (0, 0)
}

fn directory_identity(path: &Path) -> RecoveryResult<(u64, u64)> {
    Ok(identity(&fs::symlink_metadata(path)?))
}

fn regular_file_identity(path: &Path) -> RecoveryResult<(u64, u64)> {
    Ok(identity(&fs::symlink_metadata(path)?))
}

fn file_identity(file: &File) -> RecoveryResult<(u64, u64)> {
    Ok(identity(&file.metadata()?))
}

fn open_new_nofollow(path: &Path) -> io::Result<File> {
    let mut options = OpenOptions::new();
    options.read(true).write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600).custom_flags(libc::O_NOFOLLOW);
    }
    options.open(path)
}

fn open_existing_nofollow(path: &Path) -> io::Result<File> {
    let mut options = OpenOptions::new();
    options.read(true).write(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.custom_flags(libc::O_NOFOLLOW);
    }
    options.open(path)
}

fn write_new_json<T: Serialize>(path: &Path, value: &T) -> RecoveryResult<()> {
    let bytes = serde_json::to_vec(value)?;
    let mut file = open_new_nofollow(path)?;
    file.write_all(&bytes)?;
    file.sync_all()?;
    Ok(())
}

fn replace_json<T: Serialize>(path: &Path, value: &T) -> RecoveryResult<()> {
    let parent = path.parent().ok_or_else(|| {
        RecoveryError::InvalidPath("recovery state file has no parent".to_owned())
    })?;
    validate_private_directory(parent)?;
    if fs::symlink_metadata(path).is_ok() {
        validate_single_link_file(path)?;
    }
    let temp = parent.join(format!(".publish-{}.tmp", uuid::Uuid::new_v4()));
    write_new_json(&temp, value)?;
    fs::rename(&temp, path)?;
    sync_directory(parent)
}

fn set_private_directory(path: &Path) -> RecoveryResult<()> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(path, fs::Permissions::from_mode(0o700))?;
    }
    Ok(())
}

fn sync_directory(path: &Path) -> RecoveryResult<()> {
    OpenOptions::new().read(true).open(path)?.sync_all()?;
    Ok(())
}

fn directory_entry_names(path: &Path) -> RecoveryResult<BTreeSet<String>> {
    fs::read_dir(path)?
        .map(|entry| {
            entry?
                .file_name()
                .into_string()
                .map_err(|_| RecoveryError::InvalidArtifact("non-UTF8 journal name".to_owned()))
        })
        .collect()
}

fn file_sha256(path: &Path) -> RecoveryResult<String> {
    let mut file = File::open(path)?;
    let mut hash = Sha256::new();
    let mut buffer = [0_u8; 64 * 1024];
    loop {
        let count = file.read(&mut buffer)?;
        if count == 0 {
            break;
        }
        hash.update(&buffer[..count]);
    }
    Ok(hex::encode(hash.finalize()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn digest(value: &str) -> String {
        hex::encode(Sha256::digest(value.as_bytes()))
    }

    fn fixture() -> (TempDir, CoordinatorGuard, RecoveryIntent) {
        let temp = TempDir::new().unwrap();
        let root = temp.path().canonicalize().unwrap().join("coordination");
        initialize_coordinator(&root).unwrap();
        let guard = lock_coordinator(&root, true, true).unwrap();
        let intent = RecoveryIntent {
            restore_epoch: digest("epoch"),
            snapshot_id: uuid::Uuid::new_v4().to_string(),
            expected_control_checkpoint: ControlCheckpoint {
                store_id: uuid::Uuid::new_v4().to_string(),
                sequence: 0,
                current_hash: digest("genesis"),
            },
            previous_generation_id: None,
            source_commit_sequence: Some(3),
        };
        (temp, guard, intent)
    }

    #[test]
    fn journal_is_strict_and_idempotent() {
        let (_temp, guard, intent) = fixture();
        let frozen = digest("frozen");
        let mut journal = RecoveryJournal::begin(&guard, intent, &frozen).unwrap();
        assert_eq!(
            RecoveryJournal::load(&guard).unwrap().current.phase,
            RecoveryPhase::Frozen
        );
        journal
            .advance(&guard, RecoveryPhase::Frozen, &frozen)
            .unwrap();
        assert_eq!(journal.current.sequence, 0);
        assert!(
            journal
                .advance(&guard, RecoveryPhase::MetricsVerified, &digest("vm"))
                .is_err()
        );
        journal
            .advance(&guard, RecoveryPhase::ControlReplayed, &digest("replay"))
            .unwrap();
        assert_eq!(RecoveryJournal::load(&guard).unwrap().current.sequence, 1);
    }

    #[test]
    fn unpublished_record_needs_explicit_external_reverification() {
        let (_temp, guard, intent) = fixture();
        let journal = RecoveryJournal::begin(&guard, intent.clone(), &digest("frozen")).unwrap();
        let next = make_record(
            1,
            RecoveryPhase::ControlReplayed,
            intent,
            &digest("replay"),
            &journal.current.current_hash,
        )
        .unwrap();
        let path = journal_dir(&guard, &journal.current.intent.restore_epoch).join(record_name(1));
        write_new_json(&path, &next).unwrap();
        sync_directory(&journal_dir(&guard, &journal.current.intent.restore_epoch)).unwrap();
        assert!(RecoveryJournal::load(&guard).is_err());
        assert!(
            RecoveryJournal::resume_unpublished(&guard, |_| {
                Err(RecoveryError::Integrity(
                    "external replay not verified".to_owned(),
                ))
            })
            .is_err()
        );
        assert!(RecoveryJournal::load(&guard).is_err());
        let resumed = RecoveryJournal::resume_unpublished(&guard, |record| {
            assert_eq!(record.evidence_sha256, digest("replay"));
            Ok(())
        })
        .unwrap();
        assert_eq!(resumed.current.phase, RecoveryPhase::ControlReplayed);
    }

    #[test]
    fn tampered_record_and_unexpected_file_fail_closed() {
        let (_temp, guard, intent) = fixture();
        RecoveryJournal::begin(&guard, intent, &digest("frozen")).unwrap();
        let journal_dir = journal_dir(
            &guard,
            &RecoveryJournal::load(&guard)
                .unwrap()
                .current
                .intent
                .restore_epoch,
        );
        fs::write(journal_dir.join("garbage"), b"x").unwrap();
        assert!(RecoveryJournal::load(&guard).is_err());
        fs::remove_file(journal_dir.join("garbage")).unwrap();
        fs::write(journal_dir.join(record_name(0)), b"{}").unwrap();
        assert!(RecoveryJournal::load(&guard).is_err());
    }

    #[test]
    fn second_generation_keeps_first_journal_immutable() {
        let (_temp, guard, first_intent) = fixture();
        let first_epoch = first_intent.restore_epoch.clone();
        let mut first =
            RecoveryJournal::begin(&guard, first_intent, &digest("freeze-one")).unwrap();
        for phase in [
            RecoveryPhase::ControlReplayed,
            RecoveryPhase::CandidateReady,
            RecoveryPhase::MetricsVerified,
            RecoveryPhase::ControlCompleted,
            RecoveryPhase::CustodyAdvanced,
            RecoveryPhase::Activating,
            RecoveryPhase::ActiveVerified,
        ] {
            first
                .advance(&guard, phase, &digest(&format!("first-{phase:?}")))
                .unwrap();
        }
        let first_hash = first.current.current_hash.clone();
        let active_generation = uuid::Uuid::new_v4().to_string();
        write_new_json(
            &guard.root.join(ACTIVE_NAME),
            &ActiveSet {
                format_version: FORMAT_VERSION,
                generation_id: active_generation.clone(),
                manifest_sha256: digest("first-generation-manifest"),
            },
        )
        .unwrap();
        let second_intent = RecoveryIntent {
            restore_epoch: digest("epoch-two"),
            snapshot_id: uuid::Uuid::new_v4().to_string(),
            expected_control_checkpoint: first.current.intent.expected_control_checkpoint.clone(),
            previous_generation_id: Some(active_generation),
            source_commit_sequence: Some(4),
        };
        let second = RecoveryJournal::begin(&guard, second_intent, &digest("freeze-two")).unwrap();
        assert_eq!(second.current.phase, RecoveryPhase::Frozen);
        assert_eq!(
            RecoveryJournal::load(&guard)
                .unwrap()
                .current
                .intent
                .restore_epoch,
            digest("epoch-two")
        );
        let historical = RecoveryJournal::load_epoch(&guard, &first_epoch).unwrap();
        assert_eq!(historical.current.phase, RecoveryPhase::ActiveVerified);
        assert_eq!(historical.current.current_hash, first_hash);
    }

    #[test]
    fn unselected_epoch_fails_closed_until_freeze_is_reverified() {
        let (_temp, guard, intent) = fixture();
        let epoch = intent.restore_epoch.clone();
        RecoveryJournal::begin(&guard, intent, &digest("frozen")).unwrap();
        fs::remove_file(guard.root.join(CURRENT_RESTORE_NAME)).unwrap();
        assert!(RecoveryJournal::load(&guard).is_err());
        assert!(
            RecoveryJournal::resume_unselected_epoch(&guard, &epoch, |_| {
                Err(RecoveryError::Integrity(
                    "freeze is not reverified".to_owned(),
                ))
            })
            .is_err()
        );
        assert!(RecoveryJournal::load(&guard).is_err());
        let resumed = RecoveryJournal::resume_unselected_epoch(&guard, &epoch, |record| {
            assert_eq!(record.evidence_sha256, digest("frozen"));
            Ok(())
        })
        .unwrap();
        assert_eq!(resumed.current.phase, RecoveryPhase::Frozen);
    }

    #[test]
    fn absent_active_pointer_and_invalid_phase_never_publish() {
        let (_temp, guard, intent) = fixture();
        let journal = RecoveryJournal::begin(&guard, intent, &digest("frozen")).unwrap();
        assert!(!guard.root.join(ACTIVE_NAME).exists());
        let invalid = ActivationManifest {
            format_version: FORMAT_VERSION,
            generation_id: uuid::Uuid::new_v4().to_string(),
            restore_epoch: journal.current.intent.restore_epoch.clone(),
            snapshot_id: journal.current.intent.snapshot_id.clone(),
            health: PathIdentity {
                path: guard.root.join("missing.db"),
                device: 0,
                inode: 0,
                is_directory: false,
                sha256: Some(digest("missing")),
            },
            control: PathIdentity {
                path: guard.root.join("missing-control.db"),
                device: 0,
                inode: 0,
                is_directory: false,
                sha256: Some(digest("missing")),
            },
            vm_storage: PathIdentity {
                path: guard.root.join("missing-vm"),
                device: 0,
                inode: 0,
                is_directory: true,
                sha256: None,
            },
            control_checkpoint: journal.current.intent.expected_control_checkpoint.clone(),
            vm_generation_id: uuid::Uuid::new_v4().to_string(),
            mapping_version: 1,
            oracle_sha256: digest("oracle"),
            full_readback_sha256: digest("readback"),
            verified_at: "2026-09-19T00:00:00Z".to_owned(),
        };
        assert!(
            publish_active_set(
                &guard,
                &journal,
                &invalid,
                &journal.current.intent.expected_control_checkpoint,
            )
            .is_err()
        );
        assert!(!guard.root.join(ACTIVE_NAME).exists());
    }

    #[test]
    fn adoption_is_immutable_and_never_creates_an_active_pointer() {
        let (_temp, guard, intent) = fixture();
        let record = AdoptionRecord {
            format_version: FORMAT_VERSION,
            snapshot_id: intent.snapshot_id,
            snapshot_sha256: digest("snapshot"),
            receipt_inventory_sha256: digest("receipts"),
            control_checkpoint: intent.expected_control_checkpoint,
            baseline_sha256: digest("baseline"),
            custody_revision: 1,
        };
        publish_adopted_unactivated(&guard, &record).unwrap();
        publish_adopted_unactivated(&guard, &record).unwrap();
        assert_eq!(read_adopted_unactivated(&guard).unwrap(), record);
        assert!(!guard.root.join(ACTIVE_NAME).exists());
        assert!(!guard.root.join(CURRENT_RESTORE_NAME).exists());
        let mut changed = record.clone();
        changed.baseline_sha256 = digest("replacement");
        assert!(publish_adopted_unactivated(&guard, &changed).is_err());
        fs::remove_file(guard.root.join(ADOPTED_NAME)).unwrap();
        assert!(read_adopted_unactivated(&guard).is_err());
    }
}
