use chrono::Utc;
use fs2::FileExt;
use rusqlite::{Connection, OpenFlags, OptionalExtension, TransactionBehavior, params};
use std::{
    collections::BTreeSet,
    fmt,
    fs::{self, OpenOptions},
    io,
    path::{Component, Path, PathBuf},
    time::Duration,
};

pub const HEALTH_APPLICATION_ID: i64 = 0x425A4852; // BZHR
pub const HEALTH_SCHEMA_VERSION: i64 = 2;
pub const DEFAULT_MAPPING_VERSION: i64 = 1;

const LEGACY_TABLES: &[&str] = &[
    "audit",
    "devices",
    "erasures",
    "events",
    "outbox",
    "pairing_codes",
    "receipts",
];
const LEGACY_INDEXES: &[&str] = &["events_type", "outbox_pending", "receipts_device"];
const V2_TABLES: &[&str] = &[
    "audit",
    "devices",
    "erasures",
    "events",
    "outbox",
    "pairing_codes",
    "projection_state",
    "receipts",
    "storage_meta",
];

#[derive(Debug)]
pub enum StorageError {
    Path(String),
    Permission(String),
    Corrupt(String),
    Incompatible(String),
    Migration(String),
    Sql(rusqlite::Error),
    Io(io::Error),
}

impl fmt::Display for StorageError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Path(message) => write!(formatter, "storage path rejected: {message}"),
            Self::Permission(message) => write!(formatter, "storage permission failure: {message}"),
            Self::Corrupt(message) => write!(formatter, "database is corrupt: {message}"),
            Self::Incompatible(message) => write!(formatter, "database is incompatible: {message}"),
            Self::Migration(message) => write!(formatter, "database migration failed: {message}"),
            Self::Sql(error) => write!(formatter, "database operation failed: {error}"),
            Self::Io(error) => write!(formatter, "storage I/O failed: {error}"),
        }
    }
}

impl std::error::Error for StorageError {}

impl From<rusqlite::Error> for StorageError {
    fn from(value: rusqlite::Error) -> Self {
        Self::Sql(value)
    }
}

impl From<io::Error> for StorageError {
    fn from(value: io::Error) -> Self {
        if value.kind() == io::ErrorKind::PermissionDenied {
            Self::Permission(value.to_string())
        } else {
            Self::Io(value)
        }
    }
}

pub type StorageResult<T> = Result<T, StorageError>;

#[derive(Debug, Clone)]
pub struct StoragePaths {
    pub data_root: PathBuf,
    pub health_db: PathBuf,
    pub control_db: PathBuf,
    pub control_mirror_dir: PathBuf,
    pub backup_dir: PathBuf,
    forbidden_paths: Vec<PathBuf>,
}

impl StoragePaths {
    pub fn new(
        data_root: PathBuf,
        health_db: PathBuf,
        control_db: PathBuf,
        control_mirror_dir: PathBuf,
        backup_dir: PathBuf,
    ) -> Self {
        Self {
            data_root,
            health_db,
            control_db,
            control_mirror_dir,
            backup_dir,
            forbidden_paths: vec![
                PathBuf::from("/opt/boaz/data"),
                PathBuf::from("/opt/boaz/data/boaz_events.db"),
            ],
        }
    }

    #[cfg(test)]
    pub fn with_forbidden_paths(mut self, paths: Vec<PathBuf>) -> Self {
        self.forbidden_paths = paths;
        self
    }

    pub fn validate_no_write(&self) -> StorageResult<()> {
        for path in [
            &self.data_root,
            &self.health_db,
            &self.control_db,
            &self.control_mirror_dir,
            &self.backup_dir,
        ] {
            validate_lexical_path(path)?;
            reject_symlink_boundary(path)?;
        }
        if self.health_db.parent() != Some(self.data_root.as_path()) {
            return Err(StorageError::Path(
                "health database must be a direct child of BOAZ_HEALTH_DATA_ROOT".to_owned(),
            ));
        }
        if self.health_db == self.control_db {
            return Err(StorageError::Path(
                "health and control databases must be distinct".to_owned(),
            ));
        }
        validate_existing_regular_single_link(&self.health_db)?;
        validate_existing_regular_single_link(&self.control_db)?;
        for forbidden in &self.forbidden_paths {
            if aliases_or_descends(&self.health_db, forbidden)?
                || aliases_or_descends(&self.data_root, forbidden)?
            {
                return Err(StorageError::Path(
                    "health storage must remain outside the Mac sync replacement path and its aliases"
                        .to_owned(),
                ));
            }
        }
        Ok(())
    }

    pub fn prepare_empty_layout(&self) -> StorageResult<()> {
        self.validate_no_write()?;
        for path in [&self.health_db, &self.control_db] {
            if path.exists() {
                return Err(StorageError::Path(
                    "init-storage requires absent database files".to_owned(),
                ));
            }
        }
        if self.backup_dir.exists()
            && fs::read_dir(&self.backup_dir)?
                .next()
                .transpose()?
                .is_some()
        {
            return Err(StorageError::Path(
                "init-storage requires an empty managed backup directory".to_owned(),
            ));
        }
        if self.control_mirror_dir.exists()
            && fs::read_dir(&self.control_mirror_dir)?
                .next()
                .transpose()?
                .is_some()
        {
            return Err(StorageError::Path(
                "init-storage requires an empty control mirror directory".to_owned(),
            ));
        }
        for directory in [
            &self.data_root,
            self.control_db
                .parent()
                .ok_or_else(|| StorageError::Path("control database has no parent".to_owned()))?,
            &self.control_mirror_dir,
            &self.backup_dir,
        ] {
            fs::create_dir_all(directory)?;
            set_private_directory(directory)?;
        }
        Ok(())
    }

    pub fn prepare_control_layout_for_migration(&self) -> StorageResult<()> {
        self.validate_control_layout_for_migration_no_write()?;
        if self.control_db.exists() {
            return Ok(());
        }
        let control_parent = self
            .control_db
            .parent()
            .ok_or_else(|| StorageError::Path("control database has no parent".to_owned()))?;
        for directory in [control_parent, self.control_mirror_dir.as_path()] {
            fs::create_dir_all(directory)?;
            set_private_directory(directory)?;
        }
        Ok(())
    }

    /// Performs every migration path/layout check that can be completed
    /// without creating a directory, lock, database, WAL, or SHM file.
    pub fn validate_control_layout_for_migration_no_write(&self) -> StorageResult<()> {
        self.validate_no_write()?;
        if !self.health_db.exists() {
            return Err(StorageError::Path(
                "migrate-storage requires an existing health database".to_owned(),
            ));
        }
        if self.control_db.exists() {
            return Ok(());
        }
        let control_parent = self
            .control_db
            .parent()
            .ok_or_else(|| StorageError::Path("control database has no parent".to_owned()))?;
        let head = control_parent.join("control.head.json");
        if head.exists() {
            return Err(StorageError::Path(
                "new control store requires an absent control head".to_owned(),
            ));
        }
        if self.control_mirror_dir.exists() {
            if !self.control_mirror_dir.is_dir() {
                return Err(StorageError::Path(
                    "control mirror path is not a directory".to_owned(),
                ));
            }
            if fs::read_dir(&self.control_mirror_dir)?
                .next()
                .transpose()?
                .is_some()
            {
                return Err(StorageError::Path(
                    "new control store requires an empty mirror directory".to_owned(),
                ));
            }
        }
        Ok(())
    }

    pub fn validate_backup_destination(&self, destination: &Path) -> StorageResult<()> {
        self.validate_no_write()?;
        validate_lexical_path(destination)?;
        reject_symlink_boundary(destination)?;
        if destination.parent() != Some(self.backup_dir.as_path()) {
            return Err(StorageError::Path(
                "backup destination must be a direct child of BOAZ_HEALTH_BACKUP_DIR".to_owned(),
            ));
        }
        if !self.backup_dir.is_dir() {
            return Err(StorageError::Path(
                "managed backup directory is missing".to_owned(),
            ));
        }
        Ok(())
    }

    pub fn validate_backup_directory(&self, directory: &Path) -> StorageResult<()> {
        self.validate_no_write()?;
        validate_lexical_path(directory)?;
        reject_symlink_boundary(directory)?;
        if directory != self.backup_dir {
            return Err(StorageError::Path(
                "backup pruning is restricted to BOAZ_HEALTH_BACKUP_DIR".to_owned(),
            ));
        }
        if !directory.is_dir() {
            return Err(StorageError::Path(
                "managed backup directory is missing".to_owned(),
            ));
        }
        Ok(())
    }
}

fn validate_lexical_path(path: &Path) -> StorageResult<()> {
    if !path.is_absolute() {
        return Err(StorageError::Path(
            "storage paths must be absolute".to_owned(),
        ));
    }
    if path.components().any(|component| {
        matches!(
            component,
            Component::ParentDir | Component::CurDir | Component::Prefix(_)
        )
    }) {
        return Err(StorageError::Path(
            "storage paths may not contain dot components".to_owned(),
        ));
    }
    Ok(())
}

fn reject_symlink_boundary(path: &Path) -> StorageResult<()> {
    // The configured authority entry and its immediate parent are controlled;
    // platform aliases above that boundary (for example macOS /var) are not.
    for entry in [Some(path), path.parent()].into_iter().flatten() {
        match fs::symlink_metadata(entry) {
            Ok(metadata) if metadata.file_type().is_symlink() => {
                return Err(StorageError::Path(
                    "storage authority or its parent is a symbolic link".to_owned(),
                ));
            }
            Ok(_) => {}
            Err(error) if error.kind() == io::ErrorKind::NotFound => {}
            Err(error) => return Err(error.into()),
        }
    }
    Ok(())
}

fn validate_existing_regular_single_link(path: &Path) -> StorageResult<()> {
    let metadata = match fs::symlink_metadata(path) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(()),
        Err(error) => return Err(error.into()),
    };
    if !metadata.file_type().is_file() {
        return Err(StorageError::Path(
            "database path is not a regular file".to_owned(),
        ));
    }
    if metadata.len() == 0 {
        return Err(StorageError::Incompatible(
            "existing zero-byte database is not a valid SQLite database".to_owned(),
        ));
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        if metadata.nlink() != 1 {
            return Err(StorageError::Path(
                "database files with hard links are not accepted".to_owned(),
            ));
        }
    }
    Ok(())
}

fn aliases_or_descends(candidate: &Path, forbidden: &Path) -> StorageResult<bool> {
    if candidate.starts_with(forbidden) {
        return Ok(true);
    }
    let candidate_canonical = match candidate.canonicalize() {
        Ok(value) => value,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(false),
        Err(error) => return Err(error.into()),
    };
    let forbidden_canonical = match forbidden.canonicalize() {
        Ok(value) => value,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(false),
        Err(error) => return Err(error.into()),
    };
    if candidate_canonical.starts_with(&forbidden_canonical) {
        return Ok(true);
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        let candidate_meta = fs::metadata(candidate)?;
        let forbidden_meta = fs::metadata(forbidden)?;
        if candidate_meta.dev() == forbidden_meta.dev()
            && candidate_meta.ino() == forbidden_meta.ino()
        {
            return Ok(true);
        }
    }
    Ok(false)
}

fn set_private_directory(path: &Path) -> StorageResult<()> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(path, fs::Permissions::from_mode(0o700))?;
    }
    Ok(())
}

fn set_private_file(path: &Path) -> StorageResult<()> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(path, fs::Permissions::from_mode(0o600))?;
    }
    Ok(())
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HealthLayout {
    LegacyV0,
    ClaimedV1,
    CurrentV2,
}

pub fn classify_health_database(path: &Path) -> StorageResult<HealthLayout> {
    validate_existing_regular_single_link(path)?;
    if !path.exists() {
        return Err(StorageError::Path("health database is missing".to_owned()));
    }
    let connection = Connection::open_with_flags(path, OpenFlags::SQLITE_OPEN_READ_ONLY)
        .map_err(classify_open_error)?;
    verify_integrity(&connection)?;
    let application_id = pragma_i64(&connection, "application_id")?;
    let version = pragma_i64(&connection, "user_version")?;
    match (application_id, version) {
        (0, 0) => {
            verify_legacy_layout(&connection)?;
            Ok(HealthLayout::LegacyV0)
        }
        (HEALTH_APPLICATION_ID, 1) => {
            verify_legacy_layout(&connection)?;
            Ok(HealthLayout::ClaimedV1)
        }
        (HEALTH_APPLICATION_ID, HEALTH_SCHEMA_VERSION) => {
            verify_v2_layout(&connection, None)?;
            Ok(HealthLayout::CurrentV2)
        }
        (HEALTH_APPLICATION_ID, value) if value > HEALTH_SCHEMA_VERSION => {
            Err(StorageError::Incompatible(format!(
                "health database version {value} is newer than supported version {HEALTH_SCHEMA_VERSION}"
            )))
        }
        (0, value) => Err(StorageError::Incompatible(format!(
            "unclaimed health database has unsupported version {value}"
        ))),
        (value, _) => Err(StorageError::Incompatible(format!(
            "unexpected SQLite application_id {value}"
        ))),
    }
}

fn classify_open_error(error: rusqlite::Error) -> StorageError {
    match &error {
        rusqlite::Error::SqliteFailure(code, _)
            if matches!(
                code.code,
                rusqlite::ErrorCode::NotADatabase | rusqlite::ErrorCode::DatabaseCorrupt
            ) =>
        {
            StorageError::Corrupt("SQLite could not read the database".to_owned())
        }
        _ => StorageError::Sql(error),
    }
}

pub fn initialize_health_database(path: &Path, control_store_id: &str) -> StorageResult<()> {
    if control_store_id.is_empty() {
        return Err(StorageError::Incompatible(
            "control store identifier is required".to_owned(),
        ));
    }
    let parent = path
        .parent()
        .ok_or_else(|| StorageError::Path("health database has no parent".to_owned()))?;
    if !parent.is_dir() {
        return Err(StorageError::Path(
            "health database parent directory is missing".to_owned(),
        ));
    }
    OpenOptions::new().write(true).create_new(true).open(path)?;
    let result = (|| -> StorageResult<()> {
        let mut connection = Connection::open_with_flags(path, OpenFlags::SQLITE_OPEN_READ_WRITE)?;
        configure_connection(&connection)?;
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        transaction.execute_batch(include_str!("schema.sql"))?;
        transaction.execute(
            "INSERT INTO storage_meta(singleton,control_store_id,created_at) VALUES (1,?1,?2)",
            params![control_store_id, Utc::now().to_rfc3339()],
        )?;
        transaction.execute(
            "INSERT INTO projection_state(singleton,generation_id,mapping_version,storage_identity,updated_at) VALUES (1,?1,?2,'unverified',?3)",
            params![uuid::Uuid::new_v4().to_string(), DEFAULT_MAPPING_VERSION, Utc::now().to_rfc3339()],
        )?;
        transaction.execute_batch(&format!(
            "PRAGMA application_id={HEALTH_APPLICATION_ID}; PRAGMA user_version={HEALTH_SCHEMA_VERSION};"
        ))?;
        transaction.commit()?;
        verify_health_database(path, Some(control_store_id))?;
        set_private_file(path)?;
        Ok(())
    })();
    if result.is_err() {
        let _ = fs::remove_file(path);
        let _ = fs::remove_file(path.with_extension("db-wal"));
        let _ = fs::remove_file(path.with_extension("db-shm"));
    }
    result
}

pub fn migrate_health_database(path: &Path, control_store_id: &str) -> StorageResult<()> {
    if std::env::var("BOAZ_HEALTH_UPLOAD_ENABLED").ok().as_deref() == Some("1") {
        return Err(StorageError::Migration(
            "migration requires BOAZ_HEALTH_UPLOAD_ENABLED to be off".to_owned(),
        ));
    }
    let layout = classify_health_database(path)?;
    if layout == HealthLayout::CurrentV2 {
        return verify_health_database(path, Some(control_store_id));
    }
    let lock_path = path
        .parent()
        .ok_or_else(|| StorageError::Path("health database has no parent".to_owned()))?
        .join("health-migration.lock");
    let lock = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(lock_path)?;
    lock.try_lock_exclusive().map_err(|error| {
        StorageError::Migration(format!(
            "another storage operation holds the migration lock: {error}"
        ))
    })?;
    let mut connection = Connection::open_with_flags(path, OpenFlags::SQLITE_OPEN_READ_WRITE)?;
    // WAL mode is a durable SQLite file-header setting and is intentionally
    // established after the read-only compatibility check but before the schema
    // transaction. A failed migration guarantees data/schema/user_version
    // rollback; it may still leave the compatible legacy database in WAL mode.
    configure_connection(&connection)?;
    let transaction = connection
        .transaction_with_behavior(TransactionBehavior::Immediate)
        .map_err(|error| StorageError::Migration(error.to_string()))?;
    let result = (|| -> StorageResult<()> {
        if layout == HealthLayout::LegacyV0 {
            transaction.execute_batch(&format!(
                "PRAGMA application_id={HEALTH_APPLICATION_ID}; PRAGMA user_version=1;"
            ))?;
        }
        transaction.execute_batch(
            "ALTER TABLE receipts ADD COLUMN projected_generation TEXT;
             ALTER TABLE receipts ADD COLUMN projection_mapping_version INTEGER;
             CREATE TABLE storage_meta (
                 singleton INTEGER PRIMARY KEY CHECK(singleton = 1),
                 control_store_id TEXT NOT NULL,
                 created_at TEXT NOT NULL
             );
             CREATE TABLE projection_state (
                 singleton INTEGER PRIMARY KEY CHECK(singleton = 1),
                 generation_id TEXT NOT NULL,
                 mapping_version INTEGER NOT NULL,
                 storage_identity TEXT NOT NULL,
                 updated_at TEXT NOT NULL
             );",
        )?;
        transaction.execute(
            "INSERT INTO storage_meta(singleton,control_store_id,created_at) VALUES (1,?1,?2)",
            params![control_store_id, Utc::now().to_rfc3339()],
        )?;
        transaction.execute(
            "INSERT INTO projection_state(singleton,generation_id,mapping_version,storage_identity,updated_at) VALUES (1,?1,?2,'unverified',?3)",
            params![uuid::Uuid::new_v4().to_string(), DEFAULT_MAPPING_VERSION, Utc::now().to_rfc3339()],
        )?;
        transaction.execute_batch(&format!("PRAGMA user_version={HEALTH_SCHEMA_VERSION};"))?;
        #[cfg(test)]
        if FAIL_MIGRATION.with(std::cell::Cell::get) {
            return Err(StorageError::Migration(
                "injected migration failure".to_owned(),
            ));
        }
        Ok(())
    })();
    if let Err(error) = result {
        return Err(StorageError::Migration(error.to_string()));
    }
    transaction
        .commit()
        .map_err(|error| StorageError::Migration(error.to_string()))?;
    set_private_file(path)?;
    verify_health_database(path, Some(control_store_id))
}

pub fn verify_health_database(
    path: &Path,
    expected_control_store_id: Option<&str>,
) -> StorageResult<()> {
    let connection = Connection::open_with_flags(path, OpenFlags::SQLITE_OPEN_READ_ONLY)
        .map_err(classify_open_error)?;
    verify_integrity(&connection)?;
    if pragma_i64(&connection, "application_id")? != HEALTH_APPLICATION_ID {
        return Err(StorageError::Incompatible(
            "health application_id does not match BZHR".to_owned(),
        ));
    }
    if pragma_i64(&connection, "user_version")? != HEALTH_SCHEMA_VERSION {
        return Err(StorageError::Incompatible(
            "health schema version is not current".to_owned(),
        ));
    }
    verify_v2_layout(&connection, expected_control_store_id)
}

pub fn open_health_database(path: &Path) -> StorageResult<Connection> {
    verify_health_database(path, None)?;
    let connection = Connection::open_with_flags(path, OpenFlags::SQLITE_OPEN_READ_WRITE)?;
    configure_connection(&connection)?;
    verify_v2_layout(&connection, None)?;
    Ok(connection)
}

fn configure_connection(connection: &Connection) -> StorageResult<()> {
    connection.busy_timeout(Duration::from_secs(5))?;
    connection.execute_batch(
        "PRAGMA journal_mode=WAL;
         PRAGMA foreign_keys=ON;
         PRAGMA synchronous=FULL;
         PRAGMA secure_delete=ON;",
    )?;
    Ok(())
}

fn pragma_i64(connection: &Connection, name: &str) -> StorageResult<i64> {
    connection
        .query_row(&format!("PRAGMA {name}"), [], |row| row.get(0))
        .map_err(StorageError::from)
}

fn verify_integrity(connection: &Connection) -> StorageResult<()> {
    let quick: String = connection
        .query_row("PRAGMA quick_check", [], |row| row.get(0))
        .map_err(classify_open_error)?;
    if quick != "ok" {
        return Err(StorageError::Corrupt(format!(
            "SQLite quick_check returned {quick}"
        )));
    }
    let foreign_key_failure: Option<i64> = connection
        .query_row("PRAGMA foreign_key_check", [], |row| row.get(0))
        .optional()?;
    if foreign_key_failure.is_some() {
        return Err(StorageError::Corrupt(
            "foreign key validation failed".to_owned(),
        ));
    }
    Ok(())
}

fn user_objects(connection: &Connection, kind: &str) -> StorageResult<BTreeSet<String>> {
    let mut statement = connection.prepare(
        "SELECT name FROM sqlite_schema WHERE type=?1 AND name NOT LIKE 'sqlite_%' ORDER BY name",
    )?;
    let rows = statement.query_map([kind], |row| row.get::<_, String>(0))?;
    Ok(rows.collect::<rusqlite::Result<BTreeSet<_>>>()?)
}

fn column_names(connection: &Connection, table: &str) -> StorageResult<Vec<String>> {
    let mut statement = connection.prepare(&format!("PRAGMA table_info('{table}')"))?;
    let rows = statement.query_map([], |row| row.get::<_, String>(1))?;
    Ok(rows.collect::<rusqlite::Result<Vec<_>>>()?)
}

fn verify_legacy_layout(connection: &Connection) -> StorageResult<()> {
    verify_named_objects(connection, LEGACY_TABLES, LEGACY_INDEXES)?;
    require_columns(
        connection,
        "pairing_codes",
        &["code_hash", "expires_at", "used_at"],
    )?;
    require_columns(
        connection,
        "devices",
        &["device_id", "token_hash", "created_at", "revoked_at"],
    )?;
    require_columns(
        connection,
        "events",
        &[
            "device_id",
            "event_id",
            "revision",
            "operation",
            "kind",
            "health_type",
            "source_json",
            "start_utc",
            "end_utc",
            "value",
            "unit",
            "payload_json",
            "payload_hash",
            "updated_at",
        ],
    )?;
    require_columns(
        connection,
        "receipts",
        &[
            "commit_sequence",
            "batch_id",
            "device_id",
            "content_hash",
            "accepted_events",
            "changed_events",
            "requires_projection",
            "received_at",
            "projected_at",
        ],
    )?;
    require_columns(
        connection,
        "audit",
        &["id", "device_id", "action", "batch_id", "at", "detail"],
    )?;
    require_columns(
        connection,
        "outbox",
        &[
            "id",
            "device_id",
            "batch_id",
            "metric_name",
            "created_at",
            "processed_at",
            "attempts",
            "last_error",
        ],
    )?;
    require_columns(
        connection,
        "erasures",
        &[
            "device_id",
            "erasure_id",
            "erasure_secret_hash",
            "requested_at",
            "metrics_deleted_at",
            "backups_expired_at",
            "backup_delete_by",
            "last_error",
        ],
    )?;
    require_schema_fragments(connection)?;
    Ok(())
}

fn verify_v2_layout(
    connection: &Connection,
    expected_control_store_id: Option<&str>,
) -> StorageResult<()> {
    verify_named_objects(connection, V2_TABLES, LEGACY_INDEXES)?;
    require_columns(
        connection,
        "receipts",
        &[
            "commit_sequence",
            "batch_id",
            "device_id",
            "content_hash",
            "accepted_events",
            "changed_events",
            "requires_projection",
            "received_at",
            "projected_at",
            "projected_generation",
            "projection_mapping_version",
        ],
    )?;
    require_columns(
        connection,
        "storage_meta",
        &["singleton", "control_store_id", "created_at"],
    )?;
    require_columns(
        connection,
        "projection_state",
        &[
            "singleton",
            "generation_id",
            "mapping_version",
            "storage_identity",
            "updated_at",
        ],
    )?;
    require_schema_fragments(connection)?;
    let control_store_id: String = connection
        .query_row(
            "SELECT control_store_id FROM storage_meta WHERE singleton=1",
            [],
            |row| row.get(0),
        )
        .map_err(|_| {
            StorageError::Incompatible("health storage_meta singleton is missing".to_owned())
        })?;
    if control_store_id.is_empty()
        || expected_control_store_id.is_some_and(|expected| expected != control_store_id)
    {
        return Err(StorageError::Incompatible(
            "health database is linked to a different control store".to_owned(),
        ));
    }
    let projection_rows: i64 = connection.query_row(
        "SELECT count(*) FROM projection_state WHERE singleton=1 AND generation_id<>'' AND mapping_version>0 AND storage_identity<>''",
        [],
        |row| row.get(0),
    )?;
    if projection_rows != 1 {
        return Err(StorageError::Incompatible(
            "projection_state singleton is missing or invalid".to_owned(),
        ));
    }
    Ok(())
}

fn verify_named_objects(
    connection: &Connection,
    tables: &[&str],
    indexes: &[&str],
) -> StorageResult<()> {
    let expected_tables = tables.iter().map(|name| (*name).to_owned()).collect();
    let expected_indexes = indexes.iter().map(|name| (*name).to_owned()).collect();
    if user_objects(connection, "table")? != expected_tables
        || user_objects(connection, "index")? != expected_indexes
        || !user_objects(connection, "trigger")?.is_empty()
    {
        return Err(StorageError::Incompatible(
            "database object inventory does not match the supported layout".to_owned(),
        ));
    }
    Ok(())
}

#[cfg(test)]
thread_local! {
    static FAIL_MIGRATION: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
}

fn require_columns(connection: &Connection, table: &str, expected: &[&str]) -> StorageResult<()> {
    let actual = column_names(connection, table)?;
    let expected: Vec<String> = expected.iter().map(|name| (*name).to_owned()).collect();
    if actual != expected {
        return Err(StorageError::Incompatible(format!(
            "table {table} does not match the supported column layout"
        )));
    }
    Ok(())
}

fn require_schema_fragments(connection: &Connection) -> StorageResult<()> {
    let required = [
        ("devices", "token_hash text not null unique"),
        ("events", "primary key(device_id, event_id)"),
        ("receipts", "batch_id text not null unique"),
        ("erasures", "erasure_secret_hash text not null unique"),
    ];
    for (table, fragment) in required {
        let sql: String = connection.query_row(
            "SELECT lower(replace(replace(sql, char(10), ' '), '  ', ' ')) FROM sqlite_schema WHERE type='table' AND name=?1",
            [table],
            |row| row.get(0),
        )?;
        let compact_sql: String = sql
            .chars()
            .filter(|character| !character.is_whitespace())
            .collect();
        let compact_fragment: String = fragment
            .chars()
            .filter(|character| !character.is_whitespace())
            .collect();
        if !compact_sql.contains(&compact_fragment) {
            return Err(StorageError::Incompatible(format!(
                "table {table} is missing a required constraint"
            )));
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    const LEGACY_SCHEMA: &str = r#"
CREATE TABLE pairing_codes (code_hash TEXT PRIMARY KEY,expires_at TEXT NOT NULL,used_at TEXT);
CREATE TABLE devices (device_id TEXT PRIMARY KEY,token_hash TEXT NOT NULL UNIQUE,created_at TEXT NOT NULL,revoked_at TEXT);
CREATE TABLE events (
 device_id TEXT NOT NULL,event_id TEXT NOT NULL,revision INTEGER NOT NULL,operation TEXT NOT NULL,kind TEXT NOT NULL,health_type TEXT NOT NULL,
 source_json TEXT,start_utc TEXT,end_utc TEXT,value REAL,unit TEXT,payload_json TEXT NOT NULL,payload_hash TEXT NOT NULL,updated_at TEXT NOT NULL,
 PRIMARY KEY(device_id,event_id),FOREIGN KEY(device_id) REFERENCES devices(device_id));
CREATE INDEX events_type ON events(device_id,health_type,operation);
CREATE TABLE receipts (
 commit_sequence INTEGER PRIMARY KEY AUTOINCREMENT,batch_id TEXT NOT NULL UNIQUE,device_id TEXT NOT NULL,content_hash TEXT NOT NULL,
 accepted_events INTEGER NOT NULL,changed_events INTEGER NOT NULL,requires_projection INTEGER NOT NULL DEFAULT 0,received_at TEXT NOT NULL,projected_at TEXT,
 FOREIGN KEY(device_id) REFERENCES devices(device_id));
CREATE INDEX receipts_device ON receipts(device_id,commit_sequence);
CREATE TABLE audit (id INTEGER PRIMARY KEY AUTOINCREMENT,device_id TEXT NOT NULL,action TEXT NOT NULL,batch_id TEXT,at TEXT NOT NULL,detail TEXT,FOREIGN KEY(device_id) REFERENCES devices(device_id));
CREATE TABLE outbox (id INTEGER PRIMARY KEY AUTOINCREMENT,device_id TEXT NOT NULL,batch_id TEXT NOT NULL,metric_name TEXT NOT NULL,created_at TEXT NOT NULL,processed_at TEXT,attempts INTEGER NOT NULL DEFAULT 0,last_error TEXT,FOREIGN KEY(device_id) REFERENCES devices(device_id),FOREIGN KEY(batch_id) REFERENCES receipts(batch_id));
CREATE INDEX outbox_pending ON outbox(processed_at,device_id,metric_name);
CREATE TABLE erasures (device_id TEXT PRIMARY KEY,erasure_id TEXT NOT NULL UNIQUE,erasure_secret_hash TEXT NOT NULL UNIQUE,requested_at TEXT NOT NULL,metrics_deleted_at TEXT,backups_expired_at TEXT,backup_delete_by TEXT NOT NULL,last_error TEXT);
"#;

    fn paths(directory: &TempDir) -> StoragePaths {
        let root = directory.path().join("data");
        StoragePaths::new(
            root.clone(),
            root.join("health.db"),
            directory.path().join("control/control.db"),
            directory.path().join("control/mirror"),
            directory.path().join("backups"),
        )
        .with_forbidden_paths(Vec::new())
    }

    fn create_legacy(path: &Path) -> Connection {
        let connection = Connection::open(path).unwrap();
        connection.execute_batch(LEGACY_SCHEMA).unwrap();
        connection
    }

    #[test]
    fn current_database_initializes_and_reopens_without_mutation() {
        let directory = TempDir::new().unwrap();
        let storage = paths(&directory);
        storage.prepare_empty_layout().unwrap();
        initialize_health_database(&storage.health_db, "control-test").unwrap();
        assert_eq!(
            classify_health_database(&storage.health_db).unwrap(),
            HealthLayout::CurrentV2
        );
        open_health_database(&storage.health_db).unwrap();
    }

    #[test]
    fn failed_legacy_migration_rolls_back_data_schema_and_version() {
        let directory = TempDir::new().unwrap();
        let storage = paths(&directory);
        storage.prepare_empty_layout().unwrap();
        let connection = create_legacy(&storage.health_db);
        connection
            .execute(
                "INSERT INTO devices(device_id,token_hash,created_at) VALUES ('phone-1','token-hash','2026-09-19T00:00:00Z')",
                [],
            )
            .unwrap();
        drop(connection);
        assert_eq!(
            classify_health_database(&storage.health_db).unwrap(),
            HealthLayout::LegacyV0
        );
        FAIL_MIGRATION.with(|flag| flag.set(true));
        let result = migrate_health_database(&storage.health_db, "control-test");
        FAIL_MIGRATION.with(|flag| flag.set(false));
        assert!(matches!(result, Err(StorageError::Migration(_))));
        assert_eq!(
            classify_health_database(&storage.health_db).unwrap(),
            HealthLayout::LegacyV0
        );
        let readback =
            Connection::open_with_flags(&storage.health_db, OpenFlags::SQLITE_OPEN_READ_ONLY)
                .unwrap();
        let device: String = readback
            .query_row("SELECT device_id FROM devices", [], |row| row.get(0))
            .unwrap();
        assert_eq!(device, "phone-1");
        assert_eq!(pragma_i64(&readback, "application_id").unwrap(), 0);
        assert_eq!(pragma_i64(&readback, "user_version").unwrap(), 0);
        assert_eq!(
            column_names(&readback, "receipts").unwrap(),
            vec![
                "commit_sequence",
                "batch_id",
                "device_id",
                "content_hash",
                "accepted_events",
                "changed_events",
                "requires_projection",
                "received_at",
                "projected_at"
            ]
        );
    }

    #[test]
    fn supported_v0_and_v1_databases_migrate_without_data_loss() {
        for claimed_v1 in [false, true] {
            let directory = TempDir::new().unwrap();
            let storage = paths(&directory);
            storage.prepare_empty_layout().unwrap();
            let connection = create_legacy(&storage.health_db);
            connection
                .execute(
                    "INSERT INTO devices(device_id,token_hash,created_at) VALUES ('phone-1','token-hash','2026-09-19T00:00:00Z')",
                    [],
                )
                .unwrap();
            if claimed_v1 {
                connection
                    .execute_batch(&format!(
                        "PRAGMA application_id={HEALTH_APPLICATION_ID}; PRAGMA user_version=1;"
                    ))
                    .unwrap();
            }
            drop(connection);
            migrate_health_database(&storage.health_db, "control-test").unwrap();
            verify_health_database(&storage.health_db, Some("control-test")).unwrap();
            let connection = open_health_database(&storage.health_db).unwrap();
            let device: String = connection
                .query_row("SELECT device_id FROM devices", [], |row| row.get(0))
                .unwrap();
            assert_eq!(device, "phone-1");
        }
    }

    #[test]
    fn unknown_future_partial_and_corrupt_databases_fail_closed() {
        let unknown_dir = TempDir::new().unwrap();
        let unknown = paths(&unknown_dir);
        unknown.prepare_empty_layout().unwrap();
        let connection = create_legacy(&unknown.health_db);
        connection
            .execute_batch("PRAGMA application_id=12345; PRAGMA user_version=2;")
            .unwrap();
        drop(connection);
        assert!(matches!(
            classify_health_database(&unknown.health_db),
            Err(StorageError::Incompatible(_))
        ));

        let future_dir = TempDir::new().unwrap();
        let future = paths(&future_dir);
        future.prepare_empty_layout().unwrap();
        let connection = create_legacy(&future.health_db);
        connection
            .execute_batch(&format!(
                "PRAGMA application_id={HEALTH_APPLICATION_ID}; PRAGMA user_version=99;"
            ))
            .unwrap();
        drop(connection);
        assert!(matches!(
            classify_health_database(&future.health_db),
            Err(StorageError::Incompatible(_))
        ));

        let partial_dir = TempDir::new().unwrap();
        let partial = paths(&partial_dir);
        partial.prepare_empty_layout().unwrap();
        let connection = Connection::open(&partial.health_db).unwrap();
        connection
            .execute_batch("CREATE TABLE devices(device_id TEXT PRIMARY KEY);")
            .unwrap();
        drop(connection);
        assert!(matches!(
            classify_health_database(&partial.health_db),
            Err(StorageError::Incompatible(_))
        ));

        let corrupt_dir = TempDir::new().unwrap();
        let corrupt = paths(&corrupt_dir);
        corrupt.prepare_empty_layout().unwrap();
        fs::write(&corrupt.health_db, b"not a sqlite database").unwrap();
        assert!(matches!(
            classify_health_database(&corrupt.health_db),
            Err(StorageError::Corrupt(_)) | Err(StorageError::Sql(_))
        ));
    }

    #[test]
    fn rejected_parent_component_creates_nothing() {
        let directory = TempDir::new().unwrap();
        let root = directory.path().join("data");
        let storage = StoragePaths::new(
            root.clone(),
            root.join("nested/../health.db"),
            directory.path().join("control/control.db"),
            directory.path().join("control/mirror"),
            directory.path().join("backups"),
        )
        .with_forbidden_paths(Vec::new());
        assert!(storage.validate_no_write().is_err());
        assert!(!root.exists());
    }

    #[cfg(unix)]
    #[test]
    fn symlink_and_hardlink_databases_are_rejected() {
        use std::os::unix::fs::symlink;
        let directory = TempDir::new().unwrap();
        let storage = paths(&directory);
        fs::create_dir_all(&storage.data_root).unwrap();
        let target = directory.path().join("target.db");
        fs::write(&target, b"not sqlite but nonempty").unwrap();
        symlink(&target, &storage.health_db).unwrap();
        assert!(storage.validate_no_write().is_err());
        fs::remove_file(&storage.health_db).unwrap();
        fs::hard_link(&target, &storage.health_db).unwrap();
        assert!(storage.validate_no_write().is_err());

        let linked_parent = directory.path().join("linked-data");
        let actual_parent = directory.path().join("actual-data");
        fs::create_dir(&actual_parent).unwrap();
        symlink(&actual_parent, &linked_parent).unwrap();
        let linked_storage = StoragePaths::new(
            linked_parent.clone(),
            linked_parent.join("health.db"),
            directory.path().join("other-control/control.db"),
            directory.path().join("other-control/mirror"),
            directory.path().join("other-backups"),
        )
        .with_forbidden_paths(Vec::new());
        assert!(linked_storage.validate_no_write().is_err());
        assert!(!actual_parent.join("health.db").exists());
    }

    #[test]
    fn legacy_alias_is_rejected_before_any_write() {
        let directory = TempDir::new().unwrap();
        let legacy_root = directory.path().join("legacy-data");
        let storage = StoragePaths::new(
            legacy_root.clone(),
            legacy_root.join("health.db"),
            directory.path().join("control/control.db"),
            directory.path().join("control/mirror"),
            directory.path().join("backups"),
        )
        .with_forbidden_paths(vec![legacy_root.clone()]);
        assert!(storage.validate_no_write().is_err());
        assert!(!legacy_root.exists());
    }
}
