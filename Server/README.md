# Boaz private Health receiver

This Rust service receives explicitly consented iPhone batches into a dedicated Tokyo health SQLite ledger, keeps revocation/erasure/recovery facts in a separate control SQLite ledger, and projects selected numeric values to a verified **native** VictoriaMetrics process. It never deploys itself or enables upload automatically. It is not the older Mac-to-Tokyo replacement target.

## HTTP contract

The receiver binds loopback `127.0.0.1` on port `8787` by default. Private HTTPS is expected to terminate at Tailscale Serve; do not use Funnel. Device routes require `Authorization: Bearer <device-token>`.

1. `boaz-health-receiver pair-code` prints a ten-minute, one-use 64-hex pairing secret. `POST /v1/health/pairings` accepts `{"code":"...","device_id":"stable-random-id"}` and returns a device token once. Pairing does not grant upload consent.
2. `POST /v1/health/batches` accepts schema v1 with at most 200 event revisions and 128 KiB. Events carry stable ID, monotone revision, `upsert`/`delete`, kind/type, optional source/times/value/unit and object metadata. Unknown or mismatched units may remain in SQLite but never silently become metrics.
3. A receipt binds `batch_id`, exact request-body SHA-256, accepted/changed counts, commit sequence and receive time. Identical retries return the original receipt; the same batch ID with different bytes is HTTP 409. `cloud_saved` proves a health SQLite commit. `metrics_current` additionally requires readback on the current projection generation/mapping version at `projected_at`.
4. `POST /v1/health/revoke` and `revoke-device DEVICE_ID` revoke credentials without deleting health history. Authorization checks the control tombstone before trusting the health-device row.
5. Cloud erasure uses a stable `erasure_id` and 64-hex recovery secret. Legal states are `pending_metrics`, `pending_backup_expiry`, and `complete`. The app keeps recovery material through pending states. A receiver status is evidence about this receiver at its returned times, not proof about unknown external copies.

The app sends versioned `boaz.sleep.deep_minutes` quantities in minutes. Raw sleep categories are retained without independently claiming a deep-sleep metric. Supported metric names begin `boaz_health_v1_`.

## Storage lifecycle

P0-R2.1 adds the offline-only adopt-active-set MANAGED_BACKUP_PATH command. It requires BOAZ_HEALTH_BOOTSTRAP=1, upload off, a stopped receiver, coordinator/lifecycle/health-operation locks, independently verified encrypted recovery storage and a pinned SSH custodian. Only a fresh genesis store without prior health events, receipts, devices, erasures, pairing state or durable `sqlite_sequence` history can enter this path. It makes a managed SQLite snapshot, confirms exactly one genesis backup control event off host, compares frozen source and snapshot receipt inventories, binds the empty acknowledgement-journal baseline once, confirms the baseline and initial journal head off host, then writes adopted-unactivated.json. An interrupted baseline reserve/CAS may retry only the same snapshot, operation ID and predecessor, followed by exact authenticated successor readback; a second backup or guessed success is rejected. It does not publish active-set.json or authorize serve. An older or emptied ledger with unproven pre-journal writes cannot acquire an RPO=0 claim.

The v2 custody state carries one monotonic revision across the control head, full acknowledgement-journal head and immutable baseline digest. Old v1 custody needs explicit offline, no-loss migration; unknown or pending histories fail closed. Normal control publication now retains a fixed intent and requires reserve, local DB/mirror/head persistence and matching off-host CAS before success. Batch and pairing success likewise require a matching independent CAS; GET does not expose a locally committed but independently unconfirmed receipt. These are code and synthetic-test properties, not evidence that an independent custodian has been provisioned.

P0-R2.1b tightened that comparison: a matching control head alone is insufficient. Before a governed control event, local verification also checks the confirmed acknowledgement-journal prefix, immutable baseline digest and revision derived from the full v2 tuple; any changed member closes the write path. Its original 30-second deadline covered the SSH protocol I/O loop only. P0-R2.1c starts the caller's budget before spawn and bounds caller-side child-reap attempts, but an uninterruptible OS operation still prevents a strict absolute 30-second guarantee. Synthetic process-kill cases for revoke and erasure intents cover after-reserve, before-CAS and after-CAS recovery; they do not attest independent SSH, encrypted mounts or every persistence boundary.

P0-R2.1a requires an additional `state-v2.history-seal.json` on the custodian. Each committed v2 operation's full reservation and successor are chained into that seal; a changed historical intent cannot pass by keeping the public head unchanged. Publication is history record → seal → public head. An unsealed tail may advance only through the original reservation and an identical CAS retry. A pre-seal v2 directory fails closed and needs separately authorized offline review; the receiver must not manufacture a seal over possibly changed history. The seal detects one-file history tampering, **not** a coordinated rollback of the entire independently administered custody domain.

Governed managed-backup events now bind their durable intent to a validated backup directory, exact database/manifest bytes and Unix file identities. The same proof is checked before local convergence and immediately before off-host CAS. Backup-expiry confirmation binds and rechecks the complete managed inventory and erasure request time. Missing, replaced, linked or changed artifacts leave the original reservation unresolved; do not delete or replace the backup to make a test pass. Other external-evidence facts without a replayable proof, including interrupted health/metrics erasure verification, stop for operator revalidation. Operational `restore_completed` remains unavailable until P0-R2.2 binds native VM readback and activation evidence.

The production SSH configuration keys are BOAZ_HEALTH_CUSTODY_SSH_TARGET, BOAZ_HEALTH_CUSTODY_KNOWN_HOSTS and BOAZ_HEALTH_CUSTODY_IDENTITY_FILE. They name a separately administered forced-command account, pinned host-key file and dedicated client key. The client invokes `/usr/bin/ssh`, not an executable selected by `PATH`, clears inherited environment, disables the SSH agent, and checks that the executable, both identity files and every ancestor are root-owned, non-symlink and not group/world writable; leaf files must be regular, nonempty and unlinked aliases. Checks repeat before each request. Operators must place the key and known-host file under root-controlled directories that the receiver can **read** through a controlled group or ACL, but cannot replace. These code checks do not attest the approved Linux host, privileged replacement resistance or what SSH actually consumed during a real run; an independently isolated host/ownership test remains required. BOAZ_HEALTH_CUSTODY_ROOT belongs only to that independent host. No repository file, local JSON fixture or configuration flag proves independent custody. The service template deliberately does not pre-run verify-storage: a crash may require exact-intent custody reconciliation before full local-chain verification.

Before creating a health backup, the CLI converges already-confirmed erasure intents against the source ledger. Prune uses pinned directory/file identities and refuses to confirm a substituted artifact. A matching custodian temporary file can be completed by the original reservation retry; an unknown or old anonymous temporary file requires offline investigation. Local synthetic process-kill tests cover selected persisted phases, not a live independent SSH host or every same-UID replacement race.

`serve` does not create or claim an unknown database. Configure validated paths, then use an explicit lifecycle command before starting the service.

| Variable | Default / purpose |
|---|---|
| `BOAZ_HEALTH_DATA_ROOT` | `/opt/boaz-health/data`; the health DB must be its direct child. |
| `BOAZ_HEALTH_DB` | `/opt/boaz-health/data/health.db`; BZHR health ledger. |
| `BOAZ_HEALTH_CONTROL_DB` | `/opt/boaz-health/control/control.db`; BZHC control ledger. |
| `BOAZ_HEALTH_CONTROL_MIRROR_DIR` | `/opt/boaz-health/control/mirror`; create-new control event mirrors. |
| `BOAZ_HEALTH_BACKUP_DIR` | `/opt/boaz-health/backups`; managed health snapshots. |
| `BOAZ_HEALTH_CONTROL_VOLUME_ENCRYPTED` | Gate flag only; it is not encryption evidence. |
| `BOAZ_HEALTH_COORD_DIR` | Mandatory stable control-domain coordinator directory; all ordinary commands acquire its lock before resolving storage paths. A missing directory never selects legacy fixed paths. |
| `BOAZ_HEALTH_BOOTSTRAP` | Explicit offline-only `1` gate for coordinator and pre-adoption storage/journal initialization; keep `0` in the service environment. |
| `BOAZ_HEALTH_ACK_JOURNAL_DIR` | Separately initialized, encrypted recovery-domain journal for exact prepared batch bytes and confirmed receipt/pairing facts. Omission denies pairing and batch receipt paths. |
| `BOAZ_HEALTH_CUSTODY_ROOT` | Only for the separately administered off-host `custody-protocol` forced-command process; never points at receiver storage. No host or key is provisioned by this repository. |
| `BOAZ_HEALTH_BACKUP_VOLUME_ENCRYPTED` | Backup/prune operator gate; `1` alone is insufficient without a verified dm-crypt mount on an independent device. |

Implemented offline lifecycle commands; these do **not** make `serve` available:

```sh
BOAZ_HEALTH_BOOTSTRAP=1 boaz-health-receiver bootstrap-coordinator
# Choose init-storage for an empty store OR migrate-storage for the reviewed legacy layout.
BOAZ_HEALTH_BOOTSTRAP=1 boaz-health-receiver init-storage
# BOAZ_HEALTH_BOOTSTRAP=1 boaz-health-receiver migrate-storage
# Only after separately provisioning an approved encrypted recovery volume:
BOAZ_HEALTH_BOOTSTRAP=1 BOAZ_HEALTH_ACK_JOURNAL_DIR=/approved/encrypted/ack-journal boaz-health-receiver init-ack-journal
boaz-health-receiver verify-storage
```

- Bootstrap requires upload off and a pre-provisioned private coordinator parent. `init-storage`, `migrate-storage` and `init-ack-journal` require the explicit offline bootstrap gate before adoption; the acknowledgement journal additionally requires the approved independent recovery mount.
- `init-storage` is only for an absent health DB, absent control DB and empty managed-backup inventory.
- `migrate-storage` requires upload off and the global operation lock. It accepts only the one reviewed legacy health layout and imports legacy revocation/erasure facts into the control store.
- Legacy migration classifies the health file read-only before changing either store. A partially seeded control chain can resume only when its existing events match an exact prefix of the legacy facts; an unrelated or divergent control store is rejected.
- `verify-storage` is read-only. It checks validated paths, store identities/versions, exact schemas, quick/foreign-key checks and the health-to-control store ID binding.

Every path check rejects traversal, symlinked file/parent components, hard links, non-regular or zero-byte database files and a dev/inode alias to the old `/opt/boaz/data` target before creating a DB, WAL, SHM or lock.

### Health ledger BZHR

- SQLite `application_id=0x425A4852` (`BZHR`), current schema v2.
- v0 is the exact reviewed legacy seven-table/three-index layout; v1 claims it; v2 adds control-store and projection-generation evidence.
- Unknown application IDs, future versions, partial layouts and corrupt files fail without reset.
- Legal v0→v1→v2 migration is one immediate transaction; every open checks application ID, user version, schema, `quick_check` and `foreign_key_check`.

### Control ledger BZHC

- SQLite `application_id=0x425A4843` (`BZHC`), current schema v3, WAL/FULL, foreign keys and secure delete. Classified v1/v2 event tables are renamed into an immutable historical prefix; future facts use a separate append-only suffix. Original event hashes are not recalculated, and no historical table is dropped or copied. Unknown/future layouts are not reset.
- Append-only events contain stable IDs, event type, device/credential/erasure hashes, times and previous/current hashes. They never contain health payloads, plaintext bearer tokens or plaintext erasure secrets.
- The DB, `control.head.json` and create-new mirror records must agree. A publication lock serializes control-chain commits. Startup, append and worker reconciliation check the full chain; hot-path authentication checks only the database/head/latest mirror tail so request cost does not grow with history. A missing or changed older mirror may therefore be detected only at the next full reconciliation (normally the idle 60-second tick, or sooner after a wake), not immediately on the next auth request. No hard detection deadline is claimed under worker failure. Detected divergence fails closed.
- Revocation/erasure intent commits here first. Health rows and metric state then converge idempotently; the implementation does not claim an atomic transaction across two SQLite files.
- Ingest, revocation, erasure and backup/prune use existing operation/lifecycle locks around their critical sections. P0-R2 requires the stable coordinator lock before storage-path resolution; there is no missing-directory fallback. Ordinary service/writes remain intentionally blocked even if local journal and pointer say `ActiveVerified`: startup does not yet authenticate off-host custody or read back the restarted live VM. This is a safety stop, not a usable cutover workflow. Local lock tests do not prove Tokyo crash recovery.

## Projection worker

The worker never accepts a VictoriaMetrics URL from a phone. It derives `boaz_health_v1_*` series from committed health rows, verifies native executable/storage/listener identity, writes through loopback `127.0.0.1:8428`, then reads back the expected values.

A controlled generation marker and mapping version bind projection evidence. Generation/version drift makes old receipts no longer current and queues required rebuilds. After external readback, outbox completion and receipt generation/version/time update together in one immediate SQLite transaction. A crash before that transaction leaves a retryable job; repeated external writes are deterministic.

The worker starts by draining pending work, reuses one HTTP client, wakes on ingest/erasure/generation changes, and uses a 60-second idle reconciliation tick with bounded exponential retry. HTTP serving and worker lifetime are supervised together; an unexpected worker exit must terminate the process for systemd restart. A `metrics_current` receipt means the current generation was read back at its timestamp, not that VictoriaMetrics retains it forever.

Current labels are intentionally narrow. Label hashing is unimplemented; do not project arbitrary Health metadata into labels.

## Deployment gates

Build on the reviewed Linux host/toolchain and run as a dedicated unprivileged `boaz-health` account with umask 0077. Configure Tailscale Serve only after an access rule limits the intended phone identity:

```sh
tailscale serve --bg http://127.0.0.1:8787
```

Run VictoriaMetrics as a native binary with its configured storage path and `-httpListenAddr=127.0.0.1:8428`. Docker proxy or HTTP 2xx cannot satisfy native identity.

The current Linux encrypted-mount verifier recognizes dm-crypt device-mapper identities. `provider-managed encryption` is not currently accepted by code and needs a deterministic verifier plus recovery proof. Health data, metrics, managed backups and the control ledger's independent recovery domain must all have recorded encryption, ownership, key custody and restart behavior. Production `backup` and `prune-backups` reject a flag-only claim: before health/control mutation they require `BOAZ_HEALTH_BACKUP_VOLUME_ENCRYPTED=1`, a physically verified dm-crypt backup mount, and a backup device distinct from both the health and control database devices. A test-only synthetic seam exercises backup/prune logic without weakening this production CLI gate. These local tests do not attest a Tokyo mount.

Copy the files under [`deploy`](deploy) only as operator-reviewed templates. They are not installed by this repository. The systemd example deliberately has **no** `verify-storage` pre-start command: after a crash, the governed startup path must first reconcile the exact pending custody reservation before full local-chain verification. It grants write access only to the configured health, control, backup and VM directories, and the template explicitly says it is not deployable before P0-R2.2 activation and VM verification. Keep `BOAZ_HEALTH_UPLOAD_ENABLED=0` until private HTTPS, every required storage domain, native VM identity, backup/restore and rollback have current evidence.

P0-R2.1c acceptance remains open. The fixed SSH path, root-controlled ancestor contract, pre-spawn deadline and bounded caller-side cleanup have local source/tests, but not an approved Linux/SSH identity-replacement or process-supervision drill. A privileged path replacement or uninterruptible OS operation is outside the code-only guarantee, and an HTTP batch's complete confirmation time remains unmeasured. The custodian's history seal detects an inconsistent file, but does not prove that the entire custody domain has not rolled back together; that requires a separately owned monotonic witness. No approved independent Linux host, witness, fixed p95/p99 target, or three-run 10k/100k confirmation baseline has been evidenced here. Do not interpret local fault tests or a successfully parsed custody reply as those operational proofs.

P0-R2.1d local counterexamples found two false-success paths and closed them in source: control publication keeps its pending intent until an independent exact custodian read confirms the CAS successor, and erasure status requires settled control/custody facts with matching intent and metrics/backup verification times. A health-only `complete` row is not authoritative. A separate test demonstrates that rolling back the *whole* custodian history, seal and head to an older consistent state remains undetectable locally. No monotonic witness has been provisioned, so normal production startup and upload remain blocked. Operators must not treat these synthetic fixes as independent-host or VM/backup deletion evidence.

The projection worker now acquires the existing health-operation freeze lock before erasure reconciliation or VM work. When backup/ingest holds it, the worker returns a retryable busy error without mutating the ledger. This closes a locally reproduced lock bypass but increases contention for the duration of a projection pass; fixed-host latency evidence is required before an efficiency claim.

## Backup and recovery

`backup PATH` takes a WAL-aware health-ledger snapshot, checks integrity, hashes the file, writes a manifest containing snapshot UUID, schema version and the current control checkpoint, then appends a control event. `prune-backups DIRECTORY` checks the entire managed inventory against database files, manifests, SHA-256 values and active control events before confirming expiry. P0-R writes a snapshot/hash/name-bound deletion intent *before* physical removal and can resume only a matching interrupted deletion. Divergent inventory fails closed. This improves the crash window but does not itself prove process-kill recovery. The commands still do not establish the complete disaster-recovery contract. Production acceptance additionally requires:

- readback verification that every snapshot file still matches its manifest SHA-256, schema and control checkpoint;
- process-crash, failed-removal, scheduler and alert evidence for the three-way inventory check, beyond the passing isolated fault tests;
- an off-host expected control head;
- restore only to an empty staging path;
- isolated native VictoriaMetrics cleanup/rebuild and readback, a completed new restore epoch and an operator-controlled cutover before replacing the running ledger.

The following operator commands now exist, but they are **not a completed restore workflow**:

```sh
boaz-health-receiver backup-control NEW_ENCRYPTED_DIRECTORY
boaz-health-receiver restore-control SOURCE --staging-path ROOT --expected-head FILE
boaz-health-receiver restore-health BACKUP --staging-path ROOT
```

`backup-control` creates a new hash-checked control bundle. `restore-control` compares a caller-supplied expected-head file and stages a verified control copy; that file alone does **not** prove independent custody. `restore-health` additionally requires the protected acknowledgement journal, an exact adopted-baseline snapshot match, and replayable confirmed receipts and pairing identities; it replays those facts before the control tombstones and erasure intents. Neither restore command opens or verifies the live health/control database, so a corrupt live database need not prevent staging; the validated live directory, upload-off gate and exclusive lifecycle lock are still required. This does **not** authorize serving or replacing a corrupt live database. The CLI then reports `projection_rebuild_required` and exits **nonzero**: it does not perform the isolated VM rebuild/readback, complete an epoch or replace the live database. All three commands require approved dm-crypt recovery storage on a separate device.

P0-R adds **library-level** pieces for a hash-checked, per-epoch recovery journal, active-set descriptor, Linux-only isolated native VM launcher, deterministic surviving-record oracle, full-series readback and evidence-bound control completion. The staging VM target is fixed to a separate loopback port and storage identity; a live VM snapshot is not accepted as a cleaned projection. These pieces are not yet an operator-run end-to-end command path. The current CLI has only a narrow, offline `adopt-active-set` that publishes `adopted-unactivated.json`; it has no accepted `restore-rebuild`, `restore-verify`, `restore-activate` or `restore-resume` path. Do not hand-edit an active-set pointer or invoke internal library methods to bypass frozen writers, independent expected-head custody, confirmed post-snapshot write accounting or post-restart live-VM readback. An old generation that may contain erased records is not a safe rollback candidate. No actual Tokyo recovery has been accepted. Never point a test restore at the running database or the old Mac replacement path.

P0-R2 adds code for exact-byte acknowledgement journaling and pairing-recovery records, plus library functions that check snapshot coverage and replay confirmed batches into a candidate. A prepared record is not an acknowledged upload; only a matching confirmed receipt may be returned. If an old snapshot predates the journal baseline or any legitimate confirmed write lacks its protected source, activation must remain blocked rather than claim zero data loss. The SSH custody client and off-host `custody-protocol` forced-command entry point implement a bounded `read/reserve/CAS` file protocol. P0-R2.1 connects custody to guarded routine control and acknowledgement paths and adds a narrow fresh-genesis adoption command, but no independently administered host has been tested. The current `serve` command therefore **fails closed** even when a local recovery phase claims `ActiveVerified`; do not interpret a local pointer or unit test as operational availability.

## Local verification

From the repository root:

```sh
cargo test --manifest-path Server/Cargo.toml --offline --locked
cargo clippy --manifest-path Server/Cargo.toml --offline --locked --all-targets -- -D warnings
python3 scripts/test_server_http.py --build
python3 scripts/verify_doc_governance.py
git diff --check
```

Use only synthetic records and isolated temporary storage. These checks can prove source behavior and failure handling; they cannot prove Tokyo Linux mounts, private tailnet access, native metrics persistence, encrypted recovery, off-host head custody, physical iPhone behavior or production rollback.

**AI-NATIVE GATE: INTERCEPT** — real upload stays disabled.
