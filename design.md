# Boaz Health — current architecture and evidence boundary

Status on 2026-09-19: **P0-R2.1 trusted-adoption and custody source work is in progress in this worktree; neither an end-to-end restore nor production release is accepted. `AI-NATIVE GATE: INTERCEPT`.** This document distinguishes callable library primitives and synthetic tests from an operational cutover. It is not evidence of a signed iPhone run, Tokyo deployment, encrypted recovery domain, independently operated off-host custodian, native VictoriaMetrics persistence, restore or rollback.

Authority order: current source and schemas → executed current-worktree evidence → this design → dated historical evidence → plans. [AGENTS.md](AGENTS.md) is the repository contract, [CLAUDE.md](CLAUDE.md) is a thin Claude Code entry, [docs/prd.html](docs/prd.html) is the sole PRD, and [memory.md](memory.md) records dated decisions and open gates.

## 1. Product boundary

| Question | Decision |
|---|---|
| User and job | One iPhone owner reads selected Apple Health/Fitness records, keeps a reliable local copy, and may explicitly send queued batches to a private Tokyo receiver. |
| Record authorities | Apple Health is the original readable source; phone `health.sqlite` is the app's local authority; Tokyo health SQLite is the cloud health-record authority; Tokyo control SQLite is revocation/erasure/recovery authority. |
| Derived store | Native VictoriaMetrics is a disposable numeric projection. It is never a health-record, credential, erasure or restore authority. |
| High-impact actions | Pairing, upload consent, cloud erasure and Tokyo cutover are separate human-authorized actions. Pairing never enables upload. |
| Non-goals | Diagnosis, prescription, HealthKit writes, public ingest, autonomous deployment, a second phone database, PostgreSQL, queues/microservices, AI/search/vector/graph components, or reuse of the old Mac replacement database. |
| Failure cost | Lost/replayed data, advanced anchors after failed pages, false cloud/metric completion, revived credentials after restore, privacy exposure or an unprepared production cutover. |

## 2. End-to-end data and trust flow

```text
Apple Health readable subset
  │ read only; a successful request or empty result is not permission/completeness proof
  ▼
iPhone health.sqlite (existing path; GRDB, WAL/FULL)
  events + revisions/deletions + query anchors + immutable upload batches + audit
  │ only after separate upload consent and pairing
  ▼
Private .ts.net HTTPS
  │ endpoint validation + bearer token; Tailscale Serve target 127.0.0.1:8787
  ▼
Rust receiver on Tokyo loopback
  ├── health SQLite BZHR v2: events, receipts, projection outbox/state
  └── control SQLite BZHC v2: tombstones, erasure/recovery facts, hash chain
        │ separate encrypted recovery domain required for production
        ▼
  Native VictoriaMetrics on 127.0.0.1:8428
  derived boaz_health_v1_* generation; verified readback before metrics_current
```

There is no direct phone-to-VictoriaMetrics path and no cross-SQLite atomicity claim. A network failure leaves a durable phone queue. A projection failure leaves a durable Tokyo job and a `cloud_saved` receipt.

## 3. Core objects and state contracts

| Object / key | Authority | Contract |
|---|---|---|
| `HealthEvent` / `event_id`, `revision` | Phone and Tokyo health ledgers | `upsert` or `delete`; monotone revision; source/time/unit/metadata remain evidence, not medical interpretation. |
| `QueryAnchor` / health type | Phone `query_anchors` | The next anchor commits in the same transaction as its page. A failed page never advances it. |
| `UploadBatch` / `batch_id`, exact bytes/hash | Phone outbox | At most 200 revisions and 128 KiB. Retries preserve ID and bytes; a receipt advances only its bound batch. |
| `DeviceIdentity` / `device_id` | Phone identity plus Tokyo control/health stores | Pairing uses a ten-minute one-use 64-hex secret. Token is stored device-only. A completed erasure rotates to one new device ID; the new identity must re-pair and re-consent. |
| `Receipt` / `batch_id`, `commit_sequence` | Tokyo health ledger | `cloud_saved` proves a health SQLite commit. `metrics_current` additionally binds the current projection generation/mapping version and a successful readback time. |
| `ControlEvent` / stable event ID and hash | Tokyo control ledger | Append-only revocation, erasure and recovery facts. Stores hashes, IDs and times, never health payloads or plaintext token/secret. |
| `ProjectionState` / generation + mapping version | Tokyo health ledger and VM marker | Generation/mapping drift invalidates old metric-current evidence and queues deterministic rebuild. |

## 4. iPhone architecture

The phone keeps the existing `health.sqlite`; P0 does not create another phone database.

- SwiftUI owns presentation; `BoazCoordinator` owns user-visible coordination; actors isolate the database, collection and gateway.
- GRDB.swift 7.11.1 `DatabasePool` uses WAL, foreign keys, a bounded busy timeout and a verified `synchronous=FULL` writer. `Package.resolved` is tracked.
- A read-only schema preflight recognizes compatible local versions before opening the writer. Unsupported, corrupt or partial databases fail closed and are never reset automatically.
- Each HealthKit page, its revisions/deletions and next anchor commit together. Network awaits never occur inside that transaction.
- File-protection verification is post-commit evidence. If it fails after SQLite commits, the records and anchor remain saved; the app reports that exact state and blocks upload until protection is verified. It does not claim rollback.
- Private endpoint editing and the network gateway use one validator. HTTPS, a private `.ts.net` host, clean origin form and no redirect are required before credentials or health bodies can be sent.
- Erasure responses are a strict state machine. Pending states retain recovery material. Contradictory status/time combinations are rejected. Only a legal `complete` for the stored erasure ID clears old credentials and rotates `device_id`, idempotently.
- UI wording is receipt-scoped: “cloud saved”, “metrics current”, erasure completion and backup status mean the named receiver returned evidence at a named time. They do not prove all global copies or future retention.

## 5. Tokyo storage authority and safe opening

### 5.1 Validated paths

All server commands and HTTP paths open SQLite only through validated storage paths. The health database must be a regular direct child of `BOAZ_HEALTH_DATA_ROOT`. Validation rejects `..`, symlinked files or parents, hard links, non-regular or zero-byte files and a dev/inode alias to the old `/opt/boaz/data` database before creating a directory, DB, WAL, SHM or lock.

The source-level environment contract is:

- `BOAZ_HEALTH_DATA_ROOT`
- `BOAZ_HEALTH_DB`
- `BOAZ_HEALTH_CONTROL_DB`
- `BOAZ_HEALTH_CONTROL_MIRROR_DIR`
- `BOAZ_HEALTH_CONTROL_VOLUME_ENCRYPTED`

`serve` does not initialize unknown storage. The explicit lifecycle commands are `init-storage`, `migrate-storage` and `verify-storage`. The CLI also exposes `backup-control`, `restore-control` and `restore-health --staging-path`; their staged, fail-closed behavior is described in §6. They do not finish projection rebuild or live cutover.

### 5.2 Health SQLite BZHR

- `application_id=0x425A4852` (`BZHR`).
- v0 is accepted only when it exactly matches the reviewed legacy seven-table/three-index layout.
- v1 claims the legacy layout; v2 adds control-store binding and projection generation/mapping evidence.
- Classification occurs read-only. Unknown IDs, future versions, partial schemas, zero-byte files and corrupt databases fail without mutation.
- A legal v0→v1→v2 transition is one immediate transaction. Migration classifies the legacy file read-only first, rejects an unrelated nonempty control store, and resumes a partially seeded control chain only when the existing events match an exact legacy prefix. Every open verifies application ID, user version, exact schema, `quick_check` and `foreign_key_check`.

### 5.3 Control SQLite BZHC

- `application_id=0x425A4843` (`BZHC`), current schema v3, WAL, FULL, foreign keys and secure delete. Classified v1/v2 layouts migrate explicitly by renaming their existing event table to an immutable historical prefix and appending future facts to a separate suffix; no historical event table is dropped or copied. Historical hashes retain their original version.
- The append-only event chain records stable event/type IDs, device/credential hashes, erasure identifiers, times, previous/current hashes and recovery metadata. It excludes health payloads and plaintext credentials.
- The database, `control.head.json` and create-new mirror records form one fail-closed evidence set. A publication lock serializes control commits. Startup, append and worker reconciliation verify the full chain; per-request authentication verifies the database/head/latest mirror tail and uses indexed tombstone reads. Tampering with an older mirror is not guaranteed to be caught by that next auth request; a healthy idle worker reaches full reconciliation on its 60-second tick, or sooner on a wake. Worker failure can delay this, so there is no unconditional detection-time guarantee. Once detected, divergence fails closed until authorized recovery.
- The health database binds one control store ID. Missing or mismatched control authority blocks startup.
- An off-host expected head and a physically separate encrypted recovery domain are production gates, not local-test claims.

## 6. Revocation, erasure and recovery ordering

SQLite cannot atomically commit across the health and control databases, so the system uses durable intent and convergence:

An operation lock now serializes ingest, revocation, erasure and backup/prune critical sections so an in-flight batch cannot cross an erasure boundary unnoticed. This is a local concurrency invariant, not a substitute for process-crash and Tokyo recovery evidence.

1. Revocation writes a control token tombstone first. Authentication checks the control store before accepting the health token row.
2. Erasure writes an intent and tombstone first, immediately rejecting the old token.
3. Health rows, receipts/jobs and compatible legacy rows converge transactionally in the health database.
4. Metric deletion and managed-backup expiry append later verified control facts.
5. Startup and runtime reconciliation repeat incomplete steps idempotently. Control intent is never rolled back to make an old credential valid.
6. A legal receiver `complete` causes one client identity rotation. Local Health data remains; any later upload requires a new pairing and new consent.

The source has a bounded staging-recovery core. `backup-control` creates a verified bundle; `restore-control` compares a caller-supplied expected-head file and stages the control copy (that file alone does not prove independent custody); `restore-health --staging-path` verifies snapshot hash/schema/control binding, stages health data and replays all applicable tombstones and erasure intents, including previously verified erasures. The restore commands do not open or require successful verification of the live health/control databases: staging can proceed after live database corruption, while live-directory path validation, upload off, an exclusive lifecycle lock and separate dm-crypt recovery storage remain mandatory. This does not repair, replace or authorize serving the corrupt live store. `restore-health` still stops at `projection_rebuild_required` with a nonzero exit and does not replace the running ledger.

P0-R added separate, fail-closed **library primitives** for a persistent coordinator lock, per-epoch phase journal and active-set descriptor; an isolated staging VM target/launcher; a deterministic oracle from surviving health events; complete Boaz-series export comparison; and evidence-bound completion. P0-R2 now requires the stable coordinator before path resolution for every ordinary command: only `bootstrap-coordinator` can create it under an explicit offline bootstrap gate, and a missing directory cannot fall back to fixed legacy paths. Operational commands still reject even a locally `ActiveVerified` journal and pointer because off-host custody and restarted live-VM export are not wired into startup. This is an intentional stop-serving state, not a usable generation. The CLI does not expose verified rebuild, activation or resume commands. `restore-health` remains nonzero at `projection_rebuild_required`. A code-level test of these primitives is not an isolated Linux drill.

P0-R2 also introduces a separately initialized encrypted acknowledgement journal: the route prepares exact batch bytes before SQLite commit and confirms their receipt after commit before returning it; pairing identity recovery uses a matching prepare/confirm record without plaintext token. Missing journal configuration makes pairing and batch receipt paths fail closed. `restore-health` now requires that journal and an exact adopted-baseline match before staging/replaying confirmed batches; it still exits nonzero before activation. The restore library cannot infer pre-journal or missing post-snapshot data from `sqlite_sequence`, and later-generation snapshots still need a new independently verified anchor. A candidate with any unproven legitimate write is rejected. None of this currently authorizes serving or activation. The SSH custody client and forced-command server are wired to governed control and acknowledgement paths in source, but no installed independent host, successful off-host advancement or accepted serving startup is evidenced. A local expected-head file remains a synthetic fixture, not off-host custody.

Managed-backup pruning compares physical database files, manifests, SHA-256 values and active control events before expiry confirmation. P0-R adds a snapshot/hash/name-bound `backup_delete_intent` before removing a file and an idempotent reconciliation path for a matching interrupted deletion. Divergent or ambiguous inventory fails closed. Local fault tests are not process-kill, scheduler/alert or native-metrics expiry evidence; those remain open.

### 6.1 P0-R authority and activation boundary

P0-R2.1 source decision (2026-09-19): one independently administered custody revision now binds three authorities: the control checkpoint, the complete acknowledged-write journal head, and a one-time generation-zero baseline digest. Each governed control event persists one fixed intent and follows reserve → local DB/mirror/head → independent CAS → outward success. Batch and pairing confirmations similarly require independent CAS; a local committed receipt alone is not externally confirmed. The coordinator has a stable custody-operation lock shared by those publishers. An interrupted exact intent can be resumed only from its original evidence; backup/restore events whose pre-commit artifacts cannot be revalidated remain closed for manual review. No cross-store transaction is claimed.

P0-R2.1a source decision (2026-09-19): managed-backup control intents now carry a file-and-manifest proof, including the exact bytes' hashes, direct-child name and Unix file identities within the validated managed root. The proof is checked before local replay and again before off-host CAS; a missing, changed, linked or replaced artifact leaves the original reservation unresolved. Backup creation, deletion intent and deletion have distinct present/absent evidence. Backup-expiry confirmation additionally binds the complete managed inventory and erasure request time, revalidating both before CAS. Legacy pending backup intents without this proof fail closed. Interrupted health/metrics erasure-verification facts without a replayable external proof remain closed for operator review; operational restore completion remains blocked pending P0-R2.2's VM and activation evidence. The local synthetic process test kills the publishing process after reserve and around CAS; it is not an independently operated Linux recovery drill.

The v2 custodian now chains the *entire* committed reservation plus successor into a required history seal, publishing mirror → seal → public head. A one-file rewrite of an older operation no longer passes full-history verification. A mirror published before its seal can only be completed by the original exact CAS; a sealed mirror can repair a missing public head. Existing unsealed v2 history is not silently adopted. The seal and complete scan cannot prove absence of an entire-domain consistent rollback: separate ownership, external monotonic anchoring and Linux crash/SSH/dm-crypt evidence remain release gates. Every forced request still scans the full history; 10k/100k latency and capacity claims remain unmeasured.

The offline adopt-active-set command is deliberately narrower than full recovery: it accepts a fresh genesis store, takes a managed SQLite snapshot under the write fence, compares source/snapshot receipt inventory, confirms the backup-created control event and baseline digest off host, and writes only adopted-unactivated.json. It does not publish an active pointer or allow serve. Historical stores with pre-journal acknowledged data cannot be retroactively declared zero-loss. Production still lacks verified independent Linux/dm-crypt custody, native VM rebuild/live-port readback, activation and second-generation restore; therefore the release gate remains INTERCEPT. The v2 forced-command custodian checks its complete history for each request, so cumulative per-batch cost may grow superlinearly; 10k/100k fixed-Linux measurements are required before an efficiency claim.

The backup CLI now converges any already-confirmed erasure intent before taking a health snapshot. Prune pins its directory and file identities for deletion and refuses to confirm a substituted artifact. The forced-command custodian accepts only a temporary file whose target, bytes and reservation precisely match the retry; unknown or legacy anonymous temporary files remain closed for manual review. Local process tests exercise equivalent persisted crash states, not every live instruction boundary. Same-UID malicious concurrency and independently anchored custody remain unproven operational boundaries.

P0-R2.1b historical checkpoint (2026-09-19): a governed control publication checks the **entire** custody v2 tuple against the locally verified control checkpoint, acknowledgement-journal confirmed prefix, immutable baseline digest and derived custody revision. Matching control heads with a changed baseline, journal head or revision fail closed before a new control intent. At this checkpoint, the SSH client applied a 30-second deadline **inside its nonblocking protocol I/O loop** only; P0-R2.1c changes that boundary below. Offline adoption can re-enter the same generation-zero baseline reservation after an interrupted CAS only by proving the exact existing snapshot, one genesis backup event, empty acknowledgement journal and authenticated exact successor readback. Both source and snapshot reject even deleted prior work recorded in `sqlite_sequence`, so an emptied ledger is not mislabeled RPO=0 genesis. Local process-kill tests cover revoke and erasure intents after reserve, before CAS and after CAS, checking that the tombstone survives retry. Independent SSH witness, dm-crypt/Linux crash boundaries, 10k/100k cost, serving activation and second-generation restore remain unverified.

### 6.2 P0-R2.1c custody and confirmation acceptance boundary

Source inspection, not an independent-host result, identified four distinct claims. P0-R2.1c now invokes a fixed `/usr/bin/ssh` rather than selecting from `PATH`, clears inherited environment and disables the SSH agent, and checks that the executable, key and known-host files and all ancestors are root-controlled, non-symlink and not group/world writable, before each request. The receiver UID must have read-only access to the identity material, not ownership or replacement ability. This removes a receiver-owned path from the accepted configuration but does not prove resistance to privileged replacement or attest the actual objects used by a real Linux SSH process. The 30-second deadline now starts before spawn, uses a child process group and bounded caller-side reap attempts; an uninterruptible kernel operation still defeats an absolute wall-clock guarantee, and a complete HTTP confirmation has no measured bound. The v2 custodian reads and sorts its complete history for each forced request; per-batch `read/reserve/CAS` can therefore become increasingly expensive. Finally, a self-consistent rollback of the custodian's history, seal and head is not detected by a same-domain hash chain alone. None of these statements is proof that an attack or outage has occurred.

The smallest sufficient acceptance design keeps the existing Rust/SQLite journals and forced-command protocol, but requires independently protected SSH identity material, explicit supervision and unknown-outcome retry rules, and a separately owned monotonic custody witness. An incremental checkpoint is considered only after correct fixed-host measurements show the full scan exceeds a *pre-frozen* latency/cost target; it must retain complete-chain verification. Constantly running duplicate receivers or VM instances, distributed databases, queues and AI orchestration add sensitive state without resolving object identity or coordinated rollback and are not adopted for this slice. The control chain, acknowledgement journal and health SQLite remain distinct authorities; no cross-file, cross-host transaction is claimed.

Before any independent test, the owner must authorize isolated Linux receiver and custodian hosts, independent encrypted domains, fixed SSH identities, a monotonic witness owner and p95/p99/throughput/disk/network budgets. The test matrix must distinguish SSH I/O time, one custody operation and an HTTP batch's end-to-end time. Timeout or lost CAS response is *unknown*, never success: only the original operation ID, payload digest and exact successor readback can complete it; otherwise writers remain closed. A local fail-first test exposed premature intent removal when CAS falsely reported success; the acknowledgement path now performs a separate exact remote read before clearing the durable pending intent. Process-kill cases span receiver and custodian reserve, local SQLite, history/seal/head durability, CAS and response boundaries. A same-domain total rollback, changed file/parent identities, 2xx-without-matching-state, and reconnect after partition must fail closed. These Linux/witness tests and three full 10k/100k-batch trials against a correct same-host baseline are `BLOCKED` until their owners and environment are approved; Mac synthetic results cannot satisfy them. Production `serve`, activation and real upload remain closed.

### 6.3 P0-R2.1d false-success counterexamples and independent witness decision

The current synthetic counterexample rolls the custodian history, seal and public head back **together** and shows that the internally consistent older state still passes the same-domain v2 verifier. The verifier is correct about chain consistency but cannot establish freshness without a separately owned monotonic witness. The minimal proposed witness would attest the complete v2 custody-state digest, store ID and strictly increasing revision (not health payload), bind the original operation ID and intent digest to each reserve/CAS, and reject any local state below or divergent from its durable revision. A one-sided post-CAS crash must preserve the original intent and stop new authority writes until exact same-ID forward reconciliation; neither side may infer that the other committed. Two-of-two custody plus witness increases confirmation latency and outage sensitivity, so ownership, authentication, encryption, rotation, audit, failover and latency budgets must be approved and measured before implementation. A quorum or extra database is not justified without a measured availability requirement. A same-host JSON copy is not a witness.

Two local false-success tests exposed narrower source bugs. A control CAS mock could return the intended successor without storing it; the publisher previously removed its durable intent. It now requires a separate exact custody read before removal. A forged health erasure row could report `complete` without matching control intent or verified metrics/backup facts. HTTP erasure reads now require settled custody and an exact match of the control intent and verification timestamps; a health row alone is never completion authority. These checks do not prove the underlying VM deletion or backup expiry on Linux. No HTTP schema changed, and production `serve` remains closed. The full independent-witness, encrypted-domain, SSH identity and fixed-host performance acceptance is still `BLOCKED`.

A further fail-first race test showed that the projection worker could reconcile an erasure's health rows while a backup held the health-operation freeze lock. The worker now takes that same lock before opening or changing the health/control ledgers or VM projection, and retries when busy. This makes the local freeze enforceable for that path, but serializes a whole projection pass with ingest/backup/erasure; the p95/p99 latency and backup wait cost remain unmeasured on Linux.

| Stage | Required persistent proof | Current scope |
|---|---|---|
| Frozen | Receiver/upload writes stopped; one stable lock; independently held expected control head and inventory checked twice | Coordinator is mandatory before path resolution; explicit bootstrap exists. All ordinary commands still stop because startup cannot verify external custody and live VM. |
| Replayed candidate | Snapshot hash/schema and newer complete control chain; tombstones replayed; confirmed post-snapshot legitimate writes accounted for | Staging replay and acknowledgement-journal coverage/replay library code exist; no adopted baseline, complete historical source or operational activation path is proven. |
| Metrics verified | A separate native VM process, encrypted candidate storage, deterministic oracle and exact full export after restart | Typed staging target, Linux identity gate, oracle and readback functions exist; no current Linux execution evidence. |
| Control completed / custody advanced | Same epoch/evidence digest appended once; DB/head/mirror and independent expected head agree | Control v3 preserves the immutable historical prefix; source paths connect guarded control commits to the custody protocol, but no real independent host or restore-completion custody receipt is accepted. |
| Activated / active verified | One atomically published generation descriptor, correct health/control/VM identities, restarted live VM full readback before service | Activation descriptor/journal primitives exist; no accepted CLI cutover or post-activation drill. |

There is no transaction spanning the health DB, control DB, file pointer and VM. A half-written phase, changed storage identity, missing external head or unproven valid record is a **stop-serving condition**, not a reason to select an older directory. An older candidate containing erased data or an older control head is forbidden as a rollback target. The old directory may be retained for encrypted forensic inspection only.

## 7. Projection generation and worker supervision

- Projection jobs are queried before expensive native-runtime attestation.
- One HTTP client is reused for the worker lifecycle.
- A VM storage generation marker and mapping version bind projection evidence. Marker/version changes demote old `metrics_current` receipts to `cloud_saved` and enqueue required rebuilds.
- External VM write/readback happens before the local completion transaction. After verified readback, outbox completion and receipt generation/version/time update together in one immediate SQLite transaction. A crash before that transaction leaves a retryable job; deterministic external writes make the retry safe.
- Ingest, erasure and generation changes wake the worker. Startup drains immediately; idle reconciliation uses a 60-second tick. Failures use bounded exponential backoff with jitter and invalidate cached runtime identity on any identity/readback failure.
- HTTP serving and the worker are supervised together. A worker panic or unexpected exit terminates the process so systemd can restart it; ingest must not continue silently without projection supervision.

`metrics_current` means only: this batch was projected and read back at `projected_at` on the current generation/mapping version. It does not promise indefinite retention. Current projection labels remain narrowly bounded; label hashing is unimplemented, so arbitrary Health metadata must not be promoted to labels.

## 8. Encryption, private access and deployment boundary

- Receiver default: loopback `127.0.0.1:8787`; private HTTPS is a Tailscale Serve target and Funnel stays off.
- Native VictoriaMetrics must own loopback `127.0.0.1:8428` with the configured executable and storage identity. Docker proxy or HTTP 2xx cannot pass.
- The implemented Linux mount verifier recognizes dm-crypt device-mapper identities. `provider-managed encryption` is not currently verifiable by this code and remains unaccepted until a deterministic verifier and recovery evidence exist.
- Production `backup` and `prune-backups` require the backup-volume flag, a verified dm-crypt mount and a distinct device from both live health and control databases before mutation; a flag alone fails. A test-only synthetic seam does not appear in the production CLI. Physical Tokyo mount and recovery-domain evidence remain open.
- Health, VM, managed backup and control recovery domains need recorded encryption, ownership, key custody and boot/recovery behavior. A configuration flag does not prove any of these.
- The dated [Tokyo preflight](docs/TOKYO_PREFLIGHT.md) is an observation from 2026-09-18, not proof of today's host.

## 9. AI-native decision

| Layer | Decision | Reason |
|---|---|---|
| Deterministic code + SQLite | Adopt | Stable identity, transactions, consent, receipts, tombstones and recovery are directly testable. |
| Meilisearch | Do not adopt | No document-retrieval task in ingestion or recovery. |
| Qdrant | Do not adopt | No semantic-retrieval benefit; unnecessary sensitive-data and model risk. |
| Neo4j / GraphRAG | Do not adopt | Relationships and lineage fit the two governed SQLite ledgers and hash chain. |
| LangGraph | Do not adopt | Native state machines and reconciliation are smaller, deterministic and recoverable. |
| LLM / paid model | Do not adopt | Model output is not a record, permission, medical authority or execution authorization. No health data is sent to an LLM. |

## 10. Verification and release gates

Run from the repository root and preserve exact output, exit code, environment, test count, skips, warnings, source hash and dirty-diff hash:

```sh
python3 scripts/generate_xcode_project.py --check
bash scripts/test_ios_simulator.sh
bash scripts/test_local_core.sh
bash scripts/test_gateway_transport.sh
xcodebuild -project boazapp.xcodeproj -scheme boazapp \
  -destination 'generic/platform=iOS' \
  -derivedDataPath /private/tmp/boazapp-dd \
  CODE_SIGNING_ALLOWED=NO build-for-testing
cargo test --manifest-path Server/Cargo.toml --offline --locked
cargo clippy --manifest-path Server/Cargo.toml --offline --locked --all-targets -- -D warnings
python3 scripts/test_server_http.py --build
python3 scripts/verify_doc_governance.py
git diff --check
```

The simulator runner must execute Debug, one architecture, no parallel tests, isolated DerivedData/xcresult, and fail on skipped tests or SQLite API/vnode warnings. Source-discovered and xcresult-executed test counts must match. The [77-case register](docs/TEST_CASES.md#current-worktree-77-case-status-register) remains `NOT_RUN`, `BLOCKED`, `FAIL` or `PASS` per exact evidence; suite success does not automatically pass device/host cases.

Still-open production gates:

- signed iPhone/Apple Watch HealthKit, locked/rebooted storage, Keychain, accessibility and visual acceptance;
- private Tokyo HTTPS from allowed/denied peers;
- independent encrypted control recovery domain and off-host expected head;
- native VictoriaMetrics identity, generation readback and retention behavior;
- isolated native staging projection rebuild/readback, completed restore epoch, second-generation restore, managed-backup operational evidence and rollback;
- representative 10k/100k performance/cost measurements against a frozen baseline.

**AI-NATIVE GATE: INTERCEPT** — P0 local source and synthetic evidence cannot authorize production upload.
