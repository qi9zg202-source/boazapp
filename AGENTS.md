# Boaz Health — repository rules for Codex and other coding agents

This file is the repository-level engineering contract. `CLAUDE.md` is the Claude Code entry point; both agents follow this file, then [design.md](design.md), [memory.md](memory.md), and the relevant source/test files. The current worktree and running system evidence outrank every document. Do not treat a plan, a passing local test, or an HTTP response as release approval.

## Product and release boundary

- Product: a private, local-first iPhone app for selected Apple Health and Fitness records, plus a separately operated Tokyo receiver. It is not a medical device and does not diagnose or prescribe.
- Current release gate: **AI-NATIVE GATE: INTERCEPT**. The codebase exists, but physical iPhone/Apple Watch acceptance, private Tokyo deployment, encrypted storage, native VictoriaMetrics identity, restore, and rollback evidence are not established by this repository. See [README.md](README.md), [docs/TEST_CASES.md](docs/TEST_CASES.md), and the dated [Tokyo preflight](docs/TOKYO_PREFLIGHT.md). A dated preflight is not proof of today's host state.
- No real upload, production deployment, host migration, access-rule change, erasure of real data, or publication is authorized by an ordinary coding/documentation request. Show impact, verification, and rollback and obtain the user's explicit decision first.

## Read before changing anything

1. Run `git status --short --branch` and inspect diffs in overlapping files. Preserve other people's modified/untracked work. Stage only reviewed paths if a commit is separately requested; never broad-stage a dirty tree.
2. Read this file, `CLAUDE.md`, `design.md`, `memory.md`, and the nearest feature documentation. For code behavior, inspect current Swift/Rust source, schemas, and tests; documents may lag.
3. Define the user task, source of truth, failure cost, in-scope files, acceptance checks, and rollback before implementing a nontrivial change. Label verified facts, design targets, and unknowns separately.
4. Prefer the smallest sufficient design. When a proposed approach lacks data authority, consent, privacy, recoverability, or measurable benefit, state `INTERCEPT — 非世界级方案`, explain the consequence, and offer a verifiable alternative.

## Module ownership and invariants

| Area | Source of truth | Invariant |
|---|---|---|
| HealthKit read allowlist and units | `boazapp/Health/HealthTypeCatalog.swift`, `HealthKitManager.swift` | Read only; an empty query does not prove permission or complete history. |
| Local event/anchor/outbox ledger | `boazapp/Core/Database/Schema.sql`, `BoazLocalDatabase.swift` | Commit each event page with its next anchor; preserve revisions, deletions, immutable batch bytes and retry state. |
| Collection and upload coordination | `boazapp/Core/Network/SyncEngine.swift`, `TokyoCloudGateway.swift` | Health access, private pairing and cloud-upload consent are separate. Network failure leaves a recoverable queue. |
| UI and status | `boazapp/UI/`, `boazapp/App/BoazCoordinator.swift` | Display evidence level: local saved, cloud saved, metrics current and erasure progress are distinct. No unsupported medical inference. |
| Tokyo health authority | `Server/src/database.rs`, `Server/src/schema.sql`, `Server/src/lib.rs` | The health SQLite ledger is opened only through validated storage paths; authenticated idempotent ingest preserves revisions and receipts. A receipt proves only its named transaction/evidence state. |
| Tokyo control authority | `Server/src/control.rs` | The separate control SQLite ledger retains revocation, erasure and recovery facts without health payloads or plaintext credentials. A control intent is committed before the health ledger converges. |
| Metrics projection | `Server/src/projection.rs` | VictoriaMetrics is native, derived and replaceable; never the health-record authority. No Docker identity shortcut or direct phone-to-metrics write. |
| Operations | `Server/deploy/`, `Server/README.md`, `docs/TOKYO_PREFLIGHT.md` | Loopback receiver, private HTTPS, encrypted storage/backup, restore and rollback before upload is opened. |

Never log or commit real HealthKit exports, database files, pairing codes, bearer tokens, erasure secrets, signing material, or private host credentials. Use synthetic fixtures for automated tests. Do not send health data to an LLM or external service without a separate, explicit data-authorization and cost review.

## AI-native decision gate

The current data path is deterministic: HealthKit → local SQLite → consented private HTTPS → Tokyo SQLite → derived metrics. Use direct code and database queries for this path. Meilisearch, Qdrant, Neo4j/GraphRAG, LangGraph, and an LLM are **not adopted** for ingestion, permissions, identity, receipts, erasure, or clinical interpretation. A future proposal must name its measurable benefit, authoritative source, lineage, privacy/cost budget, failure mode, and acceptance test before adding any layer. Model output is never a record, permission, or execution authorization.

## Change and verification loop

- When adding/removing Swift or XCTest files, run `python3 scripts/generate_xcode_project.py` and review the project-file diff. Keep Swift 6 concurrency and actor isolation explicit.
- Use `python3 scripts/generate_xcode_project.py --check` in verification so a check cannot rewrite the project. Run the governed simulator script for executable XCTest evidence; a source-method count is not an execution count.
- For ledger, protocol, projection, or erasure changes, run the representative synthetic tests and inspect both success and failed/retried paths. Keep migrations and protocol compatibility explicit.
- Baseline local checks from the repository root (record exactly what ran and the result):

  ```sh
  python3 scripts/generate_xcode_project.py --check
  bash scripts/test_ios_simulator.sh
  bash scripts/test_local_core.sh
  bash scripts/test_gateway_transport.sh
  cargo test --manifest-path Server/Cargo.toml --offline --locked
  cargo clippy --manifest-path Server/Cargo.toml --offline --locked --all-targets -- -D warnings
  xcodebuild -project boazapp.xcodeproj -scheme boazapp -destination 'generic/platform=iOS' -derivedDataPath /private/tmp/boazapp-dd CODE_SIGNING_ALLOWED=NO build-for-testing
  git diff --check
  ```

- An unsigned build compiles but does not execute iOS tests or prove HealthKit, Watch-origin samples, Keychain, locked-phone behavior, accessibility, or visual quality. Validate those on a signed physical iPhone when in scope. A macOS server test does not prove Linux deployment, encrypted mounts, private access, native VictoriaMetrics, or restore.
- For visual changes, inspect the rendered app on a real device when available; otherwise mark the visual/device check unverified. For documentation-only changes, check links, HTML rendering, cross-document consistency, and `git diff --check`.
- Report what changed, what was actually checked, failures/unknowns, and `AI-NATIVE GATE: PASS` or `AI-NATIVE GATE: INTERCEPT` for the scope being assessed. Do not use a local/documentation PASS as a production PASS.
- End each user-facing completion with the required “第二大脑 6 服务调用跟踪表”: Live LSP Compiler, Meilisearch, Qdrant, Neo4j/GraphRAG, MCP Evidence Gateway v2.5, and Git Worktree, each marked `[CALLED]` or `[NOT_NEEDED]`. Never imply a service was called merely because it is named here.

## Documentation maintenance

- `design.md`: current architecture, object/state contracts, choices and gates; update when interfaces, schemas, trust boundaries, or data flow change.
- `docs/prd.html`: the sole project-level product decision and acceptance contract; update only when a product requirement or evidence boundary changes. Keep its link in `README.md`. Root `prd.html` is only a compatibility entry and must not duplicate the PRD.
- `memory.md`: concise project decisions and unresolved questions, each dated and linked to a current source. It is **not** a personal memory store or a copy of health data.
- `README.md` and `Server/README.md`: user/developer and operator instructions. Do not add a broken evidence link or claim an absent report exists.
- A requested commit or push is a separate, scoped step. Never infer it from editing documentation; preserve this branch and all unrelated dirty work.
