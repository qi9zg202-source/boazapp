# P0-R2.1b synthetic source and regression evidence

**Disposition: INTERCEPT.** This directory records a local macOS source/test slice, not an independently operated Linux custodian, encrypted recovery domain, native VM rebuild, disaster activation, or production acceptance. No Tokyo connection, real health record, real upload, deployment, commit, or push was used.

## Identity and frozen source

- UTC: 2026-09-19 05:53:57 pre-change; 2026-09-19 06:11:08 post-change.
- Host/toolchain: Darwin 25.6.0 arm64; Rust/Cargo 1.96.0; Xcode 27.0 (27A266a); Python 3.14.6.
- Branch/HEAD: `codex/boaz-health-ios` / `480dae2dd2822cbd7facc4e840b52bb3629c8208`.
- [Pre-change manifest](source-manifest.md) and [post-change manifest](post-source-manifest.md) each cover 120 tracked and untracked non-ignored files, excluding this evidence directory. Tracked dirty-diff SHA-256: `9bdbed52ef86cad97decfae32b42fe993cd04a0d07e11cccd4d5d41215461860` before and `3866db8110fbb239d289e964d812601fdda35dc7cad4e62e06e396e84e6e40c3` after. The worktree was dirty before this slice and remains uncommitted.
- Physical source changed in this slice: `Server/src/{ack_journal,adoption,control,custody,recovery,main}.rs`, `Server/tests/{api,control_process}.rs`, `design.md`, `memory.md`, `Server/README.md`, `docs/TEST_CASES.md`. All other pre-existing edits were preserved.

## What the source now proves locally

| Proof obligation | Local disposition and limit |
|---|---|
| Governed control publication | The complete v2 custody tuple (control checkpoint, immutable baseline digest, confirmed acknowledgement-journal prefix, revision) is checked before a new control intent. Matching control head alone cannot authenticate changed acknowledgement or baseline state. Synthetic mismatch tests pass; a separate monotonic witness is not installed. |
| Genesis adoption and retry | Only an exact one-backup genesis snapshot with an empty confirmation journal can bind the baseline; durable `sqlite_sequence` history rejects a cleared-but-used ledger. An interrupted baseline reservation or completed CAS with lost local marker can resume by the same snapshot/operation ID and exact authenticated successor readback. It remains `adopted_unactivated`, not service activation. |
| Empty backup recovery watermark | After confirming zero receipts in the completed snapshot, backup manifest records `source_commit_sequence=0`, not null. A synthetic adoption-to-replay-plan test accepts that exact empty baseline and rejects null; it does not run VM or activate. |
| Revocation and erasure process death | Local child-process tests force `SIGKILL` after reserve, before CAS and after CAS. Retry keeps the original tombstone or closes; not every fsync instruction or independent host failure is covered. |
| SSH transport | A 30-second deadline covers child protocol I/O, including blocked stdin and partial stdout, in local child tests. Spawn and uninterruptible post-kill `wait` have no strict total-duration guarantee. Real SSH, pinned identity-file parent replacement and independent Linux execution remain unverified. |
| Isolated HTTP behavior | Synthetic route fixture binds the acknowledgement journal and uses a labeled route-only synthetic control event to satisfy the v2 baseline shape. This does not fabricate a physical adoption or make production `serve` start. The production unactivated-startup negative check remains intact. |

## Commands, results, and first-failure ledger

Commands were run from the repository root with synthetic fixtures. Exit `0` is a check result within that command's scope, never a production release verdict.

| Command/check | Result | Scope and caveat |
|---|---|---|
| `cargo test --manifest-path Server/Cargo.toml --offline --locked --quiet` | PASS, exit 0; 102 library + 11 binary + 8 journal + 57 API + 6 process = **184 tests**, 0 failed, 0 ignored | Final source after the adoption-to-replay test; doc tests 0. Isolated API loopback required the approved local sandbox escalation. |
| `cargo test --manifest-path Server/Cargo.toml --offline --locked -- --list \| rg ': test$' \| wc -l` | PASS, exit 0; 184 discovered tests | The executed Rust count above matches source-discovered tests. |
| `cargo clippy --manifest-path Server/Cargo.toml --offline --locked --all-targets -- -D warnings` | PASS, exit 0 | Final Rust source. |
| `cargo fmt --manifest-path Server/Cargo.toml -- --check` | PASS, exit 0 | Final Rust source. |
| `python3 scripts/test_server_http.py --build` | PASS, exit 0; 3 script checks and 1 filtered Rust loopback test | Production unactivated-startup refuses service with unchanged storage; synthetic routes pass; not a production serving proof. |
| `python3 scripts/generate_xcode_project.py --check` | PASS, exit 0 | Project file current. |
| `bash scripts/test_ios_simulator.sh` | Initial sandbox attempt FAIL, exit 1: CoreSimulator service inaccessible. Escalated rerun PASS, exit 0: 78 executed, 78 passed, 0 failed, 0 skipped, 0 SQLite runtime warnings | iPhone 17 Pro simulator UDID `E9C74289-F37B-4690-BD45-DF82D2BE7EC7`; result `/private/tmp/boaz-health-20260919T060622Z-29211.xcresult`. Synthetic 10k/100k app checks are **not** Linux recovery benchmarks. |
| `bash scripts/test_local_core.sh` | Initial sandbox attempt FAIL, exit 1: CoreSimulator service inaccessible. Escalated rerun PASS, exit 0: 2/2 executed, 0 skipped, 0 SQLite runtime warnings | Synthetic 10k/100k client-core checks; result `/private/tmp/boaz-health-20260919T061000Z-31803.xcresult`. Not an R2 custody-latency or Linux recovery measurement. |
| `bash scripts/test_gateway_transport.sh` | Initial sandbox attempt FAIL, exit 1: CoreSimulator service inaccessible. Escalated rerun PASS, exit 0: 1/1 executed, 0 skipped, 0 SQLite runtime warnings | Three synthetic gateway scenarios, no remote call; result `/private/tmp/boaz-health-20260919T061314Z-33532.xcresult`. |
| `xcodebuild -quiet -project boazapp.xcodeproj -scheme boazapp -destination 'generic/platform=iOS' -derivedDataPath /private/tmp/boazapp-p0r21b-dd CODE_SIGNING_ALLOWED=NO build-for-testing` | PASS, exit 0 | Unsigned generic iOS compile only; no signed device execution. |
| `python3 scripts/verify_doc_governance.py` | PASS, exit 0 | Historical 77-case register unchanged: 20 PASS, 42 BLOCKED, 15 NOT_RUN; source discovers 78 XCTest methods. |
| `git diff --check` | PASS, exit 0 | Tracked diff only; source-manifest verification separately covers tracked and untracked non-ignored files. |

Failure-first checks retained in the work record: the tuple mismatch, stalled-child, cleared-ledger and lost-local-adoption-marker tests each failed before their fixes and then passed; one new recovery fixture initially failed because macOS `/var` is a path alias, and passed after using a canonical temporary parent. An in-progress API compile failed while a helper was being added; an API fixture then returned 503 under the new full-tuple rule until its test-only journal/control binding was corrected. A sandboxed API loopback run passed 56/57 and failed only to bind (`EPERM`); the escalated final full suite passed 57/57. None of these initial failures is relabeled as an initial PASS.

## Not established / next gates

- **BLOCKED:** No approved independent Linux host, verified dm-crypt domains, externally administered SSH custodian, or independently monotonic rollback witness was evidenced in this run. Local JSON/child fixtures are not off-host custody.
- **BLOCKED:** End-to-end kill/restart at every reserve, DB, mirror, head, seal, CAS and filesystem sync boundary on that Linux host; SSH identity-file/parent replacement and strict invocation-wide timeout; full 10k/100k three-run p50/p95/p99 confirmation latency, peak resource cost and same-host baseline.
- **NOT_RUN:** Native isolated VictoriaMetrics rebuild and exact export, activation at the live port, controlled pointer cutover, second-generation restoration and production rollback. Production `serve` remains closed.
- **Separate release gates:** signed iPhone/Watch, real HealthKit, Tokyo private HTTPS, production encryption/key custody and actual disaster drill. Keep upload disabled and **AI-NATIVE GATE: INTERCEPT**.

Minimal Rust/SQLite/SSH custody remains preferable for this scope. A balanced checkpoint/index optimization is justified only if fixed-Linux latency breaches a frozen SLO; always-on dual VM, distributed databases, queues and AI orchestration add sensitive replicas and operating cost without solving the authority proof.
