# SQLite-first sync verification — 18 September 2026

**AI-NATIVE GATE: INTERCEPT for release.** The local implementation and synthetic checks below pass. Neither a signed iPhone nor the Tokyo deployment was accepted, and no real Health data was uploaded.

## Scope and result

The app retains its one SQLite file at `Application Support/BoazHealth/health.sqlite`. Each imported page and its HealthKit anchor commit together; an immutable local batch is sent only after separate upload consent and pairing. The Rust request and receipt format did not change. The existing database remains excluded from system backup; recovery of unsent records depends on Apple Health still making them readable.

The opening check uses SQLite `user_version=1`. An unversioned database with the supported tables and indexes is stamped in place without rebuilding its records, anchors or queued bytes. An unsupported version, partial layout or failed integrity check raises a visible error and is not reset. A file-protection error *after* `COMMIT` now reports that the records and anchor remain committed, keeps upload blocked, and allows a later protection recheck. On iOS Simulator, the protection attribute setter runs but the effective class cannot be read back; only signed-device read-back can close that release gate.

## Executed synthetic checks

| Check | Result |
|---|---|
| `scripts/test_local_core.sh` | **11/11 passed**. 10,000 synthetic records, 20 import pages, 50 batches; failed page/anchor rollback, revisions/deletions, stable retry bytes, receipt matching, schema compatibility/corruption, and post-commit protection fault. |
| `sh scripts/test_gateway_transport.sh` | **3/3 passed**. A batch created by the actual SQLite outbox traversed the production gateway method through an in-process transport: HTTP failure/retry kept the same ID and bytes; public origin and mismatched receipt were rejected. No DNS or remote request. |
| Unsigned generic iOS `build-for-testing` | **Passed**, including the physical-device conditional compilation. No signed install. |
| iPhone 16 Pro / iOS 26.5 simulator XCTest | **47 passed, 0 failed, 0 skipped** after correction; result bundle: `/private/tmp/boazapp-sqlite-final-simulator/Logs/Test/Test-boazapp-2026.09.18_16-36-38-+0800.xcresult`. This includes the separate-consent guard and gateway test, but not real HealthKit/device protection. |
| `cargo test --manifest-path Server/Cargo.toml --offline --locked` | **26 integration tests passed**; no Rust API change in this work. |
| `cargo clippy --manifest-path Server/Cargo.toml --offline --locked --all-targets -- -D warnings` | **Passed**. |
| `python3 scripts/test_server_http.py --build` | **7/7 isolated receiver checks passed** with upload gates off and temporary SQLite intact/empty. No Tokyo connection. |
| `git diff --check` | **Passed**. No commit or push. |

The first sandboxed iOS build failed because the SwiftUI macro/CoreSimulator services were inaccessible there; the unsigned build passed outside that sandbox. The first simulator suite ran but failed **20 database tests** because the simulator did not return an effective file-protection class after accepting the setter. The simulator-only read-back branch was corrected; the final fresh-device suite passed **47/47**. A later run on the pre-existing simulator stalled before XCTest launched and was interrupted; a newly created disposable simulator completed the final pass and was deleted afterward. The first isolated receiver run also failed because sandbox loopback binding was denied; the approved isolated rerun passed. These initial failures are not counted as passes.

## Remaining gates

Signed iPhone/Watch installation, real HealthKit pages and deletions, database/WAL/SHM protection and backup behavior, locked/reboot/background recovery, and the private Tokyo Rust receiver with encrypted storage, native metric projection, receipt read-back, restore and rollback all remain unverified. Keep real upload disabled until separately approved and evidenced.

Source snapshot at 2026-09-18 16:39 CST: branch `codex/boaz-health-ios`, base commit `480dae2`, dirty worktree. SHA-256 of key source files:

```text
6f325f3d73aa1d5f87b3df9ca7666d8d8f706fb8d3b1a673c9855353f060eb56  boazapp/Core/Database/BoazLocalDatabase.swift
19e346e70bdcb67a58ebe270a68a9ef9e1fba78bee4880ef4557aa5a20d3e2f8  boazapp/Core/Network/SyncEngine.swift
a5f9daa2ee1bc00b85dd9b5fa5c114c43224469d03b1d934eab4ca54a0297661  boazapp/Core/Network/TokyoCloudGateway.swift
eca2f16aa34b78bf82fae45da5b81d30ca3c03c96559f7a84e409114056c4bf5  boazapp/App/BoazCoordinator.swift
```
