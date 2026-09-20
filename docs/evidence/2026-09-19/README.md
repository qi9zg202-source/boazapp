# Acceptance evidence — 19 September 2026

This package records the P0 implementation-cycle evidence for the combined dirty worktree on branch `codex/boaz-health-ios`, base commit `480dae2dd2822cbd7facc4e840b52bb3629c8208`. It does not replace or modify the historical [2026-09-18 evidence](../2026-09-18/README.md), and it is not production approval.

## Decision

**AI-NATIVE GATE: INTERCEPT**

Local synthetic verification is strong, but release acceptance is still blocked by signed iPhone/Apple Watch evidence, the private Tokyo Linux deployment, encrypted and separated recovery domains, native VictoriaMetrics identity/readback, and a complete disaster-restore implementation. The earlier audited defects in `ERA-04`, `BAK-03`, and `BAK-05` have source fixes and targeted local tests; the full acceptance cases are now `BLOCKED`, not `PASS`, because their host, crash-recovery and metrics observations remain open.

The complete [77-case register](case-results.csv) contains:

| Result | Count | Meaning |
|---|---:|---|
| PASS | 20 | Every expected observation for the bounded synthetic/local case was evidenced. |
| FAIL | 0 | No presently demonstrated failure remains in this refreshed case register. |
| BLOCKED | 42 | The full case requires unavailable device, Linux/Tokyo, native metrics, encryption, or restore capability. |
| NOT_RUN | 15 | The complete case was not executed or its evidence was not captured, even when a related test passed. |
| **Total** | **77** | One disposition for every case in `docs/TEST_CASES.md`. |

## Executed evidence

| Check | Recorded result | Evidence boundary |
|---|---|---|
| Earlier full iOS simulator suite | 77 executed, 77 passed, 0 failed, 0 skipped, 0 runtime warnings; the migration fault-injection test executed. | `/private/tmp/boaz-health-20260919T011250Z-45186.xcresult` and `.log`; this run preceded the added 78th XCTest and server safety edits. Temporary host paths, not signed-device evidence. |
| First 78-test simulator attempt | **FAILED:** 78 executed, 76 passed, 2 failed, 0 skipped. `LocalHarnessMigrationTests.testGatewayScenarios` and `TokyoEndpointTests.testSyntheticUploadUsesPrivateEndpointExactBytesAndStableRetryIdentity` hit unsigned-test Keychain `unavailable(-34018)`. The new fail-closed erasure test passed. | `/private/tmp/boaz-health-20260919T015202Z-82287.xcresult` and `.log`. This remains a recorded failure, resolved only by the later full rerun. A runner `PIPESTATUS` anomaly was also reported during concurrent edits; it does not turn the failed XCTest result into a pass. |
| Final full iOS simulator suite | **PASS:** 78 executed, 78 passed, 0 failed, 0 skipped, 0 runtime warnings; XCTest duration 121.824 s. | `/private/tmp/boaz-health-20260919T020024Z-87586.xcresult` and `.log`. Xcode 27.0 (`27A266a`), Darwin arm64. The 98-file pre-run SHA-256 manifest was rechecked after the run with no drift. This still does not establish signed-device or Tokyo acceptance. |
| Earlier production iOS request-path transport | One XCTest wrapper ran 3/3 scenarios; 0 failed, 0 skipped, 0 runtime warnings. | `/private/tmp/boaz-health-20260919T012247Z-53266.xcresult` and `.log`; before the final client guard edit, in-process synthetic transport only. |
| Final standalone local core | Two XCTest methods passed, each wrapping 11/11 synthetic scenarios at 10,000 and 100,000 records; 0 failed, 0 skipped, 0 runtime warnings. | `bash scripts/test_local_core.sh`; `/private/tmp/boaz-health-20260919T020402Z-88580.xcresult` and `.log`; no remote calls or phone performance proof. |
| Final standalone request-path transport | One XCTest method passed, wrapping 3/3 synthetic scenarios; 0 failed, 0 skipped, 0 runtime warnings. | `bash scripts/test_gateway_transport.sh`; `/private/tmp/boaz-health-20260919T020725Z-89818.xcresult` and `.log`; in-process transport, no DNS, TLS, tailnet or Tokyo host. |
| Rust receiver tests | Final `cargo test --manifest-path Server/Cargo.toml --offline --locked`: 32 library tests plus 37 API tests, 69 passed, 0 failed, 0 ignored. Adversarial recovery tests include symlink/hard-link artifacts, post-replay replacement and a WAL-only mutation; the CLI test verifies fail-closed gates. | Run on the current source after the server safety edits; local isolated SQLite and HTTP fixtures only, with no production Linux or Tokyo identity proof. |
| Rust static/release checks | Final Clippy with `-D warnings`, `cargo fmt --check`, and locked offline release build passed. | Re-run after the latest server recovery edits on the local toolchain; not the installed production binary. |
| Isolated receiver process | Final 8/8 HTTP checks passed with upload closed. | A sandbox-local bind attempt failed with `Operation not permitted` before the approved loopback rerun passed. This was an environment restriction, not a receiver test failure. Disposable loopback receiver only. |
| Generic iOS build | Final unsigned generic iOS `build-for-testing` passed after a sandbox-only failure involving Swift cache/CoreSimulator permissions; an approved retry passed. A nonfunctional AppIntents metadata-extraction warning was recorded. | Compilation only; sandbox denial is not a source failure. No signing, launch, HealthKit, file protection, Keychain, or accessibility proof. Full build output remains in the task transcript, not a retained log in this package. |
| Governance and generator | Documentation verifier and project-generator `--check` passed. | Structural consistency only. The PRD was not visually rendered in this cycle. |
| Simulator visual smoke | `com.beckliu.boazhealth` launched on a synthetic iPhone 17 Pro Simulator and the initial screen was inspected. The screen renders with `LOCAL ONLY` and a readable “Health sync needs attention” alert: “Cloud erasure recovery is unavailable; pairing and upload remain blocked.” | `/private/tmp/boaz-health-p0-visual.png`; unsigned-simulator Keychain unavailability triggers the expected fail-closed state. This is one initial-screen observation, not a complete visual/UX, signed-device or HealthKit acceptance. |

The retained simulator and gateway logs have these SHA-256 values:

| Temporary log | SHA-256 |
|---|---|
| `boaz-health-20260919T011250Z-45186.log` | `8ccefffba7301195bea53beee126b82e0314013d2651517f79f666f3db9b4abc` |
| `boaz-health-20260919T012247Z-53266.log` | `0e525712c4239361d6386d09291586f8fd0ec152ce26ad09a8297ab930d60030` |
| `boaz-health-20260919T015202Z-82287.log` | `e2598b9afa8bb05d0a1abbb47258495253bbab546c9ec4ee4d3393ea891b75ef` |
| `boaz-health-20260919T020024Z-87586.log` | `9b25ee73fc705036c132a715368c8e8fc05851c8ef486f959ee715af509d51aa` |
| `boaz-health-20260919T020402Z-88580.log` | `ff9e8a830032529b4921c216e80f3f7449efd630e398d4b5994d78f224873dbe` |
| `boaz-health-20260919T020725Z-89818.log` | `a651bc6a1cc6e94cfd76e4c4ebbb7e403c70103bbe283b3e6ff4c704bd40fe52` |

`/private/tmp` is volatile. These paths and hashes identify the observed local artifacts but do not make them durable repository evidence.

The frozen `docs/TEST_CASES.md` progress paragraph was written while the 78-test rerun was pending. It is part of the 98-file source identity and was intentionally not edited after the run; the final outcome above supersedes only that time-bound progress note. The 77 acceptance-case definitions and disposition rows remain unchanged.

The final standalone local-core and gateway checks also used the frozen source identity. A read-only post-check comparison of all 98 entries in `source-sha256.txt` returned 98 matches. The local-core and gateway logs show nonfunctional AppIntents metadata tool warnings, but no skipped tests or runtime warnings. The Mac/simulator figures do not establish phone storage, protected-file behavior or private-network acceptance.

The inspected simulator screenshot is a volatile 1206×2622 PNG at `/private/tmp/boaz-health-p0-visual.png`, SHA-256 `40b6b595556612f6ade619a0db0a8c77eef90fe2273d124d36dbf4eaaac2d4eb`. It is not retained in this repository, and no additional app flow, interaction, accessibility setting, or physical-device state was inspected in that smoke check.

The refreshed Rust checks were run from this repository on 2026-09-19 before this evidence update: `cargo test --manifest-path Server/Cargo.toml --offline --locked` (final exit 0, 32+37 tests passed); `cargo clippy --manifest-path Server/Cargo.toml --offline --locked --all-targets -- -D warnings` (final exit 0); `cargo fmt --manifest-path Server/Cargo.toml -- --check` (final exit 0); `cargo build --manifest-path Server/Cargo.toml --offline --locked --release` (final exit 0); `python3 scripts/test_server_http.py --build` (an earlier sandbox bind attempt exited 1 with `Operation not permitted`, then an approved loopback rerun exited 0 with 8/8 checks and upload closed). The sandbox attempt did not establish a source failure. The tool transcript is the only full output retained for these refreshed commands; no post-hoc log or test-time source hash is invented.

## Synthetic performance record

Three runs were measured on the same local host and the median was recorded. This is a regression check against the current local baseline, not an iPhone or Tokyo capacity claim.

| Dataset | Import median | Batch preparation median | Stored bytes | Peak RSS |
|---|---:|---:|---:|---:|
| 10,000 records | 2.989 s | 6.255 s | 12,342,024 | 289,357,824 |
| 100,000 records | 28.922 s | 73.907 s | 111,977,072 | 323,272,704 |

No recorded metric regressed by more than 15% against the current local baseline. `PERF-01` and `PERF-02` remain `BLOCKED` because their full acceptance criteria require fixed device hardware, UI responsiveness and free-space budgets, or native Tokyo metrics/backup/restore measurements.

## Remaining release blockers after the safety fixes

- `ERA-04` and `BAK-03` no longer have the previously identified missing-file/manifest acceptance bug: the current prune path checks physical files, manifests, SHA-256 values and active control events before deletion or expiry. Targeted tests reject missing file/manifest, missing control event and mismatched hash before mutation. The full cases still need process-crash/failed-removal/scheduler evidence, alerts and native metric/backup inventory observation.
- Production backup/prune now reject a configuration-flag-only encryption claim before ledger mutation. They require a physically verified dm-crypt backup mount on a device distinct from both live databases. The test-only synthetic backup/prune seam is not a production bypass. No Tokyo mount or encrypted recovery-domain acceptance is claimed.
- `BAK-05` has restore-specific replay of all applicable tombstones and erasure intents, including those already marked verified; an old-snapshot synthetic test confirms the erased identity is not revived in staged health data. The full case remains blocked until isolated native metric series are removed/rebuilt and read back, a completed epoch is recorded, and a second-generation restore is rehearsed.
- `backup-control`, `restore-control`, and `restore-health --staging-path` now exist. They require separated dm-crypt recovery storage; restore requires upload off and an exclusive lifecycle lock. The restore commands no longer require a healthy live database merely to stage verified artifacts, but still require validated live directories and do not repair or replace the live store. `restore-health` deliberately reports `projection_rebuild_required` and exits nonzero. It does not perform isolated native VictoriaMetrics rebuild/readback, final restore-epoch completion, operator cutover or rollback.
- A control publication lock, paired ingress/erasure operation lock, read-only legacy preclassification and exact-prefix resumable seed are present and covered by local tests. A full process-crash journal/cutover drill and off-host expected-head custody in Tokyo remain release gates.
- Hot-path authentication checks the current control tail, not the entire historical mirror chain. Startup and worker reconciliation perform full verification; an older mirror alteration may remain undetected until the next healthy reconciliation tick (normally 60 seconds idle), not necessarily the next request. The complete tamper/timing matrix remains open under `OPS-08`.
- Signed iPhone/Watch, actual HealthKit data access, protected-file behavior before/after first unlock, background delivery, accessibility, and visual acceptance are not established.
- Tokyo private HTTPS, tailnet access control, dm-crypt runtime evidence, separated recovery volumes, installed binary/listener identity, native VictoriaMetrics readback, off-host control head, disaster restore, and rollback are not established.

No real Health data was used. No real upload, production deployment, host migration, real-data deletion, commit, push, or publication was performed.
