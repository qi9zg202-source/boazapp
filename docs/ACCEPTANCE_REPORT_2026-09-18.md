# Boaz Health acceptance report — 18 September 2026

**AI-NATIVE GATE: INTERCEPT — release and real health upload are not accepted.** Local tests have produced passing evidence, and defects found during testing have been corrected. Physical iPhone/Watch acceptance and the Tokyo deployment/recovery gates remain incomplete. This report does not authorize production changes or real health transfer.

## 1. Scope and finishing criteria

Request: write detailed test cases, run tests and acceptance, and document the project including its local paths. The [test specification](TEST_CASES.md) defines **74 cases**, with priorities, fixtures, steps, expected results and evidence requirements. A case definition is not an executed test. Automated test-method counts below are separate from the 74 acceptance cases; they must not be added into a coverage percentage.

The local scope finishes when the current iOS and Rust sources compile, available automated tests actually run, reproduced defects are corrected and retested, a rendered app view is inspected, evidence is retained, and setup/recovery instructions match the source. Release acceptance additionally requires every applicable device, private-access, encrypted-storage, native-metrics, erasure, restore and rollback case to pass. Missing environment evidence keeps real upload disabled.

All new records used in tests were synthetic. The receiver tests used temporary databases; the Mac storage harness removed its temporary files. No real HealthKit records were sent to Tokyo. The Tokyo inspection was read-only.

## 2. Project and test environment

| Item | Observed value |
|---|---|
| Local repository | `/Users/beckliu/Documents/0agentproject2026/boazapp` |
| Xcode project | `/Users/beckliu/Documents/0agentproject2026/boazapp/boazapp.xcodeproj` |
| Rust service | `/Users/beckliu/Documents/0agentproject2026/boazapp/Server` |
| Branch / starting commit | `codex/boaz-health-ios` / `480dae2` |
| Tested source identity | Exact source hashes and base commit are retained in [source-manifest.json](evidence/2026-09-18/source-manifest.json). Tests include working-tree changes after the starting commit. |
| Mac | macOS 26.6.2, build 25G83, arm64 |
| Xcode / compiler | Xcode 27.0, build 27A266a; Apple Swift 6.4; Swift 6 language mode |
| iPhone SDK / deployment target | SDK 27.0 / minimum iOS 17 |
| Executed iOS environment | iPhone 16 Pro simulator, iOS 26.5; unsigned test build |
| Physical devices | CoreDevice listed a paired Watch; no available physical iPhone was listed. Signing and physical device execution were not performed. Device serials are excluded from retained evidence. |
| Rust | rustc and Cargo 1.96.0; locked dependencies; local macOS binary |
| Temporary build/results directory | `/private/tmp/boazapp-acceptance-20260918` |
| Fixed source copy for final iOS run | `/private/tmp/boazapp-acceptance-source`; copied source hashes are compared with the live repository before evidence is sealed. |
| Retained evidence | [`docs/evidence/2026-09-18/`](evidence/2026-09-18/) |

The first build reported stale CoreDevice/CoreSimulator services. A subsequent `devicectl list devices` refreshed the simulator service; actual simulator tests then ran successfully. The Simulator desktop application itself was not installed at the Xcode application path and could not be opened through Computer Use. The simulator runtime still supports test execution and screenshot capture. This permits a rendered dashboard inspection, but does not establish a completed interactive settings/consent/audit flow.

## 3. Executed checks

Final test counts and artifact hashes are recorded in [results.json](evidence/2026-09-18/results.json). The retained logs are the evidence for each command, including warnings and limitations.

| Check | Actual result and scope | Evidence |
|---|---|---|
| iPhone target and XCTest bundle build | Generic iOS `build-for-testing` succeeded. No Swift compiler warning/error was observed. Xcode emitted two AppIntents metadata-tool warnings because these targets do not use AppIntents. The literal zero-warning build requirement therefore remains qualified. | [Device build log](evidence/2026-09-18/ios-device-build.log) |
| Executed iOS tests | **47 passed, 0 failed, 0 skipped.** Covers units, local transaction/anchor behavior, receipts, immutable retry bytes, byte bounds, backup exclusion flag, sleep intervals/history and averages, consent, client request construction, endpoint/redirect rules and workout queue recovery. | [Xcode summary](evidence/2026-09-18/ios-summary.json), [per-test results](evidence/2026-09-18/ios-tests.json), [action log](evidence/2026-09-18/ios-action.json) |
| Rust integration suite | **26 passed, 0 failed.** Covers exact retries/conflicts, device isolation, validation, revisions/deletions, gate failures, pending metrics, revocation/erasure, backups and restoration. These invoke the real router and SQLite in temporary fixtures. | [Server test log](evidence/2026-09-18/server-tests.log) |
| Rust static check and release build | Clippy passed with warnings treated as errors; native macOS release binary built. This is not a Linux release artifact. | [Clippy](evidence/2026-09-18/server-clippy.log), [release build](evidence/2026-09-18/server-release.log) |
| Real local HTTP process | **7/7 checks passed** with all upload gates off: unauthenticated status 401; pairing/batch 503; missing route 404; loopback connection/startup address; intact empty SQLite; child and temporary-file cleanup. | [HTTP results](evidence/2026-09-18/server-http.json) |
| Production Swift core, 10,000 records | **11/11 scenarios passed** in a macOS executable using actual production sources. 20 import pages and 50 upload batches; all records retained. Includes legacy schema compatibility, rejection of future/partial/corrupt databases without reset, and a committed-page protection failure. | [10k log](evidence/2026-09-18/local-core.log) |
| Production Swift core, 100,000 records | **11/11 scenarios passed**. 200 import pages and 500 bounded upload batches; no missing records. This measures local SQLite/batch work with synthetic acknowledgments, not network or HealthKit throughput. | [100k log](evidence/2026-09-18/local-core-100k.log) |
| Production client request path | **3/3 synthetic transport scenarios passed**. The actual upload method sends the intended private URL, authorization header and unchanged batch bytes on a 503 retry, rejects a public origin before transport, and rejects a mismatched receipt. URLProtocol supplies responses inside the test process; no remote server or TLS connection is involved. | [Transport log](evidence/2026-09-18/gateway-transport.log) |
| More than 100,000 sleep records | A real temporary SQLite fixture contains **100,001 samples across 730 nights**. Tests verify every expected day, earliest/latest nights, idempotent refresh, actual raw-session deletion, timezone re-keying, page boundaries, cancellation, corrupt later pages and concurrent changes. | `SleepHistoryTests` in the [simulator log](evidence/2026-09-18/ios-tests.json) |
| Rendered dashboard | App launched in the simulator. Empty state visibly shows local-only mode, upload off, zero local/pending records, unavailable sleep and unavailable activity measurements. Visible text/layout were inspected. Interactive settings, audit, populated data, VoiceOver and physical haptics remain unverified. | [Dashboard screenshot](evidence/2026-09-18/dashboard-empty.png) |
| Current Tokyo preflight | Read-only SSH confirmed Docker proxy on `127.0.0.1:8428`, no Serve configuration, inactive health receiver, and all three required health storage directories missing. | [Tokyo readback](evidence/2026-09-18/tokyo-preflight.log) |

An in-process test does not establish private HTTPS operation. A local backup test sets a synthetic environment flag to exercise its branch; it does not prove encryption. Redirect-policy tests keep network tasks suspended and call the policy directly; the separate URLProtocol test executes request construction and response handling without leaving the process. No successful native VictoriaMetrics projection/readback was run in this environment.

The combined simulator run initially executed 43 methods with 26 database failures because the simulator did not expose a file-protection attribute. The failure log is retained as [ios-protection-failure.log](evidence/2026-09-18/ios-protection-failure.log), bound to its earlier [source snapshot](evidence/2026-09-18/source-protection-failure.json). Simulator builds now apply the setting but skip effective-class read-back; physical builds still require it. This is an explicit simulator limitation, not an encryption pass. Final results below refer to the subsequent rerun.

## 4. Defects corrected during testing

| Defect | Correction and regression evidence |
|---|---|
| Receipt without a matching request hash could advance cloud state; polled projection status was not bound to the stored request. | Both upload and local state transitions now require matching batch ID, count, body hash, positive commit sequence and valid timestamps. A projected state requires a projection timestamp. Invalid/missing proof and malformed JSON leave the queue unchanged. |
| Another device reusing a globally unique batch ID caused an internal server error. | Receiver returns `409 batch_conflict` before mutation and reveals no other device's receipt. Two-device regression passes. |
| Concurrent attempts to redeem one pairing code could return an internal error during SQLite lock escalation. | Pairing now obtains the write transaction before reading the code. Eight simultaneous attempts produce exactly one successful redemption; the pairing and duplicate-batch race tests also passed ten repeated runs. |
| Derived sleep read at most 100,000 raw rows and could remove valid older derived days. | Complete history now uses 500-row keyset pages and one-session accumulation. Reconciliation occurs only after a complete stable scan, with a version check inside the write transaction. The 100,001-row regression passes. |
| URLSession could follow a redirect beyond the chosen private endpoint. | Every redirect is refused. Endpoint parsing accepts only a valid `.ts.net` HTTPS root origin with absent/443 port and no credentials, query or fragment. Tests cover 32 endpoint strings and 20 redirect combinations. |
| The protected local health folder was not explicitly excluded from system backups. | Added the directory backup-exclusion attribute. The simulator verifies the attribute; actual iOS protection/backup behavior remains a physical-device gate. |
| A finite but extremely large invalid sleep-stage value could trap when converted to an integer. | Stage range is validated before conversion. Tests include a huge finite value, NaN, infinity, noninteger stages and invalid durations. |
| An unavailable workout could stop collection before later pending workouts. | Workout collection isolates failures, retains failed work for a later retry and continues eligible jobs within a bounded pass. Regression tests cover failure and multi-pass behavior. |
| The main sleep card searched only seven days and at most 10,000 records; overnight averages could lose the selected night's data behind newer readings. | The card now uses complete paged sessions. SQLite computes each average within the chosen interval without a row cap. Regressions cover a 20-day-old night with 10,001 records and more than 10,000 readings both inside and after the averaging window. |
| Schema setup did not explicitly reject an incompatible existing phone database. | The shared task added a non-destructive schema/integrity check and version marker. The combined verification exercises compatible unversioned data, future versions, partial schema and corrupt contents. No database reset is used as recovery. |
| A file-protection error after SQLite commit could be presented as a failed transaction. | The shared task separates committed data from a later protection error and blocks upload until protection is verified. The injected error retains the committed page and anchor; this proves the software gate, not effective physical-device encryption. |
| A throwing database initializer closed an already-owned SQLite handle twice. | Once all stored properties are initialized, the destructor owns cleanup even if a later check throws. Removed the extra close; failed-open fixtures and simulator reruns no longer produce SQLite invalid-handle messages. |
| Activity-date components did not carry the Gregorian calendar/era required by HealthKit; the full-history epoch also depended on the user's calendar system. | Query components now carry Gregorian era/date fields and the user's timezone. The scan starts at Gregorian 1 January 2014. Actual predicate construction and synthetic DST, leap-day, non-Gregorian input, year-boundary and range-limit cases cover the conversion. Real multi-window HealthKit activity import remains a device gate. |

## 5. Measurements and their limits

| Synthetic Mac workload | Import / local save | Prepare and acknowledge batches | SQLite + WAL + SHM at measurement |
|---|---:|---:|---:|
| 10,000 quantities, 20 pages / 50 batches | 2.034 s | 6.492 s | 11,652,448 bytes |
| 100,000 quantities, 200 pages / 500 batches | 20.614 s | 74.906 s | 112,010,008 bytes |

These are single development-Mac observations, not a phone or Tokyo capacity promise. The ledger includes retained immutable batch bodies and audit entries, so this size is not just raw sample storage. Peak phone memory, battery use, UI responsiveness under initial import, Tokyo disk use, network time, metric projection delay, backup/restore duration and long-run growth still need representative environment measurements. No arbitrary performance threshold was invented to turn these observations into a release pass.

The 100,001-row sleep fixture uses repeated overlapping readings spread over 730 nights. It exercises complete history and duplicate interval handling, but does not establish a worst-case bound for one pathological session containing all records. Memory is proportional to the largest session plus selected day summaries.

## 6. Acceptance disposition

The [case-results.csv](evidence/2026-09-18/case-results.csv) has one entry for each of the 74 specified cases: **13 PASS, 2 FAIL, 39 BLOCKED, 20 NOT_RUN**. `PASS` requires the stated case observations; `BLOCKED` identifies an unavailable device/host prerequisite; `NOT_RUN` identifies a remaining variant or integration procedure. Passing related unit tests is recorded as partial evidence without promoting an incomplete case to PASS. The two failures are the observed Tokyo native-metrics and private-HTTPS/service gates.

Required evidence still missing:

1. Signed physical iPhone/Watch installation, selected Health permissions and partial availability, actual full-history HealthKit queries/deletions, workout associations, locked/reboot/background behavior and offline recovery.
2. Completed dashboard/settings/audit/consent/stop/erasure interactions, populated-data checks, VoiceOver, Dynamic Type and physical haptics. The inspected empty screenshot covers only its visible screen.
3. A Linux receiver on verified encrypted storage, native VictoriaMetrics identity, restricted private HTTPS, denied-peer tests and successful receipt-to-metrics readback, including outage recovery and deletion.
4. Encrypted backup restore and rollback on the approved host, scheduled backup expiry and erasure confirmation. Restoring a historical snapshot also requires reconciling later erasures and revocations before reopening ingest. The present snapshot tests do not prove that disaster-recovery reconciliation; no independent retained erasure/revocation journal has been accepted.
5. Remaining injected collection/calendar/transport/failure variants named in the case register and representative phone/Tokyo resource budgets.

The live Tokyo failures independently prevent deployment acceptance. Existing cloud upload gates remain closed; queued local records remain durable. Real upload must stay disabled until these requirements have current evidence and the user gives separate in-app consent.

## 7. Reproduce the local checks

```sh
cd /Users/beckliu/Documents/0agentproject2026/boazapp
python3 scripts/generate_xcode_project.py
./scripts/test_local_core.sh
bash scripts/test_gateway_transport.sh
BOAZ_TEST_RECORDS=100000 ./scripts/test_local_core.sh
cargo test --manifest-path Server/Cargo.toml --offline --locked
cargo clippy --manifest-path Server/Cargo.toml --offline --locked --all-targets -- -D warnings
cargo build --manifest-path Server/Cargo.toml --release --offline --locked
python3 scripts/test_server_http.py --build
xcodebuild -project boazapp.xcodeproj -scheme boazapp \
  -destination 'generic/platform=iOS' \
  -derivedDataPath /private/tmp/boazapp-acceptance-dd \
  CODE_SIGNING_ALLOWED=NO build-for-testing
xcrun simctl list devices available
```

Select an actual available simulator identifier from the last command, then run:

```sh
xcodebuild -project boazapp.xcodeproj -scheme boazapp \
  -destination 'platform=iOS Simulator,id=<available-simulator-identifier>' \
  -parallel-testing-enabled NO \
  -derivedDataPath /private/tmp/boazapp-acceptance-sim \
  -resultBundlePath /private/tmp/boaz-health-new-run.xcresult \
  CODE_SIGNING_ALLOWED=NO test
git diff --check
```

Use a new result-bundle path for each run. The HTTP test requires permission to bind a temporary local socket; a sandbox denial is an environment error, not a passing HTTP test. Retain the full logs, exact source hashes, command exit codes and redacted environment observations with any future report.

## 8. Recovery and change boundary

All test databases are disposable synthetic copies. The HTTP test terminates its child process in `finally`; the Mac harness removes its temporary directory on exit. Xcode build products and full `.xcresult` bundles stay under `/private/tmp`, outside Git. Retained evidence contains no bearer tokens, pairing codes, health exports or personal-device serial numbers.

The code changes are local to this feature branch. Other documentation and source changes appeared in the shared working directory during the run and were preserved. The final iOS run uses a fixed copy of the combined source to avoid concurrent edits during compilation; its file hashes must match the live repository before this report is finalized. This acceptance work does not merge, publish, install a Tokyo service, migrate its existing VM data or erase real cloud records. Production change and rollback instructions remain in [TOKYO_PREFLIGHT.md](TOKYO_PREFLIGHT.md) and [Server/README.md](../Server/README.md).
