# GRDB client architecture verification — 18 September 2026

**Scope:** local client source, synthetic databases, package-linked iOS compilation and simulator checks. No real HealthKit export, cloud upload, Tokyo deployment, device erasure, commit or push was performed. The worktree remains dirty; this document does not replace the earlier SQLite3-era [acceptance report](ACCEPTANCE_REPORT_2026-09-18.md).

**Release decision:** `AI-NATIVE GATE: INTERCEPT`. A local pass cannot establish signed iPhone/Watch protection and usability or private Tokyo encryption, native metrics identity, restore, erasure and rollback.

## Dependency and implementation boundary

The project generator and Xcode project pin the official GRDB.swift **7.11.1** product. `Package.resolved` records Git revision `b83108d10f42680d78f23fe4d4d80fc88dab3212` at the canonical `https://github.com/groue/GRDB.swift.git` location. The app owns a `DatabasePool`, WAL/FULL writer and at most four readers; schema v2 retains the same `health.sqlite` path and adds an event-change clock. Pairing/erasure bodies use `Codable`; persisted upload-batch bytes are not re-encoded. These are source facts, not claims of physical durability.

Initial HTTPS package resolution failed on GitHub TLS. An approved one-command Git URL rewrite to the same official repository over SSH port 443 resolved the exact tag without changing the project URL or global Git configuration. The dependency version and revision above are the reproducibility boundary.

## Executed checks and limits

| Check | Result | Evidence limit |
|---|---|---|
| Final-source unsigned generic iOS `build-for-testing` | **PASS** after GRDB resolution; app and XCTest target compile under Swift 6. | No tests run by this command and no phone install. |
| GRDB migration simulator XCTest | **PASS: initial targeted 11 tests** (`/private/tmp/boaz-grdb-migration-final.xcresult`); the final source's 15 migration tests also passed in the full suite below. | Synthetic v0/v1/v2, WAL/FULL/FK, interrupted-transaction rollback, external-writer, indexed aggregation, batch-race and protection-gate checks; not power-loss or signed-phone proof. |
| Gateway synthetic simulator XCTest | **PASS: original 3/3 scenarios** through one XCTest wrapper; `bash scripts/test_gateway_transport.sh` also passed after the memory fix. Earlier result: `/private/tmp/boaz-gateway-final.xcresult`. | In-process URL protocol; no DNS, private HTTPS or real upload. |
| Rust receiver `cargo test --manifest-path Server/Cargo.toml --offline --locked` | **PASS: 26 integration tests**. | Server source was not changed for this migration; no Tokyo host run. |
| Rust `cargo clippy --manifest-path Server/Cargo.toml --offline --locked --all-targets -- -D warnings` | **PASS**. | Static server check only. |
| Original 11 core scenarios, 10k/100k workloads | **PASS: 11/11 at each scale**, zero XCTest failures. After the memory fix, isolated result bundles: `/private/tmp/boaz-core-10k-memory-fixed.xcresult` and `/private/tmp/boaz-core-100k-memory-fixed.xcresult`. `bash scripts/test_local_core.sh` also passed both cases (22/22 internal scenarios) in 105.057 seconds. | 10k: 20 pages/50 batches; 100k: 200 pages/500 batches. Awake isolated measurements below; no actual HealthKit/network throughput. |
| Final-source full iOS simulator XCTest suite | **PASS: 72/72, 0 failed/skipped, 0 runtime warnings** in 118.772 seconds on an awake disposable iPhone 17 Pro/iOS 26.5 simulator. Result: `/private/tmp/boaz-full-memory-fixed.xcresult`. | Includes the memory fix, v1 rollback, indexed aggregate, cancellation and migrated harnesses. No signed phone run. An earlier 72/72 run was sleep-contaminated (`/private/tmp/boaz-full-final.xcresult`). |
| Generator, package lock, documentation/diff and wrapper checks | **PASS:** both wrapper scripts, generator idempotence, project-file lint, shell syntax and `git diff --check`. The local package checkout matched the exact pinned tag/revision. | Read-only cross-document review; no commit, signed install or production data. Untracked new files require precise inclusion if a commit is separately requested. |
| Disposable simulator empty-dashboard launch and screenshot | **PASS:** app launched; `/private/tmp/boaz-empty-dashboard-20260918.png` was visually inspected. | `LOCAL ONLY`, zero records/pending upload, upload off and an explicit ambiguous sleep-access empty state rendered. No real HealthKit permission, content, touch-flow or accessibility audit. |

The first simulator launch failed before XCTest because Xcode reused a GRDB module for the wrong simulator architecture; isolated arm64 build output and a new disposable simulator recovered test launch. The first 10k core run passed scenarios CORE-01–08, then failed a **synthetic fixture** that set `user_version=0` on a v2 file without removing v2-only triggers/table. The fixture now removes those objects; a separate frozen historical v1 SQL fixture also checks exact rows, anchor, receipt and retry bytes. That interrupted run is not counted as a passing core suite.

The **post-fix isolated, awake GRDB/FULL simulator** runs measured 10k import/batch preparation at **2.524/5.753 seconds**, 12,267,552 database/WAL/SHM bytes, **289,357,824 B peak test-process RSS**, and a 105.744 ms maximum gap in a 100 ms main-actor scheduling probe; 100k measured **24.770/70.968 seconds**, 113,491,496 bytes, **322,764,800 B peak RSS**, and a 104.126 ms maximum scheduling gap. The 100k ledger retained every record and completed 500 batches. The probe measures scheduling, not rendered frame rate or touch responsiveness. The earlier direct-SQLite3 **Mac** baseline was 2.034/6.492 seconds (10k) and 20.614/74.906 seconds (100k), with no recorded peak memory/heartbeat. Different runtime/hardware and the missing baseline dimensions prevent controlled before/after speed, memory or UI claims. Phone memory, battery, initial-import time and storage headroom remain device measurements.

An isolated awake 100k run **before** the batch-memory fix was 277,053,440 B resident after import but 4,203,085,824 B after preparing 500 batches (4,203,102,208 B peak), with 71.014 seconds of batch work. The same post-fix path was 322,748,416 B resident after batches (322,764,800 B peak) with 70.968 seconds of batch work. This ~92.3% lower peak follows a per-batch `autoreleasepool` around temporary JSON size probes; batch construction, transaction, hash, limits and persisted retry bytes remain unchanged. It is a same-simulator synthetic comparison of the memory fix, **not** a direct SQLite3-versus-GRDB benchmark or phone capacity claim.

The earlier complete simulator run took 6,231 wall-clock seconds, but `pmset -g log` records clamshell sleep followed by repeated roughly 15–18-minute sleep intervals during that run (e.g. 18:40:49–18:57:57 and 18:57:59–19:15:04 local time). XCTest operation timing and heartbeat gaps contain those pauses. Do not treat that duration, its 3.16 GB test-process peak, or its million-millisecond scheduling gaps as app throughput or visual responsiveness. The final awake suite above is the current correctness run.

## Reproduction and source identity

From the repository root, the final simulator build/test used the following commands (replace the disposable simulator ID and clone path on another machine). The package clone was the official GRDB tag/revision pinned above; `-disableAutomaticPackageResolution` requires it to be present already. Both commands exited 0:

```sh
xcodebuild -quiet -project boazapp.xcodeproj -scheme boazapp \
  -destination 'platform=iOS Simulator,id=1F964859-1070-4D3B-B6E4-7176D869519B' \
  -derivedDataPath /private/tmp/boaz-sim-agent-dd \
  -clonedSourcePackagesDirPath /private/tmp/boaz-grdb-packages \
  -disableAutomaticPackageResolution -parallel-testing-enabled NO \
  ONLY_ACTIVE_ARCH=YES ARCHS=arm64 CODE_SIGNING_ALLOWED=NO build-for-testing
xcodebuild -quiet -project boazapp.xcodeproj -scheme boazapp \
  -destination 'platform=iOS Simulator,id=1F964859-1070-4D3B-B6E4-7176D869519B' \
  -derivedDataPath /private/tmp/boaz-sim-agent-dd \
  -clonedSourcePackagesDirPath /private/tmp/boaz-grdb-packages \
  -disableAutomaticPackageResolution -parallel-testing-enabled NO \
  -resultBundlePath /private/tmp/boaz-full-memory-fixed.xcresult \
  CODE_SIGNING_ALLOWED=NO test-without-building
xcrun xcresulttool get test-results summary --path /private/tmp/boaz-full-memory-fixed.xcresult
xcodebuild -quiet -project boazapp.xcodeproj -scheme boazapp \
  -destination 'generic/platform=iOS' -derivedDataPath /private/tmp/boaz-generic-final-dd \
  -clonedSourcePackagesDirPath /private/tmp/boaz-grdb-packages \
  -disableAutomaticPackageResolution CODE_SIGNING_ALLOWED=NO build-for-testing
```

The 10k/100k cases can be rerun with `BOAZ_TEST_DESTINATION` set to an available simulator and `bash scripts/test_local_core.sh`; that script executes both isolated XCTest cases. The original three gateway cases use `bash scripts/test_gateway_transport.sh`. Project regeneration with `PYTHONDONTWRITEBYTECODE=1 python3 scripts/generate_xcode_project.py` left `project.pbxproj` byte-identical. All fixtures contain synthetic values. These source hashes identify the reviewed dirty worktree atop `480dae2dd2822cbd7facc4e840b52bb3629c8208` (not a commit of these changes):

| File | SHA-256 |
|---|---|
| `BoazLocalDatabase.swift` | `13c2592977fbfa5e053f560095b108e5cde8db90e133689fe78c7b138a7b06d5` |
| `Schema.sql` | `91e18b075f564d653daa8d2357506003facf1cfe5943e6a31b4a7ab47a7c3990` |
| `SyncEngine.swift` | `8a26216774e813e57dfa3f724031ecfb4f1d099cde020366246500d59f2ca110` |
| `TokyoCloudGateway.swift` | `6da0d28792199ac1fb5efcff24379936963ee288081f7049bde48d7a1197bd3c` |
| `Tests/Fixtures/SchemaV1.sql` | `9d48a69d99971c6707c55373569e99271f573c0420227ecb21524159543dece4` |
| `project.pbxproj` | `abc56ad6a1bd9b60daf94d4f1701e84a9b7e5db33299103b62512159efec3444` |

The historical fixture's contents after its two explanatory comment lines have SHA-256 `ccc9edb6c2ffda7a2b57de27382de9b317112396f2260b64458ddb9737402ec3`, identical to `git show HEAD:boazapp/Core/Database/Schema.sql`. The temporary `.xcresult` bundles and simulator screenshot are local evidence; the command, counts, timings, limits and hashes above remain in this report if temporary output is later removed.

## Physical iPhone boundary

A paired wired iPhone 16 Pro runs iOS 27.0 with Developer Mode enabled, but CoreDevice reports **connected (no DDI)** and DDI services not enabled. Xcode 27.0 and the installed DDI both identify build `27A266a`; [Apple lists Xcode 27 device support through iOS 27](https://developer.apple.com/xcode/system-requirements). Xcode's error is DDI personalization (`CoreDeviceError 12040/12051`, `CryptexKitHost.TatsuError 4`) before app launch. Its cause is unknown; it is not evidence that Boaz Health code failed. No cache clearing, system package installation, unpairing, diagnostic archive, signed install or device HealthKit query was performed. `devicectl diagnose` may collect broad host/device diagnostics; review its privacy scope separately if escalation to Apple is needed.

## Review verdicts

| Domain | Result and remaining evidence |
|---|---|
| E1 — domain/device boundary | **INTERCEPT:** physical iPhone/Watch and Tokyo behavior untested. |
| E2 — product outcome | **INTERCEPT:** manual-sync/error UI still needs signed-device flow verification. |
| E3 — data authority | **INTERCEPT for release:** synthetic migration and ledger checks passed; signed-device end-to-end record identity and restore not proven. |
| E4 — AI/evidence architecture | **PASS for deterministic local design only:** no retrieval/model service is needed for ingestion; runtime claims remain limited to executed tests. |
| E5 — privacy/reliability | **INTERCEPT:** device protection/backup and Tokyo recovery gates remain open. |
| E6 — validation/economics | **INTERCEPT:** the full simulator suite passed, but controlled pre/post resource comparison and representative device/host costs remain open. |
