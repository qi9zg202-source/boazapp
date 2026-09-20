# Source identity record — 19 September 2026

## Recorded identity

| Field | Value |
|---|---|
| Repository | `/Users/beckliu/Documents/0agentproject2026/boazapp` |
| Branch | `codex/boaz-health-ios` |
| Base commit | `480dae2dd2822cbd7facc4e840b52bb3629c8208` |
| Worktree | Dirty combined worktree containing tracked modifications and untracked implementation, test, documentation, and evidence files |
| Data policy | Synthetic records only; no real Health data |
| Mutation boundary | No commit, push, deployment, production upload, host migration, or real-data erasure |

The first iOS simulator and gateway runs preceded the added 78th XCTest and server safety edits. The first 78-test simulator run failed 76/78 because two synthetic gateway tests encountered unsigned-test Keychain `unavailable(-34018)`; its failure is retained. Before the final full simulator run, a [98-file SHA-256 source manifest](source-sha256.txt) was frozen. Its aggregate SHA-256 is `7c8abd4be9b1b6159b3e187de14f3e7689592505ae5da80c95eb95a69b78e562`, and the tracked binary diff hash at that freeze was `aef60da9c1cc851f4f618e91d73a46239248ab69a08a136c39952a38679012ba`. The manifest includes 98 project files and excludes only the mutable `docs/evidence/2026-09-19` package. All 98 entries were checked after the final 78/78 simulator run with no drift; this source identity is bound to that final run, not retroactively to earlier runs. The refreshed Rust tests, Clippy, format, release build and isolated HTTP checks have recorded command results, but no separate frozen Rust test-start manifest was captured. The initial documentation-time tracked diff hash was `1fdbb54debb2c314f42e801951cea77b9180fab8abb2f9bb23c0d40c5a85b552`; another at 2026-09-19 01:44:47 UTC was `bfbe761d01e5bb1130858959a28474ba3db23ef68dd42bc8d34afbdc219a71c6`. Those earlier diff hashes exclude untracked files and are not complete tested-tree identities.

The full simulator and gateway logs were still present when this record was created:

| Artifact | SHA-256 |
|---|---|
| `/private/tmp/boaz-health-20260919T011250Z-45186.log` | `8ccefffba7301195bea53beee126b82e0314013d2651517f79f666f3db9b4abc` |
| `/private/tmp/boaz-health-20260919T012247Z-53266.log` | `0e525712c4239361d6386d09291586f8fd0ec152ce26ad09a8297ab930d60030` |
| `/private/tmp/boaz-health-20260919T015202Z-82287.log` | `e2598b9afa8bb05d0a1abbb47258495253bbab546c9ec4ee4d3393ea891b75ef` |
| `/private/tmp/boaz-health-20260919T020024Z-87586.log` | `9b25ee73fc705036c132a715368c8e8fc05851c8ef486f959ee715af509d51aa` |
| `/private/tmp/boaz-health-20260919T020402Z-88580.log` | `ff9e8a830032529b4921c216e80f3f7449efd630e398d4b5994d78f224873dbe` |
| `/private/tmp/boaz-health-20260919T020725Z-89818.log` | `a651bc6a1cc6e94cfd76e4c4ebbb7e403c70103bbe283b3e6ff4c704bd40fe52` |

The `.xcresult` bundles at matching paths were present, but no canonical bundle hash was recorded. `/private/tmp` is volatile and is not an evidence archive.

The final suite ran on Xcode 27.0 (`27A266a`) on Darwin arm64 and reported 78 executed, 78 passed, 0 failed, 0 skipped, 0 runtime warnings in 121.824 seconds. The `source-sha256.txt` manifest is the exact source-file identity for that run. It does not include the mutable evidence package or attest a signed iPhone, Apple Watch, encrypted Tokyo mount or production restoration.

The same 98-file identity was rechecked after the standalone `scripts/test_local_core.sh` run (2 XCTest methods, 11/11 synthetic scenarios at each of 10,000 and 100,000 records) and `scripts/test_gateway_transport.sh` run (1 XCTest method, 3/3 synthetic scenarios); 98/98 files still matched. Both had 0 failed/skipped/runtime-warning test results. The final unsigned generic iOS build-for-testing succeeded only on an approved retry after a sandbox Swift-cache/CoreSimulator permission failure; no build-log artifact was added to this package. Rust 32+37 tests, Clippy, release build and isolated receiver 8/8 also passed locally, without a separate frozen test-start source manifest.

## Reproduction template

For a future release candidate, first freeze a clean or explicitly enumerated worktree. Before running tests, record:

1. exact branch and commit;
2. `git status --short` output;
3. a sorted path and SHA-256 manifest for every source, schema, fixture, project, lock, deployment, and test file used;
4. an aggregate hash of that manifest;
5. toolchain and SDK versions;
6. exact commands, UTC start/end times, exit codes, warnings, test counts, skips, and result-bundle hashes;
7. the dirty patch hash only as an additional signal, never as a substitute for untracked-file hashes.

Bind every future case result to that frozen manifest. A hash captured after source changes proves only the later snapshot and must not be presented as the identity that produced an earlier run.
