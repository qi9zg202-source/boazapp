# Boaz Health P0-R — isolated recovery implementation evidence

Status: **implementation checkpoint, not disaster-recovery acceptance**. All test fixtures must be synthetic. No Tokyo production changes, real upload, real health-data deletion, commit or push are authorized by this record. `AI-NATIVE GATE: INTERCEPT`.

## Source and environment identity

| Item | Recorded observation |
|---|---|
| Date of this record | 2026-09-19 UTC; individual commands below retain their own times where captured. |
| Working branch and HEAD | `codex/boaz-health-ios`, `480dae2dd2822cbd7facc4e840b52bb3629c8208` (rechecked 2026-09-19T02:44:19Z). |
| Local host | `Darwin 25.6.0 arm64`; **not** the required isolated Linux/dm-crypt/native-VM recovery environment. |
| Pre-implementation source freeze | [source-manifest.md](source-manifest.md), captured 2026-09-19T02:29:56Z; 102 tracked and untracked non-ignored files, prior dirty diff SHA-256 `aef60da9c1cc851f4f618e91d73a46239248ab69a08a136c39952a38679012ba`. The manifest file itself has SHA-256 `2b652f549bc41b90bfe3a57f5ad2c95ebf737a7b80b73750c6344e33dcbe881c`. |
| Post-change identity | [post-source-manifest.md](post-source-manifest.md), captured 2026-09-19T03:04:02Z after the final migration test/fix, covers 103 tracked and untracked non-ignored files outside this new P0-R evidence directory. Manifest SHA-256 `18c9006d9f5cf71f4fcd933d857b6f862957012c44523894bd368024c65ce548`; tracked dirty-diff SHA-256 `a0e199e80680a50805e06114b19043abeca01c73167b42da8bee7c521dabbf3e`; pre-manifest Git-status SHA-256 `297a4747430da13e1e714b01707796078e3387678cc24cb740f76e39ea7f7ea7`. Implementation run reported `shasum -a 256 -c` as 103/103 verified, exit 0. The evidence files are outside this manifest's scope. |

## Scope and actual result

The worktree contains a control schema v2 migration preserving old event hashes, intent-first managed-backup pruning with constrained resume, and code-level recovery coordinator/native staging VM/oracle/readback constructs. The CLI conditionally takes a stable coordinator lock and fails closed on an unserved journal/active-set. The existing `restore-health` still exits nonzero at `projection_rebuild_required`; no accepted operator command completes native VM rebuild, external custody advance, generation activation, live restart/readback or second-generation recovery. Exact code and command results must be checked against the final worktree, not this prose alone.

Consequently, the P0-R end-to-end result is **BLOCKED**, not PASS. A missing independent Linux host, physically verified separate dm-crypt volumes, off-host expected-head custody and current native VM run mean the exercise cannot be replaced by macOS unit tests or an HTTP 2xx response. There is no evidence for the requested 10k/100k three-run Linux benchmark or a same-host baseline; performance acceptance is `NOT_RUN`.

Open source-review point: the BZHC v1→v2 migration uses a single SQLite transaction to rename the old `control_events` table, copy and compare every event, then drop the old transactional table before commit. This is not a database reset, and interruption tests must prove rollback, but it is a table rebuild; the plan's strict “no drop-and-recreate” wording needs a recorded interpretation before calling that migration compliant. No later v1-only binary may open a v2 store after new events are written.

## Command ledger

Exit codes below are from the document check or reported by the implementation run; those two observation channels are identified separately. The implementation run's individual UTC command times, durations and final source hash were not supplied, so none are invented. A blank, `RUNNING` or `NOT_RUN` item is not a pass.

| UTC / command | Exit | Observation / boundary |
|---|---:|---|
| 2026-09-19T02:44:19Z — `git branch --show-current` | 0 | `codex/boaz-health-ios`. |
| 2026-09-19T02:44:19Z — `git rev-parse HEAD` | 0 | `480dae2dd2822cbd7facc4e840b52bb3629c8208`. |
| 2026-09-19T02:44:19Z — `uname -srm` | 0 | `Darwin 25.6.0 arm64`. |
| 2026-09-19T02:44:19Z — `shasum -a 256 docs/evidence/2026-09-19-p0r/source-manifest.md` | 0 | Hash shown above. |
| 2026-09-19T02:44:19Z — `git diff --check` | 0 | No whitespace errors at that checkpoint; rerun on final worktree. |
| 2026-09-19T02:44:19Z — `python3 scripts/verify_doc_governance.py` | 1 | Reported two links to this then-not-yet-created README as broken. The README was subsequently created; a clean rerun is required. This intermediate failure is not a pass. |
| 2026-09-19T02:44Z — `python3 scripts/verify_doc_governance.py` | 0 | After this README was created: documentation internally consistent; 77 case definitions and status rows, 78 discovered XCTest methods. This is document/source discovery, not XCTest execution. |
| 2026-09-19T02:44Z — `git diff --check` | 0 | No whitespace errors at the document checkpoint; final rerun still required. |
| Reported by the implementation run by 2026-09-19T02:46:24Z — `python3 scripts/generate_xcode_project.py --check` | 0 | Generated project matched source at that checkpoint; no project rewrite. |
| Reported by the implementation run by 2026-09-19T02:46:24Z — `bash scripts/test_local_core.sh` | 1 | The restricted macOS session could not connect to CoreSimulatorService/simdiskimaged. This is a failed/environment-blocked execution, **not PASS**. |
| Reported by the implementation run by 2026-09-19T02:46:24Z — `bash scripts/test_gateway_transport.sh` | 1 | The same CoreSimulatorService/simdiskimaged connection-refused blocker; synthetic transport checks were not accepted. |
| 2026-09-19T02:48:50Z — `python3 scripts/verify_doc_governance.py` | 0 | Document governance still passes after the P0-R annex/evidence update; 77 historical case rows remain unchanged and 78 XCTest methods are discovered, not executed by this command. |
| 2026-09-19T02:48:50Z — `git diff --check` | 0 | No whitespace errors at the document checkpoint. Source work was still ongoing; repeat after the final edit. |
| Implementation run, time not captured — `cargo test --manifest-path Server/Cargo.toml --offline --locked`, final rerun | 0 | 52/52 library, 3/3 main and 44/44 API tests passed; 0 failed, 0 ignored. The added API case exercises a real control-v1 CLI migration and rejects a health/control store-ID mismatch without writing. A prior fresh-v2 misclassification was fixed before this rerun. This is synthetic/macOS source evidence, not a native Linux restore. |
| Implementation run, time not captured — `cargo clippy --manifest-path Server/Cargo.toml --offline --locked --all-targets -- -D warnings` | 0 | Lint passed at that source checkpoint. |
| Implementation run, time not captured — `cargo fmt --manifest-path Server/Cargo.toml -- --check`, first check | 1 | Found formatting differences. The formatter was then run and modified source. This first check remains a recorded failure. |
| Implementation run, time not captured — `cargo fmt --manifest-path Server/Cargo.toml -- --check`, final check | 0 | Formatting passed after source was formatted; does not supersede the initial failure record. |
| Implementation run, time not captured — `python3 scripts/test_server_http.py --build`, restricted attempt | `BLOCKED` | Sandboxed socket setup returned `EPERM`; no HTTP acceptance was claimed from this attempt. Exact process exit was not supplied. |
| Implementation run, time not captured — `python3 scripts/test_server_http.py --build`, authorized retry before harness update | 1 | An obsolete harness assertion expected BZHC v1 after the implementation had moved to v2. This was a test failure, not a server PASS. |
| Implementation run, time not captured — `python3 scripts/test_server_http.py --build`, after harness update | 0 | 8/8 isolated synthetic HTTP checks passed. This does not establish private Tokyo HTTPS or native VM persistence. |
| Implementation run, time not captured — `python3 scripts/test_server_http.py --build`, final rerun after migration fix | 0 | 8/8 isolated synthetic HTTP checks passed again; still not native Linux or Tokyo-private HTTPS evidence. |
| Implementation run, time not captured — `bash scripts/test_local_core.sh`, authorized retry | 0 | 2/2 XCTest methods and 11/11 local-core synthetic scenarios at each configured size passed. The earlier restricted-session failure remains recorded above. |
| Implementation run, time not captured — `bash scripts/test_gateway_transport.sh`, authorized retry | 0 | 1/1 XCTest method and 3/3 synthetic gateway scenarios passed. The earlier restricted-session failure remains recorded above. |
| Implementation run, time not captured — `bash scripts/test_ios_simulator.sh`, authorized retry | 0 | 78/78 XCTest methods passed, 0 skipped, 0 SQLite runtime warnings. Simulator evidence is not signed iPhone/Watch or real HealthKit acceptance. |
| Implementation run, time not captured — unsigned generic iOS `xcodebuild`, first restricted attempt | 74 | Dependency clone failed on DNS in the sandbox. No build pass was claimed from this attempt. |
| Implementation run, time not captured — unsigned generic iOS `xcodebuild -quiet` with `/private/tmp/boazapp-p0r-ios-dd` and `CODE_SIGNING_ALLOWED=NO`, authorized retry | 0 | Generic iOS app and test target compiled. This does not execute device tests or prove signing, HealthKit or file protection on a phone. |
| Implementation run, time not captured — final `cargo clippy --manifest-path Server/Cargo.toml --offline --locked --all-targets -- -D warnings` | 0 | Passed after final source formatting. |
| Implementation run, time not captured — final `python3 scripts/generate_xcode_project.py --check` | 0 | Project generator check passed without rewriting. |
| Implementation run, time not captured — final `python3 scripts/verify_doc_governance.py` | 0 | Governance passed at the reported source checkpoint; documentation was edited again afterward, so recheck before closure. |
| Implementation run, time not captured — final `git diff --check` | 0 | No whitespace errors at the reported source checkpoint; documentation was edited again afterward, so recheck before closure. |

Required final checks: project-generator `--check`, governed iOS simulator run, local-core and gateway-transport scripts, unsigned generic iOS build, offline locked Rust tests, Clippy `-D warnings`, `cargo fmt --check`, isolated HTTP harness, document governance, final `git diff --check`, and the Linux/native-VM fault-injection and two-generation restore drill. Record each actual command, exit code, test count, skipped count, warnings and source hash. If not run or blocked, say so explicitly; do not copy the earlier [2026-09-19 report](../2026-09-19/README.md) into this run.

## Acceptance and release boundary

The added [P0-R acceptance annex](../../TEST_CASES.md#p0-r-disaster-recovery-acceptance-annex) keeps every full disaster-recovery case open until process-level and isolated Linux evidence exists. A passing local Rust suite establishes only its exercised source behaviors. `AI-NATIVE GATE: INTERCEPT` remains the product-release conclusion; signed iPhone/Watch, real HealthKit, Tokyo private HTTPS, production encryption/off-host control head, disaster restore and operational rollback have separate gates.
