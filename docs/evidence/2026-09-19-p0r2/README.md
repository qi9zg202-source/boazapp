# Boaz Health P0-R2 implementation evidence — 2026-09-19

Status at 2026-09-19 03:46 UTC: **partial implementation; no activated recovery generation or isolated Linux acceptance. AI-NATIVE GATE: INTERCEPT.** Synthetic data only. No Tokyo production access, real upload, deployment, commit or push. Historical 2026-09-18 and 2026-09-19 evidence remains unchanged.

## Frozen source and environment

- Pre-implementation [source manifest](source-manifest.md): 106 tracked and untracked non-ignored files; SHA-256 `3d740a624b3f7e1b09789599f36df692509d8c8a60786bbdb6728f460ab6fd8e`.
- Branch `codex/boaz-health-ios`; HEAD `480dae2dd2822cbd7facc4e840b52bb3629c8208`; pre-edit dirty tracked diff SHA-256 `a0e199e80680a50805e06114b19043abeca01c73167b42da8bee7c521dabbf3e`.
- Host `Darwin 25.6.0 arm64`, Rust/Cargo `1.96.0`, Xcode `27.0 (27A266a)`. This is **not** the required independent Linux/dm-crypt/native-VM host.
- Post-edit [source inventory](post-source-manifest.md): 109 tracked/untracked non-ignored files outside this evidence directory, SHA-256 `3f4effeaff141e0117a02aea1d0772772bc38f1471ec30b3ae00cb695bcfae2f`; final tracked dirty diff SHA-256 at 03:46 UTC `944258543733ab88a2b7f7d8f380bb01399e7b40dfcb120ce499b37f91fb802e`. Files in this evidence directory are excluded to avoid a self-referential hash. Existing dirty files were preserved.

## Command ledger (append actual observations; do not erase failures)

| Command | Exit/status | Observation and scope |
|---|---|---|
| `git status --short --branch`, `git rev-parse HEAD`, `git diff --check` before edits | 0 | Dirty worktree confirmed; no whitespace error at freeze. |
| `cargo test --manifest-path Server/Cargo.toml --offline --locked --test api` during integration | 101 | 4/45 passed, 41 failed because a test fixture used macOS `/var` symlink as coordinator parent. This was a test-path error, not a server acceptance. |
| `cargo test --manifest-path Server/Cargo.toml --offline --locked --test api` after canonical temp-path fix | 101 | 41/45 passed, 4 failed on old error-text expectations intercepted earlier by the new coordinator gate. |
| `cargo test --manifest-path Server/Cargo.toml --offline --locked --test api` after updating those negative assertions | 0, with warnings | 45/45 passed, 0 ignored. Compilation still reported three work-in-progress `custody.rs` unused-import warnings; not a zero-warning final check. |
| `python3 scripts/generate_xcode_project.py --check` | 0 | Generated project matches source at this checkpoint. |
| `python3 scripts/verify_doc_governance.py` | 0 | 77 case definitions/status rows and 78 discovered XCTest methods; discovery is not execution. |
| `git diff --check` | 0 | No whitespace error at this checkpoint; rerun after final edits. |
| `bash scripts/test_local_core.sh` restricted first attempt | 1 | CoreSimulatorService/simdiskimaged connection invalid/refused. **Not PASS**; retry outside the restricted simulator boundary if authorized. |
| `bash scripts/test_local_core.sh` with simulator access | 0 | 2 XCTest methods, 11/11 synthetic scenarios at 10k and 100k, 0 failed/skipped/runtime warnings; result `/private/tmp/boaz-health-20260919T033010Z-69800.xcresult`. This is a macOS simulator measure, not Linux recovery. |
| `bash scripts/test_gateway_transport.sh` restricted first attempt | 1 | CoreSimulatorService refused the restricted process. **Not PASS**. |
| `bash scripts/test_gateway_transport.sh` with simulator access | 0 | 1 XCTest method, 3/3 synthetic transport scenarios; 0 failed/skipped/runtime warnings; result `/private/tmp/boaz-health-20260919T033557Z-74518.xcresult`. No remote call. |
| `cargo test --manifest-path Server/Cargo.toml --offline --locked` during recovery integration | 101 | 62/63 library tests passed; one final-volume recovery fixture failed because its directory was not private. The fixture was corrected and tested again. |
| `cargo test --manifest-path Server/Cargo.toml --offline --locked` final code pass | 0 | 66 library + 4 binary + 6 acknowledgement-journal + 45 API tests passed; 0 failed/ignored. Includes no-DROP migration, exact-snapshot replay and failure-closed checks. No native Linux VM was run. |
| `cargo clippy --manifest-path Server/Cargo.toml --offline --locked --all-targets -- -D warnings` | 0 | Completed without warnings. Earlier in-progress recovery code had unused-item warnings; those were removed before this check. |
| `cargo fmt --manifest-path Server/Cargo.toml -- --check` first attempt | 1 | Formatting differences in newly edited Rust files. Ran `cargo fmt`; subsequent check exited 0. |
| `cargo fmt --manifest-path Server/Cargo.toml -- --check` after formatting | 0 | No remaining formatting difference at the checked revision. |
| `python3 scripts/test_server_http.py --build` restricted first attempt | 1 | Local socket creation denied by the sandbox. Not an application pass. |
| `python3 scripts/test_server_http.py --build` with loopback access | 1 | Legacy script's `init-storage` failed on missing mandatory coordinator. The production fallback must not be reopened to make this harness pass; the isolated HTTP harness needs a separate adapted fixture. |
| `bash scripts/test_ios_simulator.sh` with simulator access | 0 | 78 discovered XCTest methods, 78 executed, 0 failed/skipped/runtime warnings; result `/private/tmp/boaz-health-20260919T033844Z-75866.xcresult`. |
| `xcodebuild -project boazapp.xcodeproj -scheme boazapp -configuration Debug -destination 'generic/platform=iOS' -derivedDataPath /private/tmp/boaz-health-p0r2-generic-dd CODE_SIGNING_ALLOWED=NO build-for-testing` restricted first attempt | 74 | Swift package checkout attempted a blocked GitHub lookup; not a build pass. |
| Same unsigned generic iOS build with Xcode cache/developer-service access | 0 | `TEST BUILD SUCCEEDED`; compiles app and tests, does not execute on signed device. |
| `python3 scripts/generate_xcode_project.py --check`, `python3 scripts/verify_doc_governance.py`, `git diff --check` final checks | 0 each | Generated project current; documentation has 77/77 case/status entries and 78 discovered XCTest methods; no tracked whitespace error. |
| Final Rust full test, Clippy and fmt rerun after receipt hardening | 0 each | Same 66+4+6+45 tests pass; strict lint and format pass after the final code change. |
| `sed -n '7,115p' docs/evidence/2026-09-19-p0r2/post-source-manifest.md \| shasum -a 256 -c -` | 0 | All 109 inventoried files matched their recorded SHA-256. |
| Pre/post manifest comparison for `docs/evidence/2026-09-18/` and `docs/evidence/2026-09-19/` | 0 | No historical evidence file hash changed. |

The Mac simulator's single 100k run measured 29.436 s import, 76.118 s batch preparation, 112,084,264 SQLite/WAL bytes and 348,798,976 peak resident bytes. This is **not** the required three-run fixed Linux recovery baseline; no Linux RTO/RPO or 15% regression conclusion can be drawn. The fixture generator is deterministic and has no random seed parameter.

## Open acceptance

- No approved independent Linux host, separately verified dm-crypt recovery volumes or off-host control-head custody has been exercised in this run. Native VM full restore, kill-9 boundaries, second-generation restore and 10k/100k three-run baseline are `BLOCKED`/`NOT_RUN`, not PASS.
- Current strict coordinator gate deliberately refuses an unadopted `serve`. Until the external custody and live-VM readback launch path are complete, a local journal/pointer alone must not open receiver writes.
- The acknowledgement journal and exact-adoption-snapshot replay now have synthetic tests, but there is no independently attested baseline-adoption CLI, routine control-event custody CAS, verified restore-rebuild/activate/resume CLI, live-port VM readback or second-generation source anchor. These are implementation gaps, not environment-only blockers. A later snapshot or pre-journal missing write remains a hard stop.
- A prepared/confirmed pairing record has only token hashes. A crash after SQLite pairing commit but before journal confirmation cannot reissue the plaintext token; the client must pair again, and no seamless recovery is claimed.
