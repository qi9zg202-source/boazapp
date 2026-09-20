# Boaz Health P0-R2.1 evidence — 2026-09-19

Status at 2026-09-19 04:36 UTC: source implementation and local synthetic verification completed for this slice; independent-environment acceptance remains blocked. No independent Linux custodian, dm-crypt deployment, native VM activation or Tokyo access was exercised. AI-NATIVE GATE: INTERCEPT.

## Frozen authority

- Branch: codex/boaz-health-ios; HEAD: 480dae2dd2822cbd7facc4e840b52bb3629c8208.
- Pre-change [source manifest](source-manifest.md): 112 tracked/untracked non-ignored files. Pre-change tracked dirty diff SHA-256: 944258543733ab88a2b7f7d8f380bb01399e7b40dfcb120ce499b37f91fb802e.
- Post-change [source manifest](post-source-manifest.md): 113 tracked/untracked non-ignored files outside this self-referential evidence directory; manifest SHA-256 ad0fbc71521ff13cc3d9bd2b157ab26fb6571912b3ddcf8ce4525d4a3a1967ce. Final tracked dirty diff SHA-256: 10f1d5059469914e8b15c998870e2ee9300f9908ee363b828e4aead84c038c7a.
- The 26 inventoried files under historical docs/evidence/2026-09-18/ and docs/evidence/2026-09-19/ matched their pre-change SHA-256 values after this run.
- This run used synthetic records only. Existing uncommitted and untracked work was preserved. Historical 2026-09-18 and 2026-09-19 evidence was not edited.
- Host class: macOS/Darwin arm64 with Xcode simulator and local Rust. It cannot attest an independent Linux custodian or dm-crypt volume.

## Actual command ledger

| Command / check | Status | Observed evidence |
|---|---|---|
| Initial python3 scripts/test_server_http.py --build | FAIL during concurrent implementation | custody.rs had incomplete in-progress functions; retained as a first-run failure, not a product pass. |
| Initial cargo test --manifest-path Server/Cargo.toml --offline --locked | FAIL in default sandbox | 79/79 library, 4/4 binary and 6/6 then-current ack tests passed; 45/46 API tests passed. The new local TCP test failed on loopback bind EPERM. No test was skipped to hide this. |
| Focused local TCP test with reviewed loopback permission | PASS | 1/1 synthetic route test, 0 failed/ignored. No Tokyo or external network call. |
| Initial cargo clippy --all-targets -- -D warnings | FAIL during integration | New test fixture had an unread field; corrected and rerun. |
| cargo test --manifest-path Server/Cargo.toml --offline --locked with reviewed loopback permission | PASS | 79 library + 4 binary + 8 acknowledgement-journal + 47 API tests, 0 failed/ignored; includes real local TCP test. |
| cargo clippy --manifest-path Server/Cargo.toml --offline --locked --all-targets -- -D warnings | PASS | Exit 0, no warnings. |
| cargo fmt --manifest-path Server/Cargo.toml -- --check | PASS | Exit 0. |
| python3 scripts/test_server_http.py --build with reviewed loopback permission | PASS | Production binary refused unactivated serve with DB SHA-256 unchanged and no listener; adopt-active-set rejected an unverified volume without changing DBs or publishing a marker; test-only Rust TCP fixture exercised synthetic routes. This is not production startup acceptance. |
| python3 scripts/generate_xcode_project.py --check | PASS | Generated Xcode project current. |
| python3 scripts/verify_doc_governance.py | PASS after documentation completion | Historical 77/77 register unchanged; 78 XCTest methods discovered. |
| git diff --check | PASS after documentation completion | No tracked whitespace error. |
| sed -n '9,121p' docs/evidence/2026-09-19-p0r21/post-source-manifest.md \| shasum -a 256 -c - | PASS | 113/113 inventoried source files matched; 0 failed. Current evidence files are excluded from this manifest to avoid self-reference. |
| bash scripts/test_ios_simulator.sh | PASS | iPhone 17 Pro simulator E9C74289-F37B-4690-BD45-DF82D2BE7EC7; 78 discovered/78 executed, 0 failed, 0 skipped, 0 SQLite runtime warnings. Result: /private/tmp/boaz-health-20260919T042352Z-93007.xcresult. No physical iPhone or HealthKit entitlement acceptance. |
| bash scripts/test_local_core.sh | PASS | 2/2 XCTest methods, 11/11 synthetic cases each at 10k and 100k, 0 failed/skipped/SQLite runtime warnings. Result: /private/tmp/boaz-health-20260919T042718Z-93902.xcresult. Mac numbers are not a Linux recovery baseline. |
| bash scripts/test_gateway_transport.sh | PASS | 1/1 XCTest, 3/3 synthetic gateway scenarios, 0 failed/skipped/runtime warnings. Result: /private/tmp/boaz-health-20260919T043038Z-94787.xcresult; no remote call. |
| xcodebuild -quiet -project boazapp.xcodeproj -scheme boazapp -configuration Debug -destination generic/platform=iOS -derivedDataPath /private/tmp/boaz-health-p0r21-generic-dd CODE_SIGNING_ALLOWED=NO build-for-testing | PASS | Exit 0; compiled app and tests unsigned, did not execute on a signed iPhone. |

## Scope and open gates

- The v2 custody protocol and guarded control/acknowledgement flows have synthetic tests. The offline adoption verifier accepts only an empty genesis ledger and a matching managed snapshot. The adoption command publishes only adopted-unactivated.json, never an active-set pointer.
- A backup/restore control reservation made before its SQLite commit deliberately fails closed if the external artifact cannot be independently revalidated; automatic repair for every such boundary is not claimed.
- No independent host, encrypted-volume identity, real SSH key/host identity, process kill-9 matrix, 10k/100k fixed-Linux latency baseline, native VM rebuild, live-port readback, cutover or second-generation restore is accepted. These remain BLOCKED or NOT_RUN.
- The current v2 custodian scans its complete history per operation; repeated per-batch confirmation may create superlinear cumulative cost. Throughput and a 15% regression conclusion are NOT_RUN until fixed-host measurements.
- The production serve path still refuses to launch. Nothing here enables real upload, deploys, commits or pushes.
