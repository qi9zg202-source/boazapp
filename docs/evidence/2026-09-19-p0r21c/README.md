# P0-R2.1c local implementation and acceptance record

Recorded 2026-09-19 06:36:55 UTC. Source identity: `codex/boaz-health-ios`, HEAD `480dae2dd2822cbd7facc4e840b52bb3629c8208`; pre-change inventory and hashes are in `source-manifest.md`. The working tree was already dirty and remains uncommitted. This record does not replace the 2026-09-18 or earlier 2026-09-19 evidence.

## Scope and result

`AI-NATIVE GATE: INTERCEPT`. Only local code and synthetic fixtures were changed or tested. No Tokyo connection, real health data, real upload, deployment, commit or push occurred. The `/boaz` local skill was queried for an authorized isolated Linux receiver/custodian/witness. Its context was degraded and its historical records did not establish current authorization for these P0-R2.1c hosts. No live-host or performance claim is made.

The SSH custodian now fixes `/usr/bin/ssh`, rejects receiver-writable or linked executable/credential paths and ancestors, clears inherited environment, disables agent identity, and bounds ordinary I/O plus caller-side child cleanup. These checks do **not** prove Linux ACL/mount identity, privileged replacement resistance, or a strict wall-clock bound for uninterruptible kernel work. A false-success CAS test first demonstrated premature deletion of a durable acknowledgement intent. The handler now requires a separate exact remote read before clearing that intent. Existing HTTP structures are unchanged; production `serve` remains gated off.

## Executed checks (first failure retained)

| Check / command | Outcome and evidence limit |
|---|---|
| Focused custody unit tests | Fail-first test exit 101 before SSH hardening; after implementation 27/27 PASS. Local process/file simulations only. |
| `false_cas_success_must_keep_original_intent_until_remote_readback` | First run exit 101, 0/1: fake CAS success cleared the original intent. After the exact-read fix exit 0, 1/1; full API suite 61/61 PASS, 0 skipped. |
| `cargo test --manifest-path Server/Cargo.toml --offline --locked` | First sandbox run exit 101: 60/61 API tests passed; loopback bind denied by the sandbox. Retried with local loopback permission: exit 0, 191/191 tests across five binaries, 0 failed, 0 ignored. Counts from `cargo test ... -- --list`: 105 + 11 + 8 + 61 + 6. Not a Linux/SSH acceptance result. |
| `cargo clippy --manifest-path Server/Cargo.toml --offline --locked --all-targets -- -D warnings` | Exit 0. |
| `cargo fmt --manifest-path Server/Cargo.toml -- --check` | One earlier new-test formatting check exited 1; formatted those lines, final exit 0. |
| `python3 scripts/test_server_http.py --build` | First sandbox run exit 1 (`Operation not permitted` on localhost); local-only permission rerun exit 0. Production unactivated startup rejected without storage mutation, unverified adoption rejected, and synthetic loopback routes passed. |
| `python3 scripts/generate_xcode_project.py --check` | Exit 0; generated project current. |
| `bash scripts/test_ios_simulator.sh` | First sandbox run exit 1 because CoreSimulatorService was inaccessible. First escalated run exit 1: two high-volume cases crashed or timed out while three simulator jobs were concurrent. A serial script retry was interrupted after dependency submodule checkout stalled for over two minutes. Exact same suite then ran serially with the already built local package cache; `xcodebuild test` exit 0, xcresult `78/78` passed, `0` failed, `0` skipped, `0` runtime warnings, on iPhone 17 Pro simulator iOS 26.5. The first failures remain part of this evidence, and concurrency/resource causation is not proven. Final bundle: `/private/tmp/boaz-health-20260919T0635-serial.xcresult`. |
| `bash scripts/test_local_core.sh` | First sandbox attempt exit 1 (CoreSimulator unavailable); escalated concurrent run exit 1 because the 100k test crashed or timed out. That test passed in the later serial full-suite xcresult, but the standalone script was not itself rerun to a green exit. |
| `bash scripts/test_gateway_transport.sh` | First sandbox attempt exit 1; escalated exit 0 with 1/1 XCTest and 3/3 synthetic transport scenarios, 0 skipped/warnings. |
| `xcodebuild -quiet ... -destination 'generic/platform=iOS' ... CODE_SIGNING_ALLOWED=NO build-for-testing` | Exit 0 using the completed local package cache. This is compile evidence only, not signed-device evidence. |
| `python3 scripts/verify_doc_governance.py`; `git diff --check` | Both exit 0; governance reports 77 cases/status rows and 78 discovered XCTest methods. |

The successful serial simulator xcresult was independently read with `xcrun xcresulttool get test-results summary --path /private/tmp/boaz-health-20260919T0635-serial.xcresult`: 78 total, 78 passed, 0 failed, 0 skipped, empty `runtimeWarnings`. No Mac result is a substitute for physical iPhone/Watch or Linux recovery.

## Identity and source hashes

Host: Darwin arm64, Xcode 27.0 (27A266a), Rust/Cargo 1.96.0. Final recorded tracked-diff SHA-256: `5f286adc5090a585da82c9f66ce78beda7663bb60009f3c5aff4a30c043e292b`. Final digest of the sorted `git ls-files -co --exclude-standard` SHA-256 manifest, excluding this evidence directory: `d768d0b557fa9cc529637c1e5005eb443b6e8618103c7f7bbb0015041b4094d7`.

Changed-file SHA-256: `Server/src/custody.rs` `f0e5de08c6f77f228d9674326b3055c17ef840ed6d1db3f03c8304dd74e31d29`; `Server/src/lib.rs` `f37f0e9664eb39364ee537b7832de47653bdc2feb59cd18b99d5aa3b07b6cab6`; `Server/tests/api.rs` `ee5dda51df091ca9e31663300b7a110d61597a0dde72361b629fb72e1c9c173e`; `design.md` `3a9fd69510f5292788bb22c60e119ca5ebaffe43ed124b111ddc4c34973060f9`; `memory.md` `0bed2e19773fdd55fe3ce7bdd3e08838a7bb10d88f4bbeafe769cf83ed88aef4`; `Server/README.md` `77b47a44b22c5a6a9fab6ae63131a41ca84da625c698a83a5ed595cdf25899b0`; `docs/TEST_CASES.md` `769e30d18b57dd9617d1b422ef832a436d813c7d8f5cc43406b8ffc9f8afe203`.

## Blocked release evidence and next gate

`BLOCKED`: independently authorized Linux receiver, independently operated custodian and monotonic witness, separate encrypted domains, actual SSH binary/key/known-host consumption, kill/restart at durable boundaries, and 10k/100k three-run p95/p99, resource and network measurements against a pre-frozen same-host baseline. No approved host identity, owner, allowed test scope or performance target was provided. The existing forced-command store still scans its full history for each request; per-batch remote confirmation cost is therefore unmeasured. Do not optimize by weakening complete-chain verification without a measured trigger and replacement proof. P0-R2.2 native VM activation, second-generation restore, signed devices and Tokyo production remain separate gates.
