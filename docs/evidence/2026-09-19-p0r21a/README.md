# P0-R2.1a — synthetic control-publication and backup-crash evidence

**Decision: AI-NATIVE GATE: INTERCEPT.** This record covers local synthetic behavior only. No Tokyo production connection, real HealthKit data, real upload, deployment, commit or push occurred. Independent Linux, dm-crypt volumes, an independently administered SSH custodian, a complete native-VM restore, and 10k/100k custody-latency acceptance remain unverified.

## Frozen scope and provenance

- Branch `codex/boaz-health-ios`; HEAD `480dae2dd2822cbd7facc4e840b52bb3629c8208`; existing tracked and untracked changes were preserved.
- Pre-change source SHA-256 manifest: [source-manifest.md](source-manifest.md), frozen `2026-09-19T05:10:54Z`, 116 non-ignored tracked/untracked files; pre-change tracked diff SHA-256 `10f1d5059469914e8b15c998870e2ee9300f9908ee363b828e4aead84c038c7a`.
- Host: Darwin 25.6.0 arm64; rustc 1.96.0; Xcode 27.0 (27A266a). All storage and custodians in the tests were temporary synthetic fixtures. No `BOAZ_HEALTH_*` production configuration was present.
- Final source manifest and tracked diff hash are recorded separately in [post-source-manifest.md](post-source-manifest.md). The manifest includes untracked source; `git diff` alone does not.

## What changed and what the receipts mean

1. A governed backup control intent now fixes a validated managed directory, exact database and manifest hashes, Unix file identities, schema, timestamp and control checkpoint. Replaying after a crash rechecks that evidence before local convergence and immediately before custody CAS. Missing or changed evidence leaves the original operation unresolved; it does not mint a replacement success.
2. Backup-expiry confirmation binds and rechecks the full physical/control inventory and erasure request time. Health backup first converges an already-confirmed erasure intent; prune pins the directory/file identities for deletion and refuses to confirm a substituted item.
3. Custody v2 now seals the full historical reservation/successor chain. Exact reservation retries can recover recognized, byte-identical temporary files after a process kill. Unknown, conflicting and legacy anonymous temporary files fail closed. This same-domain seal does **not** prevent a coordinated rollback of the whole custodian; an external monotonic anchor remains required.
4. The production receiver still refuses unactivated service. The isolated HTTP test uses test-only routes and cannot enable real upload. No Rust batch, receipt, pairing, revoke or erase HTTP structure was changed.

## First failures retained, then corrections

| Check | First observed result | Final scoped result |
|---|---|---|
| Custodied backup without a real artifact | `cargo test ... custodied_backup_cannot_publish_without_replayable_artifact_evidence`: exit 101; a caller-supplied 64-hex hash was wrongly accepted. | Artifact-bound intent test passes. |
| Historical custody intent rewrite | New v2 history-tamper test: exit 101; an old operation ID/hash could change while the public head stayed the same. | Required chained history seal rejects the rewrite. |
| Custody seal temporary file after interrupted publication | New seal-temp retry test: exit 101, `unknown custody root file`. | Exact reservation/bytes retry recovers; conflict remains closed. |
| Backup after confirmed erase intent but before health deletion | `backup_after_control_erasure_intent_reconciles_before_snapshot`: exit 101; snapshot still had one erased-device event. | 1/1 pass; erased-device events, receipts, outbox, audit and device rows absent from the backup. |
| Prune after validated file replaced | `prune_does_not_confirm_deletion_after_validated_file_is_replaced`: exit 101; prior code falsely confirmed deletion. | 1/1 pass; no `backup_deleted` confirmation for substituted item. |
| Rust full suite during fixture migration | Exit 101; ten API tests failed (nine fixtures lacked managed-root binding, one local-loopback sandbox denial). | Fixtures now bind the managed root; full suite passes under the approved local-loopback permission. |
| Clippy during implementation | Exit 101 for `collapsible_if`, later exit 101 for two new `main.rs` lint findings. | Final `-D warnings` rerun passes. |
| iOS/local-core/gateway initial sandbox attempts | Exit 1 before execution: CoreSimulatorService permission/connection failure. | Approved simulator reruns pass; initial failures were not counted as test passes. |

## Reproducible check ledger

Unless noted, commands ran from the repository root on the host above, using synthetic fixtures. `PASS` means only the named local check exited 0. The test suite source and the actual execution count must be considered together; no skipped test was observed in the final runs.

| Command or check | Exit / execution | Status and limit |
|---|---|---|
| `cargo test --manifest-path Server/Cargo.toml --offline --locked` | 0; 95 library + 4 binary + 8 acknowledgement-journal + 49 API + 4 process = **160/160**, 0 failed/ignored | PASS, local synthetic only. Process tests include reserve/CAS kills and equivalent temporary-file disk states followed by real forced-command retry. Not a full independent-host fsync-point matrix. |
| `cargo clippy --manifest-path Server/Cargo.toml --offline --locked --all-targets -- -D warnings` | 0 on final rerun | PASS. First failures retained above. |
| `cargo fmt --manifest-path Server/Cargo.toml -- --check` | 0 | PASS. |
| `python3 scripts/test_server_http.py --build` | 0; production unactivated refusal, adoption volume refusal, 1 synthetic route test | PASS for isolated route and fail-closed startup only. |
| `python3 scripts/generate_xcode_project.py --check` | 0 | PASS. |
| `bash scripts/test_ios_simulator.sh` | 0; **78/78**, 0 failed, 0 skipped, 0 runtime warnings | PASS, iPhone 17 Pro simulator `E9C74289-F37B-4690-BD45-DF82D2BE7EC7`; result `/private/tmp/boaz-health-20260919T053612Z-13525.xcresult`. Not signed-device evidence. |
| `bash scripts/test_local_core.sh` | 0; 2/2 10k/100k synthetic cases, 0 skipped/warnings | PASS, macOS simulator only; result `/private/tmp/boaz-health-20260919T053917Z-14746.xcresult`. Not Linux custody performance. |
| `bash scripts/test_gateway_transport.sh` | 0; 1/1 synthetic case, 0 skipped/warnings | PASS, private-URL/mock transport path; result `/private/tmp/boaz-health-20260919T054251Z-17261.xcresult`. No real upload. |
| `xcodebuild -quiet -project boazapp.xcodeproj -scheme boazapp -configuration Debug -destination 'generic/platform=iOS' -derivedDataPath /private/tmp/boaz-health-p0r21a-generic-dd CODE_SIGNING_ALLOWED=NO build-for-testing` | 0 | PASS, compile only, unsigned. |
| `python3 scripts/verify_doc_governance.py` | 0; 77 historical cases, 77 rows, 78 discovered XCTest methods | PASS for governance consistency; historical 20 PASS / 42 BLOCKED / 15 NOT_RUN unchanged. |
| `git diff --check` | 0 | PASS for tracked diff whitespace only; untracked files are in source manifests. |

## Acceptance still blocked or not run

| Claim | Status | Reason and required evidence |
|---|---|---|
| Independently owned monotonic control head and acknowledged-write custody | BLOCKED | No approved separate Linux host, pinned real SSH identity or authenticated off-host expected-head service was available. Local forced-command fixtures do not establish ownership independence or detect entire-domain coordinated rollback. |
| Separate verified dm-crypt health/control/backup domains and Tokyo storage identity | BLOCKED | This host is macOS; no approved Linux encrypted mounts or Tokyo operations were used. |
| Every filesystem/SQLite/mirror/seal/head/CAS instruction boundary and adversarial same-UID rename race | NOT_RUN | Local tests kill at selected persisted stages and reject a replaced backup; they do not exhaust all live concurrent replacement interleavings. Keep the service UID and managed directory isolated operationally. |
| 10k/100k off-host per-batch confirmation cost, p50/p95/p99, peak RSS/disk, three runs each | BLOCKED | No fixed approved Linux host or same-host correct baseline. The current forced-command path re-enumerates and verifies history per request; source inspection indicates superlinear cumulative work. The macOS client 10k/100k numbers above cannot validate this cost. |
| Native VM rebuild/readback, active pointer, second-generation recovery, signed iPhone/Watch and real HealthKit | BLOCKED | P0-R2.2 and separate release gates; this slice did not implement or accept them. |

**Operational decision:** keep `serve` and real upload closed. Continue P0-R2.2 only after separate custody/volume and performance evidence, or retain `INTERCEPT` with the exact missing proofs. No historical `2026-09-18` or `2026-09-19` evidence was rewritten.
