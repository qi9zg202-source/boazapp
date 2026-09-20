# Tokyo deployment preflight — 2026-09-18

**Decision: keep upload disabled.** This is a read-only observation of the currently reachable `boaz-tokyo-vps` host. It does not authorize or perform production changes.

## Verified current state

| Gate | Observation | Result |
|---|---|
| Private host access | Tailscale control plane reported the Tokyo peer online; read-only SSH over its Tailscale IP returned Linux and root identity. | Reachable for inspection |
| Native VictoriaMetrics | Port `127.0.0.1:8428` is owned by `docker-proxy` for a container named `victoriametrics`. The container runs `victoriametrics/victoria-metrics:latest`, binary version `v1.151.0`. Its bind-mounted data is `/opt/boaz/victoria-metrics-data` (57 MiB at inspection). | **Fail** |
| Separate health storage | `/opt/boaz-health/data`, `/opt/boaz-health/victoria-metrics-data`, and `/opt/boaz-health/backups` do not exist. The present VM data sits on the root `ext4` filesystem. | **Fail** |
| Encryption tooling | The host showed one system disk and no `cryptsetup` binary. No dedicated encrypted volume was visible. | **Fail** |
| Private HTTPS | `tailscale serve status` returned `No serve config`. | **Fail** |
| Receiver | `boaz-health.service` was inactive. | Not deployed |
| Restore and phone checks | No encrypted backup restore, physical iPhone/Watch run, or signed installation was performed. The Mac's CoreDevice service times out and its simulator runtime is older than Xcode requires. | **Unverified** |

The receiver's sample environment keeps every upload gate at `0`. Its runtime check rejects pairing and batch ingest if the native VM or encrypted mounts fail, including after startup. No real HealthKit data has been sent.

## Proposed change sequence and rollback

1. **Choose storage and key custody.** Provision encrypted storage for the health ledger, native VM, and managed backups. The key must be managed outside the data it protects. Decide whether a provider-managed encrypted volume or an operator-unlocked LUKS volume is appropriate; document boot recovery and key rotation. Verify mounts and a complete backup restore before opening ingest.
2. **Prepare native VM without touching existing data.** Pin a native VictoriaMetrics binary matching the observed `v1.151.0` format. Snapshot and verify the 57 MiB existing VM data, copy to the chosen encrypted storage, then schedule a short cutover. Stop only the `victoriametrics` container, start the native binary on loopback `8428`, and verify process path, storage argument, listener ownership, and representative query/export readback. Keep the original container configuration and data unchanged until acceptance. **Rollback:** stop native VM and restart the original container against its untouched data; leave health ingest closed.
3. **Install the receiver closed.** Build the Rust service on a verified Linux toolchain, install under a dedicated `boaz-health` account, use a separate encrypted SQLite path, and start with `BOAZ_HEALTH_UPLOAD_ENABLED=0`. Check loopback binding, receipt database integrity, log redaction, and service restart behavior. **Rollback:** stop the service; no existing Boaz record path is changed.
4. **Establish recovery and private access.** Schedule encrypted managed backups and pruning, then restore one snapshot into an isolated path and compare integrity and counts. Add a tailnet rule restricted to the approved phone, configure Tailscale Serve HTTPS with no Funnel, and verify that unauthorized peers cannot pair or post. **Rollback:** remove Serve and return all upload flags to `0`.
5. **Device acceptance before consent.** Repair the Mac's CoreDevice/Xcode mismatch or use a working signing host, install on the physical iPhone, and inspect Health permissions, history paging, locked-phone/background behavior, accessibility, haptics, offline retry, duplicate response recovery, erasure, and Tailscale disconnect. Measure import time and storage. Only then generate a one-time pairing code and ask for separate in-app upload consent.

Production cutover requires an explicit decision on encrypted volume and key custody, a maintenance window for the existing VM migration, a successful rollback drill, and live phone/Tokyo evidence. **AI-NATIVE GATE: INTERCEPT** until those receipts exist.
