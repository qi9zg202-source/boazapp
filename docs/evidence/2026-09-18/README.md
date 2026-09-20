# Acceptance evidence — 18 September 2026

Read the [acceptance report](../../ACCEPTANCE_REPORT_2026-09-18.md) for the decision and limitations, and [test cases](../../TEST_CASES.md) for the expected observations. These files describe a development run, not production approval.

| Artifact | Meaning |
|---|---|
| `source-manifest.json` | Base commit, branch, exact source/test file hashes, and an aggregate hash for the tested working tree. Includes new files that have not been committed. |
| `results.json` | Executed commands, results, environment, measurements, limitations and artifact hashes. |
| `case-results.csv` | One disposition per acceptance case. Related unit evidence does not make an incomplete case pass. |
| `ios-summary.json`, `ios-tests.json`, `ios-action.json` | Unmodified `xcresulttool` exports from the final passing simulator run, including every test name/result and the action log. The separate generic device build only compiles. |
| `ios-accepted-final.log` | Complete command output from the acceptance task's final combined simulator run. |
| `ios-device-build.log` | Unsigned generic iPhone app and test-bundle build, including Xcode tool warnings. |
| `local-core.log`, `local-core-100k.log` | Production Swift ledger logic with synthetic records in disposable Mac databases. The measurements are not phone or network benchmarks. |
| `gateway-transport.log` | Production client request construction exercised through an in-process URL protocol. No DNS, TLS, tailnet or remote server is involved. |
| `server-tests.log`, `server-clippy.log`, `server-release.log` | Rust request/storage tests, static checks and native macOS release build. |
| `server-concurrency-repeat.log` | Ten repeated runs each of the duplicate-batch and single-use-pairing concurrency cases. |
| `server-http.json`, `server-http-build.log` | Real temporary loopback receiver process, upload disabled, seven checks and cleanup. |
| `dashboard-empty.png` | Inspected empty dashboard; no personal Health data. Does not establish completed consent/settings/audit interactions. |
| `tokyo-preflight.log` | Read-only host observations. Missing private HTTPS, inactive receiver and Docker metrics listener block deployment acceptance. |

Full Xcode `.xcresult` bundles and build products remain under `/private/tmp`; that location is temporary and is not a backup. The retained Xcode JSON exports include test names, final outcomes and the action log; the command output is retained separately. Preserve a new result bundle separately when reproducing the run.

## Verify the source and artifact hashes

From the repository root:

```sh
python3 - <<'PY'
from pathlib import Path
import hashlib, json

root = Path.cwd()
evidence = root / 'docs/evidence/2026-09-18'
manifest = json.loads((evidence / 'source-manifest.json').read_text())
results = json.loads((evidence / 'results.json').read_text())
failures = []
for relative, expected in manifest['files'].items():
    path = root / relative
    if not path.is_file() or hashlib.sha256(path.read_bytes()).hexdigest() != expected:
        failures.append('Source changed or missing: ' + relative)
for relative, expected in results['artifact_sha256'].items():
    path = evidence / relative
    if not path.is_file() or hashlib.sha256(path.read_bytes()).hexdigest() != expected:
        failures.append('Evidence changed or missing: ' + relative)
if failures:
    raise SystemExit('\n'.join(failures))
print('Recorded source and evidence hashes match.')
PY
```

A later source edit should make this check fail until a new verification run is recorded. Hash agreement confirms file identity, not test completeness, encryption, device behavior or deployment readiness. Do not update hashes alone to make changed code appear tested.
