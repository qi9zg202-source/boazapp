#!/bin/bash
# Run a deterministic iOS Simulator test pass and verify the xcresult evidence.
set -euo pipefail

project_root=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
device_inventory=$(mktemp /private/tmp/boaz-health-simulators.XXXXXX)
summary_json=$(mktemp /private/tmp/boaz-health-summary.XXXXXX)
tests_json=$(mktemp /private/tmp/boaz-health-tests.XXXXXX)
trap 'rm -f "$device_inventory" "$summary_json" "$tests_json"' EXIT

xcrun simctl list devices available -j >"$device_inventory"
device_udid=$(python3 - "$device_inventory" "${BOAZ_TEST_DEVICE_UDID:-}" <<'PY'
import json
import re
import sys

inventory = json.load(open(sys.argv[1], encoding="utf-8"))
preferred = sys.argv[2]
candidates = []
for runtime, devices in inventory.get("devices", {}).items():
    match = re.search(r"iOS-(\d+)(?:-(\d+))?", runtime)
    version = tuple(int(item or 0) for item in match.groups()) if match else (0, 0)
    for device in devices:
        if device.get("isAvailable", True) and ".iPhone-" in device.get("deviceTypeIdentifier", ""):
            candidates.append((version, device))

if preferred:
    selected = next((device for _, device in candidates if device.get("udid") == preferred), None)
    if selected is None:
        raise SystemExit(f"Requested iPhone simulator is unavailable: {preferred}")
else:
    booted = [(version, device) for version, device in candidates if device.get("state") == "Booted"]
    pool = booted or candidates
    if not pool:
        raise SystemExit("No available iPhone simulator was found")
    selected = max(pool, key=lambda item: (
        item[0], item[1].get("lastUsedAt", ""), item[1].get("name", "")
    ))[1]
print(selected["udid"])
PY
)

xcrun simctl boot "$device_udid" >/dev/null 2>&1 || true
xcrun simctl bootstatus "$device_udid" -b

run_id="$(date -u +%Y%m%dT%H%M%SZ)-$$"
host_arch=$(uname -m)
derived_data=${BOAZ_TEST_DERIVED_DATA:-/private/tmp/boaz-health-${run_id}-dd}
result_bundle=${BOAZ_TEST_RESULT_BUNDLE:-/private/tmp/boaz-health-${run_id}.xcresult}
test_log=${BOAZ_TEST_LOG:-/private/tmp/boaz-health-${run_id}.log}
if [ -e "$result_bundle" ]; then
    echo "Refusing to overwrite existing result bundle: $result_bundle" >&2
    exit 2
fi

source_test_count=$(python3 - "$project_root/Tests" <<'PY'
from pathlib import Path
import re
import sys

pattern = re.compile(r"^\s*func\s+test[A-Za-z0-9_]+\s*\(", re.MULTILINE)
print(sum(len(pattern.findall(path.read_text())) for path in Path(sys.argv[1]).rglob("*.swift")))
PY
)
expected_count=${BOAZ_EXPECTED_TEST_COUNT:-$source_test_count}
require_migration_test=${BOAZ_REQUIRE_MIGRATION_ROLLBACK_TEST:-1}

xcodebuild_arguments=(
  test
  -project "$project_root/boazapp.xcodeproj"
  -scheme boazapp
  -configuration Debug
  -destination "platform=iOS Simulator,id=$device_udid"
  -destination-timeout 120
  -derivedDataPath "$derived_data"
  -resultBundlePath "$result_bundle"
)
if [ -n "${BOAZ_TEST_PACKAGES_DIR:-}" ]; then
    xcodebuild_arguments+=(-clonedSourcePackagesDirPath "$BOAZ_TEST_PACKAGES_DIR")
fi
for argument in "$@"; do
    xcodebuild_arguments+=("$argument")
done
xcodebuild_arguments+=(
  -parallel-testing-enabled NO
  "ARCHS=$host_arch"
  ONLY_ACTIVE_ARCH=YES
  CODE_SIGNING_ALLOWED=NO
)

set +e
xcodebuild "${xcodebuild_arguments[@]}" 2>&1 | tee "$test_log"
# `pipefail` is enabled above, so this is nonzero if either xcodebuild or the
# log capture fails. Avoid shell-specific pipeline-status array expansion.
build_status=$?
set -e

if [ ! -d "$result_bundle" ]; then
    echo "xcodebuild produced no xcresult bundle (exit $build_status)" >&2
    exit "${build_status:-1}"
fi
xcrun xcresulttool get test-results summary --path "$result_bundle" --compact >"$summary_json"
xcrun xcresulttool get test-results tests --path "$result_bundle" --compact >"$tests_json"

python3 - "$summary_json" "$tests_json" "$expected_count" "$require_migration_test" <<'PY'
import json
import sys

summary = json.load(open(sys.argv[1], encoding="utf-8"))
tests = json.load(open(sys.argv[2], encoding="utf-8"))
expected = int(sys.argv[3])
require_migration = sys.argv[4] == "1"
actual = summary["totalTestCount"]
failed = summary["failedTests"]
skipped = summary["skippedTests"]
warnings = summary.get("runtimeWarnings", [])
if actual != expected:
    raise SystemExit(f"Executed test count mismatch: expected {expected}, xcresult reports {actual}")
if failed:
    raise SystemExit(f"xcresult reports {failed} failed tests")
if skipped:
    raise SystemExit(f"xcresult reports {skipped} skipped tests")
if warnings:
    messages = "; ".join(item.get("message", "runtime warning") for item in warnings)
    raise SystemExit(f"xcresult reports runtime warnings: {messages}")
if require_migration and "testInjectedFailureInsideV1MigrationRollsBackSchemaAnchorAndOutbox" not in json.dumps(tests):
    raise SystemExit("The Debug migration rollback test was not present in executed xcresult tests")
print(f"xcresult verified: {actual} executed, {summary['passedTests']} passed, 0 failed, 0 skipped, 0 runtime warnings")
PY

if grep -Eiq 'BUG IN CLIENT OF libsqlite3|database integrity compromised by API violation|vnode unlinked while in use' "$test_log"; then
    echo "SQLite lifecycle warning detected in simulator log: $test_log" >&2
    exit 1
fi
if [ "$build_status" -ne 0 ]; then
    echo "xcodebuild failed with exit $build_status; result: $result_bundle" >&2
    exit "$build_status"
fi

echo "Simulator UDID: $device_udid"
echo "Result bundle: $result_bundle"
echo "Test log: $test_log"
