#!/bin/sh
# Execute all three synthetic gateway scenarios with an in-process URLProtocol.
set -eu
project_root=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
BOAZ_EXPECTED_TEST_COUNT=1 \
BOAZ_REQUIRE_MIGRATION_ROLLBACK_TEST=0 \
exec "$project_root/scripts/test_ios_simulator.sh" \
  -only-testing:boazappTests/LocalHarnessMigrationTests/testGatewayScenarios
