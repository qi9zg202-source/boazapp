#!/bin/sh
# Execute all 11 synthetic core scenarios at both 10k and 100k scale through XCTest.
set -eu
project_root=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
BOAZ_EXPECTED_TEST_COUNT=2 \
BOAZ_REQUIRE_MIGRATION_ROLLBACK_TEST=0 \
exec "$project_root/scripts/test_ios_simulator.sh" \
  -only-testing:boazappTests/LocalHarnessMigrationTests/testCoreScenarios10000 \
  -only-testing:boazappTests/LocalHarnessMigrationTests/testCoreScenarios100000
