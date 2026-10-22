#!/usr/bin/env bash
# End-to-end crash-recovery demo: kill the worker with SIGKILL mid-job, then
# show that the next run reclaims the orphaned row.
set -u
cd "$(dirname "$0")/.."
S=./bin/slothqueue
DB=demo/demo.db

echo "=== list ==="
$S list --db "$DB"
echo "=== status ==="
$S status --db "$DB"

echo
echo "=== crash recovery ==="
$S add 'sleep 2; echo "recovered: lockfile swept"' --priority 0 --max-runtime 10 --db "$DB"
$S run --db "$DB" --load-ceiling 0 --cooldown 0 > demo/crashed-runner.log 2>&1 &
RUNNER=$!
sleep 0.6
kill -9 "$RUNNER"
wait "$RUNNER" 2>/dev/null
echo "runner pid $RUNNER killed with SIGKILL"
echo "--- status after crash ---"
$S status --db "$DB"
echo "--- run again ---"
$S run --db "$DB" --load-ceiling 0 --cooldown 0
echo "exit=$?"
echo "--- final list ---"
$S list --db "$DB"
