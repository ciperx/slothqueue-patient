#!/usr/bin/env bash
# Rebuild demo/demo.db from scratch and run the three-job example.
# Takes ~20s on a loaded machine because the queue waits for the load ceiling.
set -u
cd "$(dirname "$0")/.."
S=./bin/slothqueue
DB=demo/demo.db

rm -f "$DB" "$DB"-wal "$DB"-shm

$S add 'python3 -c "import time; time.sleep(30)"' --priority 9 --max-runtime 2 --db "$DB"
$S add 'echo "rebuilt search index: 412 documents"' --priority 5 --db "$DB"
$S add 'python3 -c "import sys; sys.stderr.write(\"no space left on device\\n\"); sys.exit(3)"' --priority 1 --db "$DB"

echo "=== list ==="
$S list --db "$DB"

echo "=== run (ceiling 25, cooldown 1s) ==="
$S run --db "$DB" --load-ceiling 25 --cooldown 1 --poll 1
echo "exit=$?"

echo "=== status ==="
$S status --db "$DB"
echo "=== list ==="
$S list --db "$DB"
