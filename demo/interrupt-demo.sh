#!/usr/bin/env bash
# Interrupt a runner while it is waiting for the load to drop (as systemd stop
# or `timeout` would). Control returns at once; the job is still pending.
set -u
cd "$(dirname "$0")/.."
S=./bin/slothqueue
DB=demo/wait.db

$S add 'echo "nightly reindex"' --db "$DB"
$S run --db "$DB" --load-ceiling 1.5 --poll 2 &
RUNNER=$!
sleep 3
kill -TERM "$RUNNER"
wait "$RUNNER"
echo "exit=$?"
$S list --db "$DB"
