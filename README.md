# slothqueue

A patient job queue for one machine. You hand it shell commands; it runs them
one at a time, only when the machine is quiet enough to deserve them.

One SQLite file, one process, no broker, no daemon, no network, no dependencies
outside the Python standard library.

```console
$ ./bin/slothqueue add 'echo "rebuilt search index: 412 documents"' --priority 5 --db demo/hello.db
added job #1 (priority 5, max runtime 300s)
$ ./bin/slothqueue run --db demo/hello.db --load-ceiling 25 --poll 1
slothqueue: load ceiling 25.00, cooldown 5.0s, poll 1s
  load 12.23 < ceiling 25.00 — starting
  job #1 (pri 5): echo "rebuilt search index: 412 documents"
rebuilt search index: 412 documents
  job #1 done in 0.01s
finished: 1 done, 0 failed
```

The `load … < ceiling … — starting` line is the whole idea: the job did not
start until the machine said it was ready. Later sections show what happens when
it is not.

## Why patience

Every job runner I have used assumes the machine is yours alone. `cron` fires at
a wall-clock minute whether you are on a video call or not; `make -j` and
`xargs -P` saturate every core because that is what they were asked to do. On a
laptop, a CI box shared by three people, or a small VPS that also serves your
site, "start it now" is how a background task becomes the reason your editor
stutters.

The 1-minute load average is a cheap, honest signal of whether anyone else is
working. slothqueue reads it before each job:

- **Above the ceiling** (default `1.5`): sleep for `--poll` seconds and look
  again. No spinning, no retry storms — just sampling.
- **Below the ceiling**: run exactly one job, wait `--cooldown` seconds, then
  look at the load again. The cooldown is not cosmetic: finishing a job leaves
  page cache and IO queues busy for a moment, and the load average is a moving
  average that takes time to reflect the drop.

A queue that waits is not a queue that is broken. It is a queue that has decided
your interactive work comes first, and it leaves a record of every job it has
ever touched so you can see what happened while you were away.

## Install

Nothing to install. Python 3.9+ on Linux or macOS (it uses `os.getloadavg`,
POSIX process groups and `SIGKILL`; Windows is not supported).

```console
$ git clone <this repo> slothqueue && cd slothqueue
$ ./bin/slothqueue --help
```

`bin/slothqueue` puts the checkout on `sys.path` itself, so there is no venv and
no packaging step. `python3 -m slothqueue ...` works identically from the repo
root. If you want it on your `PATH`:

```console
$ ln -s "$PWD/bin/slothqueue" ~/.local/bin/slothqueue
```

## Usage

### Queue work

```console
$ S=./bin/slothqueue
$ $S add 'python3 -c "import time; time.sleep(30)"' --priority 9 --max-runtime 2 --db demo/demo.db
added job #1 (priority 9, max runtime 2s)
$ $S add 'echo "rebuilt search index: 412 documents"' --priority 5 --db demo/demo.db
added job #2 (priority 5, max runtime 300s)
$ $S add 'python3 -c "import sys; sys.stderr.write(\"no space left on device\\n\"); sys.exit(3)"' --priority 1 --db demo/demo.db
added job #3 (priority 1, max runtime 300s)
```

Higher `--priority` runs first; ties are FIFO by id. `--max-runtime` is
per-job, in seconds, default 300.

### Inspect

```console
$ $S list --db demo/demo.db
ID  PRI  STATE    EXIT  DURATION  ATT  COMMAND
1   9    pending  -     -         0    python3 -c "import time; time.sleep(30)"
2   5    pending  -     -         0    echo "rebuilt search index: 412 documents"
3   1    pending  -     -         0    python3 -c "import sys; sys.stderr.write(\"no...

$ $S status --db demo/demo.db
database : /opt/data/profiles/idea/work/slothqueue/demo/demo.db (16.0 KiB)
pending  : 0
running  : 0
done     : 1
failed   : 2
total    : 3
next up  : -
last done: 2026-10-22T20:43:54Z
```

`list --json` and `status --json` emit machine-readable rows for scripting.

### Run

The load average on this machine was ~26 while writing this, so the first real
run waited for it to drop under a ceiling of 25 (a later rerun at load ~17 went
straight through — `bash demo/run-demo.sh` reproduces the run):

```console
$ $S run --db demo/demo.db --load-ceiling 25 --cooldown 1 --poll 1
slothqueue: load ceiling 25.00, cooldown 1.0s, poll 1s
  load 26.59 >= ceiling 25.00 — waiting 1s
  load 26.38 >= ceiling 25.00 — waiting 1s
  ...                                   # 14 more samples, one per second
  load 25.24 >= ceiling 25.00 — waiting 1s
  load 24.42 < ceiling 25.00 — starting
  job #1 (pri 9): python3 -c "import time; time.sleep(30)"
  job #1 FAILED in 2.04s (timeout: exceeded max runtime of 2s (process group killed))
  cooling down 1.0s
  load 24.42 < ceiling 25.00 — starting
  job #2 (pri 5): echo "rebuilt search index: 412 documents"
rebuilt search index: 412 documents
  job #2 done in 0.05s
  cooling down 1.0s
  load 24.42 < ceiling 25.00 — starting
  job #3 (pri 1): python3 -c "import sys; sys.stderr.write(\"no space left on device\\n\"); sys.exit(3)"
no space left on device
  job #3 FAILED in 0.07s (exit code 3)
finished: 1 done, 2 failed
$ echo $?
1
```

The child inherits the runner's stdout/stderr, so job output appears in the
transcript in real time and nothing is buffered in the database.

With a ceiling the machine never meets, it simply waits — and stops cleanly when
you interrupt it (Ctrl-C, `SIGTERM`, or `timeout`), leaving the job untouched:

```console
$ $S run --db demo/wait.db --load-ceiling 1.5 --poll 2
slothqueue: load ceiling 1.50, cooldown 5.0s, poll 2s
  load 19.36 >= ceiling 1.50 — waiting 2s
  load 21.25 >= ceiling 1.50 — waiting 2s
slothqueue: interrupted; job returned to pending
$ echo $?
130
$ $S list --db demo/wait.db
ID  PRI  STATE    EXIT  DURATION  ATT  COMMAND
1   0    pending  -     -         0    echo "nightly reindex"
```

(Both `SIGINT` and `SIGTERM` raise the same clean shutdown; reproduce with
`bash demo/interrupt-demo.sh`.)

`run` exits `0` when every job it touched succeeded, `1` if any job failed, and
`2` for bad input. That makes it usable directly from `cron` or a systemd timer.

### Crash recovery

A worker was killed with `SIGKILL` while a job was running. The row stayed
`running`; the next `run` reclaimed it and ran it again:

```console
$ $S add 'sleep 2; echo "recovered: lockfile swept"' --max-runtime 10 --db demo/demo.db
added job #4 (priority 0, max runtime 10s)
$ $S run --db demo/demo.db --load-ceiling 0 --cooldown 0 > /dev/null 2>&1 &
$ sleep 0.6 && kill -9 $!
$ $S status --db demo/demo.db
database : /opt/data/profiles/idea/work/slothqueue/demo/demo.db (16.0 KiB)
pending  : 0
running  : 1
done     : 1
failed   : 2
total    : 4
next up  : -
last done: 2026-10-22T20:43:54Z

$ $S run --db demo/demo.db --load-ceiling 0 --cooldown 0
reclaimed 1 stale job(s): #4
slothqueue: load ceiling 0.00, cooldown 0.0s, poll 5s
  job #4 (pri 0): sleep 2; echo "recovered: lockfile swept"
recovered: lockfile swept
  job #4 done in 2.06s
finished: 1 done, 0 failed

$ $S list --db demo/demo.db
ID  PRI  STATE   EXIT  DURATION  ATT  COMMAND
...
4   0    done    0     2.06s     2    sleep 2; echo "recovered: lockfile swept"
```

`ATT 2` is the point: the lost attempt is recorded, not erased. Reproduce the
whole sequence yourself with `bash demo/crash-demo.sh`.

## How it works

One table:

| column | meaning |
|---|---|
| `id` | autoincrement, also the FIFO tiebreaker |
| `command` | the shell command, stored verbatim |
| `priority` | integer, higher first |
| `state` | `pending` → `running` → `done` / `failed` |
| `exit_code` | child exit status; `-9` when killed for timeout |
| `reason` | `timeout: …`, `exit code N`, `reclaimed …`, or `NULL` on success |
| `started_at`, `finished_at`, `duration` | epoch seconds and seconds elapsed |
| `attempts` | incremented at claim time, never reset |
| `max_runtime` | per-job timeout in seconds |
| `created_at` | when it was queued |
| `lease_expires_at`, `pid`, `host` | the running claim, cleared on finish |

State transitions are single SQL statements, and the file is opened in WAL mode
with a 30-second busy timeout, so several processes can inspect it safely (only
one should run it).

### Timeouts

The child is started with `start_new_session=True` and gets its own process
group. On timeout the whole group receives `SIGKILL`, so a shell pipeline or a
compiler's children do not survive their parent. The job is marked `failed` with
`exit_code=-9` and a reason naming the limit:

```
timeout: exceeded max runtime of 2s (process group killed)
```

### Leases and stale rows

When a job is claimed, `lease_expires_at = now + max_runtime + lease_grace`
(grace defaults to 30s, `run --lease-grace`). At the start of every `run`, each
`running` row is reclaimed if either:

1. its lease has expired (covers a reboot, a different machine, a dead clock), or
2. it claims this host, and the recorded pid no longer exists.

Reclaimed rows go back to `pending` (attempts preserved) with a reason like
`reclaimed from stale running job (worker pid no longer alive)`, then run
normally in the same pass.

This is deliberately **at-least-once**. If the worker is `SIGKILL`ed, the child
process group survives it (nothing can run cleanup after `SIGKILL`), so the
command may finish once as an orphan and then run again after reclaim. Write
jobs to tolerate being run twice if that matters.

## CLI reference

| command | flags |
|---|---|
| `add COMMAND` | `--priority N`, `--max-runtime SECONDS`, `--db PATH` |
| `run` | `--load-ceiling L` (0 disables gating), `--cooldown S`, `--poll S`, `--lease-grace S`, `--limit N`, `--db PATH` |
| `list` | `--state pending\|running\|done\|failed`, `--limit N`, `--json`, `--db PATH` |
| `status` | `--json`, `--db PATH` |
| `clear` | `--all`, `--db PATH` |

`--db` defaults to `$SLOTHQUEUE_DB`, then `.slothqueue.db` in the current
directory. `clear` deletes only finished jobs unless `--all` is given, so it can
never silently discard work you have not run.

## Tests

```console
$ /opt/data/work/dail_vsx/.venv/bin/python -m pytest -q
..........................                                               [100%]
26 passed in 5.52s
```

Load sampling, sleeping and the clock are injected, so the scheduling tests
assert exact poll/cooldown behaviour without waiting. The suite covers
add/list/status/clear, priority order, load gating (including "one sample per
poll, one sleep per high sample"), cooldown placement, success and failure exit
codes, per-job timeout kills, reclaim by dead pid, reclaim by expired lease,
leaving a live worker's row alone, CLI persistence across separate processes,
and a real `SIGTERM` while the runner is waiting (exits `130`, job still
`pending`).

## What it does not do

- **No distributed workers.** One SQLite file on one machine, one runner at a
  time. There is no cross-host locking, no SQS, no Redis.
- **No network, at all.** No telemetry, no API keys, no sync. It cannot see the
  load on another machine.
- **No retries or backoff.** A failed job stays failed; re-add it if you want
  another attempt. Only stale `running` rows are automatically retried.
- **No output capture.** Job stdout/stderr go to the runner's stdout/stderr.
  There is no log table, no per-job log file, no history of what a job printed.
- **No daemon or scheduler.** It runs when you invoke it. Pair it with a systemd
  timer or a `cron` entry that calls `run` every few minutes; `run` exits
  immediately when the queue is empty.
- **No GUI, no web UI, no dashboard.**
- **Load average only.** It does not look at memory pressure, disk IO, battery,
  or per-core utilisation. A job that eats 8 GB of RAM will still start on an
  idle machine.
- **No Windows support**, and no way to cancel a specific running job from
  another terminal.
