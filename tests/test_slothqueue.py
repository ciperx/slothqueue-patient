"""Tests for slothqueue.

Real subprocesses are used only for the behaviour that is *about* subprocesses
(exit codes, timeouts, cross-process persistence). Load sampling, sleeping and
the clock are injected, so the scheduling tests never touch the machine and
never really wait.
"""

import json
import os
import socket
import sqlite3
import subprocess
import sys
import time

import pytest

from slothqueue import SlothQueue, SlothQueueError

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


class FakeClock:
    def __init__(self, start=1_000_000.0):
        self.t = start

    def __call__(self):
        return self.t


class FakeSleep:
    def __init__(self):
        self.calls = []

    def __call__(self, seconds):
        self.calls.append(seconds)


def make_queue(tmp_path, loads=(0.1, 0.1, 0.1), clock=None, sleeps=None, echo=None):
    loads = list(loads)
    sleeper = sleeps if sleeps is not None else FakeSleep()
    clock = clock or FakeClock()

    def loadavg():
        return (loads.pop(0), 0.0, 0.0) if len(loads) > 1 else (loads[0], 0.0, 0.0)

    sq = SlothQueue(
        tmp_path / "queue.db",
        loadavg=loadavg,
        sleep=sleeper,
        clock=clock,
        echo=echo or (lambda msg: None),
    )
    sq.init_db()
    return sq, sleeper, clock


def force_running(db_path, job_id, *, lease, pid, host, attempts=1, started=1_000_000.0):
    conn = sqlite3.connect(str(db_path))
    try:
        conn.execute(
            "UPDATE jobs SET state='running', lease_expires_at=?, pid=?, host=?,"
            " attempts=?, started_at=? WHERE id=?",
            (lease, pid, host, attempts, started, job_id),
        )
        conn.commit()
    finally:
        conn.close()


# --------------------------------------------------------------- add / list


def test_add_returns_increasing_ids_and_persists(tmp_path):
    sq, _, _ = make_queue(tmp_path)
    first = sq.add("echo one")
    second = sq.add("echo two", priority=5, max_runtime=12)
    assert (first, second) == (1, 2)

    rows = sq.jobs()
    assert [r["id"] for r in rows] == [2, 1]  # priority 5 first
    assert rows[0]["command"] == "echo two"
    assert rows[0]["max_runtime"] == 12
    assert rows[0]["state"] == "pending"
    assert rows[0]["attempts"] == 0
    assert rows[0]["exit_code"] is None


def test_add_rejects_empty_command(tmp_path):
    sq, _, _ = make_queue(tmp_path)
    with pytest.raises(SlothQueueError):
        sq.add("   ")


def test_add_rejects_nonpositive_max_runtime(tmp_path):
    sq, _, _ = make_queue(tmp_path)
    with pytest.raises(SlothQueueError):
        sq.add("true", max_runtime=0)


def test_list_filters_by_state(tmp_path):
    sq, _, _ = make_queue(tmp_path)
    sq.add("true")
    sq.add("false")

    assert len(sq.jobs(state="pending")) == 2
    assert sq.jobs(state="done") == []
    with pytest.raises(SlothQueueError):
        sq.jobs(state="bogus")


def test_status_counts_and_next_up(tmp_path):
    sq, _, _ = make_queue(tmp_path)
    sq.add("true", priority=1)
    sq.add("false", priority=9)
    status = sq.status()
    assert status["total"] == 2
    assert status["counts"]["pending"] == 2
    assert status["counts"]["done"] == 0
    assert status["oldest_pending"]["id"] == 2  # highest priority is next
    assert status["last_finished"] is None


def test_clear_removes_finished_but_keeps_pending(tmp_path):
    sq, _, _ = make_queue(tmp_path, loads=(0.0,))
    sq.add("true")
    sq.run(load_ceiling=0, cooldown=0)
    sq.add("echo still here")

    removed = sq.clear()
    assert removed == 1
    remaining = sq.jobs()
    assert [r["command"] for r in remaining] == ["echo still here"]


def test_clear_leaves_running_rows_unless_all(tmp_path):
    sq, _, _ = make_queue(tmp_path)
    job_id = sq.add("true")
    force_running(tmp_path / "queue.db", job_id, lease=10**12, pid=os.getpid(),
                  host=socket.gethostname())
    assert sq.clear() == 0
    assert sq.jobs()[0]["state"] == "running"
    assert sq.clear(include_running=True) == 1
    assert sq.jobs() == []


# --------------------------------------------------------------- execution


def test_success_records_all_fields(tmp_path):
    sq, _, _ = make_queue(tmp_path)
    job_id = sq.add("echo hello")
    summary = sq.run(load_ceiling=0, cooldown=0)

    assert summary["processed"] == 1 and summary["done"] == 1
    job = sq.jobs()[0]
    assert job["id"] == job_id
    assert job["state"] == "done"
    assert job["exit_code"] == 0
    assert job["attempts"] == 1
    assert job["reason"] is None
    assert job["started_at"] is not None and job["finished_at"] is not None
    assert job["finished_at"] >= job["started_at"]
    assert job["duration"] is not None and job["duration"] >= 0
    assert job["lease_expires_at"] is None


def test_failure_records_exit_code_and_reason(tmp_path):
    sq, _, _ = make_queue(tmp_path)
    sq.add("exit 3")
    summary = sq.run(load_ceiling=0, cooldown=0)

    assert summary["failed"] == 1
    job = sq.jobs()[0]
    assert job["state"] == "failed"
    assert job["exit_code"] == 3
    assert "exit code 3" in job["reason"]


def test_priority_order_determines_execution_order(tmp_path):
    sq, _, _ = make_queue(tmp_path)
    log = tmp_path / "order.txt"
    sq.add("echo low >> %s" % log, priority=1)
    sq.add("echo high >> %s" % log, priority=100)
    sq.add("echo mid >> %s" % log, priority=50)

    sq.run(load_ceiling=0, cooldown=0)
    assert log.read_text().split() == ["high", "mid", "low"]


def test_attempts_increment_on_each_run(tmp_path):
    sq, _, _ = make_queue(tmp_path)
    job_id = sq.add("echo again")
    sq.run(load_ceiling=0, cooldown=0)
    conn = sqlite3.connect(str(tmp_path / "queue.db"))
    conn.execute("UPDATE jobs SET state='pending' WHERE id=?", (job_id,))
    conn.commit()
    conn.close()
    sq.run(load_ceiling=0, cooldown=0)
    assert sq.jobs()[0]["attempts"] == 2


def test_run_with_empty_queue_does_nothing(tmp_path):
    sq, sleeps, _ = make_queue(tmp_path)
    summary = sq.run(load_ceiling=0, cooldown=1)
    assert summary["processed"] == 0
    assert sleeps.calls == []


# ------------------------------------------------------------- load gating


def test_load_gate_waits_until_below_ceiling(tmp_path):
    sq, sleeps, _ = make_queue(tmp_path, loads=(4.0, 3.0, 0.4))
    sq.add("true")
    sq.run(load_ceiling=1.5, cooldown=0, poll=7.0)

    assert sleeps.calls == [7.0, 7.0]
    assert sq.jobs()[0]["state"] == "done"


def test_load_gate_samples_once_per_poll_never_busy_loops(tmp_path):
    sq, sleeps, _ = make_queue(tmp_path, loads=(9.0, 9.0, 9.0, 9.0, 9.0, 0.2))
    calls = []
    inner = sq.loadavg

    def counting_loadavg():
        calls.append(1)
        return inner()

    sq.loadavg = counting_loadavg
    sq.add("true")
    sq.run(load_ceiling=1.0, cooldown=0, poll=2.5)

    assert len(calls) == 6           # one sample per loop iteration
    assert sleeps.calls == [2.5] * 5  # exactly one sleep per high sample
    assert sq.jobs()[0]["state"] == "done"


def test_load_ceiling_zero_disables_gating(tmp_path):
    sq, sleeps, _ = make_queue(tmp_path, loads=(99.0,))
    sq.add("true")
    sq.run(load_ceiling=0, cooldown=0)
    assert sleeps.calls == []
    assert sq.jobs()[0]["state"] == "done"


def test_cooldown_sleeps_between_jobs_only(tmp_path):
    sq, sleeps, _ = make_queue(tmp_path, loads=(0.0,))
    sq.add("true")
    sq.add("true")
    sq.run(load_ceiling=0, cooldown=3.5)
    assert sleeps.calls == [3.5]  # once, between the two jobs, not after the last


# ---------------------------------------------------------------- timeouts


def test_timeout_kills_job_and_marks_failed(tmp_path):
    sq, _, _ = make_queue(tmp_path)
    sq.add(
        '%s -c "import time; time.sleep(30)"' % sys.executable,
        max_runtime=0.4,
    )
    started = time.monotonic()
    summary = sq.run(load_ceiling=0, cooldown=0)
    elapsed = time.monotonic() - started

    assert summary["failed"] == 1
    job = sq.jobs()[0]
    assert job["state"] == "failed"
    assert "timeout" in job["reason"].lower()
    assert job["exit_code"] != 0
    assert job["duration"] < 5
    assert elapsed < 10


def test_per_job_max_runtime_is_respected(tmp_path):
    sq, _, _ = make_queue(tmp_path)
    sq.add('%s -c "import time; time.sleep(0.1)"' % sys.executable, max_runtime=10)
    sq.add('%s -c "import time; time.sleep(30)"' % sys.executable, max_runtime=0.3)
    sq.run(load_ceiling=0, cooldown=0)

    fast, slow = sq.jobs()
    assert fast["state"] == "done" and fast["max_runtime"] == 10
    assert slow["state"] == "failed" and "0.3" in slow["reason"]


# ------------------------------------------------------------ crash safety


def test_reclaim_running_row_with_dead_pid_immediately(tmp_path):
    sq, _, _ = make_queue(tmp_path)
    job_id = sq.add("echo recovered")
    force_running(
        tmp_path / "queue.db", job_id, lease=10**12, pid=999_999,
        host=socket.gethostname(),
    )
    summary = sq.run(load_ceiling=0, cooldown=0)

    assert summary["reclaimed"] == [job_id]
    job = sq.jobs()[0]
    assert job["state"] == "done"
    assert job["attempts"] == 2  # the lost attempt is not forgotten


def test_reclaim_running_row_with_expired_lease_on_other_host(tmp_path):
    sq, _, clock = make_queue(tmp_path)
    job_id = sq.add("echo other-host")
    force_running(
        tmp_path / "queue.db", job_id, lease=clock() - 1, pid=12345, host="elsewhere"
    )
    summary = sq.run(load_ceiling=0, cooldown=0)

    assert summary["reclaimed"] == [job_id]
    assert sq.jobs()[0]["state"] == "done"


def test_running_row_with_live_worker_and_valid_lease_is_left_alone(tmp_path):
    sq, _, clock = make_queue(tmp_path)
    job_id = sq.add("echo mine")
    force_running(
        tmp_path / "queue.db", job_id, lease=clock() + 600, pid=os.getpid(),
        host=socket.gethostname(),
    )
    summary = sq.run(load_ceiling=0, cooldown=0)

    assert summary["processed"] == 0
    job = sq.jobs()[0]
    assert job["state"] == "running"
    assert job["attempts"] == 1


def test_new_job_gets_a_lease_when_claimed(tmp_path):
    sq, _, clock = make_queue(tmp_path)
    sq.add("true", max_runtime=20)
    job = sq.next_job()

    started, timeout = sq._claim(job)
    row = sq.jobs(state="running")[0]

    assert timeout == 20
    assert row["attempts"] == 1
    assert row["pid"] == os.getpid()
    assert row["host"] == socket.gethostname()
    assert row["lease_expires_at"] == pytest.approx(started + 20 + sq.lease_grace)


# ------------------------------------------------------------- persistence


def _env():
    env = dict(os.environ)
    env["PYTHONPATH"] = ROOT + os.pathsep + env.get("PYTHONPATH", "")
    return env


def _cli(*args, db):
    return subprocess.run(
        [sys.executable, "-m", "slothqueue", *args, "--db", str(db)],
        cwd=ROOT, env=_env(), capture_output=True, text=True, timeout=120,
    )


def test_cli_persistence_across_processes(tmp_path):
    db = tmp_path / "cross.db"

    added = _cli("add", "echo persisted", "--priority", "4", db=db)
    assert added.returncode == 0, added.stderr
    assert "added job #1" in added.stdout

    listed = _cli("list", "--json", db=db)
    assert listed.returncode == 0, listed.stderr
    rows = json.loads(listed.stdout)
    assert len(rows) == 1
    assert rows[0]["command"] == "echo persisted"
    assert rows[0]["priority"] == 4

    status = _cli("status", db=db)
    assert "pending  : 1" in status.stdout


def test_cli_run_exit_code_reflects_failure(tmp_path):
    db = tmp_path / "rc.db"
    assert _cli("add", "true", db=db).returncode == 0
    assert _cli("add", "false", db=db).returncode == 0

    run = _cli("run", "--load-ceiling", "0", "--cooldown", "0", db=db)
    assert run.returncode == 1
    assert "finished: 1 done, 1 failed" in run.stdout

    listed = _cli("list", "--json", db=db)
    states = {r["command"]: r["state"] for r in json.loads(listed.stdout)}
    assert states == {"true": "done", "false": "failed"}


def test_cli_rejects_empty_command(tmp_path):
    db = tmp_path / "bad.db"
    result = _cli("add", "  ", db=db)
    assert result.returncode == 2
    assert "non-empty" in result.stderr


def test_sigterm_while_waiting_exits_cleanly_and_keeps_job_pending(tmp_path):
    db = tmp_path / "term.db"
    assert _cli("add", "echo never runs", db=db).returncode == 0

    proc = subprocess.Popen(
        [sys.executable, "-m", "slothqueue", "run",
         "--db", str(db), "--load-ceiling", "0.001", "--poll", "1"],
        cwd=ROOT, env=_env(),
        stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True,
    )
    time.sleep(1.2)
    proc.terminate()
    out, err = proc.communicate(timeout=15)

    assert proc.returncode == 130, (out, err)
    assert "interrupted" in err
    assert "waiting" in out
    rows = json.loads(_cli("list", "--json", db=db).stdout)
    assert rows[0]["state"] == "pending"
    assert rows[0]["attempts"] == 0
