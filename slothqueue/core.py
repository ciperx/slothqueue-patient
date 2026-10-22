"""slothqueue core: a patient, local-first job queue built on SQLite.

No network, no daemon, no third-party packages. One process runs the queue;
everything it knows lives in a single SQLite file.
"""

from __future__ import annotations

import contextlib
import os
import signal
import socket
import sqlite3
import subprocess
import time
from datetime import datetime, timezone

STATES = ("pending", "running", "done", "failed")

DEFAULT_MAX_RUNTIME = 300.0
DEFAULT_LEASE_GRACE = 30.0
DEFAULT_LOAD_CEILING = 1.5
DEFAULT_COOLDOWN = 5.0
DEFAULT_POLL = 5.0

SCHEMA = """
CREATE TABLE IF NOT EXISTS jobs (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    command         TEXT    NOT NULL,
    priority        INTEGER NOT NULL DEFAULT 0,
    state           TEXT    NOT NULL DEFAULT 'pending'
                            CHECK (state IN ('pending', 'running', 'done', 'failed')),
    exit_code       INTEGER,
    reason          TEXT,
    started_at      REAL,
    finished_at     REAL,
    duration        REAL,
    attempts        INTEGER NOT NULL DEFAULT 0,
    max_runtime     REAL    NOT NULL DEFAULT 300.0,
    created_at      REAL    NOT NULL,
    lease_expires_at REAL,
    pid             INTEGER,
    host            TEXT
);
CREATE INDEX IF NOT EXISTS idx_jobs_pick ON jobs(state, priority DESC, id);
"""


class SlothQueueError(Exception):
    """Raised for invalid user input or an unsafe queue operation."""


def _pid_alive(pid):
    if not pid:
        return False
    try:
        os.kill(int(pid), 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    except (OSError, ValueError):
        return False
    return True


def _iso(ts):
    if ts is None:
        return "-"
    return datetime.fromtimestamp(ts, timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _raise_interrupt(signum, frame):
    raise KeyboardInterrupt


class SlothQueue:
    """A single-file job queue with load-gated, paced execution.

    ``loadavg``, ``sleep``, ``clock`` and ``echo`` are injectable so tests can
    drive scheduling without touching the real machine.
    """

    def __init__(
        self,
        db_path,
        *,
        loadavg=None,
        sleep=None,
        clock=None,
        echo=None,
        lease_grace=DEFAULT_LEASE_GRACE,
    ):
        self.db_path = os.path.abspath(db_path)
        self.loadavg = loadavg or os.getloadavg
        self.sleep = sleep or time.sleep
        self.clock = clock or time.time
        self.echo = echo or (lambda msg: print(msg, flush=True))
        self.lease_grace = float(lease_grace)

    # ------------------------------------------------------------------ db

    @contextlib.contextmanager
    def connect(self):
        conn = sqlite3.connect(self.db_path, timeout=30.0)
        conn.row_factory = sqlite3.Row
        try:
            conn.execute("PRAGMA busy_timeout = 30000")
            conn.execute("PRAGMA journal_mode = WAL")
            conn.execute("PRAGMA synchronous = NORMAL")
            yield conn
            conn.commit()
        finally:
            conn.close()

    def init_db(self):
        parent = os.path.dirname(self.db_path)
        if parent:
            os.makedirs(parent, exist_ok=True)
        with self.connect() as conn:
            conn.executescript(SCHEMA)

    # --------------------------------------------------------------- writes

    def add(self, command, priority=0, max_runtime=DEFAULT_MAX_RUNTIME):
        if not isinstance(command, str) or not command.strip():
            raise SlothQueueError("command must be a non-empty string")
        try:
            priority = int(priority)
        except (TypeError, ValueError):
            raise SlothQueueError("priority must be an integer")
        try:
            max_runtime = float(max_runtime)
        except (TypeError, ValueError):
            raise SlothQueueError("max runtime must be a number")
        if max_runtime <= 0:
            raise SlothQueueError("max runtime must be greater than zero")

        self.init_db()
        with self.connect() as conn:
            cur = conn.execute(
                "INSERT INTO jobs (command, priority, state, max_runtime, attempts, created_at)"
                " VALUES (?, ?, 'pending', ?, 0, ?)",
                (command, priority, max_runtime, self.clock()),
            )
            return int(cur.lastrowid)

    def clear(self, include_running=False):
        """Delete finished rows, or every row when include_running=True."""
        self.init_db()
        with self.connect() as conn:
            if include_running:
                return conn.execute("DELETE FROM jobs").rowcount
            return conn.execute(
                "DELETE FROM jobs WHERE state IN ('done', 'failed')"
            ).rowcount


    # ---------------------------------------------------------------- reads

    def jobs(self, state=None, limit=None):
        self.init_db()
        sql = "SELECT * FROM jobs"
        params = []
        if state:
            if state not in STATES:
                raise SlothQueueError("unknown state %r (choose from %s)" % (state, ", ".join(STATES)))
            sql += " WHERE state = ?"
            params.append(state)
        sql += " ORDER BY priority DESC, id ASC"
        if limit:
            sql += " LIMIT ?"
            params.append(int(limit))
        with self.connect() as conn:
            return [dict(r) for r in conn.execute(sql, params)]

    def next_job(self):
        self.init_db()
        with self.connect() as conn:
            row = conn.execute(
                "SELECT * FROM jobs WHERE state = 'pending'"
                " ORDER BY priority DESC, id ASC LIMIT 1"
            ).fetchone()
            return dict(row) if row else None

    def status(self):
        self.init_db()
        counts = {s: 0 for s in STATES}
        with self.connect() as conn:
            for row in conn.execute("SELECT state, COUNT(*) AS n FROM jobs GROUP BY state"):
                counts[row["state"]] = row["n"]
            oldest = conn.execute(
                "SELECT id, created_at FROM jobs WHERE state = 'pending'"
                " ORDER BY priority DESC, id ASC LIMIT 1"
            ).fetchone()
            last = conn.execute(
                "SELECT MAX(finished_at) AS t FROM jobs WHERE finished_at IS NOT NULL"
            ).fetchone()
        try:
            size = os.path.getsize(self.db_path)
        except OSError:
            size = 0
        return {
            "db": self.db_path,
            "db_bytes": size,
            "counts": counts,
            "total": sum(counts.values()),
            "oldest_pending": dict(oldest) if oldest else None,
            "last_finished": last["t"] if last else None,
        }

    # ----------------------------------------------------------- scheduling

    def wait_for_capacity(self, ceiling=DEFAULT_LOAD_CEILING, poll=DEFAULT_POLL):
        """Block (sleeping, never spinning) until 1-min load is under ceiling."""
        if ceiling is None or ceiling <= 0:
            return None
        while True:
            load = float(self.loadavg()[0])
            if load < ceiling:
                self.echo("  load %.2f < ceiling %.2f — starting" % (load, ceiling))
                return load
            self.echo(
                "  load %.2f >= ceiling %.2f — waiting %.0fs"
                % (load, ceiling, poll)
            )
            self.sleep(poll)

    def reclaim_stale(self):
        """Return rows abandoned by a dead worker to the pending state."""
        now = self.clock()
        mine = socket.gethostname()
        reclaimed = []
        with self.connect() as conn:
            rows = conn.execute("SELECT * FROM jobs WHERE state = 'running'").fetchall()
            for row in rows:
                lease = row["lease_expires_at"]
                expired = lease is None or float(lease) < now
                dead_here = row["host"] == mine and not _pid_alive(row["pid"])
                if not (expired or dead_here):
                    continue
                why = "lease expired" if expired else "worker pid no longer alive"
                conn.execute(
                    "UPDATE jobs SET state = 'pending', started_at = NULL,"
                    " finished_at = NULL, duration = NULL, exit_code = NULL,"
                    " lease_expires_at = NULL, pid = NULL, host = NULL,"
                    " reason = ? WHERE id = ?",
                    ("reclaimed from stale running job (%s)" % why, row["id"]),
                )
                reclaimed.append(row["id"])
        return reclaimed

    # ------------------------------------------------------------ execution

    def _claim(self, job):
        now = self.clock()
        timeout = float(job["max_runtime"])
        with self.connect() as conn:
            conn.execute(
                "UPDATE jobs SET state = 'running', attempts = attempts + 1,"
                " started_at = ?, finished_at = NULL, duration = NULL,"
                " exit_code = NULL, reason = NULL, lease_expires_at = ?,"
                " pid = ?, host = ? WHERE id = ?",
                (
                    now,
                    now + timeout + self.lease_grace,
                    os.getpid(),
                    socket.gethostname(),
                    job["id"],
                ),
            )
        return now, timeout

    def _finish(self, job_id, state, exit_code, reason, started, finished):
        with self.connect() as conn:
            conn.execute(
                "UPDATE jobs SET state = ?, exit_code = ?, reason = ?,"
                " finished_at = ?, duration = ?, lease_expires_at = NULL,"
                " pid = NULL, host = NULL WHERE id = ?",
                (
                    state,
                    exit_code,
                    reason,
                    finished,
                    max(0.0, finished - started),
                    job_id,
                ),
            )

    def execute_job(self, job):
        started, timeout = self._claim(job)
        self.echo("  job #%d (pri %d): %s" % (job["id"], job["priority"], job["command"]))
        try:
            proc = subprocess.Popen(
                job["command"],
                shell=True,
                start_new_session=True,
            )
        except OSError as exc:
            finished = self.clock()
            self._finish(job["id"], "failed", None, "spawn error: %s" % exc, started, finished)
            return {"id": job["id"], "state": "failed", "exit_code": None,
                    "duration": finished - started, "reason": "spawn error"}

        timed_out = False
        try:
            proc.communicate(timeout=timeout)
            code = proc.returncode
        except subprocess.TimeoutExpired:
            timed_out = True
            try:
                os.killpg(os.getpgid(proc.pid), signal.SIGKILL)
            except (ProcessLookupError, PermissionError):
                proc.kill()
            proc.communicate()
            code = proc.returncode if proc.returncode is not None else -signal.SIGKILL
        except KeyboardInterrupt:
            try:
                os.killpg(os.getpgid(proc.pid), signal.SIGTERM)
            except (ProcessLookupError, PermissionError):
                proc.kill()
            proc.communicate()
            with self.connect() as conn:
                conn.execute(
                    "UPDATE jobs SET state = 'pending', started_at = NULL,"
                    " lease_expires_at = NULL, pid = NULL, host = NULL,"
                    " reason = 'interrupted by operator' WHERE id = ?",
                    (job["id"],),
                )
            raise

        finished = self.clock()
        duration = max(0.0, finished - started)
        if timed_out:
            state = "failed"
            reason = "timeout: exceeded max runtime of %gs (process group killed)" % timeout
            code = -int(signal.SIGKILL)
        elif code == 0:
            state, reason = "done", None
        else:
            state = "failed"
            reason = "exit code %d" % code
        self._finish(job["id"], state, code, reason, started, finished)
        mark = "done" if state == "done" else "FAILED"
        self.echo(
            "  job #%d %s in %.2fs%s"
            % (job["id"], mark, duration, "" if reason is None else " (%s)" % reason)
        )
        return {"id": job["id"], "state": state, "exit_code": code,
                "duration": duration, "reason": reason}

    def run(
        self,
        load_ceiling=DEFAULT_LOAD_CEILING,
        cooldown=DEFAULT_COOLDOWN,
        poll=DEFAULT_POLL,
        limit=None,
    ):
        """Work the queue until empty (or limit), handling SIGTERM like Ctrl-C."""
        previous = None
        try:
            previous = signal.getsignal(signal.SIGTERM)
            signal.signal(signal.SIGTERM, _raise_interrupt)
        except (ValueError, OSError):  # not the main thread
            previous = None
        try:
            return self._run_loop(load_ceiling, cooldown, poll, limit)
        finally:
            if previous is not None:
                try:
                    signal.signal(signal.SIGTERM, previous)
                except (ValueError, OSError):
                    pass

    def _run_loop(self, load_ceiling, cooldown, poll, limit):
        self.init_db()
        reclaimed = self.reclaim_stale()
        if reclaimed:
            self.echo("reclaimed %d stale job(s): %s"
                      % (len(reclaimed), ", ".join("#%d" % i for i in reclaimed)))
        self.echo(
            "slothqueue: load ceiling %.2f, cooldown %.1fs, poll %.0fs"
            % (load_ceiling, cooldown, poll)
        )
        results = []
        while True:
            if limit is not None and len(results) >= int(limit):
                break
            job = self.next_job()
            if job is None:
                break
            self.wait_for_capacity(load_ceiling, poll)
            results.append(self.execute_job(job))
            if cooldown > 0 and self.next_job() is not None:
                self.echo("  cooling down %.1fs" % cooldown)
                self.sleep(cooldown)
        done = sum(1 for r in results if r["state"] == "done")
        failed = len(results) - done
        self.echo("finished: %d done, %d failed" % (done, failed))
        return {"processed": len(results), "done": done, "failed": failed,
                "reclaimed": reclaimed, "results": results}
