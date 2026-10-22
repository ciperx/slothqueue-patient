"""Command line interface for slothqueue."""

from __future__ import annotations

import argparse
import json
import os
import sys

from .core import (
    DEFAULT_COOLDOWN,
    DEFAULT_LEASE_GRACE,
    DEFAULT_LOAD_CEILING,
    DEFAULT_MAX_RUNTIME,
    DEFAULT_POLL,
    STATES,
    SlothQueue,
    SlothQueueError,
    _iso,
)

DB_ENV = "SLOTHQUEUE_DB"
DB_DEFAULT = ".slothqueue.db"


def _human_bytes(n):
    if n < 1024:
        return "%d B" % n
    for unit in ("KiB", "MiB", "GiB"):
        n /= 1024.0
        if n < 1024 or unit == "GiB":
            return "%.1f %s" % (n, unit)


def _cmd_add(sq, args):
    job_id = sq.add(args.cmd, priority=args.priority, max_runtime=args.max_runtime)
    print(
        "added job #%d (priority %d, max runtime %gs)"
        % (job_id, args.priority, args.max_runtime)
    )
    return 0


def _cmd_list(sq, args):
    rows = sq.jobs(state=args.state, limit=args.limit)
    if args.json:
        print(json.dumps(rows, indent=2))
        return 0
    if not rows:
        print("queue is empty")
        return 0
    header = ("ID", "PRI", "STATE", "EXIT", "DURATION", "ATT", "COMMAND")
    table = [header]
    for r in rows:
        duration = "-" if r["duration"] is None else "%.2fs" % r["duration"]
        exit_code = "-" if r["exit_code"] is None else str(r["exit_code"])
        command = " ".join(r["command"].split())
        if len(command) > 48:
            command = command[:45] + "..."
        table.append(
            (
                str(r["id"]),
                str(r["priority"]),
                r["state"],
                exit_code,
                duration,
                str(r["attempts"]),
                command,
            )
        )
    widths = [max(len(row[i]) for row in table) for i in range(len(header))]
    for row in table:
        print("  ".join(cell.ljust(widths[i]) for i, cell in enumerate(row)).rstrip())
    return 0


def _cmd_status(sq, args):
    st = sq.status()
    if args.json:
        print(json.dumps(st, indent=2, default=str))
        return 0
    print("database : %s (%s)" % (st["db"], _human_bytes(st["db_bytes"])))
    for state in STATES:
        print("%-8s : %d" % (state, st["counts"][state]))
    print("%-8s : %d" % ("total", st["total"]))
    if st["oldest_pending"]:
        op = st["oldest_pending"]
        print("next up  : #%d (queued %s)" % (op["id"], _iso(op["created_at"])))
    else:
        print("next up  : -")
    print("last done: %s" % _iso(st["last_finished"]))
    return 0


def _cmd_clear(sq, args):
    removed = sq.clear(include_running=args.all)
    print("cleared %d job(s)" % removed)
    return 0


def _cmd_run(sq, args):
    summary = sq.run(
        load_ceiling=args.load_ceiling,
        cooldown=args.cooldown,
        poll=args.poll,
        limit=args.limit or None,
    )
    if summary["processed"] == 0:
        print("nothing to do")
    return 1 if summary["failed"] else 0


def build_parser():
    common = argparse.ArgumentParser(add_help=False)
    common.add_argument(
        "--db",
        default=os.environ.get(DB_ENV, DB_DEFAULT),
        help="SQLite queue file (default: $%s or %s)" % (DB_ENV, DB_DEFAULT),
    )

    p = argparse.ArgumentParser(
        prog="slothqueue",
        description="A patient, local-first job queue. Runs shell commands "
        "only when the machine is quiet enough.",
    )
    sub = p.add_subparsers(dest="verb", required=True)

    add = sub.add_parser("add", parents=[common], help="queue a shell command")
    add.add_argument("cmd", metavar="COMMAND", help="shell command to run")
    add.add_argument(
        "--priority", type=int, default=0, help="higher runs first (default: 0)"
    )
    add.add_argument(
        "--max-runtime",
        type=float,
        default=DEFAULT_MAX_RUNTIME,
        help="seconds before the job is killed (default: %g)" % DEFAULT_MAX_RUNTIME,
    )
    add.set_defaults(func=_cmd_add)

    run = sub.add_parser("run", parents=[common], help="work the queue")
    run.add_argument(
        "--load-ceiling",
        type=float,
        default=DEFAULT_LOAD_CEILING,
        help="wait until 1-min load average is below this (default: %g; 0 disables)"
        % DEFAULT_LOAD_CEILING,
    )
    run.add_argument(
        "--cooldown",
        type=float,
        default=DEFAULT_COOLDOWN,
        help="seconds to sleep between jobs (default: %g)" % DEFAULT_COOLDOWN,
    )
    run.add_argument(
        "--poll",
        type=float,
        default=DEFAULT_POLL,
        help="seconds between load samples while waiting (default: %g)" % DEFAULT_POLL,
    )
    run.add_argument(
        "--lease-grace",
        type=float,
        default=DEFAULT_LEASE_GRACE,
        help="extra seconds before an abandoned 'running' row is reclaimed "
        "(default: %g)" % DEFAULT_LEASE_GRACE,
    )
    run.add_argument(
        "--limit", type=int, default=0, help="stop after N jobs (default: all pending)"
    )
    run.set_defaults(func=_cmd_run)

    lst = sub.add_parser("list", parents=[common], help="show queued and finished jobs")
    lst.add_argument("--state", choices=list(STATES), help="only show one state")
    lst.add_argument("--limit", type=int, default=50, help="max rows (default: 50)")
    lst.add_argument("--json", action="store_true", help="machine-readable output")
    lst.set_defaults(func=_cmd_list)

    status = sub.add_parser("status", parents=[common], help="summary counts")
    status.add_argument("--json", action="store_true", help="machine-readable output")
    status.set_defaults(func=_cmd_status)

    clear = sub.add_parser("clear", parents=[common], help="delete finished jobs")
    clear.add_argument(
        "--all", action="store_true", help="also delete pending and running jobs"
    )
    clear.set_defaults(func=_cmd_clear)
    return p


def main(argv=None):
    args = build_parser().parse_args(argv)
    sq = SlothQueue(
        args.db, lease_grace=getattr(args, "lease_grace", DEFAULT_LEASE_GRACE)
    )
    try:
        return args.func(sq, args)
    except SlothQueueError as exc:
        print("slothqueue: %s" % exc, file=sys.stderr)
        return 2
    except KeyboardInterrupt:
        print("slothqueue: interrupted; job returned to pending", file=sys.stderr)
        return 130
