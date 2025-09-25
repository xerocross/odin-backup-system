#!/usr/bin/env python3
import sqlite3, time, sys, os
from pathlib import Path


HOME = Path.home()
DB = Path("~/.odin_backup/audit.db").expanduser()
OUT = Path("/var/lib/node_exporter/textfile_collector") / "odin.prom"  # adjust path if needed
NOW = int(time.time())
COLLECTOR = Path("/var/lib/node_exporter/textfile_collector")
tmp = COLLECTOR / ".odin.tmp"
out = COLLECTOR / "odin.prom"

def q(conn, sql, args=()):
    cur = conn.execute(sql, args)
    for row in cur: yield row

def main():
    conn = sqlite3.connect(DB)
    lines = []

    # 1) last run status per job (1=ok, 0=fail)
    for job, status in q(conn, """
        WITH last AS (
          SELECT name, MAX(started_at) AS t
          FROM runs
          GROUP BY name
        )
        SELECT r.name, r.status
        FROM runs r
        JOIN last L ON L.name=r.name AND L.t=r.started_at
    """):
        v = 1 if (status == "success" or status == "skipped")  else 0
        lines.append(f'odin_backup_last_run_status{{job="{job}"}} {v}')

    # 2) failures in last 24h
    for job, cnt in q(conn, """
        SELECT name, COUNT(*) FROM runs
        WHERE started_at >= ? AND status = 'failed'
        GROUP BY name
    """, (NOW - 24*3600,)):
        lines.append(f'odin_backup_failures_24h{{job="{job}"}} {cnt}')

    # 3) run duration seconds for last run (handy for alerts if too long)
    for job, dur in q(conn, """
        WITH last AS (
          SELECT name, MAX(started_at) AS t
          FROM runs
          GROUP BY name
        )
        SELECT r.name, (r.finished_at - r.started_at)
        FROM runs r
        JOIN last L ON L.name=r.name AND L.t=r.started_at
        WHERE r.finished_at IS NOT NULL
    """):
        lines.append(f'odin_last_run_duration_seconds{{job="{job}"}} {dur}')

    text = "\n".join(lines) + "\n"

    tmp.write_text(text)

    # Ensure readable by node_exporter; 0644 is fine (read-only needed)
    os.chmod(tmp, 0o644)

    # Atomic publish
    os.replace(tmp, out)

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"exporter error: {e}", file=sys.stderr)
        sys.exit(1)
