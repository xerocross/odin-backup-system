#!/usr/bin/env python3
"""
Minimal SQLite migration runner (single-file, no deps).

Usage:
  ./migrate.py --db audit.db --dir migrations up
  ./migrate.py --db audit.db --dir migrations status

- Applies migrations named like: 0001_init.sql, 0002_run_input_sig.sql, 0003_*.py
- Records applied migrations in `schema_migrations`.
- Runs each migration in a transaction. Fails fast on error.
"""
import argparse, sqlite3, sys, time, importlib.util
from pathlib import Path
from typing import List, Tuple

def ensure_meta(conn: sqlite3.Connection):
    # Enforce FKs for everything the runner does
    conn.execute("PRAGMA foreign_keys=ON;")
    # Defer FK checks until COMMIT (SQLite 3.39+)
    try:
        conn.execute("PRAGMA defer_foreign_keys=ON;")
    except sqlite3.OperationalError:
        pass  # older SQLite, safe to ignore
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("""
        CREATE TABLE IF NOT EXISTS schema_migrations (
          id INTEGER PRIMARY KEY,
          name TEXT NOT NULL,
          applied_at INTEGER NOT NULL
        );
    """)
    conn.commit()

def list_migrations(dir_path: Path) -> List[Tuple[int, str, Path]]:
    files = []
    for p in dir_path.iterdir():
        if p.suffix not in (".sql", ".py"):
            continue
        name = p.name
        try:
            num = int(name.split("_", 1)[0])
        except (ValueError, IndexError):
            continue
        files.append((num, name, p))
    files.sort(key=lambda x: x[0])
    return files

def applied_map(conn: sqlite3.Connection):
    return {row[0]: row[1] for row in conn.execute("SELECT id, name FROM schema_migrations")}

def apply_sql(conn: sqlite3.Connection, path: Path):
    sql = path.read_text()
    conn.executescript(sql)

def apply_py(conn: sqlite3.Connection, path: Path):
    spec = importlib.util.spec_from_file_location(path.stem, path)
    mod = importlib.util.module_from_spec(spec)
    assert spec and spec.loader
    spec.loader.exec_module(mod)
    if not hasattr(mod, "migrate"):
        raise RuntimeError(f"{path.name} must define a migrate(conn) function")
    mod.migrate(conn)

def apply_one(conn: sqlite3.Connection, num: int, name: str, path: Path):
    # Single atomic transaction per migration
    conn.execute("PRAGMA foreign_keys=ON;")
    try:
        conn.execute("BEGIN IMMEDIATE;")  # lock early; DDL is transactional in SQLite
        if path.suffix == ".sql":
            apply_sql(conn, path)  # do NOT BEGIN/COMMIT inside files
        else:
            apply_py(conn, path)   # migration code must NOT commit/rollback
        conn.execute(
            "INSERT INTO schema_migrations (id, name, applied_at) VALUES (?, ?, ?)",
            (num, name, int(time.time())),
        )
        conn.commit()
    except Exception:
        conn.rollback()  # all or nothing
        raise


def cmd_status(conn: sqlite3.Connection, dir_path: Path):
    ensure_meta(conn)
    migs = list_migrations(dir_path)
    applied = applied_map(conn)
    for num, name, _ in migs:
        mark = "APPLIED" if num in applied else "PENDING"
        print(f"{num:04d} {name:40s} {mark}")
    missing = sorted(set(applied) - {n for n,_,_ in migs})
    if missing:
        print("\nWarning: DB has unknown migrations:", missing)

def cmd_up(conn: sqlite3.Connection, dir_path: Path):
    ensure_meta(conn)
    migs = list_migrations(dir_path)
    applied = applied_map(conn)
    for num, name, path in migs:
        if num in applied:
            continue
        print(f"Applying {num:04d} {name} ...", flush=True)
        apply_one(conn, num, name, path)
        print("  done.")

def main(argv=None):
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", required=True, help="Path to SQLite DB file")
    ap.add_argument("--dir", required=True, help="Path to migrations directory")
    ap.add_argument("command", choices=["status", "up"], help="Show status or apply pending migrations")
    args = ap.parse_args(argv)

    db_path = Path(args.db)
    dir_path = Path(args.dir)
    dir_path.mkdir(parents=True, exist_ok=True)

    conn = sqlite3.connect(db_path)
    try:
        if args.command == "status":
            cmd_status(conn, dir_path)
        else:
            cmd_up(conn, dir_path)
    finally:
        conn.close()

if __name__ == "__main__":
    sys.exit(main())
