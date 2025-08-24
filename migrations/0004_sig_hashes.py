import sqlite3

def migrate(conn):
    cur = conn.cursor()

    cur.execute("PRAGMA table_info(runs)")
    columns = [row[1] for row in cur.fetchall()]  # second field is column name

    if "input_sig_hash" not in columns:
        cur.execute("ALTER TABLE runs ADD COLUMN input_sig_hash TEXT")

    if "output_sig_hash" not in columns:
        cur.execute("ALTER TABLE runs ADD COLUMN output_sig_hash TEXT")