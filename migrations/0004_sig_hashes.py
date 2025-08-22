# migrations/0004_sig_hashes.py
import json, hashlib

def migrate(conn):
    cur = conn.cursor()
    # Safety rails for SQLite
    cur.execute("PRAGMA foreign_keys=ON;")
    cur.execute("PRAGMA journal_mode=WAL;")

    def table_cols(table):
        return {row[1] for row in cur.execute(f"PRAGMA table_info({table})")}

    cols = table_cols("runs")

    # Add columns if missing
    if "input_sig_hash" not in cols:
        cur.execute("ALTER TABLE runs ADD COLUMN input_sig_hash TEXT")
    if "output_sig_hash" not in cols:
        cur.execute("ALTER TABLE runs ADD COLUMN output_sig_hash TEXT")

    cols = table_cols("runs")  # refresh after ALTERs

    # Is there a run-level output_sig_json column?
    has_run_output_json = "output_sig_json" in cols

    # Helpers
    def canonicalize_json(text):
        if text is None:
            return None
        try:
            obj = json.loads(text)
        except Exception:
            # Not valid JSON (or you stored raw text); hash the raw bytes
            return text
        # Canonical dump for stable hashing (order/whitespace independent)
        return json.dumps(obj, separators=(",", ":"), sort_keys=True, ensure_ascii=False)

    def sha256_hex(s):
        return hashlib.sha256(s.encode("utf-8")).hexdigest()

    # Backfill per run
    # Pull current rows including any existing hashes so we don't overwrite them.
    runs = list(cur.execute("""
        SELECT run_id, input_sig_json, {run_output_json}, input_sig_hash, output_sig_hash
        FROM runs
    """.format(run_output_json="output_sig_json" if has_run_output_json else "NULL")))
    # For output from steps, prepare a statement to fetch the most recent one
    step_output_stmt = """
        SELECT output_sig_json
        FROM steps
        WHERE run_id = ? AND output_sig_json IS NOT NULL
        ORDER BY COALESCE(finished_at, 0) DESC, id DESC
        LIMIT 1
    """

    to_update_input = []
    to_update_output = []

    for run_id, input_sig_json, run_output_json, existing_in_hash, existing_out_hash in runs:
        # ---- input hash
        if existing_in_hash is None and input_sig_json is not None:
            canon = canonicalize_json(input_sig_json)
            if canon is not None:
                to_update_input.append((sha256_hex(canon), run_id))

        # ---- output hash
        if existing_out_hash is None:
            source = None
            if has_run_output_json and run_output_json is not None:
                source = run_output_json
            else:
                row = cur.execute(step_output_stmt, (run_id,)).fetchone()
                if row and row[0] is not None:
                    source = row[0]
            if source is not None:
                canon = canonicalize_json(source)
                to_update_output.append((sha256_hex(canon), run_id))

    if to_update_input:
        cur.executemany("UPDATE runs SET input_sig_hash = ? WHERE run_id = ?", to_update_input)
    if to_update_output:
        cur.executemany("UPDATE runs SET output_sig_hash = ? WHERE run_id = ?", to_update_output)

    # Helpful indexes for equality lookups
    cur.execute("CREATE INDEX IF NOT EXISTS idx_runs_input_sig_hash  ON runs(input_sig_hash)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_runs_output_sig_hash ON runs(output_sig_hash)")
