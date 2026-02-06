import argparse
from pathlib import Path
from datetime import datetime, timedelta
import duckdb


# ----------------------------
# Robust SQL runner (handles comments/quotes)
# ----------------------------
def split_sql_statements(sql: str) -> list[str]:
    statements = []
    buff = []

    in_single = False
    in_double = False
    in_line_comment = False
    in_block_comment = False

    i = 0
    n = len(sql)

    while i < n:
        ch = sql[i]
        nxt = sql[i + 1] if i + 1 < n else ""

        # End line comment
        if in_line_comment:
            buff.append(ch)
            if ch == "\n":
                in_line_comment = False
            i += 1
            continue

        # End block comment
        if in_block_comment:
            buff.append(ch)
            if ch == "*" and nxt == "/":
                buff.append(nxt)
                in_block_comment = False
                i += 2
            else:
                i += 1
            continue

        # Start comments (only if not inside quotes)
        if not in_single and not in_double:
            if ch == "-" and nxt == "-":
                in_line_comment = True
                buff.append(ch)
                buff.append(nxt)
                i += 2
                continue
            if ch == "/" and nxt == "*":
                in_block_comment = True
                buff.append(ch)
                buff.append(nxt)
                i += 2
                continue

        # Toggle quotes
        if ch == "'" and not in_double and not in_line_comment and not in_block_comment:
            # handle escaped '' inside single quotes
            if in_single and nxt == "'":
                buff.append(ch)
                buff.append(nxt)
                i += 2
                continue
            in_single = not in_single
            buff.append(ch)
            i += 1
            continue

        if ch == '"' and not in_single and not in_line_comment and not in_block_comment:
            in_double = not in_double
            buff.append(ch)
            i += 1
            continue

        # Statement terminator
        if ch == ";" and not in_single and not in_double and not in_line_comment and not in_block_comment:
            stmt = "".join(buff).strip()
            if stmt:
                statements.append(stmt)
            buff = []
            i += 1
            continue

        buff.append(ch)
        i += 1

    tail = "".join(buff).strip()
    if tail:
        statements.append(tail)

    return statements


def run_sql_file(con: duckdb.DuckDBPyConnection, path: Path) -> None:
    sql_text = path.read_text(encoding="utf-8")
    for stmt in split_sql_statements(sql_text):
        con.execute(stmt)


# ----------------------------
# Helpers
# ----------------------------
def parse_date(s: str) -> datetime.date:
    return datetime.strptime(s, "%Y-%m-%d").date()


def table_exists(con: duckdb.DuckDBPyConnection, schema: str, table: str) -> bool:
    row = con.execute(
        """
        SELECT 1
        FROM information_schema.tables
        WHERE table_schema = ? AND table_name = ?
        LIMIT 1
        """,
        [schema, table],
    ).fetchone()
    return row is not None


def set_params_view(con: duckdb.DuckDBPyConnection, run_date: datetime.date) -> None:
    prev_date = run_date - timedelta(days=1)
    # NOTE: do NOT use prepared params here; DuckDB can't prepare CREATE VIEW
    con.execute(
        f"""
        CREATE OR REPLACE TEMP VIEW params AS
        SELECT
          DATE '{run_date.isoformat()}' AS run_date,
          DATE '{prev_date.isoformat()}' AS prev_date;
        """
    )


def ingest_day(con: duckdb.DuckDBPyConnection, run_date: datetime.date) -> None:
    d = run_date.isoformat()
    # Idempotent per day
    con.execute(f"DELETE FROM core.fact_events WHERE date(event_ts) = DATE '{d}';")
    con.execute(
        f"""
        INSERT INTO core.fact_events
        SELECT *
        FROM core.fact_events_all
        WHERE date(event_ts) = DATE '{d}';
        """
    )


def inject_session_null_spike(con: duckdb.DuckDBPyConnection, spike_date: datetime.date, spike_rate: float) -> None:
    d = spike_date.isoformat()
    # This simulates upstream session tracking failure
    con.execute(
        f"""
        UPDATE core.fact_events
        SET session_id = NULL
        WHERE date(event_ts) = DATE '{d}'
          AND random() < {spike_rate};
        """
    )


def export_csvs(con: duckdb.DuckDBPyConnection, out_dir: Path) -> None:
    out_dir.mkdir(parents=True, exist_ok=True)

    def to_duckdb_path(p: Path) -> str:
        return str(p.resolve()).replace("\\", "/")

    health_path = to_duckdb_path(out_dir / "health_daily.csv")
    checks_path = to_duckdb_path(out_dir / "check_results_with_incidents.csv")
    incidents_path = to_duckdb_path(out_dir / "incidents.csv")

    con.execute(
        f"""
        COPY (
          SELECT * FROM dq.v_health_daily
          ORDER BY check_date
        )
        TO '{health_path}' (HEADER, DELIMITER ',');
        """
    )

    con.execute(
        f"""
        COPY (
          SELECT * FROM dq.v_check_results_with_incidents
          ORDER BY check_date, severity, status, check_name
        )
        TO '{checks_path}' (HEADER, DELIMITER ',');
        """
    )

    con.execute(
        f"""
        COPY (
          SELECT * FROM dq.dq_incidents
          ORDER BY opened_at DESC
        )
        TO '{incidents_path}' (HEADER, DELIMITER ',');
        """
    )


# ----------------------------
# Main
# ----------------------------
def main():
    parser = argparse.ArgumentParser(description="Run replay + DQ checks + anomalies in DuckDB (single day or all days)")
    parser.add_argument("--db", default="db/analytics.duckdb", help="Path to DuckDB file (relative to project root)")
    parser.add_argument("--run-date", help="Run a single date (YYYY-MM-DD)")
    parser.add_argument("--run-all", action="store_true", help="Run for all distinct dates in core.fact_events_all")
    parser.add_argument("--reset-replay", action="store_true", help="TRUNCATE core.fact_events before loading dates")
    parser.add_argument("--export", action="store_true", help="Export CSVs to data/output")
    parser.add_argument("--out-dir", default="data/output", help="Export folder (default: data/output)")

    # Demo spike options
    parser.add_argument("--demo-spike", action="store_true", help="Inject session_id NULL spike on spike-date")
    parser.add_argument("--spike-date", default="2019-11-18", help="Spike date (YYYY-MM-DD)")
    parser.add_argument("--spike-rate", type=float, default=0.20, help="Spike rate (0.20 = 20%)")

    args = parser.parse_args()

    if not args.run_all and not args.run_date:
        raise SystemExit("ERROR: Provide --run-date YYYY-MM-DD OR use --run-all")

    root = Path(__file__).resolve().parents[1]
    db_path = (root / args.db).resolve()

    sql_dir = root / "sql"
    load_sql = sql_dir / "01_load.sql"
    dq_tables_sql = sql_dir / "03_dq_tables.sql"
    dq_checks_sql = sql_dir / "04_dq_checks.sql"
    anomalies_sql = sql_dir / "05_anomalies.sql"

    # Basic file checks (clear error messages)
    missing = [p for p in [dq_tables_sql, dq_checks_sql, anomalies_sql] if not p.exists()]
    if missing:
        raise SystemExit("ERROR: Missing SQL file(s):\n" + "\n".join([str(p) for p in missing]))

    con = None
    try:
        con = duckdb.connect(str(db_path))

        # If base core tables are missing, auto-run 01_load.sql (this fixes your “Missing core tables” loop)
        if not (table_exists(con, "core", "fact_events_all") and table_exists(con, "core", "fact_events")):
            if not load_sql.exists():
                raise SystemExit(
                    f"ERROR: core tables missing AND {load_sql} not found.\n"
                    f"Expected: core.fact_events_all and core.fact_events.\n"
                    f"Fix: restore sql/01_load.sql or point to the correct DB file."
                )
            print("[INFO] Base tables missing -> running sql/01_load.sql once...")
            run_sql_file(con, load_sql)

        # Ensure dq tables/views exist (idempotent)
        run_sql_file(con, dq_tables_sql)

        if args.reset_replay:
            con.execute("TRUNCATE TABLE core.fact_events;")

        # Build date list
        if args.run_all:
            dates = [
                r[0].isoformat()
                for r in con.execute(
                    """
                    SELECT DISTINCT date(event_ts) AS d
                    FROM core.fact_events_all
                    ORDER BY d
                    """
                ).fetchall()
            ]
        else:
            dates = [args.run_date]

        spike_date = parse_date(args.spike_date) if args.demo_spike else None

        # Run loop
        for idx, d_str in enumerate(dates, start=1):
            d = parse_date(d_str)

            ingest_day(con, d)

            if args.demo_spike and spike_date == d:
                inject_session_null_spike(con, d, args.spike_rate)

            set_params_view(con, d)

            # Run DQ checks for this day (04 expects TEMP VIEW params)
            run_sql_file(con, dq_checks_sql)

            # Run anomalies for this day (05 should use params OR be written for run_date filtering)
            # If your 05 is written “per run_date”, this will work. If it’s “max date only”, it will still run.
            run_sql_file(con, anomalies_sql)

            print(f"[OK] {idx}/{len(dates)} ran: {d_str}")

        if args.export:
            export_csvs(con, root / args.out_dir)
            print(f"[OK] Exported CSVs to: {(root / args.out_dir).resolve()}")

        replay_rows = con.execute("SELECT COUNT(*) FROM core.fact_events;").fetchone()[0]
        print(f"[DONE] Ran {len(dates)} day(s). core.fact_events rows = {replay_rows}")

    finally:
        if con is not None:
            con.close()


if __name__ == "__main__":
    main()
