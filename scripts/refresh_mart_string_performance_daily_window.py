"""
Delete mart.mart_string_performance_daily rows in [start, end] then run dbt incremental
for that model so staging is re-merged for those dates (via incremental watermark).

Assumes the table still has rows before `start` so MAX(date_key) lands just before the
window (typical: refresh April–May while March data remains). If not, dbt may scan a
much larger history — see post-delete MAX printed below.
"""

from __future__ import annotations

import argparse
import os
import subprocess
import sys
import urllib.parse
from datetime import datetime
from pathlib import Path

from dotenv import load_dotenv
from sqlalchemy import create_engine, text

ROOT = Path(r"C:\Users\Administrator\Documents\Code\MMSR API - Server MA")
DBT_DIR = ROOT / "dbt"


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--start", default="2026-04-01")
    p.add_argument("--end", default=None, help="inclusive; default DB CURRENT_DATE")
    p.add_argument("--skip-dbt", action="store_true")
    p.add_argument(
        "--force",
        action="store_true",
        help="run dbt even if MAX(date_key) after delete is far below --start",
    )
    args = p.parse_args()

    load_dotenv(ROOT / ".env")
    load_dotenv(ROOT / "tools" / ".env")
    engine = create_engine(
        "postgresql://{user}:{pwd}@{host}:{port}/{db}".format(
            user=os.getenv("POSTGRES_USER", "juice"),
            pwd=urllib.parse.quote_plus(os.getenv("POSTGRES_PASSWORD", "")),
            host=os.getenv("POSTGRES_HOST", "10.101.4.88"),
            port=os.getenv("POSTGRES_PORT", "5432"),
            db=os.getenv("POSTGRES_DB", "MMSR"),
        )
    )

    end_sql = "CURRENT_DATE" if args.end is None else ":end"
    params = {"start": args.start}
    if args.end is not None:
        params["end"] = args.end

    del_sql = text(
        f"""
        DELETE FROM mart.mart_string_performance_daily
        WHERE date_key >= :start AND date_key <= {end_sql}
        """
    )

    with engine.begin() as conn:
        r = conn.execute(del_sql, params)
        deleted = r.rowcount
        mx = conn.execute(
            text("SELECT MAX(date_key), MIN(date_key) FROM mart.mart_string_performance_daily")
        ).one()
        end_disp = conn.execute(text("SELECT CURRENT_DATE")).scalar() if args.end is None else args.end

    print({"deleted_rows": deleted, "max_date_key_remaining": mx[0], "min_date_key_remaining": mx[1], "window_end": str(end_disp)})

    if mx[0] is None:
        print("ERROR: mart_string_performance_daily is empty after delete; incremental watermark breaks.", file=sys.stderr)
        sys.exit(1)

    start_d = datetime.strptime(args.start, "%Y-%m-%d").date()
    max_d = mx[0]
    if max_d < start_d:
        span_days = (start_d - max_d).days
        if span_days > 60 and not args.force:
            print(
                f"ERROR: MAX(date_key) after delete is {max_d}, {span_days} days before --start {start_d}; "
                "dbt incremental would rescan too much history. Fix data gap or use --force.",
                file=sys.stderr,
            )
            sys.exit(1)

    if args.skip_dbt:
        print("skip-dbt set; done.")
        return

    cmd = ["dbt", "run", "--select", "mart_string_performance_daily"]
    print("running:", " ".join(cmd), "in", DBT_DIR)
    r = subprocess.run(cmd, cwd=DBT_DIR)
    sys.exit(r.returncode)


if __name__ == "__main__":
    main()
