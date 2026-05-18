"""
Per-site checker for mart.mart_string_performance_daily over a date window.

Reports whether each site has adequate non-null coverage for:
  1) string_dc_capacity_kw_stc
  2) daily_poa_kwh_m2
  3) daily_energy_kwh (daily energy)
  4) layout_orient_code (non-blank)

Rule: keterangan ADA if fill_pct >= threshold (default 90%), else TIDAK.
Also writes fill % columns for detail.
"""

from __future__ import annotations

import argparse
import csv
import os
import urllib.parse
from pathlib import Path

from dotenv import load_dotenv
from sqlalchemy import create_engine, text

ROOT = Path(r"C:\Users\Administrator\Documents\Code\MMSR API - Server MA")


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--start", default="2026-04-01", help="date_key inclusive (YYYY-MM-DD)")
    p.add_argument("--end", default=None, help="date_key inclusive; default = DB current_date")
    p.add_argument(
        "--threshold-pct",
        type=float,
        default=90.0,
        help="min fill %% for status ADA (default 90)",
    )
    p.add_argument(
        "--out-csv",
        default=None,
        help="default: docs/audit-tables/string_mart_site_fields_<start>_<end>.csv",
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

    end_clause = "CURRENT_DATE" if args.end is None else ":end"
    params = {"start": args.start, "thr": args.threshold_pct}
    if args.end is not None:
        params["end"] = args.end

    sql = text(
        f"""
        WITH b AS (
            SELECT
                site_id,
                site_name,
                string_dc_capacity_kw_stc,
                daily_poa_kwh_m2,
                daily_energy_kwh,
                layout_orient_code
            FROM mart.mart_string_performance_daily
            WHERE date_key >= :start
              AND date_key <= {end_clause}
        )
        SELECT
            site_id,
            MAX(site_name) AS site_name,
            COUNT(*) AS total_rows,
            ROUND(100.0 * COUNT(string_dc_capacity_kw_stc)::numeric / NULLIF(COUNT(*), 0), 2)
                AS pct_string_dc_capacity,
            ROUND(100.0 * COUNT(daily_poa_kwh_m2)::numeric / NULLIF(COUNT(*), 0), 2)
                AS pct_daily_poa,
            ROUND(100.0 * COUNT(daily_energy_kwh)::numeric / NULLIF(COUNT(*), 0), 2)
                AS pct_daily_energy_kwh,
            ROUND(
                100.0 * COUNT(*) FILTER (
                    WHERE layout_orient_code IS NOT NULL
                      AND btrim(layout_orient_code::text) <> ''
                )::numeric / NULLIF(COUNT(*), 0),
                2
            ) AS pct_layout_orient_code,
            CASE
                WHEN 100.0 * COUNT(string_dc_capacity_kw_stc)::numeric / NULLIF(COUNT(*), 0) >= :thr
                THEN 'ADA' ELSE 'TIDAK'
            END AS keterangan_string_dc_capacity,
            CASE
                WHEN 100.0 * COUNT(daily_poa_kwh_m2)::numeric / NULLIF(COUNT(*), 0) >= :thr
                THEN 'ADA' ELSE 'TIDAK'
            END AS keterangan_daily_poa,
            CASE
                WHEN 100.0 * COUNT(daily_energy_kwh)::numeric / NULLIF(COUNT(*), 0) >= :thr
                THEN 'ADA' ELSE 'TIDAK'
            END AS keterangan_daily_energy,
            CASE
                WHEN 100.0 * COUNT(*) FILTER (
                    WHERE layout_orient_code IS NOT NULL
                      AND btrim(layout_orient_code::text) <> ''
                )::numeric / NULLIF(COUNT(*), 0) >= :thr
                THEN 'ADA' ELSE 'TIDAK'
            END AS keterangan_layout_orient_code
        FROM b
        GROUP BY site_id
        ORDER BY site_id
        """
    )

    with engine.connect() as conn:
        rows = conn.execute(sql, params).fetchall()
        end_disp = conn.execute(text("SELECT CURRENT_DATE::text")).scalar() if args.end is None else args.end

    out = args.out_csv
    if not out:
        safe_start = str(args.start).replace("-", "")
        safe_end = str(end_disp).replace("-", "")
        out_path = ROOT / "docs" / "audit-tables" / f"string_mart_site_fields_{safe_start}_{safe_end}.csv"
    else:
        out_path = Path(out)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    if not rows:
        print("no rows in window", args.start, "to", end_disp)
        return

    fieldnames = list(rows[0]._mapping.keys())
    with out_path.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        for row in rows:
            w.writerow(dict(row._mapping))

    print(f"window: {args.start} .. {end_disp}  (ADA if fill >= {args.threshold_pct}%)")
    print(f"wrote {out_path}  sites={len(rows)}")
    for row in rows:
        m = dict(row._mapping)
        print(
            m["site_id"],
            m.get("site_name", ""),
            "| cap:", m["keterangan_string_dc_capacity"],
            "poa:", m["keterangan_daily_poa"],
            "energy:", m["keterangan_daily_energy"],
            "orient:", m["keterangan_layout_orient_code"],
        )


if __name__ == "__main__":
    main()
