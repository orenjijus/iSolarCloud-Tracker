import csv
import os
import urllib.parse
from pathlib import Path

from dotenv import load_dotenv
from sqlalchemy import create_engine, text


def main() -> None:
    root = Path(r"C:\Users\Administrator\Documents\Code\MMSR API - Server MA")
    out_csv = root / "docs" / "audit-tables" / "21_string_daily_health_45d.csv"
    out_csv.parent.mkdir(parents=True, exist_ok=True)

    load_dotenv(root / ".env")
    load_dotenv(root / "tools" / ".env")
    engine = create_engine(
        "postgresql://{user}:{pwd}@{host}:{port}/{db}".format(
            user=os.getenv("POSTGRES_USER", "juice"),
            pwd=urllib.parse.quote_plus(os.getenv("POSTGRES_PASSWORD", "")),
            host=os.getenv("POSTGRES_HOST", "10.101.4.88"),
            port=os.getenv("POSTGRES_PORT", "5432"),
            db=os.getenv("POSTGRES_DB", "MMSR"),
        )
    )

    sql = text(
        """
        SELECT
            site_id,
            MAX(site_name) AS site_name,
            COUNT(*) AS total_rows,
            COUNT(string_dc_capacity_kw_stc) AS capacity_rows,
            ROUND(100.0 * COUNT(string_dc_capacity_kw_stc)::numeric / NULLIF(COUNT(*), 0), 2) AS capacity_fill_pct,
            COUNT(daily_poa_kwh_m2) AS poa_rows,
            ROUND(100.0 * COUNT(daily_poa_kwh_m2)::numeric / NULLIF(COUNT(*), 0), 2) AS poa_fill_pct,
            COUNT(daily_ghi_kwh_m2) AS ghi_rows,
            ROUND(100.0 * COUNT(daily_ghi_kwh_m2)::numeric / NULLIF(COUNT(*), 0), 2) AS ghi_fill_pct
        FROM mart.mart_string_performance_daily
        WHERE date_key >= CURRENT_DATE - INTERVAL '45 day'
        GROUP BY site_id
        ORDER BY site_id
        """
    )

    sql_global = text(
        """
        SELECT
            COUNT(*) AS total_rows,
            COUNT(string_dc_capacity_kw_stc) AS capacity_rows,
            ROUND(100.0 * COUNT(string_dc_capacity_kw_stc)::numeric / NULLIF(COUNT(*), 0), 2) AS capacity_fill_pct,
            COUNT(daily_poa_kwh_m2) AS poa_rows,
            ROUND(100.0 * COUNT(daily_poa_kwh_m2)::numeric / NULLIF(COUNT(*), 0), 2) AS poa_fill_pct,
            COUNT(daily_ghi_kwh_m2) AS ghi_rows,
            ROUND(100.0 * COUNT(daily_ghi_kwh_m2)::numeric / NULLIF(COUNT(*), 0), 2) AS ghi_fill_pct
        FROM mart.mart_string_performance_daily
        WHERE date_key >= CURRENT_DATE - INTERVAL '45 day'
        """
    )

    with engine.connect() as conn:
        rows = conn.execute(sql).fetchall()
        global_row = conn.execute(sql_global).fetchone()

    if rows:
        with out_csv.open("w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=list(rows[0]._mapping.keys()))
            writer.writeheader()
            for row in rows:
                writer.writerow(dict(row._mapping))

    print("global", dict(global_row._mapping))
    print(f"wrote={out_csv} rows={len(rows)}")
    for row in rows:
        m = row._mapping
        if m["capacity_fill_pct"] < 95 or m["poa_fill_pct"] < 95 or m["ghi_fill_pct"] < 95:
            print(dict(m))


if __name__ == "__main__":
    main()
