import csv
import os
import urllib.parse
from pathlib import Path

from dotenv import load_dotenv
from sqlalchemy import create_engine, text


def main() -> None:
    root = Path(r"C:\Users\Administrator\Documents\Code\MMSR API - Server MA")
    out_csv = root / "docs" / "audit-tables" / "19_string_capacity_source_vs_mart.csv"
    out_csv.parent.mkdir(parents=True, exist_ok=True)

    load_dotenv(root / ".env")
    load_dotenv(root / "tools" / ".env")

    host = os.getenv("POSTGRES_HOST", "10.101.4.88")
    port = os.getenv("POSTGRES_PORT", "5432")
    db = os.getenv("POSTGRES_DB", "MMSR")
    user = os.getenv("POSTGRES_USER", "juice")
    pwd = urllib.parse.quote_plus(os.getenv("POSTGRES_PASSWORD", ""))
    engine = create_engine(f"postgresql://{user}:{pwd}@{host}:{port}/{db}")

    sql = """
    WITH mart_sites AS (
      SELECT
        site_id,
        MAX(site_name) AS site_name,
        COUNT(*) AS mart_rows,
        COUNT(string_dc_capacity_kw_stc) AS mart_capacity_rows
      FROM mart.mart_string_performance_daily
      GROUP BY site_id
    ),
    layout_by_site AS (
      SELECT
        site_id,
        COUNT(*) FILTER (WHERE COALESCE(is_active, TRUE) = TRUE) AS layout_active_rows,
        SUM(CASE WHEN COALESCE(is_active, TRUE) = TRUE THEN COALESCE(module_qty, 0) ELSE 0 END) AS total_module_qty,
        COUNT(*) FILTER (
          WHERE COALESCE(is_active, TRUE) = TRUE
            AND module_qty IS NOT NULL
            AND pv_module_p_nom_wp_master IS NOT NULL
        ) AS layout_rows_with_capacity_inputs,
        SUM(
          CASE
            WHEN COALESCE(is_active, TRUE) = TRUE
             AND module_qty IS NOT NULL
             AND pv_module_p_nom_wp_master IS NOT NULL
            THEN (module_qty * pv_module_p_nom_wp_master) / 1000.0
            ELSE 0
          END
        ) AS layout_total_capacity_kw_stc
      FROM dimensions.dim_inverter_string_layout
      GROUP BY site_id
    )
    SELECT
      m.site_id,
      m.site_name,
      m.mart_rows,
      m.mart_capacity_rows,
      ROUND(100.0 * m.mart_capacity_rows::numeric / NULLIF(m.mart_rows, 0), 2) AS mart_capacity_fill_pct,
      COALESCE(l.layout_active_rows, 0) AS layout_active_rows,
      COALESCE(l.layout_rows_with_capacity_inputs, 0) AS layout_rows_with_capacity_inputs,
      COALESCE(l.total_module_qty, 0) AS total_module_qty,
      COALESCE(l.layout_total_capacity_kw_stc, 0) AS layout_total_capacity_kw_stc,
      CASE WHEN COALESCE(l.layout_active_rows, 0) > 0 THEN 'YES' ELSE 'NO' END AS has_layout
    FROM mart_sites m
    LEFT JOIN layout_by_site l
      ON l.site_id = m.site_id
    ORDER BY m.site_id;
    """

    with engine.connect() as conn:
        rows = conn.execute(text(sql)).fetchall()

    fieldnames = list(rows[0]._mapping.keys()) if rows else []
    with out_csv.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for r in rows:
            writer.writerow(dict(r._mapping))

    print(f"Wrote: {out_csv}")
    print(f"Rows: {len(rows)}")

    target = [r for r in rows if r._mapping["site_id"] == "NE=53771627"]
    print("Mall Panakkukang row:")
    if target:
        print(dict(target[0]._mapping))
    else:
        print("Not found in mart sites")


if __name__ == "__main__":
    main()
