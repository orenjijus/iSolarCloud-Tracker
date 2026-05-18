import os
import urllib.parse
from pathlib import Path

from dotenv import load_dotenv
from sqlalchemy import create_engine, text


def main() -> None:
    root = Path(r"C:\Users\Administrator\Documents\Code\MMSR API - Server MA")
    load_dotenv(root / ".env")
    load_dotenv(root / "tools" / ".env")

    host = os.getenv("POSTGRES_HOST", "10.101.4.88")
    port = os.getenv("POSTGRES_PORT", "5432")
    db = os.getenv("POSTGRES_DB", "MMSR")
    user = os.getenv("POSTGRES_USER", "juice")
    pwd = urllib.parse.quote_plus(os.getenv("POSTGRES_PASSWORD", ""))
    engine = create_engine(f"postgresql://{user}:{pwd}@{host}:{port}/{db}")

    sql = """
    WITH d AS (
      SELECT date_key, inverter_id, site_id, site_name, string_number
      FROM mart.mart_string_performance_daily
    ),
    site_canonical AS (
      SELECT
        d.*,
        COALESCE(da.asset_id, d.site_id) AS canonical_site_id
      FROM d
      LEFT JOIN dimensions.dim_assets da
        ON da.asset_level = 'Site'
       AND UPPER(TRIM(da.site_name)) = UPPER(TRIM(d.site_name))
    ),
    layout_dedup AS (
      SELECT
        l.site_id,
        COALESCE(NULLIF(TRIM(l.inverter_id::text), ''), NULLIF(TRIM(l.inverter_no::text), '')) AS inverter_id,
        l.string_no AS string_number,
        MAX(l.module_qty) AS layout_module_qty
      FROM staging.seed_inverter_string_layout l
      WHERE COALESCE(l.is_active, TRUE) = TRUE
        AND l.string_no IS NOT NULL
      GROUP BY 1, 2, 3
    ),
    module_wp_by_site AS (
      SELECT site_id, MAX(pv_module_p_nom_wp_master) AS module_wp
      FROM dimensions.dim_inverter_string_layout
      WHERE COALESCE(is_active, TRUE) = TRUE
        AND pv_module_p_nom_wp_master IS NOT NULL
      GROUP BY site_id
    ),
    module_wp_override AS (
      SELECT site_id::text AS site_id, inverter_id::text AS inverter_id, string_no AS string_number, MAX(module_wp_override) AS module_wp_override
      FROM staging.seed_string_module_override
      GROUP BY 1,2,3
    ),
    calc AS (
      SELECT
        s.site_id,
        CASE
          WHEN (
            COALESCE(l.layout_module_qty, 0) * COALESCE(ov.module_wp_override, mw.module_wp)
          ) / 1000.0 > 0
          THEN 1 ELSE 0 END AS has_capacity
      FROM site_canonical s
      LEFT JOIN layout_dedup l
        ON l.site_id IN (s.canonical_site_id, s.site_id)
       AND (l.inverter_id = REGEXP_REPLACE(s.inverter_id, '^(FS_|ISO_)', '') OR l.inverter_id IS NULL)
       AND l.string_number = s.string_number
      LEFT JOIN module_wp_by_site mw
        ON mw.site_id = l.site_id
      LEFT JOIN module_wp_override ov
        ON ov.site_id = l.site_id
       AND ov.inverter_id = l.inverter_id
       AND ov.string_number = l.string_number
    )
    SELECT
      site_id,
      COUNT(*) AS total_rows,
      SUM(has_capacity) AS capacity_rows,
      ROUND(100.0 * SUM(has_capacity)::numeric / NULLIF(COUNT(*), 0), 2) AS capacity_fill_pct
    FROM calc
    GROUP BY site_id
    ORDER BY site_id;
    """

    with engine.connect() as conn:
        rows = conn.execute(text(sql)).fetchall()
    for row in rows:
        print(dict(row._mapping))


if __name__ == "__main__":
    main()
