import os
import urllib.parse
from pathlib import Path

from dotenv import load_dotenv
from sqlalchemy import create_engine, text


def main() -> None:
    root = Path(r"C:\Users\Administrator\Documents\Code\MMSR API - Server MA")
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
        WITH m AS (
          SELECT
            date_key,
            site_id,
            site_name,
            inverter_id,
            REGEXP_REPLACE(inverter_id, '^(FS_|ISO_)', '') AS inverter_key,
            string_number,
            string_dc_capacity_kw_stc
          FROM mart.mart_string_performance_daily
          WHERE date_key >= CURRENT_DATE - INTERVAL '45 day'
        ),
        d AS (
          SELECT
            site_id,
            inverter_id,
            string_no AS string_number,
            MAX(module_qty) AS module_qty,
            MAX(pv_module_p_nom_wp_master) AS module_wp
          FROM dimensions.dim_inverter_string_layout
          WHERE COALESCE(is_active, TRUE)=TRUE
          GROUP BY 1,2,3
        ),
        j AS (
          SELECT
            m.site_id,
            MAX(m.site_name) AS site_name,
            COUNT(*) AS total_rows,
            COUNT(*) FILTER (WHERE m.string_dc_capacity_kw_stc IS NOT NULL) AS cap_filled_rows,
            COUNT(*) FILTER (
              WHERE m.string_dc_capacity_kw_stc IS NULL
                AND d.site_id IS NOT NULL
            ) AS matched_layout_but_null_capacity_rows,
            COUNT(*) FILTER (
              WHERE m.string_dc_capacity_kw_stc IS NULL
                AND d.site_id IS NULL
            ) AS no_layout_match_rows,
            COUNT(*) FILTER (
              WHERE m.string_dc_capacity_kw_stc IS NULL
                AND d.site_id IS NOT NULL
                AND (d.module_qty IS NULL OR d.module_wp IS NULL)
            ) AS missing_module_inputs_rows
          FROM m
          LEFT JOIN d
            ON d.site_id = m.site_id
           AND d.inverter_id = m.inverter_key
           AND d.string_number = m.string_number
          GROUP BY m.site_id
        )
        SELECT
          site_id,
          site_name,
          total_rows,
          cap_filled_rows,
          ROUND(100.0 * cap_filled_rows::numeric / NULLIF(total_rows,0), 2) AS cap_fill_pct,
          no_layout_match_rows,
          matched_layout_but_null_capacity_rows,
          missing_module_inputs_rows
        FROM j
        ORDER BY cap_fill_pct ASC, site_id;
        """
    )

    with engine.connect() as conn:
        rows = conn.execute(sql).fetchall()

    for row in rows:
        print(dict(row._mapping))


if __name__ == "__main__":
    main()
