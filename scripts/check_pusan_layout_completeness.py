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

    q_seed = """
    SELECT
      COUNT(*) AS total_rows,
      COUNT(*) FILTER (WHERE COALESCE(is_active, TRUE)=TRUE) AS active_rows,
      COUNT(orient_code) FILTER (WHERE COALESCE(is_active, TRUE)=TRUE) AS orient_filled_rows,
      COUNT(*) FILTER (WHERE COALESCE(is_active, TRUE)=TRUE AND orient_code IS NULL) AS orient_null_rows,
      COUNT(module_qty) FILTER (WHERE COALESCE(is_active, TRUE)=TRUE) AS module_qty_filled_rows
    FROM staging.seed_inverter_string_layout
    WHERE site_id = 'NE=54435794';
    """

    q_seed_capacity = """
    WITH mw AS (
      SELECT site_id, MAX(pv_module_p_nom_wp_master) AS module_wp
      FROM dimensions.dim_inverter_string_layout
      WHERE COALESCE(is_active, TRUE)=TRUE
        AND pv_module_p_nom_wp_master IS NOT NULL
      GROUP BY site_id
    ),
    ov AS (
      SELECT site_id::text AS site_id, inverter_id::text AS inverter_id, string_no, MAX(module_wp_override) AS module_wp_override
      FROM staging.seed_string_module_override
      GROUP BY 1,2,3
    ),
    b AS (
      SELECT
        l.site_id,
        COALESCE(NULLIF(TRIM(l.inverter_id::text), ''), NULLIF(TRIM(l.inverter_no::text), '')) AS inverter_id,
        l.string_no,
        l.module_qty,
        COALESCE(ov.module_wp_override, mw.module_wp) AS module_wp
      FROM staging.seed_inverter_string_layout l
      LEFT JOIN mw ON mw.site_id = l.site_id
      LEFT JOIN ov
        ON ov.site_id = l.site_id
       AND ov.inverter_id = COALESCE(NULLIF(TRIM(l.inverter_id::text), ''), NULLIF(TRIM(l.inverter_no::text), ''))
       AND ov.string_no = l.string_no
      WHERE l.site_id = 'NE=54435794'
        AND COALESCE(l.is_active, TRUE)=TRUE
    )
    SELECT
      COUNT(*) AS active_layout_rows,
      COUNT(*) FILTER (WHERE module_qty IS NOT NULL AND module_wp IS NOT NULL) AS rows_with_capacity_inputs,
      COUNT(*) FILTER (WHERE module_qty IS NULL OR module_wp IS NULL) AS rows_missing_capacity_inputs,
      SUM(CASE WHEN module_qty IS NOT NULL AND module_wp IS NOT NULL THEN (module_qty*module_wp)/1000.0 ELSE 0 END) AS total_capacity_kw_stc
    FROM b;
    """

    q_orient_dist = """
    SELECT orient_code, COUNT(*) AS rows_cnt
    FROM staging.seed_inverter_string_layout
    WHERE site_id='NE=54435794'
      AND COALESCE(is_active, TRUE)=TRUE
    GROUP BY orient_code
    ORDER BY orient_code;
    """

    q_dim = """
    SELECT
      COUNT(*) AS dim_rows,
      COUNT(*) FILTER (WHERE COALESCE(is_active, TRUE)=TRUE) AS dim_active_rows,
      COUNT(orient_code) FILTER (WHERE COALESCE(is_active, TRUE)=TRUE) AS dim_orient_filled_rows,
      COUNT(pv_module_p_nom_wp_master) FILTER (WHERE COALESCE(is_active, TRUE)=TRUE) AS dim_module_wp_filled_rows
    FROM dimensions.dim_inverter_string_layout
    WHERE site_id='NE=54435794';
    """

    with engine.connect() as conn:
        print("seed_summary", dict(conn.execute(text(q_seed)).fetchone()._mapping))
        print("seed_capacity", dict(conn.execute(text(q_seed_capacity)).fetchone()._mapping))
        print("dim_summary", dict(conn.execute(text(q_dim)).fetchone()._mapping))
        print("seed_orient_distribution")
        for row in conn.execute(text(q_orient_dist)).fetchall():
            print(dict(row._mapping))


if __name__ == "__main__":
    main()
