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
      SELECT date_key, inverter_id, site_id AS source_site_id, site_name, string_number
      FROM mart.mart_string_performance_daily
      WHERE site_id IN ('NE=51758766', 'NE=58630782')
        AND date_key >= CURRENT_DATE - INTERVAL '30 day'
    ),
    site_canonical AS (
      SELECT
        d.*,
        COALESCE(da.asset_id, d.source_site_id) AS canonical_site_id
      FROM d
      LEFT JOIN dimensions.dim_assets da
        ON da.asset_level = 'Site'
       AND UPPER(TRIM(da.site_name)) = UPPER(TRIM(d.site_name))
    ),
    layout_capacity AS (
      SELECT
        site_id,
        inverter_id,
        string_no AS string_number,
        MAX(orient_code) AS layout_orient_code
      FROM dimensions.dim_inverter_string_layout
      WHERE COALESCE(is_active, TRUE) = TRUE
      GROUP BY 1, 2, 3
    ),
    poa_by_site_orient AS (
      SELECT
        psc.site_id,
        psc.orient_code,
        sd.date_key,
        AVG(sd.daily_irradiance_kwh_m2) AS daily_poa_kwh_m2
      FROM staging.seed_poa_sensor_config psc
      JOIN mart.mart_sensor_daily sd
        ON (sd.asset_id = psc.poa_sensor_id OR sd.asset_id = CONCAT('FS_', psc.poa_sensor_id))
       AND sd.sensor_type = 'POA'
      WHERE COALESCE(psc.is_active, TRUE) = TRUE
        AND sd.daily_irradiance_kwh_m2 IS NOT NULL
        AND sd.date_key >= CURRENT_DATE - INTERVAL '30 day'
      GROUP BY 1, 2, 3
    ),
    poa_by_site AS (
      SELECT
        psc.site_id,
        sd.date_key,
        AVG(sd.daily_irradiance_kwh_m2) AS daily_poa_site_kwh_m2
      FROM staging.seed_poa_sensor_config psc
      JOIN mart.mart_sensor_daily sd
        ON (sd.asset_id = psc.poa_sensor_id OR sd.asset_id = CONCAT('FS_', psc.poa_sensor_id))
       AND sd.sensor_type = 'POA'
      WHERE COALESCE(psc.is_active, TRUE) = TRUE
        AND sd.daily_irradiance_kwh_m2 IS NOT NULL
        AND sd.date_key >= CURRENT_DATE - INTERVAL '30 day'
      GROUP BY 1, 2
    ),
    calc AS (
      SELECT
        s.source_site_id AS site_id,
        s.date_key,
        s.inverter_id,
        s.string_number,
        COALESCE(poa.daily_poa_kwh_m2, poa_site.daily_poa_site_kwh_m2) AS daily_poa_kwh_m2
      FROM site_canonical s
      LEFT JOIN layout_capacity l
        ON l.site_id = s.canonical_site_id
       AND l.inverter_id = REGEXP_REPLACE(s.inverter_id, '^(FS_|ISO_)', '')
       AND l.string_number = s.string_number
      LEFT JOIN poa_by_site_orient poa
        ON poa.date_key = s.date_key
       AND poa.site_id IN (s.canonical_site_id, s.source_site_id)
       AND poa.orient_code = l.layout_orient_code
      LEFT JOIN poa_by_site poa_site
        ON poa_site.date_key = s.date_key
       AND poa_site.site_id IN (s.canonical_site_id, s.source_site_id)
    )
    SELECT
      site_id,
      COUNT(*) AS total_rows,
      COUNT(daily_poa_kwh_m2) AS poa_rows,
      ROUND(100.0 * COUNT(daily_poa_kwh_m2)::numeric / NULLIF(COUNT(*),0), 2) AS poa_fill_pct
    FROM calc
    GROUP BY site_id
    ORDER BY site_id;
    """

    with engine.connect() as conn:
        for row in conn.execute(text(sql)).fetchall():
            print(dict(row._mapping))


if __name__ == "__main__":
    main()
