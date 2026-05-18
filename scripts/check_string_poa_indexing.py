"""
Validate orient_code → seed_poa_sensor_config → mart_sensor_daily chain per site.

Does not run dbt. Answers: for each site, what share of layout strings (with orient)
can resolve to a POA sensor that actually has data in mart_sensor_daily in the lookback window.

Example:
  python scripts/check_string_poa_indexing.py --days 90 --min-pct 10
"""

from __future__ import annotations

import argparse
import os
import urllib.parse
from pathlib import Path

from dotenv import load_dotenv
from sqlalchemy import create_engine, text

ROOT = Path(__file__).resolve().parents[1]

SQL = text(
    """
    WITH layout AS (
        SELECT
            s.site_id::text AS site_id,
            s.inverter_no::integer AS inverter_no,
            s.string_no::integer AS string_no,
            s.orient_code::integer AS orient_code
        FROM staging.seed_inverter_string_layout s
        WHERE COALESCE(s.is_active, TRUE)
          AND s.string_no IS NOT NULL
          AND s.orient_code IS NOT NULL
    ),
    poa_cfg AS (
        SELECT
            p.site_id::text AS site_id,
            p.orient_code::integer AS orient_code,
            MAX(p.poa_sensor_id::text) AS poa_sensor_id
        FROM staging.seed_poa_sensor_config p
        WHERE COALESCE(p.is_active, TRUE)
        GROUP BY 1, 2
    ),
    sensor_ok AS (
        SELECT DISTINCT ms.asset_id::text AS asset_id
        FROM mart.mart_sensor_daily ms
        WHERE ms.sensor_type = 'POA'
          AND ms.date_key >= CURRENT_DATE - (:days * INTERVAL '1 day')
          AND ms.daily_irradiance_kwh_m2 IS NOT NULL
    ),
    resolved AS (
        SELECT
            l.site_id,
            l.inverter_no,
            l.string_no,
            l.orient_code,
            c.poa_sensor_id,
            CASE
                WHEN c.poa_sensor_id IS NULL THEN FALSE
                WHEN EXISTS (
                    SELECT 1
                    FROM sensor_ok s
                    WHERE s.asset_id = c.poa_sensor_id
                       OR s.asset_id = CONCAT(
                            'FS_',
                            REGEXP_REPLACE(c.poa_sensor_id, '^(FS_|ISO_)', '')
                        )
                       OR s.asset_id = CONCAT(
                            'ISO_',
                            REGEXP_REPLACE(c.poa_sensor_id, '^(FS_|ISO_)', '')
                        )
                )
                THEN TRUE
                ELSE FALSE
            END AS sensor_has_data
        FROM layout l
        LEFT JOIN poa_cfg c
          ON REGEXP_REPLACE(c.site_id, '^(FS_|ISO_)', '')
           = REGEXP_REPLACE(l.site_id, '^(FS_|ISO_)', '')
         AND c.orient_code = l.orient_code
    ),
    by_site AS (
        SELECT
            site_id,
            COUNT(*) AS strings_with_orient,
            COUNT(*) FILTER (WHERE poa_sensor_id IS NOT NULL) AS with_poa_row,
            COUNT(*) FILTER (WHERE sensor_has_data) AS with_sensor_data
        FROM resolved
        GROUP BY 1
    )
    SELECT
        site_id,
        strings_with_orient,
        with_poa_row,
        with_sensor_data,
        CASE
            WHEN strings_with_orient = 0 THEN NULL
            ELSE ROUND(100.0 * with_sensor_data::numeric / strings_with_orient, 1)
        END AS pct_indexed
    FROM by_site
    ORDER BY pct_indexed NULLS LAST, site_id
    """
)


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--days", type=int, default=90, help="mart_sensor_daily lookback")
    p.add_argument(
        "--min-pct",
        type=float,
        default=10.0,
        help="exit 1 if any site with strings_with_orient>0 is below this %% (default 10)",
    )
    p.add_argument("--failures-only", action="store_true", help="print only sites below min-pct")
    p.add_argument(
        "--strict",
        action="store_true",
        help="exit with code 1 if any site is below min-pct (default: always exit 0)",
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

    bad: list[dict] = []
    with engine.connect() as conn:
        rows = conn.execute(SQL, {"days": args.days}).mappings().all()

    for r in rows:
        d = dict(r)
        pct = d.get("pct_indexed")
        sw = d.get("strings_with_orient") or 0
        if args.failures_only:
            if sw > 0 and pct is not None and float(pct) < args.min_pct:
                bad.append(d)
                print(d)
            elif sw > 0 and pct is None:
                bad.append(d)
                print(d)
        else:
            print(d)
            if sw > 0 and pct is not None and float(pct) < args.min_pct:
                bad.append(d)

    print(
        {
            "sites_total": len(rows),
            "sites_below_min_pct": len(bad),
            "min_pct": args.min_pct,
            "lookback_days": args.days,
        }
    )

    if bad and args.strict:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
