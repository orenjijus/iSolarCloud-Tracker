"""
Export IEM3255 revenue-meter snapshots at 23:55 WIB (Asia/Jakarta) per site/day.

Output columns:
  Site | Energy Meter (IEM3255) | Tanggal | Negative active | Positive Active | Daily GHI

Daily GHI: from mart.mart_sensor_daily (sensor_type = 'GHI'), COALESCE horizontal, plane daily).
Requires .env with POSTGRES_* (same as harvesters) or DATABASE_URL.
"""
from __future__ import annotations

import argparse
import os
import urllib.parse
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

ROOT = Path(__file__).resolve().parents[1]


def _database_url() -> str:
    for p in (ROOT / ".env", ROOT / "fusionsolar" / ".env", ROOT / "isolarcloud" / ".env"):
        load_dotenv(p, override=False)
    if url := os.getenv("DATABASE_URL"):
        return url
    host = os.getenv("POSTGRES_HOST", "10.101.4.88")
    port = os.getenv("POSTGRES_PORT", "5432")
    db = os.getenv("POSTGRES_DB", "MMSR")
    user = os.getenv("POSTGRES_USER", "juice")
    pw = os.getenv("POSTGRES_PASSWORD") or ""
    enc = urllib.parse.quote_plus(pw) if pw else ""
    return f"postgresql://{user}:{enc}@{host}:{port}/{db}"


SQL = text(
    """
WITH iem AS (
    SELECT DISTINCT esn_code, dev_name, site_id
    FROM staging.seed_meter_config
    WHERE dev_name ILIKE '%IEM3255%'
      AND UPPER(TRIM(source)) = 'FUSIONSOLAR'
),
snap AS (
    SELECT
        m.site_name,
        m.meter_dev_name,
        (m.timestamp AT TIME ZONE 'Asia/Jakarta')::date AS tanggal,
        m.metric_name,
        m.metric_value
    FROM mart.mart_meter_performance_5min m
    JOIN iem i ON m.asset_id = 'FS_' || i.esn_code
    WHERE m.metric_name IN ('negative_active_energy', 'positive_active_energy')
      AND (m.timestamp AT TIME ZONE 'Asia/Jakarta')::time = TIME '23:55:00'
      AND (:dfrom IS NULL OR (m.timestamp AT TIME ZONE 'Asia/Jakarta')::date >= CAST(:dfrom AS date))
      AND (:dto IS NULL OR (m.timestamp AT TIME ZONE 'Asia/Jakarta')::date <= CAST(:dto AS date))
),
pivot AS (
    SELECT
        site_name,
        meter_dev_name,
        tanggal,
        MAX(metric_value) FILTER (WHERE metric_name = 'negative_active_energy') AS negative_active,
        MAX(metric_value) FILTER (WHERE metric_name = 'positive_active_energy') AS positive_active
    FROM snap
    GROUP BY site_name, meter_dev_name, tanggal
),
ghi AS (
    SELECT
        site_name,
        date_key::date AS d,
        MAX(COALESCE(daily_horizontal_irradiation_kwh_m2, daily_irradiance_kwh_m2)) AS daily_ghi
    FROM mart.mart_sensor_daily
    WHERE sensor_type = 'GHI'
    GROUP BY site_name, date_key::date
)
SELECT
    p.site_name AS "Site",
    p.meter_dev_name AS "Energy Meter (IEM3255)",
    p.tanggal AS "Tanggal",
    p.negative_active AS "Negative active",
    p.positive_active AS "Positive Active",
    g.daily_ghi AS "Daily GHI"
FROM pivot p
LEFT JOIN ghi g ON g.site_name = p.site_name AND g.d = p.tanggal
ORDER BY p.site_name, p.meter_dev_name, p.tanggal
"""
)


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--from-date", dest="dfrom", default=None, help="Inclusive start date YYYY-MM-DD (Jakarta calendar)")
    ap.add_argument("--to-date", dest="dto", default=None, help="Inclusive end date YYYY-MM-DD")
    ap.add_argument(
        "-o",
        "--output",
        type=Path,
        default=ROOT / "data" / "exports" / "iem3255_meter_2355_wib_all_sites.csv",
    )
    args = ap.parse_args()

    out: Path = args.output
    out.parent.mkdir(parents=True, exist_ok=True)

    eng = create_engine(_database_url())
    df = pd.read_sql(
        SQL,
        eng,
        params={"dfrom": args.dfrom, "dto": args.dto},
    )
    if df.empty:
        raise SystemExit("No rows returned (check seed_meter_config IEM3255 + mart data).")
    df.to_csv(out, index=False, encoding="utf-8-sig")
    print(f"Wrote {len(df)} rows to {out}")


if __name__ == "__main__":
    main()
