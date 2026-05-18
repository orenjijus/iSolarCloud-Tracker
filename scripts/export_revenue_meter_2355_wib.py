"""
Export Revenue-meter (mart patokan) snapshots at 23:55 WIB per site / meter / day.

Default: all sites with seed_meter_config.meter_type = 'Revenue', April calendar month.

Columns:
  Site | Energy Meter (Revenue) | Tanggal | Negative active | Positive Active | Daily GHI

Optional: --model-substring IEM3255 to restrict dev_name (e.g. only Pusan meters).

Requires .env (POSTGRES_*) under repo root, fusionsolar/, or isolarcloud/.
"""
from __future__ import annotations

import argparse
import calendar
import os
import urllib.parse
from datetime import date
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


def _month_bounds(year: int, month: int) -> tuple[date, date]:
    last = calendar.monthrange(year, month)[1]
    return date(year, month, 1), date(year, month, last)


SQL = text(
    """
WITH rev AS (
    SELECT
        mc.dev_name,
        mc.esn_code,
        mc.site_id AS plant_or_ps_ref,
        mc.source,
        CASE UPPER(TRIM(mc.source))
            WHEN 'FUSIONSOLAR' THEN 'FS_' || mc.esn_code
            WHEN 'ISOLARCLOUD' THEN 'ISO_' || mc.esn_code
        END AS asset_id
    FROM staging.seed_meter_config mc
    WHERE mc.meter_type = 'Revenue'
      AND UPPER(TRIM(mc.source)) IN ('FUSIONSOLAR', 'ISOLARCLOUD')
      AND (:model_sub = '' OR mc.dev_name ILIKE '%' || :model_sub || '%')
),
snap AS (
    SELECT
        m.site_name,
        m.meter_dev_name,
        (m.timestamp AT TIME ZONE 'Asia/Jakarta')::date AS tanggal,
        m.metric_name,
        m.metric_value
    FROM mart.mart_meter_performance_5min m
    JOIN rev r ON m.asset_id = r.asset_id
    WHERE m.metric_name IN ('negative_active_energy', 'positive_active_energy')
      AND (m.timestamp AT TIME ZONE 'Asia/Jakarta')::time = TIME '23:55:00'
      AND (m.timestamp AT TIME ZONE 'Asia/Jakarta')::date >= CAST(:dfrom AS date)
      AND (m.timestamp AT TIME ZONE 'Asia/Jakarta')::date <= CAST(:dto AS date)
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
      AND date_key::date >= CAST(:dfrom AS date)
      AND date_key::date <= CAST(:dto AS date)
    GROUP BY site_name, date_key::date
)
SELECT
    p.site_name AS "Site",
    p.meter_dev_name AS "Energy Meter (Revenue)",
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
    ap.add_argument("--year", type=int, default=2026, help="Calendar year for --april (default 2026)")
    ap.add_argument(
        "--april",
        action="store_true",
        help="Shorthand: entire April of --year (overrides --from-date/--to-date)",
    )
    ap.add_argument("--from-date", dest="dfrom", default=None, help="YYYY-MM-DD inclusive (WIB calendar date)")
    ap.add_argument("--to-date", dest="dto", default=None, help="YYYY-MM-DD inclusive")
    ap.add_argument(
        "--model-substring",
        dest="model_sub",
        default=None,
        help="Optional filter on seed dev_name (e.g. IEM3255)",
    )
    ap.add_argument(
        "-o",
        "--output",
        type=Path,
        default=None,
        help="Output CSV path",
    )
    args = ap.parse_args()

    if args.april:
        dfrom, dto = _month_bounds(args.year, 4)
    else:
        if not args.dfrom or not args.dto:
            raise SystemExit("Provide --april or both --from-date and --to-date")
        dfrom = date.fromisoformat(args.dfrom)
        dto = date.fromisoformat(args.dto)

    out = args.output
    if out is None:
        out = (
            ROOT
            / "data"
            / "exports"
            / f"revenue_meters_2355_wib_{dfrom.isoformat()}_to_{dto.isoformat()}.csv"
        )
    out.parent.mkdir(parents=True, exist_ok=True)

    eng = create_engine(_database_url())
    df = pd.read_sql(
        SQL,
        eng,
        params={
            "dfrom": dfrom.isoformat(),
            "dto": dto.isoformat(),
            "model_sub": (args.model_sub or ""),
        },
    )
    if df.empty:
        raise SystemExit("No rows (check Revenue seed + mart_meter_performance_5min for date range).")
    df.to_csv(out, index=False, encoding="utf-8-sig")
    print(f"Wrote {len(df)} rows to {out}")


if __name__ == "__main__":
    main()
