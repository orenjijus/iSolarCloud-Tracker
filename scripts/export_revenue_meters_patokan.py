"""Export distinct Revenue meters (mart_site_performance_daily patokan) from mart + seed."""
from __future__ import annotations

import os
import urllib.parse
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

ROOT = Path(__file__).resolve().parents[1]

SQL = text(
    """
WITH rev AS (
    SELECT
        mc.*,
        CASE
            WHEN UPPER(TRIM(mc.source)) = 'FUSIONSOLAR' THEN 'FS_' || mc.esn_code
            WHEN UPPER(TRIM(mc.source)) = 'ISOLARCLOUD' THEN 'ISO_' || mc.esn_code
        END AS asset_id
    FROM staging.seed_meter_config mc
    WHERE mc.meter_type = 'Revenue'
)
SELECT DISTINCT ON (r.asset_id)
    m.site_name,
    m.system,
    m.asset_id,
    r.dev_name AS meter_dev_name_seed,
    r.esn_code,
    r.site_id AS plant_or_ps_ref,
    r.source AS meter_config_source
FROM rev r
JOIN mart.mart_meter_performance_5min m ON m.asset_id = r.asset_id
ORDER BY r.asset_id, m.site_name
"""
)


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


def main() -> None:
    out = ROOT / "data" / "exports" / "revenue_meters_mart_patokan.csv"
    out.parent.mkdir(parents=True, exist_ok=True)
    eng = create_engine(_database_url())
    pd.read_sql(SQL, eng).to_csv(out, index=False, encoding="utf-8-sig")
    print(out)


if __name__ == "__main__":
    main()
