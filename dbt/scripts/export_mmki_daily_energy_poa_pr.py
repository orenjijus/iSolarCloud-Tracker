"""Export MMKI 1/2/3 + Total MMKI (energy sum, POA avg, PR from sum E / avg POA / sum cap) to CSV."""
from __future__ import annotations

import csv
import sys
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[1]
PROFILES = ROOT / "profiles.yml"


def load_conn():
    cfg = yaml.safe_load(PROFILES.read_text(encoding="utf-8"))["mmsr_solar"]["outputs"]["dev"]
    import psycopg2

    return psycopg2.connect(
        host=cfg["host"],
        user=cfg["user"],
        password=cfg["password"],
        port=cfg["port"],
        dbname=cfg["dbname"],
    )


SQL = """
WITH base AS (
    SELECT
        (m.date_key AT TIME ZONE 'Asia/Jakarta')::date AS cal_date,
        CASE m.site_name
            WHEN 'PT. MMKI 1.75 MWp - Painting Building' THEN 'MMKI 1'
            WHEN 'PT. MMKI 5.7 MWp - Phase 2' THEN 'MMKI 2'
            WHEN 'PT. MMKI 4.292 MWP - Phase 3' THEN 'MMKI 3'
        END AS site,
        m.daily_energy_mwh::double precision AS energy_mwh,
        m.daily_poa_weighted_kwh_m2::double precision AS poa_kwh_m2,
        m.pr_poa_actual::double precision AS pr_poa,
        m.actual_capacity_kw::double precision AS cap_kw
    FROM mart.mart_site_performance_daily m
    WHERE m.site_name IN (
        'PT. MMKI 1.75 MWp - Painting Building',
        'PT. MMKI 5.7 MWp - Phase 2',
        'PT. MMKI 4.292 MWP - Phase 3'
    )
),
by_day AS (
    SELECT
        cal_date,
        SUM(energy_mwh) FILTER (WHERE energy_mwh IS NOT NULL) AS sum_energy,
        AVG(poa_kwh_m2) FILTER (WHERE poa_kwh_m2 IS NOT NULL) AS avg_poa,
        SUM(cap_kw) FILTER (WHERE cap_kw IS NOT NULL) AS sum_cap
    FROM base
    GROUP BY cal_date
),
per_site AS (
    SELECT
        cal_date,
        site,
        energy_mwh,
        poa_kwh_m2,
        pr_poa,
        CASE site
            WHEN 'MMKI 1' THEN 1
            WHEN 'MMKI 2' THEN 2
            WHEN 'MMKI 3' THEN 3
        END AS ord
    FROM base
),
total_row AS (
    SELECT
        d.cal_date,
        'Total MMKI'::text AS site,
        d.sum_energy AS energy_mwh,
        d.avg_poa AS poa_kwh_m2,
        CASE
            WHEN d.avg_poa IS NOT NULL AND d.avg_poa >= 0.1
                AND d.sum_cap IS NOT NULL AND d.sum_cap > 0
                AND d.sum_energy IS NOT NULL AND d.sum_energy >= 0.01
            THEN (d.sum_energy * 1000.0) / d.avg_poa / d.sum_cap
        END AS pr_poa,
        4 AS ord
    FROM by_day d
)
SELECT cal_date, site, energy_mwh, poa_kwh_m2, pr_poa, ord
FROM per_site
UNION ALL
SELECT cal_date, site, energy_mwh, poa_kwh_m2, pr_poa, ord
FROM total_row
ORDER BY cal_date, ord;
"""


def main() -> int:
    out = Path(sys.argv[1]) if len(sys.argv) > 1 else ROOT.parent / "exports" / "mmki_daily_energy_poa_pr.csv"
    out.parent.mkdir(parents=True, exist_ok=True)

    with load_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(SQL)
            rows = cur.fetchall()

    with out.open("w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["date", "Site", "Energy Daily (MWh)", "POA Daily (kWh/m2)", "PR POA"])
        for cal_date, site, energy_mwh, poa_kwh_m2, pr_poa, _ord in rows:
            w.writerow(
                [
                    cal_date.isoformat() if cal_date else "",
                    site or "",
                    f"{energy_mwh:.6f}" if energy_mwh is not None else "",
                    f"{poa_kwh_m2:.6f}" if poa_kwh_m2 is not None else "",
                    f"{pr_poa:.6f}" if pr_poa is not None else "",
                ]
            )

    print(f"Wrote {len(rows)} rows to {out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
