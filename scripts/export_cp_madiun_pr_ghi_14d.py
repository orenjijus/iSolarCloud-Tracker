"""Export CP Madiun PR GHI (14 hari terakhir di mart) untuk audit / Google Sheets."""

from __future__ import annotations

import csv
import os
from pathlib import Path

import psycopg2

SITE = "Charoen Pokphand Madiun"
OUT_DIR = Path(__file__).resolve().parents[1] / "data" / "exports"


def connect():
    return psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "10.101.4.88"),
        port=int(os.getenv("POSTGRES_PORT", "5432")),
        dbname=os.getenv("POSTGRES_DB", "MMSR"),
        user=os.getenv("POSTGRES_USER", "juice"),
        password=os.getenv("POSTGRES_PASSWORD", "Baramulti1!"),
    )


def main() -> None:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    conn = connect()
    cur = conn.cursor()

    cur.execute(
        """
        WITH b AS (
          SELECT MAX(date_key) AS dmax
          FROM mart.mart_string_performance_daily
          WHERE site_name = %s
        )
        SELECT
          m.date_key::date AS tanggal,
          round(100.0 * avg(m.pr_ghi_string)::numeric, 3) AS site_avg_pr_ghi_pct,
          round(avg(m.daily_ghi_kwh_m2)::numeric, 4) AS site_avg_daily_ghi_kwh_m2,
          round(sum(m.daily_energy_kwh)::numeric, 2) AS site_total_energy_kwh,
          count(*)::int AS string_rows_per_day
        FROM mart.mart_string_performance_daily m, b
        WHERE m.site_name = %s
          AND m.date_key BETWEEN b.dmax - INTERVAL '13 day' AND b.dmax
        GROUP BY m.date_key
        ORDER BY m.date_key
        """,
        (SITE, SITE),
    )
    daily_rows = cur.fetchall()

    f1 = OUT_DIR / "cp_madiun_pr_ghi_14d_daily_summary.csv"
    with f1.open("w", newline="", encoding="utf-8-sig") as f:
        w = csv.writer(f)
        w.writerow(["Site", SITE])
        w.writerow(
            [
                "Catatan",
                "14 hari = window relatif MAX(date_key) di mart.mart_string_performance_daily untuk site ini.",
            ]
        )
        w.writerow([])
        w.writerow(
            [
                "tanggal",
                "site_avg_pr_ghi_pct",
                "site_avg_daily_ghi_kwh_m2",
                "site_total_energy_kwh",
                "string_row_count",
            ]
        )
        w.writerows(daily_rows)

    cur.execute(
        """
        WITH b AS (
          SELECT MAX(date_key) AS dmax
          FROM mart.mart_string_performance_daily
          WHERE site_name = %s
        )
        SELECT
          m.date_key::date AS tanggal,
          m.inverter_name,
          m.string_number,
          round(m.daily_energy_kwh::numeric, 4) AS daily_energy_kwh,
          round(m.daily_ghi_kwh_m2::numeric, 6) AS daily_ghi_kwh_m2,
          round((m.pr_ghi_string * 100)::numeric, 3) AS pr_ghi_pct,
          round((COALESCE(m.pr_poa_string, 0) * 100)::numeric, 3) AS pr_poa_pct,
          round(m.string_dc_capacity_kw_stc::numeric, 4) AS string_dc_kw_stc
        FROM mart.mart_string_performance_daily m, b
        WHERE m.site_name = %s
          AND m.date_key BETWEEN b.dmax - INTERVAL '13 day' AND b.dmax
        ORDER BY m.date_key, m.inverter_name, m.string_number
        """,
        (SITE, SITE),
    )
    detail = cur.fetchall()

    f2 = OUT_DIR / "cp_madiun_pr_ghi_14d_string_detail.csv"
    with f2.open("w", newline="", encoding="utf-8-sig") as f:
        w = csv.writer(f)
        w.writerow(["Site", SITE])
        if daily_rows:
            w.writerow(["Periode", f"{daily_rows[0][0]} s/d {daily_rows[-1][0]}"])
        w.writerow([])
        w.writerow(
            [
                "tanggal",
                "inverter_name",
                "string_number",
                "daily_energy_kwh",
                "daily_ghi_kwh_m2",
                "pr_ghi_pct",
                "pr_poa_pct",
                "string_dc_kw_stc",
            ]
        )
        w.writerows(detail)

    cur.execute(
        """
        WITH b AS (
          SELECT MAX(date_key) AS dmax
          FROM mart.mart_string_performance_daily
          WHERE site_name = %s
        ),
        win AS (
          SELECT m.*
          FROM mart.mart_string_performance_daily m, b
          WHERE m.site_name = %s
            AND m.date_key BETWEEN b.dmax - INTERVAL '13 day' AND b.dmax
        ),
        agg AS (
          SELECT
            inverter_name,
            string_number,
            avg(pr_ghi_string) AS avg_pr_ghi,
            count(*) FILTER (WHERE daily_energy_kwh IS NULL OR daily_energy_kwh <= 0) AS days_zero_energy,
            count(*) AS days_present
          FROM win
          GROUP BY inverter_name, string_number
        )
        SELECT
          inverter_name,
          string_number,
          round((avg_pr_ghi * 100)::numeric, 3) AS avg_pr_ghi_pct_14d,
          days_zero_energy,
          days_present
        FROM agg
        WHERE avg_pr_ghi IS NOT NULL
        ORDER BY avg_pr_ghi ASC NULLS LAST
        LIMIT 25
        """,
        (SITE, SITE),
    )
    bottom = cur.fetchall()

    f3 = OUT_DIR / "cp_madiun_pr_ghi_14d_lowest_strings.csv"
    with f3.open("w", newline="", encoding="utf-8-sig") as f:
        w = csv.writer(f)
        w.writerow(["Site", SITE])
        w.writerow(
            [
                "Keterangan",
                "25 string dengan rata-rata PR GHI terendah; prioritas inspeksi/cleaning.",
            ]
        )
        w.writerow([])
        w.writerow(
            [
                "inverter_name",
                "string_number",
                "avg_pr_ghi_pct_14d",
                "days_zero_energy",
                "days_in_period",
            ]
        )
        w.writerows(bottom)

    cur.close()
    conn.close()
    print(f"Wrote:\n  {f1}\n  {f2}\n  {f3}")
    print(f"Rows daily: {len(daily_rows)} detail: {len(detail)} lowest_strings: {len(bottom)}")


if __name__ == "__main__":
    main()
