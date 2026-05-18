"""
Build a standalone HTML report for mart.mart_string_performance_daily
for a calendar month, for selected sites (default: MMKI1, GM1, Mall Panakkukang).

Usage:
  python scripts/build_string_mart_april_html_report.py
  python scripts/build_string_mart_april_html_report.py --year 2026 --month 4 --out reports/foo.html
"""

from __future__ import annotations

import argparse
import html
import os
import urllib.parse
from datetime import date
from pathlib import Path

from dotenv import load_dotenv
from sqlalchemy import create_engine, text

ROOT = Path(__file__).resolve().parents[1]

DEFAULT_SITES: dict[str, tuple[str, str]] = {
    "mmki1": ("NE=50488260", "PT. MMKI 1.75 MWp - Painting Building"),
    "gm1": ("1458125", "Garuda Metalindo 1"),
    "mall": ("NE=53771627", "PLTS Mall Panakkukang"),
}


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--year", type=int, default=2026)
    p.add_argument("--month", type=int, default=4)
    p.add_argument(
        "--out",
        type=Path,
        default=None,
        help="output HTML path (default: reports/string_mart_{y}_{m:02d}_mmki1_gm1_mall.html)",
    )
    args = p.parse_args()

    start = date(args.year, args.month, 1)
    if args.month == 12:
        end = date(args.year + 1, 1, 1)
    else:
        end = date(args.year, args.month + 1, 1)

    out_path = args.out
    if out_path is None:
        out_path = (
            ROOT
            / "reports"
            / f"string_mart_{args.year}_{args.month:02d}_mmki1_gm1_mall.html"
        )
    out_path.parent.mkdir(parents=True, exist_ok=True)

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

    site_ids = [t[0] for t in DEFAULT_SITES.values()]

    sql = text(
        """
        SELECT
            site_id::text AS site_id,
            MAX(site_name::text) AS site_name,
            date_key::text AS date_key,
            COUNT(*) AS n_rows,
            ROUND(SUM(daily_energy_kwh)::numeric, 3) AS sum_energy_kwh,
            ROUND(
                100.0 * COUNT(*) FILTER (
                    WHERE string_dc_capacity_kw_stc IS NOT NULL
                      AND string_dc_capacity_kw_stc > 0
                )::numeric / NULLIF(COUNT(*), 0),
                1
            ) AS pct_cap,
            ROUND(
                100.0 * COUNT(daily_poa_kwh_m2) FILTER (WHERE daily_poa_kwh_m2 IS NOT NULL)::numeric
                / NULLIF(COUNT(*), 0),
                1
            ) AS pct_poa,
            ROUND(
                100.0 * COUNT(daily_ghi_kwh_m2) FILTER (WHERE daily_ghi_kwh_m2 IS NOT NULL)::numeric
                / NULLIF(COUNT(*), 0),
                1
            ) AS pct_ghi,
            ROUND(
                100.0 * COUNT(pr_ghi_string) FILTER (WHERE pr_ghi_string IS NOT NULL)::numeric
                / NULLIF(COUNT(*), 0),
                1
            ) AS pct_pr_ghi,
            ROUND(
                100.0 * COUNT(pr_poa_string) FILTER (WHERE pr_poa_string IS NOT NULL)::numeric
                / NULLIF(COUNT(*), 0),
                1
            ) AS pct_pr_poa
        FROM mart.mart_string_performance_daily
        WHERE date_key >= :start
          AND date_key < :end
          AND site_id::text = ANY(:sids)
        GROUP BY site_id, date_key
        ORDER BY site_id, date_key
        """
    )

    with engine.connect() as conn:
        rows = conn.execute(sql, {"start": start, "end": end, "sids": site_ids}).fetchall()

    by_site: dict[str, list] = {sid: [] for sid in site_ids}
    for r in rows:
        m = dict(r._mapping)
        by_site[m["site_id"]].append(m)

    # Month totals per site
    sql_tot = text(
        """
        SELECT
            site_id::text AS site_id,
            MAX(site_name::text) AS site_name,
            COUNT(*) AS n_rows,
            ROUND(SUM(daily_energy_kwh)::numeric, 3) AS sum_energy_kwh,
            ROUND(
                100.0 * COUNT(*) FILTER (
                    WHERE string_dc_capacity_kw_stc IS NOT NULL
                      AND string_dc_capacity_kw_stc > 0
                )::numeric / NULLIF(COUNT(*), 0),
                1
            ) AS pct_cap,
            ROUND(
                100.0 * COUNT(daily_poa_kwh_m2) FILTER (WHERE daily_poa_kwh_m2 IS NOT NULL)::numeric
                / NULLIF(COUNT(*), 0),
                1
            ) AS pct_poa,
            ROUND(
                100.0 * COUNT(daily_ghi_kwh_m2) FILTER (WHERE daily_ghi_kwh_m2 IS NOT NULL)::numeric
                / NULLIF(COUNT(*), 0),
                1
            ) AS pct_ghi,
            ROUND(
                100.0 * COUNT(pr_ghi_string) FILTER (WHERE pr_ghi_string IS NOT NULL)::numeric
                / NULLIF(COUNT(*), 0),
                1
            ) AS pct_pr_ghi,
            ROUND(
                100.0 * COUNT(pr_poa_string) FILTER (WHERE pr_poa_string IS NOT NULL)::numeric
                / NULLIF(COUNT(*), 0),
                1
            ) AS pct_pr_poa
        FROM mart.mart_string_performance_daily
        WHERE date_key >= :start
          AND date_key < :end
          AND site_id::text = ANY(:sids)
        GROUP BY site_id
        ORDER BY site_id
        """
    )
    with engine.connect() as conn:
        totals = [dict(x._mapping) for x in conn.execute(sql_tot, {"start": start, "end": end, "sids": site_ids})]

    def esc(s: object) -> str:
        return html.escape(str(s) if s is not None else "")

    parts = [
        "<!DOCTYPE html>",
        '<html lang="id">',
        "<head>",
        '<meta charset="utf-8"/>',
        f"<title>String mart — {args.year}-{args.month:02d} — MMKI1, GM1, Mall</title>",
        """<style>
body { font-family: system-ui, Segoe UI, Roboto, sans-serif; margin: 24px; color: #1a1a1a; max-width: 1200px; }
h1 { font-size: 1.35rem; }
h2 { font-size: 1.1rem; margin-top: 2rem; border-bottom: 1px solid #ccc; padding-bottom: 6px; }
p.note { color: #444; font-size: 0.9rem; }
table { border-collapse: collapse; width: 100%; font-size: 0.85rem; margin-top: 8px; }
th, td { border: 1px solid #ddd; padding: 6px 8px; text-align: right; }
th:first-child, td:first-child { text-align: left; }
th { background: #f4f4f4; }
tr:nth-child(even) { background: #fafafa; }
.summary { background: #e8f4e8; font-weight: 600; }
.bad { color: #a30; }
.good { color: #060; }
</style>""",
        "</head>",
        "<body>",
        f"<h1>mart_string_performance_daily — {args.year}-{args.month:02d} (full month)</h1>",
        "<p class=\"note\">Grain harian: agregasi semua baris (date × inverter × string) per site. "
        "Persentase = % baris dengan nilai non-null (capacity &gt; 0; POA/GHI/PR non-null).</p>",
    ]

    for t in totals:
        parts.append("<h2>Ringkasan bulan — " + esc(t["site_name"]) + "</h2>")
        parts.append("<table>")
        parts.append(
            "<tr><th>site_id</th><th>Baris total</th><th>Σ energy (kWh)</th>"
            "<th>% cap</th><th>% POA</th><th>% GHI</th><th>% PR GHI</th><th>% PR POA</th></tr>"
        )
        parts.append(
            f"<tr class='summary'>"
            f"<td>{esc(t['site_id'])}</td>"
            f"<td>{esc(t['n_rows'])}</td>"
            f"<td>{esc(t['sum_energy_kwh'])}</td>"
            f"<td>{esc(t['pct_cap'])}</td>"
            f"<td>{esc(t['pct_poa'])}</td>"
            f"<td>{esc(t['pct_ghi'])}</td>"
            f"<td>{esc(t['pct_pr_ghi'])}</td>"
            f"<td>{esc(t['pct_pr_poa'])}</td>"
            f"</tr>"
        )
        parts.append("</table>")

    order_label = [
        ("NE=50488260", "MMKI 1"),
        ("1458125", "GM 1"),
        ("NE=53771627", "Mall Panakkukang"),
    ]
    for sid, label in order_label:
        days = by_site.get(sid, [])
        title = label + " — " + sid
        if days:
            title = days[0]["site_name"] + " (" + label + ")"
        parts.append(f"<h2>{esc(title)}</h2>")
        if not days:
            parts.append("<p class=\"note bad\">Tidak ada data di rentang ini.</p>")
            continue
        parts.append("<table>")
        parts.append(
            "<tr><th>Tanggal</th><th># baris</th><th>Σ kWh</th>"
            "<th>% cap</th><th>% POA</th><th>% GHI</th><th>% PR GHI</th><th>% PR POA</th></tr>"
        )
        for d in days:
            parts.append(
                f"<tr>"
                f"<td>{esc(d['date_key'])}</td>"
                f"<td>{esc(d['n_rows'])}</td>"
                f"<td>{esc(d['sum_energy_kwh'])}</td>"
                f"<td>{esc(d['pct_cap'])}</td>"
                f"<td>{esc(d['pct_poa'])}</td>"
                f"<td>{esc(d['pct_ghi'])}</td>"
                f"<td>{esc(d['pct_pr_ghi'])}</td>"
                f"<td>{esc(d['pct_pr_poa'])}</td>"
                f"</tr>"
            )
        parts.append("</table>")

    parts.append(
        "<p class=\"note\">Dibuat oleh <code>scripts/build_string_mart_april_html_report.py</code> — "
        "buka file ini di browser.</p>"
    )
    parts.extend(["</body>", "</html>"])

    out_path.write_text("\n".join(parts), encoding="utf-8")
    print(f"wrote {out_path}")


if __name__ == "__main__":
    main()
