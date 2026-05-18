"""
Generate static HTML PR String Audit report from mart.mart_string_performance_daily.

PR display: mart stores pr_* as (daily_energy_kwh/1000)/(irr*kW); multiply by 100000 for %.
"""

from __future__ import annotations

import argparse
import json
import os
import re
from collections import defaultdict
from datetime import date, datetime, timedelta
from statistics import mean
from typing import Any

import psycopg2
import yaml
from psycopg2.extras import RealDictCursor

SQL = """
SELECT
    date_key::date AS d,
    inverter_id,
    inverter_name,
    string_number,
    site_name,
    CASE
        WHEN pr_poa_string IS NOT NULL AND (pr_poa_string * 100000.0) <= 200
            THEN (pr_poa_string * 100000.0)::double precision
        WHEN pr_poa_string IS NOT NULL AND (pr_poa_string * 100000.0) > 200
            THEN (pr_poa_string * 100.0)::double precision   -- fallback: stored as fraction 0-1
        WHEN pr_ghi_string IS NOT NULL AND (pr_ghi_string * 100000.0) <= 200
            THEN (pr_ghi_string * 100000.0)::double precision
        WHEN pr_ghi_string IS NOT NULL AND (pr_ghi_string * 100000.0) > 200
            THEN (pr_ghi_string * 100.0)::double precision
        ELSE NULL
    END AS pr_pct
FROM mart.mart_string_performance_daily
WHERE site_id = %s
  AND date_key >= %s::date
  AND date_key <= %s::date
ORDER BY date_key, inverter_id, string_number;
"""

SQL_CLEANING = """
SELECT cleaning_date::date AS d, COALESCE(notes, '') AS notes
FROM staging.cleaning_log
WHERE is_active = 1
  AND (site_id = %s OR site_id = 'ISO_SITE_' || %s)
  AND cleaning_date >= %s::date
  AND cleaning_date <= %s::date
ORDER BY cleaning_date;
"""


def str_label(n: int) -> str:
    return f"S{n:02d}"


def inv_display_name(inverter_name: str | None, inverter_id: str) -> str:
    """Human-friendly inverter label (bukan index ISO 1..6 yang loncat)."""
    raw = (inverter_name or "").strip()
    if not raw:
        m = re.search(r"_1_(\d+)_1$", inverter_id or "")
        if m:
            return f"Inv #{m.group(1)}"
        return inverter_id[:28]
    # Rapikan: "Inverter 102" -> "Inv 102", "Inverter.102" -> "Inv 102"
    s = re.sub(r"^inverter\.?\s*", "Inv ", raw, flags=re.I)
    s = re.sub(r"^inverter\s+", "Inv ", s, flags=re.I)
    if not s.lower().startswith("inv"):
        s = "Inv " + s
    s = re.sub(r"\s+", " ", s).strip()
    return s


def inv_sort_tuple(display: str, inverter_id: str) -> tuple:
    """Urut inverter untuk heatmap/tabel: angka di label tampilan (max digit group)."""
    nums = re.findall(r"\d+", display)
    if nums:
        return (0, max(int(n) for n in nums), display.lower())
    m = re.search(r"_1_(\d+)_1$", inverter_id or "")
    if m:
        return (1, int(m.group(1)), display.lower())
    return (2, 0, display.lower())


def heatmap_color(
    pr: float | None,
    lo: float,
    hi: float,
) -> tuple[str, str]:
    """Return (css background, text color) for heatmap cell."""
    if pr is None:
        return ("rgba(30,41,59,0.85)", "var(--muted)")
    if hi <= lo:
        t = 0.5
    else:
        t = max(0.0, min(1.0, (pr - lo) / (hi - lo)))
    # biru gelap -> cyan -> kuning -> hijau
    if t < 0.33:
        r, g, b = 30 + t * 100, 50 + t * 120, 120 + t * 80
    elif t < 0.66:
        r, g, b = 50 + (t - 0.33) * 200, 150 + (t - 0.33) * 80, 180
    else:
        r, g, b = 34 + (t - 0.66) * 80, 197 - (t - 0.66) * 40, 94 + (t - 0.66) * 30
    txt = "#e8edf5" if t < 0.45 else "#0f172a"
    return (f"rgba({int(r)},{int(g)},{int(b)},0.92)", txt)


def rolling7(vals: list[float | None]) -> list[float | None]:
    out: list[float | None] = []
    for i in range(len(vals)):
        if i < 6:
            out.append(None)
            continue
        window = [vals[j] for j in range(i - 6, i + 1) if vals[j] is not None]
        out.append(mean(window) if len(window) == 7 else None)
    return out


def load_dbt_db() -> dict[str, Any]:
    path = os.path.expanduser("~/.dbt/profiles.yml")
    with open(path, encoding="utf-8") as f:
        profiles = yaml.safe_load(f)
    return profiles["mmsr_solar"]["outputs"]["dev"]


def default_start(months_back: int = 6) -> str:
    today = date.today()
    # First day of the month `months_back` months ago
    y, m = today.year, today.month
    m -= months_back
    while m <= 0:
        m += 12
        y -= 1
    return date(y, m, 1).isoformat()


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--site-id", default="1458125")
    ap.add_argument("--start", default=None, help="YYYY-MM-DD; defaults to 6 months back")
    ap.add_argument("--end", default=None, help="YYYY-MM-DD; defaults to today")
    ap.add_argument("--months-back", type=int, default=6, help="Used for default --start")
    ap.add_argument(
        "--out",
        default=os.path.join(
            os.path.dirname(__file__),
            "..",
            "reports",
            "pr_string_audit.html",
        ),
    )
    args = ap.parse_args()
    if not args.start:
        args.start = default_start(args.months_back)
    if not args.end:
        args.end = date.today().isoformat()

    cfg = load_dbt_db()
    conn = psycopg2.connect(
        host=cfg["host"],
        port=cfg["port"],
        user=cfg["user"],
        password=cfg["password"],
        dbname=cfg["dbname"],
    )
    rows: list[dict[str, Any]] = []
    cleaning_dates: list[dict[str, str]] = []
    site_name = ""
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(SQL, (args.site_id, args.start, args.end))
            for r in cur:
                rows.append(dict(r))
                if not site_name and r.get("site_name"):
                    site_name = str(r["site_name"])
            cur.execute(
                SQL_CLEANING,
                (args.site_id, args.site_id, args.start, args.end),
            )
            for r in cur:
                cleaning_dates.append(
                    {"date": r["d"].isoformat(), "notes": str(r["notes"])}
                )
    finally:
        conn.close()

    if not rows:
        raise SystemExit("No rows for given filters.")

    id_to_meta: dict[str, dict[str, str]] = {}
    for r in rows:
        iid = str(r["inverter_id"])
        if iid not in id_to_meta:
            id_to_meta[iid] = {
                "display": inv_display_name(r.get("inverter_name"), iid),
                "name": str(r.get("inverter_name") or ""),
            }

    inv_ids_sorted = sorted(
        id_to_meta.keys(),
        key=lambda x: inv_sort_tuple(id_to_meta[x]["display"], x),
    )

    dates_sorted = sorted({r["d"] for r in rows})
    labels_daily = [d.isoformat() for d in dates_sorted]

    day_to_vals: dict[Any, list[float]] = defaultdict(list)
    for r in rows:
        if r["pr_pct"] is not None:
            day_to_vals[r["d"]].append(float(r["pr_pct"]))
    values_daily = [
        mean(day_to_vals[d]) if day_to_vals.get(d) else None for d in dates_sorted
    ]
    values_roll7 = rolling7(values_daily)

    all_string_keys: set[tuple[str, str, str]] = set()
    string_means: dict[tuple[str, str, str], list[float]] = defaultdict(list)
    for r in rows:
        iid = str(r["inverter_id"])
        disp = id_to_meta[iid]["display"]
        s = str_label(int(r["string_number"]))
        all_string_keys.add((iid, disp, s))
        if r["pr_pct"] is None:
            continue
        string_means[(iid, disp, s)].append(float(r["pr_pct"]))

    mean_by_string = {k: mean(v) for k, v in string_means.items()}
    n_strings_total = len(all_string_keys)
    n_strings_with_pr = len(mean_by_string)
    all_pr_vals = [p for r in rows if r["pr_pct"] is not None for p in [float(r["pr_pct"])]]
    avg_all = mean(all_pr_vals) if all_pr_vals else 0.0
    best = max(mean_by_string.values()) if mean_by_string else 0.0
    worst = min(mean_by_string.values()) if mean_by_string else 0.0

    ranked = sorted(mean_by_string.items(), key=lambda x: x[1], reverse=True)
    top10 = ranked[:10]
    bottom10 = sorted(mean_by_string.items(), key=lambda x: x[1])[:10]

    top_labels = [f"{t[0][1]} {t[0][2]}" for t in top10]
    top_vals = [round(v, 3) for _, v in top10]
    bottom_labels = [f"{t[0][1]} {t[0][2]}" for t in bottom10]
    bottom_vals = [round(v, 3) for _, v in bottom10]

    # invSeries: inverter_id -> str -> series
    inv_series: dict[str, dict[str, list[float | None]]] = defaultdict(
        lambda: defaultdict(lambda: [None] * len(labels_daily))
    )
    day_index = {d: i for i, d in enumerate(dates_sorted)}
    for r in rows:
        iid = str(r["inverter_id"])
        s = str_label(int(r["string_number"]))
        idx = day_index[r["d"]]
        inv_series[iid][s][idx] = round(float(r["pr_pct"]), 3) if r["pr_pct"] is not None else None

    inv_series_out: dict[str, Any] = {
        iid: {
            s: [round(x, 3) if x is not None else None for x in lst]
            for s, lst in sorted(m.items(), key=lambda x: x[0])
        }
        for iid, m in sorted(
            inv_series.items(),
            key=lambda x: inv_sort_tuple(id_to_meta[x[0]]["display"], x[0]),
        )
    }

    # Semua garis untuk satu chart: urut stabil
    all_line_series: list[dict[str, Any]] = []
    for iid in inv_ids_sorted:
        disp = id_to_meta[iid]["display"]
        for s in sorted(inv_series[iid].keys(), key=lambda x: int(x.replace("S", "") or 0)):
            all_line_series.append(
                {
                    "label": f"{disp} {s}",
                    "iid": iid,
                    "str": s,
                    "data": inv_series_out[iid][s],
                }
            )

    str_nums = sorted({int(r["string_number"]) for r in rows})
    str_col_labels = [str_label(n) for n in str_nums]

    # Heatmap: mean PR per (inverter_id, string_number) over window
    cell_sum: dict[tuple[str, int], list[float]] = defaultdict(list)
    for r in rows:
        if r["pr_pct"] is None:
            continue
        cell_sum[(str(r["inverter_id"]), int(r["string_number"]))].append(float(r["pr_pct"]))
    cell_mean: dict[tuple[str, int], float] = {
        k: mean(v) for k, v in cell_sum.items()
    }
    hm_vals = [v for v in cell_mean.values() if v is not None]
    hm_lo = min(hm_vals) if hm_vals else 60.0
    hm_hi = max(hm_vals) if hm_vals else 100.0
    pad = max(3.0, (hm_hi - hm_lo) * 0.08)
    hm_lo = max(0.0, hm_lo - pad)
    hm_hi = hm_hi + pad

    heatmap_rows_html: list[str] = []
    heatmap_rows_html.append(
        "<tr><th class=\"hm-corner\">Inverter \\ String</th>"
        + "".join(f'<th class="hm-col">{h}</th>' for h in str_col_labels)
        + "</tr>"
    )
    for iid in inv_ids_sorted:
        disp = id_to_meta[iid]["display"]
        cells = []
        for sn in str_nums:
            pr = cell_mean.get((iid, sn))
            bg, fg = heatmap_color(pr, hm_lo, hm_hi)
            txt = f"{pr:.1f}" if pr is not None else "—"
            cells.append(
                f'<td class="hm-cell" style="background:{bg};color:{fg}" title="{disp} S{sn:02d}: {txt}%">{txt}</td>'
            )
        heatmap_rows_html.append(
            f'<tr><th class="hm-row">{disp}</th>{"".join(cells)}</tr>'
        )
    heatmap_table = "\n".join(heatmap_rows_html)

    detail_rows = []
    for r in rows:
        iid = str(r["inverter_id"])
        disp = id_to_meta[iid]["display"]
        s = str_label(int(r["string_number"]))
        detail_rows.append(
            {
                "date_str": r["d"].isoformat(),
                "iid": iid,
                "inv": disp,
                "str_label": s,
                "pr_pct": round(float(r["pr_pct"]), 3) if r["pr_pct"] is not None else None,
            }
        )

    id_to_display_json = {iid: id_to_meta[iid]["display"] for iid in inv_ids_sorted}

    # Distinct months in data (sorted) → for month-filter buttons in HTML
    months_in_data: list[str] = []
    seen_months: set[str] = set()
    for d in dates_sorted:
        ym = d.strftime("%Y-%m")
        if ym not in seen_months:
            seen_months.add(ym)
            months_in_data.append(ym)

    _month_abbr = ["", "Jan", "Feb", "Mar", "Apr", "Mei", "Jun",
                   "Jul", "Agu", "Sep", "Okt", "Nov", "Des"]
    def _mo_label(ym: str) -> str:
        y, m = ym.split("-")
        return f"{_month_abbr[int(m)]} {y[2:]}"

    month_buttons = "".join(
        f'<button type="button" class="btn-month" data-ym="{ym}">{_mo_label(ym)}</button>'
        for ym in months_in_data
    )

    title_site = site_name or f"Site {args.site_id}"
    period_txt = f"Periode {args.start} s/d {args.end} · Source: mart.mart_string_performance_daily"
    gen_ts = datetime.now().strftime("%Y-%m-%d %H:%M")

    os.makedirs(os.path.dirname(os.path.abspath(args.out)), exist_ok=True)

    html = f"""<!DOCTYPE html>
<html lang="id">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>PR String Audit — {title_site}</title>
<link href="https://fonts.googleapis.com/css2?family=DM+Serif+Display:ital@0;1&family=DM+Mono:wght@400;500&family=DM+Sans:wght@300;400;500;600&display=swap" rel="stylesheet">
<script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.4/dist/chart.umd.min.js"></script>
<style>
:root {{
  --bg:#0b0f1a;--surface:#111827;--surface2:#1a2234;--border:rgba(255,255,255,0.07);
  --text:#e8edf5;--muted:#6b7a99;--accent:#f59e0b;--a2:#38bdf8;--danger:#ef4444;--success:#22c55e;
}}
*{{box-sizing:border-box;margin:0;padding:0;}}
body {{ background:var(--bg); color:var(--text); font-family:'DM Sans',sans-serif; font-size:14px; line-height:1.55; }}
.stripe{{background:linear-gradient(90deg,#1a1033,#0f1e38,#0b1a2e);border-bottom:1px solid var(--border);padding:0 2rem;display:flex;align-items:center;justify-content:space-between;height:52px;flex-wrap:wrap;gap:.5rem;}}
.sid{{font-family:'DM Mono',monospace;font-size:11px;color:var(--muted);letter-spacing:.12em;text-transform:uppercase;}}
.bdg{{padding:4px 12px;border-radius:4px;font-family:'DM Mono',monospace;font-size:11px;font-weight:500;letter-spacing:.08em;background:rgba(56,189,248,0.15);border:1px solid rgba(56,189,248,0.5);color:var(--a2);}}
.hero{{padding:2.4rem 2rem 1.8rem;background:radial-gradient(ellipse 80% 60% at 50% -10%,rgba(56,189,248,0.08),transparent);border-bottom:1px solid var(--border);}}
.hi{{max-width:1320px;margin:0 auto;}}
.hl{{font-family:'DM Mono',monospace;font-size:11px;color:var(--a2);letter-spacing:.15em;text-transform:uppercase;margin-bottom:.45rem;}}
.hero h1{{font-family:'DM Serif Display',serif;font-size:clamp(1.8rem,3.2vw,2.6rem);color:#fff;line-height:1.14;margin-bottom:.35rem;}}
.hero h1 em{{color:var(--accent);font-style:italic;}}
.hsub{{color:var(--muted);font-size:12px;margin-bottom:.95rem;}}
.pills{{display:flex;gap:.4rem;flex-wrap:wrap;}}
.pill{{display:inline-flex;align-items:center;gap:5px;padding:4px 11px;border-radius:16px;font-size:10px;font-family:'DM Mono',monospace;border:1px solid rgba(255,255,255,0.12);}}
.pill .d{{width:6px;height:6px;border-radius:50%;}}
.main{{max-width:1320px;margin:0 auto;padding:1.5rem 2rem;}}
.dnav{{display:flex;gap:.4rem;flex-wrap:wrap;margin-bottom:1.2rem;}}
.dnav a{{background:var(--surface2);border:1px solid var(--border);color:var(--muted);padding:5px 11px;border-radius:6px;font-size:10px;font-family:'DM Mono',monospace;text-transform:uppercase;letter-spacing:.08em;text-decoration:none;}}
.dnav a:hover{{border-color:var(--a2);color:var(--a2);}}
.st{{font-family:'DM Serif Display',serif;font-size:1.28rem;color:#fff;margin-bottom:1rem;display:flex;align-items:center;gap:.7rem;}}
.num{{font-family:'DM Mono',monospace;font-size:10px;color:var(--muted);background:var(--surface2);border:1px solid var(--border);padding:2px 7px;border-radius:3px;}}
.grid{{display:grid;grid-template-columns:repeat(auto-fit,minmax(200px,1fr));gap:.75rem;margin-bottom:1.15rem;}}
.card{{background:var(--surface);border:1px solid var(--border);border-radius:10px;padding:.95rem 1.05rem;position:relative;overflow:hidden;}}
.card::before{{content:'';position:absolute;top:0;left:0;right:0;height:2px;background:var(--a2);}}
.k{{font-size:10px;text-transform:uppercase;letter-spacing:.1em;color:var(--muted);margin-bottom:.3rem;font-family:'DM Mono',monospace;}}
.v{{font-family:'DM Mono',monospace;font-size:1.65rem;font-weight:500;line-height:1;color:#fff;}}
.section{{background:var(--surface);border:1px solid var(--border);border-radius:10px;padding:1rem;margin-bottom:1rem;}}
.chart{{position:relative;width:100%;height:320px;}}
.chart-tall{{height:520px;}}
.split{{display:grid;grid-template-columns:1fr 1fr;gap:.75rem;}}
.note{{background:rgba(56,189,248,0.08);border:1px solid rgba(56,189,248,0.3);border-radius:10px;padding:.9rem 1rem;margin-bottom:1rem;color:rgba(232,237,245,0.88);font-size:12px;}}
.foot{{text-align:center;color:var(--muted);font-size:10px;font-family:'DM Mono',monospace;border-top:1px solid var(--border);margin-top:1rem;padding-top:1rem;}}
.ctrl-row{{display:flex;gap:.6rem;flex-wrap:wrap;align-items:center;margin-bottom:.8rem;}}
.sel{{background:var(--surface2);color:var(--text);border:1px solid var(--border);border-radius:6px;padding:6px 10px;font-family:'DM Mono',monospace;font-size:11px;}}
.btn-tiny{{background:var(--surface2);border:1px solid var(--border);color:var(--muted);padding:6px 12px;border-radius:6px;font-size:10px;font-family:'DM Mono',monospace;cursor:pointer;text-transform:uppercase;letter-spacing:.06em;}}
.btn-tiny:hover{{border-color:var(--a2);color:var(--a2);}}
.btn-month{{background:var(--surface2);border:1px solid var(--border);color:var(--muted);padding:5px 11px;border-radius:5px;font-size:10px;font-family:'DM Mono',monospace;cursor:pointer;letter-spacing:.05em;}}
.btn-month:hover{{border-color:var(--a2);color:var(--a2);}}
.btn-month.active{{background:rgba(56,189,248,0.15);border-color:var(--a2);color:var(--a2);}}
.cleaning-legend{{display:flex;align-items:center;gap:6px;font-size:10px;font-family:'DM Mono',monospace;color:var(--muted);}}
.cleaning-line-icon{{display:inline-block;width:18px;height:2px;background:repeating-linear-gradient(90deg,#fbbf24 0px,#fbbf24 4px,transparent 4px,transparent 8px);vertical-align:middle;}}
.table-wrap{{max-height:420px;overflow:auto;border:1px solid var(--border);border-radius:8px;}}
.pt{{width:100%;border-collapse:collapse;font-size:12px;}}
.pt th{{position:sticky;top:0;background:var(--surface2);text-align:left;padding:7px 9px;font-size:10px;text-transform:uppercase;letter-spacing:.08em;color:var(--muted);font-family:'DM Mono',monospace;border-bottom:1px solid var(--border);}}
.pt td{{padding:7px 9px;border-bottom:1px solid rgba(255,255,255,.05);}}
.hm-wrap{{overflow:auto;max-width:100%;border:1px solid var(--border);border-radius:8px;}}
.hm{{border-collapse:collapse;font-size:10px;font-family:'DM Mono',monospace;min-width:max-content;}}
.hm-corner,.hm-row{{position:sticky;left:0;z-index:2;background:var(--surface2)!important;color:var(--muted)!important;text-align:left;padding:6px 8px;border:1px solid var(--border);max-width:140px;white-space:nowrap;}}
.hm-corner{{z-index:3;left:0;top:0;}}
.hm-col{{position:sticky;top:0;z-index:1;background:var(--surface2);color:var(--muted);padding:6px 4px;border:1px solid var(--border);text-align:center;min-width:36px;}}
.hm-cell{{text-align:center;padding:5px 4px;border:1px solid rgba(255,255,255,.06);font-weight:600;}}
.hm-legend{{display:flex;align-items:center;gap:10px;margin-top:.6rem;font-size:11px;color:var(--muted);font-family:'DM Mono',monospace;flex-wrap:wrap;}}
.hm-scale{{height:8px;width:180px;border-radius:4px;background:linear-gradient(90deg,rgb(30,80,140),rgb(50,180,200),rgb(34,197,94));}}
@media(max-width:900px){{.split{{grid-template-columns:1fr;}}}}
</style>
</head>
<body>
<div class="stripe">
  <span class="sid">{title_site} (site_id {args.site_id}) · String PR Audit · {len(labels_daily)} hari · {n_strings_with_pr}/{n_strings_total} string ada PR</span>
  <span class="bdg">PR POA (fallback GHI) · Heatmap rata-rata periode</span>
</div>
<div class="hero">
  <div class="hi">
    <div class="hl">Solar PV · String Performance Audit</div>
    <h1>Audit <em>PR String</em><br>
      <span style="font-size:.62em;color:var(--muted)">{title_site}</span></h1>
    <div class="hsub">{period_txt}</div>
    <div class="pills">
      <span class="pill"><span class="d" style="background:#38bdf8"></span>{n_strings_total} Strings</span>
      <span class="pill"><span class="d" style="background:#22c55e"></span>Best: {best:.2f}%</span>
      <span class="pill"><span class="d" style="background:#ef4444"></span>Worst: {worst:.2f}%</span>
      <span class="pill"><span class="d" style="background:#f59e0b"></span>Avg: {avg_all:.2f}%</span>
    </div>
  </div>
</div>
<div class="main">
  <nav class="dnav">
    <a href="#kpi">01 KPI</a>
    <a href="#daily-section">02 Daily Trend</a>
    <a href="#heatmap">03 Heatmap</a>
    <a href="#top">04 Top String</a>
    <a href="#bottom">05 Bottom String</a>
    <a href="#trend-all">06 Trend String (filter)</a>
    <a href="#detail-table">07 Detail Table</a>
    <a href="waterfall.html" style="border-color:rgba(245,158,11,.5);color:var(--accent)" title="PR Waterfall Loss Analysis">08+ Waterfall ↗</a>
  </nav>

  <div id="kpi">
    <h2 class="st"><span class="num">01</span> Key Metrics</h2>
  </div>
  <div class="grid">
    <div class="card"><div class="k">Jumlah String (site)</div><div class="v">{n_strings_total}</div></div>
    <div class="card"><div class="k">Rata-rata PR String</div><div class="v">{avg_all:.2f}%</div></div>
    <div class="card"><div class="k">String Terbaik</div><div class="v">{best:.2f}%</div></div>
    <div class="card"><div class="k">String Terburuk</div><div class="v">{worst:.2f}%</div></div>
  </div>

  <div class="note">Label inverter memakai <strong>nama aset</strong> dari mart. Heatmap: rata-rata PR per sel di periode. Bagian 06: pilih inverter dan/atau string — <strong>Semua</strong> menampilkan banyak garis sekaligus; kombinasi spesifik untuk fokus satu atau beberapa seri.</div>

  <div id="daily-section" class="section">
    <h2 class="st"><span class="num">02</span> Trend PR Harian (Rata-rata Site)</h2>
    <div class="ctrl-row" id="monthFilterBar">
      <button type="button" class="btn-month active" data-ym="__all__">Semua</button>
      {month_buttons}
    </div>
    <div class="ctrl-row" style="margin-top:-.2rem;margin-bottom:.6rem">
      <span class="cleaning-legend"><span class="cleaning-line-icon"></span> Tanggal cleaning</span>
    </div>
    <div class="chart"><canvas id="cDaily"></canvas></div>
  </div>

  <div id="heatmap" class="section">
    <h2 class="st"><span class="num">03</span> Heatmap PR (rata-rata sebulan / periode) — Inverter ↓ · String →</h2>
    <div class="hm-wrap">
      <table class="hm">{heatmap_table}</table>
    </div>
    <div class="hm-legend">
      <span>Skala PR %: {hm_lo:.1f}</span>
      <span class="hm-scale"></span>
      <span>{hm_hi:.1f}</span>
      <span>· Sel kosong (—) = tidak ada PR pada periode</span>
    </div>
  </div>

  <div class="split">
    <div id="top" class="section">
      <h2 class="st"><span class="num">04</span> Top 10 String (PR Tertinggi)</h2>
      <div class="chart"><canvas id="top10"></canvas></div>
    </div>
    <div id="bottom" class="section">
      <h2 class="st"><span class="num">05</span> Bottom 10 String (PR Terendah)</h2>
      <div class="chart"><canvas id="bottom10"></canvas></div>
    </div>
  </div>

  <div id="trend-all" class="section">
    <h2 class="st"><span class="num">06</span> Trend Harian PR per String</h2>
    <div class="ctrl-row">
      <label for="invTrendSelect" style="font-family:'DM Mono',monospace;color:var(--muted);font-size:11px">Inverter</label>
      <select id="invTrendSelect" class="sel"></select>
      <label for="strTrendSelect" style="font-family:'DM Mono',monospace;color:var(--muted);font-size:11px">String</label>
      <select id="strTrendSelect" class="sel"></select>
      <button type="button" class="btn-tiny" id="trendResetAll" title="Kembalikan ke Semua inverter + Semua string">Semua + Semua</button>
      <span id="trendSeriesCount" style="font-family:'DM Mono',monospace;font-size:11px;color:var(--muted)"></span>
    </div>
    <div class="chart chart-tall" id="cAllStringsWrap"><canvas id="cAllStrings"></canvas></div>
  </div>

  <div id="detail-table" class="section">
    <h2 class="st"><span class="num">07</span> Table Detail PR String (Lengkap per Tanggal)</h2>
    <div class="ctrl-row">
      <label for="invTableSelect" style="font-family:'DM Mono',monospace;color:var(--muted);font-size:11px">Filter Inverter</label>
      <select id="invTableSelect" class="sel"></select>
    </div>
    <div class="table-wrap">
      <table class="pt">
        <thead>
          <tr><th>Tanggal</th><th>Inverter</th><th>String</th><th>PR (%)</th></tr>
        </thead>
        <tbody id="detailTableBody"></tbody>
      </table>
    </div>
  </div>

  <div class="foot">Generated {gen_ts} · scripts/generate_pr_string_audit_html.py · site_id={args.site_id}</div>
</div>

<script>
Chart.defaults.color = '#93a1b5';
Chart.defaults.borderColor = 'rgba(148,163,184,.25)';

const labelsDaily = {json.dumps(labels_daily)};
const valuesDaily = {json.dumps([round(v, 3) if v is not None else None for v in values_daily])};
const valuesRoll7 = {json.dumps([round(v, 3) if v is not None else None for v in values_roll7])};
const topLabels = {json.dumps(top_labels)};
const topVals = {json.dumps(top_vals)};
const bottomLabels = {json.dumps(bottom_labels)};
const bottomVals = {json.dumps(bottom_vals)};
const invSeries = {json.dumps(inv_series_out)};
const idToDisplay = {json.dumps(id_to_display_json)};
const detailRows = {json.dumps(detail_rows)};
const allLineSeries = {json.dumps(all_line_series)};
const invIdsOrdered = {json.dumps(inv_ids_sorted)};
const stringsAllSorted = {json.dumps(str_col_labels)};
const cleaningDates = {json.dumps(cleaning_dates)};

// ── Month filter state ──────────────────────────────────────────────
let activeYm = '__all__';
function idxRange(ym) {{
  if (ym === '__all__') return [0, labelsDaily.length - 1];
  let s = -1, e = -1;
  labelsDaily.forEach((d, i) => {{
    if (d.startsWith(ym)) {{ if (s < 0) s = i; e = i; }}
  }});
  return s < 0 ? [0, labelsDaily.length - 1] : [s, e];
}}
function sliceRange(arr, s, e) {{ return arr.slice(s, e + 1); }}

// ── Cleaning-date vertical lines plugin ────────────────────────────
const cleaningPlugin = {{
  id: 'cleaningLines',
  afterDraw(chart) {{
    if (!cleaningDates.length) return;
    const {{ ctx, chartArea, scales }} = chart;
    const lbls = chart.data.labels;
    ctx.save();
    ctx.strokeStyle = 'rgba(251,191,36,0.82)';
    ctx.lineWidth = 1.8;
    ctx.setLineDash([5, 4]);
    cleaningDates.forEach(item => {{
      const idx = lbls.indexOf(item.date);
      if (idx < 0) return;
      const x = scales.x.getPixelForValue(idx);
      ctx.beginPath();
      ctx.moveTo(x, chartArea.top);
      ctx.lineTo(x, chartArea.bottom);
      ctx.stroke();
    }});
    ctx.restore();
  }}
}};
Chart.register(cleaningPlugin);

// ── Site-level daily trend chart ───────────────────────────────────
let chartDaily = null;
function renderDailyChart() {{
  const [s, e] = idxRange(activeYm);
  const lbls = sliceRange(labelsDaily, s, e);
  const vDay = sliceRange(valuesDaily, s, e);
  const vR7  = sliceRange(valuesRoll7, s, e);
  if (chartDaily) chartDaily.destroy();
  chartDaily = new Chart(document.getElementById('cDaily'), {{
    type: 'line',
    data: {{
      labels: lbls,
      datasets: [
        {{ label: 'Rata-rata harian', data: vDay, borderColor: '#38bdf8', borderWidth: 1.5, pointRadius: 0, tension: 0.2, spanGaps: true }},
        {{ label: 'Rolling 7 hari',   data: vR7,  borderColor: '#f59e0b', borderWidth: 2,   pointRadius: 0, tension: 0.2, spanGaps: true }}
      ]
    }},
    options: {{
      responsive: true,
      maintainAspectRatio: false,
      plugins: {{
        legend: {{ position: 'bottom', labels: {{ boxWidth: 10, font: {{ size: 10 }} }} }},
        tooltip: {{ mode: 'nearest', intersect: false }}
      }},
      interaction: {{ mode: 'nearest', axis: 'x', intersect: false }},
      scales: {{ y: {{ title: {{ display: true, text: 'PR %' }} }} }}
    }}
  }});
}}
renderDailyChart();

// ── Month filter buttons ───────────────────────────────────────────
document.getElementById('monthFilterBar').addEventListener('click', (e) => {{
  const btn = e.target.closest('.btn-month');
  if (!btn) return;
  activeYm = btn.dataset.ym;
  document.querySelectorAll('#monthFilterBar .btn-month').forEach(b => b.classList.toggle('active', b === btn));
  renderDailyChart();
  renderTrendStringChart();  // section 06 follows same month filter
}});

new Chart(document.getElementById('top10'), {{
  type: 'bar',
  data: {{ labels: topLabels, datasets: [{{ data: topVals, backgroundColor: 'rgba(34,197,94,0.5)', borderColor: '#22c55e' }}] }},
  options: {{ indexAxis: 'y', responsive: true, maintainAspectRatio: false,
    plugins: {{ legend: {{ display: false }} }},
    scales: {{ x: {{ title: {{ display: true, text: 'PR % (rata per string)' }} }} }}
  }}
}});

new Chart(document.getElementById('bottom10'), {{
  type: 'bar',
  data: {{ labels: bottomLabels, datasets: [{{ data: bottomVals, backgroundColor: 'rgba(239,68,68,0.45)', borderColor: '#ef4444' }}] }},
  options: {{ indexAxis: 'y', responsive: true, maintainAspectRatio: false,
    plugins: {{ legend: {{ display: false }} }},
    scales: {{ x: {{ title: {{ display: true, text: 'PR % (rata per string)' }} }} }}
  }}
}});

const ALL = '__ALL__';

function palette(i, n) {{
  return 'hsla(' + Math.round((i * 360) / Math.max(n, 1)) + ',55%,58%,0.22)';
}}

function stringsForInv(iid) {{
  if (iid === ALL) return stringsAllSorted.slice();
  const keys = Object.keys(invSeries[iid] || {{}});
  return keys.sort((a, b) => (parseInt(a.replace(/^S/i, ''), 10) || 0) - (parseInt(b.replace(/^S/i, ''), 10) || 0));
}}

const invTrendSel = document.getElementById('invTrendSelect');
const strTrendSel = document.getElementById('strTrendSelect');

(function initTrendFilters() {{
  invTrendSel.innerHTML = '';
  const o0 = document.createElement('option');
  o0.value = ALL;
  o0.textContent = 'Semua inverter';
  invTrendSel.appendChild(o0);
  invIdsOrdered.forEach(iid => {{
    const o = document.createElement('option');
    o.value = iid;
    o.textContent = idToDisplay[iid] || iid;
    invTrendSel.appendChild(o);
  }});
  invTrendSel.value = ALL;
}})();

function refillStrTrend() {{
  const prev = strTrendSel.value;
  strTrendSel.innerHTML = '';
  const o0 = document.createElement('option');
  o0.value = ALL;
  o0.textContent = 'Semua string';
  strTrendSel.appendChild(o0);
  stringsForInv(invTrendSel.value).forEach(s => {{
    const o = document.createElement('option');
    o.value = s;
    o.textContent = s;
    strTrendSel.appendChild(o);
  }});
  const ok = prev && [...strTrendSel.options].some(x => x.value === prev);
  strTrendSel.value = ok ? prev : ALL;
}}

let chartAll = null;
function renderTrendStringChart() {{
  const invF = invTrendSel.value;
  const strF = strTrendSel.value;
  const [rs, re] = idxRange(activeYm);
  const lbls = sliceRange(labelsDaily, rs, re);

  const filtered = allLineSeries.filter(s =>
    (invF === ALL || s.iid === invF) && (strF === ALL || s.str === strF)
  );
  const nSer = filtered.length;
  document.getElementById('trendSeriesCount').textContent = nSer ? (nSer + ' seri') : '0 seri';

  const wrap = document.getElementById('cAllStringsWrap');
  wrap.className = 'chart' + ((nSer > 20 || nSer === 0) ? ' chart-tall' : '');

  const few = nSer <= 5 && nSer > 0;
  const ds = filtered.map((s, i) => ({{
    label: s.label,
    data: sliceRange(s.data, rs, re),
    borderColor: few
      ? 'hsla(' + ((i * 67) % 360) + ',75%,62%,0.92)'
      : palette(i, Math.max(nSer, 1)),
    borderWidth: few ? 2.2 : 1,
    pointRadius: few ? 2 : 0,
    tension: 0.15,
    spanGaps: true
  }}));

  if (chartAll) chartAll.destroy();
  chartAll = new Chart(document.getElementById('cAllStrings'), {{
    type: 'line',
    data: {{ labels: lbls, datasets: ds }},
    options: {{
      responsive: true,
      maintainAspectRatio: false,
      plugins: {{
        legend: {{ display: few && nSer > 1, position: 'bottom', labels: {{ boxWidth: 10, font: {{ size: 10 }} }} }},
        tooltip: {{
          filter: (item) => item.parsed.y != null,
          mode: 'nearest',
          intersect: false
        }}
      }},
      interaction: {{ mode: 'nearest', axis: 'x', intersect: false }},
      scales: {{ y: {{ title: {{ display: true, text: 'PR %' }} }} }}
    }}
  }});
}}

invTrendSel.addEventListener('change', () => {{
  refillStrTrend();
  renderTrendStringChart();
}});
strTrendSel.addEventListener('change', renderTrendStringChart);
document.getElementById('trendResetAll').addEventListener('click', () => {{
  invTrendSel.value = ALL;
  refillStrTrend();
  strTrendSel.value = ALL;
  renderTrendStringChart();
}});
refillStrTrend();
renderTrendStringChart();

const invTableSel = document.getElementById('invTableSelect');
const allOpt = document.createElement('option');
allOpt.value = '';
allOpt.textContent = 'Semua';
invTableSel.appendChild(allOpt);
Object.keys(idToDisplay).sort((a,b) => (idToDisplay[a]||'').localeCompare(idToDisplay[b]||'')).forEach(iid => {{
  const o = document.createElement('option');
  o.value = iid;
  o.textContent = idToDisplay[iid];
  invTableSel.appendChild(o);
}});

function refillTable() {{
  const f = invTableSel.value;
  const tb = document.getElementById('detailTableBody');
  tb.innerHTML = '';
  const rows = detailRows.filter(r => !f || r.iid === f).sort((a,b) => b.date_str.localeCompare(a.date_str) || a.inv.localeCompare(b.inv));
  rows.forEach(r => {{
    const tr = document.createElement('tr');
    tr.innerHTML = '<td>' + r.date_str + '</td><td>' + r.inv + '</td><td>' + r.str_label + '</td><td>' + (r.pr_pct != null ? r.pr_pct : '—') + '</td>';
    tb.appendChild(tr);
  }});
}}
invTableSel.addEventListener('change', refillTable);
refillTable();
</script>
</body>
</html>
"""

    with open(args.out, "w", encoding="utf-8") as f:
        f.write(html)
    print(f"Wrote {os.path.abspath(args.out)}")


if __name__ == "__main__":
    main()
