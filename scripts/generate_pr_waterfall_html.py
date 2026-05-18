"""
PR Waterfall Loss Analysis — per site HTML report.

Sections:
  08  Monthly PR Waterfall (stacked: DC-losses, inverter loss, MV loss, availability)
  09  Daily PR Trend: Meter / Inverter AC / DC (3 lines)
  10  Inverter Efficiency daily (per inverter)
  11  MV/Cable Loss % daily
  12  Soiling Analysis: temperature-corrected PR + slope between cleanings

Site-id mapping:
  mart_site_performance_daily  → 'ISO_SITE_{sid}' or 'FS_SITE_{sid}'
  mart_inverter_yield_daily    → raw sid  (strip prefix)
  mart_inverter_performance_5min → matched by site_name
  mart_sensor_daily            → matched by site_name

Usage:
  python generate_pr_waterfall_html.py --site-id 1458125 --site-name "Garuda Metalindo 1"
  python generate_pr_waterfall_html.py --site-id NE=50488260
"""
from __future__ import annotations

import argparse
import json
import os
import re
from collections import defaultdict
from datetime import date, datetime
from statistics import mean, median
from typing import Any

import psycopg2
import yaml
from psycopg2.extras import RealDictCursor

# ── SQL ───────────────────────────────────────────────────────────────────────

SQL_SITE_DAILY = """
SELECT
    date_key::date                   AS d,
    pr_poa_actual::double precision  AS pr_actual,
    daily_pr_poa_target::double precision  AS pr_target,
    availability_percent::double precision AS avail_pct,
    daily_poa_weighted_kwh_m2::double precision AS poa_kwh_m2,
    daily_energy_mwh::double precision     AS meter_mwh,
    actual_capacity_kw::double precision   AS cap_kw
FROM mart.mart_site_performance_daily
WHERE site_name = %s
  AND date_key >= %s::date
  AND date_key <= %s::date
ORDER BY date_key;
"""

SQL_INV_YIELD = """
SELECT
    date_key::date            AS d,
    asset_id,
    inverter_name,
    daily_yield_kwh::double precision AS ac_kwh
FROM mart.mart_inverter_yield_daily
WHERE site_id = %s
  AND date_key >= %s::date
  AND date_key <= %s::date
ORDER BY date_key, asset_id;
"""

SQL_INV_DC = """
SELECT
    date_key::date  AS d,
    asset_id,
    SUM(metric_value) * (5.0/60.0) / 1000.0 AS dc_kwh
FROM mart.mart_inverter_performance_5min
WHERE site_name = %s
  AND date_key >= %s::date
  AND date_key <= %s::date
  AND metric_name = 'inv_dc_power'
  AND metric_unit = 'W'
GROUP BY date_key::date, asset_id
ORDER BY date_key::date, asset_id;
"""

SQL_METER_DELTA = """
SELECT
    date_key::date AS d,
    (MAX(metric_value) - MIN(metric_value)) / 1000.0 AS meter_kwh
FROM mart.mart_meter_performance_5min
WHERE site_name = %s
  AND date_key >= %s::date
  AND date_key <= %s::date
  AND metric_name = 'positive_active_energy'
  AND metric_unit = 'Wh'
  AND meter_type = 'Revenue'
GROUP BY date_key::date
HAVING (MAX(metric_value) - MIN(metric_value)) > 0
ORDER BY date_key::date;
"""

SQL_CLEANING = """
SELECT cleaning_date::date AS d, COALESCE(notes,'') AS notes
FROM staging.cleaning_log
WHERE is_active = 1
  AND (site_id = %s OR site_id = 'ISO_SITE_' || %s)
  AND cleaning_date >= %s::date
  AND cleaning_date <= %s::date
ORDER BY cleaning_date;
"""

# ── Helpers ───────────────────────────────────────────────────────────────────

def load_dbt_db() -> dict[str, Any]:
    path = os.path.expanduser("~/.dbt/profiles.yml")
    with open(path, encoding="utf-8") as f:
        profiles = yaml.safe_load(f)
    return profiles["mmsr_solar"]["outputs"]["dev"]


def default_start(months_back: int = 6) -> str:
    today = date.today()
    y, m = today.year, today.month - months_back
    while m <= 0:
        m += 12
        y -= 1
    return date(y, m, 1).isoformat()


def strip_prefix(sid: str) -> str:
    """Remove ISO_SITE_ / FS_SITE_ prefix to get raw site_id."""
    for p in ("ISO_SITE_", "FS_SITE_"):
        if sid.startswith(p):
            return sid[len(p):]
    return sid


def linreg_slope(xs: list[float], ys: list[float]) -> float | None:
    """Least-squares slope of ys ~ xs. Returns None if < 2 points."""
    n = len(xs)
    if n < 3:
        return None
    sx = sum(xs); sy = sum(ys)
    sxx = sum(x * x for x in xs); sxy = sum(x * y for x, y in zip(xs, ys))
    denom = n * sxx - sx * sx
    if abs(denom) < 1e-12:
        return None
    return (n * sxy - sx * sy) / denom


_GAMMA = 0.0037   # %/°C temperature coefficient (crystalline silicon typical)
_T_AMB = 30.0     # assumed ambient for Indonesia (°C) — no sensor data available

def temp_correct_pr(pr: float | None, poa_kwh_m2: float | None) -> float | None:
    """Approximate PR @ 25°C using NOCT model (no sensor data)."""
    if pr is None or poa_kwh_m2 is None:
        return None
    # avg irradiance W/m² (assume 6 h daylight)
    g_avg = poa_kwh_m2 * 1000.0 / 6.0
    t_mod = _T_AMB + g_avg * (45.0 - 20.0) / 800.0   # NOCT=45°C
    correction = 1.0 - _GAMMA * (t_mod - 25.0)
    if correction <= 0:
        return pr
    return pr / correction


_MO_ID = ["","Jan","Feb","Mar","Apr","Mei","Jun","Jul","Agu","Sep","Okt","Nov","Des"]

def ym_label(yr: int, mo: int) -> str:
    return f"{_MO_ID[mo]} {str(yr)[2:]}"


def inv_display(name: str | None, asset_id: str) -> str:
    raw = (name or "").strip()
    if not raw:
        m = re.search(r"_1_(\d+)_1$", asset_id or "")
        return f"Inv #{m.group(1)}" if m else asset_id[:28]
    s = re.sub(r"^inverter\.?\s*", "Inv ", raw, flags=re.I)
    s = re.sub(r"^inverter\s+", "Inv ", s, flags=re.I)
    if not s.lower().startswith("inv"):
        s = "Inv " + s
    return re.sub(r"\s+", " ", s).strip()


# ── Main ──────────────────────────────────────────────────────────────────────

def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--site-id",   required=True)
    ap.add_argument("--site-name", default=None, help="If omitted, fetched from DB")
    ap.add_argument("--start", default=None)
    ap.add_argument("--end",   default=None)
    ap.add_argument("--months-back", type=int, default=6)
    ap.add_argument("--out", default=None)
    args = ap.parse_args()

    if not args.start:
        args.start = default_start(args.months_back)
    if not args.end:
        args.end = date.today().isoformat()

    raw_sid = strip_prefix(args.site_id)

    cfg = load_dbt_db()
    conn = psycopg2.connect(
        host=cfg["host"], port=cfg["port"],
        user=cfg["user"], password=cfg["password"], dbname=cfg["dbname"],
    )

    site_rows: list[dict] = []
    inv_yield: list[dict] = []
    inv_dc:    list[dict] = []
    meter_delta: list[dict] = []
    cleaning:  list[dict] = []

    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            # Resolve site_name if not given
            if not args.site_name:
                cur.execute(
                    "SELECT DISTINCT site_name FROM mart.mart_inverter_yield_daily "
                    "WHERE site_id=%s LIMIT 1;", (raw_sid,)
                )
                row = cur.fetchone()
                if not row:
                    cur.execute(
                        "SELECT DISTINCT site_name FROM mart.mart_string_performance_daily "
                        "WHERE site_id=%s LIMIT 1;", (args.site_id,)
                    )
                    row = cur.fetchone()
                args.site_name = str(row["site_name"]) if row else args.site_id

            cur.execute(SQL_SITE_DAILY,  (args.site_name, args.start, args.end))
            site_rows = [dict(r) for r in cur]

            cur.execute(SQL_INV_YIELD, (raw_sid, args.start, args.end))
            inv_yield = [dict(r) for r in cur]

            cur.execute(SQL_INV_DC, (args.site_name, args.start, args.end))
            inv_dc = [dict(r) for r in cur]

            cur.execute(SQL_METER_DELTA, (args.site_name, args.start, args.end))
            meter_delta = [dict(r) for r in cur]

            cur.execute(SQL_CLEANING, (args.site_id, raw_sid, args.start, args.end))
            cleaning = [dict(r) for r in cur]
    finally:
        conn.close()

    if not site_rows:
        raise SystemExit(f"No site performance data for site_name='{args.site_name}'")

    # ── Index data by date ────────────────────────────────────────────────────
    def iso(d: Any) -> str:
        return d.isoformat() if hasattr(d, "isoformat") else str(d)[:10]

    site_by_d = {iso(r["d"]): r for r in site_rows}
    labels = sorted(site_by_d)

    # Inverter AC yield: date → asset_id → kwh
    inv_ac_by_d: dict[str, dict[str, float]] = defaultdict(dict)
    inv_names: dict[str, str] = {}
    for r in inv_yield:
        d = iso(r["d"]); aid = str(r["asset_id"])
        inv_ac_by_d[d][aid] = float(r["ac_kwh"] or 0)
        if aid not in inv_names:
            inv_names[aid] = inv_display(r.get("inverter_name"), aid)

    # Inverter DC: date → asset_id → kwh
    inv_dc_by_d: dict[str, dict[str, float]] = defaultdict(dict)
    for r in inv_dc:
        d = iso(r["d"]); aid = str(r["asset_id"])
        inv_dc_by_d[d][aid] = float(r["dc_kwh"] or 0)

    # Revenue meter: date → kwh
    meter_by_d = {iso(r["d"]): float(r["meter_kwh"] or 0) for r in meter_delta}

    cleaning_dates = [{"date": iso(r["d"]), "notes": str(r["notes"])} for r in cleaning]
    cleaning_set   = {c["date"] for c in cleaning_dates}

    # ── Daily calculations ────────────────────────────────────────────────────
    daily_out: list[dict] = []
    inv_sorted = sorted(inv_names.keys())

    for d in labels:
        sr = site_by_d[d]
        poa  = float(sr["poa_kwh_m2"] or 0)
        cap  = float(sr["cap_kw"] or 0)
        pr_a = float(sr["pr_actual"] or 0)   # 0-1
        pr_t = float(sr["pr_target"] or 0)   # 0-1
        avail = float(sr["avail_pct"] or 1)
        meter_mwh = float(sr["meter_mwh"] or 0)

        theoretical_kwh = poa * cap  # kWh

        # AC inverter total
        ac_total = sum(inv_ac_by_d[d].values()) if d in inv_ac_by_d else None
        # DC total
        dc_total = sum(inv_dc_by_d[d].values()) if d in inv_dc_by_d else None
        # Revenue meter
        rev_meter_kwh = meter_by_d.get(d)
        # Fall back to site_performance energy if meter delta not available
        if not rev_meter_kwh and meter_mwh:
            rev_meter_kwh = meter_mwh * 1000.0

        # PR from each energy source
        pr_dc    = (dc_total / theoretical_kwh * 100) if (dc_total and theoretical_kwh > 0) else None
        pr_ac    = (ac_total / theoretical_kwh * 100) if (ac_total and theoretical_kwh > 0) else None
        pr_meter = pr_a * 100  # already in 0-1

        # Losses (all in PR % units)
        inv_loss = (pr_dc - pr_ac)   if (pr_dc is not None and pr_ac is not None) else None
        mv_loss  = (pr_ac - pr_meter) if (pr_ac is not None) else None
        avail_loss = ((1.0 - avail) * pr_t * 100) if pr_t else None
        gap_total  = (pr_t - pr_a) * 100
        dc_gap    = (pr_t * 100 - (pr_dc or pr_meter) - (avail_loss or 0))

        # Inverter efficiency per inverter
        inv_eff: dict[str, float | None] = {}
        for aid in inv_sorted:
            ac = inv_ac_by_d[d].get(aid)
            dc = inv_dc_by_d[d].get(aid)
            inv_eff[aid] = (ac / dc * 100) if (ac and dc and dc > 0) else None

        # MV loss %
        mv_loss_pct = None
        if ac_total and rev_meter_kwh and ac_total > 0:
            mv_loss_pct = (ac_total - rev_meter_kwh) / ac_total * 100

        # Temperature-corrected PR (approximate)
        pr_t_corr = temp_correct_pr(pr_meter, poa)

        daily_out.append({
            "d":          d,
            "pr_target":  round(pr_t * 100, 3),
            "pr_meter":   round(pr_meter, 3),
            "pr_ac":      round(pr_ac, 3) if pr_ac is not None else None,
            "pr_dc":      round(pr_dc, 3) if pr_dc is not None else None,
            "pr_t_corr":  round(pr_t_corr, 3) if pr_t_corr else None,
            "inv_loss":   round(inv_loss, 3) if inv_loss is not None else None,
            "mv_loss":    round(mv_loss, 3) if mv_loss is not None else None,
            "mv_loss_pct": round(mv_loss_pct, 3) if mv_loss_pct is not None else None,
            "avail_loss": round(avail_loss, 3) if avail_loss is not None else None,
            "gap_total":  round(gap_total, 3),
            "dc_gap":     round(dc_gap, 3) if dc_gap is not None else None,
            "inv_eff":    {aid: round(v, 2) if v is not None else None for aid, v in inv_eff.items()},
        })

    # ── Monthly waterfall aggregation ─────────────────────────────────────────
    months_out: list[dict] = []
    mo_groups: dict[str, list[dict]] = defaultdict(list)
    for r in daily_out:
        ym = r["d"][:7]
        mo_groups[ym].append(r)

    for ym in sorted(mo_groups):
        rows = mo_groups[ym]
        yr, mo = int(ym[:4]), int(ym[5:7])

        def avg(key: str) -> float | None:
            vals = [r[key] for r in rows if r[key] is not None]
            return round(mean(vals), 3) if vals else None

        pr_target_mo = avg("pr_target")
        pr_meter_mo  = avg("pr_meter")
        pr_ac_mo     = avg("pr_ac")
        pr_dc_mo     = avg("pr_dc")
        avail_loss_mo = avg("avail_loss")
        inv_loss_mo  = avg("inv_loss")
        mv_loss_mo   = avg("mv_loss")

        dc_gap_mo = None
        if pr_target_mo is not None and pr_dc_mo is not None:
            dc_gap_mo = round(pr_target_mo - pr_dc_mo - (avail_loss_mo or 0), 3)

        months_out.append({
            "ym":         ym,
            "lbl":        ym_label(yr, mo),
            "pr_target":  pr_target_mo,
            "pr_meter":   pr_meter_mo,
            "pr_ac":      pr_ac_mo,
            "pr_dc":      pr_dc_mo,
            "avail_loss": avail_loss_mo,
            "inv_loss":   inv_loss_mo,
            "mv_loss":    mv_loss_mo,
            "dc_gap":     dc_gap_mo,
        })

    # ── Soiling slope between cleanings ──────────────────────────────────────
    # Segments: [start, cleaning1), [cleaning1, cleaning2), ..., [last_cleaning, end)
    seg_boundaries = [""] + [c["date"] for c in cleaning_dates] + ["9999-99-99"]
    segments: list[dict] = []
    for i in range(len(seg_boundaries) - 1):
        lo, hi = seg_boundaries[i], seg_boundaries[i + 1]
        seg_rows = [
            r for r in daily_out
            if (not lo or r["d"] > lo) and r["d"] < hi
            and r["pr_t_corr"] is not None
        ]
        if len(seg_rows) < 3:
            continue
        day0 = seg_rows[0]["d"]
        xs = [(r["d"][:10] > day0[:10] and 1 or 0) +
              sum(1 for rr in seg_rows if rr["d"] < r["d"]) for r in seg_rows]
        xs = list(range(len(seg_rows)))
        ys = [r["pr_t_corr"] for r in seg_rows]
        slope = linreg_slope(xs, ys)  # PR %/day
        segments.append({
            "start":    seg_rows[0]["d"],
            "end":      seg_rows[-1]["d"],
            "n_days":   len(seg_rows),
            "slope":    round(slope, 4) if slope is not None else None,
            "slope_per_yr": round(slope * 365, 2) if slope is not None else None,
            "pr_start": round(ys[0], 2),
            "pr_end":   round(ys[-1], 2),
            "label":    "Setelah cleaning" if lo else "Awal periode",
        })

    # ── JS series ─────────────────────────────────────────────────────────────
    d_labels  = [r["d"] for r in daily_out]
    s_pr_meter = [r["pr_meter"] for r in daily_out]
    s_pr_ac    = [r["pr_ac"]    for r in daily_out]
    s_pr_dc    = [r["pr_dc"]    for r in daily_out]
    s_pr_target = [r["pr_target"] for r in daily_out]
    s_pr_tcorr  = [r["pr_t_corr"] for r in daily_out]
    s_mv_pct   = [r["mv_loss_pct"] for r in daily_out]

    inv_eff_series: list[dict] = []
    for aid in inv_sorted:
        inv_eff_series.append({
            "label": inv_names[aid],
            "data":  [r["inv_eff"].get(aid) for r in daily_out],
        })

    wf_labels   = [m["lbl"]        for m in months_out]
    wf_pr_meter = [m["pr_meter"]   for m in months_out]
    wf_avail    = [m["avail_loss"] for m in months_out]
    wf_inv_loss = [m["inv_loss"]   for m in months_out]
    wf_mv_loss  = [m["mv_loss"]    for m in months_out]
    wf_dc_gap   = [m["dc_gap"]     for m in months_out]
    wf_target   = [m["pr_target"]  for m in months_out]

    title    = args.site_name
    period   = f"{args.start} s/d {args.end}"
    gen_ts   = datetime.now().strftime("%Y-%m-%d %H:%M")
    has_dc   = any(r["pr_dc"] is not None for r in daily_out)
    has_mv   = any(r["mv_loss_pct"] is not None for r in daily_out)

    if not args.out:
        slug = re.sub(r"[^a-z0-9]+", "-", title.lower()).strip("-")[:48]
        out_dir = os.path.join(os.path.dirname(__file__), "..", "reports", "pr_string_audit", slug)
        args.out = os.path.join(out_dir, "waterfall.html")
    os.makedirs(os.path.dirname(os.path.abspath(args.out)), exist_ok=True)

    html = f"""<!DOCTYPE html>
<html lang="id">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>PR Waterfall — {title}</title>
<link href="https://fonts.googleapis.com/css2?family=DM+Mono:wght@400;500&family=DM+Sans:wght@300;400;500;600&family=DM+Serif+Display:ital@0;1&display=swap" rel="stylesheet">
<script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.4/dist/chart.umd.min.js"></script>
<style>
:root{{--bg:#0b0f1a;--surface:#111827;--surface2:#1a2234;--border:rgba(255,255,255,0.07);--text:#e8edf5;--muted:#6b7a99;--accent:#f59e0b;--a2:#38bdf8;}}
*{{box-sizing:border-box;margin:0;padding:0;}}
body{{background:var(--bg);color:var(--text);font-family:'DM Sans',sans-serif;font-size:14px;line-height:1.55;}}
.stripe{{background:linear-gradient(90deg,#1a1033,#0f1e38,#0b1a2e);border-bottom:1px solid var(--border);padding:0 2rem;display:flex;align-items:center;justify-content:space-between;height:52px;gap:.5rem;}}
.sid{{font-family:'DM Mono',monospace;font-size:11px;color:var(--muted);letter-spacing:.1em;text-transform:uppercase;}}
.bdg{{padding:4px 12px;border-radius:4px;font-family:'DM Mono',monospace;font-size:11px;background:rgba(56,189,248,.15);border:1px solid rgba(56,189,248,.5);color:var(--a2);}}
.hero{{padding:2rem 2rem 1.5rem;background:radial-gradient(ellipse 80% 60% at 50% -10%,rgba(56,189,248,.08),transparent);border-bottom:1px solid var(--border);}}
.hi{{max-width:1320px;margin:0 auto;}}
.hl{{font-family:'DM Mono',monospace;font-size:11px;color:var(--a2);letter-spacing:.15em;text-transform:uppercase;margin-bottom:.4rem;}}
.hero h1{{font-family:'DM Serif Display',serif;font-size:clamp(1.5rem,3vw,2.2rem);color:#fff;line-height:1.14;margin-bottom:.3rem;}}
.hero h1 em{{color:var(--accent);font-style:italic;}}
.hsub{{color:var(--muted);font-size:12px;}}
.main{{max-width:1320px;margin:0 auto;padding:1.5rem 2rem;}}
.back{{display:inline-block;margin-bottom:1rem;font-family:'DM Mono',monospace;font-size:10px;color:var(--muted);text-decoration:none;border:1px solid var(--border);padding:4px 10px;border-radius:5px;}}
.back:hover{{color:var(--a2);border-color:var(--a2);}}
.sec{{background:var(--surface);border:1px solid var(--border);border-radius:10px;padding:1rem;margin-bottom:1rem;}}
.st{{font-family:'DM Serif Display',serif;font-size:1.15rem;color:#fff;display:flex;align-items:center;gap:.6rem;margin-bottom:.9rem;}}
.num{{font-family:'DM Mono',monospace;font-size:10px;color:var(--muted);background:var(--surface2);border:1px solid var(--border);padding:2px 7px;border-radius:3px;}}
.chart{{position:relative;width:100%;height:320px;}}
.chart-tall{{height:440px;}}
.note{{background:rgba(56,189,248,.07);border:1px solid rgba(56,189,248,.3);border-radius:8px;padding:.7rem .9rem;margin-bottom:.8rem;font-size:11px;color:rgba(232,237,245,.85);}}
.slope-grid{{display:grid;grid-template-columns:repeat(auto-fill,minmax(220px,1fr));gap:.6rem;margin-top:.6rem;}}
.slope-card{{background:var(--surface2);border:1px solid var(--border);border-radius:8px;padding:.7rem .85rem;}}
.slope-lbl{{font-size:10px;color:var(--muted);font-family:'DM Mono',monospace;margin-bottom:.25rem;}}
.slope-val{{font-family:'DM Mono',monospace;font-size:1rem;font-weight:600;color:#fff;}}
.slope-sub{{font-size:10px;color:var(--muted);margin-top:.15rem;}}
.pos{{color:#ef4444;}} .neg{{color:#22c55e;}}
.foot{{text-align:center;color:var(--muted);font-size:10px;font-family:'DM Mono',monospace;border-top:1px solid var(--border);margin-top:1rem;padding-top:1rem;}}
.legend-row{{display:flex;gap:1rem;flex-wrap:wrap;margin-top:.6rem;font-size:11px;font-family:'DM Mono',monospace;color:var(--muted);}}
.leg-dot{{width:10px;height:10px;border-radius:2px;display:inline-block;margin-right:4px;vertical-align:middle;}}
</style>
</head>
<body>
<div class="stripe">
  <span class="sid">PR Waterfall · {title} · {period}</span>
  <span class="bdg">Loss Breakdown</span>
</div>
<div class="hero"><div class="hi">
  <div class="hl">Solar PV · PR Loss Analysis</div>
  <h1>PR <em>Waterfall</em><br>
    <span style="font-size:.6em;color:var(--muted)">{title}</span></h1>
  <div class="hsub">{period} · Generated {gen_ts}</div>
</div></div>

<div class="main">
<a href="index.html" class="back">← Kembali ke laporan site</a>

<div class="note">
  Waterfall menunjukkan komposisi gap PR aktual vs target per bulan.
  {"✓ Data DC inverter tersedia — inverter efficiency & soiling analisis aktif." if has_dc else "⚠ Data DC inverter tidak tersedia untuk site ini; loss breakdown terbatas."}
  {"✓ Revenue meter digunakan untuk MV/cable loss." if has_mv else ""}
  Temperature correction menggunakan estimasi NOCT (T_amb=30°C, NOCT=45°C) — tidak ada sensor suhu.
</div>

<!-- 08 Monthly Waterfall -->
<div class="sec">
  <div class="st"><span class="num">08</span> Monthly PR Waterfall (Loss per Komponen)</div>
  <div class="chart"><canvas id="cWaterfall"></canvas></div>
  <div class="legend-row">
    <span><span class="leg-dot" style="background:#22c55e"></span>PR Aktual (meter)</span>
    <span><span class="leg-dot" style="background:#f97316"></span>Inverter loss (DC→AC)</span>
    <span><span class="leg-dot" style="background:#38bdf8"></span>MV/cable loss</span>
    <span><span class="leg-dot" style="background:#fbbf24"></span>Availability loss</span>
    <span><span class="leg-dot" style="background:#ef4444"></span>DC losses (soiling+shading+temp)</span>
    <span>─ PR Target</span>
  </div>
</div>

<!-- 09 Daily PR Breakdown -->
<div class="sec">
  <div class="st"><span class="num">09</span> Daily PR — Meter / Inverter AC / DC</div>
  <div class="note">Tiga lapis PR: DC (sebelum inverter), AC inverter output, meter (setelah kabel MV). Gap antara lapisan = masing-masing losses. Garis putus-putus kuning = target. Garis vertikal = tanggal cleaning.</div>
  <div class="chart chart-tall"><canvas id="cDailyPR"></canvas></div>
</div>

<!-- 10 Inverter Efficiency -->
<div class="sec">
  <div class="st"><span class="num">10</span> Efisiensi Inverter Harian (DC→AC)</div>
  {"<div class='note'>⚠ Data DC tidak tersedia untuk site ini.</div>" if not has_dc else ""}
  <div class="chart"><canvas id="cInvEff"></canvas></div>
</div>

<!-- 11 MV Cable Loss -->
<div class="sec">
  <div class="st"><span class="num">11</span> MV / Cable Loss % Harian</div>
  {"<div class='note'>⚠ Revenue meter tidak tersedia.</div>" if not has_mv else ""}
  <div class="chart"><canvas id="cMvLoss"></canvas></div>
</div>

<!-- 12 Soiling Analysis -->
<div class="sec">
  <div class="st"><span class="num">12</span> Soiling Analysis — PR Temperature-Corrected</div>
  <div class="note">PR dikoreksi temperatur (NOCT model). Garis vertikal kuning = cleaning.
    Slope negatif antar cleaning = estimasi laju soiling. Slope positif = perbaikan / PR recovery.</div>
  <div class="chart chart-tall"><canvas id="cSoiling"></canvas></div>
  <div class="slope-grid" id="slopeGrid"></div>
</div>

<div class="foot">Generated {gen_ts} · scripts/generate_pr_waterfall_html.py · {args.site_id}</div>
</div>

<script>
Chart.defaults.color = '#93a1b5';
Chart.defaults.borderColor = 'rgba(148,163,184,.18)';

const D_LABELS  = {json.dumps(d_labels)};
const S_METER   = {json.dumps(s_pr_meter)};
const S_AC      = {json.dumps(s_pr_ac)};
const S_DC      = {json.dumps(s_pr_dc)};
const S_TARGET  = {json.dumps(s_pr_target)};
const S_TCORR   = {json.dumps(s_pr_tcorr)};
const S_MV_PCT  = {json.dumps(s_mv_pct)};
const INV_EFF   = {json.dumps(inv_eff_series)};
const WF_LABELS = {json.dumps(wf_labels)};
const WF_METER  = {json.dumps(wf_pr_meter)};
const WF_AVAIL  = {json.dumps(wf_avail)};
const WF_INV    = {json.dumps(wf_inv_loss)};
const WF_MV     = {json.dumps(wf_mv_loss)};
const WF_DC_GAP = {json.dumps(wf_dc_gap)};
const WF_TARGET = {json.dumps(wf_target)};
const CLEANING  = {json.dumps(cleaning_dates)};
const SEGMENTS  = {json.dumps(segments)};

// ── Cleaning-line plugin ──────────────────────────────────────────────────────
const cleanPlugin = {{
  id:'cLines',
  afterDraw(chart) {{
    if (!CLEANING.length) return;
    const {{ctx,chartArea,scales}} = chart;
    const lbls = chart.data.labels;
    ctx.save();
    ctx.strokeStyle='rgba(251,191,36,.85)';
    ctx.lineWidth=1.8;
    ctx.setLineDash([5,4]);
    CLEANING.forEach(c=>{{
      const idx=lbls.indexOf(c.date);
      if(idx<0) return;
      const x=scales.x.getPixelForValue(idx);
      ctx.beginPath();ctx.moveTo(x,chartArea.top);ctx.lineTo(x,chartArea.bottom);ctx.stroke();
    }});
    ctx.restore();
  }}
}};
Chart.register(cleanPlugin);

// ── 08 Waterfall ──────────────────────────────────────────────────────────────
new Chart(document.getElementById('cWaterfall'),{{
  type:'bar',
  data:{{
    labels:WF_LABELS,
    datasets:[
      {{label:'PR Aktual (meter)',data:WF_METER,backgroundColor:'rgba(34,197,94,.6)',borderColor:'#22c55e',borderWidth:1,stack:'s'}},
      {{label:'MV/Cable loss',data:WF_MV,backgroundColor:'rgba(56,189,248,.5)',borderColor:'#38bdf8',borderWidth:1,stack:'s'}},
      {{label:'Inverter loss',data:WF_INV,backgroundColor:'rgba(249,115,22,.55)',borderColor:'#f97316',borderWidth:1,stack:'s'}},
      {{label:'Availability loss',data:WF_AVAIL,backgroundColor:'rgba(251,191,36,.55)',borderColor:'#fbbf24',borderWidth:1,stack:'s'}},
      {{label:'DC losses (soiling+temp+other)',data:WF_DC_GAP,backgroundColor:'rgba(239,68,68,.5)',borderColor:'#ef4444',borderWidth:1,stack:'s'}},
      {{label:'PR Target',data:WF_TARGET,type:'line',borderColor:'rgba(248,250,252,.6)',borderDash:[6,3],borderWidth:2,pointRadius:0,tension:0,fill:false,stack:undefined}}
    ]
  }},
  options:{{
    responsive:true,maintainAspectRatio:false,
    plugins:{{legend:{{position:'bottom',labels:{{boxWidth:10,font:{{size:10}}}}}},
      tooltip:{{mode:'index',intersect:false}}}},
    scales:{{
      x:{{stacked:true}},
      y:{{stacked:true,title:{{display:true,text:'PR %'}},min:0}}
    }}
  }}
}});

// ── 09 Daily PR 3 lines ───────────────────────────────────────────────────────
new Chart(document.getElementById('cDailyPR'),{{
  type:'line',
  data:{{
    labels:D_LABELS,
    datasets:[
      {{label:'PR DC (sebelum inverter)',data:S_DC,borderColor:'rgba(56,189,248,.85)',borderWidth:1.5,pointRadius:0,tension:.15,spanGaps:true}},
      {{label:'PR AC (inverter output)',data:S_AC,borderColor:'rgba(34,197,94,.85)',borderWidth:1.5,pointRadius:0,tension:.15,spanGaps:true}},
      {{label:'PR Meter (Revenue)',data:S_METER,borderColor:'rgba(249,115,22,.85)',borderWidth:2,pointRadius:0,tension:.15,spanGaps:true}},
      {{label:'PR Target',data:S_TARGET,borderColor:'rgba(248,250,252,.45)',borderDash:[5,4],borderWidth:1.5,pointRadius:0,tension:.1,spanGaps:true}}
    ]
  }},
  options:{{
    responsive:true,maintainAspectRatio:false,
    plugins:{{legend:{{position:'bottom',labels:{{boxWidth:10,font:{{size:10}}}}}},tooltip:{{mode:'nearest',intersect:false}}}},
    interaction:{{mode:'nearest',axis:'x',intersect:false}},
    scales:{{y:{{title:{{display:true,text:'PR %'}}}}}}
  }}
}});

// ── 10 Inverter Efficiency ────────────────────────────────────────────────────
(function(){{
  const n=INV_EFF.length;
  const ds=INV_EFF.map((s,i)=>{{
    const hue=Math.round(i*360/Math.max(n,1));
    return {{label:s.label,data:s.data,borderColor:`hsla(${{hue}},70%,62%,.88)`,
      borderWidth:1.5,pointRadius:0,tension:.15,spanGaps:true}};
  }});
  new Chart(document.getElementById('cInvEff'),{{
    type:'line',data:{{labels:D_LABELS,datasets:ds}},
    options:{{responsive:true,maintainAspectRatio:false,
      plugins:{{legend:{{position:'bottom',labels:{{boxWidth:10,font:{{size:10}}}}}},tooltip:{{mode:'nearest',intersect:false}}}},
      interaction:{{mode:'nearest',axis:'x',intersect:false}},
      scales:{{y:{{title:{{display:true,text:'Efisiensi %'}},min:90,max:102}}}}
    }}
  }});
}})();

// ── 11 MV Loss % ─────────────────────────────────────────────────────────────
new Chart(document.getElementById('cMvLoss'),{{
  type:'line',
  data:{{labels:D_LABELS,datasets:[
    {{label:'MV/Cable loss %',data:S_MV_PCT,borderColor:'#38bdf8',borderWidth:1.5,pointRadius:0,tension:.15,spanGaps:true,fill:true,
      backgroundColor:'rgba(56,189,248,.08)'}}
  ]}},
  options:{{responsive:true,maintainAspectRatio:false,
    plugins:{{legend:{{display:false}},tooltip:{{mode:'nearest',intersect:false}}}},
    scales:{{y:{{title:{{display:true,text:'Loss %'}},min:0}}}}
  }}
}});

// ── 12 Soiling (temp-corrected PR) ────────────────────────────────────────────
(function(){{
  // slope regression line per segment
  const slopeLinePlugin = {{
    id:'slopeLines',
    afterDraw(chart) {{
      if (!SEGMENTS.length) return;
      const {{ctx,chartArea,scales}} = chart;
      const lbls = chart.data.labels;
      SEGMENTS.forEach(seg => {{
        const si = lbls.indexOf(seg.start), ei = lbls.indexOf(seg.end);
        if (si<0||ei<0||!seg.slope) return;
        const x0=scales.x.getPixelForValue(si), x1=scales.x.getPixelForValue(ei);
        const y0=scales.y.getPixelForValue(seg.pr_start), y1=scales.y.getPixelForValue(seg.pr_end);
        ctx.save();
        ctx.strokeStyle = seg.slope < 0 ? 'rgba(239,68,68,.7)' : 'rgba(34,197,94,.7)';
        ctx.lineWidth=2; ctx.setLineDash([8,3]);
        ctx.beginPath();ctx.moveTo(x0,y0);ctx.lineTo(x1,y1);ctx.stroke();
        ctx.restore();
      }});
    }}
  }};
  Chart.register(slopeLinePlugin);

  new Chart(document.getElementById('cSoiling'),{{
    type:'line',
    data:{{labels:D_LABELS,datasets:[
      {{label:'PR T-corrected',data:S_TCORR,borderColor:'#38bdf8',borderWidth:1.5,
        pointRadius:0,tension:.15,spanGaps:true}},
      {{label:'PR Target',data:S_TARGET,borderColor:'rgba(248,250,252,.35)',
        borderDash:[5,4],borderWidth:1.5,pointRadius:0,tension:.1,spanGaps:true}}
    ]}},
    options:{{responsive:true,maintainAspectRatio:false,
      plugins:{{legend:{{position:'bottom',labels:{{boxWidth:10,font:{{size:10}}}}}},
        tooltip:{{mode:'nearest',intersect:false}}}},
      interaction:{{mode:'nearest',axis:'x',intersect:false}},
      scales:{{y:{{title:{{display:true,text:'PR % (temp-corrected)'}}}}}}
    }}
  }});

  // Slope cards
  const grid = document.getElementById('slopeGrid');
  SEGMENTS.forEach(seg => {{
    if (!seg.slope) return;
    const cls = seg.slope < 0 ? 'pos' : 'neg';
    const sign = seg.slope < 0 ? '' : '+';
    grid.innerHTML += `<div class="slope-card">
      <div class="slope-lbl">${{seg.label}} · ${{seg.start}} → ${{seg.end}}</div>
      <div class="slope-val ${{cls}}">${{sign}}${{seg.slope_per_yr}} %/tahun</div>
      <div class="slope-sub">Slope: ${{sign}}${{seg.slope}} %/hari · ${{seg.n_days}} hari data</div>
      <div class="slope-sub">PR awal: ${{seg.pr_start}}% → PR akhir: ${{seg.pr_end}}%</div>
    </div>`;
  }});
}})();
</script>
</body>
</html>
"""

    with open(args.out, "w", encoding="utf-8") as f:
        f.write(html)
    print(f"Wrote {os.path.abspath(args.out)}")


if __name__ == "__main__":
    main()
