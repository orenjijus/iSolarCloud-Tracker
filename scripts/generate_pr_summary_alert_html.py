"""
Generate PR Summary & Alert HTML — semua site dalam satu halaman.

Isi:
  1. Kartu ringkasan per site: PR aktual vs target, status, jumlah bulan below-target
  2. Tabel below-target: site × bulan di mana PR aktual < target (dengan gap %)
  3. Tabel zero-PR alert: string yang punya kapasitas tapi PR=0 ≥ 30% hari dalam periode

Usage:
  python generate_pr_summary_alert_html.py
  python generate_pr_summary_alert_html.py --start 2025-11-01 --end 2026-04-30
  python generate_pr_summary_alert_html.py --out reports/pr_summary_alert.html
"""

from __future__ import annotations

import argparse
import json
import os
from collections import defaultdict
from datetime import date, datetime
from statistics import mean
from typing import Any

import psycopg2
import yaml
from psycopg2.extras import RealDictCursor

# ── SQL ───────────────────────────────────────────────────────────────────────

SQL_ACTUAL = """
SELECT
    site_id,
    site_name,
    EXTRACT(YEAR  FROM date_key)::int AS yr,
    EXTRACT(MONTH FROM date_key)::int AS mo,
    AVG(
        CASE
            WHEN pr_poa_string IS NOT NULL AND (pr_poa_string * 100000.0) <= 200
                THEN (pr_poa_string * 100000.0)
            WHEN pr_poa_string IS NOT NULL
                THEN (pr_poa_string * 100.0)
            WHEN pr_ghi_string IS NOT NULL AND (pr_ghi_string * 100000.0) <= 200
                THEN (pr_ghi_string * 100000.0)
            WHEN pr_ghi_string IS NOT NULL
                THEN (pr_ghi_string * 100.0)
            ELSE NULL
        END
    ) AS avg_pr_pct
FROM mart.mart_string_performance_daily
WHERE date_key >= %s::date
  AND date_key <= %s::date
  AND COALESCE(pr_poa_string, pr_ghi_string) IS NOT NULL
  AND COALESCE(pr_poa_string, pr_ghi_string) > 0
GROUP BY site_id, site_name, yr, mo
ORDER BY site_name, yr, mo;
"""

SQL_TARGET = """
SELECT
    asset_id,
    site_name,
    year::int  AS yr,
    month::int AS mo,
    (monthly_pr_poa_target * 100.0)::double precision AS pr_target_pct
FROM mart.mart_simulation_targets_monthly
WHERE year::int >= %s AND year::int <= %s
ORDER BY site_name, yr, mo;
"""

SQL_ZERO_STRINGS = """
SELECT
    site_id,
    site_name,
    inverter_id,
    inverter_name,
    string_number,
    AVG(string_dc_capacity_kw_stc)::double precision   AS cap_kw,
    COUNT(*)                                            AS total_days,
    SUM(CASE WHEN COALESCE(pr_poa_string, pr_ghi_string) = 0    THEN 1 ELSE 0 END) AS days_zero,
    SUM(CASE WHEN COALESCE(pr_poa_string, pr_ghi_string) IS NULL THEN 1 ELSE 0 END) AS days_null
FROM mart.mart_string_performance_daily
WHERE date_key >= %s::date
  AND date_key <= %s::date
  AND string_dc_capacity_kw_stc > 0
GROUP BY site_id, site_name, inverter_id, inverter_name, string_number
HAVING
    SUM(CASE WHEN COALESCE(pr_poa_string, pr_ghi_string) = 0 THEN 1 ELSE 0 END)::float
    / NULLIF(COUNT(*), 0) >= 0.30
ORDER BY site_name, days_zero DESC;
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


_MO_ID = ["", "Jan", "Feb", "Mar", "Apr", "Mei", "Jun",
          "Jul", "Agu", "Sep", "Okt", "Nov", "Des"]


def ym_label(yr: int, mo: int) -> str:
    return f"{_MO_ID[mo]} {str(yr)[2:]}"


def inv_display(name: str | None, iid: str) -> str:
    import re
    raw = (name or "").strip()
    if not raw:
        m = re.search(r"_1_(\d+)_1$", iid or "")
        return f"Inv #{m.group(1)}" if m else iid[:28]
    s = re.sub(r"^inverter\.?\s*", "Inv ", raw, flags=re.I)
    s = re.sub(r"^inverter\s+", "Inv ", s, flags=re.I)
    if not s.lower().startswith("inv"):
        s = "Inv " + s
    return re.sub(r"\s+", " ", s).strip()


# ── Main ──────────────────────────────────────────────────────────────────────

def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--start", default=None)
    ap.add_argument("--end",   default=None)
    ap.add_argument("--months-back", type=int, default=6)
    ap.add_argument(
        "--out",
        default=os.path.join(
            os.path.dirname(__file__), "..", "reports", "pr_string_summary_alert.html"
        ),
    )
    args = ap.parse_args()
    if not args.start:
        args.start = default_start(args.months_back)
    if not args.end:
        args.end = date.today().isoformat()

    start_yr = int(args.start[:4])
    end_yr   = int(args.end[:4])

    cfg = load_dbt_db()
    conn = psycopg2.connect(
        host=cfg["host"], port=cfg["port"],
        user=cfg["user"], password=cfg["password"], dbname=cfg["dbname"],
    )

    actual_rows: list[dict] = []
    target_rows: list[dict] = []
    zero_rows:   list[dict] = []
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(SQL_ACTUAL, (args.start, args.end))
            actual_rows = [dict(r) for r in cur]

            cur.execute(SQL_TARGET, (start_yr, end_yr))
            target_rows = [dict(r) for r in cur]

            cur.execute(SQL_ZERO_STRINGS, (args.start, args.end))
            zero_rows = [dict(r) for r in cur]
    finally:
        conn.close()

    # ── Build actual dict: site_id → (yr, mo) → avg_pr_pct ──────────────────
    actual: dict[str, dict[tuple, float]] = defaultdict(dict)
    site_names: dict[str, str] = {}
    for r in actual_rows:
        sid  = str(r["site_id"])
        site_names[sid] = str(r["site_name"])
        actual[sid][(int(r["yr"]), int(r["mo"]))] = float(r["avg_pr_pct"])

    # ── Build target dict: site_id → (yr, mo) → target_pct ──────────────────
    # asset_id = "ISO_SITE_1637095" → strip prefix → "1637095"
    target: dict[str, dict[tuple, float]] = defaultdict(dict)
    for r in target_rows:
        asset_id = str(r["asset_id"])
        sid = asset_id.replace("ISO_SITE_", "") if asset_id.startswith("ISO_SITE_") else asset_id
        target[sid][(int(r["yr"]), int(r["mo"]))] = float(r["pr_target_pct"])

    # ── Below-target records ─────────────────────────────────────────────────
    below_records: list[dict] = []
    for sid, ym_map in actual.items():
        t_map = target.get(sid, {})
        for (yr, mo), act_pr in sorted(ym_map.items()):
            if (yr, mo) not in t_map:
                continue
            tgt = t_map[(yr, mo)]
            gap = act_pr - tgt
            if gap < 0:
                below_records.append({
                    "sid":       sid,
                    "site_name": site_names.get(sid, sid),
                    "yr":        yr,
                    "mo":        mo,
                    "ym_label":  ym_label(yr, mo),
                    "actual":    round(act_pr, 2),
                    "target":    round(tgt, 2),
                    "gap":       round(gap, 2),
                })

    # ── Per-site summary card data ────────────────────────────────────────────
    all_sids = sorted(site_names.keys(), key=lambda s: site_names[s])
    site_summaries: list[dict] = []
    for sid in all_sids:
        t_map    = target.get(sid, {})
        ym_map   = actual.get(sid, {})
        n_below  = sum(1 for r in below_records if r["sid"] == sid)
        n_months = len(ym_map)
        n_zero   = sum(1 for r in zero_rows if str(r["site_id"]) == sid)
        has_target = bool(t_map)

        # Latest month with data
        latest_ym = max(ym_map.keys()) if ym_map else None
        latest_pr  = round(ym_map[latest_ym], 2) if latest_ym else None
        latest_tgt = round(t_map.get(latest_ym, 0), 2) if (latest_ym and t_map) else None
        latest_gap = round(latest_pr - latest_tgt, 2) if (latest_pr is not None and latest_tgt) else None
        latest_lbl = ym_label(*latest_ym) if latest_ym else "—"

        site_summaries.append({
            "sid":        sid,
            "name":       site_names[sid],
            "n_months":   n_months,
            "n_below":    n_below,
            "n_zero":     n_zero,
            "has_target": has_target,
            "latest_lbl": latest_lbl,
            "latest_pr":  latest_pr,
            "latest_tgt": latest_tgt,
            "latest_gap": latest_gap,
        })

    # Sort: sites with most alerts first
    site_summaries.sort(key=lambda x: (-(x["n_zero"] + x["n_below"] * 2), x["name"]))

    # ── Zero-string rows for JS ───────────────────────────────────────────────
    zero_js = []
    for r in zero_rows:
        sid  = str(r["site_id"])
        pct_zero = round(float(r["days_zero"]) / max(int(r["total_days"]), 1) * 100, 1)
        zero_js.append({
            "sid":       sid,
            "site":      str(r["site_name"]),
            "inv":       inv_display(r.get("inverter_name"), str(r["inverter_id"])),
            "str_num":   int(r["string_number"]),
            "cap_kw":    round(float(r["cap_kw"] or 0), 2),
            "total":     int(r["total_days"]),
            "n_zero":    int(r["days_zero"]),
            "pct_zero":  pct_zero,
            "level":     "critical" if pct_zero >= 80 else ("warning" if pct_zero >= 50 else "notice"),
        })

    below_js = [
        {"sid": r["sid"], "site": r["site_name"], "ym": r["ym_label"],
         "actual": r["actual"], "target": r["target"], "gap": r["gap"]}
        for r in below_records
    ]

    gen_ts   = datetime.now().strftime("%Y-%m-%d %H:%M")
    period   = f"{args.start} s/d {args.end}"

    # ── HTML cards ────────────────────────────────────────────────────────────
    def status_badge(s: dict) -> str:
        if s["n_zero"] > 0 and s["n_below"] > 0:
            return '<span class="badge badge-crit">⚠ PR + String 0%</span>'
        if s["n_zero"] > 0:
            return '<span class="badge badge-warn">String 0%</span>'
        if s["n_below"] > 0:
            return '<span class="badge badge-below">Below Target</span>'
        if s["latest_gap"] is not None:
            return '<span class="badge badge-ok">✓ On Target</span>'
        return '<span class="badge badge-grey">No Target Data</span>'

    def gap_color(g: float | None) -> str:
        if g is None: return "var(--muted)"
        if g < -5:    return "#ef4444"
        if g < 0:     return "#f59e0b"
        return "#22c55e"

    cards_html = ""
    for s in site_summaries:
        gap_txt = f"{s['latest_gap']:+.2f}%" if s["latest_gap"] is not None else "—"
        gap_col = gap_color(s["latest_gap"])
        pr_txt  = f"{s['latest_pr']:.2f}%" if s["latest_pr"] is not None else "—"
        tgt_txt = f"{s['latest_tgt']:.2f}%" if s["latest_tgt"] is not None else "—"
        cards_html += f"""
  <div class="card" data-sid="{s['sid']}">
    <div class="card-name">{s['name']}</div>
    {status_badge(s)}
    <div class="card-row">
      <span class="card-lbl">Bulan terakhir</span><span class="card-val">{s['latest_lbl']}</span>
    </div>
    <div class="card-row">
      <span class="card-lbl">PR aktual</span><span class="card-val">{pr_txt}</span>
    </div>
    <div class="card-row">
      <span class="card-lbl">PR target</span><span class="card-val">{tgt_txt}</span>
    </div>
    <div class="card-row">
      <span class="card-lbl">Gap</span>
      <span class="card-val" style="color:{gap_col};font-weight:600">{gap_txt}</span>
    </div>
    <div class="card-footer">
      <span title="Bulan below target">{s['n_below']} bln ↓</span>
      <span title="String PR=0%">{s['n_zero']} str 0%</span>
      <span title="Bulan ada data">{s['n_months']} bln data</span>
    </div>
  </div>"""

    os.makedirs(os.path.dirname(os.path.abspath(args.out)), exist_ok=True)

    html = f"""<!DOCTYPE html>
<html lang="id">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>PR String Summary & Alert</title>
<link href="https://fonts.googleapis.com/css2?family=DM+Mono:wght@400;500&family=DM+Sans:wght@300;400;500;600&family=DM+Serif+Display:ital@0;1&display=swap" rel="stylesheet">
<style>
:root{{
  --bg:#0b0f1a;--surface:#111827;--surface2:#1a2234;--border:rgba(255,255,255,0.07);
  --text:#e8edf5;--muted:#6b7a99;--accent:#f59e0b;--a2:#38bdf8;
}}
*{{box-sizing:border-box;margin:0;padding:0;}}
body{{background:var(--bg);color:var(--text);font-family:'DM Sans',sans-serif;font-size:14px;line-height:1.55;}}
.stripe{{background:linear-gradient(90deg,#1a1033,#0f1e38,#0b1a2e);border-bottom:1px solid var(--border);padding:0 2rem;display:flex;align-items:center;justify-content:space-between;height:52px;gap:.5rem;flex-wrap:wrap;}}
.sid{{font-family:'DM Mono',monospace;font-size:11px;color:var(--muted);letter-spacing:.1em;text-transform:uppercase;}}
.bdg-top{{padding:4px 12px;border-radius:4px;font-family:'DM Mono',monospace;font-size:11px;background:rgba(239,68,68,.15);border:1px solid rgba(239,68,68,.5);color:#fca5a5;}}
.hero{{padding:2rem 2rem 1.5rem;background:radial-gradient(ellipse 80% 60% at 50% -10%,rgba(239,68,68,.07),transparent);border-bottom:1px solid var(--border);}}
.hi{{max-width:1320px;margin:0 auto;}}
.hl{{font-family:'DM Mono',monospace;font-size:11px;color:var(--a2);letter-spacing:.15em;text-transform:uppercase;margin-bottom:.4rem;}}
.hero h1{{font-family:'DM Serif Display',serif;font-size:clamp(1.6rem,3vw,2.4rem);color:#fff;line-height:1.14;margin-bottom:.3rem;}}
.hero h1 em{{color:#f87171;font-style:italic;}}
.hsub{{color:var(--muted);font-size:12px;margin-bottom:1rem;}}
.pills{{display:flex;gap:.4rem;flex-wrap:wrap;}}
.pill{{display:inline-flex;align-items:center;gap:5px;padding:4px 11px;border-radius:16px;font-size:10px;font-family:'DM Mono',monospace;border:1px solid rgba(255,255,255,.12);}}
.main{{max-width:1320px;margin:0 auto;padding:1.5rem 2rem;}}
.sec-title{{font-family:'DM Serif Display',serif;font-size:1.2rem;color:#fff;display:flex;align-items:center;gap:.6rem;margin-bottom:.9rem;}}
.num{{font-family:'DM Mono',monospace;font-size:10px;color:var(--muted);background:var(--surface2);border:1px solid var(--border);padding:2px 7px;border-radius:3px;}}
.section{{background:var(--surface);border:1px solid var(--border);border-radius:10px;padding:1.1rem 1.1rem;margin-bottom:1rem;}}

/* Cards grid */
.cards{{display:grid;grid-template-columns:repeat(auto-fill,minmax(230px,1fr));gap:.75rem;margin-bottom:1rem;}}
.card{{background:var(--surface2);border:1px solid var(--border);border-radius:10px;padding:.9rem 1rem;position:relative;overflow:hidden;}}
.card::before{{content:'';position:absolute;top:0;left:0;right:0;height:2px;background:var(--a2);}}
.card-name{{font-size:.8rem;font-weight:600;color:#fff;margin-bottom:.45rem;line-height:1.3;}}
.card-row{{display:flex;justify-content:space-between;font-size:11px;margin-top:3px;}}
.card-lbl{{color:var(--muted);font-family:'DM Mono',monospace;font-size:10px;}}
.card-val{{font-family:'DM Mono',monospace;font-size:11px;color:var(--text);}}
.card-footer{{display:flex;gap:.5rem;margin-top:.6rem;padding-top:.5rem;border-top:1px solid var(--border);font-family:'DM Mono',monospace;font-size:10px;color:var(--muted);}}
.card-footer span{{flex:1;text-align:center;}}
.badge{{display:inline-block;padding:2px 8px;border-radius:4px;font-size:9px;font-family:'DM Mono',monospace;font-weight:500;letter-spacing:.05em;margin-bottom:.4rem;}}
.badge-crit{{background:rgba(239,68,68,.2);border:1px solid rgba(239,68,68,.6);color:#fca5a5;}}
.badge-warn{{background:rgba(245,158,11,.18);border:1px solid rgba(245,158,11,.5);color:#fcd34d;}}
.badge-below{{background:rgba(249,115,22,.18);border:1px solid rgba(249,115,22,.5);color:#fdba74;}}
.badge-ok{{background:rgba(34,197,94,.15);border:1px solid rgba(34,197,94,.4);color:#86efac;}}
.badge-grey{{background:rgba(107,122,153,.15);border:1px solid var(--border);color:var(--muted);}}

/* Tables */
.ctrl-row{{display:flex;gap:.6rem;flex-wrap:wrap;align-items:center;margin-bottom:.8rem;}}
.sel{{background:var(--surface2);color:var(--text);border:1px solid var(--border);border-radius:6px;padding:6px 10px;font-family:'DM Mono',monospace;font-size:11px;}}
.tw{{max-height:480px;overflow:auto;border:1px solid var(--border);border-radius:8px;}}
.pt{{width:100%;border-collapse:collapse;font-size:12px;}}
.pt th{{position:sticky;top:0;background:var(--surface2);text-align:left;padding:7px 10px;font-size:10px;text-transform:uppercase;letter-spacing:.07em;color:var(--muted);font-family:'DM Mono',monospace;border-bottom:1px solid var(--border);}}
.pt td{{padding:7px 10px;border-bottom:1px solid rgba(255,255,255,.04);font-size:12px;}}
.pt tr:hover td{{background:rgba(255,255,255,.025);}}
.lv-critical{{color:#fca5a5;font-weight:600;}}
.lv-warning{{color:#fcd34d;}}
.lv-notice{{color:#fdba74;}}
.gap-neg-big{{color:#ef4444;font-weight:600;}}
.gap-neg{{color:#f59e0b;}}
.gap-ok{{color:#22c55e;}}
.foot{{text-align:center;color:var(--muted);font-size:10px;font-family:'DM Mono',monospace;border-top:1px solid var(--border);margin-top:1rem;padding-top:1rem;}}
.bar{{height:8px;border-radius:3px;background:rgba(239,68,68,.7);display:inline-block;max-width:80px;}}
@media(max-width:700px){{.main{{padding:1rem;}}}}
</style>
</head>
<body>
<div class="stripe">
  <span class="sid">PR String Summary · Alert · {period}</span>
  <span class="bdg-top">{len(below_records)} bulan below target · {len(zero_rows)} string PR=0%</span>
</div>
<div class="hero">
  <div class="hi">
    <div class="hl">Solar PV · String Performance Alert</div>
    <h1>Summary &amp; <em>Alert</em><br>
      <span style="font-size:.58em;color:var(--muted)">Semua Site — {period}</span></h1>
    <div class="hsub">Periode {period} · {len(site_summaries)} site · Generated {gen_ts}</div>
    <div class="pills">
      <span class="pill" style="border-color:rgba(239,68,68,.4)">⬇ {len(below_records)} bulan below-target</span>
      <span class="pill" style="border-color:rgba(245,158,11,.4)">⚠ {len(zero_rows)} string PR=0%</span>
      <span class="pill">{len([s for s in site_summaries if s['has_target']])} site ada target</span>
    </div>
  </div>
</div>
<div class="main">

  <div class="section">
    <div class="sec-title"><span class="num">01</span> Ringkasan Per Site</div>
    <div class="cards">{cards_html}</div>
  </div>

  <div class="section">
    <div class="sec-title"><span class="num">02</span> Bulan Below PR Target</div>
    <div class="ctrl-row">
      <select id="belowSiteSel" class="sel"><option value="">Semua site</option></select>
      <span id="belowCount" style="font-family:'DM Mono',monospace;font-size:11px;color:var(--muted)"></span>
    </div>
    <div class="tw">
      <table class="pt">
        <thead><tr>
          <th>Site</th><th>Bulan</th><th>PR Aktual</th><th>PR Target</th><th>Gap</th>
        </tr></thead>
        <tbody id="belowBody"></tbody>
      </table>
    </div>
  </div>

  <div class="section">
    <div class="sec-title"><span class="num">03</span> String PR = 0% (Ada Kapasitas)</div>
    <div class="ctrl-row">
      <select id="zeroSiteSel" class="sel"><option value="">Semua site</option></select>
      <select id="zeroLvSel" class="sel">
        <option value="">Semua level</option>
        <option value="critical">Critical ≥80%</option>
        <option value="warning">Warning ≥50%</option>
        <option value="notice">Notice ≥30%</option>
      </select>
      <span id="zeroCount" style="font-family:'DM Mono',monospace;font-size:11px;color:var(--muted)"></span>
    </div>
    <div class="tw">
      <table class="pt">
        <thead><tr>
          <th>Site</th><th>Inverter</th><th>String</th><th>Kapasitas</th>
          <th>Hari 0%</th><th>Total Hari</th><th>% Hari 0%</th><th>Level</th>
        </tr></thead>
        <tbody id="zeroBody"></tbody>
      </table>
    </div>
  </div>

  <div class="foot">Generated {gen_ts} · scripts/generate_pr_summary_alert_html.py · Periode {period}</div>
</div>

<script>
const belowData = {json.dumps(below_js)};
const zeroData  = {json.dumps(zero_js)};

// ── populate site selects ─────────────────────────────────────────────────
function uniqSites(arr) {{
  const seen = new Set();
  return arr.map(r => r.site).filter(s => {{ if (seen.has(s)) return false; seen.add(s); return true; }}).sort();
}}
function fillSel(sel, arr) {{
  uniqSites(arr).forEach(s => {{
    const o = document.createElement('option'); o.value = s; o.textContent = s; sel.appendChild(o);
  }});
}}
fillSel(document.getElementById('belowSiteSel'), belowData);
fillSel(document.getElementById('zeroSiteSel'),  zeroData);

// ── Below table ───────────────────────────────────────────────────────────
function gapClass(g) {{
  if (g < -5) return 'gap-neg-big';
  if (g < 0)  return 'gap-neg';
  return 'gap-ok';
}}
function renderBelow() {{
  const f    = document.getElementById('belowSiteSel').value;
  const rows = f ? belowData.filter(r => r.site === f) : belowData;
  document.getElementById('belowCount').textContent = rows.length + ' baris';
  const tb = document.getElementById('belowBody');
  tb.innerHTML = '';
  rows.forEach(r => {{
    const tr = document.createElement('tr');
    tr.innerHTML =
      '<td>' + r.site + '</td>' +
      '<td style="font-family:\'DM Mono\',monospace">' + r.ym + '</td>' +
      '<td style="font-family:\'DM Mono\',monospace">' + r.actual.toFixed(2) + '%</td>' +
      '<td style="font-family:\'DM Mono\',monospace">' + r.target.toFixed(2) + '%</td>' +
      '<td class="' + gapClass(r.gap) + '" style="font-family:\'DM Mono\',monospace">' + (r.gap > 0 ? '+' : '') + r.gap.toFixed(2) + '%</td>';
    tb.appendChild(tr);
  }});
}}
document.getElementById('belowSiteSel').addEventListener('change', renderBelow);
renderBelow();

// ── Zero-PR table ─────────────────────────────────────────────────────────
function lvLabel(lv) {{
  if (lv === 'critical') return '<span class="lv-critical">Critical</span>';
  if (lv === 'warning')  return '<span class="lv-warning">Warning</span>';
  return '<span class="lv-notice">Notice</span>';
}}
function renderZero() {{
  const fs   = document.getElementById('zeroSiteSel').value;
  const flv  = document.getElementById('zeroLvSel').value;
  const rows = zeroData.filter(r =>
    (!fs  || r.site === fs) && (!flv || r.level === flv)
  );
  document.getElementById('zeroCount').textContent = rows.length + ' string';
  const tb = document.getElementById('zeroBody');
  tb.innerHTML = '';
  rows.forEach(r => {{
    const barW = Math.round(r.pct_zero * 0.8);
    const tr = document.createElement('tr');
    tr.innerHTML =
      '<td>' + r.site + '</td>' +
      '<td>' + r.inv + '</td>' +
      '<td style="font-family:\'DM Mono\',monospace">S' + String(r.str_num).padStart(2,'0') + '</td>' +
      '<td style="font-family:\'DM Mono\',monospace">' + r.cap_kw.toFixed(2) + ' kW</td>' +
      '<td style="font-family:\'DM Mono\',monospace;color:#ef4444">' + r.n_zero + '</td>' +
      '<td style="font-family:\'DM Mono\',monospace">' + r.total + '</td>' +
      '<td><span class="bar" style="width:' + barW + 'px"></span> ' +
        '<span style="font-family:\'DM Mono\',monospace;font-size:11px">' + r.pct_zero + '%</span></td>' +
      '<td>' + lvLabel(r.level) + '</td>';
    tb.appendChild(tr);
  }});
}}
document.getElementById('zeroSiteSel').addEventListener('change', renderZero);
document.getElementById('zeroLvSel').addEventListener('change', renderZero);
renderZero();
</script>
</body>
</html>
"""

    with open(args.out, "w", encoding="utf-8") as f:
        f.write(html)
    print(f"Wrote {os.path.abspath(args.out)}")
    print(f"  Below-target records : {len(below_records)}")
    print(f"  Zero-PR string alerts: {len(zero_rows)}")


if __name__ == "__main__":
    main()
