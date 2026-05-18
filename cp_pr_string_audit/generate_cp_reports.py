"""
Generate PR string audit HTML reports for all sites.

Output:
  cp_pr_string_audit/output/cp_pr_string_audit_<site>.html
"""

from __future__ import annotations

import json
import os
from datetime import date
from pathlib import Path

import pandas as pd
import psycopg2


OUT_DIR = Path(__file__).resolve().parent / "output"
OUT_DIR.mkdir(exist_ok=True)


def connect():
    return psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "10.101.4.88"),
        port=int(os.getenv("POSTGRES_PORT", "5432")),
        dbname=os.getenv("POSTGRES_DB", "MMSR"),
        user=os.getenv("POSTGRES_USER", "juice"),
        password=os.getenv("POSTGRES_PASSWORD", "Baramulti1!"),
    )


def list_sites(conn) -> list[str]:
    sql = """
    SELECT DISTINCT site_name
    FROM mart.mart_string_performance_daily
    WHERE site_name IS NOT NULL
    ORDER BY 1
    """
    with conn.cursor() as cur:
        cur.execute(sql)
        return [r[0] for r in cur.fetchall()]


def load_site_data(conn, site_name: str) -> pd.DataFrame:
    sql = """
    WITH bounds AS (
        SELECT MAX(date_key) AS max_date
        FROM mart.mart_string_performance_daily
        WHERE site_name = %(site)s
    ),
    module_spec AS (
        SELECT
            site_name,
            MAX(pv_module_p_nom_wp_master)::numeric AS module_watt_wp
        FROM dimensions.dim_inverter_string_layout
        WHERE site_name = %(site)s
        GROUP BY site_name
    ),
    stg_layout AS (
        SELECT
            site_name,
            string_no,
            MAX(module_qty) FILTER (WHERE module_qty > 0)::numeric AS module_qty
        FROM staging.seed_inverter_string_layout
        WHERE site_name = %(site)s
        GROUP BY site_name, string_no
    )
    SELECT
        m.date_key::date AS date_key,
        m.site_name,
        m.inverter_name,
        m.string_number,
        COALESCE(
            m.pr_poa_string,
            m.pr_ghi_string,
            CASE
                WHEN m.daily_energy_kwh IS NOT NULL
                 AND m.daily_ghi_kwh_m2 IS NOT NULL
                 AND m.daily_ghi_kwh_m2 > 0
                 AND sl.module_qty IS NOT NULL
                 AND ms.module_watt_wp IS NOT NULL
                 AND sl.module_qty > 0
                 AND ms.module_watt_wp > 0
                THEN m.daily_energy_kwh / (
                    m.daily_ghi_kwh_m2 * ((sl.module_qty * ms.module_watt_wp) / 1000.0)
                )
                ELSE NULL
            END
        ) AS pr_string
    FROM mart.mart_string_performance_daily m
    CROSS JOIN bounds b
    LEFT JOIN module_spec ms
      ON ms.site_name = m.site_name
    LEFT JOIN stg_layout sl
      ON sl.site_name = m.site_name
     AND sl.string_no = m.string_number
    WHERE m.site_name = %(site)s
      AND m.date_key BETWEEN (b.max_date - INTERVAL '29 day') AND b.max_date
      AND COALESCE(
          m.pr_poa_string,
          m.pr_ghi_string,
          CASE
              WHEN m.daily_energy_kwh IS NOT NULL
               AND m.daily_ghi_kwh_m2 IS NOT NULL
               AND m.daily_ghi_kwh_m2 > 0
               AND sl.module_qty IS NOT NULL
               AND ms.module_watt_wp IS NOT NULL
               AND sl.module_qty > 0
               AND ms.module_watt_wp > 0
              THEN m.daily_energy_kwh / (
                  m.daily_ghi_kwh_m2 * ((sl.module_qty * ms.module_watt_wp) / 1000.0)
              )
              ELSE NULL
          END
      ) IS NOT NULL
    ORDER BY m.date_key, m.inverter_name, m.string_number
    """
    return pd.read_sql(sql, conn, params={"site": site_name}, parse_dates=["date_key"])


def _safe(v: float | None, digits: int = 2) -> str:
    if v is None or pd.isna(v):
        return "N/A"
    return f"{v:.{digits}f}"


def build_html(site_name: str, df: pd.DataFrame) -> str:
    df = df.copy()
    df["pr_pct"] = df["pr_string"] * 100.0
    df["string_label"] = (
        df["inverter_name"].fillna("INV?")
        + "-S"
        + df["string_number"].fillna(0).astype(int).astype(str).str.zfill(2)
    )

    latest_date = df["date_key"].max().date()
    earliest_date = df["date_key"].min().date()

    summary = (
        df.groupby("string_label", as_index=False)["pr_pct"]
        .mean()
        .rename(columns={"pr_pct": "avg_pr_pct"})
    )
    summary = summary.sort_values("avg_pr_pct")
    bottom10 = summary.head(10).copy()
    top10 = summary.tail(10).sort_values("avg_pr_pct", ascending=False).copy()

    # Daily site-level trend
    daily = (
        df.groupby("date_key", as_index=False)["pr_pct"]
        .mean()
        .sort_values("date_key")
        .rename(columns={"pr_pct": "site_avg_pr"})
    )
    daily["roll7"] = daily["site_avg_pr"].rolling(7, min_periods=3).mean()

    site_avg = float(summary["avg_pr_pct"].mean())
    best = float(summary["avg_pr_pct"].max())
    worst = float(summary["avg_pr_pct"].min())

    labels_daily = json.dumps([d.strftime("%Y-%m-%d") for d in daily["date_key"]])
    vals_daily = json.dumps([round(v, 3) for v in daily["site_avg_pr"]])
    vals_roll7 = json.dumps([None if pd.isna(v) else round(v, 3) for v in daily["roll7"]])

    lbl_top = json.dumps(top10["string_label"].tolist())
    val_top = json.dumps([round(v, 3) for v in top10["avg_pr_pct"]])
    lbl_bottom = json.dumps(bottom10["string_label"].tolist())
    val_bottom = json.dumps([round(v, 3) for v in bottom10["avg_pr_pct"]])
    detail_rows = df[["date_key", "inverter_name", "string_number", "pr_pct"]].copy()
    detail_rows["date_str"] = detail_rows["date_key"].dt.strftime("%Y-%m-%d")
    detail_rows["inv"] = detail_rows["inverter_name"].fillna("UNKNOWN")
    detail_rows["str_label"] = "S" + detail_rows["string_number"].fillna(0).astype(int).astype(str).str.zfill(2)
    detail_rows = detail_rows[["date_str", "inv", "str_label", "pr_pct"]]
    detail_rows["pr_pct"] = detail_rows["pr_pct"].round(3)

    date_axis = [d.strftime("%Y-%m-%d") for d in daily["date_key"]]
    date_idx = {d: i for i, d in enumerate(date_axis)}
    inv_series: dict[str, dict[str, list[float | None]]] = {}
    for inv, inv_df in detail_rows.groupby("inv"):
        str_map: dict[str, list[float | None]] = {}
        for s_label, s_df in inv_df.groupby("str_label"):
            vals: list[float | None] = [None] * len(date_axis)
            for _, row in s_df.iterrows():
                idx = date_idx.get(row["date_str"])
                if idx is not None:
                    vals[idx] = float(row["pr_pct"])
            str_map[s_label] = vals
        inv_series[inv] = str_map

    inv_series_json = json.dumps(inv_series)
    detail_rows_json = detail_rows.to_json(orient="records")

    return f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>PR String Audit — {site_name}</title>
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
.split{{display:grid;grid-template-columns:1fr 1fr;gap:.75rem;}}
.note{{background:rgba(56,189,248,0.08);border:1px solid rgba(56,189,248,0.3);border-radius:10px;padding:.9rem 1rem;margin-bottom:1rem;color:rgba(232,237,245,0.88);font-size:12px;}}
.foot{{text-align:center;color:var(--muted);font-size:10px;font-family:'DM Mono',monospace;border-top:1px solid var(--border);margin-top:1rem;padding-top:1rem;}}
.ctrl-row{{display:flex;gap:.6rem;flex-wrap:wrap;align-items:center;margin-bottom:.8rem;}}
.sel{{background:var(--surface2);color:var(--text);border:1px solid var(--border);border-radius:6px;padding:6px 10px;font-family:'DM Mono',monospace;font-size:11px;}}
.table-wrap{{max-height:420px;overflow:auto;border:1px solid var(--border);border-radius:8px;}}
.pt{{width:100%;border-collapse:collapse;font-size:12px;}}
.pt th{{position:sticky;top:0;background:var(--surface2);text-align:left;padding:7px 9px;font-size:10px;text-transform:uppercase;letter-spacing:.08em;color:var(--muted);font-family:'DM Mono',monospace;border-bottom:1px solid var(--border);}}
.pt td{{padding:7px 9px;border-bottom:1px solid rgba(255,255,255,.05);}}
@media(max-width:900px){{.split{{grid-template-columns:1fr;}}}}
</style>
</head>
<body>
<div class="stripe">
  <span class="sid">{site_name} · String PR Audit · 30 Hari</span>
  <span class="bdg">Final — Tanpa Cleaning Section</span>
</div>
<div class="hero">
  <div class="hi">
    <div class="hl">Solar PV · String Performance Audit</div>
    <h1>Audit <em>PR String</em><br>
      <span style="font-size:.62em;color:var(--muted)">{site_name}</span></h1>
    <div class="hsub">Periode {earliest_date} s/d {latest_date} · Source: mart.mart_string_performance_daily</div>
    <div class="pills">
      <span class="pill"><span class="d" style="background:#38bdf8"></span>{len(summary)} Strings</span>
      <span class="pill"><span class="d" style="background:#22c55e"></span>Best: {_safe(best, 2)}%</span>
      <span class="pill"><span class="d" style="background:#ef4444"></span>Worst: {_safe(worst, 2)}%</span>
      <span class="pill"><span class="d" style="background:#f59e0b"></span>Avg: {_safe(site_avg, 2)}%</span>
    </div>
  </div>
</div>
<div class="main">
  <nav class="dnav">
    <a href="#kpi">01 KPI</a>
    <a href="#daily-section">02 Daily Trend</a>
    <a href="#top">03 Top String</a>
    <a href="#bottom">04 Bottom String</a>
    <a href="#trend">05 Trend Per String</a>
    <a href="#detail-table">06 Detail Table</a>
  </nav>

  <div id="kpi">
    <h2 class="st"><span class="num">01</span> Key Metrics</h2>
  </div>
  <div class="grid">
    <div class="card"><div class="k">Jumlah String</div><div class="v">{len(summary)}</div></div>
    <div class="card"><div class="k">Rata-rata PR String</div><div class="v">{_safe(site_avg, 2)}%</div></div>
    <div class="card"><div class="k">String Terbaik</div><div class="v">{_safe(best, 2)}%</div></div>
    <div class="card"><div class="k">String Terburuk</div><div class="v">{_safe(worst, 2)}%</div></div>
  </div>

  <div class="note">Layout disamakan dengan gaya report audit contoh, tetapi konten fokus ke performa PR string (tanpa analisis cleaning).</div>

  <div id="daily-section" class="section">
    <h2 class="st"><span class="num">02</span> Trend PR Harian (Rata-rata Site)</h2>
    <div class="chart"><canvas id="cDaily"></canvas></div>
  </div>

  <div class="split">
    <div id="top" class="section">
      <h2 class="st"><span class="num">03</span> Top 10 String (PR Tertinggi)</h2>
      <div class="chart"><canvas id="top10"></canvas></div>
    </div>
    <div id="bottom" class="section">
      <h2 class="st"><span class="num">04</span> Bottom 10 String (PR Terendah)</h2>
      <div class="chart"><canvas id="bottom10"></canvas></div>
    </div>
  </div>

  <div id="trend" class="section">
    <h2 class="st"><span class="num">05</span> Trend PR Per String (By Inverter)</h2>
    <div class="ctrl-row">
      <label for="invSelect" style="font-family:'DM Mono',monospace;color:var(--muted);font-size:11px">Inverter</label>
      <select id="invSelect" class="sel"></select>
      <label for="strSelect" style="font-family:'DM Mono',monospace;color:var(--muted);font-size:11px">String</label>
      <select id="strSelect" class="sel"></select>
    </div>
    <div class="chart"><canvas id="cStringTrend"></canvas></div>
  </div>

  <div id="detail-table" class="section">
    <h2 class="st"><span class="num">06</span> Table Detail PR String (Lengkap per Tanggal)</h2>
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

  <div class="foot">Generated {date.today()} by cp_pr_string_audit/generate_cp_reports.py</div>
</div>

<script>
Chart.defaults.color = '#93a1b5';
Chart.defaults.borderColor = 'rgba(148,163,184,.25)';

const labelsDaily = {labels_daily};
const valuesDaily = {vals_daily};
const valuesRoll7 = {vals_roll7};
const topLabels = {lbl_top};
const topVals = {val_top};
const bottomLabels = {lbl_bottom};
const bottomVals = {val_bottom};
const invSeries = {inv_series_json};
const detailRows = {detail_rows_json};

new Chart(document.getElementById('cDaily'), {{
  type:'line',
  data: {{
    labels: labelsDaily,
    datasets: [
      {{ label:'PR harian (%)', data:valuesDaily, borderColor:'#38bdf8', backgroundColor:'rgba(56,189,248,.2)', fill:true, tension:.3, pointRadius:2 }},
      {{ label:'Rolling 7 hari (%)', data:valuesRoll7, borderColor:'#f59e0b', fill:false, tension:.3, pointRadius:0, borderWidth:2 }}
    ]
  }},
  options: {{ responsive:true, maintainAspectRatio:false, plugins:{{legend:{{position:'bottom'}}}} }}
}});

new Chart(document.getElementById('top10'), {{
  type:'bar',
  data: {{ labels:topLabels, datasets:[{{ label:'PR (%)', data:topVals, backgroundColor:'rgba(16,185,129,.65)', borderColor:'#10b981', borderWidth:1 }}] }},
  options: {{ indexAxis:'y', responsive:true, maintainAspectRatio:false, plugins:{{legend:{{display:false}}}} }}
}});

new Chart(document.getElementById('bottom10'), {{
  type:'bar',
  data: {{ labels:bottomLabels, datasets:[{{ label:'PR (%)', data:bottomVals, backgroundColor:'rgba(239,68,68,.65)', borderColor:'#ef4444', borderWidth:1 }}] }},
  options: {{ indexAxis:'y', responsive:true, maintainAspectRatio:false, plugins:{{legend:{{display:false}}}} }}
}});

const palette = ['#38bdf8','#f59e0b','#22c55e','#ef4444','#a78bfa','#f472b6','#eab308','#14b8a6','#fb7185','#84cc16'];
const invSelect = document.getElementById('invSelect');
const strSelect = document.getElementById('strSelect');
const invTableSelect = document.getElementById('invTableSelect');
const detailTableBody = document.getElementById('detailTableBody');

function uniqueSorted(arr) {{
  return [...new Set(arr)].sort((a,b)=>String(a).localeCompare(String(b)));
}}

const invList = uniqueSorted(Object.keys(invSeries));
invList.forEach(inv => {{
  const o = document.createElement('option');
  o.value = inv; o.textContent = inv;
  invSelect.appendChild(o);

  const o2 = document.createElement('option');
  o2.value = inv; o2.textContent = inv;
  invTableSelect.appendChild(o2);
}});
const allOpt = document.createElement('option');
allOpt.value = 'ALL'; allOpt.textContent = 'ALL';
invTableSelect.insertBefore(allOpt, invTableSelect.firstChild);
invTableSelect.value = 'ALL';

let stringTrendChart = null;
function rebuildStringOptions() {{
  const inv = invSelect.value;
  const strings = uniqueSorted(Object.keys(invSeries[inv] || {{}}));
  strSelect.innerHTML = '';
  const all = document.createElement('option');
  all.value = 'ALL'; all.textContent = 'ALL';
  strSelect.appendChild(all);
  strings.forEach(s => {{
    const o = document.createElement('option');
    o.value = s; o.textContent = s;
    strSelect.appendChild(o);
  }});
  strSelect.value = 'ALL';
}}

function renderStringTrend() {{
  const inv = invSelect.value;
  const selectedString = strSelect.value;
  const seriesMap = invSeries[inv] || {{}};
  const keys = selectedString === 'ALL' ? uniqueSorted(Object.keys(seriesMap)) : [selectedString];
  const datasets = keys.map((k, i) => ({{
    label: `${{inv}}-${{k}}`,
    data: seriesMap[k] || [],
    borderColor: palette[i % palette.length],
    backgroundColor: 'transparent',
    borderWidth: 1.8,
    pointRadius: 1.5,
    tension: 0.25,
    spanGaps: true
  }}));

  if (stringTrendChart) stringTrendChart.destroy();
  stringTrendChart = new Chart(document.getElementById('cStringTrend'), {{
    type: 'line',
    data: {{ labels: labelsDaily, datasets }},
    options: {{
      responsive: true,
      maintainAspectRatio: false,
      interaction: {{ mode:'index', intersect:false }},
      plugins: {{ legend: {{ position:'bottom' }} }},
      scales: {{ y: {{ title: {{ display:true, text:'PR (%)' }} }} }}
    }}
  }});
}}

function renderDetailTable() {{
  const inv = invTableSelect.value;
  const rows = detailRows.filter(r => inv === 'ALL' ? true : r.inv === inv);
  detailTableBody.innerHTML = rows
    .map(r => `<tr><td>${{r.date_str}}</td><td>${{r.inv}}</td><td>${{r.str_label}}</td><td>${{Number(r.pr_pct).toFixed(3)}}%</td></tr>`)
    .join('');
}}

invSelect.addEventListener('change', () => {{
  rebuildStringOptions();
  renderStringTrend();
}});
strSelect.addEventListener('change', renderStringTrend);
invTableSelect.addEventListener('change', renderDetailTable);

if (invList.length) {{
  invSelect.value = invList[0];
  rebuildStringOptions();
  renderStringTrend();
}}
renderDetailTable();
</script>
</body>
</html>"""


def slugify_site(name: str) -> str:
    return (
        name.lower()
        .replace(".", "")
        .replace("/", " ")
        .replace("-", " ")
        .replace("  ", " ")
        .strip()
        .replace(" ", "_")
    )


def main():
    with connect() as conn:
        sites = list_sites(conn)
        if not sites:
            raise RuntimeError("No site found in mart_string_performance_daily.")

        print("Sites:", ", ".join(sites))
        for site in sites:
            df = load_site_data(conn, site)
            if df.empty:
                print(f"Skip {site}: no data")
                continue

            html = build_html(site, df)
            out_file = OUT_DIR / f"cp_pr_string_audit_{slugify_site(site)}.html"
            out_file.write_text(html, encoding="utf-8")
            print(f"Generated: {out_file}")


if __name__ == "__main__":
    main()
