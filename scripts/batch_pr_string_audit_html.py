"""
Batch-generate PR String Audit HTML: one file per site, 6 months of data.

Output layout:
  reports/pr_string_audit/
    index.html                 ← hub page with links to all sites
    <site_slug>/index.html     ← per-site report

Usage examples:
  # All sites from DB (last 6 months):
  python batch_pr_string_audit_html.py --all-sites

  # Specific site IDs:
  python batch_pr_string_audit_html.py --sites 1458125,1453245

  # From a file (one site_id per line, # comments OK):
  python batch_pr_string_audit_html.py --sites-file config/pr_audit_sites.txt

  # Custom date window:
  python batch_pr_string_audit_html.py --all-sites --start 2025-11-01 --end 2026-04-30
"""

from __future__ import annotations

import argparse
import os
import re
import subprocess
import sys
from dataclasses import dataclass, field
from pathlib import Path

import psycopg2
import yaml
from psycopg2.extras import RealDictCursor

SQL_SITES = """
SELECT DISTINCT site_id, site_name
FROM mart.mart_string_performance_daily
WHERE date_key >= %s::date
ORDER BY site_name;
"""


def load_dbt_db() -> dict:
    path = os.path.expanduser("~/.dbt/profiles.yml")
    with open(path, encoding="utf-8") as f:
        profiles = yaml.safe_load(f)
    return profiles["mmsr_solar"]["outputs"]["dev"]


def slugify(text: str) -> str:
    """Simple slug: lowercase, strip non-alphanum, collapse dashes."""
    s = text.lower()
    s = re.sub(r"[^a-z0-9]+", "-", s)
    return s.strip("-")[:48]


@dataclass
class SiteEntry:
    site_id: str
    site_name: str
    slug: str = field(init=False)

    def __post_init__(self) -> None:
        self.slug = slugify(self.site_name) or slugify(self.site_id)


def fetch_all_sites(start: str) -> list[SiteEntry]:
    cfg = load_dbt_db()
    conn = psycopg2.connect(
        host=cfg["host"], port=cfg["port"],
        user=cfg["user"], password=cfg["password"], dbname=cfg["dbname"],
    )
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(SQL_SITES, (start,))
            return [SiteEntry(str(r["site_id"]), str(r["site_name"])) for r in cur]
    finally:
        conn.close()


def parse_sites_arg(arg: str | None, file_path: str | None) -> list[str]:
    if file_path:
        lines = Path(file_path).read_text(encoding="utf-8").splitlines()
        return [ln.strip() for ln in lines if ln.strip() and not ln.strip().startswith("#")]
    if arg:
        return [s.strip() for s in arg.split(",") if s.strip()]
    raise SystemExit("Provide --all-sites, --sites, or --sites-file.")


def write_index(root: Path, entries: list[tuple[SiteEntry, Path, bool]]) -> None:
    rows = []
    for site, html_path, ok in entries:
        rel = f"{site.slug}/index.html"
        status_cls = "" if ok else ' style="color:#ef4444"'
        if ok:
            cell = f'<a href="{rel}">{site.site_name}</a>'
        else:
            cell = f'<span style="color:#ef4444">{site.site_name} (gagal)</span>'
        rows.append(
            f"<tr><td>{cell}</td>"
            f'<td style="color:#64748b;font-size:0.85em">{site.site_id}</td></tr>'
        )

    html = f"""<!DOCTYPE html>
<html lang="id">
<head>
  <meta charset="utf-8"/>
  <meta name="viewport" content="width=device-width,initial-scale=1"/>
  <title>PR String Audit — Semua Site</title>
  <style>
    *{{box-sizing:border-box;margin:0;padding:0;}}
    body{{background:#0b0f1a;color:#e2e8f0;font-family:system-ui,sans-serif;padding:2rem;}}
    h1{{font-size:1.25rem;font-weight:600;margin-bottom:.35rem;color:#fff;}}
    p{{color:#64748b;font-size:.85rem;margin-bottom:1.5rem;}}
    table{{border-collapse:collapse;width:100%;max-width:640px;}}
    th,td{{border:1px solid #1e293b;padding:8px 12px;text-align:left;}}
    th{{background:#1e293b;color:#64748b;font-size:.75rem;text-transform:uppercase;letter-spacing:.06em;}}
    tr:hover td{{background:rgba(255,255,255,.03);}}
    a{{color:#38bdf8;text-decoration:none;}}
    a:hover{{color:#7dd3fc;}}
    .hint{{margin-top:1.5rem;font-size:.8rem;color:#475569;font-family:monospace;}}
  </style>
</head>
<body>
  <h1>PR String Audit — Semua Site</h1>
  <p>Klik nama site untuk membuka laporan. Untuk navigasi lebih nyaman, jalankan:<br>
     <code style="color:#38bdf8">python -m http.server 8080</code> dari folder <code>reports/pr_string_audit/</code>
     lalu buka <code>http://localhost:8080</code></p>
  <p style="margin-bottom:1rem">
    <a href="pr_summary_alert.html" style="color:#f87171;font-weight:600;border:1px solid rgba(239,68,68,.4);padding:6px 14px;border-radius:6px;font-size:.85rem">
      ⚠ PR Summary &amp; Alert — semua site
    </a>
  </p>
  <table>
    <thead><tr><th>Site</th><th>Site ID</th></tr></thead>
    <tbody>{''.join(rows)}</tbody>
  </table>
  <p class="hint">Generated: {__import__('datetime').datetime.now().strftime('%Y-%m-%d %H:%M')}</p>
</body>
</html>
"""
    (root / "index.html").write_text(html, encoding="utf-8")


def main() -> None:
    ap = argparse.ArgumentParser(description="Batch PR String Audit HTML reports (one file per site).")
    grp = ap.add_mutually_exclusive_group()
    grp.add_argument("--all-sites", action="store_true", help="Query all sites from DB")
    grp.add_argument("--sites", default=None, help="Comma-separated site_ids")
    grp.add_argument("--sites-file", default=None, help="File with one site_id per line")
    ap.add_argument("--start", default=None, help="YYYY-MM-DD start (default: 6 months back)")
    ap.add_argument("--end", default=None, help="YYYY-MM-DD end (default: today)")
    ap.add_argument("--months-back", type=int, default=6)
    ap.add_argument("--workers", type=int, default=4, help="Parallel generate processes")
    ap.add_argument("--waterfall", action="store_true", help="Also generate waterfall loss HTML per site (~75s per site)")
    args = ap.parse_args()

    script_dir = Path(__file__).resolve().parent
    gen = script_dir / "generate_pr_string_audit_html.py"
    if not gen.is_file():
        raise SystemExit(f"Generator script not found: {gen}")

    root = script_dir.parent / "reports" / "pr_string_audit"
    root.mkdir(parents=True, exist_ok=True)

    # Resolve date window
    from datetime import date
    import calendar as _cal

    if args.end:
        end_date = args.end
    else:
        end_date = date.today().isoformat()

    if args.start:
        start_date = args.start
    else:
        today = date.today()
        y, m = today.year, today.month - args.months_back
        while m <= 0:
            m += 12
            y -= 1
        start_date = date(y, m, 1).isoformat()

    # Build site list
    if args.all_sites:
        site_entries = fetch_all_sites(start_date)
    else:
        ids = parse_sites_arg(args.sites, args.sites_file)
        # We don't have names yet — generate will use mart's site_name
        site_entries = [SiteEntry(sid, sid) for sid in ids]
        # Try to get real names from DB
        try:
            cfg = load_dbt_db()
            conn = psycopg2.connect(
                host=cfg["host"], port=cfg["port"],
                user=cfg["user"], password=cfg["password"], dbname=cfg["dbname"],
            )
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(
                    "SELECT DISTINCT site_id, site_name FROM mart.mart_string_performance_daily"
                    " WHERE site_id = ANY(%s) LIMIT 200;",
                    ([s.site_id for s in site_entries],),
                )
                name_map = {str(r["site_id"]): str(r["site_name"]) for r in cur}
            conn.close()
            site_entries = [
                SiteEntry(s.site_id, name_map.get(s.site_id, s.site_id))
                for s in site_entries
            ]
        except Exception:
            pass

    print(f"Sites: {len(site_entries)}  |  window: {start_date} → {end_date}")

    gen_wf = script_dir / "generate_pr_waterfall_html.py"

    # Build commands
    tasks: list[tuple[SiteEntry, Path, list[str]]] = []
    for site in site_entries:
        out_dir = root / site.slug
        out_dir.mkdir(parents=True, exist_ok=True)
        out_path = out_dir / "index.html"
        cmds = [[
            sys.executable, str(gen),
            "--site-id", site.site_id,
            "--start", start_date,
            "--end", end_date,
            "--out", str(out_path),
        ]]
        if args.waterfall and gen_wf.is_file():
            cmds.append([
                sys.executable, str(gen_wf),
                "--site-id", site.site_id,
                "--site-name", site.site_name,
                "--start", start_date,
                "--end", end_date,
                "--out", str(out_dir / "waterfall.html"),
            ])
        tasks.append((site, out_path, cmds))

    # Run with limited parallelism
    import concurrent.futures
    results: list[tuple[SiteEntry, Path, bool]] = []

    def run_one(item: tuple[SiteEntry, Path, list]) -> tuple[SiteEntry, Path, bool]:
        site, out_path, cmds = item
        ok = True
        for cmd in cmds:
            r = subprocess.run(cmd, capture_output=True, text=True)
            if r.returncode != 0:
                ok = False
                print(f"    STDERR ({cmd[-1]}): {r.stderr[:300]}", file=sys.stderr)
        tag = "✓" if ok else "✗"
        print(f"  {tag} {site.site_name} ({site.site_id})")
        return site, out_path, ok

    with concurrent.futures.ThreadPoolExecutor(max_workers=args.workers) as pool:
        for res in pool.map(run_one, tasks):
            results.append(res)

    write_index(root, results)

    n_ok = sum(1 for _, _, ok in results if ok)
    n_fail = len(results) - n_ok
    print(f"\nDone: {n_ok}/{len(results)} berhasil.")
    print(f"Index: {root / 'index.html'}")
    if n_fail:
        print(f"{n_fail} gagal — cek output di atas.", file=sys.stderr)
        raise SystemExit(1)


if __name__ == "__main__":
    main()
