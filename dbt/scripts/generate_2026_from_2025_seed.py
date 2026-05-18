"""
Build calendar-year 2026 rows from 2025 rows in seed_daily_simulation_target:
  all numeric irradiance / energy / PR columns × 0.996 (99.6%).

- Date: same month-day as 2025, year bumped to 2026.
- Month label: English month name for the 2026 date.
- Week: copied from the 2025 row (display-only in seed; mart uses dim_date).
- For each (Site_Code, 2026 date): overwrites any existing 2026 row if a 2025
  source day exists; otherwise keeps the existing 2026 row unchanged.

Usage (from repo root):
  python dbt/scripts/generate_2026_from_2025_seed.py
Optional:
  python dbt/scripts/generate_2026_from_2025_seed.py 0.996
"""
from __future__ import annotations

import csv
import sys
from datetime import date, datetime
from decimal import Decimal, ROUND_HALF_UP
from pathlib import Path

SEED_PATH = Path(__file__).resolve().parents[1] / "seeds" / "seed_daily_simulation_target.csv"

SEED_COLUMNS = [
    "Site_Name",
    "Site_Code",
    "Week",
    "Month",
    "Date",
    "GHI",
    "POA",
    "Energy Simulation (MW)",
    "Energy Target (MW)",
    "Daily PR POA Simulation",
    "Daily PR POA Target",
    "GHI vs POA",
    "Daily PR GHI Simulation",
    "Daily PR GHI Target",
]

NUMERIC_COLS = [
    "GHI",
    "POA",
    "Energy Simulation (MW)",
    "Energy Target (MW)",
    "Daily PR POA Simulation",
    "Daily PR POA Target",
    "GHI vs POA",
    "Daily PR GHI Simulation",
    "Daily PR GHI Target",
]

MONTH_NAMES = {
    1: "January",
    2: "February",
    3: "March",
    4: "April",
    5: "May",
    6: "June",
    7: "July",
    8: "August",
    9: "September",
    10: "October",
    11: "November",
    12: "December",
}


def parse_date(s: str) -> date | None:
    s = (s or "").strip()
    if not s:
        return None
    for fmt in ("%d/%m/%Y", "%d-%m-%Y"):
        try:
            return datetime.strptime(s, fmt).date()
        except ValueError:
            continue
    return None


def fmt_date(d: date) -> str:
    return d.strftime("%d/%m/%Y")


def scale_decimal(raw: str, factor: Decimal) -> str:
    t = (raw or "").strip().replace(",", ".")
    if not t:
        t = "0"
    v = Decimal(t)
    out = (v * factor).quantize(Decimal("0.000000000001"), rounding=ROUND_HALF_UP)
    s = format(out, "f").rstrip("0").rstrip(".")
    return s if s else "0"


def row_key(site_code: str, d: date) -> tuple[str, str]:
    return (site_code.strip(), fmt_date(d))


def main() -> int:
    factor = Decimal(sys.argv[1]) if len(sys.argv) > 1 else Decimal("0.996")

    rows_in_order: list[dict] = []
    by_key: dict[tuple[str, str], dict] = {}

    with SEED_PATH.open(newline="", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f, delimiter=";")
        for row in reader:
            for k in SEED_COLUMNS:
                if k not in row:
                    row[k] = ""
            rows_in_order.append(row)
            sc = row["Site_Code"].strip()
            dt = parse_date(row.get("Date", ""))
            if sc and dt:
                by_key[row_key(sc, dt)] = row

    synthetic: list[dict] = []
    for row in rows_in_order:
        dt = parse_date(row.get("Date", ""))
        if dt is None or dt.year != 2025:
            continue
        sc = row["Site_Code"].strip()
        if not sc:
            continue
        try:
            d26 = dt.replace(year=2026)
        except ValueError:
            continue

        new_r = {k: row.get(k, "") for k in SEED_COLUMNS}
        new_r["Site_Name"] = row.get("Site_Name", "").strip()
        new_r["Site_Code"] = sc
        new_r["Week"] = row.get("Week", "").strip()
        new_r["Month"] = MONTH_NAMES[d26.month]
        new_r["Date"] = fmt_date(d26)
        for col in NUMERIC_COLS:
            new_r[col] = scale_decimal(row.get(col, ""), factor)
        synthetic.append(new_r)

    for new_r in synthetic:
        sc = new_r["Site_Code"].strip()
        dt = parse_date(new_r["Date"])
        if not sc or dt is None:
            continue
        by_key[row_key(sc, dt)] = new_r

    # Stable output: sort by site_code, date
    def sort_key(r: dict) -> tuple:
        sc = r["Site_Code"].strip()
        d = parse_date(r.get("Date", "")) or date.min
        return (sc, d)

    merged = sorted(by_key.values(), key=sort_key)

    with SEED_PATH.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=SEED_COLUMNS, delimiter=";", quoting=csv.QUOTE_MINIMAL)
        w.writeheader()
        for r in merged:
            w.writerow({k: r.get(k, "") for k in SEED_COLUMNS})

    n2026 = sum(1 for r in merged if (parse_date(r.get("Date", "")) or date.min).year == 2026)
    print(
        {
            "factor": str(factor),
            "synthetic_rows_from_2025": len(synthetic),
            "total_seed_rows": len(merged),
            "rows_year_2026": n2026,
            "seed_path": str(SEED_PATH),
        }
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
