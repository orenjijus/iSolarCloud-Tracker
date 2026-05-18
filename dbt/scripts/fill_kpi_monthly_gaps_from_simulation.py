"""
Fill gaps in seed_daily_kpi_monthly.csv using monthly sums of
\"Energy Target (MW)\" from seed_daily_simulation_target.csv.

Rationale: energy_kpi_daily = kpi_monthly * (daily_target / monthly_target_sum).
If kpi_monthly equals monthly_target_sum, then energy_kpi_daily = daily_target.

Only inserts/replaces rows where the monthly aggregate from simulation is > 0.
Existing KPI values are kept unless the key (site_id, year, month) is missing
or the current Energy_KPI_MWh is empty/zero.

site_id mapping matches seed convention:
  - Site_Code starting with NE=  -> FS_SITE_<Site_Code>
  - else                         -> ISO_SITE_<Site_Code>
"""
from __future__ import annotations

import csv
from collections import defaultdict
from datetime import datetime
from decimal import Decimal
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SIM_PATH = ROOT / "seeds" / "seed_daily_simulation_target.csv"
KPI_PATH = ROOT / "seeds" / "seed_daily_kpi_monthly.csv"

KPI_FIELDS = [
    "site_Name",
    "site_id",
    "Year",
    "Month",
    "Month_Number",
    "Energy_KPI_MWh",
]

MONTH_NUM_TO_NAME = {
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


def parse_sim_date(s: str) -> datetime | None:
    s = (s or "").strip()
    if not s:
        return None
    for fmt in ("%d/%m/%Y", "%d-%m-%Y"):
        try:
            return datetime.strptime(s, fmt)
        except ValueError:
            continue
    return None


def to_kpi_site_id(site_code: str) -> str:
    sc = site_code.strip()
    if sc.upper().startswith("NE="):
        return "FS_SITE_" + sc
    return "ISO_SITE_" + sc


def fmt_kpi_val(v: Decimal) -> str:
    q = v.quantize(Decimal("0.01"))
    return format(q, "f")


def main() -> None:
    # (site_code, year, month) -> sum energy target
    monthly_target: dict[tuple[str, int, int], Decimal] = defaultdict(lambda: Decimal(0))
    site_names: dict[str, str] = {}

    with SIM_PATH.open(newline="", encoding="utf-8-sig") as f:
        r = csv.DictReader(f, delimiter=";")
        for row in r:
            code = (row.get("Site_Code") or "").strip()
            if not code:
                continue
            dt = parse_sim_date(row.get("Date", ""))
            if not dt:
                continue
            name = (row.get("Site_Name") or "").strip()
            if name:
                site_names.setdefault(code, name)
            raw = (row.get("Energy Target (MW)") or "0").strip().replace(",", ".")
            try:
                val = Decimal(raw)
            except Exception:
                val = Decimal(0)
            monthly_target[(code, dt.year, dt.month)] += val

    rows: list[dict] = []
    with KPI_PATH.open(newline="", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        for row in reader:
            rows.append({k: row.get(k, "") for k in KPI_FIELDS})

    by_key: dict[tuple[str, int, int], dict] = {}
    for row in rows:
        try:
            y = int(row["Year"])
            m = int(row["Month_Number"])
        except ValueError:
            continue
        sid = row.get("site_id", "").strip()
        if sid and 1 <= m <= 12:
            by_key[(sid, y, m)] = row

    added = 0
    updated = 0

    for (code, year, month), total in monthly_target.items():
        if total <= 0:
            continue
        kid = to_kpi_site_id(code)
        key = (kid, year, month)
        val_str = fmt_kpi_val(total)
        name = site_names.get(code, "")

        if key not in by_key:
            by_key[key] = {
                "site_Name": name,
                "site_id": kid,
                "Year": str(year),
                "Month": MONTH_NUM_TO_NAME[month],
                "Month_Number": str(month),
                "Energy_KPI_MWh": val_str,
            }
            added += 1
            continue

        existing = by_key[key]
        cur = (existing.get("Energy_KPI_MWh") or "").strip().replace('"', "")
        if not cur or cur == "0" or cur == "0.0":
            existing["site_Name"] = existing.get("site_Name") or name
            existing["Energy_KPI_MWh"] = val_str
            existing["Month"] = MONTH_NUM_TO_NAME[month]
            existing["Month_Number"] = str(month)
            updated += 1

    out_rows = sorted(
        by_key.values(),
        key=lambda x: (
            x.get("site_Name") or "",
            int(x.get("Year") or 0),
            int(x.get("Month_Number") or 0),
        ),
    )

    with KPI_PATH.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=KPI_FIELDS, quoting=csv.QUOTE_MINIMAL)
        w.writeheader()
        for row in out_rows:
            w.writerow({k: row.get(k, "") for k in KPI_FIELDS})

    print({"filled_new": added, "filled_empty": updated, "total_kpi_rows": len(out_rows)})


if __name__ == "__main__":
    main()
