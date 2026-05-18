"""
Merge external daily simulation CSV into dbt seed seed_daily_simulation_target.csv.

- Preserves semicolon delimiter and column order expected by mart_simulation_targets_daily.
- Normalizes dates to DD/MM/YYYY for TO_DATE(..., 'DD/MM/YYYY').
- Maps numeric Month -> English month name (existing seed convention).
- New file rows win on duplicate (site_code, calendar date).
- Drops rows with missing Site_Code or unparseable Date; coerces blank numerics to 0.
"""
from __future__ import annotations

import csv
import re
import sys
from datetime import datetime
from pathlib import Path

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


def _norm_num(val: str) -> str:
    if val is None:
        return "0"
    s = str(val).strip()
    if s == "" or s.lower() in ("nan", "none", "null"):
        return "0"
    return s.replace(",", ".")


def _parse_date_to_key(s: str) -> tuple[datetime | None, str | None]:
    """Return (datetime, DD/MM/YYYY) or (None, None) if invalid."""
    if s is None:
        return None, None
    raw = str(s).strip()
    if not raw:
        return None, None
    for fmt in ("%d/%m/%Y", "%d-%m-%Y", "%Y-%m-%d"):
        try:
            dt = datetime.strptime(raw, fmt)
            return dt, dt.strftime("%d/%m/%Y")
        except ValueError:
            continue
    m = re.match(r"^(\d{1,2})/(\d{1,2})/(\d{4})$", raw)
    if m:
        d, mo, y = int(m.group(1)), int(m.group(2)), int(m.group(3))
        try:
            dt = datetime(y, mo, d)
            return dt, dt.strftime("%d/%m/%Y")
        except ValueError:
            return None, None
    return None, None


def _month_label(month_val: str) -> str:
    s = str(month_val).strip() if month_val is not None else ""
    if not s:
        return ""
    if s.isdigit():
        return MONTH_NUM_TO_NAME.get(int(s), s)
    return s


def _row_from_new(r: dict) -> dict | None:
    site_code = str(r.get("Site_Code", "")).strip()
    if not site_code:
        return None
    _, date_out = _parse_date_to_key(r.get("Date", ""))
    if not date_out:
        return None
    ghi = _norm_num(r.get("GHI"))
    poa = _norm_num(r.get("POA"))
    ghi_vs_raw = r.get("GHI vs POA")
    if ghi_vs_raw is not None and str(ghi_vs_raw).strip() != "":
        ghi_vs = _norm_num(ghi_vs_raw)
    else:
        try:
            ghi_vs = _norm_num(str(float(ghi) - float(poa)))
        except ValueError:
            ghi_vs = "0"

    return {
        "Site_Name": str(r.get("Site_Name", "")).strip(),
        "Site_Code": site_code,
        "Week": str(r.get("Week", "")).strip(),
        "Month": _month_label(r.get("Month", "")),
        "Date": date_out,
        "GHI": ghi,
        "POA": poa,
        "Energy Simulation (MW)": _norm_num(r.get("Energy Simulation (MW)")),
        "Energy Target (MW)": _norm_num(r.get("Energy Target (MW)")),
        "Daily PR POA Simulation": _norm_num(r.get("Daily PR POA Simulation")),
        "Daily PR POA Target": _norm_num(r.get("Daily PR POA Target")),
        "GHI vs POA": ghi_vs,
        "Daily PR GHI Simulation": _norm_num(r.get("Daily PR GHI Simulation")),
        "Daily PR GHI Target": _norm_num(r.get("Daily PR GHI Target")),
    }


def _row_from_seed(r: dict) -> dict | None:
    site_code = str(r.get("Site_Code", "")).strip()
    if not site_code:
        return None
    _, date_out = _parse_date_to_key(r.get("Date", ""))
    if not date_out:
        return None
    out = {k: str(r.get(k, "")).strip() if r.get(k) is not None else "" for k in SEED_COLUMNS}
    out["Site_Code"] = site_code
    out["Date"] = date_out
    out["Month"] = _month_label(out.get("Month", ""))
    for k in SEED_COLUMNS:
        if k in ("Site_Name", "Site_Code", "Week", "Month", "Date"):
            continue
        if not str(out.get(k, "")).strip():
            out[k] = "0"
        else:
            out[k] = _norm_num(out[k])
    return out


def merge(seed_path: Path, incoming_path: Path, out_path: Path) -> dict:
    by_key: dict[tuple[str, str], dict] = {}
    skipped_new = 0
    skipped_old = 0

    with seed_path.open(newline="", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f, delimiter=";")
        for r in reader:
            row = _row_from_seed(r)
            if row is None:
                skipped_old += 1
                continue
            key = (row["Site_Code"], row["Date"])
            by_key[key] = row

    with incoming_path.open(newline="", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f, delimiter=",")
        for r in reader:
            row = _row_from_new(r)
            if row is None:
                skipped_new += 1
                continue
            key = (row["Site_Code"], row["Date"])
            by_key[key] = row

    rows = sorted(by_key.values(), key=lambda x: (x["Site_Code"], _parse_date_to_key(x["Date"])[0] or datetime.min))

    out_path.parent.mkdir(parents=True, exist_ok=True)
    with out_path.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=SEED_COLUMNS, delimiter=";", quoting=csv.QUOTE_MINIMAL)
        w.writeheader()
        for row in rows:
            w.writerow({k: row[k] for k in SEED_COLUMNS})

    return {
        "total_rows": len(rows),
        "skipped_incoming": skipped_new,
        "skipped_existing": skipped_old,
    }


def main() -> int:
    root = Path(__file__).resolve().parents[1]
    seed = root / "seeds" / "seed_daily_simulation_target.csv"
    if len(sys.argv) >= 2:
        incoming = Path(sys.argv[1])
    else:
        print("Usage: merge_seed_daily_simulation.py <incoming_csv>", file=sys.stderr)
        return 1
    stats = merge(seed, incoming, seed)
    print(stats)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
