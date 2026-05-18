"""
Audit seed_daily_simulation_target for calendar year 2026 completeness.
"""
from __future__ import annotations

import csv
from collections import defaultdict
from datetime import date, timedelta
from pathlib import Path

SEED_PATH = Path(__file__).resolve().parents[1] / "seeds" / "seed_daily_simulation_target.csv"
YEAR = 2026
FULL_YEAR_START = date(YEAR, 1, 1)
FULL_YEAR_END = date(YEAR, 12, 31)
EXPECTED_FULL_YEAR = (FULL_YEAR_END - FULL_YEAR_START).days + 1  # 365 for 2026

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


def parse_dmy(s: str) -> date | None:
    s = (s or "").strip()
    if not s:
        return None
    parts = s.replace("-", "/").split("/")
    if len(parts) != 3:
        return None
    try:
        d, m, y = int(parts[0]), int(parts[1]), int(parts[2])
        return date(y, m, d)
    except ValueError:
        return None


def dmy_string(d: date) -> str:
    return d.strftime("%d/%m/%Y")


def contiguous_ranges(sorted_missing: list[date]) -> list[tuple[date, date]]:
    if not sorted_missing:
        return []
    ranges: list[tuple[date, date]] = []
    start = prev = sorted_missing[0]
    for d in sorted_missing[1:]:
        if d == prev + timedelta(days=1):
            prev = d
            continue
        ranges.append((start, prev))
        start = prev = d
    ranges.append((start, prev))
    return ranges


def fmt_range(a: date, b: date) -> str:
    if a == b:
        return dmy_string(a)
    return f"{dmy_string(a)} – {dmy_string(b)}"


def main() -> None:
    by_site: dict[str, dict] = defaultdict(lambda: {"name": "", "dates": set(), "rows": []})

    with SEED_PATH.open(newline="", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f, delimiter=";")
        for row in reader:
            dt = parse_dmy(row.get("Date", ""))
            if dt is None or dt.year != YEAR:
                continue
            code = (row.get("Site_Code") or "").strip()
            name = (row.get("Site_Name") or "").strip()
            if not code:
                continue
            rec = by_site[code]
            rec["name"] = name or rec["name"]
            if dt in rec["dates"]:
                rec.setdefault("dup_dates", set()).add(dt)
            else:
                rec["dates"].add(dt)
            rec["rows"].append((dt, row))

    # Per-site analysis
    issues = []
    summary_rows = []

    for site_code in sorted(by_site.keys()):
        rec = by_site[site_code]
        dates_in_year = {d for d in rec["dates"] if FULL_YEAR_START <= d <= FULL_YEAR_END}
        actual = len(dates_in_year)
        expected = EXPECTED_FULL_YEAR
        full_set = {
            FULL_YEAR_START + timedelta(days=i) for i in range(EXPECTED_FULL_YEAR)
        }
        missing_sorted = sorted(full_set - dates_in_year)

        # Null / blank targets in 2026 rows
        null_rows = []
        dup_dates = set(rec.get("dup_dates", set()))
        for dt, row in rec["rows"]:
            if dt.year != YEAR or dt not in full_set:
                continue
            for col in NUMERIC_COLS:
                v = row.get(col)
                if v is None or str(v).strip() == "":
                    null_rows.append((dt, col))

        has_gap = len(missing_sorted) > 0
        not_full = actual != expected
        has_null = len(null_rows) > 0
        has_dup = len(dup_dates) > 0

        summary_rows.append(
            {
                "site_code": site_code,
                "site_name": rec["name"],
                "expected": expected,
                "actual": actual,
                "missing_count": len(missing_sorted),
                "missing_ranges": "; ".join(
                    fmt_range(a, b) for a, b in contiguous_ranges(missing_sorted)
                ),
                "has_duplicate_dates": has_dup,
                "null_issue_count": len(null_rows),
            }
        )

        if has_gap or not_full or has_null or has_dup:
            issues.append(
                {
                    "site_code": site_code,
                    "site_name": rec["name"],
                    "expected": expected,
                    "actual": actual,
                    "missing_count": len(missing_sorted),
                    "missing_ranges": contiguous_ranges(missing_sorted),
                    "dup_dates": sorted(dup_dates),
                    "null_rows_sample": null_rows[:20],
                }
            )

    # Sites that should be in 2026 but have zero rows? — skip unless we have a master list

    print("=== 2026 TARGET AUDIT: seed_daily_simulation_target ===\n")
    print(f"Seed file: {SEED_PATH}")
    print(f"Expected days in {YEAR} (full year): {EXPECTED_FULL_YEAR}\n")

    print(f"Sites with any {YEAR} row: {len(by_site)}")
    print(f"Sites with gaps / incomplete / nulls / duplicate dates: {len(issues)}\n")

    if issues:
        print("--- SITES WITH ISSUES ---\n")
        for x in issues:
            print(f"Site: {x['site_name']} (code: {x['site_code']})")
            print(f"  Expected: {x['expected']}  Actual distinct dates: {x['actual']}  Missing: {x['missing_count']}")
            if x["missing_ranges"]:
                mr = "; ".join(fmt_range(a, b) for a, b in x["missing_ranges"])
                print(f"  Missing date ranges: {mr}")
            if x["dup_dates"]:
                print(f"  Duplicate dates in seed (same site/date): {len(x['dup_dates'])} (e.g. {x['dup_dates'][:5]}...)")
            if x["null_rows_sample"]:
                print(f"  Null/empty numeric samples (first {len(x['null_rows_sample'])}):")
                for dt, col in x["null_rows_sample"]:
                    print(f"    {dmy_string(dt)}  {col}")
            print()

    print("--- FULL SUMMARY (all sites with 2026 data) ---\n")
    print(
        "site_name|site_code|expected|actual|missing|missing_ranges|null_fields|dup_dates"
    )
    for s in sorted(summary_rows, key=lambda r: (-r["missing_count"], r["site_name"])):
        flag = ""
        if s["missing_count"] or s["null_issue_count"] or s["has_duplicate_dates"]:
            flag = "ISSUE"
        print(
            f"{s['site_name']}|{s['site_code']}|{s['expected']}|{s['actual']}|{s['missing_count']}|{s['missing_ranges']}|{s['null_issue_count']}|{s['has_duplicate_dates']}|{flag}"
        )


if __name__ == "__main__":
    main()
