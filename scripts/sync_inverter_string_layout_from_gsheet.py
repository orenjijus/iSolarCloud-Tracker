"""
Pull tab **JOIN String** from Google Sheet **Data Static MMSR** and merge into
`dbt/seeds/seed_inverter_string_layout.csv`.

Sheet export (CSV publik):
  https://docs.google.com/spreadsheets/d/1FRHHLbolgKicVw8gYTtfAxRw9n_YmRhCXxX3grbmqVE
  gid = JOIN String tab

Kolom sheet yang dipakai: Sites, Inverter No, MPPT No, String No, Module Qty, Orient Code.

Usage:
  python scripts/sync_inverter_string_layout_from_gsheet.py
  python scripts/sync_inverter_string_layout_from_gsheet.py --only "GM 1,CP MJL"
  python scripts/sync_inverter_string_layout_from_gsheet.py --dry-run

Setelah CSV ter-update, jalankan `dbt seed --select seed_inverter_string_layout` (atau full seed)
dan sinkronkan ke `staging.seed_inverter_string_layout` di Postgres sesuai proyek kamu.
"""

from __future__ import annotations

import argparse
import io
from pathlib import Path

import pandas as pd
import requests

SPREADSHEET_ID = "1FRHHLbolgKicVw8gYTtfAxRw9n_YmRhCXxX3grbmqVE"
JOIN_STRING_GID = "1795010703"
EXPORT_URL = (
    f"https://docs.google.com/spreadsheets/d/{SPREADSHEET_ID}/export"
    f"?format=csv&gid={JOIN_STRING_GID}"
)

ROOT = Path(__file__).resolve().parents[1]
SEED_PATH = ROOT / "dbt" / "seeds" / "seed_inverter_string_layout.csv"
EXPORT_SNAPSHOT = ROOT / "data" / "gsheet_exports" / "join_string_latest.csv"

# Kode kolom "Sites" di sheet -> (site_id, site_name) seperti di seed CSV
SITE_MAP: dict[str, tuple[str, str]] = {
    "PMM": ("NE=54435794", "PT. Pusan Manis Mulia 2.06 MWp - Tangerang"),
    "MID": ("NE=53771627", "PLTS Mall Panakkukang"),
    "SLI": ("1479456", "Shoetown Ligung Indonesia"),
    "MMKI 1": ("NE=50488260", "PT. MMKI 1.75 MWp - Painting Building"),
    "MMKI 2": ("NE=51758766", "PT. MMKI 5.7 MWp - Phase 2"),
    "MMKI 3": ("NE=58630782", "PT. MMKI 4.292 MWP - Phase 3"),
    "Suparma": ("NE=62806354", "PLTS ONGRID PT SUPARMA TBK"),
    "CP Bandung": ("1637095", "Charoen Pokphand Bandung"),
    "CP MDN": ("1637816", "Charoen Pokphand Madiun"),
    "CP MJL": ("1614122", "Charoen Pokphand Majalengka"),
    "FLN": ("1628909", "PLTS Frina Lestari Nusantara"),
    "GD": ("NE=61847068", "PT Gelora Djaja 1 MWp"),
    "GM 1": ("1458125", "Garuda Metalindo 1"),
    "GM 2": ("1453245", "Garuda Metalindo 2"),
    "GM 3": ("1445767", "Garuda Metalindo (IKP)"),
    "GM MPF": ("1449886", "Garuda Metalindo (MPF)"),
    "STBC": ("1763661", "PLTS On Grid Sorini Towa Berlian Corporindo"),
    "SPF": ("1680199", "PLTS Rooftop Sumatera Prima Fibreboard"),
}


def _source_for_site_id(site_id: str) -> str:
    return "FusionSolar" if site_id.startswith("NE=") else "iSolarCloud"


def fetch_join_string() -> pd.DataFrame:
    r = requests.get(EXPORT_URL, timeout=60)
    r.raise_for_status()
    df = pd.read_csv(io.StringIO(r.text))
    df.columns = [c.strip() for c in df.columns]
    return df


def build_seed_rows(j: pd.DataFrame, site_codes: set[str] | None) -> pd.DataFrame:
    codes = set(SITE_MAP.keys()) if site_codes is None else site_codes
    unknown = codes - set(SITE_MAP.keys())
    if unknown:
        raise ValueError(f"Kode Sites tidak dikenal: {sorted(unknown)}")

    m = j[j["Sites"].astype(str).str.strip().isin(codes)].copy()
    if m.empty:
        raise ValueError("Tidak ada baris untuk kode Sites yang dipilih.")

    def map_site_id(code: str) -> str:
        return SITE_MAP[code.strip()][0]

    def map_site_name(code: str) -> str:
        return SITE_MAP[code.strip()][1]

    m["site_id"] = m["Sites"].astype(str).str.strip().map(map_site_id)
    m["site_name"] = m["Sites"].astype(str).str.strip().map(map_site_name)
    m["source"] = m["site_id"].map(_source_for_site_id)
    m["inverter_id"] = ""
    m["inverter_name"] = ""
    m["is_active"] = True
    m["commission_date"] = ""
    m["decommission_date"] = ""
    m["notes"] = "from JOIN String (Google Sheet Data Static MMSR)"

    m["inverter_no"] = pd.to_numeric(m["Inverter No"], errors="coerce")
    m["mppt_no"] = pd.to_numeric(m["MPPT No"], errors="coerce")
    m["string_no"] = pd.to_numeric(m["String No"], errors="coerce")
    m["module_qty"] = pd.to_numeric(m["Module Qty"], errors="coerce")
    m["orient_code"] = pd.to_numeric(m["Orient Code"], errors="coerce")

    m = m.dropna(subset=["inverter_no", "mppt_no", "string_no"])

    out = m[
        [
            "source",
            "site_id",
            "site_name",
            "inverter_id",
            "inverter_name",
            "inverter_no",
            "mppt_no",
            "string_no",
            "module_qty",
            "orient_code",
            "is_active",
            "commission_date",
            "decommission_date",
            "notes",
        ]
    ]
    return out


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument(
        "--only",
        type=str,
        default=None,
        help='Kode Sites dipisah koma, mis. "GM 1,CP MJL". Default: semua kode di SITE_MAP.',
    )
    p.add_argument(
        "--dry-run",
        action="store_true",
        help="Ambil sheet + tampilkan ringkasan; tidak tulis seed CSV.",
    )
    p.add_argument(
        "--use-snapshot",
        action="store_true",
        help=f"Baca {EXPORT_SNAPSHOT} alih-alih unduh (untuk offline).",
    )
    args = p.parse_args()

    site_codes: set[str] | None = None
    if args.only:
        site_codes = {x.strip() for x in args.only.split(",") if x.strip()}

    EXPORT_SNAPSHOT.parent.mkdir(parents=True, exist_ok=True)
    if args.use_snapshot:
        j = pd.read_csv(EXPORT_SNAPSHOT)
        j.columns = [c.strip() for c in j.columns]
    else:
        j = fetch_join_string()
        j.to_csv(EXPORT_SNAPSHOT, index=False)

    new_rows = build_seed_rows(j, site_codes)
    targets = {SITE_MAP[c][1] for c in (site_codes or SITE_MAP)}

    if args.dry_run:
        print("dry-run: tidak menulis CSV")
        print(new_rows.groupby("site_name").size().to_string())
        print("total rows:", len(new_rows))
        return

    seed = pd.read_csv(SEED_PATH, sep=";")
    seed = seed[~seed["site_name"].astype(str).isin(targets)].copy()
    merged = pd.concat([seed, new_rows], ignore_index=True)
    merged = merged.sort_values(
        ["site_name", "inverter_no", "mppt_no", "string_no"], kind="stable"
    )
    merged.to_csv(SEED_PATH, sep=";", index=False)

    g = merged.groupby("site_name")["inverter_no"].nunique()
    for site in sorted(targets):
        n = int(g.get(site, 0))
        print(f"{site}: {n} inverter (distinct inverter_no in seed)")


if __name__ == "__main__":
    main()
