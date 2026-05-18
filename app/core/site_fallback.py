"""Cadangan daftar site tanpa `dbt show` — dari seed_inverter_config.csv."""
from __future__ import annotations

from pathlib import Path

import pandas as pd


def load_sites_from_seed_inverter(seeds_dir: Path) -> pd.DataFrame:
    """
    Satu baris per (platform, site_id) dari `seed_inverter_config.csv`.
    Kolom: site_name, site_id, system_norm, option_label
    """
    path = seeds_dir / "seed_inverter_config.csv"
    if not path.exists():
        raise FileNotFoundError(f"Tidak ada {path}")
    inv = pd.read_csv(path, delimiter=";", dtype=str)
    inv["system_norm"] = inv["source"].astype(str).str.strip().str.lower()
    inv = inv[inv["system_norm"].isin(["isolarcloud", "fusionsolar"])].copy()
    sub = inv.groupby(["system_norm", "site_id"], as_index=False).agg(
        site_name=("site_name", "first"),
    )
    sub["site_id"] = sub["site_id"].astype(str).str.strip()
    sub["option_label"] = sub.apply(
        lambda r: f"{str(r['site_name']).strip() or '(tanpa nama)'} — `{r['site_id']}`",
        axis=1,
    )
    return sub
