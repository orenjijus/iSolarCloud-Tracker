"""Resolve folder project dbt + validasi dbt_project.yml."""
from __future__ import annotations

import os
from pathlib import Path


def resolve_dbt_project_dir(workspace_root: Path) -> Path:
    """
    Lokasi folder yang berisi `dbt_project.yml`.

    Override dengan env `MMSR_DBT_PROJECT_DIR` jika project dbt tidak di `./dbt`
    relatif ke repo (mis. struktur folder berbeda).
    """
    env = os.environ.get("MMSR_DBT_PROJECT_DIR", "").strip()
    if env:
        return Path(env).expanduser().resolve()
    return (workspace_root / "dbt").resolve()


def dbt_project_yml_path(dbt_dir: Path) -> Path:
    return dbt_dir / "dbt_project.yml"


def assert_dbt_project_file(dbt_dir: Path) -> None:
    """Raise FileNotFoundError dengan pesan jelas jika manifest project hilang."""
    yml = dbt_project_yml_path(dbt_dir)
    if not yml.is_file():
        raise FileNotFoundError(
            f"dbt tidak bisa jalan: tidak ada file `{yml}`. "
            "Pastikan Anda berada di root repo yang berisi folder `dbt/`, "
            "atau set environment variable `MMSR_DBT_PROJECT_DIR` ke folder yang berisi `dbt_project.yml`. "
            "Jika file terhapus: pulihkan dari git (`git checkout -- dbt/dbt_project.yml`) atau salin dari repositori lengkap."
        )
