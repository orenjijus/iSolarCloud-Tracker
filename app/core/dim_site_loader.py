"""Load dim_site rows via `dbt show` (no extra DB driver)."""
from __future__ import annotations

import json
import re
import subprocess
from pathlib import Path
from typing import Any

from core.dbt_paths import assert_dbt_project_file
from core.dbt_runner import get_dbt_executable


def _strip_ansi(text: str) -> str:
    return re.sub(r"\x1b\[[0-9;]*m", "", text)


def fetch_dim_site_rows(
    dbt_project_dir: Path,
    limit: int = 2000,
    timeout_s: int = 120,
) -> list[dict[str, Any]]:
    """
    Run `dbt show --select dim_site --output json`.

    stdout/stderr digabung (dbt sering kirim log ke stderr). Tanpa `--quiet`
    supaya kompatibel dengan variasi dbt/OS; JSON diekstrak dengan raw_decode.
    """
    assert_dbt_project_file(dbt_project_dir)
    exe = get_dbt_executable()
    cmd = [
        exe,
        "show",
        "--select",
        "dim_site",
        "--limit",
        str(limit),
        "--output",
        "json",
    ]
    proc = subprocess.run(
        cmd,
        cwd=str(dbt_project_dir),
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        encoding="utf-8",
        errors="replace",
        timeout=timeout_s,
    )
    combined = _strip_ansi(proc.stdout or "")

    if proc.returncode != 0:
        tail = combined.strip()[-4000:] if combined else "(tanpa output)"
        raise RuntimeError(
            f"dbt show gagal (exit {proc.returncode}). Potongan output:\n{tail}"
        )

    if not combined.strip():
        raise RuntimeError(
            "dbt show tidak mengeluarkan output (stdout+stderr kosong). "
            "Coba di terminal: `cd dbt` lalu "
            "`dbt show --select dim_site --limit 5 --output json` "
            "untuk melihat error asli (database, profile, atau model tidak ada)."
        )

    # Cari objek JSON yang diproduksi dbt show (memuat key "node" dan "show")
    m = re.search(r'\{\s*"node"\s*:\s*"dim_site"', combined)
    if not m:
        tail = combined.strip()[-3500:]
        raise RuntimeError(
            "Tidak menemukan blok JSON dari `dbt show` (bukan format yang diharapkan). "
            "Potongan output terakhir:\n"
            f"{tail}"
        )

    decoder = json.JSONDecoder()
    try:
        payload, _ = decoder.raw_decode(combined, m.start())
    except json.JSONDecodeError as exc:
        snippet = combined[m.start() : m.start() + 800]
        raise RuntimeError(
            f"Gagal parse JSON dari dbt show: {exc}. Awal blok:\n{snippet}"
        ) from exc

    rows = payload.get("show")
    if not isinstance(rows, list):
        raise RuntimeError(f"Field 'show' bukan list: {type(rows)!r}")
    return rows
