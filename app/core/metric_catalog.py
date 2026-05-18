"""Load metric catalog CSV + kamus Markdown; build verify SQL."""
from __future__ import annotations

import re
from datetime import date
from pathlib import Path

import pandas as pd

CATALOG_COLUMNS = [
    "table_schema",
    "table_name",
    "column_name",
    "display_name_id",
    "business_definition",
    "formula",
    "unit",
    "source_trail",
    "how_to_verify",
    "where_to_correct",
    "common_confusion",
    "priority",
]


def catalog_dir(workspace_root: Path) -> Path:
    return workspace_root / "docs" / "data-dictionary" / "catalog"


def dictionary_dir(workspace_root: Path) -> Path:
    return workspace_root / "docs" / "data-dictionary"


def list_catalog_tables(workspace_root: Path) -> list[str]:
    root = catalog_dir(workspace_root)
    if not root.is_dir():
        return []
    return sorted(p.stem for p in root.glob("*.csv"))


def load_catalog(workspace_root: Path, table_stem: str) -> pd.DataFrame:
    path = catalog_dir(workspace_root) / f"{table_stem}.csv"
    if not path.is_file():
        raise FileNotFoundError(f"Katalog tidak ada: {path}")
    df = pd.read_csv(path, delimiter=";", dtype=str).fillna("")
    missing = [c for c in CATALOG_COLUMNS if c not in df.columns]
    if missing:
        raise ValueError(f"Kolom hilang di {path.name}: {missing}")
    return df[CATALOG_COLUMNS]


def load_dictionary_markdown(workspace_root: Path, table_stem: str) -> str | None:
    path = dictionary_dir(workspace_root) / f"{table_stem}.md"
    if not path.is_file():
        return None
    return path.read_text(encoding="utf-8")


def filter_catalog(
    df: pd.DataFrame,
    *,
    priority: str | None = None,
    search: str = "",
) -> pd.DataFrame:
    out = df.copy()
    if priority and priority != "Semua":
        out = out[out["priority"].str.upper() == priority.upper()]
    if search.strip():
        q = search.strip().lower()
        mask = out.apply(
            lambda row: any(q in str(v).lower() for v in row),
            axis=1,
        )
        out = out[mask]
    return out.reset_index(drop=True)


def catalog_to_html(df: pd.DataFrame, title: str) -> str:
    """HTML ringkas untuk cetak / Save as PDF dari browser."""
    rows = []
    for _, r in df.iterrows():
        rows.append(
            f"""
            <tr>
              <td><code>{r['column_name']}</code></td>
              <td>{r['display_name_id']}</td>
              <td>{r['business_definition']}</td>
              <td><code>{r['formula']}</code></td>
              <td>{r['unit']}</td>
              <td>{r['source_trail']}</td>
              <td>{r['how_to_verify']}</td>
              <td>{r['where_to_correct']}</td>
              <td>{r['common_confusion']}</td>
            </tr>"""
        )
    body = "\n".join(rows)
    return f"""<!DOCTYPE html>
<html lang="id">
<head>
  <meta charset="utf-8"/>
  <title>{title}</title>
  <style>
    body {{ font-family: Segoe UI, Arial, sans-serif; margin: 24px; color: #111; }}
    h1 {{ font-size: 1.4rem; }}
    table {{ border-collapse: collapse; width: 100%; font-size: 0.85rem; }}
    th, td {{ border: 1px solid #ccc; padding: 6px 8px; vertical-align: top; }}
    th {{ background: #f0f4f8; }}
    code {{ font-size: 0.8rem; }}
    @media print {{ body {{ margin: 12px; }} }}
  </style>
</head>
<body>
  <h1>{title}</h1>
  <p>Katalog metrik MMSR — cetak halaman ini (Ctrl+P) untuk PDF.</p>
  <table>
    <thead>
      <tr>
        <th>Kolom DB</th><th>Nama tampilan</th>
        <th>Definisi</th><th>Rumus</th><th>Unit</th>
        <th>Sumber</th><th>Cara cek</th><th>Koreksi</th><th>Kebingungan umum</th>
      </tr>
    </thead>
    <tbody>{body}</tbody>
  </table>
</body>
</html>"""


_VERIFY_SQL = "verify_site_performance_daily_one_day.sql"


def verify_sql_path(workspace_root: Path) -> Path:
    return workspace_root / "dbt" / "analyses" / _VERIFY_SQL


def build_verify_sql(workspace_root: Path, site_name: str, check_date: date) -> str:
    path = verify_sql_path(workspace_root)
    if not path.is_file():
        raise FileNotFoundError(f"Query verifikasi tidak ada: {path}")
    sql = path.read_text(encoding="utf-8")
    site_esc = site_name.replace("'", "''")
    date_str = check_date.isoformat()
    sql = re.sub(
        r"'[^']*'::text AS site_name",
        f"'{site_esc}'::text AS site_name",
        sql,
        count=1,
    )
    sql = re.sub(
        r"'\d{4}-\d{2}-\d{2}'::date AS check_date",
        f"'{date_str}'::date AS check_date",
        sql,
        count=1,
    )
    return sql
