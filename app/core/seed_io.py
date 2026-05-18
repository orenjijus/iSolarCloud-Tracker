"""Read/write seed CSV files."""
from __future__ import annotations

from pathlib import Path
from typing import Optional

import pandas as pd


def _normalize_column_names(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = [str(c).replace("\ufeff", "").strip() for c in df.columns]
    return df


def read_seed(seed_name: str, seeds_dir: Path, delimiter: str = ";") -> pd.DataFrame:
    path = seeds_dir / f"{seed_name}.csv"
    if not path.exists():
        raise FileNotFoundError(f"Seed file tidak ditemukan: {path}")
    df = pd.read_csv(path, delimiter=delimiter, dtype=str, keep_default_na=False)
    df = _normalize_column_names(df)
    for col in df.columns:
        df[col] = df[col].str.strip()
    return df


def write_seed(
    seed_name: str,
    df: pd.DataFrame,
    seeds_dir: Path,
    delimiter: str = ";",
    backup: bool = True,
) -> Path:
    path = seeds_dir / f"{seed_name}.csv"
    if backup and path.exists():
        bak_path = path.with_suffix(".csv.bak")
        bak_path.write_bytes(path.read_bytes())
    df.to_csv(path, sep=delimiter, index=False, encoding="utf-8")
    return path


def validate_seed(df: pd.DataFrame, required_columns: list[str]) -> list[str]:
    errors: list[str] = []
    missing_cols = [c for c in required_columns if c not in df.columns]
    if missing_cols:
        errors.append(f"Kolom wajib tidak ditemukan: {', '.join(missing_cols)}")
        return errors
    for col in required_columns:
        null_count = df[col].replace("", pd.NA).isna().sum()
        if null_count > 0:
            errors.append(f"Kolom '{col}' memiliki {null_count} baris kosong.")
    return errors


def parse_upload(
    uploaded_file,
    expected_columns: list[str],
    delimiter: str = ";",
) -> tuple[Optional[pd.DataFrame], list[str]]:
    errors: list[str] = []
    filename = uploaded_file.name.lower()
    try:
        if filename.endswith(".xlsx") or filename.endswith(".xls"):
            df = pd.read_excel(uploaded_file, dtype=str)
        elif filename.endswith(".csv"):
            uploaded_file.seek(0)
            df = None
            last_err: Exception | None = None
            for sep, use_python in (
                (None, True),
                (delimiter, False),
                (",", False),
                (";", False),
            ):
                try:
                    uploaded_file.seek(0)
                    if use_python:
                        cand = pd.read_csv(
                            uploaded_file,
                            sep=None,
                            engine="python",
                            dtype=str,
                            keep_default_na=False,
                        )
                    else:
                        cand = pd.read_csv(
                            uploaded_file,
                            delimiter=sep,
                            dtype=str,
                            keep_default_na=False,
                        )
                    if cand is not None and len(cand.columns) >= 2:
                        df = cand
                        break
                except Exception as exc:
                    last_err = exc
                    continue
            if df is None:
                raise last_err or ValueError("Tidak bisa mem-parse CSV")
        else:
            errors.append("Format file tidak didukung. Gunakan .xlsx atau .csv")
            return None, errors
    except Exception as exc:
        errors.append(f"Gagal membaca file: {exc}")
        return None, errors

    df = _normalize_column_names(df)
    for col in df.columns:
        df[col] = df[col].astype(str).str.strip()

    missing = [c for c in expected_columns if c not in df.columns]
    if missing:
        errors.append(f"Kolom wajib tidak ditemukan di file: {', '.join(missing)}")
        errors.append(f"Kolom yang ada: {', '.join(df.columns.tolist())}")
        return None, errors
    return df, errors
