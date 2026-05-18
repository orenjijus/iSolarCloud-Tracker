"""
EDA PLTS mengikuti dokumentasi Solar Data Tools, PVAnalytics, dan pvlib-python.

Strategi: SATU SITE DULU — jalankan EDA untuk satu site, cek hasil & validasi metode/data.
Setelah oke, baru implement ke site lain (loop EDA_SITE atau batch).

- Solar Data Tools: DataHandler → run_pipeline(), plot heatmap, daily energy, data quality.
- PVAnalytics: quality (gaps, outliers), features (daytime).
- pvlib: clearsky (opsional).

Data: power time series mart.mart_inverter_performance_5min (inv_active_power), satu site.
Exclude: Samator & Klinik. Set PGPASSWORD; EDA_SITE (default satu site), EDA_DAYS.
"""
import os
import sys
from pathlib import Path

try:
    from dotenv import load_dotenv
    _eda_dir = Path(__file__).resolve().parent
    load_dotenv(_eda_dir / ".env")
except ImportError:
    pass

import pandas as pd
import numpy as np

EXCLUDE = "site_name NOT ILIKE '%Samator%' AND site_name NOT ILIKE '%Klinik%'"


def get_conn():
    import psycopg2
    # Prefer eda/.env: POSTGRES_* ; fallback to PGHOST/PGUSER/...
    host = os.environ.get("POSTGRES_HOST") or os.environ.get("PGHOST", "10.101.4.88")
    port = int(os.environ.get("POSTGRES_PORT") or os.environ.get("PGPORT", "5432"))
    user = os.environ.get("POSTGRES_USER") or os.environ.get("PGUSER", "juice")
    password = os.environ.get("POSTGRES_PASSWORD") or os.environ.get("PGPASSWORD")
    dbname = os.environ.get("POSTGRES_DB") or os.environ.get("PGDATABASE", "MMSR")
    if not password:
        print("Set POSTGRES_PASSWORD (or PGPASSWORD) in eda/.env")
        sys.exit(1)
    return psycopg2.connect(host=host, port=port, user=user, password=password, dbname=dbname)


def load_power_timeseries(conn, site_name: str, days: int = 365):
    """Load inverter AC power (inv_active_power) 5min, satu site, agregat per timestamp → DataFrame dengan DatetimeIndex dan kolom ac_power."""
    q = """
    SELECT timestamp AT TIME ZONE 'Asia/Jakarta' AS ts, SUM(metric_value) AS ac_power
    FROM mart.mart_inverter_performance_5min
    WHERE site_name = %(site_name)s AND metric_name = 'inv_active_power' AND metric_value IS NOT NULL
      AND date_key >= CURRENT_DATE - INTERVAL '1 day' * %(days)s
    GROUP BY timestamp
    ORDER BY timestamp
    """
    df = pd.read_sql(q, conn, params={"site_name": site_name, "days": days})
    df = df.rename(columns={"ts": "timestamp"})
    df["timestamp"] = pd.to_datetime(df["timestamp"])
    df = df.set_index("timestamp")
    df.index = df.index.tz_localize(None)  # Solar Data Tools often expects naive local time
    return df


def run_solar_data_tools(df_power, out_dir: str, power_col: str = "ac_power"):
    """Solar Data Tools: DataHandler → run_pipeline → report + plot heatmap, daily energy, data quality."""
    try:
        from solardatatools import DataHandler
    except ImportError:
        print("Solar Data Tools tidak terpasang (pip install solar-data-tools). Lewat ke PVAnalytics.")
        return None

    dh = DataHandler(df_power)
    print("Running Solar Data Tools pipeline (can take 1–2 min)...")
    dh.run_pipeline(power_col=power_col, fix_tz=True)
    print("\n--- Solar Data Tools report ---")
    dh.report()

    os.makedirs(out_dir, exist_ok=True)
    try:
        fig = dh.plot_heatmap()
        if fig is not None:
            fig.savefig(Path(out_dir) / "sdt_heatmap.png", dpi=100, bbox_inches="tight")
            print("Saved:", Path(out_dir) / "sdt_heatmap.png")
    except Exception as e:
        print("plot_heatmap skip:", e)
    try:
        fig = dh.plot_daily_energy()
        if fig is not None:
            fig.savefig(Path(out_dir) / "sdt_daily_energy.png", dpi=100, bbox_inches="tight")
            print("Saved:", Path(out_dir) / "sdt_daily_energy.png")
    except Exception as e:
        print("plot_daily_energy skip:", e)
    try:
        fig = dh.plot_data_quality_scatter()
        if fig is not None:
            fig.savefig(Path(out_dir) / "sdt_data_quality_scatter.png", dpi=100, bbox_inches="tight")
            print("Saved:", Path(out_dir) / "sdt_data_quality_scatter.png")
    except Exception as e:
        print("plot_data_quality_scatter skip:", e)
    return dh


def run_pvanalytics_quality(df_power, out_dir: str, power_col: str = "ac_power"):
    """PVAnalytics: completeness, outliers, daytime mask. Simpan ringkasan + plot ke out_dir."""
    import pvanalytics.quality.gaps as gaps
    import pvanalytics.quality.outliers as outliers
    import pvanalytics.features.daytime as daytime

    series = df_power[power_col].dropna()
    out_path = Path(out_dir)
    out_path.mkdir(parents=True, exist_ok=True)
    lines = ["=== PVAnalytics results ===", f"Series: {power_col}, n={len(series)}", ""]

    # Completeness score per day
    comp = None
    try:
        comp = gaps.completeness_score(series)
        lines.append("--- completeness_score (per day) ---")
        lines.append(comp.describe().to_string())
        lines.append("")
        print("\n--- PVAnalytics completeness (sample days) ---")
        print(comp.describe())
    except Exception as e:
        lines.append(f"completeness_score: skip ({e})")
        print("completeness_score skip:", e)

    # Outliers (Tukey IQR)
    mask_outlier = None
    try:
        mask_outlier = outliers.tukey(series, k=2.0)
        n_out = (~mask_outlier).sum()
        lines.append(f"--- outliers (Tukey k=2) ---")
        lines.append(f"Outlier count: {n_out} / {len(series)}")
        lines.append("")
        print(f"PVAnalytics outliers (Tukey k=2): {n_out} points")
    except Exception as e:
        lines.append(f"outliers.tukey: skip ({e})")
        print("outliers.tukey skip:", e)

    # Daytime mask (power-or-irradiance)
    daytime_mask = None
    try:
        daytime_mask = daytime.power_or_irradiance(series)
        n_day = int(daytime_mask.sum())
        lines.append("--- daytime (power_or_irradiance) ---")
        lines.append(f"Daytime points: {n_day} / {len(daytime_mask)}")
        print(f"PVAnalytics daytime: {n_day} / {len(daytime_mask)} points")
    except Exception as e:
        lines.append(f"daytime: skip ({e})")
        print("daytime.power_or_irradiance skip:", e)

    # Simpan ringkasan teks
    summary_file = out_path / "pvanalytics_summary.txt"
    summary_file.write_text("\n".join(lines), encoding="utf-8")
    print("Saved:", summary_file)

    # Plot: power time series + daytime mask
    try:
        import matplotlib
        matplotlib.use("Agg")
        import matplotlib.pyplot as plt

        fig, axes = plt.subplots(2, 1, figsize=(12, 5), sharex=True)
        axes[0].plot(series.index, series.values, alpha=0.7, label=power_col)
        axes[0].set_ylabel(power_col)
        axes[0].set_title("Power (5min) — PVAnalytics input")
        axes[0].legend(loc="upper right")

        if daytime_mask is not None:
            axes[1].fill_between(daytime_mask.index, 0, 1, where=daytime_mask.values, alpha=0.5, label="daytime")
            axes[1].set_ylabel("Daytime mask")
            axes[1].set_xlabel("Timestamp")
            axes[1].set_title("PVAnalytics: daytime (power_or_irradiance)")
            axes[1].legend(loc="upper right")
        axes[1].set_ylim(-0.1, 1.1)
        plt.tight_layout()
        fig.savefig(out_path / "pvanalytics_power_daytime.png", dpi=100, bbox_inches="tight")
        plt.close()
        print("Saved:", out_path / "pvanalytics_power_daytime.png")
    except Exception as e:
        print("PVAnalytics plot skip:", e)


def run_pvlib_clearsky_optional(lat: float, lon: float, times: pd.DatetimeIndex, out_dir: str):
    """pvlib: generate clearsky GHI untuk lokasi; bisa dipakai PVAnalytics clearsky_limits / variability_index."""
    try:
        import pvlib
        from pvlib.location import Location

        loc = Location(lat=lat, lon=lon, tz="Asia/Jakarta")
        cs = loc.get_clearsky(times)
        print("\n--- pvlib clearsky (sample) ---")
        print(cs.head())
        return cs
    except Exception as e:
        print("pvlib clearsky skip:", e)
        return None


def main():
    out_dir = Path(__file__).parent / "output"
    conn = get_conn()

    # Satu site dulu untuk validasi metode & data; setelah oke baru roll out ke site lain
    site_name = os.environ.get("EDA_SITE", "PT. MMKI 1.75 MWp - Painting Building")
    days = int(os.environ.get("EDA_DAYS", "180"))

    print("=== EDA satu site (validasi dulu, baru implement ke lain) ===")
    print(f"Site: {site_name}")
    print(f"Days: {days}")
    print("Loading power time series...")
    df_power = load_power_timeseries(conn, site_name, days=days)
    conn.close()

    if df_power.empty or len(df_power) < 100:
        print("Too few rows. Try another site or more days.")
        sys.exit(1)

    print("Shape:", df_power.shape)
    print("Date range:", df_power.index.min(), "to", df_power.index.max())

    # Solar Data Tools pipeline + plots (optional; butuh pip install solar-data-tools)
    run_solar_data_tools(df_power, str(out_dir))

    # PVAnalytics quality
    run_pvanalytics_quality(df_power, str(out_dir))

    # Optional: pvlib clearsky (butuh lat/lon dari dim_assets)
    # run_pvlib_clearsky_optional(lat, lon, df_power.index, str(out_dir))

    print("\nEDA PV stack selesai.")


if __name__ == "__main__":
    main()
