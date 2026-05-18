"""
Jalankan EDA Site PLTS (site daily + device 5min daily agg).
Strategi: SATU SITE DULU — set EDA_SITE untuk analisis satu site saja; validasi hasil & metode
sebelum roll out ke semua site.
Exclude: Samator & Klinik. Butuh: PGHOST, PGUSER, PGPASSWORD, PGDATABASE (atau .env).
"""
import os
import sys

try:
    from dotenv import load_dotenv
    from pathlib import Path
    _eda_dir = Path(__file__).resolve().parent
    load_dotenv(_eda_dir / ".env")
except ImportError:
    pass

import pandas as pd
import numpy as np

EXCLUDE = "site_name NOT ILIKE '%Samator%' AND site_name NOT ILIKE '%Klinik%'"

def get_conn():
    try:
        import psycopg2
    except ImportError:
        print("pip install psycopg2-binary")
        sys.exit(1)
    # Prefer eda/.env: POSTGRES_* ; fallback to PGHOST/PGUSER/...
    host = os.environ.get("POSTGRES_HOST") or os.environ.get("PGHOST", "10.101.4.88")
    port = int(os.environ.get("POSTGRES_PORT") or os.environ.get("PGPORT", "5432"))
    user = os.environ.get("POSTGRES_USER") or os.environ.get("PGUSER", "juice")
    password = os.environ.get("POSTGRES_PASSWORD") or os.environ.get("PGPASSWORD", "")
    dbname = os.environ.get("POSTGRES_DB") or os.environ.get("PGDATABASE", "MMSR")
    if not password:
        print("Set POSTGRES_PASSWORD (or PGPASSWORD) in eda/.env")
        sys.exit(1)
    return psycopg2.connect(host=host, port=port, user=user, password=password, dbname=dbname)

def main():
    conn = get_conn()
    out_dir = os.path.join(os.path.dirname(__file__), "output")
    os.makedirs(out_dir, exist_ok=True)

    # --- Site daily (excl Samator/Klinik); opsional satu site saja (EDA_SITE) ---
    q_daily = f"""
    SELECT * FROM mart.mart_site_performance_daily
    WHERE {EXCLUDE} ORDER BY date_key, site_id
    """
    df = pd.read_sql(q_daily, conn)
    df["date_key"] = pd.to_datetime(df["date_key"])

    one_site = os.environ.get("EDA_SITE", "").strip()
    if one_site:
        df = df[df["site_name"] == one_site]
        if df.empty:
            print("EDA_SITE tidak ditemukan:", one_site)
            conn.close()
            sys.exit(1)
        print("=== Site daily — SATU SITE (validasi dulu) ===")
        print("Site:", one_site)
    else:
        print("=== Site daily (excl. Samator/Klinik, semua site) ===")
    print("Shape:", df.shape)
    print("Sites:", df["site_id"].nunique())
    print("Date range:", df["date_key"].min(), "to", df["date_key"].max())
    missing = df.isnull().sum()
    missing = missing[missing > 0].sort_values(ascending=False)
    print("Missing (top):", missing.head(10).to_dict())
    print("Avg daily_energy_mwh:", df["daily_energy_mwh"].mean().round(4))
    print("Avg pr_ghi_actual:", df["pr_ghi_actual"].mean().round(4))
    print("Avg availability_percent:", df["availability_percent"].mean().round(4))

    # Plots (optional)
    try:
        import matplotlib
        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
        import seaborn as sns
        sns.set_style("whitegrid")

        # 1. Histogram daily_energy_mwh
        fig, ax = plt.subplots(figsize=(8, 4))
        ax.hist(df["daily_energy_mwh"].dropna(), bins=50, edgecolor="black", alpha=0.7)
        ax.set_title("Daily energy (MWh) — excl. Samator/Klinik")
        ax.set_xlabel("daily_energy_mwh")
        fig.savefig(os.path.join(out_dir, "eda_daily_energy_hist.png"), dpi=100, bbox_inches="tight")
        plt.close()
        print("Saved:", os.path.join(out_dir, "eda_daily_energy_hist.png"))

        # 2. Time series avg energy
        daily_agg = df.groupby("date_key")["daily_energy_mwh"].mean()
        fig, ax = plt.subplots(figsize=(12, 3))
        ax.plot(daily_agg.index, daily_agg.values, alpha=0.8)
        ax.set_ylabel("Avg daily_energy_mwh")
        ax.set_title("Daily average energy (all sites) — excl. Samator/Klinik")
        fig.savefig(os.path.join(out_dir, "eda_daily_energy_ts.png"), dpi=100, bbox_inches="tight")
        plt.close()
        print("Saved:", os.path.join(out_dir, "eda_daily_energy_ts.png"))

        # 3. Correlation heatmap
        cols = ["daily_energy_mwh", "daily_ghi_kwh_m2", "availability_percent", "pr_ghi_actual", "pr_poa_actual"]
        cols = [c for c in cols if c in df.columns]
        corr = df[cols].corr()
        fig, ax = plt.subplots(figsize=(8, 6))
        sns.heatmap(corr, annot=True, fmt=".2f", cmap="coolwarm", center=0, square=True, ax=ax)
        ax.set_title("Correlation (site daily) — excl. Samator/Klinik")
        fig.savefig(os.path.join(out_dir, "eda_daily_corr.png"), dpi=100, bbox_inches="tight")
        plt.close()
        print("Saved:", os.path.join(out_dir, "eda_daily_corr.png"))
    except Exception as e:
        print("Plots skip:", e)

    # --- Device 5min daily aggregates (optional: set RUN_DEVICE_AGG=1, last 7 days) ---
    if os.environ.get("RUN_DEVICE_AGG") == "1":
        print("\n=== Device 5min daily aggregates (excl. Samator/Klinik, last 7 days) ===")
        for table, dtype in [
            ("mart_meter_performance_5min", "meter"),
            ("mart_sensor_measurements_5min", "sensor"),
            ("mart_inverter_performance_5min", "inverter"),
        ]:
            q = f"""
            SELECT date_key, site_name, system, asset_id, metric_name, metric_unit,
                   COUNT(*) AS n_points, ROUND(AVG(metric_value)::numeric, 6) AS avg_value,
                   MIN(metric_value) AS min_value, MAX(metric_value) AS max_value
            FROM mart.{table}
            WHERE {EXCLUDE} AND metric_value IS NOT NULL
              AND date_key >= CURRENT_DATE - INTERVAL '7 days'
            GROUP BY date_key, site_name, system, asset_id, metric_name, metric_unit
            """
            d = pd.read_sql(q, conn)
            d["date_key"] = pd.to_datetime(d["date_key"])
            d["device_type"] = dtype
            print(f"  {dtype}: rows={len(d)}, sites={d['site_name'].nunique()}, metrics={d['metric_name'].nunique()}")
    else:
        print("\nDevice 5min aggregates skipped (set RUN_DEVICE_AGG=1 to run).")

    conn.close()
    print("\nEDA run selesai.")

if __name__ == "__main__":
    main()
