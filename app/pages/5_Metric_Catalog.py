"""Metric Catalog — kamus metrik, definisi, verifikasi 1 site × 1 hari."""
from __future__ import annotations

import json
import sys
from datetime import date, timedelta
from pathlib import Path

import pandas as pd
import streamlit as st

APP_DIR = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(APP_DIR))

from core.db_query import postgres_available, run_sql
from core.dbt_paths import resolve_dbt_project_dir
from core.metric_catalog import (
    build_verify_sql,
    catalog_to_html,
    filter_catalog,
    list_catalog_tables,
    load_catalog,
    load_dictionary_markdown,
)
from core.site_fallback import load_sites_from_seed_inverter

WORKSPACE_ROOT = APP_DIR.parent
DBT_PROJECT_DIR = resolve_dbt_project_dir(WORKSPACE_ROOT)
SEEDS_DIR = DBT_PROJECT_DIR / "seeds"

st.set_page_config(page_title="Metric Catalog — MMSR", page_icon="📖", layout="wide")
st.title("📖 Metric Catalog")
st.caption(
    "Definisi kolom mart, rumus, jejak sumber, cara cek, dan koreksi — "
    "tanpa membuka SQL model dbt."
)
st.markdown("---")

TABLE_LABELS = {
    "mart_inverter_performance_5min": "Inverter 5-menit (daily pipeline)",
    "mart_meter_performance_5min": "Meter 5-menit (daily + HV)",
    "mart_sensor_measurements_5min": "Sensor 5-menit (daily pipeline)",
    "fact_inverter_calculations_5min": "Fact inverter 5-menit (daily pipeline)",
    "fact_sensor_calculations_5min": "Fact sensor 5-menit (daily pipeline)",
    "fact_site_calculations_5min": "Fact site 5-menit (daily pipeline)",
    "mart_sensor_daily": "Sensor harian (daily pipeline)",
    "mart_site_performance_daily": "Site performance harian (P0)",
    "fact_meter_active_power_corrected_5min": "HV corrected meter power 5-menit",
    "mart_battery_performance_5min": "HV battery 5-menit",
    "mart_hidden_valley_villa_load_5min": "HV villa load 5-menit",
    "mart_hidden_valley_villa_load_daily": "HV villa load harian",
    "mart_meter_daily_hidden_valley": "Hidden Valley meter harian (P0)",
}


@st.cache_data(ttl=120)
def _catalog_cached(workspace: str, table_stem: str) -> pd.DataFrame:
    return load_catalog(Path(workspace), table_stem)


@st.cache_data(ttl=60)
def _sites_cached(seeds_dir_str: str) -> pd.DataFrame:
    return load_sites_from_seed_inverter(Path(seeds_dir_str))


tables = list_catalog_tables(WORKSPACE_ROOT)
if not tables:
    st.error("Tidak ada file di `docs/data-dictionary/catalog/`.")
    st.stop()

with st.sidebar:
    st.header("Filter")
    table_stem = st.selectbox(
        "Tabel",
        options=tables,
        format_func=lambda t: TABLE_LABELS.get(t, t),
    )
    try:
        catalog_df = _catalog_cached(str(WORKSPACE_ROOT), table_stem)
    except Exception as exc:
        st.error(str(exc))
        st.stop()

    search = st.text_input("Cari kolom / definisi", placeholder="energy, GHI, a_KPI…")
    filtered = filter_catalog(catalog_df, search=search)
    st.caption(f"{len(filtered)} / {len(catalog_df)} kolom")

tab_browse, tab_detail, tab_verify, tab_dict, tab_export = st.tabs(
    ["Daftar kolom", "Detail kolom", "Verifikasi", "Kamus lengkap", "Export"]
)

with tab_browse:
    st.subheader(f"`mart.{table_stem}`")
    display_cols = [
        "column_name",
        "display_name_id",
        "business_definition",
        "formula",
        "unit",
    ]
    st.dataframe(
        filtered[display_cols].rename(
            columns={
                "column_name": "Kolom DB",
                "display_name_id": "Nama tampilan",
                "business_definition": "Definisi",
                "formula": "Rumus",
                "unit": "Unit",
            }
        ),
        use_container_width=True,
        hide_index=True,
    )
    with st.expander("Trail sumber & koreksi (semua kolom terfilter)", expanded=False):
        st.dataframe(
            filtered[
                [
                    "column_name",
                    "source_trail",
                    "how_to_verify",
                    "where_to_correct",
                    "common_confusion",
                ]
            ],
            use_container_width=True,
            hide_index=True,
        )

with tab_detail:
    if filtered.empty:
        st.info("Tidak ada kolom yang cocok dengan filter.")
    else:
        col_pick = st.selectbox(
            "Pilih kolom",
            options=filtered["column_name"].tolist(),
            format_func=lambda c: f"{c} — {filtered.loc[filtered['column_name'] == c, 'display_name_id'].iloc[0]}",
        )
        row = filtered.loc[filtered["column_name"] == col_pick].iloc[0]
        c1, c2 = st.columns(2)
        with c1:
            st.markdown(f"**Nama tampilan:** {row['display_name_id']}")
            st.markdown(f"**Unit:** {row['unit'] or '—'}")
            st.markdown("**Definisi bisnis**")
            st.info(row["business_definition"] or "—")
            st.markdown("**Rumus**")
            st.code(row["formula"] or "—", language="text")
        with c2:
            st.markdown("**Sumber data (trail)**")
            st.code(row["source_trail"] or "—", language="text")
            st.markdown("**Cara cek**")
            st.write(row["how_to_verify"] or "—")
            st.markdown("**Koreksi jika salah**")
            st.write(row["where_to_correct"] or "—")
            if row["common_confusion"] and str(row["common_confusion"]) != "-":
                st.warning(f"Kebingungan umum: {row['common_confusion']}")

        if col_pick == "energy_a_kpi_daily_mwh":
            st.markdown("---")
            st.markdown(
                "**a_KPI (satu kalimat):** "
                "`target_harian × (KPI_bulan / simulasi_bulan)` — "
                "bukan energi aktual, bukan availability."
            )

with tab_verify:
    st.subheader("Verifikasi 1 site × 1 hari")
    st.caption(
        "Menjalankan `dbt/analyses/verify_site_performance_daily_one_day.sql` "
        "dengan site & tanggal yang Anda pilih."
    )

    if table_stem != "mart_site_performance_daily":
        st.warning("Query verifikasi saat ini hanya untuk `mart_site_performance_daily`.")
    else:
        try:
            site_df = _sites_cached(str(SEEDS_DIR))
            site_names = sorted(
                site_df["site_name"].dropna().astype(str).str.strip().unique().tolist(),
                key=str.lower,
            )
        except Exception as exc:
            st.error(f"Tidak bisa memuat daftar site: {exc}")
            site_names = []

        v1, v2 = st.columns(2)
        site_name = v1.selectbox(
            "Site",
            options=site_names or ["(isi manual di bawah)"],
            index=0,
        )
        manual_site = v1.text_input("Atau ketik nama site persis seperti di mart", "")
        if manual_site.strip():
            site_name = manual_site.strip()

        default_day = date.today() - timedelta(days=2)
        check_date = v2.date_input("Tanggal", value=default_day)

        if st.button("Jalankan verifikasi", type="primary"):
            if not site_name or site_name.startswith("("):
                st.error("Pilih atau ketik nama site.")
            else:
                try:
                    sql = build_verify_sql(WORKSPACE_ROOT, site_name, check_date)
                except Exception as exc:
                    st.error(str(exc))
                    st.stop()

                with st.expander("SQL yang dijalankan", expanded=False):
                    st.code(sql, language="sql")

                if not postgres_available(WORKSPACE_ROOT):
                    st.warning(
                        "Koneksi DB tidak dikonfigurasi (`POSTGRES_HOST` + `POSTGRES_PASSWORD` di `.env`). "
                        "Salin SQL di atas ke DBeaver / psql."
                    )
                else:
                    with st.spinner("Query ke PostgreSQL…"):
                        try:
                            result = run_sql(WORKSPACE_ROOT, sql)
                        except Exception as exc:
                            st.error(f"Query gagal: {exc}")
                            st.stop()

                    for _, r in result.iterrows():
                        section = r.get("section", r.iloc[0])
                        payload = r.get("payload", r.iloc[1])
                        st.markdown(f"### {section}")
                        if payload is None or (isinstance(payload, float) and pd.isna(payload)):
                            st.caption("(kosong)")
                            continue
                        if isinstance(payload, str):
                            try:
                                payload = json.loads(payload)
                            except json.JSONDecodeError:
                                st.code(payload, language="json")
                                continue
                        st.json(payload)

                    st.success("Selesai — bandingkan `1_mart_output` dengan Excel / portal.")

with tab_dict:
    md = load_dictionary_markdown(WORKSPACE_ROOT, table_stem)
    if md:
        st.caption(
            "Checklist Ops/Finance ada di bagian bawah dokumen ini — "
            "centang manual di Markdown, bukan di aplikasi."
        )
        st.markdown(md)
    else:
        st.info(f"Belum ada `{table_stem}.md` di `docs/data-dictionary/`.")

with tab_export:
    st.subheader("Export katalog (HTML → cetak PDF)")
    title = f"MMSR Metric Catalog — {table_stem}"
    html = catalog_to_html(filtered, title)
    st.download_button(
        "Unduh HTML",
        data=html.encode("utf-8"),
        file_name=f"metric_catalog_{table_stem}.html",
        mime="text/html",
    )
    st.caption("Buka file HTML di browser → Ctrl+P → Save as PDF.")
    with st.expander("Pratinjau", expanded=False):
        st.components.v1.html(html, height=400, scrolling=True)

st.markdown("---")
st.caption(
    "Sumber: `docs/data-dictionary/` · "
    "[README kamus](../docs/data-dictionary/README.md) · "
    "Registry device: laporan audit §10"
)
