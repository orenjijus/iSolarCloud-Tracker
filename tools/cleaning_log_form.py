"""
Form input Log — Cleaning Log & Weekly Log dalam satu aplikasi Streamlit.
• Tab Cleaning Log: INSERT ke staging.cleaning_log (site dari dim_site).
• Tab Weekly Log: INSERT ke staging.seed_weekly_log + evaluasi (Seed/Mart: days_open, is_open).
Jalankan: streamlit run tools/cleaning_log_form.py
"""
import os
import urllib.parse
from datetime import date

import streamlit as st
import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

# Load .env dari root project atau dari folder tools/
load_dotenv()
_load_tools_env = os.path.join(os.path.dirname(os.path.abspath(__file__)), ".env")
if os.path.isfile(_load_tools_env):
    load_dotenv(_load_tools_env)

POSTGRES_HOST = os.getenv("POSTGRES_HOST", "10.101.4.88")
POSTGRES_PORT = os.getenv("POSTGRES_PORT", "5432")
POSTGRES_DB = os.getenv("POSTGRES_DB", "MMSR")
POSTGRES_USER = os.getenv("POSTGRES_USER", "juice")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")
encoded_password = urllib.parse.quote_plus(POSTGRES_PASSWORD or "")
DATABASE_URL = f"postgresql://{POSTGRES_USER}:{encoded_password}@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"


def _load_sites_impl():
    """Ambil daftar site dari dimensions.dim_site: site_id, site_code, site_name."""
    engine = create_engine(DATABASE_URL)
    rows = []
    last_error = None
    try:
        with engine.connect() as conn:
            rows = conn.execute(
                text("""
                    SELECT site_id, site_code, site_name
                    FROM dimensions.dim_site
                    WHERE site_name IS NOT NULL AND TRIM(site_name) != ''
                    ORDER BY site_name
                """)
            ).fetchall()
            if rows:
                rows = [(r[0], r[1], r[2]) for r in rows]
    except Exception as e1:
        last_error = str(e1)
        try:
            with engine.connect() as conn:
                rows = conn.execute(
                    text("""
                        SELECT site_id, site_name
                        FROM dimensions.dim_site
                        WHERE site_name IS NOT NULL AND TRIM(site_name) != ''
                        ORDER BY site_name
                    """)
                ).fetchall()
            if rows:
                rows = [(r[0], r[0], r[1]) for r in rows]
                last_error = None
        except Exception as e2:
            last_error = str(e2)
    return (rows, last_error if not rows else None)


@st.cache_data(ttl=300)
def load_sites():
    """Daftar site dari dimensions.dim_site untuk dropdown. Cache 5 menit."""
    try:
        sites, last_error = _load_sites_impl()
        return sites, last_error
    except Exception as e:
        return [], str(e)


def fetch_weekly_seed_recent(engine, limit=20):
    """Data terakhir di staging.seed_weekly_log (input)."""
    try:
        with engine.connect() as conn:
            df = pd.read_sql(
                text("""
                    SELECT log_date, site_id, site_name, problem_identification, corrective_action, status, created_at
                    FROM staging.seed_weekly_log
                    ORDER BY log_date DESC, created_at DESC NULLS LAST
                    LIMIT :lim
                """),
                conn,
                params={"lim": limit},
            )
            return df, None
    except Exception as e:
        return None, str(e)


def fetch_weekly_mart_recent(engine, limit=20):
    """Data terakhir di mart.mart_weekly_log (output: days_open, is_open)."""
    try:
        with engine.connect() as conn:
            df = pd.read_sql(
                text("""
                    SELECT log_date, site_name, problem_identification, corrective_action, status, days_open, is_open
                    FROM mart.mart_weekly_log
                    ORDER BY log_date DESC, site_name
                    LIMIT :lim
                """),
                conn,
                params={"lim": limit},
            )
            return df, None
    except Exception as e:
        return None, str(e)


# --- UI: satu aplikasi, dua tab ---
st.set_page_config(page_title="Input Log — Cleaning & Weekly", page_icon="📋", layout="centered")
st.title("📋 Input Log — Cleaning & Weekly")
st.caption("Satu form: Cleaning Log (staging.cleaning_log) dan Weekly Log (staging.seed_weekly_log). Site dari dim_site.")

if st.button("🔄 Refresh daftar site", help="Muat ulang daftar site dari database"):
    load_sites.clear()

sites, load_error = load_sites()
if not sites:
    st.error("Tidak ada daftar site. Pastikan koneksi DB dan tabel dimensions.dim_site ada (kolom site_id, site_name).")
    if load_error:
        st.code(load_error, language=None)
    st.caption("Pastikan: 1) .env berisi POSTGRES_*. 2) Tabel dimensions.dim_site ada dan punya site_name tidak kosong.")
    st.stop()

# Opsi dropdown (shared oleh kedua tab)
seen = set()
options = []
for site_id, site_code, site_name in sites:
    key = (site_id, site_name)
    if key in seen:
        continue
    seen.add(key)
    options.append((site_id, site_code, site_name))
label_count = {}
for (site_id, site_code, site_name) in options:
    name = (site_name or "").strip() or str(site_id)
    label_count[name] = label_count.get(name, 0) + 1
option_labels = []
for (site_id, site_code, site_name) in options:
    name = (site_name or "").strip() or str(site_id)
    if label_count[name] > 1:
        option_labels.append(f"{name} ({site_id})")
    else:
        option_labels.append(name)

tab_cleaning, tab_weekly = st.tabs(["🧹 Cleaning Log", "📋 Weekly Log"])

# ========== Tab 1: Cleaning Log ==========
with tab_cleaning:
    st.subheader("Cleaning Log")
    st.caption("Log pembersihan sensor/module per site → staging.cleaning_log.")
    with st.form("cleaning_log_form", clear_on_submit=True):
        cleaning_choice = st.selectbox(
            "Yang dibersihkan",
            ["module", "sensor", "both"],
            format_func=lambda x: {"module": "Module (panel)", "sensor": "Sensor", "both": "Keduanya (module + sensor)"}[x],
            help="Pilih module, sensor, atau keduanya.",
        )
        cleaning_date = st.date_input("Tanggal pembersihan", value=date.today())
        selected_label_clean = st.selectbox("Site", option_labels, key="site_clean", help="Pilih site (site_id & site_name dari dim_site).")
        notes = st.text_area("Catatan (opsional)", placeholder="Pembersihan rutin, setelah hujan, dll.")
        submitted_clean = st.form_submit_button("Simpan")

    if submitted_clean:
        idx = option_labels.index(selected_label_clean)
        site_id_val, site_code_val, site_name_val = options[idx]
        site_id_val = site_id_val if (site_id_val is not None and str(site_id_val).strip()) else None
        site_name_val = site_name_val if (site_name_val and str(site_name_val).strip()) else None
        if not site_id_val and not site_name_val:
            st.warning("⚠️ Site tidak valid.")
        else:
            types_to_insert = ["module", "sensor"] if cleaning_choice == "both" else [cleaning_choice]
            notes_val = (notes or "").strip() or None
            try:
                engine = create_engine(DATABASE_URL)
                with engine.connect() as conn:
                    for asset_type in types_to_insert:
                        conn.execute(
                            text("""
                                INSERT INTO staging.cleaning_log
                                (asset_type, site_id, site_name, cleaning_date, notes, is_active)
                                VALUES (:asset_type, :site_id, :site_name, :cleaning_date, :notes, 1)
                            """),
                            {
                                "asset_type": asset_type,
                                "site_id": site_id_val,
                                "site_name": site_name_val,
                                "cleaning_date": cleaning_date,
                                "notes": notes_val,
                            },
                        )
                    conn.commit()
                n = len(types_to_insert)
                st.success(f"✅ {n} record berhasil disimpan ke staging.cleaning_log ({', '.join(types_to_insert)}). Refresh Power BI untuk Days Since Last Cleaning.")
            except Exception as e:
                st.error(f"❌ Gagal menyimpan: {e}")

    st.markdown("**Cara pakai:** Pilih yang dibersihkan → Site → Tanggal & catatan → Simpan. Keduanya = 2 record (module + sensor).")

# ========== Tab 2: Weekly Log ==========
with tab_weekly:
    st.subheader("Weekly Log")
    st.caption("Log masalah & tindakan korektif per site → staging.seed_weekly_log. Mart (days_open, is_open): dbt run --select mart_weekly_log.")

    with st.form("weekly_log_form", clear_on_submit=True):
        selected_label_week = st.selectbox("Site", option_labels, key="site_week", help="Pilih site (site_id & site_name dari dim_site).")
        log_date = st.date_input("Tanggal log", value=date.today(), key="log_date", help="Tanggal kejadian/log.")
        problem_identification = st.text_area("Problem identification", placeholder="Deskripsi masalah (bisa multi-line).", height=100, key="problem")
        corrective_action = st.text_area("Corrective action", placeholder="Tindakan korektif yang dilakukan atau direncanakan.", height=80, key="action")
        status = st.selectbox(
            "Status",
            ["Open", "In Progress", "Resolved", "Closed"],
            key="status",
            help="Open/In Progress = is_open=1 di mart (prioritas). Resolved/Closed = is_open=0.",
        )
        submitted_week = st.form_submit_button("Simpan")

    if submitted_week:
        idx = option_labels.index(selected_label_week)
        site_id_val, _, site_name_val = options[idx]
        site_id_val = site_id_val if (site_id_val is not None and str(site_id_val).strip()) else None
        site_name_val = site_name_val if (site_name_val and str(site_name_val).strip()) else None
        if not site_id_val or not site_name_val:
            st.warning("⚠️ Site tidak valid.")
        else:
            problem_val = (problem_identification or "").strip() or None
            action_val = (corrective_action or "").strip() or None
            try:
                engine = create_engine(DATABASE_URL)
                with engine.connect() as conn:
                    conn.execute(
                        text("""
                            INSERT INTO staging.seed_weekly_log
                            (log_date, site_id, site_name, problem_identification, corrective_action, status)
                            VALUES (:log_date, :site_id, :site_name, :problem_identification, :corrective_action, :status)
                        """),
                        {
                            "log_date": log_date,
                            "site_id": str(site_id_val),
                            "site_name": site_name_val,
                            "problem_identification": problem_val,
                            "corrective_action": action_val,
                            "status": status,
                        },
                    )
                    conn.commit()
                st.success("✅ Record berhasil disimpan ke staging.seed_weekly_log. Jalankan 'dbt run --select mart_weekly_log' lalu cek Evaluasi di bawah.")
            except Exception as e:
                st.error(f"❌ Gagal menyimpan: {e}")

    st.divider()
    st.subheader("Evaluasi / Verifikasi (Weekly Log)")
    st.caption("Cek input di Seed dan output (days_open, is_open) di Mart. Mart: dbt run --select mart_weekly_log.")
    engine = create_engine(DATABASE_URL)
    sub1, sub2 = st.tabs(["Data terakhir di Seed (input)", "Data terakhir di Mart (output)"])
    with sub1:
        df_seed, err_seed = fetch_weekly_seed_recent(engine)
        if err_seed:
            st.error(f"Seed: {err_seed}")
            st.code("Pastikan tabel staging.seed_weekly_log ada (scripts/create_weekly_log_table.sql atau dbt seed).")
        elif df_seed is not None and len(df_seed) == 0:
            st.info("Belum ada data di Seed. Simpan lewat form di atas.")
        else:
            st.dataframe(df_seed, use_container_width=True, hide_index=True)
    with sub2:
        df_mart, err_mart = fetch_weekly_mart_recent(engine)
        if err_mart:
            st.warning(f"Mart: {err_mart}")
            st.info("Jalankan: dbt run --select mart_weekly_log lalu refresh.")
        elif df_mart is not None and len(df_mart) == 0:
            st.info("Belum ada data di Mart. Jalankan dbt run --select mart_weekly_log setelah ada data di Seed.")
        else:
            st.dataframe(df_mart, use_container_width=True, hide_index=True)
    st.markdown("**Alur lengkap:** docs/weekly-log/WEEKLY_LOG_INPUT_OUTPUT_FLOW.md")
