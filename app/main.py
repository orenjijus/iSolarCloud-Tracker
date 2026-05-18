import streamlit as st
from pathlib import Path

WORKSPACE_ROOT = Path(__file__).resolve().parent.parent
DBT_PROJECT_DIR = WORKSPACE_ROOT / "dbt"

st.set_page_config(
    page_title="MMSR Data Manager",
    page_icon="☀️",
    layout="wide",
    initial_sidebar_state="expanded",
)

st.title("☀️ MMSR Data Manager")
st.markdown("---")

col1, col2, col3 = st.columns(3)
col4, col5, _ = st.columns(3)

with col1:
    st.info("### 🔄 Reingest\nRe-run pipeline dbt untuk tanggal dan site tertentu.")
    if st.button("Buka Reingest", use_container_width=True):
        st.switch_page("pages/1_Reingest.py")

with col2:
    st.info("### 🌱 Seed Manager\nEdit / upload seed CSV lalu `dbt seed`.")
    if st.button("Buka Seed Manager", use_container_width=True):
        st.switch_page("pages/2_Seed_Manager.py")

with col3:
    st.info("### ⚙️ dbt Commands\nRun, test, seed, build, parse.")
    if st.button("Buka dbt Commands", use_container_width=True):
        st.switch_page("pages/3_dbt_Commands.py")

with col4:
    st.info("### 📋 Run History\nRiwayat eksekusi dan log.")
    if st.button("Buka Run History", use_container_width=True):
        st.switch_page("pages/4_Run_History.py")

with col5:
    st.info("### 📖 Metric Catalog\nDefinisi metrik, rumus, verifikasi 1 hari.")
    if st.button("Buka Metric Catalog", use_container_width=True):
        st.switch_page("pages/5_Metric_Catalog.py")

st.markdown("---")
st.caption(f"dbt project: `{DBT_PROJECT_DIR}`")
st.caption("Panduan: `app/docs/USER_GUIDE.md`")
