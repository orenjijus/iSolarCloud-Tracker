"""Seed Manager — edit / upload seeds."""
from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd
import streamlit as st

APP_DIR = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(APP_DIR))

from config.seed_config import SEED_GROUPS, SEEDS
from core.dbt_paths import resolve_dbt_project_dir
from core.dbt_runner import parse_dbt_result, stream_dbt
from core.run_log import append_run
from core.seed_io import parse_upload, read_seed, validate_seed, write_seed

WORKSPACE_ROOT = APP_DIR.parent
DBT_PROJECT_DIR = resolve_dbt_project_dir(WORKSPACE_ROOT)
SEEDS_DIR = DBT_PROJECT_DIR / "seeds"

st.set_page_config(page_title="Seed Manager — MMSR", page_icon="🌱", layout="wide")
st.title("🌱 Seed Manager")
st.caption("Edit atau upload CSV seed → `dbt seed`.")
st.markdown("---")


def _run_seed_tab(seed_name: str, dbt_dir: Path) -> None:
    fr = st.checkbox("--full-refresh", key=f"fr_{seed_name}")
    flags = "--full-refresh" if fr else ""
    cmd = f"dbt seed --select {seed_name} {flags}".strip()
    if st.button("▶ Jalankan dbt seed", type="primary", key=f"runSeed_{seed_name}"):
        st.code(cmd, language="bash")
        lines: list[str] = []
        box = st.empty()
        gen = stream_dbt(cmd, dbt_dir)
        try:
            while True:
                lines.append(next(gen))
                box.code("\n".join(lines[-60:]), language="text")
        except StopIteration as e:
            pass
        log = "\n".join(lines)
        r = parse_dbt_result(log)
        append_run(cmd, r["status"], log, r["duration_s"], r["pass_count"], r["warn_count"], r["error_count"], "Seed Manager")
        st.success("Selesai") if r["status"] == "success" else st.error("Gagal")


with st.sidebar:
    st.header("Pilih seed")
    grp = st.selectbox("Grup", list(SEED_GROUPS.keys()))
    seeds_in = SEED_GROUPS[grp]
    labels = [SEEDS[s]["display"] for s in seeds_in if s in SEEDS]
    lab = st.radio("Seed", labels)
    seed_name = seeds_in[labels.index(lab)]
    meta = SEEDS[seed_name]
    st.caption(meta["description"])
    st.caption(f"Mode: {meta['ui_type']}")

meta = SEEDS[seed_name]
delimiter = meta["delimiter"]
required_cols = meta["required_columns"]
ui_type = meta["ui_type"]

st.subheader(meta["display"])

if ui_type == "readonly":
    try:
        st.dataframe(read_seed(seed_name, SEEDS_DIR, delimiter), use_container_width=True)
    except FileNotFoundError:
        st.warning("File CSV tidak ada.")

elif ui_type == "form":
    tab_e, tab_s = st.tabs(["Edit", "Seed ke DB"])
    with tab_e:
        try:
            cur = read_seed(seed_name, SEEDS_DIR, delimiter)
        except FileNotFoundError:
            st.error("File tidak ada.")
            st.stop()
        sk = f"df_{seed_name}"
        if sk not in st.session_state:
            st.session_state[sk] = cur.copy()
        if st.button("Reload", key=f"rl_{seed_name}"):
            st.session_state[sk] = read_seed(seed_name, SEEDS_DIR, delimiter)
        ed = st.data_editor(st.session_state[sk], num_rows="dynamic", key=f"ed_{seed_name}")
        st.session_state[sk] = ed
        if st.button("Simpan CSV", type="primary", key=f"sv_{seed_name}"):
            err = validate_seed(ed, required_cols)
            if err:
                for e in err:
                    st.error(e)
            else:
                write_seed(seed_name, ed, SEEDS_DIR, delimiter)
                st.success("Tersimpan")
    with tab_s:
        _run_seed_tab(seed_name, DBT_PROJECT_DIR)

elif ui_type == "upload":
    t1, t2, t3 = st.tabs(["Data saat ini", "Upload", "Seed ke DB"])
    with t1:
        try:
            dc = read_seed(seed_name, SEEDS_DIR, delimiter)
            st.dataframe(dc, use_container_width=True)
            st.download_button(
                "Download CSV",
                dc.to_csv(sep=delimiter, index=False).encode("utf-8"),
                f"{seed_name}.csv",
                key=f"dl_{seed_name}",
            )
        except FileNotFoundError:
            st.warning("Belum ada file.")
    with t2:
        st.caption("CSV koma atau `;` — auto-detect. Header harus cocok seed (lihat tab Data saat ini).")
        if required_cols:
            st.info(f"Minimal: `{', '.join(required_cols)}`")
        hints = meta.get("column_hints") or {}
        if hints:
            with st.expander("Template kolom"):
                for hk, hv in hints.items():
                    st.markdown(hv if hk.startswith("_") else f"- **{hk}**: {hv}")
        up = st.file_uploader("File", type=["csv", "xlsx", "xls"], key=f"up_{seed_name}")
        if up:
            df_new, errs = parse_upload(up, required_cols, delimiter)
            if errs:
                for e in errs:
                    st.error(e)
            else:
                st.success(f"Valid: {len(df_new)} baris")
                schema_ok = True
                try:
                    ref = read_seed(seed_name, SEEDS_DIR, delimiter)
                    miss = [c for c in ref.columns if c not in df_new.columns]
                    extra = [c for c in df_new.columns if c not in ref.columns]
                    if miss:
                        schema_ok = False
                        st.error("Kolom hilang vs seed: " + ", ".join(f"`{c}`" for c in miss))
                    if extra:
                        st.warning("Kolom ekstra: " + ", ".join(f"`{c}`" for c in extra))
                except FileNotFoundError:
                    pass
                st.dataframe(df_new.head(10))
                kw = {"label": "Konfirmasi & Simpan", "type": "primary", "key": f"cf_{seed_name}", "disabled": not schema_ok}
                if not schema_ok:
                    kw["help"] = "Perbaiki header dulu"
                if st.button(**kw):
                    write_seed(seed_name, df_new, SEEDS_DIR, delimiter)
                    st.success("Tersimpan")
    with t3:
        _run_seed_tab(seed_name, DBT_PROJECT_DIR)
