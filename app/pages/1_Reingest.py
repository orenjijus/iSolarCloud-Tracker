"""
Reingest — alur sederhana dari seed (tanpa dbt show / dim_site).

Daftar site + ID dari seed_inverter_config.csv (sama seperti pipeline Anda yang lengkap).
"""
from __future__ import annotations

import sys
from datetime import date, timedelta
from pathlib import Path

import pandas as pd
import streamlit as st

APP_DIR = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(APP_DIR))

from core.dbt_paths import assert_dbt_project_file, resolve_dbt_project_dir
from core.dbt_runner import build_reingest_command, parse_dbt_result, stream_dbt
from core.run_log import append_run
from core.site_fallback import load_sites_from_seed_inverter

WORKSPACE_ROOT = APP_DIR.parent
DBT_PROJECT_DIR = resolve_dbt_project_dir(WORKSPACE_ROOT)
SEEDS_DIR = DBT_PROJECT_DIR / "seeds"

st.set_page_config(page_title="Reingest — MMSR", page_icon="🔄", layout="wide")
st.title("🔄 Reingest Data")
st.caption(
    "Pilih platform → pilih site (ID dari **seed_inverter_config.csv**) → tanggal → **Jalankan reingest**. "
    "Model: `stg_*__perf_unpivoted+` (staging + downstream)."
)
with st.expander("Folder project dbt (untuk `dbt run`)", expanded=False):
    st.code(str(DBT_PROJECT_DIR), language="text")
    st.caption("Override: environment variable `MMSR_DBT_PROJECT_DIR` jika `dbt_project.yml` tidak di `.../dbt/`.")
st.markdown("---")


@st.cache_data(ttl=60, show_spinner="Memuat seed inverter…")
def _sites_cached(seeds_dir_str: str) -> pd.DataFrame:
    return load_sites_from_seed_inverter(Path(seeds_dir_str))


col_form, col_info = st.columns([2, 1])

with col_form:
    st.subheader("Parameter")

    try:
        site_df = _sites_cached(str(SEEDS_DIR))
    except Exception as exc:
        st.error(f"Tidak bisa baca `dbt/seeds/seed_inverter_config.csv`: {exc}")
        st.stop()

    if site_df.empty:
        st.warning("Tidak ada baris di seed_inverter_config.")
        st.stop()

    platform = st.radio(
        "Platform",
        options=["isolarcloud", "fusionsolar"],
        format_func=lambda x: "iSolarCloud" if x == "isolarcloud" else "FusionSolar",
        horizontal=True,
    )
    want = "isolarcloud" if platform == "isolarcloud" else "fusionsolar"
    choices = site_df[site_df["system_norm"] == want].copy()
    if choices.empty:
        st.warning(f"Tidak ada site untuk {want} di seed_inverter_config.")
        st.stop()

    label_to_id = dict(zip(choices["option_label"], choices["site_id"]))
    selected = st.multiselect(
        "Pilih site",
        options=sorted(label_to_id.keys(), key=str.lower),
        help="Sumber: `dbt/seeds/seed_inverter_config.csv` (kolom source, site_id, site_name).",
    )
    auto_ids = [label_to_id[l] for l in selected if label_to_id.get(l)]

    with st.expander("Tambahan ID manual (opsional)"):
        manual_ids = st.text_input(
            "ID tambahan (pisahkan koma)",
            placeholder="Contoh: 1763661 atau NE=50488260,NE=51758766",
        )

    def _merge(a: list[str], m: str) -> str:
        xs = list(a) + [x.strip() for x in m.split(",") if x.strip()]
        seen: dict[str, None] = {}
        for x in xs:
            seen.setdefault(x, None)
        return ",".join(seen.keys())

    merged = _merge(auto_ids, manual_ids)
    st.markdown("**ID terpilih:**")
    st.code(merged or "_(kosong)_", language="text")

    c1, c2 = st.columns(2)
    de = date.today() - timedelta(days=1)
    ds = de - timedelta(days=2)
    start_date = c1.date_input("Tanggal mulai", value=ds)
    end_date = c2.date_input("Tanggal selesai", value=de)
    if start_date > end_date:
        st.error("Tanggal mulai tidak boleh lebih besar dari tanggal selesai.")

    sel = (
        "stg_isolarcloud__perf_unpivoted+"
        if platform == "isolarcloud"
        else "stg_fusionsolar__perf_unpivoted+"
    )
    st.markdown("**Model (tetap):**")
    st.code(sel, language="text")

    project_ok = True
    try:
        assert_dbt_project_file(DBT_PROJECT_DIR)
    except FileNotFoundError as e:
        project_ok = False
        st.error(str(e))

    can_run = bool(merged.strip()) and start_date <= end_date and project_ok

    if can_run:
        st.code(
            build_reingest_command(platform, str(start_date), str(end_date), merged.strip()),
            language="bash",
        )

    run_btn = st.button(
        "▶ Jalankan reingest",
        type="primary",
        use_container_width=True,
        disabled=not can_run,
    )

with col_info:
    st.subheader("Panduan")
    st.markdown(
        """
**Variabel dbt**
- iSolarCloud → `reingest_ps_ids`
- FusionSolar → `reingest_plant_codes`

**Seed**
- Daftar site diambil dari **`seed_inverter_config.csv`** (bukan `dbt show`).
- Update seed di tab **Seed Manager** jika site/ID berubah.

**dbt**
- Perlu **`dbt_project.yml`** + **`profiles.yml`** + model SQL di folder `models/` agar `dbt run` berhasil.
- Tanpa model staging yang lengkap, `dbt run` akan gagal — itu masalah isi project dbt, bukan halaman ini.

**Kenapa tidak pakai dim_site lagi**
- `dbt show dim_site` hanya jalan jika model **`dim_site`** ada di project dan sudah pernah di-build. Banyak checkout minimal tidak punya folder `models/` lengkap.
- Alur seed ini sama seperti reingest manual via CLI yang Anda pakai sebelumnya.
"""
    )

if run_btn:
    try:
        assert_dbt_project_file(DBT_PROJECT_DIR)
    except FileNotFoundError as e:
        st.error(str(e))
        st.stop()

    cmd = build_reingest_command(platform, str(start_date), str(end_date), merged.strip())
    st.markdown("---")
    st.subheader("Output")
    st.code(cmd, language="bash")
    lines: list[str] = []
    box = st.empty()
    gen = stream_dbt(cmd, DBT_PROJECT_DIR)
    try:
        while True:
            lines.append(next(gen))
            box.code("\n".join(lines[-80:]), language="text")
    except StopIteration:
        pass
    log = "\n".join(lines)
    res = parse_dbt_result(log)
    append_run(
        cmd,
        res["status"],
        log,
        res["duration_s"],
        res["pass_count"],
        res["warn_count"],
        res["error_count"],
        "Reingest",
    )
    if res["status"] == "success":
        st.success(f"Selesai — PASS={res['pass_count']} WARN={res['warn_count']} ERR={res['error_count']}")
    else:
        st.error(f"Gagal — ERR={res['error_count']}")
