"""dbt Commands manual."""
from __future__ import annotations

import sys
from pathlib import Path

import streamlit as st

APP_DIR = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(APP_DIR))

from core.dbt_paths import resolve_dbt_project_dir
from core.dbt_runner import parse_dbt_result, stream_dbt
from core.run_log import append_run

DBT = resolve_dbt_project_dir(APP_DIR.parent)
st.set_page_config(page_title="dbt Commands", layout="wide")
st.title("⚙️ dbt Commands")

cmd = st.selectbox("Perintah", ["dbt parse", "dbt run", "dbt seed", "dbt test", "dbt build"])
extra = st.text_input("Argumen tambahan (opsional)", placeholder='mis. --select dim_site')
full = f"{cmd} {extra}".strip()
st.code(full, language="bash")
if st.button("Jalankan", type="primary"):
    lines: list[str] = []
    box = st.empty()
    gen = stream_dbt(full, DBT)
    try:
        while True:
            lines.append(next(gen))
            box.code("\n".join(lines[-80:]), language="text")
    except StopIteration:
        pass
    log = "\n".join(lines)
    r = parse_dbt_result(log)
    append_run(full, r["status"], log, r["duration_s"], r["pass_count"], r["warn_count"], r["error_count"], "dbt Commands")
