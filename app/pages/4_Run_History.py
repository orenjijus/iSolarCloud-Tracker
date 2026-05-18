"""Run history."""
from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd
import streamlit as st

APP_DIR = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(APP_DIR))
from core.run_log import get_history, get_run

st.set_page_config(page_title="Run History", layout="wide")
st.title("📋 Run History")
rows = get_history(200)
if not rows:
    st.info("Belum ada riwayat.")
    st.stop()
df = pd.DataFrame(
    [
        {
            "Waktu": r.get("timestamp", ""),
            "Status": r.get("status"),
            "Sumber": r.get("source_page"),
            "Command": (r.get("command") or "")[:100],
            "id": r.get("id"),
        }
        for r in rows
    ]
)
st.dataframe(df, hide_index=True, use_container_width=True)
rid = st.selectbox("Detail", [r["id"] for r in rows], format_func=lambda i: next((x.get("command", "")[:60] for x in rows if x.get("id") == i), i))
rec = get_run(rid)
if rec:
    st.code(rec.get("log", ""), language="text")
