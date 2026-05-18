"""
Launcher: menjalankan form yang sama dengan cleaning_log_form.py (satu aplikasi, dua tab).
Agar "streamlit run tools/weekly_log_form.py" tetap buka form yang sama (Cleaning Log + Weekly Log).
"""
import os

if __name__ == "__main__":
    _dir = os.path.dirname(os.path.abspath(__file__))
    _path = os.path.join(_dir, "cleaning_log_form.py")
    with open(_path, "r", encoding="utf-8") as f:
        exec(compile(f.read(), _path, "exec"), {"__file__": _path})
