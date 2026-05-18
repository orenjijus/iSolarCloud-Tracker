import os
import urllib.parse
from pathlib import Path

from dotenv import load_dotenv
from sqlalchemy import create_engine, text


def main() -> None:
    root = Path(r"C:\Users\Administrator\Documents\Code\MMSR API - Server MA")
    load_dotenv(root / ".env")
    load_dotenv(root / "tools" / ".env")
    engine = create_engine(
        "postgresql://{user}:{pwd}@{host}:{port}/{db}".format(
            user=os.getenv("POSTGRES_USER", "juice"),
            pwd=urllib.parse.quote_plus(os.getenv("POSTGRES_PASSWORD", "")),
            host=os.getenv("POSTGRES_HOST", "10.101.4.88"),
            port=os.getenv("POSTGRES_PORT", "5432"),
            db=os.getenv("POSTGRES_DB", "MMSR"),
        )
    )

    days = 45
    q_before = text(
        "SELECT COUNT(*) AS c FROM mart.mart_string_performance_daily WHERE date_key >= CURRENT_DATE - (:d || ' day')::interval"
    )
    q_delete = text(
        "DELETE FROM mart.mart_string_performance_daily WHERE date_key >= CURRENT_DATE - (:d || ' day')::interval"
    )
    q_after = text(
        "SELECT COUNT(*) AS c FROM mart.mart_string_performance_daily WHERE date_key >= CURRENT_DATE - (:d || ' day')::interval"
    )

    with engine.begin() as conn:
        before = conn.execute(q_before, {"d": days}).scalar()
        deleted = conn.execute(q_delete, {"d": days}).rowcount
        after = conn.execute(q_after, {"d": days}).scalar()

    print({"days_window": days, "rows_before": before, "rows_deleted": deleted, "rows_after": after})


if __name__ == "__main__":
    main()
