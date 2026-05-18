import os
import urllib.parse
from pathlib import Path

from dotenv import load_dotenv
from sqlalchemy import create_engine, text


def main() -> None:
    root = Path(r"C:\Users\Administrator\Documents\Code\MMSR API - Server MA")
    load_dotenv(root / ".env")
    load_dotenv(root / "tools" / ".env")

    host = os.getenv("POSTGRES_HOST", "10.101.4.88")
    port = os.getenv("POSTGRES_PORT", "5432")
    db = os.getenv("POSTGRES_DB", "MMSR")
    user = os.getenv("POSTGRES_USER", "juice")
    pwd = urllib.parse.quote_plus(os.getenv("POSTGRES_PASSWORD", ""))

    engine = create_engine(f"postgresql://{user}:{pwd}@{host}:{port}/{db}")

    sql = """
    SELECT pid, state, wait_event_type, wait_event, now() - query_start AS running_for, left(query, 250) AS query
    FROM pg_stat_activity
    WHERE usename = current_user
      AND state <> 'idle'
    ORDER BY query_start;
    """

    with engine.connect() as conn:
        for row in conn.execute(text(sql)).fetchall():
            print(dict(row._mapping))


if __name__ == "__main__":
    main()
