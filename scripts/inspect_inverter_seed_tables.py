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
    q_tables = text(
        """
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema='staging' AND table_name LIKE 'seed_inverter%'
        ORDER BY table_name
        """
    )
    q_cols = text(
        """
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema='staging' AND table_name=:t
        ORDER BY ordinal_position
        """
    )
    with engine.connect() as conn:
        tables = [r[0] for r in conn.execute(q_tables).fetchall()]
        print("tables", tables)
        for t in tables:
            cols = [r[0] for r in conn.execute(q_cols, {"t": t}).fetchall()]
            print(t, cols)


if __name__ == "__main__":
    main()
