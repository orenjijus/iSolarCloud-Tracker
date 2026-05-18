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

    tables = [
        "seed_inverter_config",
        "seed_inverter_model_master",
        "seed_string_module_override",
    ]
    q = text(
        "select column_name from information_schema.columns "
        "where table_schema='staging' and table_name=:t order by ordinal_position"
    )

    with engine.connect() as conn:
        for t in tables:
            cols = [r[0] for r in conn.execute(q, {"t": t}).fetchall()]
            print(t, cols)


if __name__ == "__main__":
    main()
