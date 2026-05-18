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
    q = text(
        """
        SELECT ordinal_position, column_name, data_type, is_nullable, column_default
        FROM information_schema.columns
        WHERE table_schema='dimensions' AND table_name='dim_inverter_string_layout'
        ORDER BY ordinal_position
        """
    )
    with engine.connect() as conn:
        for row in conn.execute(q).fetchall():
            print(dict(row._mapping))


if __name__ == "__main__":
    main()
