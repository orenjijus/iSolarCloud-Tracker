import os
import urllib.parse
from pathlib import Path

from dotenv import load_dotenv
from sqlalchemy import create_engine, text

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
with engine.connect() as c:
    for r in c.execute(
        text(
            """
            SELECT column_name, data_type
            FROM information_schema.columns
            WHERE table_schema='staging' AND table_name='seed_inverter_config'
            ORDER BY ordinal_position
            """
        )
    ).fetchall():
        print(r[0], r[1])
