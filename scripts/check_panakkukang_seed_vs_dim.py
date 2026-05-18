import os
import urllib.parse
from pathlib import Path

from dotenv import load_dotenv
from sqlalchemy import create_engine, text


root = Path(r"C:\Users\Administrator\Documents\Code\MMSR API - Server MA")
load_dotenv(root / ".env")
load_dotenv(root / "tools" / ".env")

host = os.getenv("POSTGRES_HOST", "10.101.4.88")
port = os.getenv("POSTGRES_PORT", "5432")
db = os.getenv("POSTGRES_DB", "MMSR")
user = os.getenv("POSTGRES_USER", "juice")
pwd = urllib.parse.quote_plus(os.getenv("POSTGRES_PASSWORD", ""))
engine = create_engine(f"postgresql://{user}:{pwd}@{host}:{port}/{db}")

queries = {
    "seed_ne": "select count(*) as c from staging.seed_inverter_string_layout where site_id='NE=53771627'",
    "dim_ne": "select count(*) as c from dimensions.dim_inverter_string_layout where site_id='NE=53771627'",
    "dim_fs": "select count(*) as c from dimensions.dim_inverter_string_layout where site_id='FS_SITE_NE=53771627'",
}

with engine.connect() as conn:
    for key, q in queries.items():
        print(key, conn.execute(text(q)).scalar())
