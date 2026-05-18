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

    q1 = """
    SELECT
      site_id,
      COUNT(*) AS rows_cnt,
      COUNT(*) FILTER (WHERE COALESCE(is_active, TRUE) = TRUE) AS active_rows
    FROM dimensions.dim_inverter_string_layout
    WHERE site_id IN ('NE=53771627', 'FS_SITE_NE=53771627', '53771627')
    GROUP BY site_id
    ORDER BY site_id;
    """

    q2 = """
    SELECT
      site_id,
      inverter_id,
      string_no,
      module_qty,
      pv_module_p_nom_wp_master,
      is_active
    FROM dimensions.dim_inverter_string_layout
    WHERE site_id ILIKE '%53771627%'
    ORDER BY site_id, inverter_id, string_no
    LIMIT 50;
    """

    q3 = """
    SELECT
      site_id,
      inverter_id,
      string_no,
      module_qty,
      pv_module_p_nom_wp_master,
      is_active
    FROM dimensions.dim_inverter_string_layout
    WHERE site_name ILIKE '%panakkukang%'
    ORDER BY site_id, inverter_id, string_no
    LIMIT 50;
    """

    q4 = """
    SELECT DISTINCT site_name
    FROM dimensions.dim_inverter_string_layout
    WHERE site_name ILIKE '%pana%'
       OR site_name ILIKE '%panak%'
       OR site_name ILIKE '%makassar%'
    ORDER BY site_name;
    """

    q5 = """
    SELECT asset_id, site_name
    FROM dimensions.dim_assets
    WHERE asset_level = 'Site'
      AND (
        site_name ILIKE '%panakkukang%'
        OR site_name ILIKE '%pana%'
      )
    ORDER BY site_name;
    """

    with engine.connect() as conn:
        r1 = conn.execute(text(q1)).fetchall()
        r2 = conn.execute(text(q2)).fetchall()
        r3 = conn.execute(text(q3)).fetchall()
        r4 = conn.execute(text(q4)).fetchall()
        r5 = conn.execute(text(q5)).fetchall()

    print("=== site_id exact variants ===")
    for row in r1:
        print(dict(row._mapping))
    print("=== site_id like %53771627% ===")
    for row in r2:
        print(dict(row._mapping))
    print("=== site_name like %panakkukang% ===")
    for row in r3:
        print(dict(row._mapping))
    print("=== candidate site_name in layout (pana/panak/makassar) ===")
    for row in r4:
        print(dict(row._mapping))
    print("=== dim_assets site candidates ===")
    for row in r5:
        print(dict(row._mapping))


if __name__ == "__main__":
    main()
