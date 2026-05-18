import csv
import os
import urllib.parse
from pathlib import Path

from dotenv import load_dotenv
from sqlalchemy import create_engine, text


def main() -> None:
    root = Path(r"C:\Users\Administrator\Documents\Code\MMSR API - Server MA")
    out_csv = root / "docs" / "audit-tables" / "20_seed_vs_dim_inverter_counts.csv"
    out_csv.parent.mkdir(parents=True, exist_ok=True)

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

    sql = text(
        """
        WITH seed AS (
            SELECT
                site_id::text AS site_id,
                MAX(site_name)::text AS site_name,
                COUNT(*) FILTER (WHERE COALESCE(is_active, TRUE)=TRUE) AS seed_active_rows,
                COUNT(DISTINCT COALESCE(NULLIF(TRIM(inverter_id::text), ''), NULLIF(TRIM(inverter_no::text), '')))
                    FILTER (WHERE COALESCE(is_active, TRUE)=TRUE) AS seed_inverter_count
            FROM staging.seed_inverter_string_layout
            GROUP BY 1
        ),
        dim AS (
            SELECT
                site_id::text AS site_id,
                MAX(site_name)::text AS site_name,
                COUNT(*) FILTER (WHERE COALESCE(is_active, TRUE)=TRUE) AS dim_active_rows,
                COUNT(DISTINCT NULLIF(TRIM(inverter_id::text), ''))
                    FILTER (WHERE COALESCE(is_active, TRUE)=TRUE) AS dim_inverter_count
            FROM dimensions.dim_inverter_string_layout
            GROUP BY 1
        )
        SELECT
            COALESCE(s.site_id, d.site_id) AS site_id,
            COALESCE(s.site_name, d.site_name) AS site_name,
            COALESCE(s.seed_active_rows, 0) AS seed_active_rows,
            COALESCE(d.dim_active_rows, 0) AS dim_active_rows,
            COALESCE(s.seed_inverter_count, 0) AS seed_inverter_count,
            COALESCE(d.dim_inverter_count, 0) AS dim_inverter_count,
            COALESCE(d.dim_active_rows, 0) - COALESCE(s.seed_active_rows, 0) AS row_diff_dim_minus_seed,
            COALESCE(d.dim_inverter_count, 0) - COALESCE(s.seed_inverter_count, 0) AS inverter_diff_dim_minus_seed
        FROM seed s
        FULL OUTER JOIN dim d
          ON d.site_id = s.site_id
        ORDER BY site_id;
        """
    )

    with engine.connect() as conn:
        rows = conn.execute(sql).fetchall()

    if rows:
        with out_csv.open("w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=list(rows[0]._mapping.keys()))
            writer.writeheader()
            for row in rows:
                writer.writerow(dict(row._mapping))

    print(f"Wrote: {out_csv}")
    print(f"Rows: {len(rows)}")
    for row in rows:
        m = row._mapping
        if m["row_diff_dim_minus_seed"] != 0 or m["inverter_diff_dim_minus_seed"] != 0:
            print(dict(m))


if __name__ == "__main__":
    main()
