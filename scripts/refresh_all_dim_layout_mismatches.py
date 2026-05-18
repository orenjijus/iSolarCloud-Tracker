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

    q_mismatch = text(
        """
        WITH seed AS (
            SELECT
                site_id::text AS site_id,
                COUNT(*) FILTER (WHERE COALESCE(is_active, TRUE)=TRUE) AS seed_active_rows,
                COUNT(DISTINCT COALESCE(NULLIF(TRIM(inverter_id::text), ''), NULLIF(TRIM(inverter_no::text), '')))
                    FILTER (WHERE COALESCE(is_active, TRUE)=TRUE) AS seed_inverter_count
            FROM staging.seed_inverter_string_layout
            GROUP BY 1
        ),
        dim AS (
            SELECT
                site_id::text AS site_id,
                COUNT(*) FILTER (WHERE COALESCE(is_active, TRUE)=TRUE) AS dim_active_rows,
                COUNT(DISTINCT NULLIF(TRIM(inverter_id::text), ''))
                    FILTER (WHERE COALESCE(is_active, TRUE)=TRUE) AS dim_inverter_count
            FROM dimensions.dim_inverter_string_layout
            GROUP BY 1
        )
        SELECT
            COALESCE(s.site_id, d.site_id) AS site_id
        FROM seed s
        FULL OUTER JOIN dim d ON d.site_id = s.site_id
        WHERE COALESCE(s.seed_active_rows, 0) <> COALESCE(d.dim_active_rows, 0)
           OR COALESCE(s.seed_inverter_count, 0) <> COALESCE(d.dim_inverter_count, 0)
        ORDER BY 1
        """
    )

    q_refresh_site = text(
        """
        DROP TABLE IF EXISTS tmp_dim_old;
        DROP TABLE IF EXISTS tmp_dim_old_dedup;

        CREATE TEMP TABLE tmp_dim_old AS
        SELECT *
        FROM dimensions.dim_inverter_string_layout
        WHERE site_id = :sid;

        CREATE TEMP TABLE tmp_dim_old_dedup AS
        SELECT DISTINCT ON (site_id, inverter_no, string_no)
            *
        FROM tmp_dim_old
        ORDER BY site_id, inverter_no, string_no, candidate_count DESC NULLS LAST;

        DELETE FROM dimensions.dim_inverter_string_layout
        WHERE site_id = :sid;

        INSERT INTO dimensions.dim_inverter_string_layout
        (
            source, site_id, site_name, inverter_id, inverter_name, inverter_sn,
            inverter_no, mppt_no, string_no, module_qty, orient_code,
            inverter_ac_capacity_kw, inverter_model, inverter_manufacturer,
            inverter_dc_capacity_kw, mppt_count, pv_module_model, pv_module_manufacturer,
            pv_module_p_nom_wp_master, pv_module_model_year, inverter_ac_capacity_max_kw_master,
            inverter_input_count_master, is_active, commission_date, decommission_date, notes,
            candidate_count, inverter_mapping_status
        )
        SELECT
            s.source::text,
            s.site_id::text,
            s.site_name::text,
            COALESCE(NULLIF(TRIM(s.inverter_id::text), ''), NULLIF(TRIM(s.inverter_no::text), '')) AS inverter_id,
            s.inverter_name::text,
            o.inverter_sn::text,
            s.inverter_no::integer,
            s.mppt_no::integer,
            s.string_no::integer,
            s.module_qty::integer,
            s.orient_code::integer,
            o.inverter_ac_capacity_kw,
            o.inverter_model::text,
            o.inverter_manufacturer::text,
            o.inverter_dc_capacity_kw,
            o.mppt_count,
            o.pv_module_model::text,
            o.pv_module_manufacturer::text,
            o.pv_module_p_nom_wp_master,
            o.pv_module_model_year,
            o.inverter_ac_capacity_max_kw_master,
            o.inverter_input_count_master,
            COALESCE(s.is_active, TRUE) AS is_active,
            s.commission_date::integer,
            s.decommission_date::integer,
            s.notes::text,
            1::bigint AS candidate_count,
            'seed_refresh_batch'::text AS inverter_mapping_status
        FROM staging.seed_inverter_string_layout s
        LEFT JOIN tmp_dim_old_dedup o
          ON o.site_id = s.site_id::text
         AND o.inverter_no = s.inverter_no::integer
         AND o.string_no = s.string_no::integer
        WHERE s.site_id = :sid
          AND COALESCE(s.is_active, TRUE) = TRUE;
        """
    )

    q_counts = text(
        """
        WITH s AS (
          SELECT COUNT(*) AS seed_rows,
                 COUNT(DISTINCT COALESCE(NULLIF(TRIM(inverter_id::text), ''), NULLIF(TRIM(inverter_no::text), ''))) AS seed_inv
          FROM staging.seed_inverter_string_layout
          WHERE site_id = :sid AND COALESCE(is_active, TRUE)=TRUE
        ),
        d AS (
          SELECT COUNT(*) AS dim_rows,
                 COUNT(DISTINCT NULLIF(TRIM(inverter_id::text), '')) AS dim_inv
          FROM dimensions.dim_inverter_string_layout
          WHERE site_id = :sid AND COALESCE(is_active, TRUE)=TRUE
        )
        SELECT s.seed_rows, s.seed_inv, d.dim_rows, d.dim_inv FROM s CROSS JOIN d
        """
    )

    with engine.connect() as conn:
        sites = [r[0] for r in conn.execute(q_mismatch).fetchall()]

    print(f"mismatch_sites: {len(sites)}")
    for sid in sites:
        with engine.begin() as conn:
            conn.execute(q_refresh_site, {"sid": sid})
            counts = conn.execute(q_counts, {"sid": sid}).fetchone()._mapping
        print({"site_id": sid, **counts})


if __name__ == "__main__":
    main()
