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

    site_id = "NE=54435794"

    sql_old_count = text(
        "SELECT COUNT(*) FROM dimensions.dim_inverter_string_layout WHERE site_id = :sid"
    )
    sql_seed_count = text(
        "SELECT COUNT(*) FROM staging.seed_inverter_string_layout WHERE site_id = :sid AND COALESCE(is_active, TRUE)=TRUE"
    )
    sql_refresh = text(
        """
        CREATE TEMP TABLE tmp_dim_old AS
        SELECT *
        FROM dimensions.dim_inverter_string_layout
        WHERE site_id = :sid;

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
            'seed_refresh'::text AS inverter_mapping_status
        FROM staging.seed_inverter_string_layout s
        LEFT JOIN tmp_dim_old o
          ON o.site_id = s.site_id::text
         AND o.inverter_no = s.inverter_no::integer
         AND o.string_no = s.string_no::integer
        WHERE s.site_id = :sid
          AND COALESCE(s.is_active, TRUE) = TRUE;
        """
    )
    sql_new_count = text(
        "SELECT COUNT(*) FROM dimensions.dim_inverter_string_layout WHERE site_id = :sid"
    )

    with engine.begin() as conn:
        old_count = conn.execute(sql_old_count, {"sid": site_id}).scalar()
        seed_count = conn.execute(sql_seed_count, {"sid": site_id}).scalar()
        conn.execute(sql_refresh, {"sid": site_id})
        new_count = conn.execute(sql_new_count, {"sid": site_id}).scalar()

    print({"site_id": site_id, "old_dim_rows": old_count, "seed_active_rows": seed_count, "new_dim_rows": new_count})


if __name__ == "__main__":
    main()
