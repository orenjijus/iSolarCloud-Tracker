"""
Rebuild dimensions.dim_inverter_string_layout for one or more sites from
staging.seed_inverter_string_layout (active rows only).

Preserves inverter_sn, inverter specs, and PV module master fields from the
previous dim row when keys match on (site_id, inverter_no, mppt_no, string_no).

Typical flow after Google Sheet → CSV → dbt seed:
  python scripts/refresh_dim_inverter_string_layout_from_seed.py --all-gsheet-sites
"""

from __future__ import annotations

import argparse
import os
import urllib.parse
from pathlib import Path

from dotenv import load_dotenv
from sqlalchemy import create_engine, text

ROOT = Path(__file__).resolve().parents[1]


def _load_site_map() -> dict[str, tuple[str, str]]:
    import importlib.util

    path = ROOT / "scripts" / "sync_inverter_string_layout_from_gsheet.py"
    spec = importlib.util.spec_from_file_location("sync_inv_gsheet", path)
    mod = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(mod)
    return mod.SITE_MAP

REFRESH_SQL = text(
    """
    DROP TABLE IF EXISTS tmp_dim_old;
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
     AND o.mppt_no = s.mppt_no::integer
     AND o.string_no = s.string_no::integer
    WHERE s.site_id = :sid
      AND COALESCE(s.is_active, TRUE) = TRUE;
    """
)


def refresh_site(conn, site_id: str) -> dict:
    old = conn.execute(
        text("SELECT COUNT(*) FROM dimensions.dim_inverter_string_layout WHERE site_id = :sid"),
        {"sid": site_id},
    ).scalar()
    seed = conn.execute(
        text(
            "SELECT COUNT(*) FROM staging.seed_inverter_string_layout "
            "WHERE site_id = :sid AND COALESCE(is_active, TRUE)=TRUE"
        ),
        {"sid": site_id},
    ).scalar()
    conn.execute(REFRESH_SQL, {"sid": site_id})
    new = conn.execute(
        text("SELECT COUNT(*) FROM dimensions.dim_inverter_string_layout WHERE site_id = :sid"),
        {"sid": site_id},
    ).scalar()
    return {"site_id": site_id, "old_dim_rows": old, "seed_active_rows": seed, "new_dim_rows": new}


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument(
        "--site-id",
        action="append",
        dest="site_ids",
        help="site_id (repeatable), e.g. 1458125 or NE=54435794",
    )
    p.add_argument(
        "--all-gsheet-sites",
        action="store_true",
        help="refresh every site_id listed in SITE_MAP (sync_inverter_string_layout_from_gsheet.py)",
    )
    args = p.parse_args()

    if args.all_gsheet_sites:
        site_map = _load_site_map()
        site_ids = sorted({t[0] for t in site_map.values()})
    elif args.site_ids:
        site_ids = args.site_ids
    else:
        p.error("Provide --site-id (repeat) or --all-gsheet-sites")

    load_dotenv(ROOT / ".env")
    load_dotenv(ROOT / "tools" / ".env")
    engine = create_engine(
        "postgresql://{user}:{pwd}@{host}:{port}/{db}".format(
            user=os.getenv("POSTGRES_USER", "juice"),
            pwd=urllib.parse.quote_plus(os.getenv("POSTGRES_PASSWORD", "")),
            host=os.getenv("POSTGRES_HOST", "10.101.4.88"),
            port=os.getenv("POSTGRES_PORT", "5432"),
            db=os.getenv("POSTGRES_DB", "MMSR"),
        )
    )

    results = []
    with engine.begin() as conn:
        for sid in site_ids:
            seed_n = conn.execute(
                text(
                    "SELECT COUNT(*) FROM staging.seed_inverter_string_layout "
                    "WHERE site_id = :sid AND COALESCE(is_active, TRUE)=TRUE"
                ),
                {"sid": sid},
            ).scalar()
            if seed_n == 0:
                results.append({"site_id": sid, "skipped": True, "reason": "no active seed rows"})
                continue
            results.append(refresh_site(conn, sid))

    for r in results:
        print(r)


if __name__ == "__main__":
    main()
