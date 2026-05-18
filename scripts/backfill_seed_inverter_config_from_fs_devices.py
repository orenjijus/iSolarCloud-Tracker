"""
Merge staging.seed_inverter_config from staging.stg_fusionsolar__devices (inverter list).

For each inverter device: parse inverter_no from dev_name (INV-03, INV-14, ...).
- If row exists for (site_id, inverter_no): UPDATE inverter_id, inverter_sn, site_name
  (keeps existing source and other columns if already set).
- Else: INSERT with source = 'fusionsolar_auto'.

Does not delete existing rows.
"""

import os
import re
import urllib.parse
from pathlib import Path

from dotenv import load_dotenv
from sqlalchemy import create_engine, text

INV_NEW = re.compile(r"^INV-(\d+)-NEW", re.IGNORECASE)


def parse_inv_no(dev_name: str | None) -> int | None:
    if not dev_name:
        return None
    s = dev_name.strip()
    if INV_NEW.match(s):
        return int(INV_NEW.match(s).group(1))
    m = re.match(r"^INV-(\d+)", s, re.IGNORECASE)
    if not m:
        return None
    return int(m.group(1))


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

    q_devices = text(
        """
        SELECT plant_code, dev_id, dev_name
        FROM staging.stg_fusionsolar__devices
        WHERE device_category = 'Inverter'
        ORDER BY plant_code, dev_id
        """
    )

    q_plant_name = text(
        """
        SELECT plant_name::text AS plant_name
        FROM raw.fusionsolar_plants
        WHERE plant_code = :pc
        LIMIT 1
        """
    )

    # NOTE: seed_inverter_config.inverter_name is integer in this DB; do not write dev_name there.
    update_sql = text(
        """
        UPDATE staging.seed_inverter_config
        SET
            inverter_id = :inverter_id,
            inverter_sn = :inverter_sn,
            site_name = COALESCE(:site_name, site_name)
        WHERE site_id = :site_id
          AND inverter_no = :inverter_no
        """
    )

    insert_sql = text(
        """
        INSERT INTO staging.seed_inverter_config (
            source, site_id, site_name, inverter_no, inverter_sn, inverter_id, inverter_name,
            inverter_ac_capacity_kw, inverter_model, inverter_manufacturer, inverter_dc_capacity_kw,
            mppt_count, is_active, valid_from, valid_to, notes
        ) VALUES (
            'fusionsolar_auto', :site_id, :site_name, :inverter_no, :inverter_sn,
            :inverter_id, NULL,
            NULL, NULL, NULL, NULL, NULL, TRUE, NULL, NULL,
            NULL
        )
        """
    )

    with engine.connect() as conn:
        devices = conn.execute(q_devices).fetchall()

    by_plant: dict[str, list] = {}
    for r in devices:
        m = r._mapping
        pc = m["plant_code"]
        if not pc:
            continue
        by_plant.setdefault(pc, []).append(m)

    updated = 0
    inserted = 0
    skipped_parse = 0
    dup_dropped = []

    with engine.begin() as conn:
        for plant_code in sorted(by_plant.keys()):
            devs = by_plant[plant_code]
            site_name_row = conn.execute(q_plant_name, {"pc": plant_code}).fetchone()
            site_name = site_name_row[0] if site_name_row else None

            by_no: dict[int, dict] = {}
            for d in sorted(devs, key=lambda x: x["dev_id"] or ""):
                inv_no = parse_inv_no(d["dev_name"])
                if inv_no is None:
                    skipped_parse += 1
                    continue
                if inv_no in by_no:
                    dup_dropped.append((plant_code, inv_no, d["dev_id"], d["dev_name"]))
                    continue
                by_no[inv_no] = {
                    "site_id": plant_code,
                    "site_name": site_name,
                    "inverter_no": inv_no,
                    "inverter_sn": d["dev_id"],
                    "inverter_id": d["dev_id"],
                    "inverter_name": d["dev_name"],
                }

            for b in sorted(by_no.values(), key=lambda x: x["inverter_no"]):
                res = conn.execute(update_sql, b)
                if res.rowcount and res.rowcount > 0:
                    updated += res.rowcount
                    continue
                conn.execute(insert_sql, b)
                inserted += 1

    print(
        {
            "plants_seen": len(by_plant),
            "rows_updated": updated,
            "rows_inserted": inserted,
            "skipped_unparsed_dev_name": skipped_parse,
            "duplicate_inv_no_skipped": len(dup_dropped),
        }
    )
    if dup_dropped:
        print("sample duplicates:", dup_dropped[:20])


if __name__ == "__main__":
    main()
