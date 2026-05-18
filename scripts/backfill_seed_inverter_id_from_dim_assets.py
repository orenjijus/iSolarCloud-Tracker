"""
Fill staging.seed_inverter_config.inverter_id from dimensions.dim_assets, using telemetry keys
that actually appear in mart_string_performance_daily.

Join key in mart_string_performance_daily matches:
  REGEXP_REPLACE(telemetry_inverter_id, '^(FS_|ISO_)', '')
so we store inverter_id WITHOUT the FS_/ISO_ prefix (same as device_ps_key in iSolar staging).

mart_string_performance_daily layout_dedup uses
  COALESCE(seed_inverter_string_layout.inverter_id, seed_inverter_config.inverter_id)
as layout_inverter_key. In most iSolar seeds the layout column is empty, so filling
seed_inverter_config is sufficient. Note: seed_inverter_string_layout.inverter_id is often
INTEGER in Postgres and cannot hold full ps_key strings; do not use --write-string-layout-inverter-id
unless that column is text.

inverter_no is inferred from dim_assets.asset_name:
  - FusionSolar: INV-(\\d+)  -> inverter_no = that number
  - iSolar-style: first 3-digit number in 101..199 -> inverter_no = n - 100
    (covers "Inverter 101", "Inverter.102", etc.)

Does not overwrite non-blank inverter_id unless --force.

After running, mirror to CSV seeds if needed, then refresh dim layout + mart string window.
"""

from __future__ import annotations

import argparse
import csv
import os
import re
import urllib.parse
from pathlib import Path

from dotenv import load_dotenv
from sqlalchemy import create_engine, text

ROOT = Path(__file__).resolve().parents[1]

INV_NUM_DASH = re.compile(r"INV-(\d+)", re.IGNORECASE)
INV_NUM_LOOSE = re.compile(r"\bINV[\s._-]?(\d{1,3})\b", re.IGNORECASE)
INVERTER_NUM = re.compile(r"\bINVERTER[\s._-]*([0-9]{1,3})\b", re.IGNORECASE)
THREE_DIG = re.compile(r"\d{3,}")


def infer_inverter_no(asset_name: str | None) -> int | None:
    if not asset_name:
        return None
    m = INV_NUM_DASH.search(asset_name)
    if m:
        return int(m.group(1))
    m2 = INVERTER_NUM.search(asset_name)
    if m2:
        n = int(m2.group(1))
        if 1 <= n <= 99:
            return n
        if 101 <= n <= 199:
            return n - 100
    m3 = INV_NUM_LOOSE.search(asset_name)
    if m3:
        n = int(m3.group(1))
        if 1 <= n <= 99:
            return n
        if 101 <= n <= 199:
            return n - 100
    for m4 in THREE_DIG.finditer(asset_name):
        n = int(m4.group(0))
        if 101 <= n <= 199:
            return n - 100
    return None


def export_seed_inverter_config_csv(conn, out_path: Path) -> tuple[int, list[str]]:
    cols = [
        r[0]
        for r in conn.execute(
            text(
                """
                SELECT column_name
                FROM information_schema.columns
                WHERE table_schema='staging' AND table_name='seed_inverter_config'
                ORDER BY ordinal_position
                """
            )
        ).fetchall()
    ]
    rows = conn.execute(
        text(
            """
            SELECT *
            FROM staging.seed_inverter_config
            ORDER BY site_id::text, inverter_no NULLS LAST, source::text
            """
        )
    ).mappings().all()
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with out_path.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=cols, delimiter=";")
        w.writeheader()
        for row in rows:
            w.writerow({c: "" if row[c] is None else str(row[c]) for c in cols})
    return len(rows), cols


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--site-id", action="append", dest="site_ids", help="limit to site(s)")
    p.add_argument("--days", type=int, default=120, help="mart lookback for distinct keys")
    p.add_argument("--dry-run", action="store_true")
    p.add_argument(
        "--force",
        action="store_true",
        help="overwrite existing non-blank inverter_id in seed_inverter_config",
    )
    p.add_argument(
        "--write-string-layout-inverter-id",
        action="store_true",
        help="UPDATE seed_inverter_string_layout.inverter_id (only if column type is text; INTEGER will error)",
    )
    p.add_argument(
        "--export-csv",
        nargs="?",
        const=str(ROOT / "dbt" / "seeds" / "seed_inverter_config.csv"),
        help="export staging.seed_inverter_config to semicolon CSV (default: dbt/seeds/seed_inverter_config.csv)",
    )
    args = p.parse_args()

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

    site_filter = ""
    params: dict = {"days": args.days}
    if args.site_ids:
        site_filter = "AND mk.site_id = ANY(:sids)"
        params["sids"] = args.site_ids

    q_pairs = text(
        f"""
        WITH mk AS (
            SELECT
                site_id::text AS site_id,
                REGEXP_REPLACE(inverter_id::text, '^(FS_|ISO_)', '') AS ps_key,
                COUNT(*) AS mart_rows
            FROM mart.mart_string_performance_daily
            WHERE date_key >= CURRENT_DATE - (:days * INTERVAL '1 day')
            GROUP BY 1, 2
        ),
        da AS (
            SELECT
                site_id::text AS site_id,
                REGEXP_REPLACE(asset_id::text, '^(FS_|ISO_)', '') AS ps_key,
                MAX(asset_name::text) AS asset_name
            FROM dimensions.dim_assets
            WHERE asset_level = 'Device'
              AND device_category = 'Inverter'
            GROUP BY 1, 2
        )
        SELECT
            mk.site_id,
            mk.ps_key,
            mk.mart_rows,
            da.asset_name
        FROM mk
        JOIN da
          ON da.site_id = mk.site_id
         AND da.ps_key = mk.ps_key
        WHERE 1=1
        {site_filter}
        ORDER BY mk.site_id, mk.ps_key
        """
    )

    q_site_name = text(
        """
        SELECT MAX(site_name::text)
        FROM staging.seed_inverter_string_layout
        WHERE site_id::text = :sid AND COALESCE(is_active, TRUE)
        """
    )

    upd_cfg = text(
        """
        UPDATE staging.seed_inverter_config
        SET inverter_id = :ps_key
        WHERE site_id::text = :sid
          AND inverter_no = :inv
          AND COALESCE(is_active, TRUE)
          AND (
            :force
            OR inverter_id IS NULL
            OR btrim(inverter_id::text) = ''
          )
        """
    )

    ins_cfg = text(
        """
        INSERT INTO staging.seed_inverter_config (
            source, site_id, site_name, inverter_no, inverter_id, inverter_sn,
            is_active
        ) VALUES (
            'dim_assets_auto', :sid, :sname, :inv, :ps_key, :ps_key, TRUE
        )
        """
    )
    exists_cfg = text(
        """
        SELECT 1
        FROM staging.seed_inverter_config
        WHERE site_id::text = :sid
          AND inverter_no = :inv
          AND COALESCE(is_active, TRUE)
        LIMIT 1
        """
    )

    upd_layout = text(
        """
        UPDATE staging.seed_inverter_string_layout
        SET inverter_id = :ps_key::text
        WHERE site_id::text = :sid
          AND inverter_no = :inv
          AND COALESCE(is_active, TRUE)
          AND (
            :force
            OR inverter_id IS NULL
            OR btrim(inverter_id::text) = ''
          )
        """
    )

    with engine.connect() as conn:
        rows = conn.execute(q_pairs, params).fetchall()

    # (site_id, inverter_no) -> best ps_key by max mart_rows
    best: dict[tuple[str, int], tuple[str, int]] = {}
    conflicts: list[str] = []

    for r in rows:
        m = dict(r._mapping)
        sid = m["site_id"]
        ps_key = m["ps_key"]
        rows_n = int(m["mart_rows"])
        inv = infer_inverter_no(m.get("asset_name"))
        if inv is None:
            conflicts.append(f"skip infer: {sid} ps_key={ps_key} name={m.get('asset_name')!r}")
            continue
        k = (sid, inv)
        if k not in best or rows_n > best[k][1]:
            best[k] = (ps_key, rows_n)

    # detect duplicate ps_key assigned to different inv (shouldn't happen often)
    inv_by_ps: dict[tuple[str, str], int] = {}
    for (sid, inv), (ps_key, _) in best.items():
        pk2 = (sid, ps_key)
        if pk2 in inv_by_ps and inv_by_ps[pk2] != inv:
            conflicts.append(f"ps_key {ps_key} maps inv {inv_by_ps[pk2]} and {inv} on {sid}")
        inv_by_ps[pk2] = inv

    print(f"planned inverter mappings: {len(best)}")
    for (sid, inv), (ps_key, w) in sorted(best.items()):
        print({"site_id": sid, "inverter_no": inv, "inverter_id": ps_key, "mart_weight": w})
    if conflicts:
        print("--- warnings ---")
        for c in conflicts[:50]:
            print(c)
        if len(conflicts) > 50:
            print("...", len(conflicts) - 50, "more")

    if args.dry_run:
        print("dry-run: no DB writes")
        return

    updated_cfg = 0
    inserted_cfg = 0
    updated_layout = 0
    force_flag = args.force

    with engine.begin() as conn:
        for (sid, inv), (ps_key, _) in sorted(best.items()):
            sname_row = conn.execute(q_site_name, {"sid": sid}).fetchone()
            sname = sname_row[0] if sname_row and sname_row[0] else sid

            r = conn.execute(
                upd_cfg,
                {"ps_key": ps_key, "sid": sid, "inv": inv, "force": force_flag},
            )
            updated_cfg += r.rowcount or 0
            if not r.rowcount:
                already_exists = conn.execute(exists_cfg, {"sid": sid, "inv": inv}).fetchone()
                if not already_exists:
                    try:
                        conn.execute(
                            ins_cfg,
                            {"sid": sid, "sname": sname, "inv": inv, "ps_key": ps_key},
                        )
                        inserted_cfg += 1
                    except Exception as ex:
                        conflicts.append(f"INSERT fail {sid} inv {inv}: {ex}")

            if args.write_string_layout_inverter_id:
                r2 = conn.execute(
                    upd_layout,
                    {"ps_key": ps_key, "sid": sid, "inv": inv, "force": force_flag},
                )
                updated_layout += r2.rowcount or 0

    print(
        {
            "seed_inverter_config_updated": updated_cfg,
            "seed_inverter_config_inserted": inserted_cfg,
            "seed_inverter_string_layout_updated": updated_layout
            if args.write_string_layout_inverter_id
            else "skipped",
        }
    )
    if args.export_csv:
        out = Path(args.export_csv)
        with engine.connect() as conn:
            exported_rows, exported_cols = export_seed_inverter_config_csv(conn, out)
        print(
            {
                "export_csv": str(out),
                "export_rows": exported_rows,
                "export_columns": len(exported_cols),
            }
        )


if __name__ == "__main__":
    main()
