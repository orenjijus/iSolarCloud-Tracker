"""
Apply orient_code only to staging.seed_inverter_string_layout for
Garuda Metalindo 1 (site_id 1458125).

No Inverter in the source table = 101..106 maps to inverter_no 1..6 in seed.
"""

from __future__ import annotations

import os
import urllib.parse
from pathlib import Path

from dotenv import load_dotenv
from sqlalchemy import create_engine, text

ROOT = Path(r"C:\Users\Administrator\Documents\Code\MMSR API - Server MA")
SITE_ID = "1458125"

# (display_inv, mppt_no, string_no, orient_code) — display_inv 101..106
ROWS: list[tuple[int, int, int, int]] = [
    (101, 1, 1, 1),
    (101, 1, 2, 1),
    (101, 2, 3, 1),
    (101, 2, 4, 1),
    (101, 3, 6, 1),
    (101, 4, 8, 1),
    (101, 5, 10, 1),
    (101, 6, 12, 4),
    (101, 7, 14, 3),
    (101, 8, 16, 1),
    # (101, 8, 18) tidak ada di seed — di DB string 18 untuk inv1 ada di MPPT 9
    (101, 9, 17, 1),
    (101, 9, 18, 1),
    (102, 1, 1, 2),
    (102, 1, 2, 2),
    (102, 2, 3, 2),
    (102, 2, 4, 2),
    (102, 3, 6, 2),
    (102, 4, 8, 2),
    (102, 5, 10, 2),
    (102, 6, 12, 2),
    (102, 7, 14, 2),
    (102, 8, 16, 4),
    (102, 9, 17, 2),
    (102, 9, 18, 2),
    (103, 1, 1, 3),
    (103, 1, 2, 3),
    (103, 2, 4, 4),
    (103, 3, 6, 4),
    (103, 4, 8, 4),
    (103, 5, 10, 4),
    (103, 6, 12, 4),
    (103, 7, 14, 3),
    (103, 8, 16, 3),
    # di seed: string 17 untuk inv3 ada di MPPT 9, bukan 8
    (103, 9, 17, 3),
    (103, 9, 18, 3),
    (104, 1, 1, 4),
    (104, 1, 2, 4),
    (104, 2, 3, 4),  # tidak ada di sheet; disamakan dengan string 4 di MPPT 2 (orient 4)
    (104, 2, 4, 4),
    (104, 3, 6, 4),
    (104, 4, 8, 4),
    (104, 5, 10, 4),
    (104, 6, 12, 3),
    (104, 7, 14, 3),
    (104, 8, 16, 3),
    (104, 9, 17, 3),
    (104, 9, 18, 3),
    (105, 1, 1, 3),
    (105, 1, 2, 3),
    (105, 2, 4, 3),
    (105, 3, 6, 3),
    (105, 4, 8, 4),
    (105, 5, 10, 4),
    (105, 6, 12, 4),
    (105, 7, 14, 4),
    (105, 8, 16, 3),
    (105, 9, 17, 3),
    (105, 9, 18, 3),
    (106, 1, 1, 3),
    (106, 1, 2, 3),
    (106, 2, 4, 4),
    (106, 3, 6, 4),
    (106, 4, 8, 4),
    (106, 5, 10, 4),
    (106, 6, 12, 4),
    (106, 7, 14, 3),
    (106, 8, 16, 3),
    (106, 9, 17, 3),
    (106, 9, 18, 3),
]


def main() -> None:
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

    upd = text(
        """
        UPDATE staging.seed_inverter_string_layout
        SET orient_code = :oc
        WHERE site_id::text = :sid
          AND inverter_no = :inv
          AND mppt_no = :mppt
          AND string_no = :sn
          AND COALESCE(is_active, TRUE)
        """
    )

    missing = []
    updated = 0
    with engine.begin() as conn:
        for disp, mppt, sn, oc in ROWS:
            inv = disp - 100
            if inv < 1 or inv > 99:
                missing.append((disp, mppt, sn, "bad_inv"))
                continue
            r = conn.execute(
                upd,
                {"sid": SITE_ID, "inv": inv, "mppt": mppt, "sn": sn, "oc": oc},
            )
            if not r.rowcount:
                missing.append((disp, mppt, sn, oc))
            else:
                updated += r.rowcount

        sync_dim = text(
            """
            UPDATE dimensions.dim_inverter_string_layout d
            SET orient_code = s.orient_code
            FROM staging.seed_inverter_string_layout s
            WHERE d.site_id::text = :sid
              AND s.site_id::text = :sid
              AND d.inverter_no = s.inverter_no
              AND d.mppt_no = s.mppt_no
              AND d.string_no = s.string_no
              AND COALESCE(d.is_active, TRUE)
              AND COALESCE(s.is_active, TRUE)
            """
        )
        rdim = conn.execute(sync_dim, {"sid": SITE_ID})

    print(
        {
            "site_id": SITE_ID,
            "rows_in_payload": len(ROWS),
            "seed_rows_updated": updated,
            "dim_rows_updated": rdim.rowcount,
            "not_found": len(missing),
        }
    )
    if missing:
        print("not_found (display_inv, mppt, string_no, orient_or_reason):", missing[:30])
        if len(missing) > 30:
            print("...", len(missing) - 30, "more")


if __name__ == "__main__":
    main()
