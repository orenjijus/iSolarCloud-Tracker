"""
Generate dbt data lineage for mmsr_solar_data project.

Outputs:
  lineage.xml  — XML lineage (nodes + edges + columns + per-column SQL formulas)
  lineage.json — JSON version (for canvas / tooling)

Sources of truth:
  - SQL files   → model-to-model dependency edges (via {{ ref() }} / {{ source() }})
  - DB catalog  → actual column names per table (only active columns in DB)
  - CSV headers → seed column names
  - dbt_project.yml → enabled/disabled models
  - dbt target/manifest.json → compiled SQL per model (column expressions)

  Run `dbt compile` in the dbt/ folder first so manifest.json is fresh; otherwise
  formulas may be missing or stale.

Optional dependency:
  pip install sqlglot   # required for formula extraction

Usage:
  python generate_dbt_lineage.py
  python generate_dbt_lineage.py --out-dir reports/lineage
  python generate_dbt_lineage.py --no-formulas
"""
from __future__ import annotations

import argparse
import json
import os
import re
import xml.etree.ElementTree as ET
from datetime import datetime
from pathlib import Path
from typing import Any

import psycopg2
import yaml
from psycopg2.extras import RealDictCursor

# ── Paths ─────────────────────────────────────────────────────────────────────
SCRIPT_DIR = Path(__file__).resolve().parent
DBT_DIR    = SCRIPT_DIR.parent / "dbt"
MODELS_DIR = DBT_DIR / "models"
SEEDS_DIR  = DBT_DIR / "seeds"

# ── Disabled models (from dbt_project.yml) ────────────────────────────────────
DISABLED_MODELS = {"mart_inverter_performance_daily"}

# ── Layer map: model name prefix → layer id ───────────────────────────────────
def model_layer(name: str) -> str:
    if name.startswith("stg_"):       return "staging"
    if name.startswith("dim_"):       return "dimensions"
    if name.startswith("fact_"):      return "facts"
    if name.startswith("mart_"):      return "marts"
    if name.startswith("seed_"):      return "seeds"
    return "other"

# ── DB schema map: layer → postgres schema ────────────────────────────────────
LAYER_SCHEMA: dict[str, str] = {
    "staging":    "staging",
    "dimensions": "dimensions",
    "facts":      "mart",
    "marts":      "mart",
    "seeds":      "staging",
    "raw":        "raw",
}

# ── Materialization map ────────────────────────────────────────────────────────
MATERIALIZATIONS: dict[str, str] = {}  # populated from SQL config blocks

# ── Helpers ───────────────────────────────────────────────────────────────────

def load_dbt_db() -> dict[str, Any]:
    path = os.path.expanduser("~/.dbt/profiles.yml")
    with open(path, encoding="utf-8") as f:
        return yaml.safe_load(f)["mmsr_solar"]["outputs"]["dev"]


def extract_refs(sql: str) -> list[str]:
    """Extract all {{ ref('model') }} references."""
    return re.findall(r"\{\{\s*ref\s*\(\s*['\"]([^'\"]+)['\"]\s*\)\s*\}\}", sql)


def extract_sources(sql: str) -> list[tuple[str, str]]:
    """Extract all {{ source('schema', 'table') }} references."""
    return re.findall(
        r"\{\{\s*source\s*\(\s*['\"]([^'\"]+)['\"]\s*,\s*['\"]([^'\"]+)['\"]\s*\)\s*\}\}",
        sql,
    )


def extract_materialization(sql: str) -> str | None:
    m = re.search(r"materialized\s*=\s*['\"]([^'\"]+)['\"]", sql)
    return m.group(1) if m else None


def csv_header_columns(csv_path: Path, delimiter: str = ";") -> list[str]:
    try:
        first = csv_path.read_text(encoding="utf-8").splitlines()[0]
        for delim in [delimiter, ",", ";"]:
            cols = [c.strip().strip('"') for c in first.split(delim)]
            if len(cols) > 1:
                return cols
        return [first.strip()]
    except Exception:
        return []


# ── dbt manifest + column formulas (compiled SQL) ────────────────────────────

PROJECT_NAME = "mmsr_solar_data"


def load_compiled_sql_by_model() -> dict[str, str]:
    """model_name → compiled SQL from last `dbt compile` (Jinja resolved)."""
    path = DBT_DIR / "target" / "manifest.json"
    if not path.exists():
        return {}
    try:
        manifest = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    out: dict[str, str] = {}
    for node in manifest.get("nodes", {}).values():
        if node.get("resource_type") != "model":
            continue
        if node.get("package_name") != PROJECT_NAME:
            continue
        name = node.get("name")
        code = (node.get("compiled_code") or "").strip()
        if name and code:
            out[name] = code
    return out


def extract_column_formulas(compiled_sql: str) -> dict[str, str] | None:
    """
    Parse the outer SELECT of compiled SQL and return column_name → expression SQL.
    Returns None if sqlglot is not installed; {} on parse failure.
    """
    try:
        import sqlglot
        from sqlglot import exp
    except ImportError:
        return None

    try:
        tree = sqlglot.parse_one(compiled_sql, dialect="postgres")
    except Exception:
        return {}

    if not isinstance(tree, exp.Select):
        return {}

    formulas: dict[str, str] = {}
    for proj in tree.expressions:
        alias = proj.alias_or_name
        if not alias:
            continue
        if isinstance(proj, exp.Alias):
            expr_sql = proj.this.sql(dialect="postgres", pretty=False)
        else:
            expr_sql = proj.sql(dialect="postgres", pretty=False)
        formulas[str(alias)] = expr_sql
    return formulas


# ── DB column fetcher ──────────────────────────────────────────────────────────

def fetch_columns(conn, schema: str, table: str) -> list[dict]:
    """Return [{name, data_type, ordinal_position}] for table if it exists."""
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(
            """
            SELECT column_name, data_type, ordinal_position
            FROM information_schema.columns
            WHERE table_schema = %s AND table_name = %s
            ORDER BY ordinal_position
            """,
            (schema, table),
        )
        return [dict(r) for r in cur]


def table_exists(conn, schema: str, table: str) -> bool:
    with conn.cursor() as cur:
        cur.execute(
            "SELECT 1 FROM information_schema.tables WHERE table_schema=%s AND table_name=%s",
            (schema, table),
        )
        return cur.fetchone() is not None


# ── Main ──────────────────────────────────────────────────────────────────────

def build_lineage(include_formulas: bool = True) -> dict:
    """Build full lineage dict: nodes + edges."""

    compiled_by_model = load_compiled_sql_by_model() if include_formulas else {}

    # 1. Collect all SQL model files
    sql_files = list(MODELS_DIR.rglob("*.sql"))
    model_info: dict[str, dict] = {}  # model_name → {sql_path, layer, deps, source_deps, mat}

    for f in sql_files:
        name = f.stem
        if name in DISABLED_MODELS:
            continue
        sql = f.read_text(encoding="utf-8")
        mat = extract_materialization(sql) or ("view" if "staging" in str(f) else "table")
        model_info[name] = {
            "name":        name,
            "layer":       model_layer(name),
            "sql_path":    str(f.relative_to(DBT_DIR)),
            "materialized": mat,
            "refs":        list(dict.fromkeys(extract_refs(sql))),        # dedup, preserve order
            "sources":     list(dict.fromkeys(map(tuple, extract_sources(sql)))),
        }

    # 2. Collect seeds from CSV headers
    seed_info: dict[str, dict] = {}
    for csv in SEEDS_DIR.glob("seed_*.csv"):
        name = csv.stem
        # skip disabled
        if name == "seed_monthly_simulation_target":
            continue
        seed_info[name] = {
            "name":       name,
            "layer":      "seeds",
            "csv_path":   str(csv.relative_to(DBT_DIR)),
            "csv_columns": csv_header_columns(csv),
        }

    # 3. Raw sources
    raw_sources = {
        "isolarcloud_historical_data": ["timestamp", "device_ps_key", "measurement_data"],
        "isolarcloud_devices":         ["device_ps_key", "device_id", "device_name", "device_type", "ps_id"],
        "isolarcloud_power_stations":  ["ps_id", "ps_name", "latitude", "longitude"],
        "fusionsolar_historical_data": ["collect_time", "dev_id", "measurement_data"],
        "fusionsolar_devices":         ["dev_id", "dev_name", "dev_type_id", "plant_code"],
        "fusionsolar_plants":          ["plant_code", "plant_name", "latitude", "longitude"],
    }

    # 4. Fetch columns from DB
    cfg = load_dbt_db()
    conn = psycopg2.connect(
        host=cfg["host"], port=cfg["port"],
        user=cfg["user"], password=cfg["password"], dbname=cfg["dbname"],
    )

    db_columns: dict[str, list[dict]] = {}
    try:
        for name, info in model_info.items():
            layer = info["layer"]
            schema = LAYER_SCHEMA.get(layer, "public")
            cols = fetch_columns(conn, schema, name)
            if not cols:
                # try mart schema fallback for facts
                cols = fetch_columns(conn, "mart", name)
            db_columns[name] = cols

        for name in seed_info:
            cols = fetch_columns(conn, "staging", name)
            if cols:
                db_columns[name] = cols

        for src_name in raw_sources:
            cols = fetch_columns(conn, "raw", src_name)
            if cols:
                db_columns[src_name] = cols
    finally:
        conn.close()

    # 5. Build node list
    nodes: list[dict] = []

    # Raw source nodes
    for src_name, fallback_cols in raw_sources.items():
        db_cols = db_columns.get(src_name, [])
        col_list = [c["column_name"] for c in db_cols] if db_cols else fallback_cols
        nodes.append({
            "id":           f"raw.{src_name}",
            "name":         src_name,
            "layer":        "raw",
            "schema":       "raw",
            "materialized": "table",
            "columns":      col_list,
            "in_db":        bool(db_cols),
        })

    # Seed nodes
    for name, info in seed_info.items():
        db_cols = db_columns.get(name, [])
        col_list = [c["column_name"] for c in db_cols] if db_cols else info["csv_columns"]
        nodes.append({
            "id":           f"seed.{name}",
            "name":         name,
            "layer":        "seeds",
            "schema":       "staging",
            "materialized": "seed",
            "columns":      col_list,
            "in_db":        bool(db_cols),
            "csv_path":     info.get("csv_path"),
        })

    # Model nodes (staging, dimensions, facts, marts)
    formula_stats: dict[str, Any] = {
        "models_with_formulas": 0,
        "columns_with_formula": 0,
        "sqlglot_missing": False,
        "manifest_models": len(compiled_by_model),
    }
    for name, info in model_info.items():
        db_cols = db_columns.get(name, [])
        col_list = [c["column_name"] for c in db_cols] if db_cols else []
        col_formulas: dict[str, str] = {}
        if include_formulas and compiled_by_model.get(name):
            parsed = extract_column_formulas(compiled_by_model[name])
            if parsed is None:
                formula_stats["sqlglot_missing"] = True
            elif parsed:
                keys = col_list if col_list else sorted(parsed.keys())
                n_added = 0
                for col in keys:
                    f = parsed.get(col)
                    if f:
                        col_formulas[col] = f
                        n_added += 1
                if n_added:
                    formula_stats["models_with_formulas"] += 1
                    formula_stats["columns_with_formula"] += n_added
        node: dict[str, Any] = {
            "id":           name,
            "name":         name,
            "layer":        info["layer"],
            "schema":       LAYER_SCHEMA.get(info["layer"], "public"),
            "materialized": info["materialized"],
            "columns":      col_list,
            "in_db":        bool(db_cols),
            "sql_path":     info["sql_path"],
        }
        if col_formulas:
            node["column_formulas"] = col_formulas
        nodes.append(node)

    # 6. Build edge list
    edges: list[dict] = []

    for name, info in model_info.items():
        # ref edges: model → this model
        for ref in info["refs"]:
            # ref may be another model or a seed
            from_id = f"seed.{ref}" if ref in seed_info else ref
            edges.append({"from": from_id, "to": name, "type": "ref"})
        # source edges
        for schema, table in info["sources"]:
            edges.append({"from": f"raw.{table}", "to": name, "type": "source"})

    return {
        "project":   "mmsr_solar_data",
        "generated": datetime.now().isoformat(timespec="seconds"),
        "layers": [
            {"id": "raw",        "label": "Raw Sources",       "color": "#64748b"},
            {"id": "seeds",      "label": "Seed Tables",       "color": "#7c3aed"},
            {"id": "staging",    "label": "Staging Views",     "color": "#0284c7"},
            {"id": "dimensions", "label": "Dimension Tables",  "color": "#0891b2"},
            {"id": "facts",      "label": "Fact Tables",       "color": "#d97706"},
            {"id": "marts",      "label": "Mart Tables",       "color": "#16a34a"},
        ],
        "nodes": nodes,
        "edges": edges,
        "stats": {
            "total_nodes": len(nodes),
            "total_edges": len(edges),
            "nodes_in_db": sum(1 for n in nodes if n.get("in_db")),
            "total_columns": sum(len(n["columns"]) for n in nodes),
            "by_layer": {
                lid: len([n for n in nodes if n["layer"] == lid])
                for lid in ["raw","seeds","staging","dimensions","facts","marts"]
            },
            "formulas": formula_stats if include_formulas else {},
        },
        "formula_notes": {
            "source": "dbt target/manifest.json → compiled_code (run `dbt compile` in dbt/)",
            "dialect": "postgres (sqlglot)",
        },
    }


def lineage_to_xml(data: dict) -> ET.Element:
    root = ET.Element("lineage",
        project=data["project"],
        generated=data["generated"],
    )

    # Layers
    layers_el = ET.SubElement(root, "layers")
    for lyr in data["layers"]:
        ET.SubElement(layers_el, "layer", id=lyr["id"], label=lyr["label"])

    # Nodes
    nodes_el = ET.SubElement(root, "nodes")
    for n in data["nodes"]:
        node_el = ET.SubElement(nodes_el, "node",
            id=n["id"],
            name=n["name"],
            layer=n["layer"],
            schema=n["schema"],
            materialized=n["materialized"],
            in_db=str(n.get("in_db", False)).lower(),
        )
        if n.get("sql_path"):
            node_el.set("sql_path", n["sql_path"])
        if n.get("csv_path"):
            node_el.set("csv_path", n["csv_path"])
        cols_el = ET.SubElement(node_el, "columns")
        cf = n.get("column_formulas") or {}
        for col in n["columns"]:
            c_el = ET.SubElement(cols_el, "column", name=col)
            sql = cf.get(col) if isinstance(cf, dict) else None
            if sql:
                fm = ET.SubElement(c_el, "formula")
                fm.text = sql

    # Edges
    edges_el = ET.SubElement(root, "edges")
    for e in data["edges"]:
        ET.SubElement(edges_el, "edge",
            **{"from": e["from"], "to": e["to"], "type": e["type"]},
        )

    # Stats
    stats_el = ET.SubElement(root, "stats",
        total_nodes=str(data["stats"]["total_nodes"]),
        total_edges=str(data["stats"]["total_edges"]),
        nodes_in_db=str(data["stats"]["nodes_in_db"]),
        total_columns=str(data["stats"]["total_columns"]),
    )
    for lid, cnt in data["stats"]["by_layer"].items():
        ET.SubElement(stats_el, "layer_count", layer=lid, count=str(cnt))
    fs = data["stats"].get("formulas") or {}
    if fs:
        fsel = ET.SubElement(stats_el, "formulas",
            models_with_formulas=str(fs.get("models_with_formulas", 0)),
            columns_with_formula=str(fs.get("columns_with_formula", 0)),
            manifest_models=str(fs.get("manifest_models", 0)),
            sqlglot_missing=str(fs.get("sqlglot_missing", False)).lower(),
        )

    return root


def indent_xml(elem: ET.Element, level: int = 0) -> None:
    """Add pretty-print indentation to ElementTree."""
    pad = "\n" + "  " * level
    if len(elem):
        if not elem.text or not elem.text.strip():
            elem.text = pad + "  "
        if not elem.tail or not elem.tail.strip():
            elem.tail = pad
        for child in elem:
            indent_xml(child, level + 1)
        if not child.tail or not child.tail.strip():
            child.tail = pad
    else:
        if level and (not elem.tail or not elem.tail.strip()):
            elem.tail = pad


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--out-dir", default=str(SCRIPT_DIR.parent / "reports" / "lineage"))
    ap.add_argument("--no-formulas", action="store_true", help="Skip SQL formula extraction (no sqlglot / manifest needed)")
    args = ap.parse_args()

    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    print("Building lineage...")
    data = build_lineage(include_formulas=not args.no_formulas)

    # Write JSON
    json_path = out_dir / "lineage.json"
    json_path.write_text(json.dumps(data, indent=2, ensure_ascii=False), encoding="utf-8")
    print(f"  JSON → {json_path}")

    # Write XML
    root = lineage_to_xml(data)
    indent_xml(root)
    tree = ET.ElementTree(root)
    ET.indent(tree, space="  ")
    xml_path = out_dir / "lineage.xml"
    tree.write(str(xml_path), encoding="unicode", xml_declaration=True)
    print(f"  XML  → {xml_path}")

    # Print summary
    s = data["stats"]
    print(f"\nSummary:")
    print(f"  Nodes        : {s['total_nodes']}  ({s['nodes_in_db']} in DB)")
    print(f"  Edges        : {s['total_edges']}")
    print(f"  Total columns: {s['total_columns']}")
    print(f"  By layer     : {s['by_layer']}")
    fs = s.get("formulas") or {}
    if fs:
        print(f"  Formulas     : {fs.get('columns_with_formula', 0)} column exprs across "
              f"{fs.get('models_with_formulas', 0)} models (manifest: {fs.get('manifest_models', 0)} compiled)")
        if fs.get("sqlglot_missing"):
            print("  WARNING      : sqlglot not installed — pip install sqlglot")


if __name__ == "__main__":
    main()
