#!/usr/bin/env python
"""
Generate Materialized Views for Performance Monitoring (Excel Export)

This script generates SQL for creating materialized views per site per year
for performance monitoring data (meter, sensor, inverter) in wide format for Excel.

Replaces the old approach that:
- Generated views directly from raw tables (isolarcloud_historical_data, fusionsolar_historical_data)
- Required JSONB parsing
- Created separate views per device type

New approach:
- Uses normalized mart tables (mart_meter_performance_5min, mart_sensor_measurements_5min, mart_inverter_performance_5min)
- Creates one materialized view per site per year (combines all device types)
- Much more efficient and faster
"""

import logging
from sqlalchemy import create_engine, text
import os
from dotenv import load_dotenv
import urllib.parse
from pathlib import Path
from typing import List, Dict, Tuple
import argparse

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Database Configuration
POSTGRES_HOST = os.getenv("POSTGRES_HOST", "10.101.4.88")
POSTGRES_PORT = os.getenv("POSTGRES_PORT", "5432")
POSTGRES_DB = os.getenv("POSTGRES_DB", "MMSR")
POSTGRES_USER = os.getenv("POSTGRES_USER", "juice")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")

# Construct DATABASE_URL
encoded_password = urllib.parse.quote_plus(POSTGRES_PASSWORD) if POSTGRES_PASSWORD else ""
DATABASE_URL = f"postgresql://{POSTGRES_USER}:{encoded_password}@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"


def get_sites(conn) -> List[Dict]:
    """Get all sites from dim_assets."""
    query = text("""
        SELECT DISTINCT 
            site_name,
            site_id
        FROM dimensions.dim_assets
        WHERE asset_level = 'Site'
        ORDER BY site_name
    """)
    result = conn.execute(query)
    return [{"site_name": row[0], "site_id": row[1]} for row in result]


def get_inverters_for_site(conn, site_name: str) -> List[Dict]:
    """Get all inverters for a site."""
    query = text("""
        SELECT DISTINCT
            asset_id,
            asset_name
        FROM dimensions.dim_assets
        WHERE site_name = :site_name
            AND asset_level = 'Device'
            AND (asset_name ILIKE '%inverter%' OR device_category = 'Inverter')
        ORDER BY asset_id
        LIMIT 20
    """)
    result = conn.execute(query, {"site_name": site_name})
    return [{"asset_id": row[0], "asset_name": row[1]} for row in result]


def generate_mv_name(site_name: str, year: int) -> str:
    """Generate materialized view name from site name and year."""
    clean_name = site_name.lower().replace(' ', '_').replace('-', '_')
    return f"mv_performance_monitoring_5min_{clean_name}_{year}"


def generate_inverter_columns(inverters: List[Dict]) -> Tuple[str, str]:
    """
    Generate SQL for inverter columns in pivot.
    Returns: (SELECT columns, column list for final SELECT)
    """
    select_cols = []
    final_cols = []
    
    for idx, inv in enumerate(inverters, start=1):
        inv_num = idx
        asset_id_escaped = inv['asset_id'].replace("'", "''")
        
        # SELECT column in pivot CTE
        select_cols.append(f"""
        MAX(CASE 
            WHEN i.asset_id = '{asset_id_escaped}'
            AND i.metric_name = 'inv_active_power'
            THEN i.metric_value 
        END) as inverter_{inv_num}_active_power_kw""")
        
        # Final SELECT column
        final_cols.append(f"    i.inverter_{inv_num}_active_power_kw,")
    
    return ",\n".join(select_cols), "\n".join(final_cols)


def generate_mv_sql(site_name: str, year: int, inverters: List[Dict]) -> str:
    """Generate SQL for creating materialized view."""
    
    mv_name = generate_mv_name(site_name, year)
    view_name = mv_name.replace('mv_', 'vw_')
    site_name_escaped = site_name.replace("'", "''")
    
    # Generate inverter columns
    inv_select_cols, inv_final_cols = generate_inverter_columns(inverters)
    inv_count = len(inverters)
    
    sql = f"""-- ============================================================================
-- Materialized View: Performance Monitoring 5-Minute Data
-- Site: {site_name}
-- Year: {year}
-- Generated: {__file__}
-- ============================================================================

CREATE MATERIALIZED VIEW IF NOT EXISTS mart.{mv_name} AS
WITH 
-- ============================================================================
-- METER DATA: Pivot per metric
-- ============================================================================
meter_pivot AS (
    SELECT 
        m.timestamp,
        m.date_key,
        MAX(CASE WHEN m.metric_name = 'positive_active_energy' THEN m.metric_value END) as meter_positive_energy_kwh,
        MAX(CASE WHEN m.metric_name = 'negative_active_energy' THEN m.metric_value END) as meter_negative_energy_kwh,
        MAX(CASE WHEN m.metric_name = 'active_power' THEN m.metric_value END) as meter_active_power_kw,
        MAX(m.asset_name) as meter_name,
        MAX(m.asset_id) as meter_id
    FROM mart.mart_meter_performance_5min m
    WHERE m.site_name = '{site_name_escaped}'
        AND m.date_key >= '{year}-01-01'::date
        AND m.date_key < '{year + 1}-01-01'::date
        AND m.metric_name IN ('positive_active_energy', 'negative_active_energy', 'active_power')
    GROUP BY m.timestamp, m.date_key
),

-- ============================================================================
-- SENSOR DATA: Pivot per metric
-- ============================================================================
sensor_pivot AS (
    SELECT 
        s.timestamp,
        s.date_key,
        MAX(CASE 
            WHEN s.metric_name = 'irradiance' 
            AND sc.sensor_type = 'GHI' 
            THEN s.metric_value 
        END) as sensor_ghi_w_m2,
        MAX(CASE 
            WHEN s.metric_name = 'irradiance' 
            AND sc.sensor_type = 'POA' 
            THEN s.metric_value 
        END) as sensor_poa_w_m2,
        MAX(CASE WHEN s.metric_name = 'temperature' THEN s.metric_value END) as sensor_temperature_c,
        MAX(s.asset_name) as sensor_name,
        MAX(s.asset_id) as sensor_id
    FROM mart.mart_sensor_measurements_5min s
    LEFT JOIN dbt.seed_sensor_config sc 
        ON s.asset_id = CONCAT(
            CASE 
                WHEN s.system = 'fusionsolar' THEN 'FS'
                WHEN s.system = 'isolarcloud' THEN 'ISO'
                ELSE UPPER(LEFT(s.system, 3))
            END, '_', sc.device_id
        )
    WHERE s.site_name = '{site_name_escaped}'
        AND s.date_key >= '{year}-01-01'::date
        AND s.date_key < '{year + 1}-01-01'::date
        AND s.metric_name IN ('irradiance', 'temperature')
    GROUP BY s.timestamp, s.date_key
),

-- ============================================================================
-- INVERTER DATA: Pivot per inverter (semua nilai 5 menit, bukan agregasi)
-- ============================================================================
inverter_pivot AS (
    SELECT 
        i.timestamp,
        i.date_key,
{inv_select_cols},
        COUNT(DISTINCT CASE 
            WHEN i.metric_name = 'inv_active_power' 
            THEN i.asset_id 
        END) as total_inverter_count,
        STRING_AGG(DISTINCT i.asset_name, ', ' ORDER BY i.asset_name) as inverter_names
    FROM mart.mart_inverter_performance_5min i
    WHERE i.site_name = '{site_name_escaped}'
        AND i.date_key >= '{year}-01-01'::date
        AND i.date_key < '{year + 1}-01-01'::date
        AND i.metric_name = 'inv_active_power'
        AND i.metric_value IS NOT NULL
    GROUP BY i.timestamp, i.date_key
)

-- ============================================================================
-- FINAL OUTPUT: Join semua data
-- ============================================================================
SELECT 
    COALESCE(m.timestamp, s.timestamp, i.timestamp) as timestamp,
    COALESCE(m.date_key, s.date_key, i.date_key) as date_key,
    
    -- Meter columns
    m.meter_positive_energy_kwh,
    m.meter_negative_energy_kwh,
    m.meter_active_power_kw,
    m.meter_name,
    m.meter_id,
    
    -- Sensor columns
    s.sensor_ghi_w_m2,
    s.sensor_poa_w_m2,
    s.sensor_temperature_c,
    s.sensor_name,
    s.sensor_id,
    
    -- Inverter columns (per inverter, semua nilai 5 menit)
{inv_final_cols}
    i.total_inverter_count,
    i.inverter_names
    
FROM meter_pivot m
FULL OUTER JOIN sensor_pivot s 
    ON m.timestamp = s.timestamp
FULL OUTER JOIN inverter_pivot i 
    ON COALESCE(m.timestamp, s.timestamp) = i.timestamp
ORDER BY timestamp;

-- ============================================================================
-- Create Indexes
-- ============================================================================

CREATE UNIQUE INDEX IF NOT EXISTS idx_{mv_name}_unique_timestamp 
    ON mart.{mv_name}(timestamp);

CREATE INDEX IF NOT EXISTS idx_{mv_name}_date 
    ON mart.{mv_name}(date_key);

CREATE INDEX IF NOT EXISTS idx_{mv_name}_date_timestamp 
    ON mart.{mv_name}(date_key, timestamp);

-- ============================================================================
-- Grant Permissions
-- ============================================================================

GRANT SELECT ON mart.{mv_name} TO PUBLIC;

-- ============================================================================
-- Create View Wrapper
-- ============================================================================

CREATE OR REPLACE VIEW mart.{view_name} AS
SELECT * 
FROM mart.{mv_name}
ORDER BY timestamp;

GRANT SELECT ON mart.{view_name} TO PUBLIC;

"""
    return sql


def generate_all_mvs(conn, years: List[int], output_dir: Path = None, execute: bool = False):
    """Generate materialized views for all sites and years."""
    
    sites = get_sites(conn)
    logger.info(f"Found {len(sites)} sites")
    
    generated_files = []
    
    for site in sites:
        site_name = site['site_name']
        logger.info(f"Processing site: {site_name}")
        
        # Get inverters for this site
        inverters = get_inverters_for_site(conn, site_name)
        logger.info(f"  Found {len(inverters)} inverters")
        
        if len(inverters) > 20:
            logger.warning(f"  WARNING: Site has {len(inverters)} inverters, only first 20 will be included")
        
        for year in years:
            logger.info(f"  Generating MV for year {year}")
            
            # Generate SQL
            sql = generate_mv_sql(site_name, year, inverters)
            
            # Write to file if output_dir specified
            if output_dir:
                mv_name = generate_mv_name(site_name, year)
                sql_file = output_dir / f"{mv_name}.sql"
                sql_file.write_text(sql, encoding='utf-8')
                generated_files.append(sql_file)
                logger.info(f"    Written to: {sql_file}")
            
            # Execute if requested
            if execute:
                try:
                    conn.execute(text(sql))
                    conn.commit()
                    logger.info(f"    Created materialized view: mart.{mv_name}")
                except Exception as e:
                    logger.error(f"    ERROR creating MV: {e}")
                    conn.rollback()
    
    return generated_files


def main():
    parser = argparse.ArgumentParser(description='Generate Materialized Views for Performance Monitoring')
    parser.add_argument('--years', nargs='+', type=int, default=[2024, 2025, 2026],
                        help='Years to generate MVs for (default: 2024 2025 2026)')
    parser.add_argument('--output-dir', type=str, default='excel/generated_sql',
                        help='Output directory for SQL files (default: excel/generated_sql)')
    parser.add_argument('--execute', action='store_true',
                        help='Execute SQL directly (create MVs in database)')
    parser.add_argument('--site', type=str, default=None,
                        help='Generate for specific site only (default: all sites)')
    
    args = parser.parse_args()
    
    # Create output directory
    output_dir = Path(args.output_dir)
    if args.output_dir and not args.execute:
        output_dir.mkdir(parents=True, exist_ok=True)
        logger.info(f"Output directory: {output_dir}")
    
    # Connect to database
    try:
        engine = create_engine(DATABASE_URL)
        with engine.connect() as conn:
            if args.site:
                # Generate for specific site only
                sites = [{"site_name": args.site, "site_id": None}]
                inverters = get_inverters_for_site(conn, args.site)
                for year in args.years:
                    sql = generate_mv_sql(args.site, year, inverters)
                    if output_dir:
                        mv_name = generate_mv_name(args.site, year)
                        sql_file = output_dir / f"{mv_name}.sql"
                        sql_file.write_text(sql, encoding='utf-8')
                        logger.info(f"Generated: {sql_file}")
                    if args.execute:
                        conn.execute(text(sql))
                        conn.commit()
                        logger.info(f"Created MV: mart.{mv_name}")
            else:
                # Generate for all sites
                files = generate_all_mvs(conn, args.years, output_dir, args.execute)
                logger.info(f"\nGenerated {len(files)} SQL files")
    except Exception as e:
        logger.error(f"Error: {e}", exc_info=True)
        raise


if __name__ == "__main__":
    main()

