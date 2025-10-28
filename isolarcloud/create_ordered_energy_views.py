#!/usr/bin/env python
"""
Create ordered views for daily string energy data

This script creates structured views from the daily energy views
that we've already created. It organizes the columns in a more
logical way by extracting numbers from column names and
sorting them numerically rather than alphabetically.
"""

import os
import sys
import re
import psycopg2
import argparse
import logging
from datetime import datetime
from dotenv import load_dotenv

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger(__name__)

def get_db_connection():
    """
    Establish a connection to the PostgreSQL database.
    
    Returns:
        Connection object if successful, None otherwise.
    """
    load_dotenv()
    POSTGRES_HOST = os.getenv("POSTGRES_HOST", "10.101.4.88")
    POSTGRES_PORT = os.getenv("POSTGRES_PORT", "5432")
    POSTGRES_DB = os.getenv("POSTGRES_DB", "MMSR")
    POSTGRES_USER = os.getenv("POSTGRES_USER", "juice")
    POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")
    
    try:
        conn = psycopg2.connect(
            host=POSTGRES_HOST,
            port=POSTGRES_PORT,
            dbname=POSTGRES_DB,
            user=POSTGRES_USER,
            password=POSTGRES_PASSWORD
        )
        logger.info("Successfully connected to the database")
        return conn
    except Exception as e:
        logger.error(f"Database connection error: {e}")
        return None

def extract_sort_keys(column_name):
    """
    Extract sort keys from a column name.
    
    This function tries different regex patterns to extract numeric values
    from column names for sorting. It handles multiple patterns:
    - Standard pattern with device/inverter number, e.g., INV_01, SLI_INV_05
    - String/MPP number pattern, e.g., string_1, MPP_3
    - Any other numeric parts in the column name
    
    Args:
        column_name: Name of the column to analyze
        
    Returns:
        Tuple of (device_num, sub_component, sub_num) for sorting
    """
    # Extract device/inverter number (like INV_01, SLI_INV_05)
    device_match = re.search(r'(?:INV|INVERTER)_?(\d+)', column_name, re.IGNORECASE)
    device_num = int(device_match.group(1)) if device_match else 999
    
    # Extract string identifier (like A, B, F in SLI_INV_01_A_string_1)
    subtype_match = re.search(r'INV_\d+_([A-Z])', column_name)
    sub_component = subtype_match.group(1) if subtype_match else 'Z'
    
    # Extract string/MPP number (like string_1, MPP_3)
    num_match = re.search(r'(?:string|mpp)_(\d+)', column_name, re.IGNORECASE)
    
    # If no string/MPP match, look for other numbers in the column name
    if not num_match:
        # Look for the last number in the column name
        other_nums = re.findall(r'(\d+)', column_name)
        sub_num = int(other_nums[-1]) if other_nums else 999
    else:
        sub_num = int(num_match.group(1))
    
    return (device_num, sub_component, sub_num)

def create_ordered_energy_view(energy_view_name, ordered_view_name=None, execute=True):
    """
    Create an ordered view from a daily energy view.
    
    Args:
        energy_view_name: Name of the source daily energy view
        ordered_view_name: Name for the new ordered view (defaults to <energy_view_name>_ordered)
        execute: Whether to execute the SQL statement (True) or just return it (False)
        
    Returns:
        Generated SQL statement if successful, None otherwise
    """
    if ordered_view_name is None:
        ordered_view_name = f"{energy_view_name}_ordered"
    
    conn = get_db_connection()
    if not conn:
        return None
    
    cursor = conn.cursor()
    
    # Check if source view exists
    cursor.execute(f"""
        SELECT EXISTS (
            SELECT FROM information_schema.views 
            WHERE table_name = '{energy_view_name.lower()}'
        );
    """)
    view_exists = cursor.fetchone()[0]
    if not view_exists:
        logger.error(f"Source view {energy_view_name} does not exist")
        return None
    
    # Get column names from the source view
    cursor.execute(f"""
        SELECT column_name 
        FROM information_schema.columns 
        WHERE table_name = '{energy_view_name.lower()}'
        ORDER BY column_name
    """)
    all_columns = [row[0] for row in cursor.fetchall()]
    
    # Separate day_date column from energy columns
    day_date_col = 'day_date'
    energy_cols = [col for col in all_columns if col != day_date_col]
    
    # Sort energy columns based on extracted numbers
    energy_cols_with_keys = [(col, extract_sort_keys(col)) for col in energy_cols]
    sorted_energy_cols = [col for col, _ in sorted(energy_cols_with_keys, key=lambda x: x[1])]
    
    # Generate SQL for ordered view
    drop_sql = f"DROP VIEW IF EXISTS {ordered_view_name};"
    
    # Create view with SELECT that orders columns naturally
    column_list = [f'"{col}"' for col in sorted_energy_cols]
    create_sql = f"""
    CREATE VIEW {ordered_view_name} AS
    SELECT
        {day_date_col},
        {',\n        '.join(column_list)}
    FROM {energy_view_name}
    ORDER BY {day_date_col};
    """
    
    sql = drop_sql + "\n" + create_sql
    
    # Save SQL to file for reference
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    sql_filename = f"create_{ordered_view_name}_{timestamp}.sql"
    with open(sql_filename, 'w') as f:
        f.write(sql)
    logger.info(f"SQL saved to {sql_filename}")
    
    # Execute SQL if requested
    if execute:
        try:
            logger.info(f"Creating ordered view {ordered_view_name}")
            cursor.execute(sql)
            conn.commit()
            logger.info(f"View {ordered_view_name} created successfully")
        except Exception as e:
            logger.error(f"Database error: {e}")
            conn.rollback()
            return None
        finally:
            cursor.close()
            conn.close()
    else:
        logger.info(f"SQL generated for view {ordered_view_name} (not executed)")
        cursor.close()
        conn.close()
    
    return sql

def main():
    """
    Main function to parse arguments and execute the view creation.
    """
    parser = argparse.ArgumentParser(description='Create ordered energy views')
    parser.add_argument('--view', type=str, required=True,
                        help='Name of the daily energy view to order')
    parser.add_argument('--ordered-view', type=str, required=False,
                        help='Name for the new ordered view (defaults to <view>_ordered)')
    parser.add_argument('--dry-run', action='store_true',
                        help='Generate SQL but do not execute it')
    
    args = parser.parse_args()
    
    logger.info(f"Starting ordered view creation for view: {args.view}")
    
    sql = create_ordered_energy_view(
        energy_view_name=args.view,
        ordered_view_name=args.ordered_view,
        execute=not args.dry_run
    )
    
    if sql and args.dry_run:
        logger.info("Dry run completed. SQL generated but not executed.")
    elif sql:
        logger.info("View creation completed successfully.")
    else:
        logger.error("Failed to create view.")

if __name__ == "__main__":
    main()
