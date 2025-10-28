#!/usr/bin/env python
"""
Script to create daily string energy views by multiplying voltage and current
for each string in the inverter data.
"""

import os
import sys
import psycopg2
import argparse
from dotenv import load_dotenv
import logging
from datetime import datetime

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger(__name__)

def get_db_connection():
    """
    Establish a connection to the PostgreSQL database using .env file parameters.
    """
    # Load environment variables from .env file
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
    except psycopg2.Error as e:
        logger.error(f"Error connecting to the database: {e}")
        sys.exit(1)

def generate_daily_string_energy_view(table_name, view_name=None, execute=True):
    """
    Generate SQL to create a daily string energy view and optionally execute it.
    
    Args:
        table_name (str): The name of the pivoted table with voltage and current data
        view_name (str, optional): Name for the new view. Defaults to "{table_name}_daily_energy".
        execute (bool, optional): Whether to execute the SQL. Defaults to True.
    
    Returns:
        str: The generated SQL
    """
    if view_name is None:
        view_name = f"{table_name}_daily_energy"
    
    conn = get_db_connection()
    cursor = conn.cursor()
    
    try:
        # First check if the table exists
        cursor.execute(f"""
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_name = '{table_name.lower()}'
            );
        """)
        table_exists = cursor.fetchone()[0]
        if not table_exists:
            logger.error(f"Table '{table_name}' does not exist")
            return None
            
        # Get all voltage columns from the pivoted table
        logger.info(f"Retrieving voltage columns from {table_name}")
        cursor.execute(f"""
            SELECT column_name 
            FROM information_schema.columns 
            WHERE table_name = '{table_name.lower()}'
            AND column_name LIKE '%voltage%'
            ORDER BY column_name
        """)
        
        voltage_columns = [row[0] for row in cursor.fetchall()]
        logger.info(f"Found {len(voltage_columns)} voltage columns")
        
        if not voltage_columns:
            logger.error(f"No voltage columns found in table {table_name}")
            return None
        
        # Create SQL for daily energy calculations
        energy_calculations = []
        for voltage_col in voltage_columns:
            # Create corresponding current column name
            current_col = voltage_col.replace('voltage', 'current')
            
            # Check if current column exists
            cursor.execute(f"""
                SELECT 1 FROM information_schema.columns 
                WHERE table_name = '{table_name.lower()}' AND column_name = '{current_col}'
            """)
            if not cursor.fetchone():
                logger.warning(f"Current column {current_col} not found - skipping")
                continue
                
            # Create energy column name
            energy_col = voltage_col.replace('voltage', 'daily_energy_kwh')
            
            # Calculate energy in kWh:
            # 1. Multiply voltage by current to get power in watts
            # 2. Divide by 12 because each 5-minute interval represents 1/12 of an hour
            # 3. Divide by 1000 to convert from Wh to kWh
            energy_expr = f'SUM(COALESCE("{voltage_col}" * "{current_col}" / 12000.0, 0)) AS "{energy_col}"'
            energy_calculations.append(energy_expr)
        
        # First drop the view if it exists to avoid column renaming issues
        drop_sql = f"DROP VIEW IF EXISTS {view_name};"
        
        # Generate the complete SQL with proper quoting
        create_sql = f"""
        CREATE VIEW {view_name} AS
        SELECT
            date_trunc('day', timestamp) AS day_date,
            {',\n            '.join(energy_calculations)}
        FROM {table_name}
        GROUP BY day_date
        ORDER BY day_date;
        """
        
        # Combine the drop and create statements
        sql = drop_sql + "\n" + create_sql
        
        # Save SQL to file for reference
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        sql_filename = f"create_{view_name}_{timestamp}.sql"
        with open(sql_filename, 'w') as f:
            f.write(sql)
        logger.info(f"SQL saved to {sql_filename}")
        
        # Execute SQL if requested
        if execute:
            logger.info(f"Creating view {view_name}")
            cursor.execute(sql)
            conn.commit()
            logger.info(f"View {view_name} created successfully")
        
        return sql
    
    except psycopg2.Error as e:
        logger.error(f"Database error: {e}")
        conn.rollback()
        return None
    except Exception as e:
        logger.error(f"Error generating SQL: {e}")
        return None
    finally:
        cursor.close()
        conn.close()

def main():
    """
    Main function to parse arguments and execute the view creation.
    """
    parser = argparse.ArgumentParser(description='Create daily string energy views')
    parser.add_argument('--table', type=str, required=False, 
                        default='shoetown_ligung_indonesia_inverter_pivoted',
                        help='Name of the table(s) with voltage and current data. For multiple tables, separate with commas.')
    parser.add_argument('--view', type=str, required=False,
                        help='Name for the new view (only applicable when processing a single table)')
    parser.add_argument('--dry-run', action='store_true',
                        help='Generate SQL but do not execute it')
    
    args = parser.parse_args()
    
    # Split table names by comma and strip whitespace
    table_names = [table.strip() for table in args.table.split(',')]
    
    success_count = 0
    failure_count = 0
    
    for table_name in table_names:
        logger.info(f"Starting daily string energy view creation for table: {table_name}")
        
        # Only use the provided view name if processing a single table
        view_name = args.view if len(table_names) == 1 else None
        
        sql = generate_daily_string_energy_view(
            table_name=table_name,
            view_name=view_name,
            execute=not args.dry_run
        )
        
        if sql and args.dry_run:
            logger.info(f"Dry run completed for table {table_name}. SQL generated but not executed.")
            success_count += 1
        elif sql:
            logger.info(f"View creation completed successfully for table {table_name}.")
            success_count += 1
        else:
            logger.error(f"Failed to create view for table {table_name}.")
            failure_count += 1
    
    # Final summary
    total_tables = len(table_names)
    if total_tables > 1:
        logger.info(f"Process complete: {success_count}/{total_tables} views processed successfully, {failure_count} failures.")
        if failure_count > 0:
            logger.warning("Some tables could not be processed. Check the log for details.")
        elif success_count == total_tables:
            logger.info("All tables processed successfully.")
    else:
        if success_count == 1:
            logger.info("View creation completed successfully.")
        else:
            logger.error("Failed to create view.")

if __name__ == "__main__":
    main()
