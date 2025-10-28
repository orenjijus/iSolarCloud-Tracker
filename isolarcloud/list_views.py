"""
List all views in the PostgreSQL database
"""

import logging
from sqlalchemy import create_engine, text
import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Database Configuration
POSTGRES_HOST = os.getenv("POSTGRES_HOST", "10.101.4.88")
POSTGRES_PORT = os.getenv("POSTGRES_PORT", "5432")
POSTGRES_DB = os.getenv("POSTGRES_DB", "MMSR")
POSTGRES_USER = os.getenv("POSTGRES_USER", "juice")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")

# Construct DATABASE_URL from individual PostgreSQL settings
import urllib.parse
# URL encode the password to handle special characters
encoded_password = urllib.parse.quote_plus(POSTGRES_PASSWORD) if POSTGRES_PASSWORD else ""
DATABASE_URL = f"postgresql://{POSTGRES_USER}:{encoded_password}@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"

def list_all_views():
    """List all views in the database"""
    try:
        # Create database connection
        engine = create_engine(DATABASE_URL)
        
        # Connect to the database
        with engine.connect() as conn:
            # Query to get all views
            query = text("""
                SELECT viewname 
                FROM pg_catalog.pg_views 
                WHERE schemaname = 'public'
                ORDER BY viewname;
            """)
            
            result = conn.execute(query)
            views = [row[0] for row in result]
            
            if views:
                print(f"Found {len(views)} views:")
                for view in views:
                    print(f" - {view}")
            else:
                print("No views found in the database.")
    
    except Exception as e:
        print(f"Error listing views: {e}")

if __name__ == "__main__":
    list_all_views()
