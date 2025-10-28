import logging
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

# Import database config from existing module
from fusionsolar_harvester_src.fusionsolar_config import DATABASE_URL

def init_database():
    """Initialize database connection."""
    try:
        engine = create_engine(DATABASE_URL)
        Session = sessionmaker(bind=engine)
        return engine, Session
    except Exception as e:
        logging.error(f"Failed to initialize database: {e}")
        return None, None

def list_all_views():
    """List all views in the database."""
    engine, Session = init_database()
    if not engine:
        print("Database initialization failed")
        return False
    
    with engine.connect() as conn:
        # Query to list all views in the database
        query = text("""
            SELECT table_name 
            FROM information_schema.views 
            WHERE table_schema = 'public'
            ORDER BY table_name
        """)
        
        result = conn.execute(query)
        views = [row[0] for row in result]
        
        if not views:
            print("No views found in the database")
            return
        
        print(f"Found {len(views)} views in the database:")
        for view in views:
            print(f"  - {view}")
        
        # Specifically check for our full day views
        full_day_views = [v for v in views if 'full_day_data' in v]
        print(f"\nFull day views ({len(full_day_views)}):")
        for view in full_day_views:
            # Get row count
            count_query = text(f"SELECT COUNT(*) FROM {view}")
            count = conn.execute(count_query).scalar()
            print(f"  - {view}: {count} rows")
            
            # Get sample data
            sample_query = text(f"SELECT * FROM {view} LIMIT 1")
            sample = conn.execute(sample_query).fetchone()
            if sample:
                print(f"    Sample data: {sample}")
            else:
                print(f"    No data found")

if __name__ == "__main__":
    list_all_views()
