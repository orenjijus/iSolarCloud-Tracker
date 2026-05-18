"""
Test script for iSolarCloud parallel processing ingestion.
This script tests the new parallel processing implementation.
"""

import sys
import time
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))
sys.path.insert(0, str(project_root / "isolarcloud"))

# Import and initialize database first
from isolarcloud_harvester_src.isolar_db_operations import init_database, engine
from isolarcloud.tasks import run_isolarcloud_ingest

# Ensure database is initialized before importing tasks
if not engine:
    print("Initializing database...")
    init_database()
    # Re-import engine after initialization
    from isolarcloud_harvester_src.isolar_db_operations import engine as engine_after_init
    if not engine_after_init:
        print("ERROR: Database engine still not initialized after init_database()")
        sys.exit(1)

def test_parallel_ingestion():
    """Test iSolarCloud ingestion with parallel processing."""
    print("=" * 60)
    print("Testing iSolarCloud Parallel Processing Ingestion")
    print("=" * 60)
    print()
    
    # Use yesterday's date for testing
    from datetime import datetime, timedelta
    yesterday = datetime.now() - timedelta(days=1)
    test_date = yesterday.strftime('%Y-%m-%d')
    
    print(f"Test date: {test_date}")
    print(f"Expected improvement: ~16 minutes → ~2-3 minutes")
    print()
    print("Starting ingestion...")
    print("-" * 60)
    
    start_time = time.time()
    
    try:
        result = run_isolarcloud_ingest(
            start_date=test_date,
            end_date=test_date
        )
        
        execution_time = time.time() - start_time
        
        print()
        print("-" * 60)
        print("✅ Ingestion completed successfully!")
        print("-" * 60)
        print(f"Status: {result.get('status', 'unknown')}")
        print(f"Rows inserted: {result.get('rows_inserted', 0):,}")
        print(f"Execution time: {execution_time:.2f} seconds ({execution_time/60:.2f} minutes)")
        print()
        
        # Performance comparison
        expected_old_time = 980  # ~16.3 minutes
        speedup = expected_old_time / execution_time if execution_time > 0 else 0
        
        print("Performance Analysis:")
        print(f"  Old time (sequential): ~{expected_old_time} seconds (~{expected_old_time/60:.1f} minutes)")
        print(f"  New time (parallel):   {execution_time:.2f} seconds ({execution_time/60:.2f} minutes)")
        print(f"  Speedup:               {speedup:.2f}x faster")
        print()
        
        if execution_time < 300:  # Less than 5 minutes
            print("✅ SUCCESS: Parallel processing is working! Time is significantly reduced.")
        elif execution_time < 600:  # Less than 10 minutes
            print("⚠️  WARNING: Time is reduced but could be better. Check logs for issues.")
        else:
            print("❌ ISSUE: Time is still high. Parallel processing may not be working correctly.")
            print("   Check logs and verify PARALLEL_PROCESSING_ENABLED is True in config.")
        
        print("=" * 60)
        return result
        
    except Exception as e:
        execution_time = time.time() - start_time
        print()
        print("-" * 60)
        print("❌ Ingestion failed!")
        print("-" * 60)
        print(f"Error: {str(e)}")
        print(f"Execution time before error: {execution_time:.2f} seconds")
        print()
        import traceback
        traceback.print_exc()
        print("=" * 60)
        raise

if __name__ == "__main__":
    test_parallel_ingestion()

