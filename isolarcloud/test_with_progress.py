"""
Test iSolarCloud ingestion with progress monitoring.
This script runs ingestion in background and shows progress.
"""

import sys
import time
import subprocess
import threading
from pathlib import Path
from datetime import datetime, timedelta

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))
sys.path.insert(0, str(project_root / "isolarcloud"))

def monitor_log(log_file, stop_event):
    """Monitor log file and print new lines."""
    last_size = 0
    while not stop_event.is_set():
        if log_file and log_file.exists():
            current_size = log_file.stat().st_size
            if current_size > last_size:
                try:
                    with open(log_file, 'r', encoding='utf-8') as f:
                        f.seek(last_size)
                        new_content = f.read()
                        if new_content:
                            print(new_content, end='', flush=True)
                        last_size = current_size
                except Exception:
                    pass
        time.sleep(1)

def test_with_progress():
    """Test ingestion with progress monitoring."""
    print("=" * 70)
    print("iSolarCloud Parallel Processing Test - With Progress Monitoring")
    print("=" * 70)
    print()
    
    # Use yesterday's date for testing
    yesterday = datetime.now() - timedelta(days=1)
    test_date = yesterday.strftime('%Y-%m-%d')
    
    print(f"📅 Test date: {test_date}")
    print(f"⚡ Expected: ~2-3 minutes (with parallel processing)")
    print(f"🐌 Old time: ~16 minutes (sequential)")
    print()
    print("Starting ingestion...")
    print("-" * 70)
    print()
    
    # Find log file that will be created
    log_dir = Path("logs")
    log_dir.mkdir(exist_ok=True)
    
    # Start monitoring in background
    stop_event = threading.Event()
    log_file = None
    monitor_thread = None
    
    start_time = time.time()
    
    try:
        # Import and run ingestion
        from isolarcloud_harvester_src.isolar_db_operations import init_database, engine
        from isolarcloud_harvester_src.isolar_api_client import login_isolarcloud
        from isolarcloud_harvester_src.isolar_data_processing import fetch_historical_data
        
        # Initialize database
        print("🔧 Initializing database...")
        if not init_database():
            raise Exception("Failed to initialize database")
        if not engine:
            raise Exception("Database engine not initialized")
        print("✅ Database initialized")
        print()
        
        # Login
        print("🔐 Logging in to iSolarCloud API...")
        if not login_isolarcloud():
            raise Exception("Failed to login to iSolarCloud API")
        print("✅ Login successful")
        print()
        
        # Find log file (should be created by fetch_historical_data)
        time.sleep(1)  # Wait a bit for log file to be created
        log_files = list(log_dir.glob("isolarcloud_etl_*.log"))
        if log_files:
            log_files.sort(key=lambda x: x.stat().st_mtime, reverse=True)
            log_file = log_files[0]
            print(f"📄 Monitoring log: {log_file.name}")
            print("-" * 70)
            print()
            
            # Start monitoring thread
            monitor_thread = threading.Thread(
                target=monitor_log,
                args=(log_file, stop_event),
                daemon=True
            )
            monitor_thread.start()
        
        # Fetch data
        print("📥 Fetching historical data...")
        print("   (Progress will be shown below)")
        print()
        
        rows_inserted = fetch_historical_data(
            start_date_str=test_date,
            end_date_str=test_date,
            ps_ids_str=None,
            device_types_str="meter,inverter,meteo_station"
        )
        
        execution_time = time.time() - start_time
        
        # Stop monitoring
        stop_event.set()
        if monitor_thread:
            monitor_thread.join(timeout=2)
        
        print()
        print("-" * 70)
        print("=" * 70)
        print("✅ Ingestion completed successfully!")
        print("=" * 70)
        print(f"📊 Rows inserted: {rows_inserted:,}")
        print(f"⏱️  Execution time: {execution_time:.2f} seconds ({execution_time/60:.2f} minutes)")
        print()
        
        # Performance analysis
        expected_old_time = 980  # ~16.3 minutes
        speedup = expected_old_time / execution_time if execution_time > 0 else 0
        
        print("📈 Performance Analysis:")
        print("-" * 70)
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
        
        print("=" * 70)
        return {
            "status": "success",
            "rows_inserted": rows_inserted,
            "execution_time": execution_time
        }
        
    except Exception as e:
        execution_time = time.time() - start_time
        
        # Stop monitoring
        stop_event.set()
        if monitor_thread:
            monitor_thread.join(timeout=2)
        
        print()
        print("-" * 70)
        print("=" * 70)
        print("❌ Ingestion failed!")
        print("=" * 70)
        print(f"Error: {str(e)}")
        print(f"Execution time before error: {execution_time:.2f} seconds")
        print()
        import traceback
        traceback.print_exc()
        print("=" * 70)
        raise

if __name__ == "__main__":
    test_with_progress()

