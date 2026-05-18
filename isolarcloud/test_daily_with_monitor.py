"""
Test daily ingestion with real-time progress monitoring.
Runs ingestion in background and monitors log file.
"""

import subprocess
import sys
import time
import threading
from pathlib import Path
from datetime import datetime

def monitor_log_file(log_file_path, stop_event):
    """Monitor log file and print new lines."""
    last_size = 0
    while not stop_event.is_set():
        if log_file_path and log_file_path.exists():
            try:
                current_size = log_file_path.stat().st_size
                if current_size > last_size:
                    with open(log_file_path, 'r', encoding='utf-8') as f:
                        f.seek(last_size)
                        new_content = f.read()
                        if new_content:
                            print(new_content, end='', flush=True)
                        last_size = current_size
            except Exception:
                pass
        time.sleep(1)

def test_daily_with_monitor():
    """Test daily ingestion with progress monitoring."""
    print("=" * 70)
    print("iSolarCloud Daily Ingestion Test - With Progress Monitoring")
    print("Using: isolarcloud_data_harvester.py --fetch-yesterday")
    print("=" * 70)
    print()
    print("⚡ Expected: ~2-3 minutes (with parallel processing)")
    print("🐌 Old time: ~16 minutes (sequential)")
    print()
    print("Starting daily ingestion...")
    print("-" * 70)
    print()
    
    # Find or wait for log file
    log_dir = Path("logs")
    log_dir.mkdir(exist_ok=True)
    
    # Check for existing log file (from harvester.log)
    harvester_log = Path("isolarcloud_harvester.log")
    
    start_time = time.time()
    stop_event = threading.Event()
    monitor_thread = None
    
    try:
        # Start monitoring if log file exists
        if harvester_log.exists():
            print(f"📄 Monitoring log: {harvester_log.name}")
            print("-" * 70)
            print()
            
            monitor_thread = threading.Thread(
                target=monitor_log_file,
                args=(harvester_log, stop_event),
                daemon=True
            )
            monitor_thread.start()
        
        # Run the harvester script
        print("📥 Running: python isolarcloud_data_harvester.py --fetch-yesterday")
        print("   (Progress will be shown below)")
        print()
        
        process = subprocess.Popen(
            [sys.executable, "isolarcloud_data_harvester.py", "--fetch-yesterday"],
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            cwd=Path(__file__).parent,
            bufsize=1
        )
        
        # Print output in real-time
        for line in process.stdout:
            print(line, end='', flush=True)
        
        process.wait()
        return_code = process.returncode
        
        execution_time = time.time() - start_time
        
        # Stop monitoring
        stop_event.set()
        if monitor_thread:
            monitor_thread.join(timeout=2)
        
        print()
        print("-" * 70)
        print("=" * 70)
        
        if return_code == 0:
            print("✅ Daily ingestion completed successfully!")
            print("=" * 70)
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
        else:
            print("❌ Daily ingestion failed!")
            print("=" * 70)
            print(f"Return code: {return_code}")
            print(f"Execution time before error: {execution_time:.2f} seconds")
        
        print("=" * 70)
        return return_code == 0
        
    except Exception as e:
        execution_time = time.time() - start_time
        
        # Stop monitoring
        stop_event.set()
        if monitor_thread:
            monitor_thread.join(timeout=2)
        
        print()
        print("-" * 70)
        print("=" * 70)
        print("❌ Test failed!")
        print("=" * 70)
        print(f"Error: {str(e)}")
        print(f"Execution time before error: {execution_time:.2f} seconds")
        print()
        import traceback
        traceback.print_exc()
        print("=" * 70)
        return False

if __name__ == "__main__":
    success = test_daily_with_monitor()
    sys.exit(0 if success else 1)

