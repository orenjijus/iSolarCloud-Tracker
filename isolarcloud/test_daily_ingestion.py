"""
Test daily ingestion using isolarcloud_data_harvester.py
This is the correct script for daily ingestion (not fetch_historical_device_data.py)
"""

import subprocess
import sys
import time
from pathlib import Path
from datetime import datetime

def test_daily_ingestion():
    """Test daily ingestion with progress monitoring."""
    print("=" * 70)
    print("iSolarCloud Daily Ingestion Test")
    print("Using: isolarcloud_data_harvester.py --fetch-yesterday")
    print("=" * 70)
    print()
    print("⚡ Expected: ~2-3 minutes (with parallel processing)")
    print("🐌 Old time: ~16 minutes (sequential)")
    print()
    print("Starting daily ingestion...")
    print("-" * 70)
    print()
    
    start_time = time.time()
    
    try:
        # Run the harvester script
        result = subprocess.run(
            [sys.executable, "isolarcloud_data_harvester.py", "--fetch-yesterday"],
            capture_output=False,  # Show output in real-time
            text=True,
            cwd=Path(__file__).parent
        )
        
        execution_time = time.time() - start_time
        
        print()
        print("-" * 70)
        print("=" * 70)
        
        if result.returncode == 0:
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
            print(f"Return code: {result.returncode}")
            print(f"Execution time before error: {execution_time:.2f} seconds")
        
        print("=" * 70)
        return result.returncode == 0
        
    except Exception as e:
        execution_time = time.time() - start_time
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
    success = test_daily_ingestion()
    sys.exit(0 if success else 1)

