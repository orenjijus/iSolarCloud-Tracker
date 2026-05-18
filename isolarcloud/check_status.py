"""
Quick status check for iSolarCloud ingestion.
Shows current status without continuous monitoring.
"""

import os
from pathlib import Path
from datetime import datetime

def get_latest_log_file():
    """Get the most recent log file."""
    log_dir = Path("logs")
    if not log_dir.exists():
        return None
    
    log_files = list(log_dir.glob("isolarcloud_etl_*.log"))
    if not log_files:
        return None
    
    log_files.sort(key=lambda x: x.stat().st_mtime, reverse=True)
    return log_files[0]

def get_status_file():
    """Get status from status file."""
    status_file = Path("logs/isolarcloud_last_etl_status.txt")
    if not status_file.exists():
        return None
    
    status = {}
    with open(status_file, 'r') as f:
        for line in f:
            if ':' in line:
                key, value = line.strip().split(':', 1)
                status[key.strip()] = value.strip()
    
    return status

def check_status():
    """Check and display current status."""
    print("=" * 70)
    print("iSolarCloud Ingestion Status Check")
    print("=" * 70)
    print()
    
    # Check status file
    status = get_status_file()
    if status:
        print("📊 Last Run Status:")
        print("-" * 70)
        for key, value in status.items():
            print(f"  {key:20s}: {value}")
        print()
    else:
        print("⚠️  No status file found (process may not have started yet)")
        print()
    
    # Check log file
    log_file = get_latest_log_file()
    if log_file:
        file_time = datetime.fromtimestamp(log_file.stat().st_mtime)
        file_size = log_file.stat().st_size
        
        print("📄 Latest Log File:")
        print("-" * 70)
        print(f"  File: {log_file.name}")
        print(f"  Size: {file_size:,} bytes")
        print(f"  Modified: {file_time.strftime('%Y-%m-%d %H:%M:%S')}")
        print()
        
        # Show last 15 lines
        try:
            with open(log_file, 'r', encoding='utf-8') as f:
                lines = f.readlines()
                print("📋 Last 15 lines from log:")
                print("-" * 70)
                for line in lines[-15:]:
                    print(f"  {line.rstrip()}")
        except Exception as e:
            print(f"  Error reading log: {e}")
    else:
        print("⚠️  No log file found")
        print()
    
    print("=" * 70)
    print()
    print("💡 Tips:")
    print("  - Run 'python monitor_progress.py' for real-time monitoring")
    print("  - Check logs/ directory for all log files")
    print("  - Status file: logs/isolarcloud_last_etl_status.txt")

if __name__ == "__main__":
    check_status()

