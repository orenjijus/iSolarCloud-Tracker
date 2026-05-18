"""
Monitor progress of iSolarCloud ingestion in real-time.
This script watches the log file and shows progress updates.
"""

import os
import time
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
    
    # Sort by modification time, newest first
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

def tail_log_file(log_file, lines=20):
    """Get last N lines from log file."""
    if not log_file or not log_file.exists():
        return []
    
    try:
        with open(log_file, 'r', encoding='utf-8') as f:
            all_lines = f.readlines()
            return all_lines[-lines:] if len(all_lines) > lines else all_lines
    except Exception as e:
        return [f"Error reading log: {e}\n"]

def monitor_progress(interval=5):
    """Monitor progress by watching log file."""
    print("=" * 70)
    print("iSolarCloud Ingestion Progress Monitor")
    print("=" * 70)
    print()
    print("Press Ctrl+C to stop monitoring")
    print()
    
    last_size = 0
    last_lines = []
    
    try:
        while True:
            log_file = get_latest_log_file()
            
            if log_file:
                current_size = log_file.stat().st_size
                
                # If file size changed, read new lines
                if current_size != last_size:
                    new_lines = tail_log_file(log_file, lines=30)
                    
                    # Show only new lines
                    if last_lines:
                        last_line_count = len(last_lines)
                        if len(new_lines) > last_line_count:
                            for line in new_lines[last_line_count:]:
                                print(line.rstrip())
                    else:
                        # First time, show last 20 lines
                        print(f"\n📄 Log file: {log_file.name}")
                        print("-" * 70)
                        for line in new_lines[-20:]:
                            print(line.rstrip())
                    
                    last_lines = new_lines
                    last_size = current_size
                else:
                    # No new content, show status
                    status = get_status_file()
                    if status:
                        print(f"\r⏳ Waiting... Last status: {status.get('status', 'UNKNOWN')} | "
                              f"Time: {status.get('execution_time', 'N/A')}s | "
                              f"Date: {status.get('start_date', 'N/A')}", end='', flush=True)
            else:
                print(f"\r⏳ Waiting for log file to be created...", end='', flush=True)
            
            time.sleep(interval)
            
    except KeyboardInterrupt:
        print("\n\n" + "=" * 70)
        print("Monitoring stopped by user")
        print("=" * 70)
        
        # Show final status
        status = get_status_file()
        if status:
            print("\n📊 Final Status:")
            print("-" * 70)
            for key, value in status.items():
                print(f"  {key}: {value}")
        
        log_file = get_latest_log_file()
        if log_file:
            print(f"\n📄 Latest log: {log_file}")
            print("-" * 70)
            print("Last 10 lines:")
            for line in tail_log_file(log_file, lines=10):
                print(f"  {line.rstrip()}")

if __name__ == "__main__":
    monitor_progress(interval=3)

