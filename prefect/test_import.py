"""Test script to check Prefect imports"""

import sys
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

# Remove local prefect folder from path to avoid import conflict with prefect package
_prefect_folder = Path(__file__).parent
_prefect_folder_str = str(_prefect_folder)
if _prefect_folder_str in sys.path:
    sys.path.remove(_prefect_folder_str)

print("Testing Prefect imports...")
print(f"Python version: {sys.version}")
print()

try:
    import prefect
    print(f"✅ Prefect imported. Version: {prefect.__version__}")
except Exception as e:
    print(f"❌ Failed to import prefect: {e}")
    sys.exit(1)

print()
print("Testing ConcurrentTaskRunner import...")
try:
    from prefect.task_runners import ConcurrentTaskRunner
    print("✅ ConcurrentTaskRunner imported from prefect.task_runners")
except ImportError as e:
    print(f"❌ Failed to import from prefect.task_runners: {e}")
    try:
        from prefect import task_runners
        ConcurrentTaskRunner = task_runners.ConcurrentTaskRunner
        print("✅ ConcurrentTaskRunner imported from prefect.task_runners (alternative)")
    except Exception as e2:
        print(f"❌ Alternative import also failed: {e2}")
        print("⚠️  Will use default task runner")
        ConcurrentTaskRunner = None

print()
print("Testing flow import...")
try:
    # Import from local flows folder, not from prefect package
    # Need to add flows folder to path
    flows_path = Path(__file__).parent / "flows"
    sys.path.insert(0, str(flows_path))
    from mmsr_daily_pipeline import mmsr_daily_pipeline
    print("✅ Flow imported successfully!")
except Exception as e:
    print(f"❌ Failed to import flow: {e}")
    import traceback
    traceback.print_exc()
    sys.exit(1)

print()
print("All imports successful!")
