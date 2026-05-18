"""Simple test to verify imports work"""

import sys
from pathlib import Path

# Setup paths
project_root = Path(__file__).parent.parent
prefect_folder = Path(__file__).parent
flows_path = prefect_folder / "flows"

# Add project root
sys.path.insert(0, str(project_root))

# Remove prefect folder if it's in path (to avoid conflict)
if str(prefect_folder) in sys.path:
    sys.path.remove(str(prefect_folder))

# Add flows folder
sys.path.insert(0, str(flows_path))

print("Testing imports...")
print(f"Project root: {project_root}")
print(f"Prefect folder: {prefect_folder}")
print(f"Flows path: {flows_path}")
print()

# Test Prefect import
try:
    import prefect
    print(f"✅ Prefect: {prefect.__version__}")
except Exception as e:
    print(f"❌ Prefect: {e}")
    sys.exit(1)

# Test ConcurrentTaskRunner
try:
    from prefect.task_runners import ConcurrentTaskRunner
    print("✅ ConcurrentTaskRunner: OK")
except Exception as e:
    print(f"❌ ConcurrentTaskRunner: {e}")
    sys.exit(1)

# Test flow import
try:
    from mmsr_daily_pipeline import mmsr_daily_pipeline
    print("✅ Flow: OK")
    print("✅ All imports successful!")
except Exception as e:
    print(f"❌ Flow: {e}")
    import traceback
    traceback.print_exc()
    sys.exit(1)
