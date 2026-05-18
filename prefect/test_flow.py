"""
Quick test script for MMSR daily pipeline.
Run this to test the flow before production deployment.
"""

import sys
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

# Import from flows directly to avoid prefect folder name conflict
import sys
from pathlib import Path

project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

# Remove local prefect folder from path temporarily to avoid import conflict
_prefect_path = str(Path(__file__).parent)
if _prefect_path in sys.path:
    sys.path.remove(_prefect_path)

# Import flows module directly
import importlib.util
flows_path = Path(__file__).parent / "flows" / "mmsr_daily_pipeline.py"
spec = importlib.util.spec_from_file_location("mmsr_daily_pipeline", flows_path)
mmsr_module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(mmsr_module)
mmsr_daily_pipeline = mmsr_module.mmsr_daily_pipeline

if __name__ == "__main__":
    print("=" * 60)
    print("MMSR Daily Pipeline - Test Run")
    print("=" * 60)
    print()
    
    try:
        mmsr_daily_pipeline()
        print()
        print("=" * 60)
        print("✅ Flow completed successfully!")
        print("=" * 60)
    except Exception as e:
        print()
        print("=" * 60)
        print(f"❌ Flow failed: {e}")
        print("=" * 60)
        import traceback
        traceback.print_exc()
        sys.exit(1)

