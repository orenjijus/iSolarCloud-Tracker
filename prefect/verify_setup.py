"""
Script untuk verifikasi setup Prefect sebelum menjalankan flow.
"""

import os
import sys
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

def check_prefect():
    """Check Prefect installation"""
    try:
        import prefect
        print(f"✅ Prefect version: {prefect.__version__}")
        return True
    except ImportError as e:
        print(f"❌ Prefect not installed: {e}")
        return False

def check_dependencies():
    """Check required dependencies"""
    deps = ['psycopg2', 'pendulum', 'requests', 'sqlalchemy']
    all_ok = True
    for dep in deps:
        try:
            __import__(dep)
            print(f"✅ {dep}: OK")
        except ImportError:
            print(f"❌ {dep}: NOT INSTALLED")
            all_ok = False
    return all_ok

def check_env_vars():
    """Check environment variables"""
    required_vars = [
        'POSTGRES_HOST',
        'POSTGRES_PORT',
        'POSTGRES_DB',
        'POSTGRES_USER',
        'POSTGRES_PASSWORD',
        'FUSIONSOLAR_USERNAME',
        'FUSIONSOLAR_PASSWORD',
        'ISOLARCLOUD_APP_KEY',
        'ISOLARCLOUD_SECRET_KEY',
        'ISOLARCLOUD_USERNAME',
        'ISOLARCLOUD_PASSWORD'
    ]
    
    # Load from .env files
    from dotenv import load_dotenv
    load_dotenv()
    
    all_ok = True
    for var in required_vars:
        value = os.getenv(var)
        if value:
            # Mask password values
            if 'PASSWORD' in var or 'SECRET' in var or 'KEY' in var:
                print(f"✅ {var}: {'*' * min(len(value), 10)}")
            else:
                print(f"✅ {var}: {value}")
        else:
            print(f"❌ {var}: NOT SET")
            all_ok = False
    return all_ok

def check_flow_import():
    """Check if flow can be imported"""
    try:
        from prefect.flows.mmsr_daily_pipeline import mmsr_daily_pipeline
        print("✅ Flow import: OK")
        return True
    except Exception as e:
        print(f"❌ Flow import failed: {e}")
        return False

def check_dbt():
    """Check if dbt is available"""
    import subprocess
    try:
        result = subprocess.run(
            ['dbt', '--version'],
            capture_output=True,
            text=True,
            timeout=5
        )
        if result.returncode == 0:
            version_line = result.stdout.split('\n')[0]
            print(f"✅ dbt: {version_line}")
            return True
        else:
            print("❌ dbt: Command failed")
            return False
    except FileNotFoundError:
        print("❌ dbt: NOT FOUND (install with: pip install dbt-postgres)")
        return False
    except Exception as e:
        print(f"❌ dbt: Error checking - {e}")
        return False

def main():
    print("=" * 60)
    print("Prefect Setup Verification")
    print("=" * 60)
    print()
    
    results = []
    
    print("1. Checking Prefect installation...")
    results.append(check_prefect())
    print()
    
    print("2. Checking dependencies...")
    results.append(check_dependencies())
    print()
    
    print("3. Checking environment variables...")
    results.append(check_env_vars())
    print()
    
    print("4. Checking flow import...")
    results.append(check_flow_import())
    print()
    
    print("5. Checking dbt installation...")
    results.append(check_dbt())
    print()
    
    print("=" * 60)
    if all(results):
        print("✅ All checks passed! Ready to run Prefect.")
        return 0
    else:
        print("❌ Some checks failed. Please fix the issues above.")
        return 1

if __name__ == "__main__":
    sys.exit(main())

