# Fix: Missing PRIMARY KEY Constraints in Raw Schema

## Problem
The ingestion scripts were failing with error:
```
(psycopg2.errors.InvalidColumnReference) there is no unique or exclusion constraint matching the ON CONFLICT specification
```

This occurred because **all 6 tables** in the `raw` schema were migrated from the `public` schema, but the PRIMARY KEY constraints were not properly migrated, causing `ON CONFLICT` clauses in the INSERT statements to fail.

**Affected Tables:**
1. `raw.isolarcloud_power_stations` - needs PRIMARY KEY on `ps_id`
2. `raw.isolarcloud_devices` - needs PRIMARY KEY on `device_ps_key`
3. `raw.isolarcloud_historical_data` - needs PRIMARY KEY on `(device_ps_key, timestamp)`
4. `raw.fusionsolar_plants` - needs PRIMARY KEY on `plant_code`
5. `raw.fusionsolar_devices` - needs PRIMARY KEY on `dev_id`
6. `raw.fusionsolar_historical_data` - needs PRIMARY KEY on `(dev_id, collect_time)`

## Root Cause
When tables are moved between schemas using `ALTER TABLE ... SET SCHEMA`, constraints should be preserved. However, if tables were created in `raw` schema without constraints, or if constraints were lost during migration, the `ON CONFLICT` clause cannot work because it requires a unique constraint or primary key.

## Solution

### Option 1: Run SQL Script (Immediate Fix)
Run the SQL script to add missing constraints:
```bash
psql -U postgres -d MMSR -f fix_raw_schema_constraints.sql
```

This script will:
- Check if PRIMARY KEY constraints exist on both tables
- Add them if they don't exist
- Verify the constraints were added

### Option 2: Re-run Database Initialization (Automatic Fix)
The `init_database()` functions in both harvester modules have been updated to automatically check and add PRIMARY KEY constraints if they don't exist. Simply re-run the database initialization:

**For iSolarCloud:**
```python
from isolarcloud_harvester_src.isolar_db_operations import init_database
init_database()
```

**For FusionSolar:**
```python
from fusionsolar_harvester_src.fusionsolar_db_operations import init_database
init_database()
```

Or simply run your ETL scripts - they will call `init_database()` automatically and fix the constraints.

## Changes Made

### 1. SQL Script: `fix_raw_schema_constraints.sql`
- Adds PRIMARY KEY constraints to **all 6 tables** in raw schema
- Safe to run multiple times (checks if constraint exists first)
- Handles: power_stations, devices, and historical_data for both iSolarCloud and FusionSolar

### 2. Updated `isolarcloud/isolarcloud_harvester_src/isolar_db_operations.py`
- Modified `init_database()` to check and add PRIMARY KEY constraints if missing
- Ensures constraints exist for:
  - `isolarcloud_power_stations` (ps_id)
  - `isolarcloud_devices` (device_ps_key)
  - `isolarcloud_historical_data` (device_ps_key, timestamp)
- Works for both new and existing tables

### 3. Updated `fusionsolar/fusionsolar_harvester_src/fusionsolar_db_operations.py`
- Modified `init_database()` to check and add PRIMARY KEY constraints if missing
- Ensures constraints exist for:
  - `fusionsolar_plants` (plant_code)
  - `fusionsolar_devices` (dev_id)
  - `fusionsolar_historical_data` (dev_id, collect_time)
- Works for both new and existing tables

## Verification

After applying the fix, verify constraints exist:
```sql
SELECT 
    schemaname,
    tablename,
    constraintname,
    contype
FROM pg_constraint c
JOIN pg_class cl ON c.conrelid = cl.oid
JOIN pg_namespace n ON cl.relnamespace = n.oid
WHERE n.nspname = 'raw'
  AND cl.relname IN (
      'isolarcloud_power_stations',
      'isolarcloud_devices',
      'isolarcloud_historical_data',
      'fusionsolar_plants',
      'fusionsolar_devices',
      'fusionsolar_historical_data'
  )
  AND c.contype = 'p'
ORDER BY tablename, constraintname;
```

Expected output (all 6 tables should have PRIMARY KEY constraints):
- `raw.isolarcloud_power_stations` → `isolarcloud_power_stations_pkey` on `(ps_id)`
- `raw.isolarcloud_devices` → `isolarcloud_devices_pkey` on `(device_ps_key)`
- `raw.isolarcloud_historical_data` → `isolarcloud_historical_data_pkey` on `(device_ps_key, timestamp)`
- `raw.fusionsolar_plants` → `fusionsolar_plants_pkey` on `(plant_code)`
- `raw.fusionsolar_devices` → `fusionsolar_devices_pkey` on `(dev_id)`
- `raw.fusionsolar_historical_data` → `fusionsolar_historical_data_pkey` on `(dev_id, collect_time)`

## Next Steps

1. **Run the SQL script** or **re-run database initialization** to add constraints
2. **Re-run your ETL scripts** - they should now work without the constraint error
3. **Monitor logs** to confirm data is being ingested successfully

