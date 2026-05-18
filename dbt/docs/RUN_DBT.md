# Running dbt with Separate Schemas

## Step 1: Create the Schemas
Run the SQL as superuser (postgres user):

```sql
-- Execute SETUP_SCHEMAS.sql
\i SETUP_SCHEMAS.sql
```

Or connect as postgres and run:
```bash
psql -h 10.101.4.88 -U postgres -d MMSR -f SETUP_SCHEMAS.sql
```

## Step 2: Verify Schemas Created
```sql
SELECT schema_name 
FROM information_schema.schemata 
WHERE schema_name LIKE 'dbt_%';
```

You should see:
- dbt_staging
- dbt_intermediate
- dbt_dimensions
- dbt_marts

## Step 3: Run dbt Transformations

### Load seed (already done ✅)
```bash
dbt seed
```

### Build staging layer
```bash
dbt run --select staging
```

### Build dimensions
```bash
dbt run --select dimensions
```

### Build intermediate
```bash
dbt run --select intermediate
```

### Build marts (final tables for PowerBI)
```bash
dbt run --select marts
```

### Or run everything at once
```bash
dbt run
```

## Step 4: Check Results
```sql
-- Check staging
SELECT COUNT(*) FROM dbt_staging.stg_isolarcloud__perf_unpivoted;

-- Check dimensions
SELECT COUNT(*) FROM dbt_dimensions.dim_assets;

-- Check marts
SELECT COUNT(*) FROM dbt_marts.mart_inverter_performance_5min;
```

## Benefits of This Approach

✅ **Raw data untouched**: Your existing tables in public schema remain unchanged
✅ **Clean organization**: Each layer in separate schema
✅ **Easy permission management**: Only grant to specific schemas
✅ **Easy rollback**: Just drop dbt_* schemas if needed
✅ **PowerBI ready**: Connect to dbt_marts schema for reports

## Schema Layout

```
public schema (existing)
├── isolarcloud_historical_data (untouched)
├── isolarcloud_devices (untouched)
├── fusionsolar_historical_data (untouched)
└── ... existing tables

dbt_staging (new)
├── stg_isolarcloud__perf_unpivoted
├── stg_fusionsolar__perf_unpivoted
└── ... staging views

dbt_dimensions (new)
├── dim_assets
└── dim_date_generated

dbt_intermediate (new)
├── int_inverters_unified_5min
└── ... unified models

dbt_marts (new) ← PowerBI connects here
├── mart_inverter_performance_5min
├── mart_string_performance_5min
└── ... final fact tables
```

## Troubleshooting

If you get permission errors:
```sql
-- Check if juice has access
SELECT grantee, privilege_type 
FROM information_schema.role_table_grants 
WHERE table_schema LIKE 'dbt_%'
AND grantee = 'juice';
```

If schemas don't exist:
```sql
-- List all schemas
\dn
```

