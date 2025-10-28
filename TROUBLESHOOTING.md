# Troubleshooting dbt Connection Issues

## Issue: UTF-8 Encoding Error

Error: `'utf-8' codec can't decode byte 0xab in position 109: invalid start byte`

### Root Cause
This error occurs when dbt tries to connect to PostgreSQL and encounters non-UTF-8 characters in the database metadata or data.

### Solutions

#### Option 1: Set Environment Variable
```powershell
# In PowerShell
$env:PGCLIENTENCODING = "UTF8"
$env:POSTGRES_PASSWORD = "your_password"
dbt debug
```

#### Option 2: Update profiles.yml
```yaml
mmsr_solar:
  outputs:
    dev:
      type: postgres
      host: 10.101.4.88
      user: juice
      password: "your_password_here"  # Use actual password first to test
      port: 5432
      dbname: MMSR
      schema: public
      threads: 1
      
  target: dev
```

#### Option 3: Check Database Encoding
Connect directly to database and check:
```sql
SHOW client_encoding;
SHOW server_encoding;

-- Fix encoding issues
ALTER DATABASE MMSR SET client_encoding = 'UTF8';
```

#### Option 4: Use Connection String Instead
Update `profiles.yml`:
```yaml
mmsr_solar:
  outputs:
    dev:
      type: postgres
      dbhost: 10.101.4.88
      dbport: 5432
      dbuser: juice
      dbname: MMSR
      dbpassword: "{{ env_var('POSTGRES_PASSWORD') }}"
      schema: public
      threads: 1
      # Alternative: use full connection string
      # connection_string: "postgresql://juice:PASSWORD@10.101.4.88:5432/MMSR"
      
  target: dev
```

### Quick Test Without dbt

Test the connection directly:
```bash
psql -h 10.101.4.88 -p 5432 -U juice -d MMSR
# or with password
PGPASSWORD=your_password psql -h 10.101.4.88 -p 5432 -U juice -d MMSR
```

### Workaround: Run Without Seeds

If seeds are causing issues, you can:
1. Manually insert data into the table
2. Comment out seed references in models
3. Use staging models that read from existing tables

```bash
# Run only staging models (skip seeds)
dbt run --select staging
```

