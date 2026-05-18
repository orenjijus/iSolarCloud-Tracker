# Panduan: Auto-Update Seeds saat dbt run

## 🎯 Masalah

**dbt run** dan **dbt seed** adalah command yang **terpisah**. Seeds tidak otomatis di-update saat `dbt run`.

### Seeds yang Perlu Update Berkala

1. **Dynamic Seeds** (perlu update berkala):
   - `seed_daily_simulation_target` - Update bulanan/rekalkulasi
   - `seed_daily_kpi_monthly` - Update bulanan/rekalkulasi
   - `seed_energy_adjustment_daily` - Update saat rekalkulasi
   - `seed_ghi_adjustment_daily` - Update saat rekalkulasi

2. **Static Seeds** (tidak perlu update, kecuali ada perubahan):
   - `seed_metric_mapper` - Hanya update jika ada perubahan sistem
   - `seed_sensor_config` - Hanya update jika ada perubahan config
   - `seed_meter_config` - Hanya update jika ada perubahan config
   - `seed_site_config` - Hanya update jika ada perubahan config
   - `seed_sensor_site_mapping` - Hanya update jika ada perubahan mapping
   - `seed_inverter_site_mapping` - Hanya update jika ada perubahan mapping
   - `seed_issue_dates` - Hanya update jika ada issue baru

## ✅ Solusi: Auto-Update Dynamic Seeds

### Opsi 1: Menggunakan Script (Recommended)

#### Windows (PowerShell)

```powershell
# Gunakan script yang sudah dibuat
cd dbt
.\scripts\run_dbt_with_seeds.ps1

# Atau dengan custom command
.\scripts\run_dbt_with_seeds.ps1 run --select staging+
```

#### Linux/Mac (Bash)

```bash
# Gunakan script yang sudah dibuat
cd dbt
chmod +x scripts/run_dbt_with_seeds.sh
./scripts/run_dbt_with_seeds.sh

# Atau dengan custom command
./scripts/run_dbt_with_seeds.sh run --select staging+
```

### Opsi 2: Manual Command Sequence

```bash
# Update dynamic seeds dulu
dbt seed --select seed_daily_simulation_target
dbt seed --select seed_daily_kpi_monthly
dbt seed --select seed_energy_adjustment_daily
dbt seed --select seed_ghi_adjustment_daily

# Lalu run dbt
dbt run
```

### Opsi 3: Menggunakan dbt Hooks (Advanced)

Tambahkan hook di `dbt_project.yml`:

```yaml
on-run-start:
  - "{{ log('Auto-updating dynamic seeds...', info=true) }}"
  - "dbt seed --select seed_daily_simulation_target"
  - "dbt seed --select seed_daily_kpi_monthly"
  - "dbt seed --select seed_energy_adjustment_daily"
  - "dbt seed --select seed_ghi_adjustment_daily"
```

**⚠️ Catatan:** Hooks di dbt tidak bisa menjalankan command dbt lain secara langsung. Gunakan script atau macro sebagai alternatif.

### Opsi 4: Menggunakan Python Script

Buat script Python untuk automate:

```python
# scripts/run_dbt_with_seeds.py
import subprocess
import sys

dynamic_seeds = [
    "seed_daily_simulation_target",
    "seed_daily_kpi_monthly",
    "seed_energy_adjustment_daily",
    "seed_ghi_adjustment_daily"
]

print("🔄 Auto-updating dynamic seeds...")
for seed in dynamic_seeds:
    print(f"  → Updating {seed}...")
    result = subprocess.run(["dbt", "seed", "--select", seed])
    if result.returncode != 0:
        print(f"  ❌ Failed to update {seed}")
        sys.exit(1)

print("✅ Dynamic seeds updated successfully")
print("")

# Run dbt command
dbt_command = sys.argv[1] if len(sys.argv) > 1 else "run"
dbt_args = sys.argv[2:] if len(sys.argv) > 2 else []

print(f"🚀 Running dbt {dbt_command}...")
subprocess.run(["dbt", dbt_command] + dbt_args)
```

Usage:
```bash
python scripts/run_dbt_with_seeds.py run
python scripts/run_dbt_with_seeds.py run --select staging+
```

## 📋 Workflow yang Disarankan

### Daily Run Normal

```bash
# Option 1: Menggunakan script (Recommended)
cd dbt
.\scripts\run_dbt_with_seeds.ps1  # Windows
# atau
./scripts/run_dbt_with_seeds.sh   # Linux/Mac

# Option 2: Manual
dbt seed --select seed_daily_simulation_target seed_daily_kpi_monthly
dbt run
```

### Rekalkulasi/Adjustment

```bash
# Update adjustment seeds
dbt seed --select seed_energy_adjustment_daily seed_ghi_adjustment_daily

# Re-run dengan vars
dbt run --select mart_site_performance_daily+ \
  --vars '{"reingest_start_date": "2025-12-01", "reingest_end_date": "2025-12-31"}'
```

### Update Static Seeds (Jarang)

```bash
# Hanya jika ada perubahan config
dbt seed --select seed_metric_mapper
dbt seed --select seed_sensor_config
# ... dll
```

## 🔍 Verifikasi Seeds Ter-update

```sql
-- Cek last update time (jika ada updated_at column)
SELECT 
    table_name,
    MAX(updated_at) as last_updated
FROM information_schema.tables t
JOIN staging.seed_daily_simulation_target s ON 1=1
WHERE table_schema = 'staging'
    AND table_name LIKE 'seed_%'
GROUP BY table_name;

-- Atau cek row count
SELECT 
    'seed_daily_simulation_target' as seed_name,
    COUNT(*) as row_count
FROM staging.seed_daily_simulation_target
UNION ALL
SELECT 
    'seed_daily_kpi_monthly',
    COUNT(*)
FROM staging.seed_daily_kpi_monthly;
```

## 📝 Checklist

- [ ] Dynamic seeds di-update sebelum `dbt run`
- [ ] Static seeds hanya di-update jika ada perubahan
- [ ] Verifikasi seeds ter-update dengan benar
- [ ] Dokumentasikan perubahan seeds jika ada

## 🎯 Kesimpulan

1. **dbt run** dan **dbt seed** memang terpisah
2. **Dynamic seeds** perlu di-update sebelum `dbt run`
3. **Static seeds** hanya di-update jika ada perubahan
4. Gunakan **script** untuk automate update dynamic seeds
5. **seed_metric_mapper** tidak perlu update (static, hanya jika ada perubahan sistem)

## 📚 Related Documentation

- [SEED_MANAGEMENT.md](./SEED_MANAGEMENT.md) - Panduan lengkap manajemen seeds
- [DEPENDENCY_GRAPH_EXPLANATION.md](./DEPENDENCY_GRAPH_EXPLANATION.md) - Dependency graph dan execution order

