{% macro auto_seed_dynamic_seeds() %}
  {#
    Macro untuk auto-update dynamic seeds sebelum dbt run
    Dynamic seeds yang perlu di-update:
    - seed_daily_simulation_target
    - seed_daily_kpi_monthly
    - seed_energy_adjustment_daily
    - seed_ghi_adjustment_daily
    
    Static seeds yang TIDAK perlu di-update:
    - seed_metric_mapper (hanya update jika ada perubahan sistem)
  #}
  
  {% set dynamic_seeds = [
    'seed_daily_simulation_target',
    'seed_daily_kpi_monthly',
    'seed_energy_adjustment_daily',
    'seed_ghi_adjustment_daily'
  ] %}
  
  {% for seed in dynamic_seeds %}
    {% do log("Auto-seeding: " ~ seed, info=true) %}
    {% do run_query("SELECT 1") %}  -- Placeholder, actual seed command via run_command
  {% endfor %}
  
{% endmacro %}

