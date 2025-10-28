# PV System Data Architecture

## Overview
This document is a template for documenting the PV System Data Architecture. It will be automatically populated with the actual implementation details during the workflow execution.

## Database Schema

### Tables
- `pv_systems`: Master table for PV system information
- `inverters`: Inverter specifications and metadata
- `sensors`: Sensor data points
- `weather_data`: Weather measurements
- `performance_metrics`: Time-series performance data

### Views
- `v_daily_production`: Daily energy production summary
- `v_system_performance`: System performance metrics
- `v_equipment_health`: Equipment health indicators

## ETL Processes

### Data Ingestion
- Source: [Source Name]
- Frequency: [Frequency]
- Process: [Process Description]

### Data Transformation
- Models: [List of dbt models]
- Schedule: [Schedule]
- Dependencies: [Dependencies]

## API Endpoints

### Data Access
- `GET /api/v1/pv-systems`: List all PV systems
- `GET /api/v1/pv-systems/{id}`: Get PV system details
- `GET /api/v1/pv-systems/{id}/performance`: Get performance data

### Management
- `POST /api/v1/ingest`: Trigger data ingestion
- `GET /api/v1/health`: System health check

## Configuration

### Database
- Host: [Hostname]
- Port: [Port]
- Database: [Database Name]
- Schema: [Schema Name]

### ETL
- dbt Project: [Project Path]
- dbt Profile: [Profile Name]
- Airflow DAG: [DAG ID]

## Maintenance

### Backup
- Schedule: [Schedule]
- Retention: [Retention Period]
- Location: [Backup Location]

### Monitoring
- Metrics: [List of monitored metrics]
- Alerts: [Alert conditions]
- Dashboard: [Dashboard URL]

## Notes
[Additional notes or special instructions]
