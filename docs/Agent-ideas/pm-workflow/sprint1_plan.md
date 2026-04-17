# Sprint 1: Foundation & MVP Setup - Project Plan

## Sprint Goal
Establish the foundation for the solar analytics platform and deliver the first working MVP focusing on iSolarCloud data integration and basic KPI calculation.

## Key Deliverables
1. Monorepo setup with proper structure
2. Basic dbt project configuration
3. Airflow pipeline for data extraction and transformation
4. Initial data model for site performance KPI
5. Documentation and testing framework

## Sprint Backlog

### 1. Monorepo Setup (Day 1-2)
- [ ] Create base project structure
  - `/solar_analytics_project`
  - `/airflow`
  - `/dbt_project`
  - `/scripts`
- [ ] Set up version control (Git)
- [ ] Create initial README with project overview
- [ ] Set up Python virtual environment

### 2. dbt Project Initialization (Day 2-3)
- [ ] Initialize dbt project
  ```bash
  dbt init dbt_project
  ```
- [ ] Configure `profiles.yml` for database connection
- [ ] Set up initial folder structure in `dbt_project`
  ```
  dbt_project/
  ├── models/
  │   ├── staging/
  │   ├── marts/
  │   └── sources.yml
  └── seeds/
  ```
- [ ] Create `sources.yml` for raw tables

### 3. Data Extraction (Day 3-4)
- [ ] Refactor iSolarCloud extractor script
  - Move to `/scripts/extract/isolarcloud_extractor.py`
  - Modify to load data to raw tables only
  - Implement basic error handling and logging
- [ ] Create raw table DDLs:
  - `raw_isolarcloud_performance`
  - `raw_isolarcloud_metadata`

### 4. Data Transformation (Day 4-7)
- [ ] Create seed files:
  - `seed_metric_mapper.csv`
  - `seed_meter_config_override.csv`
  - `seed_sensor_config_override.csv`
  - `seed_string_config.csv`
- [ ] Develop staging models:
  - `stg_isolarcloud__perf_unpivoted.sql`
  - `stg_isolarcloud__sites.sql`
  - `stg_isolarcloud__devices.sql`
- [ ] Create dimension models:
  - `dim_assets.sql`
  - `dim_date.sql`
- [ ] Build intermediate models:
  - `int_meters_unified.sql`
  - `int_sensors_unified.sql`
- [ ] Develop mart model:
  - `mart_site_performance_daily.sql`

### 5. Airflow Pipeline (Day 7-8)
- [ ] Set up Airflow environment
- [ ] Create DAG file `dag_solar_pipeline.py` with tasks:
  - Extract: Run `isolarcloud_extractor.py`
  - Seed: Run `dbt seed`
  - Transform: Run `dbt run`
  - Test: Run `dbt test`
- [ ] Configure task dependencies
- [ ] Set up error handling and notifications

### 6. Testing & Documentation (Day 9-10)
- [ ] Write unit tests for Python scripts
- [ ] Create dbt tests for data quality
- [ ] Document data flow and transformations
- [ ] Create runbook for pipeline execution
- [ ] Prepare demo of the MVP

## Success Criteria
- [ ] Data successfully extracted from iSolarCloud API
- [ ] Raw data loaded into staging tables
- [ ] dbt models run without errors
- [ ] Site performance KPI calculated and available in mart
- [ ] Airflow pipeline executes all tasks successfully
- [ ] Basic documentation in place

## Dependencies
- Access to iSolarCloud API credentials
- Database credentials and permissions
- Development environment setup

## Risks & Mitigation
| Risk | Impact | Probability | Mitigation |
|------|--------|-------------|------------|
| API rate limiting | High | Medium | Implement backoff and retry logic |
| Data quality issues | High | High | Add data validation steps |
| Performance problems | Medium | Medium | Optimize queries, add indexing |
| Scope creep | High | High | Stick to MVP scope, log enhancements for future sprints |

## Stakeholders
- Data Engineering Team
- BI Developers
- Business Analysts
- Project Sponsors

## Review & Retrospective
- Schedule for Day 10
- Review completed work against acceptance criteria
- Identify improvements for next sprint
- Update project documentation
