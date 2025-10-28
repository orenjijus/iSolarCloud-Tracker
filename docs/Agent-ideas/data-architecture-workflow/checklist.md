# Data Architecture Setup - Validation Checklist

## Database Design (MVP)
- [ ] Database created with TimescaleDB extension
- [ ] Core tables created:
  - [ ] `raw_fusionsolar_performance`
  - [ ] `raw_isolarcloud_performance`
  - [ ] `raw_fusionsolar_metadata`
  - [ ] `raw_isolarcloud_metadata`
  - [ ] `raw_fusionsolar_site_metadata`
  - [ ] `raw_isolarcloud_site_metadata`
  - [ ] `gSheet_device_metadata`
  - [ ] `gSheet_meter_config`
  - [ ] `gSheet_sensor_config`
- [ ] Basic indexes on frequently queried columns
- [ ] Simple user roles (admin, read-only)

## ETL Pipeline (MVP)
- [ ] dbt project with basic models:
  - [ ] Staging layer (platform-specific models)
  - [ ] Intermediate layer (cross-platform unified streams)
  - [ ] Mart layer (aggregations and business logic)
- [ ] Data unification for:
  - [ ] Inverters (5-min intervals)
  - [ ] Sensors (5-min intervals)
  - [ ] Meters (5-min intervals)
  - [ ] Site performance (daily)
- [ ] Critical data quality tests

## API Integration (MVP)
- [ ] Basic CRUD endpoints for core entities
- [ ] Simple authentication (if needed)
- [ ] Basic error handling
- [ ] Endpoint documentation (in code comments)

## Performance (MVP)
- [ ] Basic query performance testing
- [ ] Essential indexes in place
- [ ] Connection pooling configured

## Documentation (MVP)
- [ ] Basic schema documentation
- [ ] Key ETL processes documented
- [ ] Core API endpoints listed

## Security (Future Phase)
- [ ] Basic access control in place
- [ ] Sensitive data identified for later encryption
- [ ] Simple backup process defined

## Monitoring (Basic)
- [ ] Basic database monitoring
- [ ] ETL job status checks
- [ ] Critical error alerts

## Validation Tests (MVP)
- [ ] Core data validation tests
- [ ] Basic CI pipeline
- [ ] Critical path tests

## Sign-off (MVP)
- [ ] Data Engineer: ___________
- [ ] Senior Developer: ___________
- [ ] Date: ___________
