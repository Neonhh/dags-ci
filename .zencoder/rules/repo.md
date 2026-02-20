---
description: Repository Information Overview
alwaysApply: true
---

# astro-airflow Information

## Summary
Local development environment for Apache Airflow using Astro CLI with secure connection to Google Cloud SQL via a sidecar proxy. It facilitates DAG development, testing, and connection management for various data pipelines (AT03, AT10, AT13, AT26, etc.).

## Structure
- **dags/**: Contains all Airflow Directed Acyclic Graphs, organized by project/category.
- **.astro/**: Astro CLI specific configuration and default integrity tests.
- **environment_variables/**: Storage for environment-specific variable files.
- **secrets/**: Local directory for sensitive credentials (GCP keys, connection JSONs) - excluded from git.
- **tests/**: Automated tests, primarily focusing on DAG integrity and logic.
- **root**: Contains Docker configuration, requirement files, and utility scripts for environment setup.

## Language & Runtime
**Language**: Python  
**Version**: 3.12+  
**Build System**: Astro CLI  
**Package Manager**: pip (requirements.txt)

## Dependencies
**Main Dependencies**:
- `apache-airflow-providers-google>=16.0.0`
- `apache-airflow-providers-postgres`
- `apache-airflow-providers-celery`
- `apache-airflow-providers-smtp`
- `apache-airflow-providers-http`
- `apache-airflow-providers-jdbc`
- `pandas`
- `requests`

**Development Tools**:
- **Astro CLI** (v1.20+)
- **gcloud CLI** (for GCP management)
- **Docker Desktop** (v20.10+)

## Build & Installation
```bash
# Start the Airflow environment and Cloud SQL Proxy
astro dev start

# Initialize Airflow connections from secrets
python createConnections.py

# Optional: Convert EmailOperator to send_email()
python convert_email_operator.py
```

## Docker

**Dockerfile**: `Dockerfile` (Uses Astro Runtime 3.1-10)
**Configuration**: 
- `docker-compose.override.yml` runs `cloud-sql-proxy:2.14.0`.
- The proxy allows DAGs to connect to Cloud SQL using the host `astro_sql_proxy` on port `5432`.

## Testing

**Framework**: Pytest
**Test Location**: `tests/dags/`, `.astro/test_dag_integrity_default.py`
**Naming Convention**: `test_*.py`
**Configuration**: `.astro/dag_integrity_exceptions.txt` for skipping specific DAGs.

**Run Command**:
```bash
# Check for import errors in DAGs
astro dev run dags list-import-errors

# Run integrity tests via pytest (inside the container)
astro dev pytest
```
