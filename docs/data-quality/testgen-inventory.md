# DataOps TestGen Inventory

This document captures the assets we will integrate from the open source DataOps TestGen project and highlights the functionality that must be preserved while we adapt persistence to Databricks.

## Source Assets

| Asset | Purpose | Notes |
| --- | --- | --- |
| [`DataKitchen/dataops-testgen`](https://github.com/DataKitchen/dataops-testgen) | Core Python package (`testgen`) providing CLI, Streamlit UI, schedulers, profiling and testing logic. | Distributed via PyPI and the `datakitchen/dataops-testgen` Docker image. Requires PostgreSQL metadata database by default. |
| [`DataKitchen/data-observability-installer`](https://github.com/DataKitchen/data-observability-installer) | `dk-installer.py` helper that bootstraps Docker Compose deployments of TestGen. | Generates `docker-compose.yml`, env files, and manages upgrades/teardowns. |
| Docker image: [`datakitchen/dataops-testgen:v2`](https://hub.docker.com/r/datakitchen/dataops-testgen) | Packaged runtime used in default Compose install (`engine` + Streamlit UI). | Exposes port `8501`, mounts `/var/lib/testgen` for persistent metadata, expects Postgres sidecar. |

## Runtime Architecture

- **Engine (Streamlit UI)** – launched via `testgen run-app ui`; runs `testgen/ui/app.py` with Streamlit, requires access to TestGen metadata DB and target data warehouse connections.
- **Scheduler** – launched via `testgen run-app scheduler`; registers cron-like jobs that execute profiling and test suites; operates through Click commands registered with `@register_scheduler_job`.
- **CLI (`testgen` console script)** – entry point defined in `testgen/__main__.py`; exposes commands to provision databases, register datasets, generate tests, run validations, export results, etc.
- **Metadata Database** – PostgreSQL database storing projects, connections, table groups, profiling runs, test suites, run history, and anomaly results. Configured through env vars (`TG_METADATA_DB_*`, `TESTGEN_USERNAME/PASSWORD`, etc.).
- **Target Database Connectors** – shipped dependencies include `databricks-sql-connector`, `pyodbc`, `psycopg2`, and others, enabling profiling/testing against JDBC sources.

## Key Functional Areas to Preserve

| Area | Representative Commands / Modules | Required Integration Outcome |
| --- | --- | --- |
| **Dataset registration & connection management** | `list-connections`, `list-projects`, database config helpers (`run_launch_db_config`) | Maintain ability to register our `System` / `SystemConnection` objects as TestGen connections backed by Databricks credentials. |
| **Profiling & anomaly screening** | `run-profile`, `list-profiles`, `get-profile-anomalies` invoking `run_profiling_queries` | Continue automatic data profiling for each table group and capture anomaly metadata in Databricks. |
| **Test generation** | `run-test-generation`, `list-test-generation`, `run_test_gen_queries` | Preserve automated creation/updating of validation tests derived from profiling results. |
| **Test execution & run history** | `run-tests`, `list-test-runs`, `get-test-results`, `run_execution_steps` | Maintain execution pipeline for scheduled/manual runs and detailed result history for audit/alerting. |
| **Alerts & observability export** | `export-observability`, `get-profile-screen`, `MoonSpinner` progress UI | Ensure we can surface failing tests/anomalies in our UI, emit notifications, and integrate with future observability feeds. |
| **System maintenance** | `setup-system-db`, `upgrade-system-version`, `quick-start` | Adapt bootstrap/migration logic to provision Databricks schemas instead of Postgres schemas. |

## Configuration & Dependencies

- Environment variables consumed by the CLI/UI:
  - `TESTGEN_USERNAME`, `TESTGEN_PASSWORD`, `TG_DECRYPT_SALT`, `TG_DECRYPT_PASSWORD`
  - `TG_METADATA_DB_HOST`, `TG_METADATA_DB_PORT`, `TG_METADATA_DB_USER`, `TG_METADATA_DB_PASSWORD`, `TG_METADATA_DB_SCHEMA`
  - Optional SSL: `SSL_CERT_FILE`, `SSL_KEY_FILE`
  - Observability export: `OBSERVABILITY_API_URL`, `OBSERVABILITY_API_KEY`
- Dependencies of note for our integration:
  - Uses SQLAlchemy 1.4 and generates models under `testgen/common/models` for metadata persistence.
  - Provides Databricks connectivity via `databricks-sql-connector`, meaning the profiling/test execution logic can already target Databricks warehouses.
  - Streamlit-based UI (`streamlit`, `streamlit-aggrid`, `streamlit-authenticator`) we will replace/augment with our React frontend, so we primarily reuse backend logic via CLI/services.

## Integration Considerations for Databricks

1. **Schema Ownership** – Instead of provisioning a Postgres `testgen` schema, create a dedicated Databricks schema (configurable in Warehouse settings) to store TestGen metadata tables.
2. **Persistence Layer Adaptation** – Replace SQLAlchemy Postgres engine initialization with Databricks SQL connections; ensure encryption utilities (`testgen.common.encrypt`) operate with Databricks storage.
3. **Credential Flow** – Map our managed Databricks credentials (already managed in `databricks_settings`) to TestGen’s expected env variables when invoking CLI commands.
4. **Runtime Hosting** – Decide whether to embed TestGen modules directly in our FastAPI service or run the Docker image headless while pointing its SQLAlchemy URL at Databricks. Either approach must eliminate the Postgres dependency.
5. **UI Replacement** – Leverage CLI commands (`list-`, `get-`, `run-` endpoints) inside new FastAPI routes consumed by our React Data Quality menu, keeping Streamlit UI disabled.

This inventory will guide the subsequent steps in the Data Quality implementation plan, ensuring we preserve all critical TestGen capabilities while pivoting storage to Databricks.
