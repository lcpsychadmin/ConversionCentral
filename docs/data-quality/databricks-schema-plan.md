# Databricks Persistence Plan for Data Quality Metadata

This document defines how we will replace TestGen's default PostgreSQL metadata layer with Databricks-native storage while preserving all required capabilities (profiling, test generation, run history, and alerts).

## Design Goals

- **Single Source** – Persist all Data Quality metadata inside a dedicated Databricks schema so the platform remains cloud-native and avoids extra infrastructure.
- **Configurable Schema** – Allow administrators to pick the metadata schema name in the existing Databricks Warehouse settings (default name is `dq_metadata`).
- **Compatibility** – Mirror TestGen concepts (projects, connections, table groups, runs) so we can reuse its CLI/service logic with minimal translation.
- **Auditability** – Track timestamps, user context, and run outcomes for governance and troubleshooting.
- **Performance** – Use Delta tables with optimized Z-ordering on identifiers/dates to keep drill-down queries fast.

## New Warehouse Settings Fields

Extend the Databricks warehouse configuration to support Data Quality metadata:

| Field | Type | Description |
| --- | --- | --- |
| `data_quality_schema` | string (<= 120 chars) | Name of the Databricks schema that will host metadata tables (default `dq_metadata`). Editable; renaming triggers migration job. |
| `data_quality_storage_format` | enum (`delta`, `hudi`) | Storage format for metadata tables (initial default `delta`). Keeps future extensibility. |
| `data_quality_auto_manage_tables` | boolean | If true, backend auto-creates/updates tables on startup or when settings change. |

Validation: ensure schema and format names follow Databricks identifier rules and are unique per workspace.

## Schema Overview

All tables live in `<catalog>.<data_quality_schema>` and use `managed` tables (Databricks controls location). Columns use CamelCase converted to snake case.

### 1. `dq_projects`

Tracks logical projects (one per tenant/environment, mirroring TestGen project).

| Column | Type | Notes |
| --- | --- | --- |
| `project_key` | STRING | Primary key (e.g., `default`). |
| `name` | STRING | Display name. |
| `description` | STRING | Optional details. |
| `sql_flavor` | STRING | Target SQL dialect (expected `databricks`). |
| `created_at` | TIMESTAMP | Default `current_timestamp()`. |
| `updated_at` | TIMESTAMP | Updated on changes. |

### 2. `dq_connections`

Represents datasets/connection info derived from our `SystemConnection` records.

| Column | Type | Notes |
| --- | --- | --- |
| `connection_id` | STRING | Primary key (UUID from our DB). |
| `project_key` | STRING | FK → `dq_projects.project_key`. |
| `system_id` | STRING | FK to our systems (for reference). |
| `name` | STRING | Friendly name. |
| `catalog` | STRING | Databricks catalog used for profiling/tests. |
| `schema_name` | STRING | Schema name. |
| `http_path` | STRING | For audit; credentials remain in our Secrets store. |
| `managed_credentials_ref` | STRING | Key for retrieving secure token from our config store. |
| `created_at` / `updated_at` | TIMESTAMP | Metadata tracking. |
| `is_active` | BOOLEAN | Soft toggle. |

### 3. `dq_table_groups`

Groups of tables (datasets) corresponding to TestGen table groups; each maps to our ingestion tables.

| Column | Type | Notes |
| --- | --- | --- |
| `table_group_id` | STRING | Primary key (UUID). |
| `connection_id` | STRING | FK → `dq_connections.connection_id`. |
| `name` | STRING | Display (e.g., `<system>-default`). |
| `description` | STRING | Optional. |
| `profiling_include_mask` | STRING | Pattern filters (from settings). |
| `profiling_exclude_mask` | STRING | Pattern filters. |
| `created_at` / `updated_at` | TIMESTAMP | |

### 4. `dq_tables`

Individual table entries within a group, linked to our `tables` records.

| Column | Type | Notes |
| --- | --- | --- |
| `table_id` | STRING | Primary key (UUID). |
| `table_group_id` | STRING | FK → `dq_table_groups`. |
| `schema_name` | STRING | Logical schema. |
| `table_name` | STRING | Table identifier. |
| `source_table_id` | STRING | Optional reference to our `tables.id`. |
| `created_at` | TIMESTAMP | |

### 5. `dq_profiles`

Stores profiling runs results.

| Column | Type | Notes |
| --- | --- | --- |
| `profile_run_id` | STRING | Primary key. |
| `table_group_id` | STRING | FK. |
| `status` | STRING | `pending/running/completed/failed`. |
| `started_at` / `completed_at` | TIMESTAMP | |
| `row_count` | BIGINT | Optional summary. |
| `anomaly_count` | INT | Count of detected issues. |
| `payload_path` | STRING | Path to profile result in lakehouse (if storing large JSON). |
|
### 6. `dq_profile_anomalies`

Detailed anomalies per profile run.

| Column | Type | Notes |
| --- | --- | --- |
| `profile_run_id` | STRING | FK → `dq_profiles`. |
| `table_name` | STRING | |
| `column_name` | STRING | Nullable. |
| `anomaly_type` | STRING | Type code. |
| `severity` | STRING | e.g., `info/warn/error`. |
| `description` | STRING | Human-readable summary. |
| `detected_at` | TIMESTAMP | |

### 7. `dq_tests`

Generated tests (rules) for each dataset.

| Column | Type | Notes |
| --- | --- | --- |
| `test_id` | STRING | Primary key. |
| `table_group_id` | STRING | FK. |
| `test_suite_key` | STRING | Default `default_suite`. |
| `name` | STRING | |
| `rule_type` | STRING | Matches TestGen taxonomy. |
| `definition` | STRING | JSON/SQL of the rule. |
| `created_at` / `updated_at` | TIMESTAMP | |

### 8. `dq_test_runs`

Execution history for test suites.

| Column | Type | Notes |
| --- | --- | --- |
| `test_run_id` | STRING | Primary key. |
| `test_suite_key` | STRING | |
| `project_key` | STRING | FK. |
| `status` | STRING | `pending/running/completed/failed`. |
| `started_at` / `completed_at` | TIMESTAMP | |
| `duration_ms` | BIGINT | |
| `total_tests` | INT | |
| `failed_tests` | INT | |
| `trigger_source` | STRING | `manual`, `schedule`, `ingestion`. |

### 9. `dq_test_results`

Individual test outcomes.

| Column | Type | Notes |
| --- | --- | --- |
| `test_run_id` | STRING | FK. |
| `test_id` | STRING | FK. |
| `table_name` | STRING | |
| `column_name` | STRING | Nullable. |
| `result_status` | STRING | `pass`, `fail`, `warn`. |
| `expected_value` | STRING | Optional. |
| `actual_value` | STRING | Optional. |
| `message` | STRING | Failure context. |
| `detected_at` | TIMESTAMP | |

### 10. `dq_alerts`

Normalized alert stream for UI and notifications.

| Column | Type | Notes |
| --- | --- | --- |
| `alert_id` | STRING | Primary key. |
| `source_type` | STRING | `profile_anomaly`, `test_failure`. |
| `source_ref` | STRING | ID linking to profile/test run. |
| `severity` | STRING | |
| `title` | STRING | |
| `details` | STRING | Longer message. |
| `acknowledged` | BOOLEAN | |
| `acknowledged_by` | STRING | User ID (nullable). |
| `acknowledged_at` | TIMESTAMP | |
| `created_at` | TIMESTAMP | |

### 11. `dq_settings`

Key/value table to track schema revisions and migration status.

| Column | Type | Notes |
| --- | --- | --- |
| `key` | STRING | Primary key. |
| `value` | STRING | |
| `updated_at` | TIMESTAMP | |

## Lifecycle & Operations

1. **Provisioning**
   - When Databricks settings are saved, backend ensures `data_quality_schema` exists (`CREATE SCHEMA IF NOT EXISTS`).
   - Tables are created or upgraded via versioned Delta DDL scripts (mirroring TestGen `dbupgrade` files).
   - Store current schema version in `dq_settings` under key `schema_version`.

2. **Credentials**
   - Use existing managed Databricks warehouse token; we do not store raw tokens in the metadata tables.
   - CLI invocations run with temporary secrets fetched from our secret manager.

3. **Data Flow**
   - On `SystemConnection` creation/activation: register connection/table group records and schedule initial profiling run.
   - On ingestion completion: enqueue TestGen validation run; results are persisted in Databricks tables.
   - Alerts pipeline writes to `dq_alerts` and our notification service consumes from there.

4. **Renaming Schema**
   - Update `data_quality_schema` value -> run migration job that `CREATE SCHEMA`, copy Delta tables via `CREATE TABLE ... CLONE`, update settings, and drop old schema if confirmed.

5. **Access Control**
   - Manage permissions via Databricks SQL GRANTs: allow analytics roles read-only access; restrict write operations to service principal.

## Implementation Tasks

- Generate Delta DDL scripts (located under `app/migrations/databricks/dq/XXXX_create_dq_schema.sql`).
- Extend settings Pydantic models to include new fields.
- Update `ensure_databricks_connection` to call new provisioning helper (`ensure_data_quality_schema`).
- Build Alembic migration to surface new settings columns in Postgres config tables.
- Implement metadata service in backend for CRUD operations on these tables using `databricks-sql-connector`.
- Write tests using Databricks stub or local SQL warehouse emulator to validate DDL execution and metadata queries.

This blueprint completes step 2 of the Data Quality implementation plan and will guide persistence-layer development in upcoming steps.
