# Backend Integration Plan for Profiling Tables

## Goals
- Read column metrics and value distributions directly from `dq_profile_results` (or the legacy `dq_profile_columns`) plus `dq_profile_column_values` instead of parsing large inline payloads.
- Preserve backwards compatibility by falling back to the existing payload reader when the new tables are empty or unavailable.
- Keep the FastAPI surface area unchanged so the UI can continue to call the existing column-profile endpoint.

## Current Behavior
- The profiling notebook writes all per-table/column metrics into a JSON payload (inline or artifact). The backend fetches the latest `dq_profiles` row, loads the payload, and scrapes metrics/top values from nested dictionaries (see `app/services/data_quality_testgen.py::column_profile`).
- `data_quality_metadata.ensure_data_quality_metadata` previously provisioned only the legacy tables (`dq_profiles`, `dq_profile_anomalies`, etc.). It now creates the new profiling tables (`dq_profile_results`, `dq_profile_anomaly_results`, `dq_data_table_chars`, etc.), but older workspaces may still lack the tables until the provisioning job runs.

## Target Flow
1. Profiling notebook publishes detail rows into `dq_profile_results` (or at minimum the fallback `dq_profile_columns`) and `dq_profile_column_values`.
2. When the API receives `GET /table-groups/{table_group_id}/column-profile`, the service should:
   - Resolve the latest completed run for the group (unchanged SQL).
   - Query `dq_profile_results` (preferred) or `dq_profile_columns` for that run/table/column to obtain the canonical metric row.
   - Query `dq_profile_column_values` for matching rows (split into "top values" vs histogram buckets based on `bucket_label`).
   - Query `dq_profile_anomalies` as today.
   - Assemble the response from table data; only fall back to payload parsing if no column row exists or SQL fails.

## Query Helpers
Add read helpers to `TestGenClient`:
- `_fetch_profile_column(profile_run_id, column_name, table_name=None)` → returns a dict with the raw row from `dq_profile_columns`.
- `_fetch_profile_column_values(profile_run_id, column_name, table_name=None)` → returns a list of rows from `dq_profile_column_values`, ordered by `rank` and `bucket_label`.
- Reuse `_format_table` for fully qualified table names and centralize normalization (lowercasing of `table_name`/`column_name`).

## Implementation Steps
1. **DDL + Provisioning**
   - Update `app/services/data_quality_metadata.py::_table_definitions` to include the two new tables, mirroring the schema described in `docs/data-quality/databricks-schema-plan.md`.
   - Bump `_SCHEMA_VERSION` and `_upgrade_schema` to create tables when upgrading an existing workspace (use `CREATE TABLE IF NOT EXISTS` statements plus `ALTER TABLE ADD COLUMNS` if we expect future evolution).
   - Add automated tests in `tests/test_data_quality_metadata.py` (or a new module) to ensure provisioning scripts include the new tables.

2. **`TestGenClient` Enhancements** (file: `app/services/data_quality_testgen.py`)
   - Introduce lightweight dataclasses (e.g., `ProfileColumnRow`, `ProfileColumnValueRow`) or typed dict builders to keep the mapping readable.
   - Add helper methods `_fetch_profile_column` and `_fetch_profile_column_values` as described above. Normalize inputs using `_normalize_name` so callers can pass user-friendly casing.
   - Extend `column_profile` to:
     1. Load the latest run (existing logic).
     2. Call `_fetch_profile_column`; if it returns a row, build the `metrics`, `table_name`, `column_name`, and `data_type` directly from that row.
     3. Call `_fetch_profile_column_values` and split results into:
        - `top_values`: rows where `bucket_label` is NULL; expose `value`, `count`, `percentage` (using `relative_freq`).
        - `histogram`: rows where `bucket_label` is not NULL or bounds are present.
     4. Only invoke `_load_profile_payload` + `_locate_column_profile` when the SQL row is missing (older runs or provisioning lag).
   - Store a feature flag (e.g., `settings.testgen_profile_table_reads_enabled`) so we can temporarily disable the SQL path if needed; default to `True` once end-to-end validation is complete.

3. **Router/Schema Adjustments**
   - No API contract changes are necessary, but ensure `TestGenColumnProfile` Pydantic model can represent any new fields (currently it already accepts `metrics`, `top_values`, `histogram`, `anomalies`).
   - Consider adding an optional query parameter (`source=table|payload|auto`) for debugging, but default to automatic selection to avoid UI work right now.

4. **Fallback & Error Handling**
   - Wrap the new SQL reads in `_with_profiling_retry` so schema upgrades (creating the new tables) happen automatically using the existing retry path.
   - Log and continue to the payload fallback when SQL errors occur—never fail the endpoint simply because the table is empty.

5. **Tests**
   - Extend `tests/test_data_quality_testgen.py` (create if missing) with unit tests that:
     - Seed in-memory rows for `dq_profile_columns` / `dq_profile_column_values` (use temporary SQLite or SQLAlchemy fixtures) and assert `column_profile` builds metrics and value distributions without touching payloads.
     - Simulate missing rows to ensure fallback still works (mock `_load_profile_payload`).
   - Add regression coverage for histogram vs frequency parsing.

6. **Rollout Plan**
   - Deploy DDL/provisioning changes first so schemas are present before the API executes new queries.
   - Backfill existing runs by re-executing the profiling notebook or writing a one-off job that explodes stored payloads into the new tables.
   - Enable the SQL read path behind a feature flag; monitor logs for a few runs to confirm row counts, then remove/retire the flag when stable.

## Open Questions / Follow-ups
- Do we need to expose `dq_profile_columns` via a bulk API (table-level summaries)? If so, add follow-up story after the column-profile endpoint is migrated.
- Should we snapshot distributions over time or only keep the latest run per column? Current plan deletes/replaces per run ID, so retention is managed by `dq_profiles`. Adjust if historical trending becomes a requirement.
