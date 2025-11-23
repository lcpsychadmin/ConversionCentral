# Databricks-Managed Profiling Design

## Objectives
- Shift profiling execution from the FastAPI host to Databricks while keeping ConversionCentral as the source of truth for run metadata.
- Ensure each table group can be profiled on-demand with minimal configuration.
- Provide deterministic completion handling so `/data-quality/profile-runs` reflects real-time status.

## High-Level Architecture
1. **Job Registry** – Each data quality table group owns a Databricks job definition. The app creates/updates the job through the Databricks Jobs API and persists the returned `job_id` for reuse.
2. **Run Orchestrator** – When `/data-quality/datasets/{dataObjectId}/profile-runs` is called, the backend registers a `dq_profiles` row and triggers `jobs/run-now` with parameters (table group, profile run id, payload path, etc.). The API stores the resulting `run_id` alongside the profile row.
3. **Databricks Notebook/Wheel** – Executes profiling, writes any artifacts (payload JSON, anomalies), and posts back to `/data-quality/testgen/profile-runs/{profileRunId}/complete` with final metrics. If callback access is unavailable, the backend can poll `jobs/runs/get` as a fallback.
4. **Completion Sink** – `TestGenClient.complete_profile_run` updates `dq_profiles` and inserts anomaly records so existing UI endpoints continue to work.

## Data Model Changes
| Table | Column | Type | Purpose |
| --- | --- | --- | --- |
| `dq_table_groups` | `profiling_job_id` | STRING | Stored Databricks `job_id` per group (nullable until job is created). |
| `dq_profiles` | `databricks_run_id` | STRING | Links profile runs to Databricks runs for troubleshooting/polling. |
| `dq_profiles` | `payload_path` | STRING (existing) | Will now point to DBFS/S3 artifact produced by the Databricks notebook. |

Add indexes on `profiling_job_id` and `databricks_run_id` to speed up lookups for diagnostics.

## Configuration & Secrets
- `DATADBRICKS_HOST`: Workspace URL (e.g., `https://adb-<workspace-id>.azuredatabricks.net`).
- `DATABRICKS_TOKEN`: PAT or service-principal token stored in secure settings (Key Vault/Secret Manager); injected into the FastAPI app.
- Optional: `DATABRICKS_POLICY_ID`, `DATABRICKS_CLUSTER_ID`, default notebook path or wheel location, default warehouse if using SQL.

Expose these via `app/config.py` and surface them through `app.services.databricks_jobs.DatabricksConfig`.

### Databricks Repo–Managed Notebook
To keep profiling logic fully under application control, sync this repository into Databricks Repos and point the profiling job at the checked-in notebook:

1. In Databricks, create a Repo linked to GitHub `lcpsychadmin/ConversionCentral` (or your fork) and check out the desired branch.
2. The managed notebook lives at `docs/data-quality/notebooks/profiling.ipynb`. Databricks Repos will expose it under `/Repos/<user>/<repo>/docs/data-quality/notebooks/profiling`.
3. Set `DATBRICKS_PROFILE_NOTEBOOK_PATH` (or per-connection `profiling_notebook_path`) to that Repo path so `_ensure_job` always provisions the job with the repo-managed code.
4. When profiling logic changes, commit to the repo and ask Databricks to `git pull` (or enable automatic sync). Clients never hand-edit workspace notebooks; deployments keep code consistent with the backend.

## Service Layout
- `app/services/databricks_jobs.py`
  - Thin client wrapping REST 2.1 `jobs/create`, `jobs/update`, `jobs/run-now`, `jobs/runs/get`, `jobs/runs/cancel`.
  - Handles auth headers, retries, request logging.
- `app/services/data_quality_profiling.py`
  - Coordinates job provisioning per table group via Databricks Jobs client.
  - Responsibilities:
    1. Lookup/load a `ProfilingTarget` dataclass that includes connection metadata + stored `profiling_job_id`.
    2. Call `_ensure_job(target)` to create/update the Databricks job when missing or stale, persisting the new `profiling_job_id` with `TestGenClient` or direct SQL update.
    3. Start the profile run in Databricks by invoking `_launch_run(target, profile_run_id, payload_path)` which wraps `jobs/run-now` and records `databricks_run_id` in `dq_profiles`.
    4. Surface a `ProfilingLaunchResult` containing `profile_run_id`, `databricks_run_id`, `job_id`, and any warnings (e.g., job creation skipped due to missing notebook path).
    5. Provide helper `translate_state(run_state)` that maps Databricks lifecycle/result states into ConversionCentral statuses for polling fallback.
  - Exposes high-level method `start_profile_for_table_group(table_group_id: str)` that encapsulates the entire flow and returns the `ProfileRunSummary` used by the router.
- `app/workers/data_quality_profile_monitor.py` (optional)
  - If callback cannot reach the API, this periodic worker polls outstanding runs and marks failures/timeouts.
  - Uses the profiling service's `translate_state` helper to determine next actions and invokes `complete_profile_run` on success/failure.

### Configurable Inputs Needed by `DataQualityProfilingService`
- `databricks_profile_notebook_path`: default workspace notebook or repo path executed by each job.
- `databricks_profile_cluster_id` or `policy_id`: reused cluster or policy binding for the jobs API payload.
- `databricks_profile_callback_token`: secret passed to Databricks for authenticating the completion callback.
- Optional `databricks_profile_workspace_root`: base directory for DBFS payload exports.

## Request Flow
1. **Start Request** (`POST /data-quality/datasets/{id}/profile-runs`)
   - Resolve table contexts (existing logic).
   - For each table group:
     1. Ensure `profiling_job_id` exists; if not, call `jobs/create` with:
        - Notebook/wheel path (in Git repo or Workspace) that accepts widgets: `table_group_id`, `profile_run_id`, `callback_url`, `callback_token`.
        - Cluster spec referencing an approved policy.
     2. Call `TestGenClient.start_profile_run` to insert the `dq_profiles` row and capture `profile_run_id`.
     3. Invoke `jobs/run-now` with `table_group_id`, `profile_run_id`, `payload_path` (temporary DBFS path), and `callback_url`/token.
     4. Update the `dq_profiles` row (new `databricks_run_id` column) with the returned `run_id`.

2. **Databricks Execution**
   - Notebook loads connection/table metadata via parameters and runs profiling.
   - Writes the JSON payload to DBFS/S3; returns the path via `payload_path` parameter.
   - On success/failure, POST to `/data-quality/testgen/profile-runs/{profile_run_id}/complete`:
     ```json
     {
       "status": "completed",
       "row_count": 123456,
       "anomaly_count": 2,
       "anomalies": [
         {
           "table_name": "schema.table",
           "column_name": "col_a",
           "anomaly_type": "null_ratio",
           "severity": "medium",
           "description": "Null ratio exceeded 5%",
           "detected_at": "2025-11-19T15:12:03Z"
         }
       ]
     }
     ```
   - Include a bearer token so the FastAPI endpoint can authenticate the callback (e.g., HMAC or PAT stored in Databricks secrets).

3. **Completion Handling**
   - `complete_profile_run` updates `dq_profiles` with final status, row/anomaly counts, and writes anomalies to `dq_profile_anomalies`.
   - UI immediately reflects the new state with accurate durations.

4. **Fallback Polling (Optional)**
   - A scheduled job lists `dq_profiles` entries where `status='running'` and `databricks_run_id` is set but `started_at` older than N minutes.
   - For each, call `jobs/runs/get`:
     - If `life_cycle_state` is `TERMINATED` with result `SUCCESS`, fetch payload/anomalies from storage and call `complete_profile_run`.
     - If `SKIPPED/INTERNAL_ERROR`, mark failed and note the Databricks state in `payload_path` or a new `error_message` column.

## Notebook/Wheel Template Requirements
- Accept widgets/env vars:
  - `table_group_id`
  - `profile_run_id`
  - `payload_path`
  - `callback_url`
  - `callback_token`
- Produce structured metrics + anomalies JSON (matching `ProfileRunCompleteRequest`).
- Handle retries idempotently (if job reruns with same profile_run_id, POST completion again).

## Catalog-Driven Profiling ("Option 3")
- The FastAPI backend no longer embeds profiling payloads into the Databricks widgets. Instead, the notebook queries the `dq_tables`/`dq_table_groups` metadata to identify which Spark tables to scan.
- The legacy `profiling_payload_inline` widget has been removed; manual replays must now rely on persisted payloads or DBFS artifacts, keeping widget submissions lightweight and deterministic.
- Result DataFrames are constructed directly inside the notebook (see `docs/data-quality/notebooks/profiling.ipynb`) and persisted via `app.databricks_profiling` helpers, which means the backend only needs to track `profile_run_id`, callbacks, and Databricks job IDs.
- Any tooling that previously depended on inline payloads should now read from `dq_profile_results`, `dq_profile_columns`, and `dq_profile_column_values`, falling back to payload parsing only when no SQL rows exist.

## API & Schema Touchpoints
- `app/config.py`: add Databricks credentials + defaults.
- `app/services/data_quality_testgen.py`: extend `start_profile_run` to allow injecting `databricks_run_id` update; optionally add helper to set payload path after job submission.
- `app/routers/data_quality.py`: before returning `profile_runs`, include `databricks_run_id` if we want to expose it to the UI (optional field in `DataQualityProfileRunEntry`).
- Alembic migration to add new columns + indexes.

## Error Handling & Alerts
- If job creation fails, surface 502 error to caller (invalid configuration).
- If run submission fails after `start_profile_run`, mark the profile as `failed` immediately with an error message so UI does not show a dangling “running” entry.
- Add logging + optional notification via existing `DataQualityNotificationService` when Databricks runs fail repeatedly for the same table group.

## Implementation Checklist
1. Create Databricks jobs client + configuration plumbing.
2. Add Alembic migration for `profiling_job_id` + `databricks_run_id` columns and update SQLAlchemy models.
3. Implement `DataQualityProfilingService` with `_ensure_job`, `_launch_run`, and `start_profile_for_table_group` helpers.
4. Update dataset profiling router to call the new service, store run IDs, and handle submission failures/graceful rollback when submission fails after `start_profile_run`.
5. Provide a minimal notebook template (or pseudo code) developers can copy into Databricks.
6. (Optional) Add polling/monitor worker + CLI command to reconcile stuck runs.
7. Update docs/README with setup instructions and runbooks.

With this design in place we can move into implementation confident that each profile run will have a corresponding Databricks execution and a deterministic completion path.
