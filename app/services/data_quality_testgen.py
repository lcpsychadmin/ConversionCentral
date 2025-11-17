"""Client adapter that proxies TestGen workflows into Databricks metadata tables."""

from __future__ import annotations

import logging
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Iterable, Sequence

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from sqlalchemy.exc import SQLAlchemyError

from app.services.databricks_sql import DatabricksConnectionParams, build_sqlalchemy_url
from app.services.data_quality_metadata import _format_table

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class ProfileAnomaly:
    table_name: str
    column_name: str | None
    anomaly_type: str
    severity: str
    description: str
    detected_at: datetime | None = None


@dataclass(frozen=True)
class TestResultRecord:
    test_id: str
    table_name: str | None
    column_name: str | None
    result_status: str
    expected_value: str | None
    actual_value: str | None
    message: str | None
    detected_at: datetime | None = None


@dataclass(frozen=True)
class AlertRecord:
    source_type: str
    source_ref: str
    severity: str
    title: str
    details: str
    alert_id: str | None = None
    acknowledged: bool = False
    acknowledged_by: str | None = None
    acknowledged_at: datetime | None = None


@dataclass(frozen=True)
class SuiteFailureStatus:
    test_suite_key: str | None
    status: str
    acknowledged: bool
    acknowledged_by: str | None
    acknowledged_at: datetime | None


class TestGenClientError(RuntimeError):
    """Raised when TestGen client operations fail."""


class TestGenClient:
    """Utility for interacting with Databricks-backed TestGen tables."""

    def __init__(self, params: DatabricksConnectionParams, schema: str) -> None:
        schema = (schema or "").strip()
        if not schema:
            raise ValueError("Data quality schema is required to initialize TestGenClient.")

        self._params = params
        self._schema = schema
        self._engine: Engine | None = None

    def close(self) -> None:
        if self._engine is not None:
            self._engine.dispose()
            self._engine = None

    @property
    def schema(self) -> str:
        return self._schema

    def _get_engine(self) -> Engine:
        if self._engine is None:
            url = build_sqlalchemy_url(self._params)
            self._engine = create_engine(
                url,
                pool_pre_ping=True,
                connect_args={"timeout": 30},
            )
        return self._engine

    def ping(self) -> bool:
        settings_table = _format_table(self._params.catalog, self._schema, "dq_settings")
        statement = text(f"SELECT 1 FROM {settings_table} LIMIT 1")
        try:
            with self._get_engine().connect() as connection:
                connection.execute(statement)
            return True
        except SQLAlchemyError as exc:  # pragma: no cover - defensive
            logger.warning("TestGen ping failed: %s", exc)
            return False

    def list_projects(self) -> list[dict[str, Any]]:
        projects_table = _format_table(self._params.catalog, self._schema, "dq_projects")
        statement = text(
            f"SELECT project_key, name, description, sql_flavor FROM {projects_table} ORDER BY name"
        )
        return self._fetch(statement)

    def list_connections(self, project_key: str) -> list[dict[str, Any]]:
        connections_table = _format_table(self._params.catalog, self._schema, "dq_connections")
        statement = text(
            f"SELECT connection_id, project_key, system_id, name, catalog, schema_name, http_path, managed_credentials_ref, is_active "
            f"FROM {connections_table} WHERE project_key = :project_key ORDER BY name"
        )
        return self._fetch(statement, {"project_key": project_key})

    def list_table_groups(self, connection_id: str) -> list[dict[str, Any]]:
        table_groups_table = _format_table(self._params.catalog, self._schema, "dq_table_groups")
        statement = text(
            f"SELECT table_group_id, connection_id, name, description, profiling_include_mask, profiling_exclude_mask "
            f"FROM {table_groups_table} WHERE connection_id = :connection_id ORDER BY name"
        )
        return self._fetch(statement, {"connection_id": connection_id})

    def list_tables(self, table_group_id: str) -> list[dict[str, Any]]:
        tables_table = _format_table(self._params.catalog, self._schema, "dq_tables")
        statement = text(
            f"SELECT table_id, table_group_id, schema_name, table_name, source_table_id "
            f"FROM {tables_table} WHERE table_group_id = :table_group_id ORDER BY table_name"
        )
        return self._fetch(statement, {"table_group_id": table_group_id})

    def recent_profile_runs(self, table_group_id: str, *, limit: int = 20) -> list[dict[str, Any]]:
        profiles_table = _format_table(self._params.catalog, self._schema, "dq_profiles")
        statement = text(
            f"SELECT profile_run_id, table_group_id, status, started_at, completed_at, row_count, anomaly_count, payload_path "
            f"FROM {profiles_table} WHERE table_group_id = :table_group_id ORDER BY started_at DESC LIMIT :limit"
        )
        return self._fetch(statement, {"table_group_id": table_group_id, "limit": limit})

    def recent_test_runs(self, project_key: str, *, limit: int = 20) -> list[dict[str, Any]]:
        test_runs_table = _format_table(self._params.catalog, self._schema, "dq_test_runs")
        statement = text(
            f"SELECT test_run_id, test_suite_key, project_key, status, started_at, completed_at, duration_ms, total_tests, failed_tests, trigger_source "
            f"FROM {test_runs_table} WHERE project_key = :project_key ORDER BY started_at DESC LIMIT :limit"
        )
        return self._fetch(statement, {"project_key": project_key, "limit": limit})

    def suite_failure_statuses(self, project_key: str) -> tuple[SuiteFailureStatus, ...]:
        test_runs_table = _format_table(self._params.catalog, self._schema, "dq_test_runs")
        alerts_table = _format_table(self._params.catalog, self._schema, "dq_alerts")
        statement = text(
            f"""
            WITH ranked AS (
                SELECT
                    COALESCE(test_suite_key, '__default__') AS suite_group,
                    test_suite_key,
                    status,
                    started_at,
                    completed_at,
                    ROW_NUMBER() OVER (
                        PARTITION BY COALESCE(test_suite_key, '__default__')
                        ORDER BY COALESCE(completed_at, started_at) DESC, started_at DESC
                    ) AS rn
                FROM {test_runs_table}
                WHERE project_key = :project_key
            ),
            latest_alerts AS (
                SELECT
                    source_ref,
                    acknowledged,
                    acknowledged_by,
                    acknowledged_at,
                    ROW_NUMBER() OVER (
                        PARTITION BY source_ref
                        ORDER BY updated_at DESC, created_at DESC
                    ) AS rn
                FROM {alerts_table}
                WHERE source_type = 'test_run'
            )
            SELECT
                r.test_suite_key,
                r.status,
                COALESCE(a.acknowledged, false) AS acknowledged,
                a.acknowledged_by,
                a.acknowledged_at
            FROM ranked r
            LEFT JOIN latest_alerts a
                ON a.source_ref = CONCAT(:project_key, ':', r.suite_group)
               AND a.rn = 1
            WHERE r.rn = 1
            """
        )
        rows = self._fetch(statement, {"project_key": project_key})
        return tuple(
            SuiteFailureStatus(
                test_suite_key=row.get("test_suite_key"),
                status=row.get("status", ""),
                acknowledged=bool(row.get("acknowledged", False)),
                acknowledged_by=row.get("acknowledged_by"),
                acknowledged_at=row.get("acknowledged_at"),
            )
            for row in rows
        )

    def delete_alert_by_source(self, *, source_type: str, source_ref: str) -> None:
        alerts_table = _format_table(self._params.catalog, self._schema, "dq_alerts")
        statement = text(
            f"DELETE FROM {alerts_table} WHERE source_type = :source_type AND source_ref = :source_ref"
        )
        self._execute(statement, {"source_type": source_type, "source_ref": source_ref})

    def unresolved_failed_test_suites(self, project_key: str) -> tuple[str | None, ...]:
        test_runs_table = _format_table(self._params.catalog, self._schema, "dq_test_runs")
        statement = text(
            f"""
            WITH ranked AS (
                SELECT
                    COALESCE(test_suite_key, '__default__') AS suite_group,
                    test_suite_key,
                    status,
                    started_at,
                    completed_at,
                    ROW_NUMBER() OVER (
                        PARTITION BY COALESCE(test_suite_key, '__default__')
                        ORDER BY COALESCE(completed_at, started_at) DESC, started_at DESC
                    ) AS rn
                FROM {test_runs_table}
                WHERE project_key = :project_key
            )
            SELECT test_suite_key
            FROM ranked
            WHERE rn = 1 AND status = 'failed'
            """
        )
        rows = self._fetch(statement, {"project_key": project_key})
        return tuple(row.get("test_suite_key") for row in rows)

    def recent_alerts(self, *, limit: int = 50, include_acknowledged: bool = False) -> list[dict[str, Any]]:
        alerts_table = _format_table(self._params.catalog, self._schema, "dq_alerts")
        predicate = "" if include_acknowledged else "WHERE acknowledged = false"
        statement = text(
            f"SELECT alert_id, source_type, source_ref, severity, title, details, acknowledged, acknowledged_by, acknowledged_at, created_at "
            f"FROM {alerts_table} {predicate} ORDER BY created_at DESC LIMIT :limit"
        )
        return self._fetch(statement, {"limit": limit})

    def start_profile_run(
        self,
        table_group_id: str,
        *,
        status: str = "running",
        started_at: datetime | None = None,
        payload_path: str | None = None,
    ) -> str:
        profile_run_id = str(uuid.uuid4())
        started_at = started_at or datetime.now(timezone.utc)
        profiles_table = _format_table(self._params.catalog, self._schema, "dq_profiles")
        statement = text(
            f"INSERT INTO {profiles_table} (profile_run_id, table_group_id, status, started_at, payload_path) "
            "VALUES (:profile_run_id, :table_group_id, :status, :started_at, :payload_path)"
        )
        params = {
            "profile_run_id": profile_run_id,
            "table_group_id": table_group_id,
            "status": status,
            "started_at": started_at,
            "payload_path": payload_path,
        }
        self._execute(statement, params)
        return profile_run_id

    def complete_profile_run(
        self,
        profile_run_id: str,
        *,
        status: str,
        row_count: int | None = None,
        anomaly_count: int | None = None,
        anomalies: Sequence[ProfileAnomaly] | None = None,
    ) -> None:
        completed_at = datetime.now(timezone.utc)
        anomalies = tuple(anomalies or ())
        anomaly_count = anomaly_count if anomaly_count is not None else len(anomalies)

        profiles_table = _format_table(self._params.catalog, self._schema, "dq_profiles")
        anomalies_table = _format_table(self._params.catalog, self._schema, "dq_profile_anomalies")

        update_statement = text(
            f"UPDATE {profiles_table} "
            "SET status = :status, completed_at = :completed_at, row_count = :row_count, anomaly_count = :anomaly_count "
            "WHERE profile_run_id = :profile_run_id"
        )

        with self._get_engine().begin() as connection:
            connection.execute(
                update_statement,
                {
                    "status": status,
                    "completed_at": completed_at,
                    "row_count": row_count,
                    "anomaly_count": anomaly_count,
                    "profile_run_id": profile_run_id,
                },
            )

            if anomalies:
                insert_statement = text(
                    f"INSERT INTO {anomalies_table} "
                    "(profile_run_id, table_name, column_name, anomaly_type, severity, description, detected_at) "
                    "VALUES (:profile_run_id, :table_name, :column_name, :anomaly_type, :severity, :description, :detected_at)"
                )
                for anomaly in anomalies:
                    connection.execute(
                        insert_statement,
                        {
                            "profile_run_id": profile_run_id,
                            "table_name": anomaly.table_name,
                            "column_name": anomaly.column_name,
                            "anomaly_type": anomaly.anomaly_type,
                            "severity": anomaly.severity,
                            "description": anomaly.description,
                            "detected_at": anomaly.detected_at or datetime.now(timezone.utc),
                        },
                    )

    def start_test_run(
        self,
        *,
        project_key: str,
        test_suite_key: str | None = None,
        total_tests: int | None = None,
        trigger_source: str | None = None,
        status: str = "running",
        started_at: datetime | None = None,
    ) -> str:
        test_run_id = str(uuid.uuid4())
        started_at = started_at or datetime.now(timezone.utc)
        test_runs_table = _format_table(self._params.catalog, self._schema, "dq_test_runs")
        statement = text(
            f"INSERT INTO {test_runs_table} "
            "(test_run_id, test_suite_key, project_key, status, started_at, total_tests, trigger_source) "
            "VALUES (:test_run_id, :test_suite_key, :project_key, :status, :started_at, :total_tests, :trigger_source)"
        )
        params = {
            "test_run_id": test_run_id,
            "test_suite_key": test_suite_key,
            "project_key": project_key,
            "status": status,
            "started_at": started_at,
            "total_tests": total_tests,
            "trigger_source": trigger_source,
        }
        self._execute(statement, params)
        return test_run_id

    def complete_test_run(
        self,
        test_run_id: str,
        *,
        status: str,
        failed_tests: int | None = None,
        duration_ms: int | None = None,
    ) -> None:
        completed_at = datetime.now(timezone.utc)
        test_runs_table = _format_table(self._params.catalog, self._schema, "dq_test_runs")
        statement = text(
            f"UPDATE {test_runs_table} "
            "SET status = :status, completed_at = :completed_at, failed_tests = :failed_tests, duration_ms = :duration_ms "
            "WHERE test_run_id = :test_run_id"
        )
        params = {
            "status": status,
            "completed_at": completed_at,
            "failed_tests": failed_tests,
            "duration_ms": duration_ms,
            "test_run_id": test_run_id,
        }
        self._execute(statement, params)

    def record_test_results(self, test_run_id: str, results: Iterable[TestResultRecord]) -> None:
        results = tuple(results)
        if not results:
            return

        test_results_table = _format_table(self._params.catalog, self._schema, "dq_test_results")
        statement = text(
            f"INSERT INTO {test_results_table} "
            "(test_run_id, test_id, table_name, column_name, result_status, expected_value, actual_value, message, detected_at) "
            "VALUES (:test_run_id, :test_id, :table_name, :column_name, :result_status, :expected_value, :actual_value, :message, :detected_at)"
        )
        with self._get_engine().begin() as connection:
            for result in results:
                connection.execute(
                    statement,
                    {
                        "test_run_id": test_run_id,
                        "test_id": result.test_id,
                        "table_name": result.table_name,
                        "column_name": result.column_name,
                        "result_status": result.result_status,
                        "expected_value": result.expected_value,
                        "actual_value": result.actual_value,
                        "message": result.message,
                        "detected_at": result.detected_at or datetime.now(timezone.utc),
                    },
                )

    def create_alert(self, alert: AlertRecord) -> str:
        alert_id = alert.alert_id or str(uuid.uuid4())
        alerts_table = _format_table(self._params.catalog, self._schema, "dq_alerts")
        statement = text(
            f"INSERT INTO {alerts_table} "
            "(alert_id, source_type, source_ref, severity, title, details, acknowledged, acknowledged_by, acknowledged_at) "
            "VALUES (:alert_id, :source_type, :source_ref, :severity, :title, :details, :acknowledged, :acknowledged_by, :acknowledged_at)"
        )
        params = {
            "alert_id": alert_id,
            "source_type": alert.source_type,
            "source_ref": alert.source_ref,
            "severity": alert.severity,
            "title": alert.title,
            "details": alert.details,
            "acknowledged": bool(alert.acknowledged),
            "acknowledged_by": alert.acknowledged_by,
            "acknowledged_at": alert.acknowledged_at,
        }
        self._execute(statement, params)
        return alert_id

    def acknowledge_alert(
        self,
        alert_id: str,
        *,
        acknowledged: bool = True,
        acknowledged_by: str | None = None,
        acknowledged_at: datetime | None = None,
    ) -> None:
        alerts_table = _format_table(self._params.catalog, self._schema, "dq_alerts")
        statement = text(
            f"UPDATE {alerts_table} SET acknowledged = :acknowledged, acknowledged_by = :acknowledged_by, acknowledged_at = :acknowledged_at "
            "WHERE alert_id = :alert_id"
        )
        params = {
            "acknowledged": bool(acknowledged),
            "acknowledged_by": acknowledged_by,
            "acknowledged_at": acknowledged_at or datetime.now(timezone.utc) if acknowledged else None,
            "alert_id": alert_id,
        }
        self._execute(statement, params)

    def delete_alert(self, alert_id: str) -> None:
        alerts_table = _format_table(self._params.catalog, self._schema, "dq_alerts")
        statement = text(f"DELETE FROM {alerts_table} WHERE alert_id = :alert_id")
        self._execute(statement, {"alert_id": alert_id})

    def _execute(self, statement, params: dict[str, object]) -> None:
        try:
            with self._get_engine().begin() as connection:
                connection.execute(statement, params)
        except Exception as exc:  # noqa: BLE001
            logger.error("TestGen client execution failed: %s", exc)
            raise TestGenClientError(str(exc)) from exc

    def _fetch(self, statement, params: dict[str, object] | None = None) -> list[dict[str, Any]]:
        params = params or {}
        try:
            with self._get_engine().connect() as connection:
                result = connection.execute(statement, params)
                return [dict(row) for row in result.mappings()]
        except Exception as exc:  # noqa: BLE001
            logger.error("TestGen client fetch failed: %s", exc)
            raise TestGenClientError(str(exc)) from exc

    def __del__(self):  # pragma: no cover - best effort resource cleanup
        try:
            self.close()
        except Exception:  # noqa: BLE001
            pass
