"""Client adapter that proxies TestGen workflows into Databricks metadata tables."""

from __future__ import annotations

import json
import logging
import math
import uuid
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Callable, Iterable, Sequence, TypeVar

from sqlalchemy import bindparam, create_engine, text
from sqlalchemy.engine import Engine
from sqlalchemy.exc import SQLAlchemyError

from app.config import Settings, get_settings
from app.services.databricks_sql import DatabricksConnectionParams, build_sqlalchemy_url
from app.services.data_quality_metadata import _format_table, _table_definitions

logger = logging.getLogger(__name__)

T = TypeVar("T")

_UNSET = object()
_COLUMN_METRIC_ORDER = [
    "row_count",
    "distinct_count",
    "cardinality",
    "null_count",
    "null_percent",
    "null_percentage",
    "null_ratio",
    "unique_percent",
    "valid_percent",
    "avg",
    "mean",
    "median",
    "min",
    "p25",
    "p75",
    "max",
    "stddev",
    "std_dev",
    "zeros_count",
    "zero_count",
    "negative_count",
    "length_min",
    "length_avg",
    "length_max",
]

_COLUMN_METRIC_LABELS = {
    "row_count": "Row count",
    "distinct_count": "Distinct values",
    "cardinality": "Distinct values",
    "null_count": "Null values",
    "null_percent": "Null percentage",
    "null_percentage": "Null percentage",
    "null_ratio": "Null percentage",
    "unique_percent": "Unique percentage",
    "valid_percent": "Valid percentage",
    "avg": "Average",
    "mean": "Average",
    "median": "Median",
    "min": "Minimum",
    "p25": "25th percentile",
    "p75": "75th percentile",
    "max": "Maximum",
    "stddev": "Std deviation",
    "std_dev": "Std deviation",
    "zeros_count": "Zero values",
    "zero_count": "Zero values",
    "negative_count": "Negative values",
    "length_min": "Minimum length",
    "length_avg": "Average length",
    "length_max": "Maximum length",
}

_PERCENT_METRIC_KEYS = {
    "null_percent",
    "null_percentage",
    "null_ratio",
    "unique_percent",
    "valid_percent",
}

_PROFILE_TABLE_DEFINITIONS = _table_definitions()
_PROFILE_COLUMNS_DEFINITION = tuple(_PROFILE_TABLE_DEFINITIONS.get("dq_profile_columns", ()))
_PROFILE_COLUMN_VALUES_DEFINITION = tuple(_PROFILE_TABLE_DEFINITIONS.get("dq_profile_column_values", ()))


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

    def __init__(
        self,
        params: DatabricksConnectionParams,
        schema: str,
        *,
        settings: Settings | None = None,
    ) -> None:
        schema = (schema or "").strip()
        if not schema:
            raise ValueError("Data quality schema is required to initialize TestGenClient.")

        self._params = params
        self._schema = schema
        self._engine: Engine | None = None
        self._profiling_support_verified = False
        self._settings = settings or get_settings()
        self._profile_table_reads_enabled = bool(
            getattr(self._settings, "testgen_profile_table_reads_enabled", True)
        )

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

    def ensure_profiling_support(self) -> None:
        """Ensure profiling-specific columns exist before issuing queries."""

        if self._profiling_support_verified:
            return

        table_groups_table = _format_table(self._params.catalog, self._schema, "dq_table_groups")
        profiles_table = _format_table(self._params.catalog, self._schema, "dq_profiles")
        ddl_statements = self._profile_detail_table_statements()
        statements = (
            text(f"ALTER TABLE {table_groups_table} ADD COLUMNS (profiling_job_id STRING)"),
            text(f"ALTER TABLE {profiles_table} ADD COLUMNS (databricks_run_id STRING)"),
        )

        try:
            with self._get_engine().begin() as connection:
                for ddl in ddl_statements:
                    try:
                        connection.execute(ddl)
                    except SQLAlchemyError as exc:
                        raise
                for statement in statements:
                    try:
                        connection.execute(statement)
                    except SQLAlchemyError as exc:
                        message = str(exc).lower()
                        if "already exists" in message or "duplicate" in message:
                            continue
                        raise
        except SQLAlchemyError as exc:
            raise TestGenClientError(
                "Failed to ensure profiling metadata support; please verify Databricks schema permissions."
            ) from exc

        self._profiling_support_verified = True

    def _profile_detail_table_statements(self) -> tuple[Any, ...]:
        definitions = (
            ("dq_profile_columns", _PROFILE_COLUMNS_DEFINITION),
            ("dq_profile_column_values", _PROFILE_COLUMN_VALUES_DEFINITION),
        )
        storage_format = (self._params.data_quality_storage_format or "delta").strip().lower() or "delta"
        using_clause = storage_format.upper()
        properties_clause = (
            " TBLPROPERTIES ('delta.autoOptimize.optimizeWrite' = 'true', 'delta.autoOptimize.autoCompact' = 'true')"
            if storage_format == "delta"
            else ""
        )

        statements: list[Any] = []
        for table_name, columns in definitions:
            if not columns:
                continue
            qualified = _format_table(self._params.catalog, self._schema, table_name)
            columns_sql = ",\n                ".join(columns)
            ddl = text(
                f"CREATE TABLE IF NOT EXISTS {qualified} (\n                {columns_sql}\n            ) USING {using_clause}{properties_clause}"
            )
            statements.append(ddl)
        return tuple(statements)

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

    def get_table_group_details(self, table_group_id: str) -> dict[str, Any] | None:
        table_groups_table = _format_table(self._params.catalog, self._schema, "dq_table_groups")
        connections_table = _format_table(self._params.catalog, self._schema, "dq_connections")
        statement = text(
            f"SELECT groups.table_group_id, groups.connection_id, groups.name, groups.description, "
            "groups.profiling_include_mask, groups.profiling_exclude_mask, groups.profiling_job_id, "
            "conns.name AS connection_name, conns.project_key, conns.system_id, conns.catalog, conns.schema_name, "
            "conns.http_path, conns.managed_credentials_ref, conns.is_active "
            f"FROM {table_groups_table} AS groups "
            f"JOIN {connections_table} AS conns ON conns.connection_id = groups.connection_id "
            "WHERE groups.table_group_id = :table_group_id LIMIT 1"
        )

        def _operation() -> dict[str, Any] | None:
            rows = self._fetch(statement, {"table_group_id": table_group_id})
            return rows[0] if rows else None

        return self._with_profiling_retry(_operation)

    def list_tables(self, table_group_id: str) -> list[dict[str, Any]]:
        tables_table = _format_table(self._params.catalog, self._schema, "dq_tables")
        statement = text(
            f"SELECT table_id, table_group_id, schema_name, table_name, source_table_id "
            f"FROM {tables_table} WHERE table_group_id = :table_group_id ORDER BY table_name"
        )
        return self._fetch(statement, {"table_group_id": table_group_id})

    def list_test_suites(
        self,
        *,
        project_key: str | None = None,
        data_object_id: str | None = None,
    ) -> list[dict[str, Any]]:
        test_suites_table = _format_table(self._params.catalog, self._schema, "dq_test_suites")
        predicates: list[str] = []
        params: dict[str, Any] = {}

        if project_key:
            predicates.append("project_key = :project_key")
            params["project_key"] = project_key

        if data_object_id:
            predicates.append("data_object_id = :data_object_id")
            params["data_object_id"] = data_object_id

        where_clause = f" WHERE {' AND '.join(predicates)}" if predicates else ""
        statement = text(
            f"SELECT test_suite_key, project_key, name, description, severity, product_team_id, application_id, data_object_id, data_definition_id, created_at, updated_at "
            f"FROM {test_suites_table}{where_clause} ORDER BY name"
        )
        rows = self._fetch(statement, params)
        self._repair_invalid_suite_keys(rows)
        return rows

    def get_test_suite(self, test_suite_key: str) -> dict[str, Any] | None:
        test_suites_table = _format_table(self._params.catalog, self._schema, "dq_test_suites")
        statement = text(
            f"SELECT test_suite_key, project_key, name, description, severity, product_team_id, application_id, data_object_id, data_definition_id, created_at, updated_at "
            f"FROM {test_suites_table} WHERE test_suite_key = :test_suite_key LIMIT 1"
        )
        rows = self._fetch(statement, {"test_suite_key": test_suite_key})
        if not rows:
            return None
        self._repair_invalid_suite_keys(rows)
        return rows[0]

    def create_test_suite(
        self,
        *,
        project_key: str | None,
        name: str,
        description: str | None,
        severity: str | None,
        product_team_id: str | None,
        application_id: str | None,
        data_object_id: str | None,
        data_definition_id: str | None,
    ) -> str:
        test_suite_key = str(uuid.uuid4())
        test_suites_table = _format_table(self._params.catalog, self._schema, "dq_test_suites")
        statement = text(
            f"INSERT INTO {test_suites_table} "
            "(test_suite_key, project_key, name, description, severity, product_team_id, application_id, data_object_id, data_definition_id, created_at, updated_at) "
            "VALUES (:test_suite_key, :project_key, :name, :description, :severity, :product_team_id, :application_id, :data_object_id, :data_definition_id, current_timestamp(), current_timestamp())"
        )
        params = {
            "test_suite_key": test_suite_key,
            "project_key": project_key,
            "name": name,
            "description": description,
            "severity": severity,
            "product_team_id": product_team_id,
            "application_id": application_id,
            "data_object_id": data_object_id,
            "data_definition_id": data_definition_id,
        }
        self._execute(statement, params)
        return test_suite_key

    def update_test_suite(
        self,
        test_suite_key: str,
        *,
        project_key=_UNSET,
        name=_UNSET,
        description=_UNSET,
        severity=_UNSET,
        product_team_id=_UNSET,
        application_id=_UNSET,
        data_object_id=_UNSET,
        data_definition_id=_UNSET,
    ) -> None:
        assignments: list[str] = []
        params: dict[str, Any] = {"test_suite_key": test_suite_key}

        def _append(column: str, value: Any) -> None:
            assignments.append(f"{column} = :{column}")
            params[column] = value

        if project_key is not _UNSET:
            _append("project_key", project_key)
        if name is not _UNSET:
            _append("name", name)
        if description is not _UNSET:
            _append("description", description)
        if severity is not _UNSET:
            _append("severity", severity)
        if product_team_id is not _UNSET:
            _append("product_team_id", product_team_id)
        if application_id is not _UNSET:
            _append("application_id", application_id)
        if data_object_id is not _UNSET:
            _append("data_object_id", data_object_id)
        if data_definition_id is not _UNSET:
            _append("data_definition_id", data_definition_id)

        if not assignments:
            assignments.append("updated_at = current_timestamp()")
        else:
            assignments.append("updated_at = current_timestamp()")

        set_clause = ", ".join(assignments)
        test_suites_table = _format_table(self._params.catalog, self._schema, "dq_test_suites")
        statement = text(
            f"UPDATE {test_suites_table} SET {set_clause} WHERE test_suite_key = :test_suite_key"
        )
        self._execute(statement, params)

    def delete_test_suite(self, test_suite_key: str) -> None:
        test_suites_table = _format_table(self._params.catalog, self._schema, "dq_test_suites")
        statement = text(
            f"DELETE FROM {test_suites_table} WHERE test_suite_key = :test_suite_key"
        )
        self._execute(statement, {"test_suite_key": test_suite_key})

    def list_suite_tests(self, test_suite_key: str) -> list[dict[str, Any]]:
        tests_table = _format_table(self._params.catalog, self._schema, "dq_tests")
        statement = text(
            f"SELECT test_id, table_group_id, test_suite_key, name, rule_type, definition, created_at, updated_at "
            f"FROM {tests_table} WHERE test_suite_key = :test_suite_key ORDER BY name"
        )
        return self._fetch(statement, {"test_suite_key": test_suite_key})

    def get_test(self, test_id: str) -> dict[str, Any] | None:
        tests_table = _format_table(self._params.catalog, self._schema, "dq_tests")
        statement = text(
            f"SELECT test_id, table_group_id, test_suite_key, name, rule_type, definition, created_at, updated_at "
            f"FROM {tests_table} WHERE test_id = :test_id LIMIT 1"
        )
        rows = self._fetch(statement, {"test_id": test_id})
        return rows[0] if rows else None

    def create_test(
        self,
        *,
        table_group_id: str,
        test_suite_key: str,
        name: str,
        rule_type: str,
        definition: dict[str, Any] | str | None = None,
    ) -> str:
        test_id = str(uuid.uuid4())
        definition_payload = (
            definition
            if isinstance(definition, str)
            else json.dumps(definition or {})
        )

        tests_table = _format_table(self._params.catalog, self._schema, "dq_tests")
        statement = text(
            f"INSERT INTO {tests_table} "
            "(test_id, table_group_id, test_suite_key, name, rule_type, definition, created_at, updated_at) "
            "VALUES (:test_id, :table_group_id, :test_suite_key, :name, :rule_type, :definition, current_timestamp(), current_timestamp())"
        )
        params = {
            "test_id": test_id,
            "table_group_id": table_group_id,
            "test_suite_key": test_suite_key,
            "name": name,
            "rule_type": rule_type,
            "definition": definition_payload,
        }
        self._execute(statement, params)
        return test_id

    def update_test(
        self,
        test_id: str,
        *,
        table_group_id=_UNSET,
        test_suite_key=_UNSET,
        name=_UNSET,
        rule_type=_UNSET,
        definition=_UNSET,
    ) -> None:
        assignments: list[str] = []
        params: dict[str, Any] = {"test_id": test_id}

        def _append(column: str, value: Any) -> None:
            assignments.append(f"{column} = :{column}")
            params[column] = value

        if table_group_id is not _UNSET:
            _append("table_group_id", table_group_id)
        if test_suite_key is not _UNSET:
            _append("test_suite_key", test_suite_key)
        if name is not _UNSET:
            _append("name", name)
        if rule_type is not _UNSET:
            _append("rule_type", rule_type)
        if definition is not _UNSET:
            payload = (
                definition
                if isinstance(definition, str)
                else json.dumps(definition or {})
            )
            _append("definition", payload)

        assignments.append("updated_at = current_timestamp()")

        tests_table = _format_table(self._params.catalog, self._schema, "dq_tests")
        statement = text(
            f"UPDATE {tests_table} SET {', '.join(assignments)} WHERE test_id = :test_id"
        )
        self._execute(statement, params)

    def delete_test(self, test_id: str) -> None:
        tests_table = _format_table(self._params.catalog, self._schema, "dq_tests")
        statement = text(f"DELETE FROM {tests_table} WHERE test_id = :test_id")
        self._execute(statement, {"test_id": test_id})

    @staticmethod
    def _is_invalid_suite_key(value: Any) -> bool:
        if value is None:
            return True
        candidate = value.strip() if isinstance(value, str) else str(value).strip()
        if not candidate:
            return True
        return candidate.lower() in {"undefined", "null"}

    def _repair_invalid_suite_keys(self, suites: list[dict[str, Any]]) -> None:
        invalid_rows = [row for row in suites if self._is_invalid_suite_key(row.get("test_suite_key"))]
        if not invalid_rows:
            return

        suites_table = _format_table(self._params.catalog, self._schema, "dq_test_suites")

        for row in invalid_rows:
            new_key = str(uuid.uuid4())
            params: dict[str, Any] = {"new_key": new_key, "name": row.get("name")}

            conditions: list[str] = [
                "(test_suite_key IS NULL OR TRIM(test_suite_key) = '' OR LOWER(test_suite_key) IN ('undefined','null'))",
                "name = :name",
            ]

            project_key = row.get("project_key")
            if project_key is not None:
                params["project_key"] = project_key
                conditions.append("project_key = :project_key")
            else:
                conditions.append("project_key IS NULL")

            created_at = row.get("created_at")
            if created_at is not None:
                params["created_at"] = created_at
                conditions.append("created_at = :created_at")
            else:
                conditions.append("created_at IS NULL")

            condition_sql = " AND ".join(conditions)

            statement = text(
                f"UPDATE {suites_table} SET test_suite_key = :new_key, updated_at = current_timestamp() "
                f"WHERE {condition_sql}"
            )

            with self._get_engine().begin() as connection:
                connection.execute(statement, params)

            row["test_suite_key"] = new_key

    def recent_profile_runs(self, table_group_id: str, *, limit: int = 20) -> list[dict[str, Any]]:
        profiles_table = _format_table(self._params.catalog, self._schema, "dq_profiles")
        statement = text(
            f"SELECT profile_run_id, table_group_id, status, started_at, completed_at, row_count, anomaly_count, databricks_run_id "
            f"FROM {profiles_table} WHERE table_group_id = :table_group_id ORDER BY started_at DESC LIMIT :limit"
        )

        return self._with_profiling_retry(
            lambda: self._fetch(statement, {"table_group_id": table_group_id, "limit": limit})
        )

    def list_profile_runs_overview(
        self,
        *,
        table_group_id: str | None = None,
        limit: int = 50,
    ) -> list[dict[str, Any]]:
        profiles_table = _format_table(self._params.catalog, self._schema, "dq_profiles")
        table_groups_table = _format_table(self._params.catalog, self._schema, "dq_table_groups")
        connections_table = _format_table(self._params.catalog, self._schema, "dq_connections")

        filters: list[str] = []
        params: dict[str, Any] = {"limit": limit}
        if table_group_id:
            filters.append("p.table_group_id = :table_group_id")
            params["table_group_id"] = table_group_id

        where_clause = f"WHERE {' AND '.join(filters)}" if filters else ""

        statement = text(
            f"""
            SELECT
                p.profile_run_id,
                p.table_group_id,
                p.status,
                p.started_at,
                p.completed_at,
                p.row_count,
                p.anomaly_count,
                p.databricks_run_id,
                g.name AS table_group_name,
                g.description AS table_group_description,
                g.connection_id,
                c.name AS connection_name,
                c.catalog,
                c.schema_name,
                c.project_key,
                c.system_id
            FROM {profiles_table} p
            LEFT JOIN {table_groups_table} g ON g.table_group_id = p.table_group_id
            LEFT JOIN {connections_table} c ON c.connection_id = g.connection_id
            {where_clause}
            ORDER BY COALESCE(p.completed_at, p.started_at) DESC, p.started_at DESC
            LIMIT :limit
            """
        )
        return self._with_profiling_retry(lambda: self._fetch(statement, params))

    def list_table_groups_with_connections(self) -> list[dict[str, Any]]:
        table_groups_table = _format_table(self._params.catalog, self._schema, "dq_table_groups")
        connections_table = _format_table(self._params.catalog, self._schema, "dq_connections")
        statement = text(
            f"""
            SELECT
                g.table_group_id,
                g.connection_id,
                g.profiling_job_id,
                g.name AS table_group_name,
                g.description AS table_group_description,
                c.name AS connection_name,
                c.catalog,
                c.schema_name,
                c.project_key,
                c.system_id
            FROM {table_groups_table} g
            LEFT JOIN {connections_table} c ON c.connection_id = g.connection_id
            ORDER BY g.name
            """
        )
        return self._with_profiling_retry(lambda: self._fetch(statement))

    def profile_run_anomaly_counts(
        self,
        profile_run_ids: Sequence[str],
    ) -> dict[str, dict[str, int]]:
        if not profile_run_ids:
            return {}

        anomalies_table = _format_table(self._params.catalog, self._schema, "dq_profile_anomalies")
        statement = (
            text(
                f"SELECT profile_run_id, severity, COUNT(*) AS count FROM {anomalies_table} "
                "WHERE profile_run_id IN :profile_run_ids GROUP BY profile_run_id, severity"
            )
            .bindparams(bindparam("profile_run_ids", expanding=True))
        )

        rows = self._fetch(statement, {"profile_run_ids": tuple(profile_run_ids)})
        result: dict[str, dict[str, int]] = {}
        for row in rows:
            run_id = row.get("profile_run_id")
            severity = row.get("severity") or "unknown"
            count = int(row.get("count") or 0)
            if not run_id:
                continue
            bucket = result.setdefault(run_id, {})
            bucket[str(severity)] = count
        return result

    def column_profile(
        self,
        table_group_id: str,
        *,
        column_name: str,
        table_name: str | None = None,
        physical_name: str | None = None,
    ) -> dict[str, Any] | None:
        normalized_column = (column_name or "").strip()
        if not normalized_column:
            raise ValueError("column_name is required to build a column profile")

        run = self._latest_completed_profile_run(table_group_id)
        if not run:
            return None

        table_entry: Mapping[str, Any] | None = None
        column_entry: Mapping[str, Any] | None = None

        table_profile_entries = self._load_column_profile_from_tables(
            run,
            normalized_column=normalized_column,
            table_name=table_name,
            physical_name=physical_name,
        )
        if not table_profile_entries:
            return None

        table_entry, column_entry = table_profile_entries

        metrics = self._build_column_metrics(column_entry)
        if run.get("row_count") is not None and not any(metric.get("key") == "row_count" for metric in metrics):
            metrics.insert(
                0,
                self._metric_entry(
                    "row_count",
                    _COLUMN_METRIC_LABELS.get("row_count", "Row count"),
                    run.get("row_count"),
                ),
            )

        anomalies = self._fetch_profile_anomalies(run.get("profile_run_id"), normalized_column)

        return {
            "table_group_id": table_group_id,
            "profile_run_id": run.get("profile_run_id"),
            "status": run.get("status"),
            "started_at": run.get("started_at"),
            "completed_at": run.get("completed_at"),
            "row_count": run.get("row_count"),
            "table_name": self._extract_table_name(table_entry) or table_name or physical_name,
            "column_name": self._extract_column_name(column_entry) or normalized_column,
            "data_type": self._extract_data_type(column_entry),
            "metrics": metrics,
            "top_values": self._build_top_values(column_entry),
            "histogram": self._build_histogram(column_entry),
            "anomalies": anomalies,
        }

    def list_profile_run_anomalies(self, profile_run_id: str) -> list[dict[str, Any]]:
        return self._fetch_profile_anomalies(profile_run_id, None)

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
    ) -> str:
        profile_run_id = str(uuid.uuid4())
        started_at = started_at or datetime.now(timezone.utc)
        profiles_table = _format_table(self._params.catalog, self._schema, "dq_profiles")
        statement = text(
            f"INSERT INTO {profiles_table} (profile_run_id, table_group_id, status, started_at) "
            "VALUES (:profile_run_id, :table_group_id, :status, :started_at)"
        )
        params = {
            "profile_run_id": profile_run_id,
            "table_group_id": table_group_id,
            "status": status,
            "started_at": started_at,
        }
        self._execute(statement, params)
        return profile_run_id

    def update_table_group_profiling_job(self, table_group_id: str, profiling_job_id: str | None) -> None:
        table_groups_table = _format_table(self._params.catalog, self._schema, "dq_table_groups")
        statement = text(
            f"UPDATE {table_groups_table} SET profiling_job_id = :profiling_job_id, updated_at = current_timestamp() "
            "WHERE table_group_id = :table_group_id"
        )
        return self._with_profiling_retry(
            lambda: self._execute(
                statement,
                {
                    "profiling_job_id": profiling_job_id,
                    "table_group_id": table_group_id,
                },
            )
        )

    def update_profile_run_databricks_run(
        self,
        profile_run_id: str,
        *,
        databricks_run_id: str | None,
    ) -> None:
        profiles_table = _format_table(self._params.catalog, self._schema, "dq_profiles")
        statement = text(
            f"UPDATE {profiles_table} SET databricks_run_id = :databricks_run_id WHERE profile_run_id = :profile_run_id"
        )
        return self._with_profiling_retry(
            lambda: self._execute(
                statement,
                {
                    "databricks_run_id": databricks_run_id,
                    "profile_run_id": profile_run_id,
                },
            )
        )

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

    def _latest_completed_profile_run(self, table_group_id: str) -> dict[str, Any] | None:
        profiles_table = _format_table(self._params.catalog, self._schema, "dq_profiles")
        statement = text(
            f"SELECT profile_run_id, table_group_id, status, started_at, completed_at, row_count, anomaly_count "
            f"FROM {profiles_table} "
            "WHERE table_group_id = :table_group_id "
            "AND (completed_at IS NOT NULL OR LOWER(status) IN ('completed','complete','success','succeeded','finished')) "
            "ORDER BY COALESCE(completed_at, started_at) DESC LIMIT 1"
        )
        rows = self._fetch(statement, {"table_group_id": table_group_id})
        return rows[0] if rows else None

    def _fetch_profile_anomalies(self, profile_run_id: str | None, column_name: str | None) -> list[dict[str, Any]]:
        if not profile_run_id:
            return []

        anomalies_table = _format_table(self._params.catalog, self._schema, "dq_profile_anomalies")
        filters = ["profile_run_id = :profile_run_id"]
        params: dict[str, Any] = {"profile_run_id": profile_run_id}

        normalized_column = (column_name or "").strip().lower()
        if normalized_column:
            filters.append("LOWER(column_name) = :column_name")
            params["column_name"] = normalized_column

        statement = text(
            f"SELECT table_name, column_name, anomaly_type, severity, description, detected_at "
            f"FROM {anomalies_table} WHERE {' AND '.join(filters)} ORDER BY detected_at DESC"
        )
        return self._fetch(statement, params)

    def _load_column_profile_from_tables(
        self,
        run: Mapping[str, Any],
        *,
        normalized_column: str,
        table_name: str | None,
        physical_name: str | None,
    ) -> tuple[Mapping[str, Any] | None, Mapping[str, Any] | None] | None:
        if not self._profile_table_reads_enabled:
            return None

        profile_run_id = (run.get("profile_run_id") or "").strip()
        if not profile_run_id:
            return None

        candidates = self._table_filter_candidates(table_name, physical_name)
        if not candidates:
            candidates = (None,)
        else:
            candidates = (*candidates, None)

        for candidate in candidates:
            try:
                column_row = self._fetch_profile_column(
                    profile_run_id,
                    normalized_column,
                    table_filter=candidate,
                )
            except TestGenClientError as exc:
                logger.warning("Column profile table lookup failed: %s", exc)
                return None

            if not column_row:
                continue

            try:
                value_rows = self._fetch_profile_column_values(
                    profile_run_id,
                    normalized_column,
                    table_filter=candidate,
                )
            except TestGenClientError as exc:
                logger.warning("Column profile values lookup failed: %s", exc)
                return None

            return self._build_table_profile_entries(column_row, value_rows)

        return None

    def _fetch_profile_column(
        self,
        profile_run_id: str,
        column_name: str,
        *,
        table_filter: str | None,
    ) -> dict[str, Any] | None:
        columns_table = _format_table(self._params.catalog, self._schema, "dq_profile_columns")
        filters = [
            "profile_run_id = :profile_run_id",
            "LOWER(column_name) = :column_name",
        ]
        params: dict[str, Any] = {
            "profile_run_id": profile_run_id,
            "column_name": column_name.lower(),
        }
        if table_filter:
            filters.append(
                "(LOWER(table_name) = :table_name OR LOWER(CONCAT_WS('.', schema_name, table_name)) = :table_name)"
            )
            params["table_name"] = table_filter

        statement = text(
            f"SELECT * FROM {columns_table} WHERE {' AND '.join(filters)} ORDER BY generated_at DESC LIMIT 1"
        )

        rows = self._with_profiling_retry(lambda: self._fetch(statement, params))
        return rows[0] if rows else None

    def _fetch_profile_column_values(
        self,
        profile_run_id: str,
        column_name: str,
        *,
        table_filter: str | None,
    ) -> list[dict[str, Any]]:
        values_table = _format_table(self._params.catalog, self._schema, "dq_profile_column_values")
        filters = [
            "profile_run_id = :profile_run_id",
            "LOWER(column_name) = :column_name",
        ]
        params: dict[str, Any] = {
            "profile_run_id": profile_run_id,
            "column_name": column_name.lower(),
        }
        if table_filter:
            filters.append(
                "(LOWER(table_name) = :table_name OR LOWER(CONCAT_WS('.', schema_name, table_name)) = :table_name)"
            )
            params["table_name"] = table_filter

        order_clause = (
            "ORDER BY CASE WHEN bucket_label IS NULL AND bucket_lower_bound IS NULL AND bucket_upper_bound IS NULL THEN 0 ELSE 1 END, "
            "COALESCE(rank, 2147483647), "
            "bucket_label"
        )
        statement = text(
            f"SELECT * FROM {values_table} WHERE {' AND '.join(filters)} {order_clause}"
        )
        return self._with_profiling_retry(lambda: self._fetch(statement, params))

    def _table_filter_candidates(self, *names: str | None) -> tuple[str, ...]:
        candidates: list[str] = []
        seen: set[str] = set()
        for name in names:
            normalized = self._normalize_name(name)
            if not normalized or normalized in seen:
                continue
            candidates.append(normalized)
            seen.add(normalized)
            if "." in normalized:
                unqualified = normalized.split(".")[-1]
                if unqualified and unqualified not in seen:
                    candidates.append(unqualified)
                    seen.add(unqualified)
        return tuple(candidates)

    def _build_table_profile_entries(
        self,
        column_row: Mapping[str, Any],
        value_rows: Sequence[Mapping[str, Any]] | None,
    ) -> tuple[Mapping[str, Any], Mapping[str, Any]]:
        metrics_payload = self._column_metrics_from_row(column_row)
        column_name = column_row.get("column_name")
        column_entry: dict[str, Any] = {
            "column_name": column_name,
            "column": column_name,
            "name": column_name,
            "data_type": column_row.get("data_type"),
            "metrics": metrics_payload,
        }
        if metrics_payload.get("null_ratio") is not None:
            column_entry["null_ratio"] = metrics_payload["null_ratio"]

        top_values_payload, histogram_payload = self._partition_value_rows(value_rows or [])
        if top_values_payload:
            column_entry["top_values"] = top_values_payload
        if histogram_payload:
            column_entry["histogram"] = histogram_payload

        table_label = column_row.get("table_name")
        table_entry: dict[str, Any] = {
            "table_name": table_label,
            "name": table_label,
        }
        qualified_name = column_row.get("qualified_name")
        if qualified_name:
            table_entry["qualified_name"] = qualified_name
            table_entry["physical_name"] = qualified_name

        schema_name = column_row.get("schema_name")
        if schema_name:
            table_entry["schema_name"] = schema_name

        return table_entry, column_entry

    def _column_metrics_from_row(self, row: Mapping[str, Any]) -> dict[str, Any]:
        metrics: dict[str, Any] = {
            "row_count": row.get("row_count"),
            "distinct_count": row.get("distinct_count"),
            "null_count": row.get("null_count"),
            "non_null_count": row.get("non_null_count"),
            "min_value": row.get("min_value"),
            "max_value": row.get("max_value"),
            "avg_value": row.get("avg_value"),
            "stddev_value": row.get("stddev_value"),
            "median_value": row.get("median_value"),
            "p95_value": row.get("p95_value"),
            "true_count": row.get("true_count"),
            "false_count": row.get("false_count"),
            "min_length": row.get("min_length"),
            "max_length": row.get("max_length"),
            "avg_length": row.get("avg_length"),
            "non_ascii_ratio": row.get("non_ascii_ratio"),
            "min_date": row.get("min_date"),
            "max_date": row.get("max_date"),
            "date_span_days": row.get("date_span_days"),
            "null_ratio": None,
        }

        row_count = row.get("row_count")
        null_count = row.get("null_count")
        if row_count and null_count is not None:
            try:
                metrics["null_ratio"] = float(null_count) / float(row_count)
            except (TypeError, ValueError, ZeroDivisionError):
                metrics["null_ratio"] = None

        if metrics.get("non_null_count") is None and row_count is not None and null_count is not None:
            try:
                metrics["non_null_count"] = max(int(row_count) - int(null_count), 0)
            except (TypeError, ValueError):
                metrics["non_null_count"] = None

        metrics_json = row.get("metrics_json")
        if isinstance(metrics_json, str) and metrics_json.strip():
            try:
                parsed = json.loads(metrics_json)
            except (json.JSONDecodeError, TypeError):
                parsed = None
            if isinstance(parsed, Mapping):
                for key, value in parsed.items():
                    metrics.setdefault(key, value)

        return metrics

    def _partition_value_rows(
        self,
        value_rows: Sequence[Mapping[str, Any]],
    ) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
        top_values: list[dict[str, Any]] = []
        histogram: list[dict[str, Any]] = []
        for row in value_rows:
            bucket_label = row.get("bucket_label")
            lower = row.get("bucket_lower_bound")
            upper = row.get("bucket_upper_bound")
            has_bucket = bucket_label is not None or lower is not None or upper is not None
            if has_bucket:
                histogram.append(
                    {
                        "label": bucket_label,
                        "lower": lower,
                        "upper": upper,
                        "count": row.get("frequency"),
                    }
                )
            else:
                top_values.append(
                    {
                        "value": row.get("value"),
                        "count": row.get("frequency"),
                        "percentage": row.get("relative_freq"),
                    }
                )
        return top_values, histogram


    def _build_column_metrics(self, column_entry: Mapping[str, Any] | None) -> list[dict[str, Any]]:
        if not isinstance(column_entry, Mapping):
            return []

        metrics_source: dict[str, Any] = {}

        for key in ("metrics", "summary"):
            nested = column_entry.get(key)
            if isinstance(nested, Mapping):
                metrics_source.update(nested)

        for key, value in column_entry.items():
            if isinstance(value, (Mapping, list)):
                continue
            if key in {"column_name", "column", "name", "data_type", "dataType", "column_type", "columnType"}:
                continue
            metrics_source.setdefault(key, value)

        metrics: list[dict[str, Any]] = []
        seen: set[str] = set()

        for key in _COLUMN_METRIC_ORDER:
            if key not in metrics_source or metrics_source[key] is None:
                continue
            metrics.append(
                self._metric_entry(
                    key,
                    _COLUMN_METRIC_LABELS.get(key, key.replace("_", " ").title()),
                    metrics_source[key],
                    "%" if key in _PERCENT_METRIC_KEYS else None,
                )
            )
            seen.add(key)

        for key, value in metrics_source.items():
            if key in seen or key in _COLUMN_METRIC_ORDER:
                continue
            if value is None:
                continue
            metrics.append(
                self._metric_entry(
                    key,
                    _COLUMN_METRIC_LABELS.get(key, key.replace("_", " ").title()),
                    value,
                    "%" if key in _PERCENT_METRIC_KEYS else None,
                )
            )

        return metrics

    @staticmethod
    def _metric_entry(key: str, label: str, value: Any, unit: str | None = None) -> dict[str, Any]:
        return {
            "key": key,
            "label": label,
            "value": value,
            "formatted": TestGenClient._format_metric_value(value, unit),
            "unit": unit,
        }

    @staticmethod
    def _format_metric_value(value: Any, unit: str | None) -> str | None:
        if value is None:
            return None
        if isinstance(value, (int, float)):
            numeric = float(value)
            if unit == "%":
                if math.isfinite(numeric) and -1.0 <= numeric <= 1.0:
                    numeric *= 100.0
                return f"{numeric:.2f}%"
            if isinstance(value, int) or numeric.is_integer():
                return f"{int(round(numeric)):,}"
            if math.isfinite(numeric):
                return f"{numeric:,.4f}".rstrip("0").rstrip(".")
            return str(numeric)
        if isinstance(value, datetime):
            return value.isoformat()
        return str(value)

    def _build_top_values(self, column_entry: Mapping[str, Any] | None) -> list[dict[str, Any]]:
        if not isinstance(column_entry, Mapping):
            return []

        values = column_entry.get("top_values")
        if not isinstance(values, list):
            values = column_entry.get("frequencies")
        if not isinstance(values, list):
            values = column_entry.get("topValues")

        if not isinstance(values, list):
            return []

        result: list[dict[str, Any]] = []
        for item in values:
            if not isinstance(item, Mapping):
                continue
            percentage = self._normalize_percentage(item.get("percentage") or item.get("percent"))
            result.append(
                {
                    "value": item.get("value", item.get("label")),
                    "count": item.get("count"),
                    "percentage": percentage,
                }
            )
        return result

    @staticmethod
    def _normalize_percentage(value: Any) -> float | None:
        if value is None:
            return None
        try:
            numeric = float(value)
        except (TypeError, ValueError):
            return None
        if -1.0 <= numeric <= 1.0:
            numeric *= 100.0
        return numeric

    def _build_histogram(self, column_entry: Mapping[str, Any] | None) -> list[dict[str, Any]]:
        if not isinstance(column_entry, Mapping):
            return []

        bins = column_entry.get("histogram")
        if not isinstance(bins, list):
            bins = column_entry.get("bins")
        if not isinstance(bins, list):
            return []

        result: list[dict[str, Any]] = []
        for item in bins:
            if not isinstance(item, Mapping):
                continue
            label = item.get("label")
            lower = item.get("lower") if "lower" in item else item.get("start")
            upper = item.get("upper") if "upper" in item else item.get("end")
            if label is None and (lower is not None or upper is not None):
                label = f"{lower} – {upper}"
            result.append(
                {
                    "label": label if label is not None else "",
                    "count": item.get("count"),
                    "lower": lower,
                    "upper": upper,
                }
            )
        return result

    @staticmethod
    def _extract_table_name(table_entry: Mapping[str, Any] | None) -> str | None:
        if not isinstance(table_entry, Mapping):
            return None
        for key in ("table_name", "tableName", "name", "physical_name", "physicalName"):
            value = table_entry.get(key)
            if value:
                return str(value)
        return None

    @staticmethod
    def _extract_column_name(column_entry: Mapping[str, Any] | None) -> str | None:
        if not isinstance(column_entry, Mapping):
            return None
        for key in ("column_name", "column", "name", "columnName"):
            value = column_entry.get(key)
            if value:
                return str(value)
        return None

    @staticmethod
    def _extract_data_type(column_entry: Mapping[str, Any] | None) -> str | None:
        if not isinstance(column_entry, Mapping):
            return None
        for key in ("data_type", "dataType", "column_type", "columnType", "type"):
            value = column_entry.get(key)
            if value:
                return str(value)
        nested = column_entry.get("metrics")
        if isinstance(nested, Mapping):
            for key in ("data_type", "column_type"):
                value = nested.get(key)
                if value:
                    return str(value)
        return None

    @staticmethod
    def _normalize_name(value: str | None) -> str:
        return value.strip().lower() if isinstance(value, str) else ""

    def _with_profiling_retry(self, operation: Callable[[], T]) -> T:
        # Ensure required profiling columns exist before issuing dependent queries.
        self.ensure_profiling_support()
        try:
            return operation()
        except TestGenClientError as exc:
            if self._should_retry_profiling_operation(exc):
                self._profiling_support_verified = False
                self.ensure_profiling_support()
                return operation()
            raise

    def _should_retry_profiling_operation(self, error: TestGenClientError) -> bool:
        message = str(error).lower()
        return "profiling_job_id" in message or "databricks_run_id" in message

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
