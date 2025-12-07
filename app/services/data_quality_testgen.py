"""Client adapter that proxies TestGen workflows into Databricks metadata tables."""

from __future__ import annotations

import json
import logging
import math
import uuid
from decimal import Decimal
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

_NUMERIC_GENERAL_TYPES = {
    "numeric",
    "number",
    "integer",
    "int",
    "bigint",
    "smallint",
    "tinyint",
    "decimal",
    "double",
    "float",
    "real",
    "long",
    "short",
}

_NUMERIC_DISTRIBUTION_LABELS = {
    "nonZero": "Non-zero values",
    "zero": "Zero values",
    "null": "Null values",
}


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
                    except SQLAlchemyError:
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
            f"SELECT project_key, name, description, sql_flavor, workspace_id FROM {projects_table} ORDER BY name"
        )
        return self._fetch(statement)

    def list_connections(self, project_key: str) -> list[dict[str, Any]]:
        connections_table = _format_table(self._params.catalog, self._schema, "dq_connections")
        statement = text(
            f"SELECT connection_id, project_key, name, catalog, schema_name, http_path, managed_credentials_ref, is_active "
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
            "conns.name AS connection_name, conns.project_key, conns.catalog, conns.schema_name, "
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

    def fetch_table_characteristics(
        self,
        *,
        table_ids: Sequence[str] | None = None,
        table_group_ids: Sequence[str] | None = None,
    ) -> list[dict[str, Any]]:
        table_chars_table = _format_table(self._params.catalog, self._schema, "dq_data_table_chars")
        filters: list[str] = []
        params: dict[str, Any] = {}

        if table_ids:
            filters.append("table_id IN :table_ids")
            params["table_ids"] = tuple(table_ids)
        if table_group_ids:
            filters.append("table_group_id IN :table_group_ids")
            params["table_group_ids"] = tuple(table_group_ids)

        where_clause = f" WHERE {' AND '.join(filters)}" if filters else ""
        statement = text(
            f"""
            SELECT
                table_id,
                table_group_id,
                schema_name,
                table_name,
                record_count,
                column_count,
                latest_anomaly_ct,
                dq_score_profiling,
                latest_run_completed_at
            FROM {table_chars_table}{where_clause}
            """
        )

        if table_ids:
            statement = statement.bindparams(bindparam("table_ids", expanding=True))
        if table_group_ids:
            statement = statement.bindparams(bindparam("table_group_ids", expanding=True))

        return self._with_profiling_retry(lambda: self._fetch(statement, params))

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
                p.dq_score_profiling,
                p.databricks_run_id,
                g.name AS table_group_name,
                g.description AS table_group_description,
                g.connection_id,
                c.name AS connection_name,
                c.catalog,
                c.schema_name,
                c.project_key
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
                c.project_key
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

    def export_profiling_payload(
        self,
        table_group_id: str,
        *,
        profile_run_id: str | None = None,
    ) -> dict[str, Any] | None:
        """Reconstruct a profiling payload for the latest (or requested) run."""

        normalized_group = (table_group_id or "").strip()
        if not normalized_group:
            raise ValueError("table_group_id is required to export a profiling payload")

        run_row: Mapping[str, Any] | None = None
        if profile_run_id:
            run_row = self._fetch_profile_run_record(profile_run_id, normalized_group)
        if run_row is None:
            run_row = self._latest_completed_profile_run(normalized_group)
        if not run_row:
            return None

        resolved_run_id = (run_row.get("profile_run_id") or "").strip()
        if not resolved_run_id:
            return None

        tables = self._build_tables_payload(normalized_group, resolved_run_id)
        if not tables:
            return None

        summary = self._build_profile_summary_payload(run_row)
        payload: dict[str, Any] = {
            "table_group_id": normalized_group,
            "profile_run_id": resolved_run_id,
            "summary": summary,
            "tables": tables,
        }
        return payload

    def list_profile_run_anomalies(self, profile_run_id: str) -> list[dict[str, Any]]:
        return self._fetch_profile_anomalies(profile_run_id, None)

    def delete_profile_runs(self, profile_run_ids: Sequence[str]) -> int:
        normalized = tuple(sorted({(run_id or "").strip() for run_id in profile_run_ids if (run_id or "").strip()}))
        if not normalized:
            return 0

        detail_tables = (
            "dq_profile_anomaly_results",
            "dq_profile_anomalies",
            "dq_profile_results",
            "dq_profile_column_values",
            "dq_profile_columns",
            "dq_profile_operations",
        )

        statements = [
            text(
                f"DELETE FROM {_format_table(self._params.catalog, self._schema, table_name)} "
                "WHERE profile_run_id IN :profile_run_ids"
            ).bindparams(bindparam("profile_run_ids", expanding=True))
            for table_name in detail_tables
        ]

        profiles_table = _format_table(self._params.catalog, self._schema, "dq_profiles")
        delete_profiles_statement = text(
            f"DELETE FROM {profiles_table} WHERE profile_run_id IN :profile_run_ids"
        ).bindparams(bindparam("profile_run_ids", expanding=True))

        try:
            with self._get_engine().begin() as connection:
                for statement in statements:
                    connection.execute(statement, {"profile_run_ids": normalized})
                result = connection.execute(delete_profiles_statement, {"profile_run_ids": normalized})
                return int(result.rowcount or 0)
        except SQLAlchemyError as exc:  # pragma: no cover - defensive
            logger.error("Failed to delete profiling runs: %s", exc)
            raise TestGenClientError(str(exc)) from exc

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

    def _fetch_profile_run_record(
        self,
        profile_run_id: str,
        table_group_id: str | None = None,
    ) -> dict[str, Any] | None:
        profiles_table = _format_table(self._params.catalog, self._schema, "dq_profiles")
        filters = ["profile_run_id = :profile_run_id"]
        params: dict[str, Any] = {"profile_run_id": profile_run_id}
        if table_group_id:
            filters.append("table_group_id = :table_group_id")
            params["table_group_id"] = table_group_id
        statement = text(
            f"SELECT profile_run_id, table_group_id, status, started_at, completed_at, row_count, anomaly_count, databricks_run_id "
            f"FROM {profiles_table} WHERE {' AND '.join(filters)} LIMIT 1"
        )
        rows = self._fetch(statement, params)
        return rows[0] if rows else None

    def _build_profile_summary_payload(self, row: Mapping[str, Any]) -> dict[str, Any]:
        summary: dict[str, Any] = {
            "profile_run_id": row.get("profile_run_id"),
            "table_group_id": row.get("table_group_id"),
            "status": row.get("status"),
            "started_at": self._isoformat(row.get("started_at")),
            "completed_at": self._isoformat(row.get("completed_at")),
            "databricks_run_id": row.get("databricks_run_id"),
        }
        row_count = self._coerce_int(row.get("row_count"))
        if row_count is not None:
            summary["row_count"] = row_count
        anomaly_count = self._coerce_int(row.get("anomaly_count"))
        if anomaly_count is not None:
            summary["anomaly_count"] = anomaly_count
        return summary

    def _build_tables_payload(self, table_group_id: str, profile_run_id: str) -> list[dict[str, Any]]:
        table_catalog = self._fetch_table_catalog(table_group_id)
        result_rows = self._fetch_profile_result_rows(profile_run_id)
        if not result_rows:
            return []

        value_rows = self._fetch_profile_value_rows(profile_run_id)
        grouped_values = self._group_value_rows(value_rows)
        anomaly_rows = self._fetch_profile_anomaly_rows(profile_run_id)
        column_anomalies, table_anomalies = self._group_anomaly_rows(anomaly_rows)

        tables: dict[tuple[str, str], dict[str, Any]] = {}
        for row in result_rows:
            table_entry = self._ensure_table_entry(tables, row, table_catalog, table_group_id)
            if table_entry is None:
                continue
            column_entry = self._build_column_entry(row, grouped_values, column_anomalies)
            if column_entry is None:
                continue
            table_entry.setdefault("columns", []).append(column_entry)
            row_count = column_entry.get("row_count")
            if row_count is not None and table_entry.get("row_count") is None:
                table_entry["row_count"] = row_count
                table_entry.setdefault("metrics", {})["row_count"] = row_count

        for key, anomalies in table_anomalies.items():
            table_entry = tables.get(key)
            if table_entry is None:
                metadata = self._lookup_table_metadata(key, table_catalog)
                if not metadata:
                    continue
                schema_name, table_name = self._split_schema_table(
                    metadata.get("schema_name"),
                    metadata.get("table_name"),
                )
                table_entry = {
                    "table_id": metadata.get("table_id"),
                    "table_group_id": table_group_id,
                    "schema_name": schema_name,
                    "table_name": table_name,
                    "columns": [],
                    "anomalies": [],
                }
                tables[key] = table_entry
            table_entry["anomalies"] = anomalies

        for table_entry in tables.values():
            table_entry.setdefault("columns", [])
            table_entry.setdefault("anomalies", [])
            metrics = table_entry.setdefault("metrics", {})
            metrics["column_count"] = len(table_entry["columns"])

        return list(tables.values())

    def _fetch_table_catalog(self, table_group_id: str) -> dict[str, dict[str, Any]]:
        tables_table = _format_table(self._params.catalog, self._schema, "dq_tables")
        statement = text(
            f"SELECT table_id, table_group_id, schema_name, table_name FROM {tables_table} WHERE table_group_id = :table_group_id"
        )
        rows = self._fetch(statement, {"table_group_id": table_group_id})
        by_id: dict[str, Mapping[str, Any]] = {}
        by_name: dict[str, Mapping[str, Any]] = {}
        for row in rows:
            table_id = row.get("table_id")
            if table_id:
                by_id[str(table_id)] = row
            normalized = self._normalize_table_name(row.get("schema_name"), row.get("table_name"))
            if normalized:
                by_name[normalized] = row
        return {"by_id": by_id, "by_name": by_name}

    def _fetch_profile_result_rows(self, profile_run_id: str) -> list[dict[str, Any]]:
        results_table = _format_table(self._params.catalog, self._schema, "dq_profile_results")
        statement = text(
            f"SELECT result_id, profile_run_id, table_id, column_id, schema_name, table_name, column_name, data_type, general_type, "
            "record_count, null_count, distinct_count, min_value, max_value, avg_value, stddev_value, percentiles_json, top_values_json, metrics_json "
            f"FROM {results_table} WHERE profile_run_id = :profile_run_id "
            "ORDER BY schema_name, table_name, column_name"
        )
        return self._fetch(statement, {"profile_run_id": profile_run_id})

    def _fetch_profile_value_rows(self, profile_run_id: str) -> list[dict[str, Any]]:
        values_table = _format_table(self._params.catalog, self._schema, "dq_profile_column_values")
        order_clause = (
            "ORDER BY schema_name, table_name, column_name, "
            "CASE WHEN bucket_label IS NULL AND bucket_lower_bound IS NULL AND bucket_upper_bound IS NULL THEN 0 ELSE 1 END, "
            "COALESCE(rank, 2147483647), bucket_label"
        )
        statement = text(
            f"SELECT profile_run_id, schema_name, table_name, column_name, value, frequency, relative_freq, rank, "
            f"bucket_label, bucket_lower_bound, bucket_upper_bound FROM {values_table} "
            "WHERE profile_run_id = :profile_run_id "
            f"{order_clause}"
        )
        try:
            return self._fetch(statement, {"profile_run_id": profile_run_id})
        except TestGenClientError as exc:
            logger.debug("Profile column values lookup failed: %s", exc)
            return []

    def _fetch_profile_anomaly_rows(self, profile_run_id: str) -> list[dict[str, Any]]:
        results_table = _format_table(self._params.catalog, self._schema, "dq_profile_anomaly_results")
        statement = text(
            f"SELECT profile_run_id, table_id, column_id, table_name, column_name, anomaly_type_id, severity, likelihood, detail, pii_risk, dq_dimension, detected_at "
            f"FROM {results_table} WHERE profile_run_id = :profile_run_id"
        )
        try:
            rows = self._fetch(statement, {"profile_run_id": profile_run_id})
        except TestGenClientError as exc:
            logger.debug("Profile anomaly results lookup failed: %s", exc)
            rows = []
        if rows:
            return rows

        legacy_table = _format_table(self._params.catalog, self._schema, "dq_profile_anomalies")
        legacy_statement = text(
            f"SELECT profile_run_id, NULL AS table_id, NULL AS column_id, table_name, column_name, anomaly_type AS anomaly_type_id, severity, NULL AS likelihood, description AS detail, NULL AS pii_risk, NULL AS dq_dimension, detected_at "
            f"FROM {legacy_table} WHERE profile_run_id = :profile_run_id"
        )
        try:
            return self._fetch(legacy_statement, {"profile_run_id": profile_run_id})
        except TestGenClientError as exc:
            logger.debug("Legacy profile anomaly lookup failed: %s", exc)
            return []

    def _group_value_rows(
        self,
        rows: Sequence[Mapping[str, Any]],
    ) -> dict[tuple[str, str], dict[str, list[dict[str, Any]]]]:
        grouped: dict[tuple[str, str], dict[str, list[dict[str, Any]]]] = {}
        for row in rows:
            key = self._column_lookup_key(row)
            if key is None:
                continue
            bucket_label = row.get("bucket_label")
            lower = self._coerce_float(row.get("bucket_lower_bound"))
            upper = self._coerce_float(row.get("bucket_upper_bound"))
            has_bucket = bucket_label is not None or lower is not None or upper is not None
            payload = {
                "value": row.get("value"),
                "count": self._coerce_int(row.get("frequency")),
                "percentage": self._coerce_float(row.get("relative_freq")),
                "label": bucket_label,
                "lower": lower,
                "upper": upper,
            }
            entry = grouped.setdefault(key, {"top_values": [], "histogram": []})
            target = entry["histogram" if has_bucket else "top_values"]
            target.append({k: v for k, v in payload.items() if v is not None})
        return grouped

    def _group_anomaly_rows(
        self,
        rows: Sequence[Mapping[str, Any]],
    ) -> tuple[dict[tuple[str, str], list[dict[str, Any]]], dict[tuple[str, str], list[dict[str, Any]]]]:
        column_anomalies: dict[tuple[str, str], list[dict[str, Any]]] = {}
        table_anomalies: dict[tuple[str, str], list[dict[str, Any]]] = {}
        for row in rows:
            entry = self._build_anomaly_entry(row)
            if entry is None:
                continue
            column_key = self._column_lookup_key(row)
            if column_key is not None:
                column_anomalies.setdefault(column_key, []).append(entry)
                continue
            table_key = self._table_lookup_key(row)
            if table_key is not None:
                table_anomalies.setdefault(table_key, []).append(entry)
        return column_anomalies, table_anomalies

    def _build_anomaly_entry(self, row: Mapping[str, Any]) -> dict[str, Any] | None:
        anomaly_type = row.get("anomaly_type_id") or row.get("anomaly_type")
        severity = row.get("severity")
        detail = row.get("detail") or row.get("description")
        if not anomaly_type and not severity and not detail:
            return None
        entry: dict[str, Any] = {
            "anomaly_type_id": anomaly_type,
            "severity": severity,
            "likelihood": row.get("likelihood"),
            "detail": detail,
            "pii_risk": row.get("pii_risk"),
            "dq_dimension": row.get("dq_dimension"),
        }
        detected_at = self._isoformat(row.get("detected_at"))
        if detected_at is not None:
            entry["detected_at"] = detected_at
        column_name = row.get("column_name")
        if column_name:
            entry["column_name"] = column_name
        return entry

    def _ensure_table_entry(
        self,
        tables: dict[tuple[str, str], dict[str, Any]],
        row: Mapping[str, Any],
        table_catalog: Mapping[str, Mapping[str, Any]],
        table_group_id: str,
    ) -> dict[str, Any] | None:
        key = self._table_lookup_key(row)
        if key is None:
            return None
        if key in tables:
            return tables[key]

        metadata = self._lookup_table_metadata(key, table_catalog)
        schema_name, table_name = self._split_schema_table(row.get("schema_name"), row.get("table_name"))
        entry: dict[str, Any] = {
            "table_id": metadata.get("table_id") or row.get("table_id"),
            "table_group_id": table_group_id,
            "schema_name": schema_name or metadata.get("schema_name"),
            "table_name": table_name or metadata.get("table_name"),
            "columns": [],
            "anomalies": [],
        }
        tables[key] = entry
        return entry

    def _lookup_table_metadata(
        self,
        key: tuple[str, str],
        table_catalog: Mapping[str, Mapping[str, Any]],
    ) -> Mapping[str, Any]:
        kind, value = key
        if kind == "id":
            return table_catalog.get("by_id", {}).get(value, {})
        return table_catalog.get("by_name", {}).get(value, {})

    @staticmethod
    def _split_schema_table(
        schema_name: Any,
        table_name: Any,
    ) -> tuple[str | None, str | None]:
        schema = schema_name if isinstance(schema_name, str) and schema_name.strip() else None
        table = table_name if isinstance(table_name, str) and table_name.strip() else None
        if schema is None and isinstance(table, str) and "." in table:
            schema, _, final = table.rpartition(".")
            table = final
        return schema, table

    @staticmethod
    def _normalize_table_name(schema_name: Any, table_name: Any) -> str:
        schema, table = TestGenClient._split_schema_table(schema_name, table_name)
        normalized_table = (table or "").strip().lower()
        if not normalized_table:
            return ""
        normalized_schema = (schema or "").strip().lower()
        return f"{normalized_schema}.{normalized_table}" if normalized_schema else normalized_table

    def _table_lookup_key(self, row: Mapping[str, Any]) -> tuple[str, str] | None:
        table_id = row.get("table_id")
        if table_id:
            return ("id", str(table_id))
        normalized = self._normalize_table_name(row.get("schema_name"), row.get("table_name"))
        if not normalized:
            return None
        return ("name", normalized)

    def _column_lookup_key(self, row: Mapping[str, Any]) -> tuple[str, str] | None:
        column_id = row.get("column_id")
        if column_id:
            return ("id", str(column_id))
        return self._column_name_key(row)

    def _column_name_key(self, row: Mapping[str, Any]) -> tuple[str, str] | None:
        normalized_table = self._normalize_table_name(row.get("schema_name"), row.get("table_name"))
        column_name = (row.get("column_name") or "").strip().lower()
        if not normalized_table or not column_name:
            return None
        return ("name", f"{normalized_table}::{column_name}")

    def _build_column_entry(
        self,
        row: Mapping[str, Any],
        value_rows: Mapping[tuple[str, str], dict[str, list[dict[str, Any]]]],
        column_anomalies: Mapping[tuple[str, str], list[dict[str, Any]]],
    ) -> dict[str, Any] | None:
        column_name = row.get("column_name")
        if not column_name:
            return None
        schema_name, table_name = self._split_schema_table(row.get("schema_name"), row.get("table_name"))
        metrics = self._build_column_metrics_payload(row)
        entry: dict[str, Any] = {
            "column_id": row.get("column_id"),
            "column_name": column_name,
            "schema_name": schema_name,
            "table_name": table_name,
            "data_type": row.get("data_type"),
            "general_type": row.get("general_type"),
        }
        if metrics:
            entry["metrics"] = metrics
        text_profile_payload = metrics.get("text_profile") if metrics else None
        if isinstance(text_profile_payload, Mapping):
            entry["text_profile"] = text_profile_payload
        for key in (
            "row_count",
            "null_count",
            "distinct_count",
            "non_null_count",
            "avg_value",
            "stddev_value",
            "median_value",
            "p95_value",
        ):
            value = metrics.get(key)
            if value is not None:
                entry[key] = value
        for key in ("min_value", "max_value"):
            value = row.get(key)
            if value is not None:
                entry[key] = value

        lookup_key = self._column_lookup_key(row)
        name_lookup_key = self._column_name_key(row)
        grouped = value_rows.get(lookup_key) if lookup_key else None
        anomalies = column_anomalies.get(lookup_key) if lookup_key else None
        if grouped is None and name_lookup_key and name_lookup_key != lookup_key:
            grouped = value_rows.get(name_lookup_key)
        if not anomalies and name_lookup_key and name_lookup_key != lookup_key:
            anomalies = column_anomalies.get(name_lookup_key)
        if grouped:
            if grouped["top_values"]:
                entry["top_values"] = grouped["top_values"]
            if grouped["histogram"]:
                entry["histogram"] = grouped["histogram"]
        if anomalies:
            entry["anomalies"] = anomalies

        if "top_values" not in entry and row.get("top_values_json"):
            parsed = self._parse_json_field(row.get("top_values_json"))
            if isinstance(parsed, Mapping):
                top_values = parsed.get("top_values")
                histogram = parsed.get("histogram")
                if top_values:
                    entry["top_values"] = top_values
                if histogram:
                    entry["histogram"] = histogram

        numeric_profile_payload = self._build_numeric_profile_payload(entry, metrics, row)
        if numeric_profile_payload:
            entry["numeric_profile"] = numeric_profile_payload

        return entry

    def _build_numeric_profile_payload(
        self,
        entry: Mapping[str, Any],
        metrics: Mapping[str, Any],
        row: Mapping[str, Any],
    ) -> dict[str, Any] | None:
        if not self._is_numeric_column(entry, row):
            return None

        stats, value_count, zero_count, null_count = self._extract_numeric_profile_stats(entry, metrics, row)
        distribution_bars = self._build_numeric_distribution_bars(value_count, zero_count, null_count)
        box_plot = self._build_numeric_box_plot(entry, metrics, row)
        histogram = entry.get("histogram") or []
        top_values = entry.get("top_values") or []

        if not stats and not distribution_bars and box_plot is None and not histogram and not top_values:
            return None

        payload: dict[str, Any] = {}
        if stats:
            payload["stats"] = stats
        if distribution_bars:
            payload["distribution_bars"] = distribution_bars
        if box_plot:
            payload["box_plot"] = box_plot
        if histogram:
            payload["histogram"] = histogram
        if top_values:
            payload["top_values"] = top_values
        return payload

    def _is_numeric_column(self, entry: Mapping[str, Any], row: Mapping[str, Any]) -> bool:
        general_type = (entry.get("general_type") or row.get("general_type") or "").strip().lower()
        if general_type in _NUMERIC_GENERAL_TYPES:
            return True
        data_type = (entry.get("data_type") or row.get("data_type") or "").strip().lower()
        if not data_type:
            return False
        return any(token in data_type for token in _NUMERIC_GENERAL_TYPES)

    def _extract_numeric_profile_stats(
        self,
        entry: Mapping[str, Any],
        metrics: Mapping[str, Any],
        row: Mapping[str, Any],
    ) -> tuple[dict[str, Any] | None, int | None, int | None, int | None]:
        stats: dict[str, Any] = {}
        record_count = self._coerce_int(metrics.get("row_count") or row.get("record_count"))
        null_count = self._coerce_int(metrics.get("null_count") or row.get("null_count"))
        value_count = self._coerce_int(metrics.get("non_null_count"))
        if value_count is None and record_count is not None and null_count is not None:
            value_count = max(record_count - null_count, 0)
        distinct_count = self._coerce_int(metrics.get("distinct_count"))
        average = self._coerce_float(metrics.get("avg_value") or entry.get("avg_value"))
        stddev = self._coerce_float(metrics.get("stddev_value") or entry.get("stddev_value"))
        minimum = self._coerce_float(entry.get("min_value") or metrics.get("min_value"))
        maximum = self._coerce_float(entry.get("max_value") or metrics.get("max_value"))
        minimum_positive = self._coerce_float(
            metrics.get("minimum_positive")
            or metrics.get("min_positive")
            or metrics.get("min_positive_value")
            or metrics.get("minimumPositive")
            or metrics.get("minimum_positive_value")
        )
        percentile_25 = self._coerce_float(metrics.get("p25") or metrics.get("percentile_25"))
        percentile_75 = self._coerce_float(metrics.get("p75") or metrics.get("percentile_75"))
        median = self._coerce_float(metrics.get("median_value") or metrics.get("median") or metrics.get("p50"))
        zero_count = self._coerce_int(metrics.get("zero_count") or metrics.get("zeros_count"))

        if record_count is not None:
            stats["record_count"] = record_count
        if value_count is not None:
            stats["value_count"] = value_count
        if distinct_count is not None:
            stats["distinct_count"] = distinct_count
        if average is not None:
            stats["average"] = average
        if stddev is not None:
            stats["stddev"] = stddev
        if minimum is not None:
            stats["minimum"] = minimum
        if minimum_positive is not None:
            stats["minimum_positive"] = minimum_positive
        if maximum is not None:
            stats["maximum"] = maximum
        if percentile_25 is not None:
            stats["percentile_25"] = percentile_25
        if median is not None:
            stats["median"] = median
        if percentile_75 is not None:
            stats["percentile_75"] = percentile_75
        if zero_count is not None:
            stats["zero_count"] = zero_count
        if null_count is not None:
            stats["null_count"] = null_count

        return (stats or None, value_count, zero_count, null_count)

    def _build_numeric_distribution_bars(
        self,
        value_count: int | None,
        zero_count: int | None,
        null_count: int | None,
    ) -> list[dict[str, Any]]:
        if value_count is None or zero_count is None:
            return []

        non_zero_count = max(value_count - zero_count, 0)
        total = sum(count for count in (non_zero_count, zero_count, null_count) if count is not None)
        if total <= 0:
            total = None

        bars: list[dict[str, Any]] = []
        for key, count in ("nonZero", non_zero_count), ("zero", zero_count), ("null", null_count):
            if count is None:
                continue
            percentage = self._safe_ratio(count, total) if total else None
            bars.append(
                {
                    "key": key,
                    "label": _NUMERIC_DISTRIBUTION_LABELS.get(key),
                    "count": count,
                    "percentage": percentage,
                }
            )
        return bars

    def _build_numeric_box_plot(
        self,
        entry: Mapping[str, Any],
        metrics: Mapping[str, Any],
        row: Mapping[str, Any],
    ) -> dict[str, Any] | None:
        min_value = self._coerce_float(entry.get("min_value") or row.get("min_value"))
        max_value = self._coerce_float(entry.get("max_value") or row.get("max_value"))
        p25 = self._coerce_float(metrics.get("p25") or metrics.get("percentile_25"))
        p75 = self._coerce_float(metrics.get("p75") or metrics.get("percentile_75"))
        median = self._coerce_float(metrics.get("median_value") or metrics.get("median") or metrics.get("p50"))
        mean = self._coerce_float(metrics.get("avg_value") or entry.get("avg_value"))
        std_dev = self._coerce_float(metrics.get("stddev_value") or entry.get("stddev_value"))

        if not any(value is not None for value in (min_value, max_value, p25, p75, median, mean, std_dev)):
            return None

        return {
            "min": min_value,
            "p25": p25,
            "median": median,
            "p75": p75,
            "max": max_value,
            "mean": mean,
            "std_dev": std_dev,
        }

    def _build_column_metrics_payload(self, row: Mapping[str, Any]) -> dict[str, Any]:
        metrics: dict[str, Any] = {}
        for key, source in (
            ("row_count", row.get("record_count")),
            ("null_count", row.get("null_count")),
            ("distinct_count", row.get("distinct_count")),
            ("non_null_count", row.get("non_null_count")),
        ):
            value = self._coerce_int(source)
            if value is not None:
                metrics[key] = value
        if "non_null_count" not in metrics and metrics.get("row_count") is not None and metrics.get("null_count") is not None:
            metrics["non_null_count"] = max(metrics["row_count"] - metrics["null_count"], 0)
        metrics["min_value"] = row.get("min_value")
        metrics["max_value"] = row.get("max_value")
        for key, source in (
            ("avg_value", row.get("avg_value")),
            ("stddev_value", row.get("stddev_value")),
        ):
            value = self._coerce_float(source)
            if value is not None:
                metrics[key] = value
        percentiles = self._parse_json_field(row.get("percentiles_json"))
        if isinstance(percentiles, Mapping):
            for key, value in percentiles.items():
                coerced = self._coerce_float(value)
                if coerced is not None:
                    metrics[key] = coerced
        metrics_json = self._parse_json_field(row.get("metrics_json"))
        if isinstance(metrics_json, Mapping):
            for key, value in metrics_json.items():
                if value is not None:
                    metrics.setdefault(key, value)
        return {key: value for key, value in metrics.items() if value is not None}

    @staticmethod
    def _parse_json_field(value: Any) -> Any:
        if not isinstance(value, str):
            return None
        text = value.strip()
        if not text:
            return None
        try:
            return json.loads(text)
        except json.JSONDecodeError:
            return None

    @staticmethod
    def _coerce_number(value: Any) -> float | int | None:
        if value is None:
            return None
        if isinstance(value, bool):
            return int(value)
        if isinstance(value, (int, float)):
            return value
        if isinstance(value, Decimal):
            if value == value.to_integral_value():
                try:
                    return int(value)
                except (TypeError, ValueError):
                    return float(value)
            return float(value)
        if isinstance(value, str) and value.strip():
            try:
                return int(value)
            except ValueError:
                try:
                    return float(value)
                except ValueError:
                    return None
        try:
            return float(value)
        except (TypeError, ValueError):
            return None

    @staticmethod
    def _coerce_int(value: Any) -> int | None:
        number = TestGenClient._coerce_number(value)
        if number is None:
            return None
        try:
            return int(number)
        except (TypeError, ValueError):
            return None

    @staticmethod
    def _coerce_float(value: Any) -> float | None:
        number = TestGenClient._coerce_number(value)
        if number is None:
            return None
        try:
            return float(number)
        except (TypeError, ValueError):
            return None

    @staticmethod
    def _safe_ratio(numerator: int | float | None, denominator: int | float | None) -> float | None:
        if numerator is None or denominator in (None, 0):
            return None
        try:
            value = float(numerator) / float(denominator)
        except (TypeError, ValueError, ZeroDivisionError):
            return None
        if math.isnan(value) or math.isinf(value):
            return None
        return max(value, 0.0)

    @staticmethod
    def _isoformat(value: Any) -> str | None:
        if value is None:
            return None
        if isinstance(value, datetime):
            if value.tzinfo is None:
                value = value.replace(tzinfo=timezone.utc)
            else:
                value = value.astimezone(timezone.utc)
            return value.isoformat()
        if isinstance(value, str) and value.strip():
            return value
        return None

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
