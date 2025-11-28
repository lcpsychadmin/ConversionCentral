"""Provisioning helpers for data quality metadata tables in Databricks."""

from __future__ import annotations

import logging
import re
from collections import OrderedDict
from dataclasses import dataclass
from typing import Iterable, Sequence, Set

from sqlalchemy import create_engine, select, text
from sqlalchemy.engine import Engine
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import selectinload

from app.services.databricks_sql import DatabricksConnectionParams, build_sqlalchemy_url
from app.database import SessionLocal
from app.models import (
    ConnectionTableSelection,
    DataDefinition,
    DataDefinitionTable,
    DataObject,
    DataObjectSystem,
    System,
    SystemConnection,
)
from app.services.data_quality_keys import (
    build_connection_id,
    build_project_key,
    build_table_group_id,
    build_table_id,
    create_definition_table_key_set,
    selection_matches_keys,
)

logger = logging.getLogger(__name__)

_SCHEMA_VERSION = "3"
_SUPPORTED_STORAGE_FORMATS = {"delta"}
_PROJECT_KEY_PREFIX = "system:"
_CONNECTION_ID_PREFIX = "conn:"
_TABLE_GROUP_ID_PREFIX = "group:"
_TABLE_ID_PREFIX = "selection:"


_SANITIZE_PATTERN = re.compile(r"[^A-Za-z0-9]+")


def _sanitize_part(value: str | None, *, default: str | None = None) -> str | None:
    if not value:
        return default
    sanitized = _SANITIZE_PATTERN.sub("_", value.strip()).strip("_")
    normalized = sanitized.lower()
    if normalized:
        return normalized
    return default


def _ingestion_schema_name(connection: SystemConnection) -> str | None:
    system = getattr(connection, "system", None)
    candidates = (
        getattr(system, "name", None),
        getattr(system, "physical_name", None),
    )
    for candidate in candidates:
        normalized = _sanitize_part(candidate)
        if normalized:
            return normalized
    return None


def _ingestion_table_name(selection: ConnectionTableSelection) -> str:
    schema_part = _sanitize_part(getattr(selection, "schema_name", None) or "default", default="segment")
    table_part = _sanitize_part(getattr(selection, "table_name", None) or "table", default="segment")
    parts = [part for part in (schema_part, table_part) if part]
    return "_".join(parts) if parts else "segment"


@dataclass(frozen=True)
class TableSeed:
    table_id: str
    table_group_id: str
    schema_name: str | None
    table_name: str
    source_table_id: str | None = None


@dataclass(frozen=True)
class TableGroupSeed:
    table_group_id: str
    connection_id: str
    name: str
    description: str | None = None
    profiling_include_mask: str | None = None
    profiling_exclude_mask: str | None = None
    profiling_job_id: str | None = None
    tables: tuple[TableSeed, ...] = ()


@dataclass(frozen=True)
class ConnectionSeed:
    connection_id: str
    project_key: str
    system_id: str
    name: str
    catalog: str | None
    schema_name: str | None
    http_path: str | None
    managed_credentials_ref: str | None
    is_active: bool
    table_groups: tuple[TableGroupSeed, ...] = ()


@dataclass(frozen=True)
class ProjectSeed:
    project_key: str
    name: str
    description: str | None
    sql_flavor: str
    connections: tuple[ConnectionSeed, ...] = ()


@dataclass(frozen=True)
class DataQualitySeed:
    projects: tuple[ProjectSeed, ...] = ()


def _format_connection_name(system_name: str, data_object_name: str, connection_type: str | None) -> str:
    suffix = f" ({connection_type.upper()})" if connection_type else ""
    return f"{system_name} · {data_object_name}{suffix}"


def _tables_for_connection(
    connection: SystemConnection,
    *,
    data_object_id,
    table_keys: Set["DefinitionTableKey"],
) -> tuple[TableSeed, ...]:
    selections = list(connection.catalog_selections or [])
    if not selections:
        return ()

    group_id = build_table_group_id(connection.id, data_object_id)
    ingestion_schema = _ingestion_schema_name(connection)

    def _build_tables(filter_keys: Set["DefinitionTableKey"] | None) -> list[TableSeed]:
        result: list[TableSeed] = []
        for selection in selections:
            if filter_keys is not None and not selection_matches_keys(
                selection.schema_name, selection.table_name, filter_keys
            ):
                continue
            schema = ingestion_schema or (selection.schema_name.strip() if selection.schema_name else None)
            table_name = _ingestion_table_name(selection)
            result.append(
                TableSeed(
                    table_id=build_table_id(selection.id, data_object_id),
                    table_group_id=group_id,
                    schema_name=schema,
                    table_name=table_name,
                    source_table_id=None,
                )
            )
        return result

    primary = _build_tables(table_keys)
    if primary:
        return tuple(primary)
    if table_keys:
        fallback = _build_tables(None)
        return tuple(fallback)
    return tuple(primary)


def ensure_data_quality_metadata(
    params: DatabricksConnectionParams,
    seed_override: DataQualitySeed | None = None,
) -> None:
    """Ensure the configured data quality schema exists and contains baseline tables.

    When ``seed_override`` is provided the caller is responsible for constructing the
    desired project/connection/table set (e.g., from a dbt manifest). The ORM-based
    metadata collection will be skipped in that case.
    """

    if not params.data_quality_auto_manage_tables:
        logger.info("Databricks data quality auto-management disabled; skipping metadata provisioning.")
        return

    schema = (params.data_quality_schema or "").strip()
    if not schema:
        logger.info("No data quality schema configured; skipping metadata provisioning.")
        return

    storage_format = (params.data_quality_storage_format or "delta").strip().lower()
    if storage_format not in _SUPPORTED_STORAGE_FORMATS:
        logger.warning(
            "Data quality storage format '%s' is not currently supported for auto-provisioning; skipping.",
            storage_format,
        )
        return

    try:
        seed = seed_override or _collect_metadata(params)
        _apply_schema(params, schema, storage_format, seed)
    except SQLAlchemyError as exc:  # pragma: no cover - defensive guard for runtime failures
        logger.exception("Failed to provision data quality metadata tables: %s", exc)
    except Exception as exc:  # pragma: no cover - defensive guard
        logger.exception("Unexpected error while provisioning data quality metadata tables: %s", exc)


def _apply_schema(
    params: DatabricksConnectionParams,
    schema: str,
    storage_format: str,
    seed: DataQualitySeed,
) -> None:
    engine: Engine | None = None
    try:
        engine = create_engine(
            build_sqlalchemy_url(params),
            pool_pre_ping=True,
            connect_args={"timeout": 30},
        )
        with engine.connect() as connection:
            _run_statements(connection.execute, _schema_statements(params, schema, storage_format))
            _upgrade_schema(connection.execute, params, schema)
            _seed_metadata(connection, params, schema, seed)
    finally:
        if engine is not None:
            engine.dispose()


def _run_statements(execute, statements: Iterable[str]) -> None:
    for statement in statements:
        execute(text(statement))


def _upgrade_schema(execute, params: DatabricksConnectionParams, schema: str) -> None:
    table_groups_table = _format_table(params.catalog, schema, "dq_table_groups")
    profiles_table = _format_table(params.catalog, schema, "dq_profiles")
    table_chars_table = _format_table(params.catalog, schema, "dq_data_table_chars")
    column_chars_table = _format_table(params.catalog, schema, "dq_data_column_chars")
    statements = [
        (table_groups_table, "profiling_job_id STRING"),
        (profiles_table, "databricks_run_id STRING"),
        (profiles_table, "payload_path STRING"),
        (profiles_table, "table_count BIGINT"),
        (profiles_table, "column_count BIGINT"),
        (profiles_table, "profile_mode STRING"),
        (profiles_table, "profile_version STRING"),
        (profiles_table, "dq_score_profiling DOUBLE"),
        (profiles_table, "dq_score_testing DOUBLE"),
        (table_chars_table, "last_complete_profile_run_id STRING"),
        (table_chars_table, "dq_score_profiling DOUBLE"),
        (table_chars_table, "dq_score_testing DOUBLE"),
        (column_chars_table, "last_complete_profile_run_id STRING"),
        (column_chars_table, "dq_score_profiling DOUBLE"),
        (column_chars_table, "dq_score_testing DOUBLE"),
    ]
    for table_name, column_definition in statements:
        statement = text(f"ALTER TABLE {table_name} ADD COLUMNS ({column_definition})")
        try:
            execute(statement)
        except SQLAlchemyError as exc:
            error_message = str(exc).lower()
            if "already exists" in error_message:
                continue
            logger.warning("Failed to upgrade table %s with column %s: %s", table_name, column_definition, exc)
            raise


def _schema_statements(
    params: DatabricksConnectionParams,
    schema: str,
    storage_format: str,
) -> Iterable[str]:
    qualified_schema = _format_schema(params.catalog, schema)

    yield f"CREATE SCHEMA IF NOT EXISTS {qualified_schema}"

    for table_name, column_definitions in _table_definitions().items():
        qualified_table = _format_table(params.catalog, schema, table_name)
        columns_sql = ",\n        ".join(column_definitions)
        ddl = (
            f"CREATE TABLE IF NOT EXISTS {qualified_table} (\n        {columns_sql}\n) "
            f"USING {storage_format.upper()}"
        )
        if storage_format == "delta":
            ddl += " TBLPROPERTIES ('delta.autoOptimize.optimizeWrite' = 'true', 'delta.autoOptimize.autoCompact' = 'true')"
        yield ddl

    settings_table = _format_table(params.catalog, schema, "dq_settings")
    yield f"DELETE FROM {settings_table} WHERE key = 'schema_version'"
    yield (
        f"INSERT INTO {settings_table} (key, value, updated_at) "
        f"VALUES ('schema_version', '{_SCHEMA_VERSION}', current_timestamp())"
    )


def _collect_metadata(params: DatabricksConnectionParams) -> DataQualitySeed:
    projects: list[ProjectSeed] = []

    with SessionLocal() as session:
        systems = (
            session.execute(
                select(System).options(
                    selectinload(System.connections).selectinload(SystemConnection.catalog_selections),
                    selectinload(System.data_object_links)
                    .selectinload(DataObjectSystem.data_object)
                    .selectinload(DataObject.data_definitions)
                    .selectinload(DataDefinition.tables)
                    .selectinload(DataDefinitionTable.table),
                )
            )
            .scalars()
            .unique()
            .all()
        )

        for system in systems:
            if system.status and system.status.lower() != "active":
                continue

            data_object_links = [link for link in system.data_object_links if link.data_object is not None]

            for link in data_object_links:
                data_object = link.data_object
                if not data_object or (data_object.status and data_object.status.lower() == "archived"):
                    continue

                definitions = [
                    definition
                    for definition in data_object.data_definitions
                    if definition.system_id == system.id
                ]

                if not definitions:
                    continue

                definition_keys = create_definition_table_key_set(definitions)
                connections: list[ConnectionSeed] = []

                for connection in system.connections:
                    if not connection.active:
                        continue

                    tables = _tables_for_connection(
                        connection,
                        data_object_id=data_object.id,
                        table_keys=definition_keys,
                    )

                    if not tables:
                        continue

                    connection_id = build_connection_id(connection.id, data_object.id)
                    table_group_id = build_table_group_id(connection.id, data_object.id)

                    table_groups = (
                        TableGroupSeed(
                            table_group_id=table_group_id,
                            connection_id=connection_id,
                            name=f"{data_object.name} Tables",
                            description=data_object.description or system.description,
                            tables=tables,
                        ),
                    )

                    connections.append(
                        ConnectionSeed(
                            connection_id=connection_id,
                            project_key=build_project_key(system.id, data_object.id),
                            system_id=str(system.id),
                            name=_format_connection_name(
                                system.name,
                                data_object.name,
                                connection.connection_type,
                            ),
                            catalog=params.catalog,
                            schema_name=params.schema_name,
                            http_path=params.http_path,
                            managed_credentials_ref=None,
                            # Profiling should still consider the connection active even when ingestion is disabled.
                            is_active=bool(connection.active),
                            table_groups=table_groups,
                        )
                    )

                if connections:
                    projects.append(
                        ProjectSeed(
                            project_key=build_project_key(system.id, data_object.id),
                            name=f"{system.name} · {data_object.name}",
                            description=data_object.description or system.description,
                            sql_flavor="databricks-sql",
                            connections=tuple(connections),
                        )
                    )

    return DataQualitySeed(projects=tuple(projects))

def _seed_metadata(connection, params: DatabricksConnectionParams, schema: str, seed: DataQualitySeed) -> None:
    if not seed.projects:
        return

    projects_table = _format_table(params.catalog, schema, "dq_projects")
    connections_table = _format_table(params.catalog, schema, "dq_connections")
    table_groups_table = _format_table(params.catalog, schema, "dq_table_groups")
    tables_table = _format_table(params.catalog, schema, "dq_tables")

    for project in seed.projects:
        _merge_project(connection, projects_table, project)
        for conn_seed in project.connections:
            _merge_connection(connection, connections_table, conn_seed)
            for group_seed in conn_seed.table_groups:
                _merge_table_group(connection, table_groups_table, group_seed)
                for table_seed in group_seed.tables:
                    _merge_table(connection, tables_table, table_seed)
                _prune_tables(connection, tables_table, group_seed.table_group_id, [t.table_id for t in group_seed.tables])
            _prune_connection_groups(connection, table_groups_table, conn_seed.connection_id, [g.table_group_id for g in conn_seed.table_groups])
        _prune_connections(connection, connections_table, project.project_key, [c.connection_id for c in project.connections])

    _prune_projects(connection, projects_table, [p.project_key for p in seed.projects])


def _merge_project(connection, projects_table: str, project: ProjectSeed) -> None:
    connection.execute(
        text(
            f"""
MERGE INTO {projects_table} AS target
USING (
    SELECT :project_key AS project_key,
           :name AS name,
           :description AS description,
           :sql_flavor AS sql_flavor
) AS source
ON target.project_key = source.project_key
WHEN MATCHED THEN UPDATE SET
    name = source.name,
    description = source.description,
    sql_flavor = source.sql_flavor,
    updated_at = current_timestamp()
WHEN NOT MATCHED THEN INSERT (project_key, name, description, sql_flavor, created_at, updated_at)
VALUES (source.project_key, source.name, source.description, source.sql_flavor, current_timestamp(), current_timestamp())
"""
        ),
        {
            "project_key": project.project_key,
            "name": project.name,
            "description": project.description,
            "sql_flavor": project.sql_flavor,
        },
    )


def _merge_connection(connection, connections_table: str, conn_seed: ConnectionSeed) -> None:
    connection.execute(
        text(
            f"""
MERGE INTO {connections_table} AS target
USING (
    SELECT :connection_id AS connection_id,
           :project_key AS project_key,
           :system_id AS system_id,
           :name AS name,
           :catalog AS catalog,
           :schema_name AS schema_name,
           :http_path AS http_path,
           :managed_credentials_ref AS managed_credentials_ref,
           :is_active AS is_active
) AS source
ON target.connection_id = source.connection_id
WHEN MATCHED THEN UPDATE SET
    project_key = source.project_key,
    system_id = source.system_id,
    name = source.name,
    catalog = source.catalog,
    schema_name = source.schema_name,
    http_path = source.http_path,
    managed_credentials_ref = source.managed_credentials_ref,
    is_active = source.is_active,
    updated_at = current_timestamp()
WHEN NOT MATCHED THEN INSERT (
    connection_id,
    project_key,
    system_id,
    name,
    catalog,
    schema_name,
    http_path,
    managed_credentials_ref,
    is_active,
    created_at,
    updated_at
)
VALUES (
    source.connection_id,
    source.project_key,
    source.system_id,
    source.name,
    source.catalog,
    source.schema_name,
    source.http_path,
    source.managed_credentials_ref,
    source.is_active,
    current_timestamp(),
    current_timestamp()
)
"""
        ),
        {
            "connection_id": conn_seed.connection_id,
            "project_key": conn_seed.project_key,
            "system_id": conn_seed.system_id,
            "name": conn_seed.name,
            "catalog": conn_seed.catalog,
            "schema_name": conn_seed.schema_name,
            "http_path": conn_seed.http_path,
            "managed_credentials_ref": conn_seed.managed_credentials_ref,
            "is_active": conn_seed.is_active,
        },
    )


def _merge_table_group(connection, table_groups_table: str, group_seed: TableGroupSeed) -> None:
    connection.execute(
        text(
            f"""
MERGE INTO {table_groups_table} AS target
USING (
    SELECT :table_group_id AS table_group_id,
           :connection_id AS connection_id,
           :name AS name,
           :description AS description,
           :profiling_include_mask AS profiling_include_mask,
           :profiling_exclude_mask AS profiling_exclude_mask,
           :profiling_job_id AS profiling_job_id
) AS source
ON target.table_group_id = source.table_group_id
WHEN MATCHED THEN UPDATE SET
    connection_id = source.connection_id,
    name = source.name,
    description = source.description,
    profiling_include_mask = source.profiling_include_mask,
    profiling_exclude_mask = source.profiling_exclude_mask,
    profiling_job_id = source.profiling_job_id,
    updated_at = current_timestamp()
WHEN NOT MATCHED THEN INSERT (
    table_group_id,
    connection_id,
    name,
    description,
    profiling_include_mask,
    profiling_exclude_mask,
    profiling_job_id,
    created_at,
    updated_at
)
VALUES (
    source.table_group_id,
    source.connection_id,
    source.name,
    source.description,
    source.profiling_include_mask,
    source.profiling_exclude_mask,
    source.profiling_job_id,
    current_timestamp(),
    current_timestamp()
)
"""
        ),
        {
            "table_group_id": group_seed.table_group_id,
            "connection_id": group_seed.connection_id,
            "name": group_seed.name,
            "description": group_seed.description,
            "profiling_include_mask": group_seed.profiling_include_mask,
            "profiling_exclude_mask": group_seed.profiling_exclude_mask,
            "profiling_job_id": group_seed.profiling_job_id,
        },
    )


def _merge_table(connection, tables_table: str, table_seed: TableSeed) -> None:
    connection.execute(
        text(
            f"""
MERGE INTO {tables_table} AS target
USING (
    SELECT :table_id AS table_id,
           :table_group_id AS table_group_id,
           :schema_name AS schema_name,
           :table_name AS table_name,
           :source_table_id AS source_table_id
) AS source
ON target.table_id = source.table_id
WHEN MATCHED THEN UPDATE SET
    table_group_id = source.table_group_id,
    schema_name = source.schema_name,
    table_name = source.table_name,
    source_table_id = source.source_table_id
WHEN NOT MATCHED THEN INSERT (
    table_id,
    table_group_id,
    schema_name,
    table_name,
    source_table_id,
    created_at
)
VALUES (
    source.table_id,
    source.table_group_id,
    source.schema_name,
    source.table_name,
    source.source_table_id,
    current_timestamp()
)
"""
        ),
        {
            "table_id": table_seed.table_id,
            "table_group_id": table_seed.table_group_id,
            "schema_name": table_seed.schema_name,
            "table_name": table_seed.table_name,
            "source_table_id": table_seed.source_table_id,
        },
    )


def _prune_tables(connection, tables_table: str, table_group_id: str, active_ids: Sequence[str]) -> None:
    prefix = f"{_TABLE_ID_PREFIX}%"
    if active_ids:
        placeholders, params = _bind_params("table_id", active_ids)
        params.update({"table_group_id": table_group_id, "prefix": prefix})
        connection.execute(
            text(
                f"DELETE FROM {tables_table} "
                "WHERE table_group_id = :table_group_id "
                "AND table_id LIKE :prefix "
                f"AND table_id NOT IN ({placeholders})"
            ),
            params,
        )
    else:
        connection.execute(
            text(
                f"DELETE FROM {tables_table} "
                "WHERE table_group_id = :table_group_id "
                "AND table_id LIKE :prefix"
            ),
            {"table_group_id": table_group_id, "prefix": prefix},
        )


def _prune_connection_groups(connection, table_groups_table: str, connection_id: str, active_ids: Sequence[str]) -> None:
    prefix = f"{_TABLE_GROUP_ID_PREFIX}%"
    if active_ids:
        placeholders, params = _bind_params("group_id", active_ids)
        params.update({"connection_id": connection_id, "prefix": prefix})
        connection.execute(
            text(
                f"DELETE FROM {table_groups_table} "
                "WHERE connection_id = :connection_id "
                "AND table_group_id LIKE :prefix "
                f"AND table_group_id NOT IN ({placeholders})"
            ),
            params,
        )
    else:
        connection.execute(
            text(
                f"DELETE FROM {table_groups_table} "
                "WHERE connection_id = :connection_id "
                "AND table_group_id LIKE :prefix"
            ),
            {"connection_id": connection_id, "prefix": prefix},
        )


def _prune_connections(connection, connections_table: str, project_key: str, active_ids: Sequence[str]) -> None:
    prefix = f"{_CONNECTION_ID_PREFIX}%"
    if active_ids:
        placeholders, params = _bind_params("connection_id", active_ids)
        params.update({"project_key": project_key, "prefix": prefix})
        connection.execute(
            text(
                f"DELETE FROM {connections_table} "
                "WHERE project_key = :project_key "
                "AND connection_id LIKE :prefix "
                f"AND connection_id NOT IN ({placeholders})"
            ),
            params,
        )
    else:
        connection.execute(
            text(
                f"DELETE FROM {connections_table} "
                "WHERE project_key = :project_key "
                "AND connection_id LIKE :prefix"
            ),
            {"project_key": project_key, "prefix": prefix},
        )


def _prune_projects(connection, projects_table: str, active_keys: Sequence[str]) -> None:
    prefix = f"{_PROJECT_KEY_PREFIX}%"
    if active_keys:
        placeholders, params = _bind_params("project_key", active_keys)
        params["prefix"] = prefix
        connection.execute(
            text(
                f"DELETE FROM {projects_table} "
                "WHERE project_key LIKE :prefix "
                f"AND project_key NOT IN ({placeholders})"
            ),
            params,
        )
    else:
        connection.execute(
            text(f"DELETE FROM {projects_table} WHERE project_key LIKE :prefix"),
            {"prefix": prefix},
        )


def _bind_params(prefix: str, values: Sequence[str]) -> tuple[str, dict[str, str]]:
    placeholders: list[str] = []
    params: dict[str, str] = {}
    for index, value in enumerate(values):
        key = f"{prefix}_{index}"
        placeholders.append(f":{key}")
        params[key] = value
    return ", ".join(placeholders), params


def _table_definitions() -> "OrderedDict[str, list[str]]":
    return OrderedDict(
        (
            (
                "dq_projects",
                [
                    "project_key STRING NOT NULL",
                    "name STRING",
                    "description STRING",
                    "sql_flavor STRING",
                    "created_at TIMESTAMP",
                    "updated_at TIMESTAMP",
                ],
            ),
            (
                "dq_connections",
                [
                    "connection_id STRING NOT NULL",
                    "project_key STRING NOT NULL",
                    "system_id STRING",
                    "name STRING",
                    "catalog STRING",
                    "schema_name STRING",
                    "http_path STRING",
                    "managed_credentials_ref STRING",
                    "created_at TIMESTAMP",
                    "updated_at TIMESTAMP",
                    "is_active BOOLEAN",
                ],
            ),
            (
                "dq_table_groups",
                [
                    "table_group_id STRING NOT NULL",
                    "connection_id STRING NOT NULL",
                    "name STRING",
                    "description STRING",
                    "profiling_include_mask STRING",
                    "profiling_exclude_mask STRING",
                    "profiling_job_id STRING",
                    "created_at TIMESTAMP",
                    "updated_at TIMESTAMP",
                ],
            ),
            (
                "dq_tables",
                [
                    "table_id STRING NOT NULL",
                    "table_group_id STRING NOT NULL",
                    "schema_name STRING",
                    "table_name STRING",
                    "source_table_id STRING",
                    "created_at TIMESTAMP",
                ],
            ),
            (
                "dq_profiles",
                [
                    "profile_run_id STRING NOT NULL",
                    "table_group_id STRING NOT NULL",
                    "status STRING",
                    "started_at TIMESTAMP",
                    "completed_at TIMESTAMP",
                    "row_count BIGINT",
                    "anomaly_count INT",
                    "payload_path STRING",
                    "databricks_run_id STRING",
                ],
            ),
            (
                "dq_profile_anomalies",
                [
                    "profile_run_id STRING NOT NULL",
                    "table_name STRING",
                    "column_name STRING",
                    "anomaly_type STRING",
                    "severity STRING",
                    "description STRING",
                    "detected_at TIMESTAMP",
                ],
            ),
            (
                "dq_profile_anomaly_types",
                [
                    "anomaly_type_id STRING NOT NULL",
                    "name STRING",
                    "category STRING",
                    "default_severity STRING",
                    "default_likelihood STRING",
                    "description STRING",
                    "created_at TIMESTAMP",
                    "updated_at TIMESTAMP",
                ],
            ),
            (
                "dq_profile_results",
                [
                    "result_id STRING NOT NULL",
                    "profile_run_id STRING",
                    "table_id STRING",
                    "column_id STRING",
                    "schema_name STRING",
                    "table_name STRING",
                    "column_name STRING",
                    "data_type STRING",
                    "general_type STRING",
                    "record_count BIGINT",
                    "null_count BIGINT",
                    "distinct_count BIGINT",
                    "min_value STRING",
                    "max_value STRING",
                    "avg_value DOUBLE",
                    "stddev_value DOUBLE",
                    "percentiles_json STRING",
                    "top_values_json STRING",
                    "metrics_json STRING",
                    "generated_at TIMESTAMP",
                ],
            ),
            (
                "dq_profile_anomaly_results",
                [
                    "anomaly_result_id STRING NOT NULL",
                    "profile_run_id STRING",
                    "table_id STRING",
                    "column_id STRING",
                    "table_name STRING",
                    "column_name STRING",
                    "anomaly_type_id STRING",
                    "severity STRING",
                    "likelihood STRING",
                    "detail STRING",
                    "pii_risk STRING",
                    "dq_dimension STRING",
                    "detected_at TIMESTAMP",
                    "dismissed BOOLEAN",
                    "dismissed_by STRING",
                    "dismissed_at TIMESTAMP",
                ],
            ),
            (
                "dq_profile_operations",
                [
                    "operation_id STRING NOT NULL",
                    "profile_run_id STRING",
                    "target_table STRING",
                    "rows_written BIGINT",
                    "duration_ms BIGINT",
                    "status STRING",
                    "error_payload STRING",
                    "started_at TIMESTAMP",
                    "completed_at TIMESTAMP",
                ],
            ),
            (
                "dq_profile_columns",
                [
                    "profile_run_id STRING NOT NULL",
                    "schema_name STRING",
                    "table_name STRING NOT NULL",
                    "column_name STRING NOT NULL",
                    "qualified_name STRING",
                    "data_type STRING",
                    "general_type STRING",
                    "ordinal_position INT",
                    "row_count BIGINT",
                    "null_count BIGINT",
                    "non_null_count BIGINT",
                    "distinct_count BIGINT",
                    "min_value STRING",
                    "max_value STRING",
                    "avg_value DOUBLE",
                    "stddev_value DOUBLE",
                    "median_value DOUBLE",
                    "p95_value DOUBLE",
                    "true_count BIGINT",
                    "false_count BIGINT",
                    "min_length INT",
                    "max_length INT",
                    "avg_length DOUBLE",
                    "non_ascii_ratio DOUBLE",
                    "min_date DATE",
                    "max_date DATE",
                    "date_span_days INT",
                    "metrics_json STRING",
                    "generated_at TIMESTAMP",
                ],
            ),
            (
                "dq_profile_column_values",
                [
                    "profile_run_id STRING NOT NULL",
                    "schema_name STRING",
                    "table_name STRING NOT NULL",
                    "column_name STRING NOT NULL",
                    "value STRING",
                    "value_hash STRING",
                    "frequency BIGINT",
                    "relative_freq DOUBLE",
                    "rank INT",
                    "bucket_label STRING",
                    "bucket_lower_bound DOUBLE",
                    "bucket_upper_bound DOUBLE",
                    "generated_at TIMESTAMP",
                ],
            ),
            (
                "dq_data_table_chars",
                [
                    "table_id STRING NOT NULL",
                    "table_group_id STRING",
                    "schema_name STRING",
                    "table_name STRING",
                    "record_count BIGINT",
                    "column_count INT",
                    "data_point_count BIGINT",
                    "critical_data_element BOOLEAN",
                    "data_source STRING",
                    "source_system STRING",
                    "source_process STRING",
                    "business_domain STRING",
                    "stakeholder_group STRING",
                    "transform_level STRING",
                    "data_product STRING",
                    "dq_score_profiling DOUBLE",
                    "dq_score_testing DOUBLE",
                    "last_complete_profile_run_id STRING",
                    "latest_anomaly_ct INT",
                    "latest_run_completed_at TIMESTAMP",
                    "created_at TIMESTAMP",
                    "updated_at TIMESTAMP",
                ],
            ),
            (
                "dq_data_column_chars",
                [
                    "column_id STRING NOT NULL",
                    "table_id STRING",
                    "schema_name STRING",
                    "table_name STRING",
                    "column_name STRING",
                    "data_type STRING",
                    "functional_data_type STRING",
                    "critical_data_element BOOLEAN",
                    "pii_risk STRING",
                    "dq_dimension STRING",
                    "tags_json STRING",
                    "dq_score_profiling DOUBLE",
                    "dq_score_testing DOUBLE",
                    "last_complete_profile_run_id STRING",
                    "latest_anomaly_ct INT",
                    "latest_run_completed_at TIMESTAMP",
                    "created_at TIMESTAMP",
                    "updated_at TIMESTAMP",
                ],
            ),
            (
                "dq_test_suites",
                [
                    "test_suite_key STRING NOT NULL",
                    "project_key STRING",
                    "name STRING",
                    "description STRING",
                    "severity STRING",
                    "product_team_id STRING",
                    "application_id STRING",
                    "data_object_id STRING",
                    "data_definition_id STRING",
                    "created_at TIMESTAMP",
                    "updated_at TIMESTAMP",
                ],
            ),
            (
                "dq_tests",
                [
                    "test_id STRING NOT NULL",
                    "table_group_id STRING NOT NULL",
                    "test_suite_key STRING",
                    "name STRING",
                    "rule_type STRING",
                    "definition STRING",
                    "created_at TIMESTAMP",
                    "updated_at TIMESTAMP",
                ],
            ),
            (
                "dq_test_runs",
                [
                    "test_run_id STRING NOT NULL",
                    "test_suite_key STRING",
                    "project_key STRING",
                    "status STRING",
                    "started_at TIMESTAMP",
                    "completed_at TIMESTAMP",
                    "duration_ms BIGINT",
                    "total_tests INT",
                    "failed_tests INT",
                    "trigger_source STRING",
                ],
            ),
            (
                "dq_test_results",
                [
                    "test_run_id STRING NOT NULL",
                    "test_id STRING NOT NULL",
                    "table_name STRING",
                    "column_name STRING",
                    "result_status STRING",
                    "expected_value STRING",
                    "actual_value STRING",
                    "message STRING",
                    "detected_at TIMESTAMP",
                ],
            ),
            (
                "dq_alerts",
                [
                    "alert_id STRING NOT NULL",
                    "source_type STRING",
                    "source_ref STRING",
                    "severity STRING",
                    "title STRING",
                    "details STRING",
                    "acknowledged BOOLEAN",
                    "acknowledged_by STRING",
                    "acknowledged_at TIMESTAMP",
                    "created_at TIMESTAMP",
                ],
            ),
            (
                "dq_settings",
                [
                    "key STRING NOT NULL",
                    "value STRING",
                    "updated_at TIMESTAMP",
                ],
            ),
        )
    )


def _format_schema(catalog: str | None, schema: str) -> str:
    if catalog:
        return f"{_escape_identifier(catalog)}.{_escape_identifier(schema)}"
    return _escape_identifier(schema)


def _format_table(catalog: str | None, schema: str, table: str) -> str:
    schema_ref = _format_schema(catalog, schema)
    return f"{schema_ref}.{_escape_identifier(table)}"


def _escape_identifier(identifier: str) -> str:
    cleaned = identifier.strip().replace("`", "")
    return f"`{cleaned}`"