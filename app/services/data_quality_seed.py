from __future__ import annotations

from dataclasses import dataclass
from uuid import UUID


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
    system_id: str | None
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
    workspace_id: UUID | None = None
    connections: tuple[ConnectionSeed, ...] = ()


@dataclass(frozen=True)
class DataQualitySeed:
    projects: tuple[ProjectSeed, ...] = ()


__all__ = [
    "DataQualitySeed",
    "ProjectSeed",
    "ConnectionSeed",
    "TableGroupSeed",
    "TableSeed",
]
