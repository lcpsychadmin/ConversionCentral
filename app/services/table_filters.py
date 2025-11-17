from __future__ import annotations

from app.models.entities import Table, SystemConnection
from app.schemas import SystemConnectionType


def table_is_constructed(table: Table) -> bool:
    definition_tables = getattr(table, "definition_tables", []) or []
    return any(getattr(def_table, "is_construction", False) for def_table in definition_tables)


def connection_is_databricks(connection: SystemConnection) -> bool:
    if not getattr(connection, "active", False):
        return False

    connection_type = str(getattr(connection, "connection_type", "") or "").lower()
    if connection_type != SystemConnectionType.JDBC.value:
        return False

    connection_string = (getattr(connection, "connection_string", "") or "").strip().lower()
    return connection_string.startswith("jdbc:databricks:")


def table_has_databricks_connection(table: Table) -> bool:
    system = getattr(table, "system", None)
    if system is None:
        return False

    for connection in getattr(system, "connections", []) or []:
        if connection_is_databricks(connection):
            return True

    return False


def table_is_databricks_eligible(table: Table) -> bool:
    return table_is_constructed(table) or table_has_databricks_connection(table)
