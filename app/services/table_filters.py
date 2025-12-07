from __future__ import annotations

from app.models.entities import Table, SystemConnection
from app.schemas import SystemConnectionType


def table_is_constructed(table: Table) -> bool:
    definition_tables = getattr(table, "definition_tables", []) or []
    return any(getattr(def_table, "is_construction", False) for def_table in definition_tables)


def connection_is_databricks(connection: SystemConnection) -> bool:
    if not getattr(connection, "active", False):
        return False

    if getattr(connection, "use_databricks_managed_connection", False):
        return True

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


def _system_indicates_databricks(table: Table) -> bool:
    system = getattr(table, "system", None)
    if system is None:
        return False

    system_type = (getattr(system, "system_type", "") or "").strip().lower()
    if system_type == "databricks":
        return True

    physical_name = (getattr(system, "physical_name", "") or "").strip().lower()
    if physical_name.startswith("dbr_"):
        return True

    system_name = (getattr(system, "name", "") or "").strip().lower()
    if "databricks" in system_name:
        return True

    return False


def table_is_databricks_eligible(table: Table) -> bool:
    if table_is_constructed(table) or table_has_databricks_connection(table):
        return True
    return _system_indicates_databricks(table)
