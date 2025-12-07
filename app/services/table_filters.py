from __future__ import annotations

from app.models.entities import Table, SystemConnection
from app.schemas import SystemConnectionType


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


def table_is_databricks_eligible(table: Table) -> bool:
    # Profiling now runs locally, so the UI should expose every table regardless of
    # Databricks-specific metadata. Keeping this helper simplifies callers that
    # previously referenced the eligibility check.
    return True
