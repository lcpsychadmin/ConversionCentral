from __future__ import annotations

import logging
from dataclasses import dataclass
from types import SimpleNamespace
from typing import Iterable, Sequence
from uuid import UUID

from sqlalchemy import func, or_
from sqlalchemy.orm import Session, selectinload

from app.models import DataDefinition, DataDefinitionTable, Report, SystemConnection, Table
from app.services.data_construction_sync import delete_constructed_tables_for_definition
from app.services.ingestion_storage import DatabricksIngestionStorage
from app.services.ingestion_support import (
    build_ingestion_schema_name,
    build_ingestion_table_name,
)

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class CatalogRemoval:
    """Details about a catalog table unselection that now requires cleanup."""

    system_connection_id: UUID
    schema_name: str | None
    table_name: str
    system_id: UUID | None = None


def cascade_cleanup_for_catalog_removals(
    db: Session,
    removals: Iterable[CatalogRemoval],
    *,
    ingestion_storage: DatabricksIngestionStorage | None = None,
) -> None:
    """Remove application metadata created for catalog selections that were unselected.

    This operation removes related tables, data definitions, constructed tables, reports,
    and associated warehouse artifacts such as Databricks ingestion tables.
    """

    removal_list = list(removals)
    if not removal_list:
        return

    storage = ingestion_storage
    if storage is None:
        try:
            storage = DatabricksIngestionStorage()
        except Exception as exc:  # pragma: no cover - configuration specific
            logger.warning(
                "Skipping Databricks ingestion cleanup because the warehouse is not configured: %s",
                exc,
            )
            storage = None

    reports_cache: Sequence[Report] | None = None
    reports_to_delete: set[UUID] = set()
    connection_cache: dict[UUID, SystemConnection | None] = {}

    for removal in removal_list:
        schema_normalized = (removal.schema_name or "").strip().lower()
        table_normalized = removal.table_name.strip().lower()
        if not table_normalized:
            continue

        tables_query = db.query(Table)

        if removal.system_id:
            tables_query = tables_query.filter(Table.system_id == removal.system_id)
        elif removal.system_connection_id:
            tables_query = (
                tables_query.join(
                    DataDefinitionTable,
                    DataDefinitionTable.table_id == Table.id,
                )
                .filter(DataDefinitionTable.system_connection_id == removal.system_connection_id)
            )
        else:
            logger.info(
                "Catalog cleanup skipped for %s.%s – missing system and connection identifiers.",
                removal.schema_name or "<default>",
                removal.table_name,
            )
            continue

        tables_query = tables_query.filter(func.lower(Table.physical_name) == table_normalized)
        if schema_normalized:
            tables_query = tables_query.filter(func.lower(Table.schema_name) == schema_normalized)
        else:
            tables_query = tables_query.filter(
                or_(
                    Table.schema_name.is_(None),
                    func.lower(Table.schema_name) == "",
                )
            )

        matched_tables = tables_query.options(
            selectinload(Table.definition_tables).selectinload(DataDefinitionTable.data_definition),
        ).all()

        if not matched_tables:
            logger.info(
                "Catalog cleanup skipped for %s.%s – no matching tables found.",
                removal.schema_name or "<default>",
                removal.table_name,
            )
            continue

        connection = connection_cache.get(removal.system_connection_id)
        if removal.system_connection_id not in connection_cache:
            connection = db.get(SystemConnection, removal.system_connection_id)
            connection_cache[removal.system_connection_id] = connection

        for table in matched_tables:
            definition_ids = {
                link.data_definition.id
                for link in table.definition_tables
                if link.data_definition is not None
            }

            for definition_id in definition_ids:
                delete_constructed_tables_for_definition(definition_id, db)
                definition = db.get(DataDefinition, definition_id)
                if definition is not None:
                    logger.info(
                        "Deleting data definition %s linked to table %s", definition_id, table.id
                    )
                    db.delete(definition)

            if storage is not None and connection is not None:
                ingestion_schema = build_ingestion_schema_name(connection)
                selection_stub = SimpleNamespace(
                    schema_name=removal.schema_name,
                    table_name=removal.table_name,
                )
                ingestion_table = build_ingestion_table_name(connection, selection_stub)
                try:
                    storage.drop_table(ingestion_schema, ingestion_table)
                    logger.info(
                        "Dropped Databricks ingestion table %s.%s for system %s",
                        ingestion_schema or "<default>",
                        ingestion_table,
                        getattr(connection, "system_id", None),
                    )
                except Exception as exc:  # pragma: no cover - backend specific
                    logger.warning(
                        "Failed to drop Databricks table for %s.%s: %s",
                        removal.schema_name or "<default>",
                        removal.table_name,
                        exc,
                    )

            if reports_cache is None:
                reports_cache = db.query(Report).all()
            if reports_cache:
                for report in reports_cache:
                    if report.id in reports_to_delete:
                        continue
                    if _report_references_table(report, table.id):
                        reports_to_delete.add(report.id)

            logger.info("Removing table metadata record %s for catalog cleanup", table.id)
            db.delete(table)

    if reports_to_delete:
        reports = db.query(Report).filter(Report.id.in_(reports_to_delete)).all()
        for report in reports:
            logger.info("Deleting report %s due to catalog cleanup", report.id)
            db.delete(report)

    db.flush()


def _report_references_table(report: Report, table_id: UUID) -> bool:
    definition = getattr(report, "definition", None)
    if not isinstance(definition, dict):
        return False

    tables = definition.get("tables")
    if not isinstance(tables, list):
        return False

    for entry in tables:
        if not isinstance(entry, dict):
            continue
        raw_identifier = entry.get("tableId") or entry.get("table_id")
        if not raw_identifier:
            continue
        try:
            candidate = UUID(str(raw_identifier))
        except ValueError:
            continue
        if candidate == table_id:
            return True
    return False
