from __future__ import annotations

from typing import Dict, List

from sqlalchemy import select
from sqlalchemy.orm import Session, joinedload, selectinload

from app.models.entities import DataDefinition, DataDefinitionTable, DataObject, ProcessArea, System
from app.schemas.data_quality import (
    DataQualityDatasetApplication,
    DataQualityDatasetDataObject,
    DataQualityDatasetDefinition,
    DataQualityDatasetProductTeam,
    DataQualityDatasetTable,
)


def _safe_sort_key(value: str | None) -> str:
    return value.lower() if value else ""


def _table_sort_key(table: DataQualityDatasetTable) -> tuple[int, str]:
    load_order = table.load_order if table.load_order is not None else 10**6
    return load_order, _safe_sort_key(table.table_name)


def build_dataset_hierarchy(db: Session) -> List[DataQualityDatasetProductTeam]:
    stmt = (
        select(DataDefinition)
        .options(
            joinedload(DataDefinition.data_object).joinedload(DataObject.process_area),
            joinedload(DataDefinition.system),
            selectinload(DataDefinition.tables).selectinload(DataDefinitionTable.table),
        )
    )
    definitions = db.execute(stmt).unique().scalars().all()

    product_map: Dict[str, Dict[str, object]] = {}

    for definition in definitions:
        data_object = definition.data_object
        system = definition.system
        if data_object is None or system is None:
            continue

        process_area = data_object.process_area
        if process_area is None:
            continue

        tables = [
            DataQualityDatasetTable(
                data_definition_table_id=table_link.id,
                table_id=table_link.table.id,
                schema_name=table_link.table.schema_name,
                table_name=table_link.table.name,
                physical_name=table_link.table.physical_name,
                alias=table_link.alias,
                description=table_link.description or table_link.table.description,
                load_order=table_link.load_order,
                is_constructed=table_link.is_construction,
                table_type=table_link.table.table_type,
            )
            for table_link in definition.tables
            if table_link.table is not None
        ]

        product_entry = product_map.setdefault(
            str(process_area.id),
            {
                "process_area": process_area,
                "applications": {},
            },
        )

        application_entry = product_entry["applications"].setdefault(
            str(system.id),
            {
                "system": system,
                "data_objects": {},
            },
        )

        data_object_entry = application_entry["data_objects"].setdefault(
            str(data_object.id),
            {
                "data_object": data_object,
                "definitions": [],
            },
        )

        data_object_entry["definitions"].append(
            {
                "definition": definition,
                "tables": tables,
            }
        )

    product_teams: List[DataQualityDatasetProductTeam] = []

    for product_entry in sorted(
        product_map.values(),
        key=lambda entry: _safe_sort_key(entry["process_area"].name),
    ):
        process_area: ProcessArea = product_entry["process_area"]  # type: ignore[assignment]
        applications: List[DataQualityDatasetApplication] = []

        for application_entry in sorted(
            product_entry["applications"].values(),
            key=lambda entry: _safe_sort_key(entry["system"].name),
        ):
            system: System = application_entry["system"]  # type: ignore[assignment]
            data_objects: List[DataQualityDatasetDataObject] = []

            for data_object_entry in sorted(
                application_entry["data_objects"].values(),
                key=lambda entry: _safe_sort_key(entry["data_object"].name),
            ):
                data_object_model: DataObject = data_object_entry["data_object"]  # type: ignore[assignment]
                definitions_payload: List[DataQualityDatasetDefinition] = []

                for definition_entry in sorted(
                    data_object_entry["definitions"],
                    key=lambda entry: _safe_sort_key(entry["definition"].description)
                    or entry["definition"].id.hex,
                ):
                    definition_model: DataDefinition = definition_entry["definition"]  # type: ignore[assignment]
                    tables: List[DataQualityDatasetTable] = sorted(
                        definition_entry["tables"],
                        key=_table_sort_key,
                    )

                    definitions_payload.append(
                        DataQualityDatasetDefinition(
                            data_definition_id=definition_model.id,
                            description=definition_model.description,
                            tables=tables,
                        )
                    )

                data_objects.append(
                    DataQualityDatasetDataObject(
                        data_object_id=data_object_model.id,
                        name=data_object_model.name,
                        description=data_object_model.description,
                        data_definitions=definitions_payload,
                    )
                )

            applications.append(
                DataQualityDatasetApplication(
                    application_id=system.id,
                    name=system.name,
                    description=system.description,
                    physical_name=system.physical_name,
                    data_objects=data_objects,
                )
            )

        product_teams.append(
            DataQualityDatasetProductTeam(
                product_team_id=process_area.id,
                name=process_area.name,
                description=process_area.description,
                applications=applications,
            )
        )

    return product_teams
