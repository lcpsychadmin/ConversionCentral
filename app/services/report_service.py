from __future__ import annotations

from datetime import datetime
from typing import Any
from uuid import UUID

from fastapi.encoders import jsonable_encoder
from sqlalchemy import select
from sqlalchemy.orm import Session, selectinload

from app.models.entities import DataObject, ProcessArea, Report
from app.schemas.reporting import ReportDesignerDefinition, ReportStatus


class ReportNotFoundError(Exception):
    """Raised when a report cannot be located."""


_UNSET = object()


def _normalize_definition(definition: ReportDesignerDefinition | dict[str, Any]) -> dict[str, Any]:
    """Return a JSON-serializable representation of the designer definition."""
    if isinstance(definition, ReportDesignerDefinition):
        data = definition.dict(by_alias=True)
    if hasattr(definition, "dict"):
        data = definition.dict(by_alias=True)  # type: ignore[assignment]
    else:
        data = dict(definition)

    return jsonable_encoder(data)


def _resolve_report_associations(
    db: Session,
    process_area_id: UUID | None,
    data_object_id: UUID | None,
) -> tuple[UUID | None, UUID | None]:
    normalized_process_area_id = process_area_id
    normalized_data_object_id = data_object_id

    if data_object_id is not None:
        data_object = db.get(DataObject, data_object_id)
        if data_object is None:
            raise ValueError("Selected data object could not be found.")
        normalized_data_object_id = data_object.id
        if normalized_process_area_id is not None and data_object.process_area_id != normalized_process_area_id:
            raise ValueError("Selected data object does not belong to the chosen product team.")
        normalized_process_area_id = data_object.process_area_id

    if normalized_process_area_id is not None:
        if db.get(ProcessArea, normalized_process_area_id) is None:
            raise ValueError("Selected product team could not be found.")

    return normalized_process_area_id, normalized_data_object_id


def list_reports(db: Session, status: ReportStatus | None = None) -> list[Report]:
    statement = (
        select(Report)
        .options(selectinload(Report.process_area), selectinload(Report.data_object))
        .order_by(Report.updated_at.desc())
    )
    if status is not None:
        statement = statement.where(Report.status == status.value)
    return list(db.execute(statement).scalars())


def get_report(db: Session, report_id: UUID) -> Report | None:
    return db.get(Report, report_id)


def require_report(db: Session, report_id: UUID) -> Report:
    report = get_report(db, report_id)
    if not report:
        raise ReportNotFoundError(str(report_id))
    return report


def create_report(
    db: Session,
    *,
    name: str,
    description: str | None,
    definition: ReportDesignerDefinition | dict[str, Any],
    status: ReportStatus = ReportStatus.DRAFT,
    process_area_id: UUID | None = None,
    data_object_id: UUID | None = None,
) -> Report:
    cleaned_name = name.strip()
    if not cleaned_name:
        raise ValueError("Report name cannot be empty.")

    normalized_description = description.strip() if description and description.strip() else None
    payload = _normalize_definition(definition)

    resolved_process_area_id, resolved_data_object_id = _resolve_report_associations(
        db,
        process_area_id,
        data_object_id,
    )

    if status == ReportStatus.PUBLISHED and (resolved_process_area_id is None or resolved_data_object_id is None):
        raise ValueError("Published reports must be linked to a product team and data object.")

    report = Report(
        name=cleaned_name,
        description=normalized_description,
        status=status.value,
        definition=payload,
        process_area_id=resolved_process_area_id,
        data_object_id=resolved_data_object_id,
    )

    if status == ReportStatus.PUBLISHED:
        report.published_at = datetime.utcnow()

    db.add(report)
    db.commit()
    db.refresh(report)
    return report


def update_report(
    db: Session,
    report: Report,
    *,
    name: str | None = None,
    description: str | None = None,
    definition: ReportDesignerDefinition | dict[str, Any] | None = None,
    status: ReportStatus | None = None,
    process_area_id: UUID | None | object = _UNSET,
    data_object_id: UUID | None | object = _UNSET,
) -> Report:
    if name is not None:
        cleaned_name = name.strip()
        if not cleaned_name:
            raise ValueError("Report name cannot be empty.")
        report.name = cleaned_name

    if description is not None:
        cleaned_description = description.strip()
        report.description = cleaned_description or None

    if definition is not None:
        report.definition = _normalize_definition(definition)

    if status is not None:
        report.status = status.value
        if status == ReportStatus.PUBLISHED:
            report.published_at = datetime.utcnow()
        else:
            report.published_at = None

    if process_area_id is not _UNSET or data_object_id is not _UNSET:
        next_process_area_id = report.process_area_id if process_area_id is _UNSET else process_area_id
        next_data_object_id = report.data_object_id if data_object_id is _UNSET else data_object_id
        resolved_process_area_id, resolved_data_object_id = _resolve_report_associations(
            db,
            next_process_area_id,
            next_data_object_id,
        )
        report.process_area_id = resolved_process_area_id
        report.data_object_id = resolved_data_object_id

    db.add(report)
    db.commit()
    db.refresh(report)
    return report


def publish_report(
    db: Session,
    report: Report,
    *,
    process_area_id: UUID | None = None,
    data_object_id: UUID | None = None,
) -> Report:
    target_process_area_id = process_area_id if process_area_id is not None else report.process_area_id
    target_data_object_id = data_object_id if data_object_id is not None else report.data_object_id
    resolved_process_area_id, resolved_data_object_id = _resolve_report_associations(
        db,
        target_process_area_id,
        target_data_object_id,
    )

    if resolved_process_area_id is None or resolved_data_object_id is None:
        raise ValueError("Published reports must be linked to a product team and data object.")

    report.process_area_id = resolved_process_area_id
    report.data_object_id = resolved_data_object_id

    return update_report(
        db,
        report,
        status=ReportStatus.PUBLISHED,
        process_area_id=resolved_process_area_id,
        data_object_id=resolved_data_object_id,
    )


def delete_report(db: Session, report: Report) -> None:
    db.delete(report)
    db.commit()


def upsert_report(
    db: Session,
    *,
    report_id: UUID | None,
    name: str,
    description: str | None,
    definition: ReportDesignerDefinition | dict[str, Any],
    status: ReportStatus = ReportStatus.DRAFT,
    process_area_id: UUID | None = None,
    data_object_id: UUID | None = None,
) -> Report:
    if report_id:
        report = require_report(db, report_id)
        return update_report(
            db,
            report,
            name=name,
            description=description,
            definition=definition,
            status=status,
            process_area_id=process_area_id,
            data_object_id=data_object_id,
        )
    return create_report(
        db,
        name=name,
        description=description,
        definition=definition,
        status=status,
        process_area_id=process_area_id,
        data_object_id=data_object_id,
    )
