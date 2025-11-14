from __future__ import annotations

import csv
from datetime import datetime
from io import StringIO
from typing import List
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, Query, Response, status
from fastapi.responses import StreamingResponse
from sqlalchemy.orm import Session

from app.database import get_db
from app.models.entities import Report
from app.schemas.reporting import (
    ReportCreateRequest,
    ReportDatasetResponse,
    ReportDesignerDefinition,
    ReportListItem,
    ReportPreviewRequest,
    ReportPreviewResponse,
    ReportPublishRequest,
    ReportResponse,
    ReportStatus,
    ReportUpdateRequest,
)
from app.services.report_service import (
    ReportNotFoundError,
    create_report,
    delete_report,
    list_reports,
    publish_report as publish_report_record,
    require_report,
    update_report,
)
from app.services.reporting_designer import ReportPreviewError, generate_report_preview

router = APIRouter(prefix="/reporting", tags=["Reporting"])


def _serialize_list_item(report: Report) -> ReportListItem:
    product_team = report.process_area
    data_object = report.data_object
    return ReportListItem(
        id=report.id,
        name=report.name,
        description=report.description,
        status=ReportStatus(report.status),
        created_at=report.created_at,
        updated_at=report.updated_at,
        published_at=report.published_at,
        product_team_id=report.process_area_id,
        product_team_name=product_team.name if product_team else None,
        data_object_id=report.data_object_id,
        data_object_name=data_object.name if data_object else None,
    )


def _serialize_response(report: Report) -> ReportResponse:
    definition = ReportDesignerDefinition.parse_obj(report.definition)
    product_team = report.process_area
    data_object = report.data_object
    return ReportResponse(
        id=report.id,
        name=report.name,
        description=report.description,
        status=ReportStatus(report.status),
        created_at=report.created_at,
        updated_at=report.updated_at,
        published_at=report.published_at,
        product_team_id=report.process_area_id,
        product_team_name=product_team.name if product_team else None,
        data_object_id=report.data_object_id,
        data_object_name=data_object.name if data_object else None,
        definition=definition,
    )


def _build_dataset(
    db: Session,
    report: Report,
    *,
    limit: int,
) -> ReportDatasetResponse:
    if report.status != ReportStatus.PUBLISHED.value:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Only published reports can be previewed in the catalog.",
        )

    definition = ReportDesignerDefinition.parse_obj(report.definition)
    try:
        preview = generate_report_preview(db, definition, limit=limit)
    except ReportPreviewError as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc

    return ReportDatasetResponse(
        report_id=report.id,
        name=report.name,
        limit=preview.limit,
        row_count=preview.row_count,
        generated_at=datetime.utcnow(),
        columns=preview.columns,
        rows=preview.rows,
    )


@router.post("/preview", response_model=ReportPreviewResponse)
def preview_report(request: ReportPreviewRequest, db: Session = Depends(get_db)) -> ReportPreviewResponse:
    try:
        return generate_report_preview(db, request.definition, limit=request.limit)
    except ReportPreviewError as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc


@router.get("/reports", response_model=List[ReportListItem])
def list_saved_reports(
    status: ReportStatus | None = Query(default=None),
    db: Session = Depends(get_db),
) -> List[ReportListItem]:
    reports = list_reports(db, status)
    return [_serialize_list_item(report) for report in reports]


@router.post("/reports", response_model=ReportResponse, status_code=status.HTTP_201_CREATED)
def create_saved_report(request: ReportCreateRequest, db: Session = Depends(get_db)) -> ReportResponse:
    try:
        report = create_report(
            db,
            name=request.name,
            description=request.description,
            definition=request.definition,
            status=request.status,
            process_area_id=request.product_team_id,
            data_object_id=request.data_object_id,
        )
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc
    return _serialize_response(report)


@router.get("/reports/{report_id}", response_model=ReportResponse)
def read_report(report_id: UUID, db: Session = Depends(get_db)) -> ReportResponse:
    try:
        report = require_report(db, report_id)
    except ReportNotFoundError:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Report not found.") from None
    return _serialize_response(report)


@router.put("/reports/{report_id}", response_model=ReportResponse)
def update_saved_report(
    report_id: UUID,
    request: ReportUpdateRequest,
    db: Session = Depends(get_db),
) -> ReportResponse:
    try:
        report = require_report(db, report_id)
    except ReportNotFoundError:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Report not found.") from None

    try:
        process_area_id = report.process_area_id
        data_object_id = report.data_object_id
        if "product_team_id" in request.__fields_set__:
            process_area_id = request.product_team_id
        if "data_object_id" in request.__fields_set__:
            data_object_id = request.data_object_id

        report = update_report(
            db,
            report,
            name=request.name,
            description=request.description,
            definition=request.definition,
            status=request.status,
            process_area_id=process_area_id,
            data_object_id=data_object_id,
        )
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc

    return _serialize_response(report)


@router.post("/reports/{report_id}/publish", response_model=ReportResponse)
def publish_report(
    report_id: UUID,
    request: ReportPublishRequest | None = None,
    db: Session = Depends(get_db),
) -> ReportResponse:
    try:
        report = require_report(db, report_id)
    except ReportNotFoundError:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Report not found.") from None

    try:
        process_area_id = report.process_area_id
        data_object_id = report.data_object_id
        if request is not None:
            report = update_report(
                db,
                report,
                name=request.name,
                description=request.description,
                definition=request.definition,
                process_area_id=request.product_team_id,
                data_object_id=request.data_object_id,
            )
            process_area_id = report.process_area_id
            data_object_id = report.data_object_id
        report = publish_report_record(
            db,
            report,
            process_area_id=process_area_id,
            data_object_id=data_object_id,
        )
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc

    return _serialize_response(report)


@router.delete("/reports/{report_id}", status_code=status.HTTP_204_NO_CONTENT)
def delete_saved_report(report_id: UUID, db: Session = Depends(get_db)) -> Response:
    try:
        report = require_report(db, report_id)
    except ReportNotFoundError:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Report not found.") from None

    delete_report(db, report)
    return Response(status_code=status.HTTP_204_NO_CONTENT)


@router.get("/reports/{report_id}/dataset", response_model=ReportDatasetResponse)
def fetch_report_dataset(
    report_id: UUID,
    limit: int = Query(default=500, ge=1, le=5_000),
    db: Session = Depends(get_db),
) -> ReportDatasetResponse:
    try:
        report = require_report(db, report_id)
    except ReportNotFoundError:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Report not found.") from None

    return _build_dataset(db, report, limit=limit)


@router.get("/reports/{report_id}/export")
def export_report_dataset(
    report_id: UUID,
    limit: int = Query(default=5_000, ge=1, le=50_000),
    db: Session = Depends(get_db),
) -> StreamingResponse:
    try:
        report = require_report(db, report_id)
    except ReportNotFoundError:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Report not found.") from None

    dataset = _build_dataset(db, report, limit=limit)

    buffer = StringIO()
    writer = csv.DictWriter(buffer, fieldnames=dataset.columns, extrasaction="ignore")
    writer.writeheader()
    for row in dataset.rows:
        writer.writerow({column: row.get(column) for column in dataset.columns})

    buffer.seek(0)
    content = buffer.getvalue()
    safe_name = dataset.name.strip().replace("\n", " ").replace("\r", " ") or "report"
    normalized = "_".join(part for part in safe_name.split(" ") if part)
    sanitized = "".join(ch if ch.isalnum() or ch in {"_", "-"} else "_" for ch in normalized.lower())
    filename = f"{(sanitized or 'report')}_{dataset.report_id}.csv"

    headers = {
        "Content-Disposition": f"attachment; filename={filename}",
        "Cache-Control": "no-store",
    }

    return StreamingResponse(iter([content]), media_type="text/csv", headers=headers)
