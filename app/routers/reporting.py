from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.orm import Session

from app.database import get_db
from app.schemas.reporting import ReportPreviewRequest, ReportPreviewResponse
from app.services.reporting_designer import ReportPreviewError, generate_report_preview

router = APIRouter(prefix="/reporting", tags=["Reporting"])


@router.post("/preview", response_model=ReportPreviewResponse)
def preview_report(request: ReportPreviewRequest, db: Session = Depends(get_db)) -> ReportPreviewResponse:
    try:
        return generate_report_preview(db, request.definition, limit=request.limit)
    except ReportPreviewError as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc
