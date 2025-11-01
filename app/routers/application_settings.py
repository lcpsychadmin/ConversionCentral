from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.orm import Session

from app.database import get_control_db
from app.schemas import AdminEmailSetting, AdminEmailUpdate
from app.services.application_settings import get_admin_email, set_admin_email

router = APIRouter(prefix="/application-settings", tags=["Application Settings"])


@router.get("/admin-email", response_model=AdminEmailSetting)
def read_admin_email(db: Session = Depends(get_control_db)) -> AdminEmailSetting:
    email = get_admin_email(db)
    return AdminEmailSetting(email=email)


@router.put("/admin-email", response_model=AdminEmailSetting, status_code=status.HTTP_200_OK)
def update_admin_email(payload: AdminEmailUpdate, db: Session = Depends(get_control_db)) -> AdminEmailSetting:
    try:
        stored_email = set_admin_email(db, payload.email)
    except Exception as exc:  # pragma: no cover - defensive
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Unable to store admin email") from exc
    return AdminEmailSetting(email=stored_email)


__all__ = ["router"]
