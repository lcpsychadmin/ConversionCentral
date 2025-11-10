from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.orm import Session

from app.database import get_control_db
from app.schemas import AdminEmailSetting, AdminEmailUpdate, CompanySettingsRead, CompanySettingsUpdate
from app.services.application_settings import (
    get_admin_email,
    get_company_settings,
    set_admin_email,
    set_company_settings,
)

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


@router.get("/company", response_model=CompanySettingsRead)
def read_company_settings(db: Session = Depends(get_control_db)) -> CompanySettingsRead:
    return get_company_settings(db)


@router.put("/company", response_model=CompanySettingsRead)
def update_company_settings(
    payload: CompanySettingsUpdate,
    db: Session = Depends(get_control_db),
) -> CompanySettingsRead:
    updates = payload.dict(exclude_unset=True)
    if not updates:
        return get_company_settings(db)

    current = get_company_settings(db)
    site_title = updates.get("site_title", current.site_title)
    logo_data_url = updates.get("logo_data_url", current.logo_data_url)
    theme_mode = updates.get("theme_mode", current.theme_mode)
    accent_color = updates.get("accent_color", current.accent_color)

    try:
        return set_company_settings(
            db,
            site_title=site_title,
            logo_data_url=logo_data_url,
            theme_mode=theme_mode,
            accent_color=accent_color,
        )
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc
    except Exception as exc:  # pragma: no cover - defensive
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Unable to store company settings") from exc


__all__ = ["router"]
