from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.orm import Session

from app.database import get_control_db
from app.models import ApplicationDatabaseSetting
from app.schemas import (
    ApplicationDatabaseApplyRequest,
    ApplicationDatabaseSettingRead,
    ApplicationDatabaseStatus,
    ApplicationDatabaseTestRequest,
    ApplicationDatabaseTestResult,
)
from app.services.application_database import (
    ApplicationDatabaseConnectionError,
    ApplicationDatabaseMigrationError,
    apply_application_database_setting,
    get_active_setting,
    test_application_database,
)
from app.services.application_settings import get_admin_email

router = APIRouter(prefix="/application-database", tags=["Application Database"])


def _serialize(setting: ApplicationDatabaseSetting) -> ApplicationDatabaseSettingRead:
    return ApplicationDatabaseSettingRead(
        id=setting.id,
        engine=setting.engine,
        connection_display=setting.connection_display,
        applied_at=setting.applied_at,
        display_name=setting.display_name,
        created_at=setting.created_at,
        updated_at=setting.updated_at,
    )


@router.get("/status", response_model=ApplicationDatabaseStatus)
def get_application_database_status(db: Session = Depends(get_control_db)) -> ApplicationDatabaseStatus:
    setting = get_active_setting(db)
    admin_email = get_admin_email(db)
    if not setting:
        return ApplicationDatabaseStatus(configured=False, setting=None, admin_email=admin_email)
    return ApplicationDatabaseStatus(configured=True, setting=_serialize(setting), admin_email=admin_email)


@router.post("/test", response_model=ApplicationDatabaseTestResult)
def test_application_database_endpoint(
    payload: ApplicationDatabaseTestRequest,
) -> ApplicationDatabaseTestResult:
    return test_application_database(payload)


@router.post("/apply", response_model=ApplicationDatabaseSettingRead, status_code=status.HTTP_201_CREATED)
def apply_application_database_endpoint(
    payload: ApplicationDatabaseApplyRequest,
    db: Session = Depends(get_control_db),
) -> ApplicationDatabaseSettingRead:
    try:
        setting = apply_application_database_setting(db, payload)
    except ApplicationDatabaseConnectionError as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc
    except ApplicationDatabaseMigrationError as exc:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(exc)) from exc
    return _serialize(setting)


__all__ = ["router"]
