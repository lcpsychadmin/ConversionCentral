from __future__ import annotations

from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy import select, update
from sqlalchemy.orm import Session

from app.database import get_db
from app.models import DatabricksSqlSetting
from app.schemas import (
    DatabricksSqlSettingCreate,
    DatabricksSqlSettingRead,
    DatabricksSqlSettingTestRequest,
    DatabricksSqlSettingTestResult,
    DatabricksSqlSettingUpdate,
)
from app.services.databricks_sql import (
    DatabricksConnectionError,
    DatabricksConnectionParams,
    test_databricks_connection,
)

router = APIRouter(prefix="/databricks/settings", tags=["Databricks Settings"])


def _serialize(setting: DatabricksSqlSetting) -> DatabricksSqlSettingRead:
    return DatabricksSqlSettingRead(
        id=setting.id,
        display_name=setting.display_name,
        workspace_host=setting.workspace_host,
        http_path=setting.http_path,
        catalog=setting.catalog,
        schema_name=setting.schema_name,
        warehouse_name=setting.warehouse_name,
        is_active=setting.is_active,
        created_at=setting.created_at,
        updated_at=setting.updated_at,
        has_access_token=bool(setting.access_token),
    )


def _get_active_setting(db: Session) -> DatabricksSqlSetting | None:
    stmt = (
        select(DatabricksSqlSetting)
        .order_by(DatabricksSqlSetting.is_active.desc(), DatabricksSqlSetting.updated_at.desc())
        .limit(1)
    )
    result = db.execute(stmt).scalars().first()
    return result


@router.get("", response_model=DatabricksSqlSettingRead | None)
def get_databricks_setting(db: Session = Depends(get_db)) -> DatabricksSqlSettingRead | None:
    setting = _get_active_setting(db)
    if not setting:
        return None
    return _serialize(setting)


@router.post("", response_model=DatabricksSqlSettingRead, status_code=status.HTTP_201_CREATED)
def create_databricks_setting(
    payload: DatabricksSqlSettingCreate,
    db: Session = Depends(get_db),
) -> DatabricksSqlSettingRead:
    existing = _get_active_setting(db)
    if existing:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="A Databricks connection is already configured. Update the existing settings instead.",
        )

    params = DatabricksConnectionParams(
        workspace_host=payload.workspace_host,
        http_path=payload.http_path,
        access_token=payload.access_token,
        catalog=payload.catalog,
        schema_name=payload.schema_name,
    )
    try:
        test_databricks_connection(params)
    except DatabricksConnectionError as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc

    setting = DatabricksSqlSetting(
        display_name=payload.display_name,
        workspace_host=payload.workspace_host.strip(),
        http_path=payload.http_path.strip(),
        access_token=payload.access_token,
        catalog=payload.catalog.strip() if payload.catalog else None,
        schema_name=payload.schema_name.strip() if payload.schema_name else None,
        warehouse_name=payload.warehouse_name.strip() if payload.warehouse_name else None,
        is_active=True,
    )

    db.add(setting)
    db.commit()
    db.refresh(setting)

    return _serialize(setting)


@router.put("/{setting_id}", response_model=DatabricksSqlSettingRead)
def update_databricks_setting(
    setting_id: UUID,
    payload: DatabricksSqlSettingUpdate,
    db: Session = Depends(get_db),
) -> DatabricksSqlSettingRead:
    setting = db.get(DatabricksSqlSetting, setting_id)
    if not setting:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Databricks setting not found")

    data = payload.dict(exclude_unset=True)

    if data:
        if "workspace_host" in data and data["workspace_host"] is not None:
            setting.workspace_host = data["workspace_host"].strip()
        if "http_path" in data and data["http_path"] is not None:
            setting.http_path = data["http_path"].strip()
        if "display_name" in data and data["display_name"] is not None:
            setting.display_name = data["display_name"].strip()
        if "catalog" in data:
            setting.catalog = data["catalog"].strip() if data["catalog"] else None
        if "schema_name" in data:
            setting.schema_name = data["schema_name"].strip() if data["schema_name"] else None
        if "warehouse_name" in data:
            setting.warehouse_name = data["warehouse_name"].strip() if data["warehouse_name"] else None
        if "access_token" in data:
            setting.access_token = data["access_token"] if data["access_token"] else None
        if data.get("is_active") is True and not setting.is_active:
            setting.is_active = True
            # Deactivate any other rows.
            db.execute(
                update(DatabricksSqlSetting)
                .where(DatabricksSqlSetting.id != setting.id)
                .values(is_active=False)
            )
        elif data.get("is_active") is False:
            setting.is_active = False

    # Validate the connection if any core parameters changed.
    should_validate = any(
        key in data
        for key in ("workspace_host", "http_path", "catalog", "schema_name", "warehouse_name", "access_token")
    )
    if should_validate and setting.access_token:
        params = DatabricksConnectionParams(
            workspace_host=setting.workspace_host,
            http_path=setting.http_path,
            access_token=setting.access_token,
            catalog=setting.catalog,
            schema_name=setting.schema_name,
        )
        try:
            test_databricks_connection(params)
        except DatabricksConnectionError as exc:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc

    db.commit()
    db.refresh(setting)
    return _serialize(setting)


@router.post("/test", response_model=DatabricksSqlSettingTestResult)
def test_databricks_setting(payload: DatabricksSqlSettingTestRequest) -> DatabricksSqlSettingTestResult:
    params = DatabricksConnectionParams(
        workspace_host=payload.workspace_host.strip(),
        http_path=payload.http_path.strip(),
        access_token=payload.access_token,
        catalog=payload.catalog.strip() if payload.catalog else None,
        schema_name=payload.schema_name.strip() if payload.schema_name else None,
    )
    try:
        elapsed_ms, summary = test_databricks_connection(params)
    except DatabricksConnectionError as exc:
        return DatabricksSqlSettingTestResult(success=False, message=str(exc))
    return DatabricksSqlSettingTestResult(
        success=True,
        message=f"Connection successful ({summary}).",
        duration_ms=elapsed_ms,
    )

