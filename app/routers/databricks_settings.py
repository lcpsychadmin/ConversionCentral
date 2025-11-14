from __future__ import annotations

from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy import select, update
from sqlalchemy.orm import Session

from app.database import get_db
from app.models import DatabricksSqlSetting
from app.config import get_settings
from app.schemas import (
    DatabricksSqlSettingCreate,
    DatabricksSqlSettingRead,
    DatabricksSqlSettingTestRequest,
    DatabricksSqlSettingTestResult,
    DatabricksSqlSettingUpdate,
)
from app.ingestion import reset_ingestion_engine
from app.services.databricks_bootstrap import ensure_databricks_connection
from app.services.databricks_sql import (
    DatabricksConnectionError,
    DatabricksConnectionParams,
    test_databricks_connection,
)

router = APIRouter(prefix="/databricks/settings", tags=["Databricks Settings"])


def _serialize(
    setting: DatabricksSqlSetting,
    *,
    constructed_schema_fallback: str | None = None,
    ingestion_batch_rows_fallback: int | None = None,
    ingestion_method_fallback: str | None = None,
    spark_compute_fallback: str | None = None,
) -> DatabricksSqlSettingRead:
    constructed_schema = setting.constructed_schema
    if (constructed_schema is None or not constructed_schema.strip()) and constructed_schema_fallback:
        constructed_schema = constructed_schema_fallback.strip() or None

    ingestion_batch_rows = setting.ingestion_batch_rows
    if ingestion_batch_rows is None and ingestion_batch_rows_fallback:
        ingestion_batch_rows = ingestion_batch_rows_fallback

    ingestion_method = setting.ingestion_method or ingestion_method_fallback or "sql"
    ingestion_method = ingestion_method.strip().lower()

    spark_compute = setting.spark_compute or spark_compute_fallback
    spark_compute = spark_compute.strip().lower() if isinstance(spark_compute, str) and spark_compute.strip() else None

    return DatabricksSqlSettingRead(
        id=setting.id,
        display_name=setting.display_name,
        workspace_host=setting.workspace_host,
        http_path=setting.http_path,
        catalog=setting.catalog,
        schema_name=setting.schema_name,
        constructed_schema=constructed_schema,
        ingestion_batch_rows=ingestion_batch_rows,
        ingestion_method=ingestion_method,
        spark_compute=spark_compute,
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
    config = get_settings()
    return _serialize(
        setting,
        constructed_schema_fallback=config.databricks_constructed_schema,
        ingestion_batch_rows_fallback=config.databricks_ingestion_batch_rows,
        ingestion_method_fallback=config.databricks_ingestion_method,
        spark_compute_fallback=config.databricks_spark_compute,
    )


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
        constructed_schema=payload.constructed_schema,
        ingestion_batch_rows=payload.ingestion_batch_rows,
        ingestion_method=payload.ingestion_method,
        spark_compute=payload.spark_compute,
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
        constructed_schema=payload.constructed_schema.strip() if payload.constructed_schema else None,
        ingestion_batch_rows=payload.ingestion_batch_rows,
        ingestion_method=payload.ingestion_method or "sql",
        spark_compute=payload.spark_compute,
        warehouse_name=payload.warehouse_name.strip() if payload.warehouse_name else None,
        is_active=True,
    )

    db.add(setting)
    db.commit()
    db.refresh(setting)

    config = get_settings()
    record = _serialize(
        setting,
        constructed_schema_fallback=config.databricks_constructed_schema,
        ingestion_batch_rows_fallback=config.databricks_ingestion_batch_rows,
        ingestion_method_fallback=config.databricks_ingestion_method,
        spark_compute_fallback=config.databricks_spark_compute,
    )
    reset_ingestion_engine()
    ensure_databricks_connection()
    return record


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
        if "constructed_schema" in data:
            setting.constructed_schema = (
                data["constructed_schema"].strip() if data["constructed_schema"] else None
            )
        if "ingestion_batch_rows" in data:
            setting.ingestion_batch_rows = data["ingestion_batch_rows"]
        if "ingestion_method" in data and data["ingestion_method"] is not None:
            setting.ingestion_method = data["ingestion_method"]
        if "warehouse_name" in data:
            setting.warehouse_name = data["warehouse_name"].strip() if data["warehouse_name"] else None
        if "spark_compute" in data:
            value = data["spark_compute"]
            setting.spark_compute = value.strip().lower() if isinstance(value, str) and value.strip() else None
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
        for key in (
            "workspace_host",
            "http_path",
            "catalog",
            "schema_name",
            "constructed_schema",
            "ingestion_batch_rows",
            "warehouse_name",
            "ingestion_method",
            "access_token",
            "spark_compute",
        )
    )
    if should_validate and setting.access_token:
        params = DatabricksConnectionParams(
            workspace_host=setting.workspace_host,
            http_path=setting.http_path,
            access_token=setting.access_token,
            catalog=setting.catalog,
            schema_name=setting.schema_name,
            constructed_schema=setting.constructed_schema,
            ingestion_batch_rows=setting.ingestion_batch_rows,
            ingestion_method=setting.ingestion_method,
            spark_compute=setting.spark_compute,
        )
        try:
            test_databricks_connection(params)
        except DatabricksConnectionError as exc:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc

    db.commit()
    db.refresh(setting)
    config = get_settings()
    record = _serialize(
        setting,
        constructed_schema_fallback=config.databricks_constructed_schema,
        ingestion_batch_rows_fallback=config.databricks_ingestion_batch_rows,
        ingestion_method_fallback=config.databricks_ingestion_method,
        spark_compute_fallback=config.databricks_spark_compute,
    )
    reset_ingestion_engine()
    ensure_databricks_connection()
    return record


@router.post("/test", response_model=DatabricksSqlSettingTestResult)
def test_databricks_setting(payload: DatabricksSqlSettingTestRequest) -> DatabricksSqlSettingTestResult:
    params = DatabricksConnectionParams(
        workspace_host=payload.workspace_host.strip(),
        http_path=payload.http_path.strip(),
        access_token=payload.access_token,
        catalog=payload.catalog.strip() if payload.catalog else None,
        schema_name=payload.schema_name.strip() if payload.schema_name else None,
        constructed_schema=payload.constructed_schema.strip() if payload.constructed_schema else None,
        ingestion_batch_rows=payload.ingestion_batch_rows,
        ingestion_method=payload.ingestion_method or "sql",
        spark_compute=payload.spark_compute,
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

