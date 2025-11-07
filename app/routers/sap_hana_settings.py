from __future__ import annotations

from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy import select, update
from sqlalchemy.orm import Session

from app.database import get_db
from app.models import SapHanaSetting
from app.schemas import (
    SapHanaSettingCreate,
    SapHanaSettingRead,
    SapHanaSettingTestRequest,
    SapHanaSettingTestResult,
    SapHanaSettingUpdate,
)
from app.services.sap_hana_sql import SapHanaConnectionParams, SapHanaConnectionError, test_sap_hana_connection
from app.ingestion.engine import reset_sap_hana_engine

router = APIRouter(prefix="/sap-hana/settings", tags=["SAP HANA Settings"])


def _serialize(setting: SapHanaSetting) -> SapHanaSettingRead:
    return SapHanaSettingRead(
        id=setting.id,
        display_name=setting.display_name,
        host=setting.host,
        port=setting.port,
        database_name=setting.database_name,
        username=setting.username,
        schema_name=setting.schema_name,
        tenant=setting.tenant,
        use_ssl=setting.use_ssl,
        ingestion_batch_rows=setting.ingestion_batch_rows,
        is_active=setting.is_active,
        created_at=setting.created_at,
        updated_at=setting.updated_at,
        has_password=bool(setting.password),
    )


def _get_active_setting(db: Session) -> SapHanaSetting | None:
    stmt = (
        select(SapHanaSetting)
        .order_by(SapHanaSetting.is_active.desc(), SapHanaSetting.updated_at.desc())
        .limit(1)
    )
    return db.execute(stmt).scalars().first()


def _build_params(
    *,
    host: str,
    port: int,
    username: str,
    password: str,
    database_name: str,
    schema_name: str | None,
    tenant: str | None,
    use_ssl: bool,
    ingestion_batch_rows: int | None,
) -> SapHanaConnectionParams:
    return SapHanaConnectionParams(
        host=host.strip(),
        port=port,
        username=username.strip(),
        password=password,
        database_name=database_name.strip(),
        schema_name=schema_name.strip() if schema_name else None,
        tenant=tenant.strip() if tenant else None,
        use_ssl=use_ssl,
        ingestion_batch_rows=ingestion_batch_rows,
    )


@router.get("", response_model=SapHanaSettingRead | None)
def get_sap_hana_setting(db: Session = Depends(get_db)) -> SapHanaSettingRead | None:
    setting = _get_active_setting(db)
    if not setting:
        return None
    return _serialize(setting)


@router.post("", response_model=SapHanaSettingRead, status_code=status.HTTP_201_CREATED)
def create_sap_hana_setting(
    payload: SapHanaSettingCreate,
    db: Session = Depends(get_db),
) -> SapHanaSettingRead:
    existing = _get_active_setting(db)
    if existing:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="An SAP HANA configuration already exists. Update the existing settings instead.",
        )

    params = _build_params(
        host=payload.host,
        port=payload.port,
        username=payload.username,
        password=payload.password,
        database_name=payload.database_name,
        schema_name=payload.schema_name,
        tenant=payload.tenant,
        use_ssl=payload.use_ssl,
        ingestion_batch_rows=payload.ingestion_batch_rows,
    )
    try:
        test_sap_hana_connection(params)
    except SapHanaConnectionError as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc

    setting = SapHanaSetting(
        display_name=payload.display_name.strip() if payload.display_name else "SAP HANA Warehouse",
        host=params.host,
        port=params.port,
        username=params.username,
        password=params.password,
        database_name=params.database_name,
        schema_name=params.schema_name,
        tenant=params.tenant,
        use_ssl=params.use_ssl,
        ingestion_batch_rows=params.ingestion_batch_rows,
        is_active=True,
    )
    db.add(setting)
    db.commit()
    db.refresh(setting)

    reset_sap_hana_engine()

    return _serialize(setting)


@router.put("/{setting_id}", response_model=SapHanaSettingRead)
def update_sap_hana_setting(
    setting_id: UUID,
    payload: SapHanaSettingUpdate,
    db: Session = Depends(get_db),
) -> SapHanaSettingRead:
    setting = db.get(SapHanaSetting, setting_id)
    if not setting:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="SAP HANA setting not found")

    data = payload.dict(exclude_unset=True)

    if data:
        if "display_name" in data and data["display_name"] is not None:
            setting.display_name = data["display_name"].strip() or "SAP HANA Warehouse"
        if "host" in data and data["host"] is not None:
            setting.host = data["host"].strip()
        if "port" in data and data["port"] is not None:
            setting.port = int(data["port"])
        if "database_name" in data and data["database_name"] is not None:
            setting.database_name = data["database_name"].strip()
        if "username" in data and data["username"] is not None:
            setting.username = data["username"].strip()
        if "password" in data:
            setting.password = data["password"] if data["password"] else None
        if "schema_name" in data:
            setting.schema_name = data["schema_name"].strip() if data["schema_name"] else None
        if "tenant" in data:
            setting.tenant = data["tenant"].strip() if data["tenant"] else None
        if "use_ssl" in data and data["use_ssl"] is not None:
            setting.use_ssl = bool(data["use_ssl"])
        if "ingestion_batch_rows" in data:
            setting.ingestion_batch_rows = data["ingestion_batch_rows"]
        if data.get("is_active") is True and not setting.is_active:
            setting.is_active = True
            db.execute(
                update(SapHanaSetting)
                .where(SapHanaSetting.id != setting.id)
                .values(is_active=False)
            )
        elif data.get("is_active") is False:
            setting.is_active = False

    should_validate = any(
        key in data
        for key in ("host", "port", "database_name", "username", "password", "tenant", "use_ssl", "schema_name")
    )
    effective_password = setting.password

    if should_validate and effective_password:
        params = _build_params(
            host=setting.host,
            port=setting.port,
            username=setting.username,
            password=effective_password,
            database_name=setting.database_name,
            schema_name=setting.schema_name,
            tenant=setting.tenant,
            use_ssl=setting.use_ssl,
            ingestion_batch_rows=setting.ingestion_batch_rows,
        )
        try:
            test_sap_hana_connection(params)
        except SapHanaConnectionError as exc:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc

    db.add(setting)
    db.commit()
    db.refresh(setting)

    reset_sap_hana_engine()

    return _serialize(setting)


@router.post("/test", response_model=SapHanaSettingTestResult)
def test_sap_hana_setting(payload: SapHanaSettingTestRequest) -> SapHanaSettingTestResult:
    params = _build_params(
        host=payload.host,
        port=payload.port,
        username=payload.username,
        password=payload.password,
        database_name=payload.database_name,
        schema_name=payload.schema_name,
        tenant=payload.tenant,
        use_ssl=payload.use_ssl,
        ingestion_batch_rows=payload.ingestion_batch_rows,
    )
    try:
        elapsed_ms, summary = test_sap_hana_connection(params)
    except SapHanaConnectionError as exc:
        return SapHanaSettingTestResult(success=False, message=str(exc), duration_ms=None)
    return SapHanaSettingTestResult(success=True, message=f"Connection successful ({summary}).", duration_ms=elapsed_ms)
