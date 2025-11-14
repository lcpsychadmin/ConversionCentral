from __future__ import annotations

from functools import lru_cache
from typing import Generator, Optional
from uuid import UUID

from sqlalchemy import create_engine, select
from sqlalchemy.engine import Engine
from sqlalchemy.pool import StaticPool
from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy.exc import OperationalError, ProgrammingError

from app.config import get_settings
from app.database import SessionLocal
from app.models import DatabricksSqlSetting, SapHanaSetting
from app.services.databricks_sql import DatabricksConnectionParams, build_sqlalchemy_url as build_databricks_url
from app.services.sap_hana_sql import SapHanaConnectionParams, build_sqlalchemy_url as build_sap_hana_url


@lru_cache(maxsize=1)
def get_ingestion_engine() -> Engine:
    settings = get_settings()
    override_url = getattr(settings, "ingestion_database_url", None)
    if override_url:
        engine_kwargs: dict[str, object] = {"pool_pre_ping": True, "future": True}
        if override_url.startswith("sqlite"):
            engine_kwargs["connect_args"] = {"check_same_thread": False}
            engine_kwargs["poolclass"] = StaticPool
        return create_engine(override_url, **engine_kwargs)

    params = get_ingestion_connection_params()
    url = build_databricks_url(params)
    return create_engine(url, pool_pre_ping=True, future=True)


@lru_cache(maxsize=1)
def _get_session_factory() -> sessionmaker[Session]:
    return sessionmaker(bind=get_ingestion_engine(), autocommit=False, autoflush=False, future=True)


def get_ingestion_session() -> Generator[Session, None, None]:
    factory = _get_session_factory()
    session = factory()
    try:
        yield session
    finally:
        session.close()


def reset_ingestion_engine() -> None:
    # Helper for tests to force the engine/session factory to be rebuilt with new settings.
    get_ingestion_engine.cache_clear()
    _get_session_factory.cache_clear()
    get_ingestion_connection_params.cache_clear()


def _resolve_databricks_params(settings) -> DatabricksConnectionParams:
    host = settings.databricks_host
    http_path = settings.databricks_http_path
    token = settings.databricks_token
    catalog = settings.databricks_catalog
    schema_name = settings.databricks_schema
    constructed_schema = settings.databricks_constructed_schema
    ingestion_batch_rows = settings.databricks_ingestion_batch_rows
    ingestion_method = settings.databricks_ingestion_method or "sql"
    spark_compute = settings.databricks_spark_compute

    record = None
    with SessionLocal() as session:
        stmt = (
            select(DatabricksSqlSetting)
            .where(DatabricksSqlSetting.is_active.is_(True))
            .order_by(DatabricksSqlSetting.updated_at.desc())
            .limit(1)
        )
        try:
            record = session.execute(stmt).scalars().first()
        except (OperationalError, ProgrammingError):
            session.rollback()
        else:
            if record:
                host = record.workspace_host or host
                http_path = record.http_path or http_path
                token = record.access_token or token
                catalog = record.catalog or catalog
                schema_name = record.schema_name or schema_name
                constructed_schema = record.constructed_schema or constructed_schema
                ingestion_batch_rows = record.ingestion_batch_rows or ingestion_batch_rows
                ingestion_method = record.ingestion_method or ingestion_method
                spark_compute = record.spark_compute or spark_compute

    if not host or not http_path or not token:
        raise RuntimeError(
            "Databricks SQL warehouse configuration is missing. Configure the connection via the settings API or environment variables."
        )

    # Unity Catalog workspaces expose the "workspace" catalog by default. When neither
    # environment variables nor stored settings define a catalog, fall back to
    # "workspace" so metadata browsing does not generate NO_SUCH_CATALOG errors.
    if not catalog:
        catalog = "workspace"

    return DatabricksConnectionParams(
        workspace_host=host.strip(),
        http_path=http_path.strip(),
        access_token=token,
        catalog=catalog.strip() if isinstance(catalog, str) and catalog.strip() else None,
        schema_name=schema_name.strip() if isinstance(schema_name, str) and schema_name.strip() else None,
        constructed_schema=constructed_schema.strip()
        if isinstance(constructed_schema, str) and constructed_schema.strip()
        else None,
        ingestion_batch_rows=int(ingestion_batch_rows)
        if isinstance(ingestion_batch_rows, int) and ingestion_batch_rows > 0
        else None,
        ingestion_method=(ingestion_method or "sql").strip().lower(),
        spark_compute=(spark_compute.strip().lower() if isinstance(spark_compute, str) and spark_compute.strip() else None),
    )


@lru_cache(maxsize=1)
def get_ingestion_connection_params() -> DatabricksConnectionParams:
    settings = get_settings()
    return _resolve_databricks_params(settings)


@lru_cache(maxsize=8)
def get_sap_hana_connection_params(setting_id: UUID | None = None) -> SapHanaConnectionParams:
    with SessionLocal() as session:
        if setting_id is not None:
            record = session.get(SapHanaSetting, setting_id)
        else:
            stmt = (
                select(SapHanaSetting)
                .order_by(SapHanaSetting.is_active.desc(), SapHanaSetting.updated_at.desc())
                .limit(1)
            )
            record = session.execute(stmt).scalars().first()

        if not record or not record.host or not record.username:
            raise RuntimeError("SAP HANA configuration is missing. Configure the warehouse before scheduling ingestion.")

        password = record.password
        if not password:
            raise RuntimeError("SAP HANA configuration does not include credentials. Update the settings with a password.")

        return SapHanaConnectionParams(
            host=record.host.strip(),
            port=record.port or 30015,
            username=record.username.strip(),
            password=password,
            database_name=record.database_name.strip(),
            schema_name=record.schema_name.strip() if record.schema_name else None,
            tenant=record.tenant.strip() if record.tenant else None,
            use_ssl=bool(record.use_ssl),
            ingestion_batch_rows=record.ingestion_batch_rows,
        )


@lru_cache(maxsize=8)
def get_sap_hana_engine(setting_id: UUID | None = None) -> Engine:
    params = get_sap_hana_connection_params(setting_id)
    url = build_sap_hana_url(params)
    return create_engine(url, pool_pre_ping=True, future=True)


def reset_sap_hana_engine() -> None:
    get_sap_hana_engine.cache_clear()
    get_sap_hana_connection_params.cache_clear()
