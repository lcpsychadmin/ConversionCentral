from __future__ import annotations

from functools import lru_cache
from typing import Generator, Optional

from sqlalchemy import create_engine, select
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session, sessionmaker

from app.config import get_settings
from app.database import SessionLocal
from app.models import DatabricksSqlSetting
from app.services.databricks_sql import DatabricksConnectionParams, build_sqlalchemy_url


@lru_cache(maxsize=1)
def get_ingestion_engine() -> Engine:
    settings = get_settings()
    override_url = getattr(settings, "ingestion_database_url", None)
    if override_url:
        return create_engine(override_url, pool_pre_ping=True, future=True)

    params = _resolve_databricks_params(settings)
    url = build_sqlalchemy_url(params)
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


def _resolve_databricks_params(settings) -> DatabricksConnectionParams:
    host = settings.databricks_host
    http_path = settings.databricks_http_path
    token = settings.databricks_token
    catalog = settings.databricks_catalog
    schema_name = settings.databricks_schema

    with SessionLocal() as session:
        stmt = (
            select(DatabricksSqlSetting)
            .where(DatabricksSqlSetting.is_active.is_(True))
            .order_by(DatabricksSqlSetting.updated_at.desc())
            .limit(1)
        )
        record = session.execute(stmt).scalars().first()
        if record:
            host = record.workspace_host or host
            http_path = record.http_path or http_path
            token = record.access_token or token
            catalog = record.catalog or catalog
            schema_name = record.schema_name or schema_name

    if not host or not http_path or not token:
        raise RuntimeError(
            "Databricks SQL warehouse configuration is missing. Configure the connection via the settings API or environment variables."
        )

    return DatabricksConnectionParams(
        workspace_host=host.strip(),
        http_path=http_path.strip(),
        access_token=token,
        catalog=catalog.strip() if isinstance(catalog, str) and catalog.strip() else None,
        schema_name=schema_name.strip() if isinstance(schema_name, str) and schema_name.strip() else None,
    )
