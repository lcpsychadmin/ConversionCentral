from __future__ import annotations

from functools import lru_cache
from typing import Generator, Optional

from sqlalchemy import create_engine
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session, sessionmaker

from app.config import get_settings


@lru_cache(maxsize=1)
def get_ingestion_engine() -> Engine:
    settings = get_settings()
    url = getattr(settings, "ingestion_database_url", None)
    if not url:
        raise RuntimeError(
            "Ingestion database URL is not configured. Set the INGESTION_DATABASE_URL environment variable."
        )

    connect_args: dict[str, object] = {}
    if url.startswith("mssql+pyodbc"):
        # Ensure connections stay healthy and leverage fast executemany for bulk loads.
        connect_args = {"autocommit": False}

    return create_engine(url, pool_pre_ping=True, future=True, connect_args=connect_args)


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
