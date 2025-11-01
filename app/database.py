from __future__ import annotations

from threading import Lock
from typing import Generator

from sqlalchemy import create_engine, select
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session, declarative_base, sessionmaker
from sqlalchemy.pool import StaticPool

from app.config import get_settings

Base = declarative_base()

settings = get_settings()
_DEFAULT_DATABASE_URL = settings.database_url

_engine_lock = Lock()


def _create_engine_with_fallback(url: str):
    engine_kwargs: dict[str, object] = {"future": True}
    if url.startswith("sqlite"):
        engine_kwargs["connect_args"] = {"check_same_thread": False}
        engine_kwargs["poolclass"] = StaticPool

    try:
        return create_engine(url, **engine_kwargs)
    except ModuleNotFoundError as exc:
        if "psycopg2" in str(exc) and "psycopg2" in url:
            fallback_url = url.replace("psycopg2", "psycopg")
            try:
                __import__("psycopg")
            except ModuleNotFoundError:
                raise
            return create_engine(fallback_url, future=True)
        raise


_control_engine = _create_engine_with_fallback(_DEFAULT_DATABASE_URL)
ControlSessionLocal = sessionmaker(
    autocommit=False,
    autoflush=False,
    bind=_control_engine,
    future=True,
)

SessionLocal = sessionmaker(autocommit=False, autoflush=False, future=True)
_runtime_engine = None
_current_runtime_url = _DEFAULT_DATABASE_URL


def _load_active_database_url() -> str:
    try:
        from app.models.entities import ApplicationDatabaseSetting
    except ImportError:
        return _DEFAULT_DATABASE_URL

    try:
        with ControlSessionLocal() as session:
            stmt = (
                select(ApplicationDatabaseSetting)
                .order_by(
                    ApplicationDatabaseSetting.is_active.desc(),
                    ApplicationDatabaseSetting.applied_at.desc(),
                )
                .limit(1)
            )
            setting = session.execute(stmt).scalars().first()
            if setting and setting.engine != "default_postgres" and setting.connection_url:
                return setting.connection_url
    except SQLAlchemyError:
        return _DEFAULT_DATABASE_URL

    return _DEFAULT_DATABASE_URL


def _initialize_runtime_engine() -> None:
    global _runtime_engine, _current_runtime_url
    target_url = _load_active_database_url()
    engine = _create_engine_with_fallback(target_url)
    SessionLocal.configure(bind=engine)
    _runtime_engine = engine
    _current_runtime_url = target_url


_initialize_runtime_engine()


def refresh_runtime_engine(new_url: str | None = None) -> str:
    global _runtime_engine, _current_runtime_url
    target_url = new_url or _load_active_database_url()

    with _engine_lock:
        if _runtime_engine is not None and target_url == _current_runtime_url:
            return _current_runtime_url

        new_engine = _create_engine_with_fallback(target_url)
        SessionLocal.configure(bind=new_engine)

        if _runtime_engine is not None:
            _runtime_engine.dispose()

        _runtime_engine = new_engine
        _current_runtime_url = target_url
        return _current_runtime_url


def get_runtime_database_url() -> str:
    return _current_runtime_url


def get_db() -> Generator[Session, None, None]:
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


def get_control_db() -> Generator[Session, None, None]:
    db = ControlSessionLocal()
    try:
        yield db
    finally:
        db.close()
