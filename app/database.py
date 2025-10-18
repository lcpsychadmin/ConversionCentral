from sqlalchemy import create_engine
from sqlalchemy.orm import declarative_base, sessionmaker

from app.config import get_settings

settings = get_settings()


def _create_engine_with_fallback(url: str):
    try:
        return create_engine(url, future=True)
    except ModuleNotFoundError as exc:
        if "psycopg2" in str(exc) and "psycopg2" in url:
            fallback_url = url.replace("psycopg2", "psycopg")
            try:
                __import__("psycopg")
            except ModuleNotFoundError:
                raise
            return create_engine(fallback_url, future=True)
        raise


engine = _create_engine_with_fallback(settings.database_url)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine, future=True)

Base = declarative_base()


def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()
