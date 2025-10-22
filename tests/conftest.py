import os
import sys
from pathlib import Path

from collections.abc import Generator

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import create_engine
from sqlalchemy.pool import StaticPool
from sqlalchemy.orm import Session, sessionmaker

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

TEST_DATABASE_URL = "sqlite://"
os.environ.setdefault("DATABASE_URL", TEST_DATABASE_URL)
os.environ.setdefault("INGESTION_DATABASE_URL", TEST_DATABASE_URL)
os.environ.setdefault("ENABLE_SQL_SERVER_SYNC", "false")

from app.database import Base, get_db
from app.main import app


def _create_testing_engine():
    return create_engine(
        TEST_DATABASE_URL,
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
        future=True,
    )


def _create_test_sessionmaker(engine):
    return sessionmaker(bind=engine, autocommit=False, autoflush=False, future=True)


@pytest.fixture(scope="session")
def engine():
    engine = _create_testing_engine()
    Base.metadata.create_all(bind=engine)
    yield engine
    Base.metadata.drop_all(bind=engine)


@pytest.fixture(scope="function")
def db_session(engine) -> Generator[Session, None, None]:
    Base.metadata.drop_all(bind=engine)
    Base.metadata.create_all(bind=engine)
    connection = engine.connect()
    transaction = connection.begin()

    TestingSessionLocal = _create_test_sessionmaker(connection)
    session = TestingSessionLocal()

    try:
        yield session
    finally:
        session.close()
        transaction.rollback()
        connection.close()


class PrefixedTestClient(TestClient):
    api_prefix = "/api"

    def request(self, method: str, url: str, *args, **kwargs):  # type: ignore[override]
        if url.startswith("/"):
            url = f"{self.api_prefix}{url}"
        return super().request(method, url, *args, **kwargs)


@pytest.fixture()
def client(db_session: Session) -> TestClient:
    def override_get_db():
        try:
            yield db_session
        finally:
            pass

    app.dependency_overrides[get_db] = override_get_db
    return PrefixedTestClient(app)
