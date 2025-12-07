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

from app.database import Base, get_db, get_control_db  # noqa: E402
from app.main import app  # noqa: E402
from app.models import Workspace  # noqa: E402


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

    TestingSessionLocal = _create_test_sessionmaker(connection)
    session = TestingSessionLocal()

    # Seed a default workspace so API flows that require one always succeed during tests.
    session.add(
        Workspace(
            name="Default Workspace",
            description="Test workspace",
            is_default=True,
            is_active=True,
        )
    )
    session.commit()

    try:
        yield session
    finally:
        session.close()
        connection.close()


class PrefixedTestClient(TestClient):
    api_prefix = "/api"

    def request(self, method: str, url: str, *args, **kwargs):  # type: ignore[override]
        if url.startswith("/"):
            url = f"{self.api_prefix}{url}"
        return super().request(method, url, *args, **kwargs)


@pytest.fixture()
def client(db_session: Session) -> Generator[TestClient, None, None]:
    def override_get_db():
        try:
            yield db_session
        finally:
            pass

    def override_get_control_db():
        try:
            yield db_session
        finally:
            pass

    app.dependency_overrides[get_db] = override_get_db
    app.dependency_overrides[get_control_db] = override_get_control_db

    client = PrefixedTestClient(app)
    try:
        yield client
    finally:
        app.dependency_overrides.pop(get_db, None)
        app.dependency_overrides.pop(get_control_db, None)
