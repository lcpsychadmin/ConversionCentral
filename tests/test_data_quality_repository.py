from __future__ import annotations

from uuid import UUID

from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

from app.database import Base
from app.services.data_quality_repository import LocalDataQualityRepository
from app.services.data_quality_seed import (
    ConnectionSeed,
    DataQualitySeed,
    ProjectSeed,
    TableGroupSeed,
    TableSeed,
)


WORKSPACE_ID = "a0000000-0000-0000-0000-000000000001"


def _build_session_factory() -> sessionmaker:
    engine = create_engine("sqlite:///:memory:", future=True)
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine, future=True)
    with Session.begin() as session:
        session.execute(
            text(
                """
                INSERT INTO workspaces (id, name, is_default, is_active, created_at, updated_at)
                VALUES (:id, 'Test Workspace', 1, 1, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
                """
            ),
            {"id": WORKSPACE_ID},
        )
    return Session


def _make_seed(group_suffix: str) -> DataQualitySeed:
    project_key = f"workspace:{WORKSPACE_ID}"
    table = TableSeed(
        table_id=f"selection:{group_suffix}:table",
        table_group_id=f"group:{group_suffix}",
        schema_name="main",
        table_name=f"tbl_{group_suffix}",
        source_table_id="source-table",
    )
    group = TableGroupSeed(
        table_group_id=f"group:{group_suffix}",
        connection_id="conn:alpha",
        name=f"Group {group_suffix}",
        description=None,
        tables=(table,),
    )
    connection = ConnectionSeed(
        connection_id="conn:alpha",
        project_key=project_key,
        system_id="system:alpha",
        name="Alpha",
        catalog="workspace",
        schema_name="dq",
        http_path="/sql/test",
        managed_credentials_ref=None,
        is_active=True,
        table_groups=(group,),
    )
    project = ProjectSeed(
        project_key=project_key,
        name="Alpha",
        description=None,
        sql_flavor="databricks",
        workspace_id=UUID(WORKSPACE_ID),
        connections=(connection,),
    )
    return DataQualitySeed(projects=(project,))


def test_local_repository_seed_and_prune():
    session_factory = _build_session_factory()
    repo = LocalDataQualityRepository(session_factory=session_factory)

    repo.seed_metadata(_make_seed("one"))

    with session_factory() as session:
        assert session.execute(text("SELECT COUNT(*) FROM dq_projects")).scalar_one() == 1
        assert session.execute(text("SELECT COUNT(*) FROM dq_tables")).scalar_one() == 1

    repo.seed_metadata(_make_seed("two"))

    with session_factory() as session:
        projects = session.execute(text("SELECT project_key FROM dq_projects")).scalars().all()
        table_groups = session.execute(text("SELECT table_group_id FROM dq_table_groups")).scalars().all()
        tables = session.execute(text("SELECT table_id FROM dq_tables")).scalars().all()

    assert projects == [f"workspace:{WORKSPACE_ID}"]
    assert table_groups == ["group:two"]
    assert tables == ["selection:two:table"]
