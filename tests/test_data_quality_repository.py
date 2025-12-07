from __future__ import annotations

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


def _build_session_factory() -> sessionmaker:
    engine = create_engine("sqlite:///:memory:", future=True)
    Base.metadata.create_all(engine)
    return sessionmaker(bind=engine, future=True)


def _make_seed(group_suffix: str) -> DataQualitySeed:
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
        project_key="system:alpha",
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
        project_key="system:alpha",
        name="Alpha",
        description=None,
        sql_flavor="databricks",
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

    assert projects == ["system:alpha"]
    assert table_groups == ["group:two"]
    assert tables == ["selection:two:table"]
