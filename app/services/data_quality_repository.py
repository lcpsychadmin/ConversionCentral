from __future__ import annotations

from collections.abc import Callable

from sqlalchemy.orm import Session

from app.database import SessionLocal
from app.models.data_quality import (
    DataQualityConnection,
    DataQualityProject,
    DataQualityTable,
    DataQualityTableGroup,
)
from app.services.data_quality_seed import (
    ConnectionSeed,
    DataQualitySeed,
    ProjectSeed,
    TableGroupSeed,
)


class LocalDataQualityRepository:
    """Persist data quality metadata inside the local application database."""

    def __init__(self, session_factory: Callable[[], Session] | None = None) -> None:
        self._session_factory = session_factory or SessionLocal

    def seed_metadata(self, seed: DataQualitySeed) -> None:
        if not seed.projects:
            return

        with self._session_factory() as session:
            self._apply_seed(session, seed.projects)
            session.commit()

    def _apply_seed(self, session: Session, projects: tuple[ProjectSeed, ...]) -> None:
        active_project_keys: set[str] = set()

        for project in projects:
            self._upsert_project(session, project)
            active_project_keys.add(project.project_key)
            connection_ids = self._upsert_connections(session, project)
            self._prune_connections(session, project.project_key, connection_ids)

        self._prune_projects(session, active_project_keys)

    def _upsert_project(self, session: Session, project: ProjectSeed) -> None:
        record = session.get(DataQualityProject, project.project_key)
        if record is None:
            record = DataQualityProject(project_key=project.project_key)
            session.add(record)
        record.name = project.name
        record.description = project.description
        record.sql_flavor = project.sql_flavor

    def _upsert_connections(self, session: Session, project: ProjectSeed) -> set[str]:
        active_connection_ids: set[str] = set()

        for conn in project.connections:
            record = session.get(DataQualityConnection, conn.connection_id)
            if record is None:
                record = DataQualityConnection(connection_id=conn.connection_id, project_key=project.project_key)
                session.add(record)
            record.project_key = project.project_key
            record.name = conn.name
            record.catalog = conn.catalog
            record.schema_name = conn.schema_name
            record.http_path = conn.http_path
            record.managed_credentials_ref = conn.managed_credentials_ref
            record.is_active = conn.is_active

            group_ids = self._upsert_table_groups(session, conn)
            self._prune_table_groups(session, conn.connection_id, group_ids)
            active_connection_ids.add(conn.connection_id)

        return active_connection_ids

    def _upsert_table_groups(self, session: Session, connection_seed: ConnectionSeed) -> set[str]:
        active_group_ids: set[str] = set()

        for group in connection_seed.table_groups:
            record = session.get(DataQualityTableGroup, group.table_group_id)
            if record is None:
                record = DataQualityTableGroup(table_group_id=group.table_group_id, connection_id=connection_seed.connection_id)
                session.add(record)
            record.connection_id = connection_seed.connection_id
            record.name = group.name
            record.description = group.description
            record.profiling_include_mask = group.profiling_include_mask
            record.profiling_exclude_mask = group.profiling_exclude_mask
            record.profiling_job_id = group.profiling_job_id

            table_ids = self._upsert_tables(session, group)
            self._prune_tables(session, group.table_group_id, table_ids)
            active_group_ids.add(group.table_group_id)

        return active_group_ids

    def _upsert_tables(self, session: Session, group_seed: TableGroupSeed) -> set[str]:
        active_table_ids: set[str] = set()

        for table in group_seed.tables:
            record = session.get(DataQualityTable, table.table_id)
            if record is None:
                record = DataQualityTable(table_id=table.table_id, table_group_id=group_seed.table_group_id)
                session.add(record)
            record.table_group_id = group_seed.table_group_id
            record.schema_name = table.schema_name
            record.table_name = table.table_name
            record.source_table_id = table.source_table_id
            active_table_ids.add(table.table_id)

        return active_table_ids

    def _prune_projects(self, session: Session, keep_keys: set[str]) -> None:
        query = session.query(DataQualityProject.project_key)
        if keep_keys:
            query = query.filter(~DataQualityProject.project_key.in_(keep_keys))
        stale_keys = [row[0] for row in query.all()]
        if not stale_keys:
            return
        stale_connection_ids = [
            row[0]
            for row in session.query(DataQualityConnection.connection_id)
            .filter(DataQualityConnection.project_key.in_(stale_keys))
            .all()
        ]
        if stale_connection_ids:
            stale_group_ids = [
                row[0]
                for row in session.query(DataQualityTableGroup.table_group_id)
                .filter(DataQualityTableGroup.connection_id.in_(stale_connection_ids))
                .all()
            ]
            if stale_group_ids:
                session.query(DataQualityTable).filter(
                    DataQualityTable.table_group_id.in_(stale_group_ids)
                ).delete(synchronize_session=False)
                session.query(DataQualityTableGroup).filter(
                    DataQualityTableGroup.table_group_id.in_(stale_group_ids)
                ).delete(synchronize_session=False)
            session.query(DataQualityConnection).filter(
                DataQualityConnection.connection_id.in_(stale_connection_ids)
            ).delete(synchronize_session=False)
        session.query(DataQualityProject).filter(DataQualityProject.project_key.in_(stale_keys)).delete(
            synchronize_session=False
        )

    def _prune_connections(self, session: Session, project_key: str, keep_ids: set[str]) -> None:
        query = session.query(DataQualityConnection.connection_id).filter(
            DataQualityConnection.project_key == project_key
        )
        if keep_ids:
            query = query.filter(~DataQualityConnection.connection_id.in_(keep_ids))
        stale_ids = [row[0] for row in query.all()]
        if not stale_ids:
            return
        session.query(DataQualityTableGroup).filter(
            DataQualityTableGroup.connection_id.in_(stale_ids)
        ).delete(synchronize_session=False)
        session.query(DataQualityConnection).filter(
            DataQualityConnection.connection_id.in_(stale_ids)
        ).delete(synchronize_session=False)

    def _prune_table_groups(self, session: Session, connection_id: str, keep_ids: set[str]) -> None:
        query = session.query(DataQualityTableGroup.table_group_id).filter(
            DataQualityTableGroup.connection_id == connection_id
        )
        if keep_ids:
            query = query.filter(~DataQualityTableGroup.table_group_id.in_(keep_ids))
        stale_ids = [row[0] for row in query.all()]
        if not stale_ids:
            return
        session.query(DataQualityTable).filter(DataQualityTable.table_group_id.in_(stale_ids)).delete(
            synchronize_session=False
        )
        session.query(DataQualityTableGroup).filter(DataQualityTableGroup.table_group_id.in_(stale_ids)).delete(
            synchronize_session=False
        )

    def _prune_tables(self, session: Session, table_group_id: str, keep_ids: set[str]) -> None:
        query = session.query(DataQualityTable).filter(DataQualityTable.table_group_id == table_group_id)
        if keep_ids:
            query = query.filter(~DataQualityTable.table_id.in_(keep_ids))
        query.delete(synchronize_session=False)
