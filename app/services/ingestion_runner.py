from __future__ import annotations

from collections.abc import Mapping
from datetime import datetime, timezone

from sqlalchemy import MetaData, Table as SqlAlchemyTable, create_engine, select

from app.models import IngestionJob, SystemConnection, Table as TableModel
from app.schemas import SystemConnectionType
from app.services.connection_resolver import UnsupportedConnectionError, resolve_sqlalchemy_url
from app.services.connection_testing import ConnectionTestError
from app.services.ingestion_storage import DatabricksIngestionStorage


class IngestionRunner:
    """Coordinates extracting data from a source connection and loading it into Databricks."""

    def __init__(self, storage: DatabricksIngestionStorage | None = None):
        self.storage = storage or DatabricksIngestionStorage()

    def ingest_table(
        self,
        system_connection: SystemConnection,
        table_model: TableModel,
        *,
        job: IngestionJob | None = None,
        batch_size: int = 5_000,
        replace: bool = False,
    ) -> int:
        if job is not None:
            job.status = "running"
            job.started_at = job.started_at or datetime.now(timezone.utc)

        engine = None
        total_rows = 0
        try:
            connection_type = (
                system_connection.connection_type
                if isinstance(system_connection.connection_type, SystemConnectionType)
                else SystemConnectionType(system_connection.connection_type)
            )
            source_url = resolve_sqlalchemy_url(
                connection_type,
                system_connection.connection_string,
            )

            engine = create_engine(source_url, future=True, pool_pre_ping=True)
            metadata = MetaData()
            source_table = SqlAlchemyTable(
                table_model.physical_name or table_model.name,
                metadata,
                schema=table_model.schema_name,
                autoload_with=engine,
            )
            stmt = select(source_table)

            with engine.connect() as connection:
                result = connection.execution_options(stream_results=True).execute(stmt)
                buffer: list[Mapping[str, object]] = []
                for row in result.mappings():
                    buffer.append(dict(row))
                    if len(buffer) >= batch_size:
                        total_rows += self.storage.load_rows(
                            table_model,
                            buffer,
                            replace=replace if total_rows == 0 else False,
                        )
                        buffer.clear()
                if buffer:
                    total_rows += self.storage.load_rows(
                        table_model,
                        buffer,
                        replace=replace if total_rows == 0 else False,
                    )
        except UnsupportedConnectionError as exc:
            if job is not None:
                job.status = "failed"
                job.completed_at = datetime.now(timezone.utc)
                job.notes = (str(exc) or "Unsupported connection.")[:2000]
            raise ConnectionTestError(str(exc)) from exc
        except Exception as exc:
            if job is not None:
                job.status = "failed"
                job.completed_at = datetime.now(timezone.utc)
                job.notes = (str(exc) or "Ingestion failed.")[:2000]
            raise
        finally:
            if engine is not None:
                engine.dispose()

        if job is not None:
            self._mark_job_complete(job, total_rows)

        return total_rows

    def _mark_job_complete(self, job: IngestionJob, row_count: int) -> None:
        job.row_count = row_count
        job.status = "completed"
        job.completed_at = datetime.now(timezone.utc)
