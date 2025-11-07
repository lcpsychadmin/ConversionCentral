from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Mapping, Sequence
from uuid import UUID, uuid4

from sqlalchemy import text
from sqlalchemy.engine import Engine

from app.config import get_settings
from app.ingestion.engine import (
    get_ingestion_connection_params,
    get_ingestion_engine,
)


def _quote_identifier(identifier: str) -> str:
    return f"`{identifier.replace('`', '``')}`"


def _format_schema_path(path: str | None) -> str:
    if not path:
        return ""
    parts = [part.strip() for part in path.split('.') if part.strip()]
    if not parts:
        return ""
    return ".".join(_quote_identifier(segment) for segment in parts)


class _Sentinel:
    """Placeholder to detect omitted optional arguments."""


_SENTINEL = _Sentinel()


@dataclass(frozen=True)
class ConstructedDataRecord:
    id: UUID
    constructed_table_id: UUID
    row_identifier: str | None
    payload: dict[str, Any]
    created_at: datetime
    updated_at: datetime


class ConstructedDataStore:
    """Persist constructed data rows inside the configured ingestion warehouse."""

    TABLE_NAME = "constructed_data_rows"
    LEGACY_TABLE_NAME = "constructed_data"
    DEFAULT_SCHEMA = "constructed_data"

    def __init__(self, *, engine: Engine | None = None, schema: str | None = None) -> None:
        settings = get_settings()
        self.engine = engine or get_ingestion_engine()
        override_url = settings.ingestion_database_url
        self._initialized = False

        self.dialect = self.engine.dialect.name
        if self.dialect == "sqlite":  # Used during tests
            self.backend = "sqlite"
            self.schema = None
        elif self.dialect == "databricks":
            self.backend = "databricks"
            if schema:
                self.schema = schema
            elif override_url:
                self.schema = self.DEFAULT_SCHEMA
            else:
                params = get_ingestion_connection_params()
                self.schema = (
                    params.constructed_schema
                    or params.schema_name
                    or self.DEFAULT_SCHEMA
                )
        else:
            # Fall back to the original relational table when using PostgreSQL or other RDBMS engines.
            self.backend = "rdbms"
            self.schema = None

        self._schema_sql = _format_schema_path(self.schema) if self.schema else ""

    def insert_row(
        self,
        *,
        constructed_table_id: UUID,
        payload: Mapping[str, Any],
        row_identifier: str | None = None,
    ) -> ConstructedDataRecord:
        row_id = uuid4()
        payload_json = self._encode_payload(payload)
        qualified_table = self._qualified_table()
        self._ensure_storage()

        if self.backend == "sqlite":
            now = datetime.now(timezone.utc).isoformat()
            stmt = text(
                """
                INSERT INTO constructed_data_rows (
                    row_id,
                    constructed_table_id,
                    row_identifier,
                    payload,
                    created_at,
                    updated_at
                ) VALUES (:row_id, :constructed_table_id, :row_identifier, :payload, :created_at, :updated_at)
                """
            )
            params = {
                "row_id": str(row_id),
                "constructed_table_id": str(constructed_table_id),
                "row_identifier": row_identifier,
                "payload": payload_json,
                "created_at": now,
                "updated_at": now,
            }
        elif self.backend == "databricks":
            stmt = text(
                f"""
                INSERT INTO {qualified_table} (
                    row_id,
                    constructed_table_id,
                    row_identifier,
                    payload,
                    created_at,
                    updated_at
                ) VALUES (:row_id, :constructed_table_id, :row_identifier, :payload, current_timestamp(), current_timestamp())
                """
            )
            params = {
                "row_id": str(row_id),
                "constructed_table_id": str(constructed_table_id),
                "row_identifier": row_identifier,
                "payload": payload_json,
            }
        else:
            stmt = text(
                f"""
                INSERT INTO {qualified_table} (
                    id,
                    constructed_table_id,
                    row_identifier,
                    payload,
                    created_at,
                    updated_at
                ) VALUES (:row_id, :constructed_table_id, :row_identifier, CAST(:payload AS JSONB), now(), now())
                """
            )
            params = {
                "row_id": str(row_id),
                "constructed_table_id": str(constructed_table_id),
                "row_identifier": row_identifier,
                "payload": payload_json,
            }

        with self.engine.begin() as connection:
            connection.execute(stmt, params)

        record = self.get_row(row_id)
        if record is None:  # pragma: no cover - defensive branch
            raise RuntimeError("Failed to load constructed data row after insert.")
        return record

    def insert_rows(
        self,
        constructed_table_id: UUID,
        rows: Sequence[Mapping[str, Any]],
        *,
        identifiers: Sequence[str | None] | None = None,
    ) -> list[ConstructedDataRecord]:
        if identifiers and len(identifiers) != len(rows):
            raise ValueError("Identifiers length must match rows length when provided.")

        self._ensure_storage()
        row_ids: list[UUID] = []
        qualified_table = self._qualified_table()

        if self.backend == "sqlite":
            now = datetime.now(timezone.utc).isoformat()
            stmt = text(
                """
                INSERT INTO constructed_data_rows (
                    row_id,
                    constructed_table_id,
                    row_identifier,
                    payload,
                    created_at,
                    updated_at
                ) VALUES (:row_id, :constructed_table_id, :row_identifier, :payload, :created_at, :updated_at)
                """
            )
        elif self.backend == "databricks":
            stmt = text(
                f"""
                INSERT INTO {qualified_table} (
                    row_id,
                    constructed_table_id,
                    row_identifier,
                    payload,
                    created_at,
                    updated_at
                ) VALUES (:row_id, :constructed_table_id, :row_identifier, :payload, current_timestamp(), current_timestamp())
                """
            )
        else:
            stmt = text(
                f"""
                INSERT INTO {qualified_table} (
                    id,
                    constructed_table_id,
                    row_identifier,
                    payload,
                    created_at,
                    updated_at
                ) VALUES (:row_id, :constructed_table_id, :row_identifier, CAST(:payload AS JSONB), now(), now())
                """
            )

        with self.engine.begin() as connection:
            for index, row in enumerate(rows):
                row_id = uuid4()
                row_ids.append(row_id)
                params = {
                    "row_id": str(row_id),
                    "constructed_table_id": str(constructed_table_id),
                    "row_identifier": (identifiers[index] if identifiers else None),
                    "payload": self._encode_payload(row),
                }
                if self.backend == "sqlite":
                    params["created_at"] = now
                    params["updated_at"] = now
                connection.execute(stmt, params)

        records: list[ConstructedDataRecord] = []
        for row_id in row_ids:
            record = self.get_row(row_id)
            if record is None:  # pragma: no cover - defensive branch
                raise RuntimeError("Failed to load constructed data row after bulk insert.")
            records.append(record)
        return records

    def get_row(self, row_id: UUID) -> ConstructedDataRecord | None:
        self._ensure_storage()
        select_columns = self._select_columns()
        table_name = self._qualified_table()
        id_column = self._id_column()
        stmt = text(
            f"""
            SELECT {select_columns}
            FROM {table_name}
            WHERE {id_column} = :row_id
            """
        )
        with self.engine.connect() as connection:
            result = connection.execute(stmt, {"row_id": str(row_id)}).mappings().first()

        if not result:
            return None
        return self._row_from_mapping(result)

    def list_rows(self, constructed_table_id: UUID) -> list[ConstructedDataRecord]:
        self._ensure_storage()
        select_columns = self._select_columns()
        table_name = self._qualified_table()
        stmt = text(
            f"""
            SELECT {select_columns}
            FROM {table_name}
            WHERE constructed_table_id = :constructed_table_id
            ORDER BY updated_at DESC
            """
        )
        with self.engine.connect() as connection:
            result = connection.execute(
                stmt,
                {"constructed_table_id": str(constructed_table_id)},
            ).mappings()
        return [self._row_from_mapping(row) for row in result]

    def list_all_rows(self) -> list[ConstructedDataRecord]:
        self._ensure_storage()
        select_columns = self._select_columns()
        table_name = self._qualified_table()
        stmt = text(
            f"""
            SELECT {select_columns}
            FROM {table_name}
            ORDER BY updated_at DESC
            """
        )
        with self.engine.connect() as connection:
            result = connection.execute(stmt).mappings()
        return [self._row_from_mapping(row) for row in result]

    def update_row(
        self,
        row_id: UUID,
        *,
        constructed_table_id: UUID | None = None,
        payload: Mapping[str, Any] | None = None,
        row_identifier: str | None | _Sentinel = _SENTINEL,
    ) -> ConstructedDataRecord:
        self._ensure_storage()
        assignments: list[str] = []
        params: dict[str, Any] = {"row_id": str(row_id)}

        if constructed_table_id is not None:
            assignments.append("constructed_table_id = :constructed_table_id")
            params["constructed_table_id"] = str(constructed_table_id)
        if payload is not None:
            if self.backend == "rdbms":
                assignments.append("payload = CAST(:payload AS JSONB)")
            else:
                assignments.append("payload = :payload")
            params["payload"] = self._encode_payload(payload)
        if row_identifier is not _SENTINEL:
            assignments.append("row_identifier = :row_identifier")
            params["row_identifier"] = row_identifier

        if not assignments:
            record = self.get_row(row_id)
            if record is None:
                raise KeyError(str(row_id))
            return record

        if self.backend == "sqlite":
            assignments.append("updated_at = :updated_at")
            params["updated_at"] = datetime.now(timezone.utc).isoformat()
        elif self.backend == "databricks":
            assignments.append("updated_at = current_timestamp()")
        else:
            assignments.append("updated_at = now()")

        set_clause = ", ".join(assignments)
        table_name = self._qualified_table()
        id_column = self._id_column()
        stmt = text(
            f"""
            UPDATE {table_name}
            SET {set_clause}
            WHERE {id_column} = :row_id
            """
        )

        with self.engine.begin() as connection:
            result = connection.execute(stmt, params)
            if result.rowcount == 0:
                raise KeyError(str(row_id))

        record = self.get_row(row_id)
        if record is None:  # pragma: no cover - defensive branch
            raise RuntimeError("Failed to load constructed data row after update.")
        return record

    def delete_row(self, row_id: UUID) -> None:
        self._ensure_storage()
        table_name = self._qualified_table()
        id_column = self._id_column()
        stmt = text(
            f"DELETE FROM {table_name} WHERE {id_column} = :row_id"
        )
        with self.engine.begin() as connection:
            connection.execute(stmt, {"row_id": str(row_id)})

    def delete_rows_for_table(self, constructed_table_id: UUID) -> int:
        self._ensure_storage()
        stmt = text(
            f"DELETE FROM {self._qualified_table()} WHERE constructed_table_id = :constructed_table_id"
        )
        with self.engine.begin() as connection:
            result = connection.execute(
                stmt,
                {"constructed_table_id": str(constructed_table_id)},
            )
        return result.rowcount or 0

    def _ensure_storage(self) -> None:
        if self._initialized:
            return
        if self.backend == "sqlite":
            create_stmt = text(
                """
                CREATE TABLE IF NOT EXISTS constructed_data_rows (
                    row_id TEXT PRIMARY KEY,
                    constructed_table_id TEXT NOT NULL,
                    row_identifier TEXT,
                    payload TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                )
                """
            )
            with self.engine.begin() as connection:
                connection.execute(create_stmt)
            self._initialized = True
            return

        if self.backend == "databricks":
            if not self.schema:
                raise RuntimeError("Constructed data schema is not configured for Databricks storage.")

            if not self._schema_sql:
                raise RuntimeError("Constructed data schema path is not valid for Databricks storage.")

            create_schema_stmt = text(f"CREATE SCHEMA IF NOT EXISTS {self._schema_sql}")
            create_table_stmt = text(
                f"""
                CREATE TABLE IF NOT EXISTS {self._qualified_table()} (
                    row_id STRING,
                    constructed_table_id STRING,
                    row_identifier STRING,
                    payload STRING,
                    created_at TIMESTAMP,
                    updated_at TIMESTAMP
                ) USING DELTA
                """
            )
            with self.engine.begin() as connection:
                connection.execute(create_schema_stmt)
                connection.execute(create_table_stmt)
            self._initialized = True
            return

        # Relational fallback (e.g., PostgreSQL) retains the legacy constructed_data table.
        create_table_stmt = text(
            """
            CREATE TABLE IF NOT EXISTS constructed_data (
                id UUID PRIMARY KEY,
                constructed_table_id UUID NOT NULL,
                row_identifier TEXT,
                payload JSONB NOT NULL,
                created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
            )
            """
        )
        create_index_stmt = text(
            "CREATE INDEX IF NOT EXISTS ix_constructed_data_constructed_table_id ON constructed_data (constructed_table_id)"
        )
        with self.engine.begin() as connection:
            connection.execute(create_table_stmt)
            connection.execute(create_index_stmt)
        self._initialized = True
        return

    def _qualified_table(self) -> str:
        if self.backend == "sqlite":
            return self.TABLE_NAME
        if self.backend == "databricks":
            if not self._schema_sql:
                raise RuntimeError("Constructed data schema path is not valid for Databricks storage.")
            return f"{self._schema_sql}.{_quote_identifier(self.TABLE_NAME)}"
        return self.LEGACY_TABLE_NAME

    def _select_columns(self) -> str:
        if self.backend in {"sqlite", "databricks"}:
            return "row_id, constructed_table_id, row_identifier, payload, created_at, updated_at"
        return "id, constructed_table_id, row_identifier, payload, created_at, updated_at"

    def _id_column(self) -> str:
        return "row_id" if self.backend in {"sqlite", "databricks"} else "id"

    def _row_from_mapping(self, row: Mapping[str, Any]) -> ConstructedDataRecord:
        payload = self._decode_payload(row.get("payload"))
        created_at = self._coerce_datetime(row.get("created_at"))
        updated_at = self._coerce_datetime(row.get("updated_at"))
        row_identifier = row.get("row_identifier")

        row_id_value = row.get("row_id") if "row_id" in row else row.get("id")
        constructed_table_value = row.get("constructed_table_id")

        if isinstance(row_id_value, UUID):
            row_uuid = row_id_value
        else:
            row_uuid = UUID(str(row_id_value))

        if isinstance(constructed_table_value, UUID):
            constructed_table_uuid = constructed_table_value
        else:
            constructed_table_uuid = UUID(str(constructed_table_value))

        return ConstructedDataRecord(
            id=row_uuid,
            constructed_table_id=constructed_table_uuid,
            row_identifier=row_identifier,
            payload=payload,
            created_at=created_at,
            updated_at=updated_at,
        )

    def _encode_payload(self, data: Mapping[str, Any]) -> str:
        return json.dumps(dict(data), separators=(",", ":"))

    def _decode_payload(self, value: Any) -> dict[str, Any]:
        if isinstance(value, str):
            try:
                return json.loads(value)
            except json.JSONDecodeError:  # pragma: no cover - defensive fallback
                return {}
        if value is None:
            return {}
        return dict(value)

    def _coerce_datetime(self, value: Any) -> datetime:
        if isinstance(value, datetime):
            return value
        if isinstance(value, str):
            normalized = value.replace(" ", "T")
            try:
                result = datetime.fromisoformat(normalized)
                if result.tzinfo is None:
                    return result.replace(tzinfo=timezone.utc)
                return result
            except ValueError:  # pragma: no cover - defensive fallback
                pass
        return datetime.now(timezone.utc)
