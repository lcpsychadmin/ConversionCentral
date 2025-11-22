"""Reusable helpers for persisting profiling outputs from Databricks notebooks."""

from __future__ import annotations

from contextlib import contextmanager, suppress
from datetime import datetime, timezone
import json
import time
import uuid
from typing import Any, Mapping, MutableMapping, Sequence, TYPE_CHECKING, Protocol


class _DataFrameProtocol(Protocol):
    columns: Sequence[str]

    def count(self) -> int: ...

    def createOrReplaceTempView(self, name: str) -> None: ...


if TYPE_CHECKING:  # pragma: no cover - used purely for type checking / editors
    from pyspark.sql import DataFrame as DataFrameLike
else:
    DataFrameLike = _DataFrameProtocol


class ProfilingMetadataWriter:
    """Executes idempotent MERGEs into the ConversionCentral metadata tables.

    The writer is designed to run from within the Databricks profiling notebook. It
    intentionally limits its Spark surface area so unit tests can stub the ``spark``
    session without requiring a full Databricks environment.
    """

    def __init__(
        self,
        spark_session: Any,
        *,
        schema: str,
        profile_run_id: str,
        catalog: str | None = None,
    ) -> None:
        schema = (schema or "").strip()
        profile_run_id = (profile_run_id or "").strip()
        if not schema:
            raise ValueError("Data quality schema is required for ProfilingMetadataWriter.")
        if not profile_run_id:
            raise ValueError("profile_run_id is required for ProfilingMetadataWriter.")

        self._spark = spark_session
        self._schema = schema
        self._catalog = (catalog or "").strip()
        self._profile_run_id = profile_run_id

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------
    def merge_dataframe(
        self,
        df: DataFrameLike | None,
        *,
        target_table: str,
        key_columns: Sequence[str],
        update_columns: Sequence[str] | None = None,
    ) -> int:
        """Merge a DataFrame into the specified metadata table.

        Args:
            df: Spark DataFrame containing the rows to upsert.
            target_table: Unqualified metadata table name (e.g., ``"dq_profile_results"``).
            key_columns: Column names that uniquely identify records.
            update_columns: Optional override for the columns that should be updated.

        Returns:
            Number of rows present in ``df``.
        """

        qualified_table = self._metadata_table(target_table)
        if df is None:
            self._record_operation(qualified_table, rows_written=0, status="skipped", duration_ms=0)
            return 0

        tracker: MutableMapping[str, Any]
        with self._track_operation(qualified_table) as tracker:
            row_count = int(self._safe_count(df))
            if row_count == 0:
                tracker["status"] = "skipped"
                tracker["rows_written"] = 0
                return 0

            columns = self._normalize_columns(df, update_columns)
            self._validate_keys_present(columns, key_columns)

            tracker["rows_written"] = row_count
            temp_view = self._register_temp_view(df, target_table)
            try:
                merge_sql = self._build_merge_sql(
                    qualified_table=qualified_table,
                    temp_view=temp_view,
                    key_columns=key_columns,
                    update_columns=columns,
                )
                self._spark.sql(merge_sql)
            finally:
                self._drop_temp_view(temp_view)

        return row_count

    # ------------------------------------------------------------------
    # Operation tracking
    # ------------------------------------------------------------------
    @contextmanager
    def _track_operation(self, target_table: str) -> MutableMapping[str, Any]:
        tracker: MutableMapping[str, Any] = {
            "rows_written": 0,
            "status": "success",
            "error_payload": None,
        }
        started_at = datetime.now(timezone.utc)
        start = time.perf_counter()
        try:
            yield tracker
        except Exception as exc:
            tracker["status"] = "failed"
            tracker["error_payload"] = self._serialize_error(exc)
            raise
        finally:
            completed_at = datetime.now(timezone.utc)
            duration_ms = int((time.perf_counter() - start) * 1000)
            self._record_operation(
                target_table,
                rows_written=int(tracker.get("rows_written", 0) or 0),
                status=str(tracker.get("status", "success")),
                duration_ms=duration_ms,
                error_payload=tracker.get("error_payload"),
                started_at=started_at,
                completed_at=completed_at,
            )

    def _record_operation(
        self,
        target_table: str,
        *,
        rows_written: int,
        status: str,
        duration_ms: int,
        error_payload: str | None = None,
        started_at: datetime | None = None,
        completed_at: datetime | None = None,
    ) -> None:
        operations_table = self._metadata_table("dq_profile_operations")
        operation_id = uuid.uuid4().hex
        columns = (
            "operation_id",
            "profile_run_id",
            "target_table",
            "rows_written",
            "duration_ms",
            "status",
            "error_payload",
            "started_at",
            "completed_at",
        )
        values = (
            self._sql_literal(operation_id),
            self._sql_literal(self._profile_run_id),
            self._sql_literal(target_table),
            str(int(rows_written) if rows_written is not None else 0),
            str(int(duration_ms) if duration_ms is not None else 0),
            self._sql_literal(status),
            self._sql_literal(error_payload),
            self._sql_literal(started_at or datetime.now(timezone.utc)),
            self._sql_literal(completed_at or datetime.now(timezone.utc)),
        )
        columns_sql = ", ".join(self._escape_identifier(column) for column in columns)
        values_sql = ", ".join(values)
        statement = f"INSERT INTO {operations_table} ({columns_sql}) VALUES ({values_sql})"
        self._spark.sql(statement)

    # ------------------------------------------------------------------
    # SQL helpers
    # ------------------------------------------------------------------
    def _build_merge_sql(
        self,
        *,
        qualified_table: str,
        temp_view: str,
        key_columns: Sequence[str],
        update_columns: Sequence[str],
    ) -> str:
        join_conditions = " AND ".join(
            f"target.{self._escape_identifier(column)} = source.{self._escape_identifier(column)}"
            for column in key_columns
        )
        update_assignments = ", ".join(
            f"target.{self._escape_identifier(column)} = source.{self._escape_identifier(column)}"
            for column in update_columns
        )
        insert_columns = ", ".join(self._escape_identifier(column) for column in update_columns)
        insert_values = ", ".join(
            f"source.{self._escape_identifier(column)}" for column in update_columns
        )
        return (
            f"MERGE INTO {qualified_table} AS target "
            f"USING {temp_view} AS source "
            f"ON {join_conditions} "
            f"WHEN MATCHED THEN UPDATE SET {update_assignments} "
            f"WHEN NOT MATCHED THEN INSERT ({insert_columns}) VALUES ({insert_values})"
        )

    def _register_temp_view(self, df: DataFrameLike, base_name: str) -> str:
        view_name = f"_cc_{base_name}_{uuid.uuid4().hex}"
        df.createOrReplaceTempView(view_name)
        return view_name

    def _drop_temp_view(self, view_name: str) -> None:
        with suppress(Exception):
            catalog = getattr(self._spark, "catalog", None)
            if catalog is not None:
                catalog.dropTempView(view_name)

    def _metadata_table(self, table_name: str) -> str:
        escaped_table = self._escape_identifier(table_name)
        if self._catalog:
            return f"{self._escape_identifier(self._catalog)}.{self._escape_identifier(self._schema)}.{escaped_table}"
        return f"{self._escape_identifier(self._schema)}.{escaped_table}"

    @staticmethod
    def _escape_identifier(identifier: str) -> str:
        cleaned = (identifier or "").strip().replace("`", "")
        if not cleaned:
            raise ValueError("Identifiers cannot be empty.")
        return f"`{cleaned}`"

    @staticmethod
    def _normalize_columns(df: DataFrameLike, update_columns: Sequence[str] | None) -> list[str]:
        if update_columns:
            ordered = list(dict.fromkeys(update_columns))
        else:
            ordered = list(dict.fromkeys(getattr(df, "columns", [])))
        if not ordered:
            raise ValueError("DataFrame must expose at least one column before merge.")
        return ordered

    @staticmethod
    def _validate_keys_present(columns: Sequence[str], key_columns: Sequence[str]) -> None:
        missing = [column for column in key_columns if column not in columns]
        if missing:
            missing_list = ", ".join(missing)
            raise ValueError(f"Merge keys missing from DataFrame columns: {missing_list}")

    @staticmethod
    def _safe_count(df: DataFrameLike) -> int:
        try:
            return int(df.count())
        except Exception as exc:  # pragma: no cover - defensive path
            raise RuntimeError("Unable to count rows for merge operation") from exc

    @staticmethod
    def _serialize_error(exc: Exception) -> str:
        payload: Mapping[str, Any] = {
            "type": exc.__class__.__name__,
            "message": str(exc),
        }
        serialized = json.dumps(payload, ensure_ascii=False)
        return serialized[:4000]

    @staticmethod
    def _sql_literal(value: Any) -> str:
        if value is None:
            return "NULL"
        if isinstance(value, datetime):
            normalized = value.astimezone(timezone.utc)
            return f"'{normalized.strftime('%Y-%m-%d %H:%M:%S')}'"
        text = str(value).replace("'", "''")
        return f"'{text}'"
