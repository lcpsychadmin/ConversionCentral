from __future__ import annotations

import itertools
import json
import logging
import queue
import threading
import uuid
from contextlib import suppress
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Iterable, Optional, Sequence, cast

from sqlalchemy import MetaData, Table as SqlTable, and_, case, create_engine, func, select
from sqlalchemy.engine import Engine
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.sql import sqltypes as satypes

from app.database import SessionLocal
from app.ingestion.engine import get_ingestion_connection_params
from app.models import (
    DataQualityDataTableCharacteristic,
    DataQualityProfileColumn,
    DataQualityProfileColumnValue,
    DataQualityProfileResult,
    DataQualityTable,
    SystemConnection,
)
from app.schemas import SystemConnectionType
from app.services.connection_resolver import UnsupportedConnectionError, resolve_sqlalchemy_url
from app.services.data_quality_backend import LOCAL_BACKEND, get_metadata_backend
from app.services.data_quality_keys import parse_table_group_id
from app.services.data_quality_local_client import LocalTestGenClient
from app.services.data_quality_profiling import PreparedProfileRun, ProfilingLaunchResult
from app.services.data_quality_testgen import TestGenClient, TestGenClientError

logger = logging.getLogger(__name__)


_DUMMY_LITERAL_VALUES = (
    "blank",
    "error",
    "missing",
    "tbd",
    "n/a",
    "na",
    "#na",
    "none",
    "null",
    "unknown",
    "(blank)",
    "(error)",
    "(missing)",
    "(tbd)",
    "(n/a)",
    "(#na)",
    "(none)",
    "(null)",
    "(unknown)",
    "[blank]",
    "[error]",
    "[missing]",
    "[tbd]",
    "[n/a]",
    "[#na]",
    "[none]",
    "[null]",
    "[unknown]",
    ".",
    "?",
)


def _shape_pattern(value: str | None) -> str | None:
    if value is None:
        return None
    tokens: list[str] = []
    for char in value:
        if char.isalpha():
            tokens.append("A" if char.isupper() else "a")
        elif char.isdigit():
            tokens.append("9")
        elif char.isspace():
            tokens.append(" ")
        elif char in {"-", "_"}:
            tokens.append(char)
        else:
            tokens.append("#")
    return "".join(tokens) if tokens else None


@dataclass(frozen=True)
class _ConnectionSnapshot:
    id: uuid.UUID
    connection_type: SystemConnectionType
    connection_string: str


@dataclass(frozen=True)
class _TableDefinition:
    table_id: str
    schema_name: str | None
    table_name: str


@dataclass
class _ColumnProfile:
    column_name: str
    data_type: str | None
    general_type: str
    ordinal_position: int
    row_count: int
    non_null_count: int
    null_count: int
    distinct_count: int | None
    min_value: str | None
    max_value: str | None
    avg_value: float | None
    metrics: dict[str, Any]
    top_values: list[tuple[str | None, int]]


@dataclass
class _TableProfile:
    definition: _TableDefinition
    row_count: int
    columns: list[_ColumnProfile]


def _create_testgen_client() -> TestGenClient:
    backend = get_metadata_backend()
    if backend == LOCAL_BACKEND:
        return cast(TestGenClient, LocalTestGenClient())

    params = get_ingestion_connection_params()
    schema = (params.data_quality_schema or "").strip()
    if not schema:
        raise RuntimeError("Databricks data quality schema is not configured; cannot run profiling locally.")
    return TestGenClient(params, schema=schema)


class LocalProfilingRunner:
    """Executes profiling runs inside the API container instead of Databricks."""

    _SENTINEL = object()

    def __init__(self) -> None:
        self._jobs: "queue.Queue[tuple[int, PreparedProfileRun] | object]" = queue.Queue()
        self._thread: threading.Thread | None = None
        self._stop_event = threading.Event()
        self._job_id_counter = itertools.count(1)

    def start(self) -> None:
        if self._thread and self._thread.is_alive():
            return
        logger.info("Starting local profiling runner")
        self._stop_event.clear()
        self._thread = threading.Thread(target=self._run_loop, name="local-profiling-runner", daemon=True)
        self._thread.start()

    def stop(self) -> None:
        if not self._thread:
            return
        logger.info("Stopping local profiling runner")
        self._stop_event.set()
        self._jobs.put(self._SENTINEL)
        with suppress(RuntimeError):
            self._thread.join(timeout=5)
        self._thread = None

    def enqueue(self, prepared: PreparedProfileRun) -> ProfilingLaunchResult:
        if not self._thread or not self._thread.is_alive():
            logger.warning("Local profiling runner was not running; starting automatically.")
            self.start()

        job_id = next(self._job_id_counter)
        self._jobs.put((job_id, prepared))
        return ProfilingLaunchResult(
            table_group_id=prepared.target.table_group_id,
            profile_run_id=prepared.profile_run_id,
            job_id=job_id,
            databricks_run_id=job_id,
        )

    def _run_loop(self) -> None:
        while not self._stop_event.is_set():
            item = self._jobs.get()
            if item is self._SENTINEL:
                break
            job_id, prepared = item  # type: ignore[misc]
            self._process_job(job_id, prepared)

    def _process_job(self, job_id: int, prepared: PreparedProfileRun) -> None:
        logger.info(
            "Executing local profiling run job_id=%s table_group_id=%s profile_run_id=%s",
            job_id,
            prepared.target.table_group_id,
            prepared.profile_run_id,
        )
        try:
            run_inputs = self._load_run_inputs(prepared)
            table_profiles = self._profile_connection(run_inputs)
            if not table_profiles:
                raise RuntimeError("No tables produced profiling metrics for this run.")
            total_rows = sum(profile.row_count for profile in table_profiles)
            self._persist_profile_rows(
                prepared.profile_run_id,
                prepared.target.table_group_id,
                table_profiles,
            )
            self._complete_run(
                prepared.profile_run_id,
                status="succeeded",
                row_count=total_rows,
                anomaly_count=0,
            )
        except Exception as exc:  # pragma: no cover - defensive logging
            logger.exception(
                "Local profiling run failed job_id=%s table_group_id=%s: %s",
                job_id,
                prepared.target.table_group_id,
                exc,
            )
            self._complete_run(prepared.profile_run_id, status="failed")

    def _complete_run(
        self,
        profile_run_id: str,
        *,
        status: str,
        row_count: Optional[int] = None,
        anomaly_count: Optional[int] = None,
    ) -> None:
        client: TestGenClient | None = None
        try:
            client = _create_testgen_client()
            client.complete_profile_run(
                profile_run_id,
                status=status,
                row_count=row_count,
                anomaly_count=anomaly_count,
                anomalies=(),
            )
        except TestGenClientError as exc:  # pragma: no cover - defensive logging
            logger.warning("Failed to finalize local profile run %s: %s", profile_run_id, exc)
        finally:
            if client is not None:
                with suppress(Exception):
                    client.close()

    # ------------------------------------------------------------------
    # Run context + inputs
    # ------------------------------------------------------------------
    def _load_run_inputs(self, prepared: PreparedProfileRun) -> tuple[_ConnectionSnapshot, list[_TableDefinition]]:
        connection_id_text, _ = parse_table_group_id(prepared.target.table_group_id)
        if not connection_id_text:
            raise RuntimeError("Table group is not linked to a system connection.")
        try:
            connection_uuid = uuid.UUID(connection_id_text)
        except ValueError as exc:
            raise RuntimeError("Table group connection identifier is invalid.") from exc

        with SessionLocal() as session:
            connection: SystemConnection | None = session.get(SystemConnection, connection_uuid)
            if connection is None:
                raise RuntimeError("System connection referenced by table group no longer exists.")

            table_rows: Sequence[DataQualityTable] = (
                session.query(DataQualityTable)
                .filter(DataQualityTable.table_group_id == prepared.target.table_group_id)
                .order_by(
                    DataQualityTable.schema_name.asc().nullslast(),
                    DataQualityTable.table_name.asc().nullslast(),
                )
                .all()
            )
            tables: list[_TableDefinition] = []
            for row in table_rows:
                name = (row.table_name or "").strip()
                if not name:
                    continue
                tables.append(
                    _TableDefinition(
                        table_id=row.table_id,
                        schema_name=(row.schema_name or "").strip() or None,
                        table_name=name,
                    )
                )

        if not tables:
            raise RuntimeError("Table group does not include any tables to profile.")

        try:
            connection_type = (
                connection.connection_type
                if isinstance(connection.connection_type, SystemConnectionType)
                else SystemConnectionType(connection.connection_type)
            )
        except ValueError as exc:
            raise RuntimeError("System connection type is invalid for profiling.") from exc

        snapshot = _ConnectionSnapshot(
            id=connection_uuid,
            connection_type=connection_type,
            connection_string=connection.connection_string,
        )
        return snapshot, tables

    # ------------------------------------------------------------------
    # Data collection
    # ------------------------------------------------------------------
    def _profile_connection(self, inputs: tuple[_ConnectionSnapshot, list[_TableDefinition]]) -> list[_TableProfile]:
        connection, tables = inputs
        try:
            source_url = resolve_sqlalchemy_url(connection.connection_type, connection.connection_string)
        except UnsupportedConnectionError as exc:
            raise RuntimeError(str(exc)) from exc

        engine = create_engine(source_url, pool_pre_ping=True)
        metadata = MetaData()
        collected: list[_TableProfile] = []
        try:
            for definition in tables:
                profile = self._profile_table(engine, metadata, definition)
                if profile is None:
                    continue
                collected.append(profile)
        finally:
            with suppress(Exception):
                engine.dispose()
        return collected

    def _profile_table(
        self,
        engine: Engine,
        metadata: MetaData,
        definition: _TableDefinition,
    ) -> _TableProfile | None:
        schema_name, table_name = self._normalize_identifiers(definition.schema_name, definition.table_name)
        logger.info("Profiling table %s.%s", schema_name or "default", table_name)

        try:
            table = SqlTable(table_name, metadata, schema=schema_name, autoload_with=engine)
        except SQLAlchemyError as exc:
            logger.warning("Unable to reflect table %s.%s: %s", schema_name, table_name, exc)
            return None

        row_count = self._fetch_row_count(engine, table)
        columns: list[_ColumnProfile] = []
        for ordinal, column in enumerate(table.columns, start=1):
            if self._should_skip_column(column.name):
                continue
            try:
                columns.append(self._profile_column(engine, table, column, row_count, ordinal))
            except SQLAlchemyError as exc:
                logger.warning("Column profile failed for %s.%s.%s: %s", schema_name, table_name, column.name, exc)
                continue

        return _TableProfile(definition=definition, row_count=row_count, columns=columns)

    @staticmethod
    def _fetch_row_count(engine: Engine, table: SqlTable) -> int:
        stmt = select(func.count()).select_from(table)
        with engine.connect() as connection:
            result = connection.execute(stmt)
            value = result.scalar()
        return int(value or 0)

    def _profile_column(
        self,
        engine: Engine,
        table: SqlTable,
        column,
        row_count: int,
        ordinal: int,
    ) -> _ColumnProfile:
        general_type = self._infer_general_type(column.type)
        aggregate_expressions = [
            func.count(column).label("non_null_count"),
            func.count(func.distinct(column)).label("distinct_count"),
        ]

        min_label = None
        max_label = None
        avg_label = None

        if general_type in {"numeric", "datetime", "string"}:
            min_label = "min_value"
            max_label = "max_value"
            aggregate_expressions.append(func.min(column).label(min_label))
            aggregate_expressions.append(func.max(column).label(max_label))

        if general_type == "numeric":
            avg_label = "avg_value"
            aggregate_expressions.append(func.avg(column).label(avg_label))

        stmt = select(*aggregate_expressions).select_from(table)
        with engine.connect() as connection:
            metrics_row = connection.execute(stmt).mappings().one()

        non_null_count = int(metrics_row.get("non_null_count") or 0)
        distinct_count = metrics_row.get("distinct_count")
        distinct_count_int = int(distinct_count) if distinct_count is not None else None
        null_count = max(row_count - non_null_count, 0)

        min_value = metrics_row.get(min_label) if min_label else None
        max_value = metrics_row.get(max_label) if max_label else None
        avg_value = metrics_row.get(avg_label) if avg_label else None

        metrics: dict[str, Any] = {
            "non_null_count": non_null_count,
        }

        if general_type == "numeric":
            metrics["avg_value"] = self._convert_numeric(avg_value)
        string_length_stats: dict[str, Any] | None = None
        if general_type == "string":
            length_stmt = select(
                func.min(func.length(column)).label("min_length"),
                func.max(func.length(column)).label("max_length"),
                func.avg(func.length(column)).label("avg_length"),
            ).select_from(table)
            with engine.connect() as connection:
                length_row = connection.execute(length_stmt).mappings().one()
            string_length_stats = {
                "min_length": self._safe_int(length_row.get("min_length")),
                "max_length": self._safe_int(length_row.get("max_length")),
                "avg_length": self._convert_numeric(length_row.get("avg_length")),
            }
            metrics.update(string_length_stats)

        top_values = self._fetch_top_values(engine, table, column, row_count)
        if general_type == "string":
            text_profile = self._build_text_profile(
                engine,
                table,
                column.name,
                row_count=row_count,
                non_null_count=non_null_count,
                null_count=null_count,
                min_length=string_length_stats.get("min_length") if string_length_stats else None,
                max_length=string_length_stats.get("max_length") if string_length_stats else None,
                avg_length=string_length_stats.get("avg_length") if string_length_stats else None,
                min_text=self._stringify_value(min_value),
                max_text=self._stringify_value(max_value),
                top_values=top_values,
            )
            if text_profile:
                metrics["text_profile"] = text_profile

        return _ColumnProfile(
            column_name=column.name,
            data_type=str(column.type),
            general_type=general_type,
            ordinal_position=ordinal,
            row_count=row_count,
            non_null_count=non_null_count,
            null_count=null_count,
            distinct_count=distinct_count_int,
            min_value=self._stringify_value(min_value),
            max_value=self._stringify_value(max_value),
            avg_value=self._convert_numeric(avg_value),
            metrics={key: value for key, value in metrics.items() if value is not None and value != ""},
            top_values=top_values,
        )

    @staticmethod
    def _fetch_top_values(engine: Engine, table: SqlTable, column, row_count: int) -> list[tuple[str | None, int]]:
        if row_count <= 0:
            return []
        stmt = (
            select(column, func.count().label("frequency"))
            .where(column.is_not(None))
            .group_by(column)
            .order_by(func.count().desc(), column.asc())
            .limit(5)
        )
        with engine.connect() as connection:
            rows = connection.execute(stmt).all()
        return [(LocalProfilingRunner._stringify_value(value), int(freq or 0)) for value, freq in rows]

    def _build_text_profile(
        self,
        engine: Engine,
        table: SqlTable,
        column_name: str,
        *,
        row_count: int,
        non_null_count: int,
        null_count: int,
        min_length: int | None,
        max_length: int | None,
        avg_length: float | None,
        min_text: str | None,
        max_text: str | None,
        top_values: list[tuple[str | None, int]],
    ) -> dict[str, Any] | None:
        if row_count <= 0:
            return None

        metrics_alias = table.alias()
        column = metrics_alias.c[column_name]
        trimmed = func.trim(column)
        length_expr = func.length(column)
        trimmed_length = func.length(trimmed)
        digits_only = func.regexp_replace(trimmed, "[^0-9]", "")
        digits_removed = func.regexp_replace(trimmed, "[0-9]", "")
        letters_only = func.regexp_replace(column, "[^A-Za-z]", "")
        letters_length = func.length(letters_only)
        no_space_value = func.replace(column, " ", "")
        space_difference = func.coalesce(length_expr - func.length(no_space_value), 0)

        def _sum(condition):
            return func.coalesce(func.sum(case((condition, 1), else_=0)), 0)

        blank_condition = and_(column.isnot(None), length_expr == 0)
        whitespace_condition = and_(column.isnot(None), trimmed_length == 0, length_expr > 0)
        numeric_only_condition = and_(column.isnot(None), trimmed_length > 0, digits_only == trimmed)
        zero_condition = and_(column.isnot(None), func.upper(trimmed) == "0")
        first_char = func.substr(trimmed, 1, 1)
        last_char = func.substr(trimmed, func.length(trimmed), 1)
        quoted_condition = and_(
            column.isnot(None),
            trimmed_length >= 2,
            first_char.in_(("'", '"')),
            first_char == last_char,
        )
        leading_space_condition = and_(column.isnot(None), column.like(" %"))
        embedded_space_condition = and_(column.isnot(None), length_expr > func.length(no_space_value))
        dummy_condition = and_(column.isnot(None), func.lower(trimmed).in_(_DUMMY_LITERAL_VALUES))
        includes_digit_condition = and_(
            column.isnot(None),
            trimmed_length > 0,
            func.length(digits_removed) < trimmed_length,
        )
        upper_case_condition = and_(
            column.isnot(None),
            letters_length > 0,
            func.upper(letters_only) == letters_only,
            func.lower(letters_only) != letters_only,
        )
        lower_case_condition = and_(
            column.isnot(None),
            letters_length > 0,
            func.lower(letters_only) == letters_only,
            func.upper(letters_only) != letters_only,
        )
        non_alpha_condition = and_(column.isnot(None), trimmed_length > 0, letters_length == 0)

        metrics_stmt = (
            select(
                _sum(blank_condition).label("blank_count"),
                _sum(whitespace_condition).label("whitespace_count"),
                _sum(numeric_only_condition).label("numeric_only_count"),
                _sum(zero_condition).label("zero_count"),
                _sum(quoted_condition).label("quoted_count"),
                _sum(leading_space_condition).label("leading_space_count"),
                _sum(embedded_space_condition).label("embedded_space_count"),
                _sum(dummy_condition).label("dummy_value_count"),
                _sum(includes_digit_condition).label("includes_digit_count"),
                _sum(upper_case_condition).label("upper_case_count"),
                _sum(lower_case_condition).label("lower_case_count"),
                _sum(non_alpha_condition).label("non_alpha_count"),
                func.coalesce(
                    func.sum(case((column.isnot(None), space_difference), else_=0)),
                    0,
                ).label("space_total"),
            )
            .select_from(metrics_alias)
        )

        duplicate_alias = table.alias()
        duplicate_column = duplicate_alias.c[column_name]
        duplicate_subquery = (
            select((func.count() - 1).label("excess"))
            .select_from(duplicate_alias)
            .where(duplicate_column.isnot(None))
            .group_by(duplicate_column)
            .having(func.count() > 1)
        ).subquery()
        duplicate_stmt = select(func.coalesce(func.sum(duplicate_subquery.c.excess), 0).label("duplicate_rows"))

        histogram_alias = table.alias()
        histogram_column = histogram_alias.c[column_name]
        histogram_stmt = (
            select(func.length(histogram_column).label("length"), func.count().label("frequency"))
            .select_from(histogram_alias)
            .where(histogram_column.isnot(None))
            .group_by(func.length(histogram_column))
            .order_by(func.length(histogram_column).asc())
            .limit(25)
        )

        with engine.connect() as connection:
            stats_row = connection.execute(metrics_stmt).mappings().one()
            duplicate_rows_value = connection.execute(duplicate_stmt).scalar()
            histogram_rows = connection.execute(histogram_stmt).all()

        blank_count = self._safe_int(stats_row.get("blank_count")) or 0
        whitespace_count = self._safe_int(stats_row.get("whitespace_count")) or 0
        numeric_only_count = self._safe_int(stats_row.get("numeric_only_count")) or 0
        zero_count = self._safe_int(stats_row.get("zero_count")) or 0
        quoted_count = self._safe_int(stats_row.get("quoted_count")) or 0
        leading_space_count = self._safe_int(stats_row.get("leading_space_count")) or 0
        embedded_space_count = self._safe_int(stats_row.get("embedded_space_count")) or 0
        dummy_value_count = self._safe_int(stats_row.get("dummy_value_count")) or 0
        includes_digit_count = self._safe_int(stats_row.get("includes_digit_count")) or 0
        upper_case_count = self._safe_int(stats_row.get("upper_case_count")) or 0
        lower_case_count = self._safe_int(stats_row.get("lower_case_count")) or 0
        non_alpha_count = self._safe_int(stats_row.get("non_alpha_count")) or 0
        space_total = self._convert_numeric(stats_row.get("space_total")) or 0.0
        zero_length_count = blank_count + whitespace_count
        missing_count = null_count + zero_length_count + dummy_value_count
        actual_value_count = max(non_null_count - zero_length_count - dummy_value_count, 0)
        duplicate_rows = self._safe_int(duplicate_rows_value) or 0
        unique_rows = max(non_null_count - duplicate_rows, 0)
        mixed_case_count = max(non_null_count - upper_case_count - lower_case_count - non_alpha_count, 0)
        average_embedded_spaces = self._safe_ratio(space_total, non_null_count)

        length_histogram = [
            {
                "label": str(int(row.length)),
                "count": int(row.frequency or 0),
                "lower": int(row.length),
                "upper": int(row.length),
                "percentage": self._safe_ratio(row.frequency, non_null_count),
            }
            for row in histogram_rows
            if row.length is not None
        ]

        pattern_counts: dict[str, int] = {}
        for value, frequency in top_values:
            pattern = _shape_pattern(value)
            if not pattern:
                continue
            pattern_counts[pattern] = pattern_counts.get(pattern, 0) + int(frequency or 0)
        frequent_patterns = [
            {
                "label": label,
                "count": count,
                "percentage": self._safe_ratio(count, non_null_count),
            }
            for label, count in sorted(pattern_counts.items(), key=lambda item: item[1], reverse=True)[:6]
        ]
        distinct_patterns = len(pattern_counts) or None
        standard_pattern_matches = frequent_patterns[0]["count"] if frequent_patterns else None

        text_top_values = [
            {
                "value": value,
                "label": value if value is not None else "(null)",
                "count": frequency,
                "percentage": self._safe_ratio(frequency, non_null_count),
            }
            for value, frequency in top_values
        ]

        missing_breakdown = []
        if null_count:
            missing_breakdown.append(
                {
                    "label": "Null",
                    "count": null_count,
                    "percentage": self._safe_ratio(null_count, row_count),
                }
            )
        if zero_length_count:
            missing_breakdown.append(
                {
                    "label": "Zero length",
                    "count": zero_length_count,
                    "percentage": self._safe_ratio(zero_length_count, row_count),
                }
            )
        if dummy_value_count:
            missing_breakdown.append(
                {
                    "label": "Dummy values",
                    "count": dummy_value_count,
                    "percentage": self._safe_ratio(dummy_value_count, row_count),
                }
            )

        duplicate_breakdown = []
        if duplicate_rows:
            duplicate_breakdown.append(
                {
                    "label": "Duplicate values",
                    "count": duplicate_rows,
                    "percentage": self._safe_ratio(duplicate_rows, non_null_count),
                }
            )
        if unique_rows:
            duplicate_breakdown.append(
                {
                    "label": "Unique values",
                    "count": unique_rows,
                    "percentage": self._safe_ratio(unique_rows, non_null_count),
                }
            )

        case_breakdown = []
        if upper_case_count:
            case_breakdown.append(
                {
                    "label": "Upper Case",
                    "count": upper_case_count,
                    "percentage": self._safe_ratio(upper_case_count, non_null_count),
                }
            )
        if lower_case_count:
            case_breakdown.append(
                {
                    "label": "Lower Case",
                    "count": lower_case_count,
                    "percentage": self._safe_ratio(lower_case_count, non_null_count),
                }
            )
        if mixed_case_count:
            case_breakdown.append(
                {
                    "label": "Mixed Case",
                    "count": mixed_case_count,
                    "percentage": self._safe_ratio(mixed_case_count, non_null_count),
                }
            )
        if non_alpha_count:
            case_breakdown.append(
                {
                    "label": "Non Alpha",
                    "count": non_alpha_count,
                    "percentage": self._safe_ratio(non_alpha_count, non_null_count),
                }
            )

        stats_payload = {
            "record_count": row_count,
            "value_count": non_null_count,
            "actual_value_count": actual_value_count,
            "null_value_count": null_count,
            "zero_length_count": zero_length_count,
            "dummy_value_count": dummy_value_count,
            "missing_count": missing_count,
            "missing_percentage": self._safe_ratio(missing_count, row_count),
            "duplicate_count": duplicate_rows,
            "duplicate_percentage": self._safe_ratio(duplicate_rows, non_null_count),
            "zero_count": zero_count,
            "numeric_only_count": numeric_only_count,
            "includes_digit_count": includes_digit_count,
            "quoted_count": quoted_count,
            "leading_space_count": leading_space_count,
            "embedded_space_count": embedded_space_count,
            "date_value_count": None,
            "average_embedded_spaces": average_embedded_spaces,
            "min_length": min_length,
            "max_length": max_length,
            "avg_length": avg_length,
            "min_text": min_text,
            "max_text": max_text,
            "distinct_patterns": distinct_patterns,
            "standard_pattern_matches": standard_pattern_matches,
        }

        return {
            "stats": stats_payload,
            "missing_breakdown": missing_breakdown,
            "duplicate_breakdown": duplicate_breakdown,
            "case_breakdown": case_breakdown,
            "frequent_patterns": frequent_patterns,
            "length_histogram": length_histogram,
            "top_values": text_top_values,
        }

    # ------------------------------------------------------------------
    # Persistence
    # ------------------------------------------------------------------
    def _persist_profile_rows(
        self,
        profile_run_id: str,
        table_group_id: str,
        tables: Iterable[_TableProfile],
    ) -> None:
        now = datetime.now(timezone.utc)
        with SessionLocal() as session:
            self._clear_profile_run_artifacts(session, profile_run_id)

            for table_profile in tables:
                definition = table_profile.definition
                schema_name, table_name = self._normalize_identifiers(
                    definition.schema_name,
                    definition.table_name,
                )
                column_count = len(table_profile.columns)

                table_characteristic = session.get(
                    DataQualityDataTableCharacteristic,
                    definition.table_id,
                )
                if table_characteristic is None:
                    table_characteristic = DataQualityDataTableCharacteristic(
                        table_id=definition.table_id,
                        table_group_id=table_group_id,
                        schema_name=schema_name,
                        table_name=table_name,
                    )
                table_characteristic.record_count = table_profile.row_count
                table_characteristic.column_count = column_count
                table_characteristic.last_complete_profile_run_id = profile_run_id
                table_characteristic.latest_run_completed_at = now
                table_characteristic.latest_anomaly_ct = 0
                session.add(table_characteristic)

                for column_profile in table_profile.columns:
                    qualified_name = self._build_qualified_name(schema_name, table_name, column_profile.column_name)
                    column_row = DataQualityProfileColumn(
                        profile_run_id=profile_run_id,
                        schema_name=schema_name,
                        table_name=table_name,
                        column_name=column_profile.column_name,
                        qualified_name=qualified_name,
                        data_type=column_profile.data_type,
                        general_type=column_profile.general_type,
                        ordinal_position=column_profile.ordinal_position,
                        row_count=column_profile.row_count,
                        null_count=column_profile.null_count,
                        non_null_count=column_profile.non_null_count,
                        distinct_count=column_profile.distinct_count,
                        min_value=column_profile.min_value,
                        max_value=column_profile.max_value,
                        avg_value=column_profile.avg_value,
                        metrics_json=self._serialize_json(column_profile.metrics),
                        generated_at=now,
                    )
                    session.add(column_row)

                    result_row = DataQualityProfileResult(
                        result_id=uuid.uuid4().hex,
                        profile_run_id=profile_run_id,
                        table_id=definition.table_id,
                        column_id=None,
                        schema_name=schema_name,
                        table_name=table_name,
                        column_name=column_profile.column_name,
                        data_type=column_profile.data_type,
                        general_type=column_profile.general_type,
                        record_count=column_profile.row_count,
                        null_count=column_profile.null_count,
                        distinct_count=column_profile.distinct_count,
                        min_value=column_profile.min_value,
                        max_value=column_profile.max_value,
                        avg_value=column_profile.avg_value,
                        stddev_value=None,
                        percentiles_json=None,
                        top_values_json=self._serialize_json(
                            [
                                {"value": value, "frequency": freq}
                                for value, freq in column_profile.top_values
                            ]
                        ),
                        metrics_json=self._serialize_json(column_profile.metrics),
                        generated_at=now,
                    )
                    session.add(result_row)

                    if column_profile.top_values:
                        for rank, (value, frequency) in enumerate(column_profile.top_values, start=1):
                            session.add(
                                DataQualityProfileColumnValue(
                                    profile_run_id=profile_run_id,
                                    schema_name=schema_name,
                                    table_name=table_name,
                                    column_name=column_profile.column_name,
                                    value=value,
                                    value_hash=self._hash_value(value),
                                    frequency=frequency,
                                    relative_freq=(frequency / column_profile.row_count)
                                    if column_profile.row_count
                                    else None,
                                    rank=rank,
                                    generated_at=now,
                                )
                            )

            session.commit()

    @staticmethod
    def _clear_profile_run_artifacts(session, profile_run_id: str) -> None:
        for model in (DataQualityProfileColumnValue, DataQualityProfileColumn, DataQualityProfileResult):
            session.query(model).filter(model.profile_run_id == profile_run_id).delete(synchronize_session=False)

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------
    @staticmethod
    def _normalize_identifiers(schema_name: str | None, table_name: str) -> tuple[str | None, str]:
        schema = (schema_name or "").strip()
        name = (table_name or "").strip()
        if not schema and "." in name:
            schema, _, suffix = name.rpartition(".")
            schema = schema or ""
            name = suffix
        return (schema or None), name

    @staticmethod
    def _should_skip_column(column_name: str) -> bool:
        normalized = (column_name or "").strip().lower()
        return normalized.startswith("__cc_")

    @staticmethod
    def _infer_general_type(column_type) -> str:
        numeric_types = (
            satypes.Integer,
            satypes.BigInteger,
            satypes.SmallInteger,
            satypes.Numeric,
            satypes.Float,
            satypes.DECIMAL,
        )
        string_types = (satypes.String, satypes.Text, satypes.Unicode, satypes.UnicodeText)
        datetime_types = (satypes.DateTime, satypes.Date, satypes.Time)
        boolean_types = (satypes.Boolean,)

        if isinstance(column_type, numeric_types):
            return "numeric"
        if isinstance(column_type, string_types):
            return "string"
        if isinstance(column_type, datetime_types):
            return "datetime"
        if isinstance(column_type, boolean_types):
            return "boolean"
        return "string"

    @staticmethod
    def _convert_numeric(value: Any) -> float | None:
        try:
            return float(value)
        except (TypeError, ValueError):
            return None

    @staticmethod
    def _safe_int(value: Any) -> int | None:
        try:
            return int(value)
        except (TypeError, ValueError):
            return None

    @staticmethod
    def _safe_ratio(numerator: Any, denominator: Any) -> float | None:
        try:
            num = float(numerator)
            den = float(denominator)
        except (TypeError, ValueError):
            return None
        if den == 0:
            return None
        return num / den

    @staticmethod
    def _stringify_value(value: Any) -> str | None:
        if value is None:
            return None
        if isinstance(value, datetime):
            return value.astimezone(timezone.utc).isoformat()
        return str(value)

    @staticmethod
    def _build_qualified_name(schema: str | None, table: str, column: str) -> str:
        parts = [part for part in (schema, table, column) if part]
        return ".".join(parts)

    @staticmethod
    def _serialize_json(payload: dict[str, Any] | list[dict[str, Any]] | None) -> str | None:
        if not payload:
            return None
        return json.dumps(payload, ensure_ascii=False)

    @staticmethod
    def _hash_value(value: str | None) -> str | None:
        if value is None:
            return None
        return uuid.uuid5(uuid.NAMESPACE_URL, value).hex


local_profiling_runner = LocalProfilingRunner()
