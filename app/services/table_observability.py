from __future__ import annotations

import logging
from collections import defaultdict
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal, ROUND_HALF_UP
from typing import Any, Callable, Generator, Mapping, Sequence
from uuid import UUID

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from sqlalchemy.orm import Session, joinedload
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from app.database import SessionLocal
from app.models import (
    ConnectionTableSelection,
    DataQualityDataColumnCharacteristic,
    DataQualityDataTableCharacteristic,
    SystemConnection,
    TableObservabilityCategorySchedule,
    TableObservabilityMetric as TableObservabilityMetricModel,
    TableObservabilityRun,
)
from app.schemas import (
    TableObservabilityCategory,
    TableObservabilityMetric as TableObservabilityMetricSchema,
    TableObservabilityPlan,
    TableObservabilityTable,
)

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class _MetricDefinition:
    key: str
    name: str
    description_template: str
    source: str | None = None
    unit: str | None = None


@dataclass(frozen=True)
class _CategoryDefinition:
    key: str
    name: str
    cadence: str
    rationale: str
    metrics: Sequence[_MetricDefinition]
    default_cron_expression: str = "0 2 * * *"
    default_timezone: str = "UTC"


_OBSERVABILITY_CATEGORY_DEFINITIONS: tuple[_CategoryDefinition, ...] = (
    _CategoryDefinition(
        key="structural",
        name="Structural Metrics",
        cadence="Daily or weekly",
        rationale=(
            "Structural attributes do not change minute-to-minute, but trending row/column counts and storage"
            " footprint highlights sudden drops, runaway growth, or partition regressions before they cascade upstream."
        ),
        metrics=(
            _MetricDefinition(
                key="row_count",
                name="Row count trend",
                description_template=(
                    "Compare record_count for {table_label} run-over-run and alert when it deviates more than +/-5%."
                ),
                source="dq_profile_results.record_count",
            ),
            _MetricDefinition(
                key="column_count",
                name="Column count drift",
                description_template=(
                    "Track column inventory for {table_label}; notify when new columns arrive or existing ones disappear."
                ),
                source="dq_profile_columns.column_name",
            ),
            _MetricDefinition(
                key="storage_size",
                name="Storage footprint",
                description_template=(
                    "Capture storage_bytes for {table_label} via DESCRIBE DETAIL / information_schema to surface runaway growth."
                ),
                source="information_schema.tables.data_length",
            ),
            _MetricDefinition(
                key="partition_health",
                name="Partition health",
                description_template=(
                    "Verify latest partitions for {table_label} arrive on schedule and contain balanced row counts."
                ),
                source="SHOW PARTITIONS / delta.log",
            ),
        ),
    ),
    _CategoryDefinition(
        key="schema_evolution",
        name="Schema & Evolution Metrics",
        cadence="Per load or schema change event",
        rationale=(
            "Schema drift can break pipelines immediately. Validating table contracts during ingestion keeps dbt/test suites stable."
        ),
        metrics=(
            _MetricDefinition(
                key="schema_drift",
                name="Schema drift",
                description_template=(
                    "Diff catalog metadata against ConversionCentral definitions for {table_label} every load to block surprises."
                ),
                source="connection_table_selections vs data_definition_tables",
            ),
            _MetricDefinition(
                key="constraints",
                name="Constraint integrity",
                description_template=(
                    "Re-evaluate primary/foreign key expectations captured for {table_label} and surface violations immediately."
                ),
                source="TestGen constraint tests",
            ),
            _MetricDefinition(
                key="type_consistency",
                name="Data type consistency",
                description_template=(
                    "Ensure data types emitted by the source for {table_label} still match dbt model + analytics expectations."
                ),
                source="dq_profile_columns.data_type",
            ),
        ),
    ),
    _CategoryDefinition(
        key="operational",
        name="Operational Metrics",
        cadence="Near real-time / per ingestion cycle",
        rationale=(
            "Operational SLOs tie directly to incident response. Detecting latency spikes or concurrency limits protects downstream SLAs."
        ),
        metrics=(
            _MetricDefinition(
                key="refresh_frequency",
                name="Refresh frequency",
                description_template=(
                    "Measure the actual arrival cadence for {table_label} against expected schedules; alert on missed windows."
                ),
                source="ingestion_loader.last_run_at",
            ),
            _MetricDefinition(
                key="load_duration",
                name="Load duration",
                description_template=(
                    "Record ingestion duration for {table_label} and flag 2x regressions which indicate upstream contention."
                ),
                source="dq_profile_operations.duration_ms",
            ),
            _MetricDefinition(
                key="concurrency",
                name="Concurrency & resource pressure",
                description_template=(
                    "Monitor Databricks/job concurrency while {table_label} loads to avoid throttling errors."
                ),
                source="databricks_jobs.run_state",
            ),
            _MetricDefinition(
                key="access_patterns",
                name="Access patterns",
                description_template=(
                    "Capture query counts + consumers hitting {table_label} to understand impact radius of incidents."
                ),
                source="warehouse_query_history",
            ),
        ),
    ),
    _CategoryDefinition(
        key="governance",
        name="Governance & Compliance Metrics",
        cadence="Weekly or monthly",
        rationale=(
            "Governance metadata evolves slowly, so scheduled reviews keep ownership, lineage, and PII coverage evergreen without noise."
        ),
        metrics=(
            _MetricDefinition(
                key="lineage",
                name="Lineage completeness",
                description_template=(
                    "Verify {table_label} has upstream/downstream lineage captured so impact analysis stays trustworthy."
                ),
                source="dbt manifest exposures",
            ),
            _MetricDefinition(
                key="pii_flags",
                name="PII flags",
                description_template=(
                    "Reconcile TestGen column tags + security catalog to ensure {table_label} PII classifications stay current."
                ),
                source="dq_data_column_chars.pii_risk",
            ),
            _MetricDefinition(
                key="audit_logs",
                name="Audit log coverage",
                description_template=(
                    "Confirm access/audit logs for {table_label} remain enabled and retained per policy."
                ),
                source="cloud_audit_log configs",
            ),
            _MetricDefinition(
                key="ownership",
                name="Ownership & stewardship",
                description_template=(
                    "Validate {table_label} retains assigned data owner + steward contacts for escalation."
                ),
                source="process_area_role_assignments",
            ),
        ),
    ),
)

_OBSERVABILITY_CATEGORY_INDEX: dict[str, _CategoryDefinition] = {
    definition.key: definition for definition in _OBSERVABILITY_CATEGORY_DEFINITIONS
}

TABLE_OBSERVABILITY_CATEGORY_KEYS: tuple[str, ...] = tuple(_OBSERVABILITY_CATEGORY_INDEX.keys())


def get_observability_category_definition(category_key: str) -> _CategoryDefinition:
    try:
        return _OBSERVABILITY_CATEGORY_INDEX[category_key]
    except KeyError as exc:  # pragma: no cover - defensive, validated by callers
        raise ValueError(f"Unknown table observability category '{category_key}'") from exc


def build_table_observability_plan(connection: SystemConnection) -> TableObservabilityPlan:
    """Construct an observability plan for each catalog-selected table."""

    selections: Sequence[ConnectionTableSelection] = sorted(
        getattr(connection, "catalog_selections", []) or [],
        key=lambda selection: (
            (selection.schema_name or "").lower(),
            (selection.table_name or "").lower(),
        ),
    )

    tables: list[TableObservabilityTable] = []
    for selection in selections:
        if not selection.table_name:
            continue
        categories = _build_categories_for_selection(selection)
        tables.append(
            TableObservabilityTable(
                selection_id=selection.id,
                schema_name=selection.schema_name or "<default>",
                table_name=selection.table_name,
                table_type=selection.table_type,
                column_count=selection.column_count,
                estimated_rows=_safe_int(selection.estimated_rows),
                categories=categories,
            )
        )

    return TableObservabilityPlan(
        system_connection_id=connection.id,
        connection_name=getattr(connection, "display_name", "Connection"),
        table_count=len(tables),
        generated_at=datetime.now(timezone.utc),
        tables=tables,
    )


def _build_categories_for_selection(selection: ConnectionTableSelection) -> list[TableObservabilityCategory]:
    categories: list[TableObservabilityCategory] = []
    for definition in _OBSERVABILITY_CATEGORY_DEFINITIONS:
        metrics = [
            TableObservabilityMetricSchema(
                name=metric.name,
                description=_format_description(metric, selection),
                baseline=_metric_baseline(metric.key, selection),
                source=metric.source,
            )
            for metric in definition.metrics
        ]
        categories.append(
            TableObservabilityCategory(
                name=definition.name,
                cadence=definition.cadence,
                rationale=definition.rationale,
                metrics=metrics,
            )
        )
    return categories


def _format_description(metric: _MetricDefinition, selection: ConnectionTableSelection) -> str:
    label = _table_label(selection)
    with_label = metric.description_template.format(table_label=label)
    return with_label


def _metric_baseline(metric_key: str, selection: ConnectionTableSelection) -> str | None:
    if metric_key == "row_count" and selection.estimated_rows is not None:
        return f"~{_format_number(selection.estimated_rows)} rows discovered during catalog scan"
    if metric_key == "column_count" and selection.column_count is not None:
        count = selection.column_count
        plural = "column" if count == 1 else "columns"
        return f"{count} {plural} detected during last catalog sync"
    return None


def _table_label(selection: ConnectionTableSelection) -> str:
    if selection.schema_name:
        return f"{selection.schema_name}.{selection.table_name}"
    return selection.table_name or "<unknown>"


def _format_number(value: int) -> str:
    return f"{int(value):,}"


def _safe_int(value: Any | None) -> int | None:
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _normalize_identifier(value: str | None) -> str:
    return (value or "").strip().lower()


def _table_key(schema: str | None, table: str | None) -> tuple[str, str]:
    return (_normalize_identifier(schema), _normalize_identifier(table))


def _decimal_from(value: int | float | Decimal | None) -> Decimal | None:
    if value is None:
        return None
    if isinstance(value, Decimal):
        return value
    try:
        return Decimal(str(value))
    except Exception:  # pragma: no cover - defensive conversion guard
        return None


def _max_datetime(first: datetime | None, second: datetime | None) -> datetime | None:
    if first is None:
        return second
    if second is None:
        return first
    return first if first >= second else second


METRIC_STATUS_OK = "ok"
METRIC_STATUS_WARNING = "warning"
METRIC_STATUS_MISSING = "missing_data"
METRIC_STATUS_NOT_CONFIGURED = "not_configured"
METRIC_STATUS_NOT_APPLICABLE = "not_applicable"
METRIC_STATUS_BASELINE = "baseline"

RUN_STATUS_PENDING = "pending"
RUN_STATUS_RUNNING = "running"
RUN_STATUS_SUCCEEDED = "succeeded"
RUN_STATUS_FAILED = "failed"


@dataclass(frozen=True)
class _SelectionContext:
    selection: ConnectionTableSelection
    connection: SystemConnection


@dataclass
class _ColumnStats:
    total_columns: int = 0
    typed_columns: int = 0
    pii_columns: int = 0
    last_observed_at: datetime | None = None


@dataclass(frozen=True)
class _ObservedMetric:
    selection_id: UUID | None
    system_connection_id: UUID | None
    schema_name: str | None
    table_name: str
    metric_category: str
    metric_name: str
    metric_unit: str | None
    metric_value_number: Decimal | None
    metric_value_text: str | None
    metric_payload: Mapping[str, object] | None
    metric_status: str | None
    recorded_at: datetime


class TableObservabilityMetricCollector:
    """Generate persisted observability metrics for catalog-selected tables."""

    def __init__(self, session: Session) -> None:
        self._session = session
        self._now = _utcnow()
        self._selection_contexts = self._load_selection_contexts()
        self._table_characteristics = self._load_table_characteristics()
        self._column_stats = self._load_column_characteristics()
        self._metric_handlers: dict[str, Callable[[
            _SelectionContext,
            str,
            _MetricDefinition,
        ], _ObservedMetric]] = {
            "row_count": self._metric_row_count,
            "column_count": self._metric_column_count,
            "storage_size": self._metric_storage_size,
            "partition_health": self._metric_not_applicable,
            "schema_drift": self._metric_schema_drift,
            "constraints": self._metric_not_configured,
            "type_consistency": self._metric_type_consistency,
            "refresh_frequency": self._metric_refresh_frequency,
            "load_duration": self._metric_missing_data,
            "concurrency": self._metric_not_configured,
            "access_patterns": self._metric_not_configured,
            "lineage": self._metric_not_configured,
            "pii_flags": self._metric_pii_flags,
            "audit_logs": self._metric_not_configured,
            "ownership": self._metric_not_configured,
        }

    def collect_for_category(self, category_key: str) -> tuple[list[_ObservedMetric], int]:
        definition = get_observability_category_definition(category_key)
        metrics: list[_ObservedMetric] = []
        for context in self._selection_contexts:
            for metric_definition in definition.metrics:
                handler = self._metric_handlers.get(metric_definition.key, self._metric_not_configured)
                metrics.append(handler(context, category_key, metric_definition))
        return metrics, len(self._selection_contexts)

    def _load_selection_contexts(self) -> list[_SelectionContext]:
        selections = (
            self._session.query(ConnectionTableSelection)
            .options(joinedload(ConnectionTableSelection.system_connection))
            .join(SystemConnection, ConnectionTableSelection.system_connection_id == SystemConnection.id)
            .filter(SystemConnection.active.is_(True))
            .order_by(
                SystemConnection.display_name.asc(),
                ConnectionTableSelection.schema_name.asc(),
                ConnectionTableSelection.table_name.asc(),
            )
            .all()
        )
        contexts: list[_SelectionContext] = []
        for selection in selections:
            connection = selection.system_connection
            if connection is None:
                continue
            contexts.append(_SelectionContext(selection=selection, connection=connection))
        return contexts

    def _load_table_characteristics(self) -> dict[tuple[str, str], DataQualityDataTableCharacteristic]:
        records = (
            self._session.query(DataQualityDataTableCharacteristic)
            .filter(DataQualityDataTableCharacteristic.table_name.is_not(None))
            .all()
        )
        indexed: dict[tuple[str, str], DataQualityDataTableCharacteristic] = {}
        for record in records:
            key = _table_key(record.schema_name, record.table_name)
            indexed[key] = record
        return indexed

    def _load_column_characteristics(self) -> dict[tuple[str, str], _ColumnStats]:
        rows = (
            self._session.query(DataQualityDataColumnCharacteristic)
            .filter(DataQualityDataColumnCharacteristic.table_name.is_not(None))
            .all()
        )
        stats: dict[tuple[str, str], _ColumnStats] = defaultdict(_ColumnStats)
        for row in rows:
            key = _table_key(row.schema_name, row.table_name)
            entry = stats[key]
            entry.total_columns += 1
            if row.data_type:
                entry.typed_columns += 1
            if row.pii_risk and row.pii_risk.lower() not in {"none", "", "low"}:
                entry.pii_columns += 1
            observed_at = row.updated_at or row.created_at
            entry.last_observed_at = _max_datetime(entry.last_observed_at, observed_at)
        return stats

    def _metric_row_count(
        self,
        context: _SelectionContext,
        category_key: str,
        definition: _MetricDefinition,
    ) -> _ObservedMetric:
        stats = self._table_characteristics.get(_table_key(context.selection.schema_name, context.selection.table_name))
        payload: dict[str, object] = {}
        recorded_at = self._now
        value_number: Decimal | None = None
        value_text: str | None = None
        status = METRIC_STATUS_MISSING

        if stats and stats.record_count is not None:
            value_number = _decimal_from(stats.record_count)
            recorded_at = stats.latest_run_completed_at or stats.updated_at or stats.created_at or self._now
            value_text = f"{stats.record_count:,} rows" if stats.record_count is not None else None
            payload["source"] = "dq_data_table_chars"
            payload["profile_run_id"] = stats.last_complete_profile_run_id
            status = METRIC_STATUS_OK
        elif context.selection.estimated_rows is not None:
            value_number = _decimal_from(context.selection.estimated_rows)
            recorded_at = context.selection.updated_at or context.selection.created_at or self._now
            value_text = f"~{context.selection.estimated_rows:,} rows (catalog scan)"
            payload["source"] = "connection_table_selections"
            status = METRIC_STATUS_BASELINE
        else:
            value_text = "No profiling data available yet"

        return self._build_metric(
            context,
            category_key,
            definition,
            metric_unit=definition.unit or "rows",
            value_number=value_number,
            value_text=value_text,
            payload=payload,
            status=status,
            recorded_at=recorded_at,
        )

    def _metric_column_count(
        self,
        context: _SelectionContext,
        category_key: str,
        definition: _MetricDefinition,
    ) -> _ObservedMetric:
        stats = self._table_characteristics.get(_table_key(context.selection.schema_name, context.selection.table_name))
        payload: dict[str, object] = {}
        status = METRIC_STATUS_MISSING
        value_number: Decimal | None = None
        value_text: str | None = None
        recorded_at = self._now

        column_count = stats.column_count if stats and stats.column_count is not None else context.selection.column_count
        if column_count is not None:
            value_number = _decimal_from(column_count)
            value_text = f"{column_count} columns"
            recorded_at = (
                (stats.updated_at if stats else None)
                or (stats.created_at if stats else None)
                or context.selection.updated_at
                or context.selection.created_at
                or self._now
            )
            payload["source"] = "dq_data_table_chars" if stats and stats.column_count is not None else "connection_table_selections"
            status = METRIC_STATUS_OK
        else:
            value_text = "Column inventory not captured yet"

        return self._build_metric(
            context,
            category_key,
            definition,
            metric_unit=definition.unit or "columns",
            value_number=value_number,
            value_text=value_text,
            payload=payload,
            status=status,
            recorded_at=recorded_at,
        )

    def _metric_storage_size(
        self,
        context: _SelectionContext,
        category_key: str,
        definition: _MetricDefinition,
    ) -> _ObservedMetric:
        stats = self._table_characteristics.get(_table_key(context.selection.schema_name, context.selection.table_name))
        recorded_at = (
            (stats.updated_at if stats else None)
            or (stats.created_at if stats else None)
            or self._now
        )
        payload: dict[str, object] = {"method": "estimated"}
        est_bytes: int | None = None

        if stats and stats.data_point_count:
            est_bytes = stats.data_point_count * 8
        elif stats and stats.record_count and stats.column_count:
            est_bytes = stats.record_count * stats.column_count * 8
        elif context.selection.estimated_rows and context.selection.column_count:
            est_bytes = context.selection.estimated_rows * context.selection.column_count * 8

        if est_bytes and est_bytes > 0:
            value_mb = Decimal(est_bytes) / Decimal(1024 * 1024)
            value_mb = value_mb.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
            value_text = f"~{value_mb} MB"
            status = METRIC_STATUS_OK
        else:
            value_mb = None
            value_text = "Storage footprint unavailable"
            status = METRIC_STATUS_MISSING

        return self._build_metric(
            context,
            category_key,
            definition,
            metric_unit=definition.unit or "MB",
            value_number=value_mb,
            value_text=value_text,
            payload=payload,
            status=status,
            recorded_at=recorded_at,
        )

    def _metric_not_applicable(
        self,
        context: _SelectionContext,
        category_key: str,
        definition: _MetricDefinition,
    ) -> _ObservedMetric:
        return self._build_metric(
            context,
            category_key,
            definition,
            value_text="Metric not applicable for current platform",
            payload={"note": "Waiting on partition metadata"},
            status=METRIC_STATUS_NOT_APPLICABLE,
        )

    def _metric_schema_drift(
        self,
        context: _SelectionContext,
        category_key: str,
        definition: _MetricDefinition,
    ) -> _ObservedMetric:
        stats = self._table_characteristics.get(_table_key(context.selection.schema_name, context.selection.table_name))
        recorded_at = (
            (stats.updated_at if stats else None)
            or (stats.created_at if stats else None)
            or context.selection.updated_at
            or context.selection.created_at
            or self._now
        )
        selection_columns = context.selection.column_count
        observed_columns = stats.column_count if stats and stats.column_count is not None else None

        if selection_columns is not None and observed_columns is not None:
            delta = observed_columns - selection_columns
            payload = {
                "catalog_columns": selection_columns,
                "observed_columns": observed_columns,
            }
            status = METRIC_STATUS_OK if delta == 0 else METRIC_STATUS_WARNING
            value_text = f"{delta:+d} column delta"
            value_number = _decimal_from(delta)
        else:
            payload = {"catalog_columns": selection_columns, "observed_columns": observed_columns}
            status = METRIC_STATUS_MISSING
            value_number = None
            value_text = "Unable to compare column inventory"

        return self._build_metric(
            context,
            category_key,
            definition,
            metric_unit=definition.unit,
            value_number=value_number,
            value_text=value_text,
            payload=payload,
            status=status,
            recorded_at=recorded_at,
        )

    def _metric_not_configured(
        self,
        context: _SelectionContext,
        category_key: str,
        definition: _MetricDefinition,
    ) -> _ObservedMetric:
        return self._build_metric(
            context,
            category_key,
            definition,
            value_text="Metric configuration not implemented",
            payload={"note": "Reserved for future release"},
            status=METRIC_STATUS_NOT_CONFIGURED,
        )

    def _metric_type_consistency(
        self,
        context: _SelectionContext,
        category_key: str,
        definition: _MetricDefinition,
    ) -> _ObservedMetric:
        stats = self._column_stats.get(_table_key(context.selection.schema_name, context.selection.table_name))
        if stats and stats.total_columns > 0:
            ratio = stats.typed_columns / stats.total_columns
            value_number = Decimal(str(ratio)).quantize(Decimal("0.0001"), rounding=ROUND_HALF_UP)
            value_text = f"{ratio:.0%} of columns typed"
            status = METRIC_STATUS_OK if ratio >= 0.95 else METRIC_STATUS_WARNING
            payload = {
                "total_columns": stats.total_columns,
                "typed_columns": stats.typed_columns,
            }
            recorded_at = stats.last_observed_at or self._now
        else:
            value_number = None
            value_text = "No column profiling data"
            status = METRIC_STATUS_MISSING
            payload = None
            recorded_at = self._now

        return self._build_metric(
            context,
            category_key,
            definition,
            metric_unit=definition.unit or "ratio",
            value_number=value_number,
            value_text=value_text,
            payload=payload,
            status=status,
            recorded_at=recorded_at,
        )

    def _metric_refresh_frequency(
        self,
        context: _SelectionContext,
        category_key: str,
        definition: _MetricDefinition,
    ) -> _ObservedMetric:
        stats = self._table_characteristics.get(_table_key(context.selection.schema_name, context.selection.table_name))
        last_refresh = None
        if stats and stats.latest_run_completed_at:
            last_refresh = stats.latest_run_completed_at
        elif context.selection.last_seen_at:
            last_refresh = context.selection.last_seen_at

        if last_refresh:
            delta = self._now - last_refresh
            hours = max(delta.total_seconds(), 0) / 3600
            value_number = Decimal(str(hours)).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
            value_text = f"{hours:.1f} hours since last refresh"
            status = METRIC_STATUS_OK if hours <= 24 else METRIC_STATUS_WARNING
            payload = {"last_refresh": last_refresh.isoformat()}
            recorded_at = self._now
        else:
            value_number = None
            value_text = "Refresh timestamp unavailable"
            status = METRIC_STATUS_MISSING
            payload = None
            recorded_at = self._now

        return self._build_metric(
            context,
            category_key,
            definition,
            metric_unit=definition.unit or "hours",
            value_number=value_number,
            value_text=value_text,
            payload=payload,
            status=status,
            recorded_at=recorded_at,
        )

    def _metric_missing_data(
        self,
        context: _SelectionContext,
        category_key: str,
        definition: _MetricDefinition,
    ) -> _ObservedMetric:
        return self._build_metric(
            context,
            category_key,
            definition,
            value_text="Metric data not captured",
            payload={"note": "Awaiting loader instrumentation"},
            status=METRIC_STATUS_MISSING,
        )

    def _metric_pii_flags(
        self,
        context: _SelectionContext,
        category_key: str,
        definition: _MetricDefinition,
    ) -> _ObservedMetric:
        stats = self._column_stats.get(_table_key(context.selection.schema_name, context.selection.table_name))
        if stats and stats.total_columns > 0:
            value_number = _decimal_from(stats.pii_columns)
            value_text = f"{stats.pii_columns} columns tagged"
            payload = {
                "total_columns": stats.total_columns,
                "pii_columns": stats.pii_columns,
            }
            status = METRIC_STATUS_OK if stats.pii_columns > 0 else METRIC_STATUS_WARNING
            recorded_at = stats.last_observed_at or self._now
        else:
            value_number = None
            value_text = "No column tags available"
            payload = None
            status = METRIC_STATUS_MISSING
            recorded_at = self._now

        return self._build_metric(
            context,
            category_key,
            definition,
            metric_unit=definition.unit or "columns",
            value_number=value_number,
            value_text=value_text,
            payload=payload,
            status=status,
            recorded_at=recorded_at,
        )

    def _build_metric(
        self,
        context: _SelectionContext,
        category_key: str,
        definition: _MetricDefinition,
        *,
        metric_unit: str | None = None,
        value_number: Decimal | None = None,
        value_text: str | None = None,
        payload: Mapping[str, object] | None = None,
        status: str | None = None,
        recorded_at: datetime | None = None,
    ) -> _ObservedMetric:
        combined_payload: dict[str, object] = {
            "display_name": definition.name,
            "description": definition.description_template,
        }
        if definition.source:
            combined_payload["source_reference"] = definition.source
        if payload:
            combined_payload.update(payload)

        return _ObservedMetric(
            selection_id=context.selection.id,
            system_connection_id=context.connection.id,
            schema_name=context.selection.schema_name,
            table_name=context.selection.table_name,
            metric_category=category_key,
            metric_name=definition.key,
            metric_unit=metric_unit,
            metric_value_number=value_number,
            metric_value_text=value_text,
            metric_payload=combined_payload,
            metric_status=status,
            recorded_at=recorded_at or self._now,
        )


class TableObservabilityScheduleService:
    """Manage global per-category observability schedules."""

    def __init__(self, session_factory: Callable[[], Session] = SessionLocal) -> None:
        self._session_factory = session_factory

    def ensure_category_schedules(self, session: Session | None = None) -> list[TableObservabilityCategorySchedule]:
        managed = False
        if session is None:
            session = self._session_factory()
            managed = True
        try:
            existing = {
                schedule.category_name: schedule
                for schedule in session.query(TableObservabilityCategorySchedule).all()
            }
            for definition in _OBSERVABILITY_CATEGORY_DEFINITIONS:
                if definition.key in existing:
                    continue
                schedule = TableObservabilityCategorySchedule(
                    category_name=definition.key,
                    cron_expression=definition.default_cron_expression,
                    timezone=definition.default_timezone,
                    is_active=True,
                )
                session.add(schedule)
            session.flush()
            if managed:
                session.commit()
            return session.query(TableObservabilityCategorySchedule).all()
        finally:
            if managed:
                session.close()


class TableObservabilityRunService:
    """Execute observability runs and persist metrics."""

    def __init__(self, session_factory: Callable[[], Session] = SessionLocal) -> None:
        self._session_factory = session_factory

    def run_for_category(self, category_key: str, *, session: Session | None = None) -> TableObservabilityRun:
        return self._execute(
            category_key,
            session=session,
            manage_session=session is None,
        )

    def run_for_schedule(self, schedule_id: UUID, *, session: Session | None = None) -> TableObservabilityRun | None:
        managed = False
        if session is None:
            session = self._session_factory()
            managed = True
        try:
            schedule = session.get(TableObservabilityCategorySchedule, schedule_id)
            if schedule is None:
                logger.info("Observability schedule %s no longer exists", schedule_id)
                if managed:
                    session.close()
                return None
            if not schedule.is_active:
                logger.info("Observability schedule %s is inactive; skipping", schedule_id)
                if managed:
                    session.close()
                return None
            run = self._execute(
                schedule.category_name,
                schedule=schedule,
                session=session,
                manage_session=False,
            )
            if managed:
                session.commit()
                session.close()
            return run
        except Exception:
            if managed:
                session.rollback()
                session.close()
            raise

    def _execute(
        self,
        category_key: str,
        *,
        schedule: TableObservabilityCategorySchedule | None = None,
        session: Session | None = None,
        manage_session: bool = True,
    ) -> TableObservabilityRun:
        managed = manage_session
        if session is None:
            session = self._session_factory()
            managed = True

        run: TableObservabilityRun | None = None
        try:
            get_observability_category_definition(category_key)
            run = TableObservabilityRun(
                schedule_id=schedule.id if schedule else None,
                category_name=category_key,
                status=RUN_STATUS_RUNNING,
                started_at=_utcnow(),
            )
            session.add(run)
            if schedule:
                schedule.last_run_started_at = run.started_at
                schedule.last_run_status = RUN_STATUS_RUNNING
                schedule.last_run_completed_at = None
                schedule.last_run_error = None
                session.add(schedule)
            session.flush()

            collector = TableObservabilityMetricCollector(session)
            metrics, table_count = collector.collect_for_category(category_key)
            run.table_count = table_count
            run.metrics_collected = len(metrics)

            metric_rows = [
                TableObservabilityMetricModel(
                    run_id=run.id,
                    selection_id=metric.selection_id,
                    system_connection_id=metric.system_connection_id,
                    schema_name=metric.schema_name,
                    table_name=metric.table_name,
                    metric_category=metric.metric_category,
                    metric_name=metric.metric_name,
                    metric_unit=metric.metric_unit,
                    metric_value_number=metric.metric_value_number,
                    metric_value_text=metric.metric_value_text,
                    metric_payload=metric.metric_payload,
                    metric_status=metric.metric_status,
                    recorded_at=metric.recorded_at,
                )
                for metric in metrics
            ]
            session.add_all(metric_rows)

            run.status = RUN_STATUS_SUCCEEDED
            run.completed_at = _utcnow()
            session.add(run)
            if schedule:
                schedule.last_run_status = run.status
                schedule.last_run_completed_at = run.completed_at
                session.add(schedule)

            if managed:
                session.commit()
                session.refresh(run)
            else:
                session.flush()
            return run
        except Exception as exc:
            run_error = str(exc)
            if run is not None:
                run.status = RUN_STATUS_FAILED
                run.error = run_error
                run.completed_at = _utcnow()
                session.add(run)
            if schedule:
                schedule.last_run_status = RUN_STATUS_FAILED
                schedule.last_run_error = run_error
                schedule.last_run_completed_at = _utcnow()
                session.add(schedule)
            if managed:
                session.rollback()
            logger.exception("Table observability run failed for category %s", category_key)
            raise
        finally:
            if managed:
                session.close()


class TableObservabilityEngine:
    """Background scheduler that keeps observability runs aligned with cron expressions."""

    def __init__(self, session_factory: Callable[[], Session] = SessionLocal) -> None:
        self._session_factory = session_factory
        self._scheduler: AsyncIOScheduler | None = None
        self._schedule_service = TableObservabilityScheduleService(session_factory)
        self._run_service = TableObservabilityRunService(session_factory)

    def start(self) -> None:
        if self._scheduler is not None and self._scheduler.running:
            return
        self._scheduler = AsyncIOScheduler()
        self._scheduler.start()
        self.reload_jobs()

    def shutdown(self) -> None:
        if self._scheduler is not None:
            self._scheduler.shutdown(wait=False)
            self._scheduler = None

    def reload_jobs(self) -> None:
        scheduler = self._scheduler
        if scheduler is None:
            return
        logger.debug("Reloading table observability schedules")
        for job in scheduler.get_jobs():
            scheduler.remove_job(job.id)

        with self._session_scope() as session:
            self._schedule_service.ensure_category_schedules(session=session)
            schedules = (
                session.query(TableObservabilityCategorySchedule)
                .filter(TableObservabilityCategorySchedule.is_active.is_(True))
                .all()
            )

        for schedule in schedules:
            try:
                trigger = self._build_trigger(schedule.cron_expression, schedule.timezone)
            except ValueError as exc:
                logger.warning(
                    "Skipping table observability schedule %s due to invalid cron expression: %s",
                    schedule.id,
                    exc,
                )
                continue
            scheduler.add_job(
                self.run_schedule,
                trigger=trigger,
                args=[str(schedule.id)],
                id=str(schedule.id),
                replace_existing=True,
            )

    def trigger_now(self, schedule_id: UUID) -> None:
        self.run_schedule(str(schedule_id))

    def run_schedule(self, schedule_id: str) -> None:
        schedule_uuid = UUID(schedule_id)
        try:
            self._run_service.run_for_schedule(schedule_uuid)
        except Exception:
            logger.exception("Table observability run errored for schedule %s", schedule_uuid)

    def _build_trigger(self, expression: str, tz_name: str | None) -> CronTrigger:
        tz = timezone.utc
        if tz_name:
            try:
                tz = ZoneInfo(tz_name)
            except ZoneInfoNotFoundError:
                logger.warning("Unknown timezone '%s' for observability schedule; defaulting to UTC", tz_name)
        return CronTrigger.from_crontab(expression, timezone=tz)

    @contextmanager
    def _session_scope(self) -> Generator[Session, None, None]:
        session = self._session_factory()
        session.expire_on_commit = False
        try:
            yield session
            session.commit()
        except Exception:
            session.rollback()
            raise
        finally:
            session.close()


table_observability_engine = TableObservabilityEngine()


__all__ = [
    "build_table_observability_plan",
    "TABLE_OBSERVABILITY_CATEGORY_KEYS",
    "get_observability_category_definition",
    "TableObservabilityMetricCollector",
    "TableObservabilityScheduleService",
    "TableObservabilityRunService",
    "TableObservabilityEngine",
    "table_observability_engine",
]
