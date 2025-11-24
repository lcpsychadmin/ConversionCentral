from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from typing import Dict, Mapping, Sequence
from uuid import UUID

from sqlalchemy.orm import Session

from app.schemas.data_quality import (
    DataQualityDatasetProfilingStatsResponse,
    DataQualityProfilingStats,
    DataQualityTableProfilingStats,
)
from app.services.data_quality_table_context import TableContext, resolve_all_table_contexts
from app.services.data_quality_testgen import TestGenClient


@dataclass
class _StatsAccumulator:
    table_count: int = 0
    profiled_table_count: int = 0
    row_count_total: int = 0
    row_count_has_value: bool = False
    column_count_total: int = 0
    column_count_has_value: bool = False
    profiled_column_total: int = 0
    profiled_column_has_value: bool = False
    anomaly_total: int = 0
    anomaly_has_value: bool = False
    dq_score_total: float = 0.0
    dq_score_count: int = 0
    last_completed_at: datetime | None = None

    def absorb(self, stats: DataQualityProfilingStats) -> None:
        self.table_count += stats.table_count
        self.profiled_table_count += stats.profiled_table_count
        if stats.row_count is not None:
            self.row_count_total += stats.row_count
            self.row_count_has_value = True
        if stats.column_count is not None:
            self.column_count_total += stats.column_count
            self.column_count_has_value = True
        if stats.profiled_column_count is not None:
            self.profiled_column_total += stats.profiled_column_count
            self.profiled_column_has_value = True
        if stats.anomaly_count is not None:
            self.anomaly_total += stats.anomaly_count
            self.anomaly_has_value = True
        if stats.dq_score is not None:
            self.dq_score_total += stats.dq_score
            self.dq_score_count += 1
        if stats.last_completed_at is not None:
            if self.last_completed_at is None or stats.last_completed_at > self.last_completed_at:
                self.last_completed_at = stats.last_completed_at

    def as_stats(self) -> DataQualityProfilingStats:
        return DataQualityProfilingStats(
            table_count=self.table_count,
            profiled_table_count=self.profiled_table_count,
            row_count=self.row_count_total if self.row_count_has_value else None,
            column_count=self.column_count_total if self.column_count_has_value else None,
            profiled_column_count=self.profiled_column_total if self.profiled_column_has_value else None,
            anomaly_count=self.anomaly_total if self.anomaly_has_value else None,
            dq_score=(self.dq_score_total / self.dq_score_count) if self.dq_score_count else None,
            last_completed_at=self.last_completed_at,
        )


def build_dataset_profiling_stats(
    db: Session,
    client: TestGenClient,
) -> DataQualityDatasetProfilingStatsResponse:
    contexts = resolve_all_table_contexts(db)
    table_ids = sorted({ctx.table_id for ctx in contexts if ctx.table_id})
    metrics = _load_table_metrics(client, table_ids)

    tables: Dict[str, DataQualityTableProfilingStats] = {}
    data_object_accumulators: Dict[UUID, _StatsAccumulator] = {}
    application_accumulators: Dict[UUID, _StatsAccumulator] = {}
    product_team_accumulators: Dict[UUID, _StatsAccumulator] = {}

    for context in contexts:
        metrics_row = metrics.get(context.table_id) if context.table_id else None
        table_stats = _build_table_stats(context, metrics_row)
        tables[str(context.data_definition_table_id)] = table_stats

        _absorb(data_object_accumulators, context.data_object_id, table_stats)
        _absorb(application_accumulators, context.application_id, table_stats)
        if context.product_team_id:
            _absorb(product_team_accumulators, context.product_team_id, table_stats)

    return DataQualityDatasetProfilingStatsResponse(
        product_teams={str(key): acc.as_stats() for key, acc in product_team_accumulators.items()},
        applications={str(key): acc.as_stats() for key, acc in application_accumulators.items()},
        data_objects={str(key): acc.as_stats() for key, acc in data_object_accumulators.items()},
        tables=tables,
    )


def _absorb(
    bucket: Dict[UUID, _StatsAccumulator],
    key: UUID,
    stats: DataQualityProfilingStats,
) -> None:
    accumulator = bucket.setdefault(key, _StatsAccumulator())
    accumulator.absorb(stats)


def _load_table_metrics(
    client: TestGenClient,
    table_ids: Sequence[str],
) -> Dict[str, Mapping[str, object]]:
    if not table_ids:
        return {}
    rows = client.fetch_table_characteristics(table_ids=table_ids)
    return {str(row.get("table_id")): row for row in rows if row.get("table_id")}


def _build_table_stats(
    context: TableContext,
    metrics: Mapping[str, object] | None,
) -> DataQualityTableProfilingStats:
    profiled = 1 if metrics else 0
    row_count = _coerce_int(metrics.get("record_count") if metrics else None)
    column_count = _coerce_int(metrics.get("column_count") if metrics else None)
    anomaly_count = _coerce_int(metrics.get("latest_anomaly_ct") if metrics else None)
    dq_score = _coerce_float(metrics.get("dq_score_profiling") if metrics else None)
    completed_at = _coerce_datetime(metrics.get("latest_run_completed_at") if metrics else None)

    return DataQualityTableProfilingStats(
        table_group_id=context.table_group_id,
        table_id=context.table_id,
        table_count=1,
        profiled_table_count=profiled,
        row_count=row_count,
        column_count=column_count,
        profiled_column_count=column_count if profiled else None,
        anomaly_count=anomaly_count,
        dq_score=dq_score,
        last_completed_at=completed_at,
    )


def _coerce_int(value: object | None) -> int | None:
    if value is None:
        return None
    if isinstance(value, bool):
        return int(value)
    if isinstance(value, int):
        return value
    if isinstance(value, Decimal):
        try:
            return int(value)
        except (TypeError, ValueError):
            return int(float(value))
    try:
        return int(str(value))
    except (TypeError, ValueError):
        try:
            return int(float(value))
        except (TypeError, ValueError):
            return None


def _coerce_float(value: object | None) -> float | None:
    if value is None:
        return None
    if isinstance(value, bool):
        return float(int(value))
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, Decimal):
        return float(value)
    try:
        return float(str(value))
    except (TypeError, ValueError):
        return None


def _coerce_datetime(value: object | None) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value
    if isinstance(value, str):
        text = value.strip()
        if not text:
            return None
        normalized = text.replace(" ", "T")
        try:
            return datetime.fromisoformat(normalized)
        except ValueError:
            return None
    return None
