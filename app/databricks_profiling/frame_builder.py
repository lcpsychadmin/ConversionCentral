"""Utilities for translating profiling payloads into Spark DataFrames."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from datetime import date, datetime, timezone
from decimal import Decimal
import hashlib
import json
import math
import uuid
from typing import Any

try:  # pragma: no cover - optional dependency for non-notebook environments
    from pyspark.sql import types as T
except ModuleNotFoundError:  # pragma: no cover - only raised outside Databricks/Spark contexts
    T = None


_FRAME_NAMES = (
    "profile_results_df",
    "profile_columns_df",
    "profile_column_values_df",
    "profile_anomalies_df",
    "table_characteristics_df",
    "column_characteristics_df",
)


def _build_profile_results_schema():  # pragma: no cover - simple struct definition
    if T is None:
        raise RuntimeError("pyspark must be installed to build metadata frames.")
    return T.StructType(
        [
            T.StructField("result_id", T.StringType(), False),
            T.StructField("profile_run_id", T.StringType(), True),
            T.StructField("table_id", T.StringType(), True),
            T.StructField("column_id", T.StringType(), True),
            T.StructField("schema_name", T.StringType(), True),
            T.StructField("table_name", T.StringType(), True),
            T.StructField("column_name", T.StringType(), True),
            T.StructField("data_type", T.StringType(), True),
            T.StructField("general_type", T.StringType(), True),
            T.StructField("record_count", T.LongType(), True),
            T.StructField("null_count", T.LongType(), True),
            T.StructField("distinct_count", T.LongType(), True),
            T.StructField("min_value", T.StringType(), True),
            T.StructField("max_value", T.StringType(), True),
            T.StructField("avg_value", T.DoubleType(), True),
            T.StructField("stddev_value", T.DoubleType(), True),
            T.StructField("percentiles_json", T.StringType(), True),
            T.StructField("top_values_json", T.StringType(), True),
            T.StructField("metrics_json", T.StringType(), True),
            T.StructField("generated_at", T.TimestampType(), True),
        ]
    )


def _build_profile_columns_schema():  # pragma: no cover - simple struct definition
    if T is None:
        raise RuntimeError("pyspark must be installed to build metadata frames.")
    return T.StructType(
        [
            T.StructField("profile_run_id", T.StringType(), False),
            T.StructField("schema_name", T.StringType(), True),
            T.StructField("table_name", T.StringType(), True),
            T.StructField("column_name", T.StringType(), True),
            T.StructField("qualified_name", T.StringType(), True),
            T.StructField("data_type", T.StringType(), True),
            T.StructField("general_type", T.StringType(), True),
            T.StructField("ordinal_position", T.IntegerType(), True),
            T.StructField("row_count", T.LongType(), True),
            T.StructField("null_count", T.LongType(), True),
            T.StructField("non_null_count", T.LongType(), True),
            T.StructField("distinct_count", T.LongType(), True),
            T.StructField("min_value", T.StringType(), True),
            T.StructField("max_value", T.StringType(), True),
            T.StructField("avg_value", T.DoubleType(), True),
            T.StructField("stddev_value", T.DoubleType(), True),
            T.StructField("median_value", T.DoubleType(), True),
            T.StructField("p95_value", T.DoubleType(), True),
            T.StructField("true_count", T.LongType(), True),
            T.StructField("false_count", T.LongType(), True),
            T.StructField("min_length", T.IntegerType(), True),
            T.StructField("max_length", T.IntegerType(), True),
            T.StructField("avg_length", T.DoubleType(), True),
            T.StructField("non_ascii_ratio", T.DoubleType(), True),
            T.StructField("min_date", T.DateType(), True),
            T.StructField("max_date", T.DateType(), True),
            T.StructField("date_span_days", T.IntegerType(), True),
            T.StructField("metrics_json", T.StringType(), True),
            T.StructField("generated_at", T.TimestampType(), True),
        ]
    )


def _build_profile_column_values_schema():  # pragma: no cover - simple struct definition
    if T is None:
        raise RuntimeError("pyspark must be installed to build metadata frames.")
    return T.StructType(
        [
            T.StructField("profile_run_id", T.StringType(), False),
            T.StructField("schema_name", T.StringType(), True),
            T.StructField("table_name", T.StringType(), True),
            T.StructField("column_name", T.StringType(), True),
            T.StructField("value", T.StringType(), True),
            T.StructField("value_hash", T.StringType(), True),
            T.StructField("frequency", T.LongType(), True),
            T.StructField("relative_freq", T.DoubleType(), True),
            T.StructField("rank", T.IntegerType(), True),
            T.StructField("bucket_label", T.StringType(), True),
            T.StructField("bucket_lower_bound", T.DoubleType(), True),
            T.StructField("bucket_upper_bound", T.DoubleType(), True),
            T.StructField("generated_at", T.TimestampType(), True),
        ]
    )


def _build_profile_anomalies_schema():  # pragma: no cover - simple struct definition
    if T is None:
        raise RuntimeError("pyspark must be installed to build metadata frames.")
    return T.StructType(
        [
            T.StructField("anomaly_result_id", T.StringType(), False),
            T.StructField("profile_run_id", T.StringType(), True),
            T.StructField("table_id", T.StringType(), True),
            T.StructField("column_id", T.StringType(), True),
            T.StructField("table_name", T.StringType(), True),
            T.StructField("column_name", T.StringType(), True),
            T.StructField("anomaly_type_id", T.StringType(), True),
            T.StructField("severity", T.StringType(), True),
            T.StructField("likelihood", T.StringType(), True),
            T.StructField("detail", T.StringType(), True),
            T.StructField("pii_risk", T.StringType(), True),
            T.StructField("dq_dimension", T.StringType(), True),
            T.StructField("detected_at", T.TimestampType(), True),
            T.StructField("dismissed", T.BooleanType(), True),
            T.StructField("dismissed_by", T.StringType(), True),
            T.StructField("dismissed_at", T.TimestampType(), True),
        ]
    )


def _build_table_characteristics_schema():  # pragma: no cover - simple struct definition
    if T is None:
        raise RuntimeError("pyspark must be installed to build metadata frames.")
    return T.StructType(
        [
            T.StructField("table_id", T.StringType(), False),
            T.StructField("table_group_id", T.StringType(), True),
            T.StructField("schema_name", T.StringType(), True),
            T.StructField("table_name", T.StringType(), True),
            T.StructField("record_count", T.LongType(), True),
            T.StructField("column_count", T.IntegerType(), True),
            T.StructField("data_point_count", T.LongType(), True),
            T.StructField("critical_data_element", T.BooleanType(), True),
            T.StructField("data_source", T.StringType(), True),
            T.StructField("source_system", T.StringType(), True),
            T.StructField("source_process", T.StringType(), True),
            T.StructField("business_domain", T.StringType(), True),
            T.StructField("stakeholder_group", T.StringType(), True),
            T.StructField("transform_level", T.StringType(), True),
            T.StructField("data_product", T.StringType(), True),
            T.StructField("dq_score_profiling", T.DoubleType(), True),
            T.StructField("dq_score_testing", T.DoubleType(), True),
            T.StructField("last_complete_profile_run_id", T.StringType(), True),
            T.StructField("latest_anomaly_ct", T.IntegerType(), True),
            T.StructField("latest_run_completed_at", T.TimestampType(), True),
            T.StructField("created_at", T.TimestampType(), True),
            T.StructField("updated_at", T.TimestampType(), True),
        ]
    )


def _build_column_characteristics_schema():  # pragma: no cover - simple struct definition
    if T is None:
        raise RuntimeError("pyspark must be installed to build metadata frames.")
    return T.StructType(
        [
            T.StructField("column_id", T.StringType(), False),
            T.StructField("table_id", T.StringType(), True),
            T.StructField("schema_name", T.StringType(), True),
            T.StructField("table_name", T.StringType(), True),
            T.StructField("column_name", T.StringType(), True),
            T.StructField("data_type", T.StringType(), True),
            T.StructField("functional_data_type", T.StringType(), True),
            T.StructField("critical_data_element", T.BooleanType(), True),
            T.StructField("pii_risk", T.StringType(), True),
            T.StructField("dq_dimension", T.StringType(), True),
            T.StructField("tags_json", T.StringType(), True),
            T.StructField("dq_score_profiling", T.DoubleType(), True),
            T.StructField("dq_score_testing", T.DoubleType(), True),
            T.StructField("last_complete_profile_run_id", T.StringType(), True),
            T.StructField("latest_anomaly_ct", T.IntegerType(), True),
            T.StructField("latest_run_completed_at", T.TimestampType(), True),
            T.StructField("created_at", T.TimestampType(), True),
            T.StructField("updated_at", T.TimestampType(), True),
        ]
    )


if T is not None:  # pragma: no branch - schema definitions happen once per interpreter session
    PROFILE_RESULTS_SCHEMA = _build_profile_results_schema()
    PROFILE_COLUMNS_SCHEMA = _build_profile_columns_schema()
    PROFILE_COLUMN_VALUES_SCHEMA = _build_profile_column_values_schema()
    PROFILE_ANOMALIES_SCHEMA = _build_profile_anomalies_schema()
    TABLE_CHARACTERISTICS_SCHEMA = _build_table_characteristics_schema()
    COLUMN_CHARACTERISTICS_SCHEMA = _build_column_characteristics_schema()
else:  # pragma: no cover - allows importing module without pyspark
    PROFILE_RESULTS_SCHEMA = None
    PROFILE_COLUMNS_SCHEMA = None
    PROFILE_COLUMN_VALUES_SCHEMA = None
    PROFILE_ANOMALIES_SCHEMA = None
    TABLE_CHARACTERISTICS_SCHEMA = None
    COLUMN_CHARACTERISTICS_SCHEMA = None


class ProfilingPayloadFrameBuilder:
    """Parses profiling payloads into row dictionaries for metadata tables."""

    def __init__(
        self,
        payload: Any,
        *,
        profile_run_id: str,
        table_group_id: str | None,
        summary: Mapping[str, Any] | None = None,
    ) -> None:
        profile_run_id = (profile_run_id or "").strip()
        if not profile_run_id:
            raise ValueError("profile_run_id is required when building profiling frames.")

        self._payload = payload
        self._profile_run_id = profile_run_id
        self._table_group_id = (table_group_id or "").strip() or None
        self._summary = summary or {}
        self._now = datetime.now(timezone.utc)
        self._completed_at = self._coerce_timestamp(
            self._summary.get("completed_at") or self._summary.get("completedAt") or self._summary.get("completed")
        ) or self._now

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------
    def build_rows(self) -> tuple[dict[str, list[dict[str, Any]]], dict[str, int]]:
        row_sets: dict[str, list[dict[str, Any]]] = {name: [] for name in _FRAME_NAMES}

        tables_processed = 0
        for raw_table in self._iter_tables(self._payload):
            table_context = self._build_table_context(raw_table)
            if table_context is None:
                continue

            column_payloads = []
            for ordinal, column_entry in enumerate(table_context["columns"], start=1):
                column_payload = self._build_column_payload(column_entry, table_context, ordinal)
                if column_payload is None:
                    continue
                column_payloads.append(column_payload)
                row_sets["profile_columns_df"].append(column_payload["profile_column_row"])
                row_sets["profile_results_df"].append(column_payload["profile_result_row"])
                row_sets["column_characteristics_df"].append(column_payload["column_characteristics_row"])
                if column_payload["value_rows"]:
                    row_sets["profile_column_values_df"].extend(column_payload["value_rows"])
                if column_payload["anomaly_rows"]:
                    row_sets["profile_anomalies_df"].extend(column_payload["anomaly_rows"])

            table_row = self._build_table_characteristics_row(table_context, column_payloads)
            if table_row is not None:
                row_sets["table_characteristics_df"].append(table_row)

            table_level_anomalies = self._build_anomaly_rows(
                table_context,
                column_id=None,
                column_name=None,
                anomalies=table_context["anomalies"],
            )
            if table_level_anomalies:
                row_sets["profile_anomalies_df"].extend(table_level_anomalies)

            tables_processed += 1

        counts = {name: len(rows) for name, rows in row_sets.items()}
        if tables_processed == 0:
            return row_sets, counts
        return row_sets, counts

    # ------------------------------------------------------------------
    # Table parsing helpers
    # ------------------------------------------------------------------
    def _iter_tables(self, payload: Any) -> Sequence[Mapping[str, Any]]:
        tables: list[Mapping[str, Any]] = []
        if isinstance(payload, Mapping):
            for key in ("tables", "table_profiles", "tablesProfiled", "tableProfiles"):
                value = payload.get(key)
                if isinstance(value, Sequence) and not isinstance(value, (str, bytes)):
                    tables.extend(item for item in value if isinstance(item, Mapping))
                    break
            else:
                tables.append(payload)
        elif isinstance(payload, Sequence) and not isinstance(payload, (str, bytes)):
            tables.extend(item for item in payload if isinstance(item, Mapping))
        return tables

    def _build_table_context(self, entry: Mapping[str, Any]) -> dict[str, Any] | None:
        table_name = self._first_text(entry, "table_name", "tableName", "name", "table")
        if not table_name:
            return None

        schema_name = self._first_text(entry, "schema_name", "schemaName", "schema")
        if schema_name is None and "." in table_name:
            schema_name, _, final = table_name.rpartition(".")
            table_name = final

        table_id = self._first_text(entry, "table_id", "tableId", "id")
        fallback_id = self._fallback_table_id(schema_name, table_name)
        table_id = table_id or fallback_id

        columns_seq = self._extract_columns(entry)
        anomalies = self._extract_anomalies(entry)
        metrics = self._merge_metrics(entry)

        table_context = {
            "raw": entry,
            "table_id": table_id,
            "table_group_id": self._first_text(entry, "table_group_id", "tableGroupId") or self._table_group_id,
            "schema_name": schema_name,
            "table_name": table_name,
            "columns": columns_seq,
            "anomalies": anomalies,
            "metrics": metrics,
            "row_count": self._coerce_int(
                self._first_value(
                    entry,
                    metrics,
                    "record_count",
                    "row_count",
                    "rows",
                    "rowCount",
                    "recordCount",
                )
            ),
            "dq_score_profiling": self._coerce_float(
                self._first_value(entry, metrics, "dq_score_profiling", "profiling_score", "dqScoreProfiling")
            ),
            "dq_score_testing": self._coerce_float(
                self._first_value(entry, metrics, "dq_score_testing", "testing_score", "dqScoreTesting")
            ),
            "critical_data_element": self._coerce_bool(
                self._first_value(entry, metrics, "critical_data_element", "criticalDataElement")
            ),
            "tags": entry.get("tags"),
        }
        return table_context

    def _build_table_characteristics_row(
        self,
        table_context: Mapping[str, Any],
        column_payloads: Sequence[dict[str, Any]],
    ) -> dict[str, Any] | None:
        table_id = table_context["table_id"]
        table_name = table_context["table_name"]
        if not table_id or not table_name:
            return None

        schema_name = table_context.get("schema_name")
        record_count = table_context.get("row_count")
        if record_count is None:
            for payload in column_payloads:
                candidate = payload.get("row_count")
                if candidate is not None:
                    record_count = candidate
                    break

        table_anomaly_ct = len(table_context.get("anomalies") or [])
        column_anomaly_total = sum(payload.get("anomaly_count", 0) for payload in column_payloads)
        latest_anomaly_ct = table_anomaly_ct + column_anomaly_total
        column_count = len(column_payloads) if column_payloads else None
        data_point_count = None
        if record_count is not None and column_count is not None:
            try:
                data_point_count = int(record_count) * int(column_count)
            except (TypeError, ValueError):
                data_point_count = None

        return {
            "table_id": table_id,
            "table_group_id": table_context.get("table_group_id"),
            "schema_name": schema_name,
            "table_name": table_name,
            "record_count": record_count,
            "column_count": column_count,
            "data_point_count": data_point_count,
            "critical_data_element": table_context.get("critical_data_element"),
            "data_source": self._first_text(table_context.get("raw", {}), "data_source", "dataSource"),
            "source_system": self._first_text(table_context.get("raw", {}), "source_system", "sourceSystem"),
            "source_process": self._first_text(table_context.get("raw", {}), "source_process", "sourceProcess"),
            "business_domain": self._first_text(table_context.get("raw", {}), "business_domain", "businessDomain"),
            "stakeholder_group": self._first_text(table_context.get("raw", {}), "stakeholder_group", "stakeholderGroup"),
            "transform_level": self._first_text(table_context.get("raw", {}), "transform_level", "transformLevel"),
            "data_product": self._first_text(table_context.get("raw", {}), "data_product", "dataProduct"),
            "dq_score_profiling": table_context.get("dq_score_profiling"),
            "dq_score_testing": table_context.get("dq_score_testing"),
            "last_complete_profile_run_id": self._profile_run_id,
            "latest_anomaly_ct": latest_anomaly_ct or None,
            "latest_run_completed_at": self._completed_at,
            "created_at": self._now,
            "updated_at": self._now,
        }

    # ------------------------------------------------------------------
    # Column parsing helpers
    # ------------------------------------------------------------------
    def _build_column_payload(
        self,
        column_entry: Mapping[str, Any],
        table_context: Mapping[str, Any],
        ordinal: int,
    ) -> dict[str, Any] | None:
        column_name = self._first_text(column_entry, "column_name", "columnName", "name", "column")
        if not column_name:
            return None

        metrics = self._merge_metrics(column_entry)
        data_type = self._first_text(column_entry, "data_type", "dataType", "type")
        general_type = self._first_text(
            column_entry,
            "general_type",
            "generalType",
            "type_category",
            "typeCategory",
        ) or self._first_text(metrics, "general_type", "generalType", "type_category")

        row_count = self._coerce_int(
            self._first_value(column_entry, metrics, "row_count", "record_count", "rows", "rowCount", "recordCount")
        )
        null_count = self._coerce_int(self._first_value(column_entry, metrics, "null_count", "nullCount"))
        distinct_count = self._coerce_int(
            self._first_value(column_entry, metrics, "distinct_count", "distinctCount", "unique_count", "uniqueCount")
        )
        non_null_count = self._coerce_int(
            self._first_value(column_entry, metrics, "non_null_count", "nonNullCount")
        )
        if non_null_count is None and row_count is not None and null_count is not None:
            try:
                non_null_count = max(int(row_count) - int(null_count), 0)
            except (TypeError, ValueError):
                non_null_count = None

        min_value = self._first_text(column_entry, "min_value", "minValue", "min") or self._first_text(
            metrics,
            "min_value",
            "minValue",
            "min",
        )
        max_value = self._first_text(column_entry, "max_value", "maxValue", "max") or self._first_text(
            metrics,
            "max_value",
            "maxValue",
            "max",
        )
        avg_value = self._coerce_float(self._first_value(column_entry, metrics, "avg_value", "avg", "avgValue", "mean"))
        stddev_value = self._coerce_float(
            self._first_value(column_entry, metrics, "stddev_value", "stddev", "stdDev", "stdDeviation")
        )
        median_value = self._coerce_float(
            self._first_value(column_entry, metrics, "median_value", "median", "p50", "percentile_50")
        )
        p95_value = self._coerce_float(
            self._first_value(column_entry, metrics, "p95_value", "p95", "percentile_95", "upper_95")
        )
        true_count = self._coerce_int(self._first_value(column_entry, metrics, "true_count", "trueCount"))
        false_count = self._coerce_int(self._first_value(column_entry, metrics, "false_count", "falseCount"))
        min_length = self._coerce_int(self._first_value(column_entry, metrics, "min_length", "minLength"))
        max_length = self._coerce_int(self._first_value(column_entry, metrics, "max_length", "maxLength"))
        avg_length = self._coerce_float(self._first_value(column_entry, metrics, "avg_length", "avgLength"))
        non_ascii_ratio = self._coerce_float(
            self._first_value(column_entry, metrics, "non_ascii_ratio", "nonAsciiRatio", "non_ascii_percent")
        )
        min_date = self._coerce_date(self._first_value(column_entry, metrics, "min_date", "minDate"))
        max_date = self._coerce_date(self._first_value(column_entry, metrics, "max_date", "maxDate"))
        date_span_days = self._coerce_int(self._first_value(column_entry, metrics, "date_span_days", "dateSpanDays"))
        if date_span_days is None and min_date and max_date:
            date_span_days = (max_date - min_date).days

        percentiles_payload = {
            key: value
            for key, value in {
                "p25": self._coerce_float(self._first_value(metrics, {}, "p25", "percentile_25")),
                "p50": median_value,
                "p75": self._coerce_float(self._first_value(metrics, {}, "p75", "percentile_75")),
                "p95": p95_value,
            }.items()
            if value is not None
        }
        percentiles_json = self._serialize_json(percentiles_payload) if percentiles_payload else None

        top_values = self._extract_sequence(column_entry, "top_values", "topValues")
        if not top_values:
            top_values = self._extract_sequence(metrics, "top_values", "topValues")

        histogram = self._extract_sequence(column_entry, "histogram", "value_distribution", "distribution")
        if not histogram:
            histogram = self._extract_sequence(metrics, "histogram", "value_distribution", "distribution")
        top_values_json = None
        if top_values or histogram:
            top_values_json = self._serialize_json({"top_values": top_values, "histogram": histogram})
        value_rows = self._build_value_rows(
            table_context,
            column_name=column_name,
            top_values=top_values,
            histogram=histogram,
            row_count=row_count,
        )

        metrics_json = self._serialize_json(
            column_entry.get("metrics_json")
            or column_entry.get("metricsJson")
            or column_entry.get("metrics")
            or metrics
        )

        qualified_name = self._qualified_column_name(table_context.get("schema_name"), table_context.get("table_name"), column_name)
        column_id = self._first_text(column_entry, "column_id", "columnId", "id")
        table_id = table_context.get("table_id")
        column_id = column_id or self._fallback_column_id(table_id, schema_name=table_context.get("schema_name"), table_name=table_context.get("table_name"), column_name=column_name)

        column_anomalies = self._extract_anomalies(column_entry)
        anomaly_rows = self._build_anomaly_rows(table_context, column_id, column_name, column_anomalies)

        profile_column_row = {
            "profile_run_id": self._profile_run_id,
            "schema_name": table_context.get("schema_name"),
            "table_name": table_context.get("table_name"),
            "column_name": column_name,
            "qualified_name": qualified_name,
            "data_type": data_type,
            "general_type": general_type,
            "ordinal_position": self._coerce_int(
                self._first_value(column_entry, metrics, "ordinal_position", "ordinalPosition", "position", "index")
            )
            or ordinal,
            "row_count": row_count,
            "null_count": null_count,
            "non_null_count": non_null_count,
            "distinct_count": distinct_count,
            "min_value": min_value,
            "max_value": max_value,
            "avg_value": avg_value,
            "stddev_value": stddev_value,
            "median_value": median_value,
            "p95_value": p95_value,
            "true_count": true_count,
            "false_count": false_count,
            "min_length": min_length,
            "max_length": max_length,
            "avg_length": avg_length,
            "non_ascii_ratio": non_ascii_ratio,
            "min_date": min_date,
            "max_date": max_date,
            "date_span_days": date_span_days,
            "metrics_json": metrics_json,
            "generated_at": self._now,
        }

        profile_result_row = {
            "result_id": uuid.uuid4().hex,
            "profile_run_id": self._profile_run_id,
            "table_id": table_id,
            "column_id": column_id,
            "schema_name": table_context.get("schema_name"),
            "table_name": table_context.get("table_name"),
            "column_name": column_name,
            "data_type": data_type,
            "general_type": general_type,
            "record_count": row_count,
            "null_count": null_count,
            "distinct_count": distinct_count,
            "min_value": min_value,
            "max_value": max_value,
            "avg_value": avg_value,
            "stddev_value": stddev_value,
            "percentiles_json": percentiles_json,
            "top_values_json": top_values_json,
            "metrics_json": metrics_json,
            "generated_at": self._now,
        }

        column_characteristics_row = {
            "column_id": column_id,
            "table_id": table_id,
            "schema_name": table_context.get("schema_name"),
            "table_name": table_context.get("table_name"),
            "column_name": column_name,
            "data_type": data_type,
            "functional_data_type": self._first_text(
                column_entry,
                "functional_data_type",
                "functionalDataType",
            )
            or general_type,
            "critical_data_element": self._coerce_bool(
                self._first_value(column_entry, metrics, "critical_data_element", "criticalDataElement", "critical")
            ),
            "pii_risk": self._first_text(column_entry, "pii_risk", "piiRisk", "pii_classification", "piiClassification"),
            "dq_dimension": self._first_text(column_entry, "dq_dimension", "dqDimension", "dimension"),
            "tags_json": self._serialize_json(column_entry.get("tags")),
            "dq_score_profiling": self._coerce_float(
                self._first_value(column_entry, metrics, "dq_score_profiling", "profiling_score", "dqScoreProfiling")
            ),
            "dq_score_testing": self._coerce_float(
                self._first_value(column_entry, metrics, "dq_score_testing", "testing_score", "dqScoreTesting")
            ),
            "last_complete_profile_run_id": self._profile_run_id,
            "latest_anomaly_ct": len(anomaly_rows) or None,
            "latest_run_completed_at": self._completed_at,
            "created_at": self._now,
            "updated_at": self._now,
        }

        return {
            "profile_column_row": profile_column_row,
            "profile_result_row": profile_result_row,
            "column_characteristics_row": column_characteristics_row,
            "anomaly_rows": anomaly_rows,
            "value_rows": value_rows,
            "row_count": row_count,
            "anomaly_count": len(anomaly_rows),
        }

    def _build_value_rows(
        self,
        table_context: Mapping[str, Any],
        *,
        column_name: str,
        top_values: Sequence[Mapping[str, Any]] | Sequence[Any],
        histogram: Sequence[Mapping[str, Any]] | Sequence[Any],
        row_count: int | None,
    ) -> list[dict[str, Any]]:
        schema_name = table_context.get("schema_name")
        table_name = table_context.get("table_name")
        base_row = {
            "profile_run_id": self._profile_run_id,
            "schema_name": schema_name,
            "table_name": table_name,
            "column_name": column_name,
            "generated_at": self._now,
        }
        rows: list[dict[str, Any]] = []
        total_rows = row_count
        if total_rows in (None, 0):
            table_row_total = self._coerce_int(table_context.get("row_count"))
            if table_row_total not in (None, 0):
                total_rows = table_row_total

        def append_row(payload: dict[str, Any]) -> None:
            rows.append({**base_row, **payload})

        for index, entry in enumerate(top_values or [], start=1):
            if not isinstance(entry, Mapping):
                continue
            raw_value = entry.get("value")
            if raw_value is None:
                raw_value = entry.get("label")
            frequency = self._coerce_int(entry.get("count"))
            relative_freq = self._coerce_float(entry.get("percentage"))
            if relative_freq is None:
                relative_freq = self._compute_relative_frequency(frequency, total_rows)
            append_row(
                {
                    "value": self._stringify_value(raw_value),
                    "value_hash": self._hash_value(raw_value),
                    "frequency": frequency,
                    "relative_freq": relative_freq,
                    "rank": self._coerce_int(entry.get("rank")) or index,
                    "bucket_label": None,
                    "bucket_lower_bound": None,
                    "bucket_upper_bound": None,
                }
            )

        for entry in histogram or []:
            if not isinstance(entry, Mapping):
                continue
            lower = self._coerce_float(entry.get("lower") or entry.get("bucket_lower") or entry.get("bucketLower"))
            upper = self._coerce_float(entry.get("upper") or entry.get("bucket_upper") or entry.get("bucketUpper"))
            label = (
                self._first_text(entry, "label", "bucket_label", "bucketLabel")
                or self._stringify_value(entry.get("value"))
            )
            raw_value = entry.get("value")
            display_value = raw_value if raw_value is not None else label
            hash_source = {
                "value": raw_value,
                "label": label,
                "lower": lower,
                "upper": upper,
            }
            frequency = self._coerce_int(entry.get("count"))
            relative_freq = self._coerce_float(entry.get("percentage"))
            if relative_freq is None:
                relative_freq = self._compute_relative_frequency(frequency, total_rows)
            append_row(
                {
                    "value": self._stringify_value(display_value),
                    "value_hash": self._hash_value(hash_source),
                    "frequency": frequency,
                    "relative_freq": relative_freq,
                    "rank": self._coerce_int(entry.get("rank")),
                    "bucket_label": label,
                    "bucket_lower_bound": lower,
                    "bucket_upper_bound": upper,
                }
            )

        return rows

    def _stringify_value(self, value: Any) -> str | None:
        if value is None:
            return None
        if isinstance(value, str):
            return value
        if isinstance(value, (datetime, date)):
            return value.isoformat()
        if isinstance(value, Decimal):
            text = format(value, "f")
            return text.rstrip("0").rstrip(".") if "." in text else text
        return str(value)

    def _hash_value(self, value: Any) -> str:
        normalized = value
        if normalized is None:
            normalized = "__cc_null__"
        try:
            serialized = json.dumps(normalized, sort_keys=True, ensure_ascii=False, default=self._json_default)
        except (TypeError, ValueError):
            serialized = str(normalized)
        return hashlib.sha256(serialized.encode("utf-8")).hexdigest()

    def _compute_relative_frequency(self, count: int | None, total: int | None) -> float | None:
        if count is None or total in (None, 0):
            return None
        try:
            ratio = float(count) / float(total)
        except (TypeError, ZeroDivisionError):
            return None
        if math.isnan(ratio) or math.isinf(ratio):
            return None
        return max(ratio, 0.0)

    # ------------------------------------------------------------------
    # Anomaly helpers
    # ------------------------------------------------------------------
    def _build_anomaly_rows(
        self,
        table_context: Mapping[str, Any],
        column_id: str | None,
        column_name: str | None,
        anomalies: Sequence[Mapping[str, Any]],
    ) -> list[dict[str, Any]]:
        rows: list[dict[str, Any]] = []
        table_label = self._table_label(table_context)
        for entry in anomalies or []:
            if not isinstance(entry, Mapping):
                continue
            row = {
                "anomaly_result_id": uuid.uuid4().hex,
                "profile_run_id": self._profile_run_id,
                "table_id": table_context.get("table_id"),
                "column_id": column_id,
                "table_name": table_label,
                "column_name": column_name or self._first_text(entry, "column_name", "columnName"),
                "anomaly_type_id": self._first_text(
                    entry,
                    "anomaly_type_id",
                    "anomaly_type",
                    "type",
                    "code",
                ),
                "severity": self._first_text(entry, "severity", "level"),
                "likelihood": self._first_text(entry, "likelihood", "probability"),
                "detail": self._first_text(entry, "detail", "description", "message"),
                "pii_risk": self._first_text(entry, "pii_risk", "piiRisk"),
                "dq_dimension": self._first_text(entry, "dq_dimension", "dqDimension", "dimension"),
                "detected_at": self._coerce_timestamp(
                    self._first_text(entry, "detected_at", "detectedAt", "timestamp")
                ),
                "dismissed": self._coerce_bool(entry.get("dismissed")) or False,
                "dismissed_by": self._first_text(entry, "dismissed_by", "dismissedBy"),
                "dismissed_at": self._coerce_timestamp(entry.get("dismissed_at") or entry.get("dismissedAt")),
            }
            rows.append(row)
        return rows

    def _table_label(self, table_context: Mapping[str, Any]) -> str | None:
        table_name = table_context.get("table_name")
        schema_name = table_context.get("schema_name")
        if schema_name:
            return f"{schema_name}.{table_name}"
        return table_name

    def _extract_columns(self, table_entry: Mapping[str, Any]) -> list[Mapping[str, Any]]:
        for key in ("columns", "column_profiles", "columnProfiles", "columnsProfiled"):
            value = table_entry.get(key)
            if isinstance(value, list):
                return [item for item in value if isinstance(item, Mapping)]
            if isinstance(value, Mapping):
                return [item for item in value.values() if isinstance(item, Mapping)]
        return []

    def _extract_anomalies(self, entry: Mapping[str, Any]) -> list[Mapping[str, Any]]:
        anomalies: list[Mapping[str, Any]] = []
        for key in ("anomalies", "anomaly_results", "anomalyResults", "findings"):
            value = entry.get(key)
            if isinstance(value, list):
                anomalies.extend(item for item in value if isinstance(item, Mapping))
        return anomalies

    # ------------------------------------------------------------------
    # Shared coercion helpers
    # ------------------------------------------------------------------
    def _first_value(self, primary: Mapping[str, Any] | None, secondary: Mapping[str, Any] | None, *keys: str) -> Any:
        for key in keys:
            for source in (primary, secondary):
                if not isinstance(source, Mapping):
                    continue
                if key in source:
                    value = source[key]
                    if value not in (None, ""):
                        return value
        return None

    def _first_text(self, mapping: Mapping[str, Any] | None, *keys: str) -> str | None:
        if not isinstance(mapping, Mapping):
            return None
        for key in keys:
            value = mapping.get(key)
            if isinstance(value, str):
                text = value.strip()
                if text:
                    return text
        return None

    def _merge_metrics(self, entry: Mapping[str, Any]) -> Mapping[str, Any]:
        metrics: dict[str, Any] = {}
        for key in ("metrics", "summary", "stats"):
            value = entry.get(key)
            if isinstance(value, Mapping):
                for nested_key, nested_value in value.items():
                    if nested_key not in metrics:
                        metrics[nested_key] = nested_value
        return metrics

    def _extract_sequence(self, entry: Mapping[str, Any], *keys: str) -> list[Any]:
        for key in keys:
            value = entry.get(key)
            if isinstance(value, list):
                return [self._sanitize_sequence_item(item) for item in value if item is not None]
            if isinstance(value, Mapping):
                nested = value.get("values") or value.get("buckets")
                if isinstance(nested, list):
                    return [self._sanitize_sequence_item(item) for item in nested if item is not None]
        return []

    def _sanitize_sequence_item(self, item: Any) -> Any:
        if isinstance(item, (str, int, float, bool)):
            return item
        if isinstance(item, Mapping):
            return {key: self._sanitize_sequence_item(value) for key, value in item.items()}
        if isinstance(item, Sequence) and not isinstance(item, (str, bytes)):
            return [self._sanitize_sequence_item(value) for value in item]
        return item

    def _qualified_column_name(self, schema_name: str | None, table_name: str | None, column_name: str) -> str:
        parts = [part for part in (schema_name, table_name, column_name) if part]
        return ".".join(parts)

    def _fallback_table_id(self, schema_name: str | None, table_name: str) -> str:
        if schema_name:
            return f"{schema_name}.{table_name}"
        return table_name

    def _fallback_column_id(
        self,
        table_id: str | None,
        *,
        schema_name: str | None,
        table_name: str | None,
        column_name: str,
    ) -> str:
        if table_id:
            base = table_id
        else:
            base = self._fallback_table_id(schema_name, table_name or column_name)
        return f"{base}::{column_name}".lower()

    def _serialize_json(self, value: Any) -> str | None:
        if value is None:
            return None
        if isinstance(value, str):
            text = value.strip()
            return text or None
        try:
            return json.dumps(value, ensure_ascii=False, default=self._json_default)
        except (TypeError, ValueError):
            return json.dumps(str(value), ensure_ascii=False)

    def _json_default(self, value: Any) -> Any:
        if isinstance(value, (datetime, date)):
            return value.isoformat()
        if isinstance(value, Decimal):
            return float(value)
        return str(value)

    def _coerce_int(self, value: Any) -> int | None:
        if value is None:
            return None
        if isinstance(value, bool):
            return int(value)
        if isinstance(value, int):
            return value
        if isinstance(value, (float, Decimal)):
            if math.isnan(float(value)):
                return None
            return int(float(value))
        if isinstance(value, str):
            text = value.strip()
            if not text:
                return None
            try:
                if "." in text:
                    return int(float(text))
                return int(text)
            except ValueError:
                return None
        return None

    def _coerce_float(self, value: Any) -> float | None:
        if value is None:
            return None
        if isinstance(value, bool):
            return float(value)
        if isinstance(value, (int, float, Decimal)):
            numeric = float(value)
            if math.isnan(numeric):
                return None
            return numeric
        if isinstance(value, str):
            text = value.strip()
            if not text:
                return None
            try:
                numeric = float(text)
            except ValueError:
                return None
            if math.isnan(numeric):
                return None
            return numeric
        return None

    def _coerce_bool(self, value: Any) -> bool | None:
        if value is None:
            return None
        if isinstance(value, bool):
            return value
        if isinstance(value, (int, float)):
            return bool(value)
        if isinstance(value, str):
            lowered = value.strip().lower()
            if lowered in {"true", "t", "yes", "1"}:
                return True
            if lowered in {"false", "f", "no", "0"}:
                return False
        return None

    def _coerce_timestamp(self, value: Any) -> datetime | None:
        if value is None:
            return None
        if isinstance(value, datetime):
            return value
        if isinstance(value, (int, float, Decimal)):
            try:
                return datetime.fromtimestamp(float(value), tz=timezone.utc)
            except (OverflowError, OSError, ValueError):
                return None
        if isinstance(value, str):
            text = value.strip()
            if not text:
                return None
            normalized = text.replace("Z", "+00:00") if text.endswith("Z") else text
            try:
                parsed = datetime.fromisoformat(normalized)
                if parsed.tzinfo is None:
                    parsed = parsed.replace(tzinfo=timezone.utc)
                return parsed
            except ValueError:
                for fmt in (
                    "%Y-%m-%d %H:%M:%S",
                    "%Y-%m-%dT%H:%M:%S",
                    "%Y-%m-%d %H:%M:%S.%f",
                    "%Y-%m-%dT%H:%M:%S.%f",
                ):
                    try:
                        parsed = datetime.strptime(text, fmt)
                        return parsed.replace(tzinfo=timezone.utc)
                    except ValueError:
                        continue
        return None

    def _coerce_date(self, value: Any) -> date | None:
        timestamp = self._coerce_timestamp(value)
        if timestamp is not None:
            return timestamp.date()
        return None


def build_metadata_frames(
    spark_session: Any,
    payload: Any,
    *,
    profile_run_id: str,
    table_group_id: str | None,
    summary: Mapping[str, Any] | None = None,
) -> tuple[dict[str, Any], dict[str, int]]:
    """Return Spark DataFrames keyed by metadata target table."""

    builder = ProfilingPayloadFrameBuilder(
        payload,
        profile_run_id=profile_run_id,
        table_group_id=table_group_id,
        summary=summary,
    )
    row_sets, counts = builder.build_rows()
    if T is None:
        raise RuntimeError("pyspark must be installed to build metadata frames.")

    frames: dict[str, Any] = {}
    frames["profile_results_df"] = spark_session.createDataFrame(
        row_sets["profile_results_df"], schema=PROFILE_RESULTS_SCHEMA
    )
    frames["profile_columns_df"] = spark_session.createDataFrame(
        row_sets["profile_columns_df"], schema=PROFILE_COLUMNS_SCHEMA
    )
    frames["profile_column_values_df"] = spark_session.createDataFrame(
        row_sets["profile_column_values_df"], schema=PROFILE_COLUMN_VALUES_SCHEMA
    )
    frames["profile_anomalies_df"] = spark_session.createDataFrame(
        row_sets["profile_anomalies_df"], schema=PROFILE_ANOMALIES_SCHEMA
    )
    frames["table_characteristics_df"] = spark_session.createDataFrame(
        row_sets["table_characteristics_df"], schema=TABLE_CHARACTERISTICS_SCHEMA
    )
    frames["column_characteristics_df"] = spark_session.createDataFrame(
        row_sets["column_characteristics_df"], schema=COLUMN_CHARACTERISTICS_SCHEMA
    )
    return frames, counts


__all__ = [
    "ProfilingPayloadFrameBuilder",
    "build_metadata_frames",
    "PROFILE_RESULTS_SCHEMA",
    "PROFILE_COLUMNS_SCHEMA",
    "PROFILE_COLUMN_VALUES_SCHEMA",
    "PROFILE_ANOMALIES_SCHEMA",
    "TABLE_CHARACTERISTICS_SCHEMA",
    "COLUMN_CHARACTERISTICS_SCHEMA",
]
