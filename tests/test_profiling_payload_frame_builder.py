"""Tests for transforming profiling payloads into metadata frame rows."""

from __future__ import annotations

from datetime import datetime, timezone

from app.databricks_profiling.frame_builder import ProfilingPayloadFrameBuilder


def _sample_payload():
    detected_at = datetime(2025, 11, 16, 12, 0, tzinfo=timezone.utc).isoformat()
    return {
        "tables": [
            {
                "table_id": "tbl-1",
                "table_group_id": "tg-1",
                "schema_name": "analytics",
                "table_name": "orders",
                "metrics": {"row_count": 250},
                "anomalies": [
                    {
                        "anomaly_type": "table_notice",
                        "severity": "info",
                        "description": "General table anomaly",
                        "detected_at": detected_at,
                    }
                ],
                "columns": [
                    {
                        "column_id": "col-1",
                        "column_name": "total",
                        "data_type": "DECIMAL(10,2)",
                        "general_type": "numeric",
                        "ordinal_position": 1,
                        "metrics": {
                            "row_count": 250,
                            "null_count": 10,
                            "distinct_count": 15,
                            "min": "1",
                            "max": "999",
                            "avg": 42.5,
                            "median": 21.0,
                            "p95": 200.0,
                        },
                        "top_values": [
                            {"value": "alpha", "count": 5, "percentage": 0.2},
                        ],
                        "anomalies": [
                            {
                                "anomaly_type_id": "null_density",
                                "severity": "high",
                                "description": "Null ratio increased",
                                "detected_at": detected_at,
                            }
                        ],
                    }
                ],
            }
        ]
    }


def test_builder_emits_rows_for_payload():
    payload = _sample_payload()
    builder = ProfilingPayloadFrameBuilder(payload, profile_run_id="run-123", table_group_id="tg-1")

    rows, counts = builder.build_rows()

    assert counts["profile_columns_df"] == 1
    assert counts["profile_results_df"] == 1
    assert counts["profile_anomalies_df"] == 2  # one table anomaly + one column anomaly

    column_row = rows["profile_columns_df"][0]
    assert column_row["column_name"] == "total"
    assert column_row["row_count"] == 250
    assert column_row["null_count"] == 10

    result_row = rows["profile_results_df"][0]
    assert result_row["table_name"] == "orders"
    assert result_row["top_values_json"] is not None

    table_row = rows["table_characteristics_df"][0]
    assert table_row["table_id"] == "tbl-1"
    assert table_row["column_count"] == 1

    column_char_row = rows["column_characteristics_df"][0]
    assert column_char_row["column_id"] == "col-1"
    assert column_char_row["latest_anomaly_ct"] == 1


def test_builder_handles_payload_without_tables():
    builder = ProfilingPayloadFrameBuilder({}, profile_run_id="run-empty", table_group_id=None)
    rows, counts = builder.build_rows()

    assert all(count == 0 for count in counts.values())
    assert all(not data for data in rows.values())
