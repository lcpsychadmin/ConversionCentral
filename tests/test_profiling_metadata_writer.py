from __future__ import annotations

from types import SimpleNamespace
from typing import Any, Sequence

import pytest

import app.databricks_profiling.metadata_writer as metadata_writer
from app.databricks_profiling.metadata_writer import ProfilingMetadataWriter


class StubCatalog:
    def __init__(self) -> None:
        self.dropped_views: list[str] = []

    def dropTempView(self, name: str) -> None:
        self.dropped_views.append(name)


class StubSpark:
    def __init__(self) -> None:
        self.sql_calls: list[str] = []
        self.catalog = StubCatalog()

    def sql(self, statement: str) -> None:
        self.sql_calls.append(statement.strip())


class StubDataFrame:
    def __init__(self, rows: Sequence[dict[str, Any]], columns: Sequence[str]) -> None:
        self._rows = list(rows)
        self.columns = list(columns)
        self.temp_views: list[str] = []

    def count(self) -> int:
        return len(self._rows)

    def createOrReplaceTempView(self, name: str) -> None:
        self.temp_views.append(name)


def _fixed_uuid_sequence(values: Sequence[str]):
    iterator = iter(values)

    def _next_uuid():
        value = next(iterator)
        return SimpleNamespace(hex=value)

    return _next_uuid


def test_merge_dataframe_generates_merge_and_operation(monkeypatch: pytest.MonkeyPatch) -> None:
    spark = StubSpark()
    df = StubDataFrame(
        rows=[
            {"profile_run_id": "run-1", "column_name": "id", "data_type": "string"},
            {"profile_run_id": "run-2", "column_name": "id", "data_type": "string"},
        ],
        columns=["profile_run_id", "column_name", "data_type"],
    )

    monkeypatch.setattr(
        metadata_writer.uuid,
        "uuid4",
        _fixed_uuid_sequence(["view-123", "op-456"]),
        raising=False,
    )

    writer = ProfilingMetadataWriter(
        spark,
        schema="dq",
        catalog="sandbox",
        profile_run_id="profile-1",
    )

    row_count = writer.merge_dataframe(
        df,
        target_table="dq_profile_columns",
        key_columns=["profile_run_id", "column_name"],
    )

    assert row_count == 2
    assert spark.sql_calls[0] == (
        "MERGE INTO `sandbox`.`dq`.`dq_profile_columns` AS target USING _cc_dq_profile_columns_view-123 AS source "
        "ON target.`profile_run_id` = source.`profile_run_id` AND target.`column_name` = source.`column_name` "
        "WHEN MATCHED THEN UPDATE SET target.`profile_run_id` = source.`profile_run_id`, "
        "target.`column_name` = source.`column_name`, target.`data_type` = source.`data_type` "
        "WHEN NOT MATCHED THEN INSERT (`profile_run_id`, `column_name`, `data_type`) "
        "VALUES (source.`profile_run_id`, source.`column_name`, source.`data_type`)"
    )
    assert "INSERT INTO `sandbox`.`dq`.`dq_profile_operations`" in spark.sql_calls[1]
    assert "'profile-1'" in spark.sql_calls[1]
    assert "'op-456'" in spark.sql_calls[1]
    assert "'`sandbox`.`dq`.`dq_profile_columns`'" in spark.sql_calls[1]

    assert spark.catalog.dropped_views == ["_cc_dq_profile_columns_view-123"]


def test_merge_dataframe_skips_empty_frames(monkeypatch: pytest.MonkeyPatch) -> None:
    spark = StubSpark()
    df = StubDataFrame(rows=[], columns=["profile_run_id", "column_name"])

    monkeypatch.setattr(
        metadata_writer.uuid,
        "uuid4",
        _fixed_uuid_sequence(["operation-empty"]),
        raising=False,
    )

    writer = ProfilingMetadataWriter(
        spark,
        schema="dq",
        profile_run_id="profile-1",
    )

    row_count = writer.merge_dataframe(
        df,
        target_table="dq_profile_columns",
        key_columns=["profile_run_id", "column_name"],
    )

    assert row_count == 0
    assert len(spark.sql_calls) == 1  # only the operations insert is executed
    assert "'skipped'" in spark.sql_calls[0]
    assert "'operation-empty'" in spark.sql_calls[0]


def test_merge_dataframe_validates_keys(monkeypatch: pytest.MonkeyPatch) -> None:
    spark = StubSpark()
    df = StubDataFrame(rows=[{"profile_run_id": "run-1"}], columns=["profile_run_id"])

    writer = ProfilingMetadataWriter(
        spark,
        schema="dq",
        profile_run_id="profile-1",
    )

    with pytest.raises(ValueError, match="Merge keys missing"):
        writer.merge_dataframe(
            df,
            target_table="dq_profile_columns",
            key_columns=["profile_run_id", "column_name"],
        )
