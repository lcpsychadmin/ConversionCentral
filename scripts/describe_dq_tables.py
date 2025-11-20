"""Utility script to inspect Databricks profiling metadata tables."""

from __future__ import annotations

import os
import sys
from pathlib import Path
from contextlib import contextmanager

from sqlalchemy import create_engine, text


# Avoid attempting to connect to the primary Postgres database when we only need
# Databricks settings for this inspection helper. When running inside the deployed
# container we want the real DATABASE_URL so only fall back to SQLite locally.
_database_url = os.environ.get("DATABASE_URL")
if not _database_url:
    os.environ["DATABASE_URL"] = "sqlite:///:memory:"
elif _database_url.startswith("postgres://"):
    os.environ["DATABASE_URL"] = _database_url.replace("postgres://", "postgresql+psycopg2://", 1)

_ROOT_DIR = Path(__file__).resolve().parents[1]
if str(_ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(_ROOT_DIR))

_DUMP_PATH = os.environ.get("DQ_DESCRIBE_DUMP")
_DUMP_FILE = Path(_DUMP_PATH) if _DUMP_PATH else None
if _DUMP_FILE is not None:
    try:
        _DUMP_FILE.parent.mkdir(parents=True, exist_ok=True)
    except OSError:
        _DUMP_FILE = None


def _emit(message: str = "") -> None:
    print(message)
    if _DUMP_FILE is None:
        return
    try:
        with _DUMP_FILE.open("a", encoding="utf-8") as handle:
            handle.write(message + "\n")
    except OSError:
        pass


_emit("Starting Databricks profiling metadata inspection helper.")
_emit(f"__name__ = {__name__}")

from app.ingestion.engine import get_ingestion_connection_params
from app.services.databricks_sql import build_sqlalchemy_url
from app.services.data_quality_metadata import _format_table


def _format_row(row: tuple[str | None, str | None, str | None]) -> str:
    col_name = (row[0] or "").strip()
    data_type = (row[1] or "").strip()
    comment = (row[2] or "").strip()
    return f"{col_name:30} {data_type:20} {comment}"


@contextmanager
def _connect():
    _emit("Resolving Databricks connection parameters via get_ingestion_connection_params().")
    params = get_ingestion_connection_params()
    _emit("Successfully resolved Databricks connection parameters; creating engine.")
    engine = create_engine(
        build_sqlalchemy_url(params),
        pool_pre_ping=True,
        connect_args={"timeout": 30},
    )
    try:
        with engine.connect() as connection:
            yield connection, params
    finally:
        engine.dispose()


def _describe_table(connection, fully_qualified: str) -> None:
    _emit()
    _emit(f"=== {fully_qualified} ===")
    rows = connection.execute(text(f"DESCRIBE EXTENDED {fully_qualified}"))
    for row in rows:
        _emit(_format_row(row))


def main() -> None:
    _emit("Entering main().")
    with _connect() as (connection, params):
        _emit("Connected to Databricks; preparing DESCRIBE statements.")
        schema = (params.data_quality_schema or "").strip()
        if not schema:
            _emit("Databricks data quality schema is not configured.")
            return
        _emit(
            "Inspecting data quality schema '%s' in catalog '%s'."
            % (schema, params.catalog or "<default>")
        )
        table_groups = _format_table(params.catalog, schema, "dq_table_groups")
        profiles = _format_table(params.catalog, schema, "dq_profiles")
        _describe_table(connection, table_groups)
        _describe_table(connection, profiles)
    _emit("Finished main().")


if __name__ == "__main__":
    main()
