"""Seed or refresh Databricks SQL settings from environment variables.

This helper copies the current Databricks workspace/warehouse configuration that the
application already reads from environment variables into the `databricks_sql_settings`
table. Persisting the record enables services such as TestGen and profiling to reuse
the same credentials without requiring manual UI input.
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import Any

from sqlalchemy import select
from sqlalchemy.exc import SQLAlchemyError

# Ensure project root is importable when the script is executed directly.
PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from app.config import get_settings  # noqa: E402  (import after path setup)
from app.database import SessionLocal  # noqa: E402
from app.models import DatabricksSqlSetting  # noqa: E402

REQUIRED_FIELDS = (
    "workspace_host",
    "http_path",
    "access_token",
)


def _normalize(value: str | None) -> str | None:
    if not isinstance(value, str):
        return None
    candidate = value.strip()
    return candidate or None


def _normalize_lower(value: str | None) -> str | None:
    normalized = _normalize(value)
    return normalized.lower() if normalized else None


def _collect_payload() -> dict[str, Any]:
    settings = get_settings()
    ingestion_batch_rows = settings.databricks_ingestion_batch_rows
    batch_rows = ingestion_batch_rows if isinstance(ingestion_batch_rows, int) and ingestion_batch_rows > 0 else None

    payload: dict[str, Any] = {
        "workspace_host": _normalize(settings.databricks_host),
        "http_path": _normalize(settings.databricks_http_path),
        "access_token": _normalize(settings.databricks_token),
        "catalog": _normalize(settings.databricks_catalog),
        "schema_name": _normalize(settings.databricks_schema),
        "constructed_schema": _normalize(settings.databricks_constructed_schema),
        "data_quality_schema": _normalize(settings.databricks_data_quality_schema) or "dq_metadata",
        "data_quality_storage_format": _normalize_lower(settings.databricks_data_quality_storage_format) or "delta",
        "data_quality_auto_manage_tables": bool(settings.databricks_data_quality_auto_manage_tables),
        "profiling_policy_id": _normalize(settings.databricks_profile_policy_id),
        "profiling_notebook_path": _normalize(settings.databricks_profile_notebook_path),
        "ingestion_batch_rows": batch_rows,
        "warehouse_name": None,
        "ingestion_method": _normalize_lower(settings.databricks_ingestion_method) or "sql",
        "spark_compute": _normalize_lower(settings.databricks_spark_compute),
    }
    return payload


def _mask_token(token: str | None) -> str | None:
    if not token:
        return None
    if len(token) <= 8:
        return "*" * len(token)
    return f"{token[:4]}***{token[-4:]}"


def _summarize_payload(payload: dict[str, Any]) -> dict[str, Any]:
    summary = dict(payload)
    summary["access_token"] = _mask_token(summary.get("access_token"))
    return summary


def _load_active_record(session) -> DatabricksSqlSetting | None:
    stmt = (
        select(DatabricksSqlSetting)
        .order_by(DatabricksSqlSetting.is_active.desc(), DatabricksSqlSetting.updated_at.desc())
        .limit(1)
    )
    return session.execute(stmt).scalars().first()


def _apply_payload(record: DatabricksSqlSetting, payload: dict[str, Any]) -> None:
    for field, value in payload.items():
        setattr(record, field, value)
    record.is_active = True


def sync_settings(display_name: str | None = None, *, dry_run: bool = False) -> int:
    payload = _collect_payload()
    missing = [field for field in REQUIRED_FIELDS if not payload.get(field)]
    if missing:
        print(
            "Unable to sync Databricks settings; missing required values for: %s"
            % ", ".join(missing)
        )
        print("Ensure DATABRICKS_HOST, DATABRICKS_HTTP_PATH, and DATABRICKS_TOKEN are set.")
        return 1

    with SessionLocal() as session:
        try:
            record = _load_active_record(session)
        except SQLAlchemyError as exc:
            print("Failed to query databricks_sql_settings: %s" % exc)
            print("Run database migrations and ensure the table exists before retrying.")
            return 1

        created = False
        if record is None:
            record = DatabricksSqlSetting(
                display_name=display_name or "Primary Warehouse",
                workspace_host=payload["workspace_host"],
                http_path=payload["http_path"],
            )
            session.add(record)
            created = True
        else:
            if display_name:
                record.display_name = display_name.strip()

        _apply_payload(record, payload)
        action = "created" if created else "updated"
        summary = _summarize_payload(payload)
        print(f"Prepared payload ({action}): {summary}")

        if dry_run:
            session.rollback()
            print("Dry-run requested; changes were not committed.")
            return 0

        try:
            session.commit()
        except SQLAlchemyError as exc:
            session.rollback()
            print("Failed to %s Databricks setting: %s" % (action, exc))
            return 1

    print(f"Successfully {action} Databricks SQL setting '{record.display_name}'.")
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Persist the current Databricks workspace/warehouse configuration into "
            "the databricks_sql_settings table."
        )
    )
    parser.add_argument(
        "--display-name",
        help="Optional display_name value stored with the setting (defaults to 'Primary Warehouse').",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print the payload without committing database changes.",
    )

    args = parser.parse_args(argv)
    return sync_settings(args.display_name, dry_run=args.dry_run)


if __name__ == "__main__":
    raise SystemExit(main())
