from __future__ import annotations

from app.config import get_settings

DATABRICKS_BACKEND = "databricks"
LOCAL_BACKEND = "local"


def get_metadata_backend() -> str:
    """Return the configured metadata backend name."""

    try:
        backend = get_settings().data_quality_metadata_backend
    except Exception:  # pragma: no cover - settings may not be initialized yet
        return DATABRICKS_BACKEND

    normalized = (backend or DATABRICKS_BACKEND).strip().lower()
    if normalized not in {DATABRICKS_BACKEND, LOCAL_BACKEND}:
        return DATABRICKS_BACKEND
    return normalized


def is_local_metadata_backend() -> bool:
    return get_metadata_backend() == LOCAL_BACKEND
