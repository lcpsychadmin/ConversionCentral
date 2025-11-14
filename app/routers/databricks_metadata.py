from __future__ import annotations

from fastapi import APIRouter

from app.constants.databricks import WAREHOUSE_DATA_TYPES

router = APIRouter(prefix="/databricks", tags=["Databricks Settings"])


@router.get("/data-types")
def list_data_types() -> list[dict[str, object]]:
    """Return reference metadata for Databricks SQL Warehouse data types."""
    return WAREHOUSE_DATA_TYPES
