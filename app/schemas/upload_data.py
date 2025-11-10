from __future__ import annotations

from enum import Enum
from typing import Optional
from uuid import UUID

from pydantic import BaseModel

from app.schemas.entities import DataWarehouseTarget


class UploadTableMode(str, Enum):
    CREATE = "create"
    REPLACE = "replace"


class UploadDataColumn(BaseModel):
    original_name: str
    field_name: str
    inferred_type: str


class UploadDataPreviewResponse(BaseModel):
    columns: list[UploadDataColumn]
    sample_rows: list[list[Optional[str]]]
    total_rows: int


class UploadDataColumnOverride(BaseModel):
    field_name: str
    target_name: Optional[str] = None
    target_type: Optional[str] = None
    exclude: Optional[bool] = None


class UploadDataCreateResponse(BaseModel):
    table_name: str
    schema_name: Optional[str] = None
    catalog: Optional[str] = None
    rows_inserted: int
    target_warehouse: DataWarehouseTarget
    table_id: Optional[UUID] = None
    constructed_table_id: Optional[UUID] = None
    data_definition_id: Optional[UUID] = None
    data_definition_table_id: Optional[UUID] = None
