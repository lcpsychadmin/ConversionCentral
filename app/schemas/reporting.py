from __future__ import annotations

from enum import Enum
from typing import Any, List, Optional
from uuid import UUID

from pydantic import BaseModel, Field, validator


class ReportSortDirection(str, Enum):
    NONE = "none"
    ASC = "asc"
    DESC = "desc"


class ReportAggregateFn(str, Enum):
    GROUP_BY = "groupBy"
    SUM = "sum"
    AVG = "avg"
    MIN = "min"
    MAX = "max"
    COUNT = "count"
    FIRST = "first"
    LAST = "last"
    EXPRESSION = "expression"
    WHERE = "where"


class ReportJoinType(str, Enum):
    INNER = "inner"
    LEFT = "left"
    RIGHT = "right"


class ReportDesignerTablePlacement(BaseModel):
    table_id: UUID = Field(..., alias="tableId")
    label: str
    physical_name: str = Field(..., alias="physicalName")
    schema_name: Optional[str] = Field(None, alias="schemaName")
    alias: Optional[str] = None
    position: dict[str, Any]


class ReportDesignerJoin(BaseModel):
    id: str
    source_table_id: UUID = Field(..., alias="sourceTableId")
    target_table_id: UUID = Field(..., alias="targetTableId")
    source_field_id: Optional[UUID] = Field(None, alias="sourceFieldId")
    target_field_id: Optional[UUID] = Field(None, alias="targetFieldId")
    join_type: ReportJoinType = Field(ReportJoinType.INNER, alias="joinType")


class ReportDesignerColumn(BaseModel):
    order: int
    field_id: UUID = Field(..., alias="fieldId")
    field_name: str = Field(..., alias="fieldName")
    field_description: Optional[str] = Field(None, alias="fieldDescription")
    table_id: UUID = Field(..., alias="tableId")
    table_name: Optional[str] = Field(None, alias="tableName")
    show: bool
    sort: ReportSortDirection
    aggregate: Optional[ReportAggregateFn] = None
    criteria: List[str]

    @validator("criteria", pre=True)
    def default_criteria(cls, value: Optional[List[str]]) -> List[str]:
        return value or []


class ReportDesignerDefinition(BaseModel):
    tables: List[ReportDesignerTablePlacement]
    joins: List[ReportDesignerJoin]
    columns: List[ReportDesignerColumn]
    criteria_row_count: int = Field(..., alias="criteriaRowCount", ge=0)
    grouping_enabled: bool = Field(..., alias="groupingEnabled")


class ReportPreviewRequest(BaseModel):
    definition: ReportDesignerDefinition
    limit: int = Field(100, ge=1, le=500)


class ReportPreviewResponse(BaseModel):
    sql: str
    limit: int
    row_count: int = Field(..., alias="rowCount")
    duration_ms: float = Field(..., alias="durationMs")
    columns: List[str]
    rows: List[dict[str, Any]]


__all__ = [
    "ReportAggregateFn",
    "ReportDesignerColumn",
    "ReportDesignerDefinition",
    "ReportDesignerJoin",
    "ReportDesignerTablePlacement",
    "ReportJoinType",
    "ReportPreviewRequest",
    "ReportPreviewResponse",
    "ReportSortDirection",
]
