from __future__ import annotations

from datetime import datetime
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


class ReportStatus(str, Enum):
    DRAFT = "draft"
    PUBLISHED = "published"


class ReportCreateRequest(BaseModel):
    name: str = Field(..., max_length=200)
    description: Optional[str] = Field(None, max_length=2000)
    definition: ReportDesignerDefinition
    status: ReportStatus = Field(default=ReportStatus.DRAFT)
    product_team_id: Optional[UUID] = Field(None, alias="productTeamId")
    data_object_id: Optional[UUID] = Field(None, alias="dataObjectId")

    class Config:
        allow_population_by_field_name = True
        anystr_strip_whitespace = True


class ReportUpdateRequest(BaseModel):
    name: Optional[str] = Field(None, max_length=200)
    description: Optional[str] = Field(None, max_length=2000)
    definition: Optional[ReportDesignerDefinition] = None
    status: Optional[ReportStatus] = None
    product_team_id: Optional[UUID] = Field(None, alias="productTeamId")
    data_object_id: Optional[UUID] = Field(None, alias="dataObjectId")

    class Config:
        allow_population_by_field_name = True
        anystr_strip_whitespace = True


class ReportPublishRequest(BaseModel):
    name: Optional[str] = Field(None, max_length=200)
    description: Optional[str] = Field(None, max_length=2000)
    definition: Optional[ReportDesignerDefinition] = None
    product_team_id: UUID = Field(..., alias="productTeamId")
    data_object_id: UUID = Field(..., alias="dataObjectId")

    class Config:
        allow_population_by_field_name = True
        anystr_strip_whitespace = True


class ReportListItem(BaseModel):
    id: UUID
    name: str
    description: Optional[str] = None
    status: ReportStatus
    created_at: datetime = Field(..., alias="createdAt")
    updated_at: datetime = Field(..., alias="updatedAt")
    published_at: Optional[datetime] = Field(None, alias="publishedAt")
    product_team_id: Optional[UUID] = Field(None, alias="productTeamId")
    product_team_name: Optional[str] = Field(None, alias="productTeamName")
    data_object_id: Optional[UUID] = Field(None, alias="dataObjectId")
    data_object_name: Optional[str] = Field(None, alias="dataObjectName")

    class Config:
        allow_population_by_field_name = True


class ReportResponse(ReportListItem):
    definition: ReportDesignerDefinition


class ReportDatasetResponse(BaseModel):
    report_id: UUID = Field(..., alias="reportId")
    name: str
    limit: int
    row_count: int = Field(..., alias="rowCount")
    generated_at: datetime = Field(..., alias="generatedAt")
    columns: List[str]
    rows: List[dict[str, Any]]

    class Config:
        allow_population_by_field_name = True


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
    "ReportStatus",
    "ReportCreateRequest",
    "ReportUpdateRequest",
    "ReportPublishRequest",
    "ReportListItem",
    "ReportResponse",
    "ReportDatasetResponse",
]
