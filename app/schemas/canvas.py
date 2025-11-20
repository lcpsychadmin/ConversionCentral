from __future__ import annotations

from typing import Any, Dict, List, Optional, Union

from pydantic import BaseModel
from typing import Literal

CanvasGraphResourceType = Union[
    Literal["source", "model", "seed", "snapshot", "exposure", "metric"],
    str,
]
CanvasGraphEdgeType = Union[
    Literal["ref", "source", "exposure", "metric"],
    str,
]
CanvasGraphLayer = Union[
    Literal["source", "staging", "intermediate", "mart", "analytics", "exposure"],
    str,
]
CanvasGraphMaterialization = Union[
    Literal["view", "table", "incremental", "ephemeral", "seed", "snapshot"],
    str,
]
CanvasGraphAccess = Union[Literal["public", "protected", "private"], str]
CanvasGraphJoinType = Union[
    Literal["inner", "left", "right", "full", "cross", "anti", "semi"],
    str,
]


class CanvasGraphPosition(BaseModel):
    x: float
    y: float


class CanvasGraphSize(BaseModel):
    width: float
    height: float


class CanvasGraphHookConfig(BaseModel):
    pre: Optional[List[str]] = None
    post: Optional[List[str]] = None


class CanvasGraphIncrementalConfig(BaseModel):
    strategy: Optional[str] = None
    unique_key: Optional[Union[str, List[str]]] = None
    partition_by: Optional[Union[str, List[str]]] = None
    cluster_by: Optional[List[str]] = None

    class Config:
        fields = {
            "unique_key": "uniqueKey",
            "partition_by": "partitionBy",
            "cluster_by": "clusterBy",
        }


class CanvasGraphColumnTest(BaseModel):
    name: str
    arguments: Optional[Dict[str, Any]] = None


CanvasGraphColumnTests = Dict[str, List[CanvasGraphColumnTest]]


class CanvasGraphModelTest(BaseModel):
    name: str
    arguments: Optional[Dict[str, Any]] = None


class CanvasGraphNodeExposure(BaseModel):
    name: str
    type: Literal["dashboard", "notebook", "ml", "analysis"]
    url: Optional[str] = None
    maturity: Optional[Literal["low", "medium", "high"]] = None
    description: Optional[str] = None
    owner: Optional[str] = None


class CanvasGraphNodeMetric(BaseModel):
    name: str
    label: Optional[str] = None
    description: Optional[str] = None
    calculation: Optional[str] = None
    meta: Optional[Dict[str, Any]] = None


class CanvasGraphColumn(BaseModel):
    id: str
    name: str
    data_type: Optional[str] = None
    description: Optional[str] = None
    tests: Optional[List[CanvasGraphColumnTest]] = None
    meta: Optional[Dict[str, Any]] = None

    class Config:
        fields = {
            "data_type": "dataType",
        }


class CanvasGraphNodeConfig(BaseModel):
    materialization: Optional[CanvasGraphMaterialization] = None
    access: Optional[CanvasGraphAccess] = None
    tags: Optional[List[str]] = None
    owner: Optional[str] = None
    path: Optional[str] = None
    sql: Optional[str] = None
    description: Optional[str] = None
    meta: Optional[Dict[str, Any]] = None
    hooks: Optional[CanvasGraphHookConfig] = None
    incremental: Optional[CanvasGraphIncrementalConfig] = None
    column_tests: Optional[CanvasGraphColumnTests] = None
    tests: Optional[List[CanvasGraphModelTest]] = None
    exposures: Optional[List[CanvasGraphNodeExposure]] = None
    metrics: Optional[List[CanvasGraphNodeMetric]] = None

    class Config:
        fields = {
            "column_tests": "columnTests",
        }


class CanvasGraphNodeOrigin(BaseModel):
    data_definition_table_id: Optional[str] = None
    system_id: Optional[str] = None
    schema_name: Optional[str] = None
    table_name: Optional[str] = None
    is_constructed: Optional[bool] = None

    class Config:
        fields = {
            "data_definition_table_id": "dataDefinitionTableId",
            "system_id": "systemId",
            "schema_name": "schemaName",
            "table_name": "tableName",
            "is_constructed": "isConstructed",
        }


class CanvasGraphNode(BaseModel):
    id: str
    name: str
    label: str
    resource_type: CanvasGraphResourceType
    position: CanvasGraphPosition
    size: Optional[CanvasGraphSize] = None
    layer: Optional[CanvasGraphLayer] = None
    description: Optional[str] = None
    origin: Optional[CanvasGraphNodeOrigin] = None
    config: Optional[CanvasGraphNodeConfig] = None
    columns: Optional[List[CanvasGraphColumn]] = None
    meta: Optional[Dict[str, Any]] = None

    class Config:
        fields = {
            "resource_type": "resourceType",
        }


class CanvasGraphJoinColumn(BaseModel):
    column: str
    field_id: Optional[str] = None

    class Config:
        fields = {
            "field_id": "fieldId",
        }


class CanvasGraphJoinFilter(BaseModel):
    expression: str
    description: Optional[str] = None


class CanvasGraphJoin(BaseModel):
    type: Optional[CanvasGraphJoinType] = None
    condition: Optional[str] = None
    source_columns: Optional[List[CanvasGraphJoinColumn]] = None
    target_columns: Optional[List[CanvasGraphJoinColumn]] = None
    filters: Optional[List[CanvasGraphJoinFilter]] = None

    class Config:
        fields = {
            "source_columns": "sourceColumns",
            "target_columns": "targetColumns",
        }


class CanvasGraphEdge(BaseModel):
    id: str
    source: str
    target: str
    type: CanvasGraphEdgeType
    description: Optional[str] = None
    relationship_id: Optional[str] = None
    join: Optional[CanvasGraphJoin] = None
    meta: Optional[Dict[str, Any]] = None

    class Config:
        fields = {
            "relationship_id": "relationshipId",
        }


class CanvasGraphGroup(BaseModel):
    id: str
    label: str
    layer: Optional[CanvasGraphLayer] = None
    color: Optional[str] = None
    description: Optional[str] = None


class CanvasGraphMetadata(BaseModel):
    project_id: Optional[str] = None
    project_name: Optional[str] = None
    definition_id: Optional[str] = None
    dbt_version: Optional[str] = None
    generated_by: Optional[str] = None
    generated_at: Optional[str] = None
    description: Optional[str] = None

    class Config:
        fields = {
            "project_id": "projectId",
            "project_name": "projectName",
            "definition_id": "definitionId",
            "dbt_version": "dbtVersion",
            "generated_by": "generatedBy",
            "generated_at": "generatedAt",
        }


class CanvasGraph(BaseModel):
    metadata: Optional[CanvasGraphMetadata] = None
    nodes: List[CanvasGraphNode]
    edges: List[CanvasGraphEdge]
    groups: Optional[List[CanvasGraphGroup]] = None
    warnings: Optional[List[str]] = None
