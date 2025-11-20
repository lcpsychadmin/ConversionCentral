from __future__ import annotations

from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field


class DbtManifestMetadata(BaseModel):
    project_name: Optional[str] = Field(default=None, alias="project_name")
    dbt_version: Optional[str] = Field(default=None, alias="dbt_version")
    generated_at: Optional[str] = Field(default=None, alias="generated_at")
    adapter_type: Optional[str] = Field(default=None, alias="adapter_type")

    class Config:
        allow_population_by_field_name = True


class DbtManifestDependsOn(BaseModel):
    nodes: List[str] = Field(default_factory=list)
    macros: List[str] = Field(default_factory=list)


class DbtManifestNode(BaseModel):
    unique_id: str = Field(alias="unique_id")
    name: str
    resource_type: str = Field(alias="resource_type")
    relation_name: Optional[str] = Field(default=None, alias="relation_name")
    database: Optional[str] = None
    schema_: Optional[str] = Field(default=None, alias="schema")
    alias: Optional[str] = None
    description: Optional[str] = None
    depends_on: DbtManifestDependsOn = Field(default_factory=DbtManifestDependsOn, alias="depends_on")
    tags: List[str] = Field(default_factory=list)
    meta: Dict[str, Any] = Field(default_factory=dict)
    config: Dict[str, Any] = Field(default_factory=dict)

    class Config:
        allow_population_by_field_name = True


class DbtManifestSource(BaseModel):
    unique_id: str = Field(alias="unique_id")
    resource_type: str = Field(alias="resource_type")
    name: str
    source_name: Optional[str] = Field(default=None, alias="source_name")
    database: Optional[str] = None
    schema_: Optional[str] = Field(default=None, alias="schema")
    identifier: Optional[str] = None
    description: Optional[str] = None
    relation_name: Optional[str] = Field(default=None, alias="relation_name")
    meta: Dict[str, Any] = Field(default_factory=dict)

    class Config:
        allow_population_by_field_name = True


class DbtManifestExposure(BaseModel):
    unique_id: str = Field(alias="unique_id")
    name: str
    type: Optional[str] = None
    maturity: Optional[str] = None
    description: Optional[str] = None
    owner: Dict[str, Any] = Field(default_factory=dict)
    depends_on: DbtManifestDependsOn = Field(default_factory=DbtManifestDependsOn, alias="depends_on")

    class Config:
        allow_population_by_field_name = True


class DbtManifestMetric(BaseModel):
    unique_id: str = Field(alias="unique_id")
    name: str
    label: Optional[str] = None
    description: Optional[str] = None
    type: Optional[str] = None
    meta: Dict[str, Any] = Field(default_factory=dict)

    class Config:
        allow_population_by_field_name = True


class DbtManifest(BaseModel):
    metadata: Optional[DbtManifestMetadata] = None
    nodes: Dict[str, DbtManifestNode] = Field(default_factory=dict)
    sources: Dict[str, DbtManifestSource] = Field(default_factory=dict)
    exposures: Dict[str, DbtManifestExposure] = Field(default_factory=dict)
    metrics: Dict[str, DbtManifestMetric] = Field(default_factory=dict)

    class Config:
        allow_population_by_field_name = True
