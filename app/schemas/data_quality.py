from __future__ import annotations

from datetime import datetime
from enum import Enum
from typing import Any, Dict, List, Optional
from uuid import UUID

from pydantic import BaseModel, Field


class TestGenProject(BaseModel):
    project_key: str
    name: str
    description: Optional[str] = None
    sql_flavor: Optional[str] = None


class TestGenConnection(BaseModel):
    connection_id: str
    project_key: str
    system_id: Optional[str] = None
    name: str
    catalog: Optional[str] = None
    schema_name: Optional[str] = None
    http_path: Optional[str] = None
    managed_credentials_ref: Optional[str] = None
    is_active: Optional[bool] = None


class TestGenTableGroup(BaseModel):
    table_group_id: str
    connection_id: str
    name: str
    description: Optional[str] = None
    profiling_include_mask: Optional[str] = None
    profiling_exclude_mask: Optional[str] = None


class TestGenTable(BaseModel):
    table_id: str
    table_group_id: str
    schema_name: Optional[str] = None
    table_name: str
    source_table_id: Optional[str] = None


class TestGenProfileRun(BaseModel):
    profile_run_id: str
    table_group_id: str
    status: str
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    row_count: Optional[int] = None
    anomaly_count: Optional[int] = None


class TestGenColumnMetric(BaseModel):
    key: str
    label: str
    value: Optional[Any] = None
    formatted: Optional[str] = None
    unit: Optional[str] = None


class TestGenColumnValueFrequency(BaseModel):
    value: Optional[Any] = None
    count: Optional[int] = None
    percentage: Optional[float] = None


class TestGenColumnHistogramBin(BaseModel):
    label: str
    count: Optional[int] = None
    lower: Optional[float] = None
    upper: Optional[float] = None


class TestGenProfileAnomaly(BaseModel):
    table_name: Optional[str] = None
    column_name: Optional[str] = None
    anomaly_type: str
    severity: str
    description: str
    detected_at: Optional[datetime] = None


class TestGenColumnProfile(BaseModel):
    table_group_id: str
    profile_run_id: Optional[str] = None
    status: Optional[str] = None
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    row_count: Optional[int] = None
    table_name: Optional[str] = None
    column_name: str
    data_type: Optional[str] = None
    metrics: List[TestGenColumnMetric] = Field(default_factory=list)
    top_values: List[TestGenColumnValueFrequency] = Field(default_factory=list)
    histogram: List[TestGenColumnHistogramBin] = Field(default_factory=list)
    anomalies: List[TestGenProfileAnomaly] = Field(default_factory=list)


class TestGenTestRun(BaseModel):
    test_run_id: str
    test_suite_key: Optional[str] = None
    project_key: str
    status: str
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    duration_ms: Optional[int] = None
    total_tests: Optional[int] = None
    failed_tests: Optional[int] = None
    trigger_source: Optional[str] = None


class TestSuiteSeverity(str, Enum):
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"
    CRITICAL = "critical"


class TestSuiteBase(BaseModel):
    description: Optional[str] = None
    product_team_id: Optional[UUID] = Field(default=None, alias="productTeamId")
    application_id: Optional[UUID] = Field(default=None, alias="applicationId")
    data_object_id: Optional[UUID] = Field(default=None, alias="dataObjectId")
    data_definition_id: Optional[UUID] = Field(default=None, alias="dataDefinitionId")
    project_key: Optional[str] = Field(default=None, alias="projectKey")

    class Config:
        allow_population_by_field_name = True


class TestSuiteCreateRequest(TestSuiteBase):
    name: str = Field(..., min_length=1, max_length=200)
    severity: TestSuiteSeverity = Field(default=TestSuiteSeverity.MEDIUM)


class TestSuiteUpdateRequest(TestSuiteBase):
    name: Optional[str] = Field(default=None, min_length=1, max_length=200)
    severity: Optional[TestSuiteSeverity] = None


class TestGenTestSuite(BaseModel):
    test_suite_key: str = Field(alias="testSuiteKey")
    project_key: Optional[str] = Field(default=None, alias="projectKey")
    name: str
    description: Optional[str] = None
    severity: Optional[TestSuiteSeverity] = None
    product_team_id: Optional[UUID] = Field(default=None, alias="productTeamId")
    application_id: Optional[UUID] = Field(default=None, alias="applicationId")
    data_object_id: Optional[UUID] = Field(default=None, alias="dataObjectId")
    data_definition_id: Optional[UUID] = Field(default=None, alias="dataDefinitionId")
    created_at: Optional[datetime] = Field(default=None, alias="createdAt")
    updated_at: Optional[datetime] = Field(default=None, alias="updatedAt")

    class Config:
        allow_population_by_field_name = True


class TestGenAlert(BaseModel):
    alert_id: str
    source_type: str
    source_ref: str
    severity: str
    title: str
    details: str
    acknowledged: bool
    acknowledged_by: Optional[str] = None
    acknowledged_at: Optional[datetime] = None
    created_at: Optional[datetime] = None


class ProfileAnomalyPayload(BaseModel):
    table_name: str
    column_name: Optional[str] = None
    anomaly_type: str
    severity: str
    description: str
    detected_at: Optional[datetime] = None


class ProfileRunStartRequest(BaseModel):
    table_group_id: str
    status: str = "running"
    started_at: Optional[datetime] = None


class ProfileRunStartResponse(BaseModel):
    profile_run_id: str


class ProfileRunCompleteRequest(BaseModel):
    status: str
    row_count: Optional[int] = None
    anomaly_count: Optional[int] = None
    anomalies: List[ProfileAnomalyPayload] = Field(default_factory=list)


class TestRunStartRequest(BaseModel):
    project_key: str
    test_suite_key: Optional[str] = None
    total_tests: Optional[int] = None
    trigger_source: Optional[str] = None
    status: str = "running"
    started_at: Optional[datetime] = None


class TestRunStartResponse(BaseModel):
    test_run_id: str


class TestRunCompleteRequest(BaseModel):
    status: str
    failed_tests: Optional[int] = None
    duration_ms: Optional[int] = None


class TestResultPayload(BaseModel):
    test_id: str
    table_name: Optional[str] = None
    column_name: Optional[str] = None
    result_status: str
    expected_value: Optional[str] = None
    actual_value: Optional[str] = None
    message: Optional[str] = None
    detected_at: Optional[datetime] = None


class TestResultBatchRequest(BaseModel):
    results: List[TestResultPayload] = Field(default_factory=list)


class AlertCreateRequest(BaseModel):
    source_type: str
    source_ref: str
    severity: str
    title: str
    details: str
    acknowledged: Optional[bool] = None
    acknowledged_by: Optional[str] = None
    acknowledged_at: Optional[datetime] = None
    alert_id: Optional[str] = None


class AlertCreateResponse(BaseModel):
    alert_id: str


class AlertAcknowledgeRequest(BaseModel):
    acknowledged: bool = True
    acknowledged_by: Optional[str] = None
    acknowledged_at: Optional[datetime] = None


class DataQualityDatasetField(BaseModel):
    data_definition_field_id: UUID = Field(alias="dataDefinitionFieldId")
    field_id: UUID = Field(alias="fieldId")
    name: str
    description: Optional[str] = None
    field_type: Optional[str] = Field(default=None, alias="fieldType")
    field_length: Optional[int] = Field(default=None, alias="fieldLength")
    decimal_places: Optional[int] = Field(default=None, alias="decimalPlaces")
    application_usage: Optional[str] = Field(default=None, alias="applicationUsage")
    business_definition: Optional[str] = Field(default=None, alias="businessDefinition")
    notes: Optional[str] = None
    display_order: Optional[int] = Field(default=None, alias="displayOrder")
    is_unique: Optional[bool] = Field(default=None, alias="isUnique")
    reference_table: Optional[str] = Field(default=None, alias="referenceTable")

    class Config:
        allow_population_by_field_name = True


class DataQualityDatasetTable(BaseModel):
    data_definition_table_id: UUID = Field(alias="dataDefinitionTableId")
    table_id: UUID = Field(alias="tableId")
    schema_name: Optional[str] = Field(default=None, alias="schemaName")
    table_name: str = Field(alias="tableName")
    physical_name: str = Field(alias="physicalName")
    alias: Optional[str] = None
    description: Optional[str] = None
    load_order: Optional[int] = Field(default=None, alias="loadOrder")
    is_constructed: bool = Field(alias="isConstructed")
    table_type: Optional[str] = Field(default=None, alias="tableType")
    fields: List[DataQualityDatasetField] = Field(default_factory=list)

    class Config:
        allow_population_by_field_name = True


class DataQualityDatasetTableContext(BaseModel):
    data_definition_table_id: UUID = Field(alias="dataDefinitionTableId")
    data_definition_id: UUID = Field(alias="dataDefinitionId")
    data_object_id: UUID = Field(alias="dataObjectId")
    application_id: UUID = Field(alias="applicationId")
    product_team_id: Optional[UUID] = Field(default=None, alias="productTeamId")
    table_group_id: str = Field(alias="tableGroupId")
    table_id: Optional[str] = Field(default=None, alias="tableId")
    schema_name: Optional[str] = Field(default=None, alias="schemaName")
    table_name: Optional[str] = Field(default=None, alias="tableName")
    physical_name: Optional[str] = Field(default=None, alias="physicalName")

    class Config:
        allow_population_by_field_name = True


class DataQualityDatasetDefinition(BaseModel):
    data_definition_id: UUID = Field(alias="dataDefinitionId")
    description: Optional[str] = None
    tables: List[DataQualityDatasetTable] = Field(default_factory=list)

    class Config:
        allow_population_by_field_name = True


class DataQualityDatasetDataObject(BaseModel):
    data_object_id: UUID = Field(alias="dataObjectId")
    name: str
    description: Optional[str] = None
    data_definitions: List[DataQualityDatasetDefinition] = Field(default_factory=list, alias="dataDefinitions")

    class Config:
        allow_population_by_field_name = True


class DataQualityDatasetApplication(BaseModel):
    application_id: UUID = Field(alias="applicationId")
    name: str
    description: Optional[str] = None
    physical_name: str = Field(alias="physicalName")
    data_objects: List[DataQualityDatasetDataObject] = Field(default_factory=list, alias="dataObjects")

    class Config:
        allow_population_by_field_name = True


class DataQualityDatasetProductTeam(BaseModel):
    product_team_id: UUID = Field(alias="productTeamId")
    name: str
    description: Optional[str] = None
    applications: List[DataQualityDatasetApplication] = Field(default_factory=list)

    class Config:
        allow_population_by_field_name = True


class DataQualityProfilingStats(BaseModel):
    table_count: int = Field(alias="tableCount")
    profiled_table_count: int = Field(alias="profiledTableCount")
    row_count: Optional[int] = Field(default=None, alias="rowCount")
    column_count: Optional[int] = Field(default=None, alias="columnCount")
    profiled_column_count: Optional[int] = Field(default=None, alias="profiledColumnCount")
    anomaly_count: Optional[int] = Field(default=None, alias="anomalyCount")
    dq_score: Optional[float] = Field(default=None, alias="dqScore")
    last_completed_at: Optional[datetime] = Field(default=None, alias="lastCompletedAt")

    class Config:
        allow_population_by_field_name = True


class DataQualityTableProfilingStats(DataQualityProfilingStats):
    table_group_id: str = Field(alias="tableGroupId")
    table_id: Optional[str] = Field(default=None, alias="tableId")

    class Config:
        allow_population_by_field_name = True


class DataQualityDatasetProfilingStatsResponse(BaseModel):
    product_teams: Dict[str, DataQualityProfilingStats] = Field(default_factory=dict, alias="productTeams")
    applications: Dict[str, DataQualityProfilingStats] = Field(default_factory=dict)
    data_objects: Dict[str, DataQualityProfilingStats] = Field(default_factory=dict, alias="dataObjects")
    tables: Dict[str, DataQualityTableProfilingStats] = Field(default_factory=dict)

    class Config:
        allow_population_by_field_name = True


class DataQualityProfileRunSummary(BaseModel):
    table_group_id: str = Field(alias="tableGroupId")
    profile_run_id: str = Field(alias="profileRunId")

    class Config:
        allow_population_by_field_name = True


class DataQualityBulkProfileRunResponse(BaseModel):
    requested_table_count: int = Field(alias="requestedTableCount")
    targeted_table_group_count: int = Field(alias="targetedTableGroupCount")
    profile_runs: List[DataQualityProfileRunSummary] = Field(default_factory=list, alias="profileRuns")
    skipped_table_ids: List[UUID] = Field(default_factory=list, alias="skippedTableIds")

    class Config:
        allow_population_by_field_name = True


class DataQualityProfileRunEntry(BaseModel):
    profile_run_id: str = Field(alias="profileRunId")
    table_group_id: str = Field(alias="tableGroupId")
    table_group_name: Optional[str] = Field(default=None, alias="tableGroupName")
    connection_id: Optional[UUID] = Field(default=None, alias="connectionId")
    connection_name: Optional[str] = Field(default=None, alias="connectionName")
    catalog: Optional[str] = None
    schema_name: Optional[str] = Field(default=None, alias="schemaName")
    data_object_id: Optional[UUID] = Field(default=None, alias="dataObjectId")
    data_object_name: Optional[str] = Field(default=None, alias="dataObjectName")
    application_id: Optional[UUID] = Field(default=None, alias="applicationId")
    application_name: Optional[str] = Field(default=None, alias="applicationName")
    product_team_id: Optional[UUID] = Field(default=None, alias="productTeamId")
    product_team_name: Optional[str] = Field(default=None, alias="productTeamName")
    status: str
    started_at: Optional[datetime] = Field(default=None, alias="startedAt")
    completed_at: Optional[datetime] = Field(default=None, alias="completedAt")
    duration_ms: Optional[int] = Field(default=None, alias="durationMs")
    row_count: Optional[int] = Field(default=None, alias="rowCount")
    anomaly_count: Optional[int] = Field(default=None, alias="anomalyCount")
    databricks_run_id: Optional[str] = Field(default=None, alias="databricksRunId")
    anomalies_by_severity: Dict[str, int] = Field(default_factory=dict, alias="anomaliesBySeverity")

    class Config:
        allow_population_by_field_name = True


class DataQualityProfileRunTableGroup(BaseModel):
    table_group_id: str = Field(alias="tableGroupId")
    table_group_name: Optional[str] = Field(default=None, alias="tableGroupName")
    connection_id: Optional[UUID] = Field(default=None, alias="connectionId")
    connection_name: Optional[str] = Field(default=None, alias="connectionName")
    catalog: Optional[str] = None
    schema_name: Optional[str] = Field(default=None, alias="schemaName")
    data_object_id: Optional[UUID] = Field(default=None, alias="dataObjectId")
    data_object_name: Optional[str] = Field(default=None, alias="dataObjectName")
    application_id: Optional[UUID] = Field(default=None, alias="applicationId")
    application_name: Optional[str] = Field(default=None, alias="applicationName")
    product_team_id: Optional[UUID] = Field(default=None, alias="productTeamId")
    product_team_name: Optional[str] = Field(default=None, alias="productTeamName")
    profiling_job_id: Optional[str] = Field(default=None, alias="profilingJobId")

    class Config:
        allow_population_by_field_name = True


class DataQualityProfileRunListResponse(BaseModel):
    runs: List[DataQualityProfileRunEntry]
    table_groups: List[DataQualityProfileRunTableGroup] = Field(default_factory=list, alias="tableGroups")

    class Config:
        allow_population_by_field_name = True


class TestDefinitionBase(BaseModel):
    name: str = Field(..., min_length=1, max_length=200)
    rule_type: str = Field(..., alias="ruleType", min_length=1, max_length=120)
    data_definition_table_id: UUID = Field(..., alias="dataDefinitionTableId")
    column_name: Optional[str] = Field(default=None, alias="columnName")
    definition: Dict[str, Any] = Field(default_factory=dict)

    class Config:
        allow_population_by_field_name = True


class TestDefinitionCreateRequest(TestDefinitionBase):
    pass


class TestDefinitionUpdateRequest(BaseModel):
    name: Optional[str] = Field(default=None, min_length=1, max_length=200)
    rule_type: Optional[str] = Field(default=None, alias="ruleType", min_length=1, max_length=120)
    data_definition_table_id: Optional[UUID] = Field(default=None, alias="dataDefinitionTableId")
    column_name: Optional[str] = Field(default=None, alias="columnName")
    definition: Optional[Dict[str, Any]] = Field(default=None)

    class Config:
        allow_population_by_field_name = True


class TestGenSuiteTest(BaseModel):
    test_id: str = Field(alias="testId")
    test_suite_key: str = Field(alias="testSuiteKey")
    table_group_id: str = Field(alias="tableGroupId")
    table_id: Optional[str] = Field(default=None, alias="tableId")
    data_definition_table_id: Optional[UUID] = Field(default=None, alias="dataDefinitionTableId")
    schema_name: Optional[str] = Field(default=None, alias="schemaName")
    table_name: Optional[str] = Field(default=None, alias="tableName")
    physical_name: Optional[str] = Field(default=None, alias="physicalName")
    column_name: Optional[str] = Field(default=None, alias="columnName")
    rule_type: str = Field(alias="ruleType")
    name: str
    definition: Dict[str, Any] = Field(default_factory=dict)
    created_at: Optional[datetime] = Field(default=None, alias="createdAt")
    updated_at: Optional[datetime] = Field(default=None, alias="updatedAt")

    class Config:
        allow_population_by_field_name = True


class DataQualityTestTypeParameter(BaseModel):
    name: str
    prompt: Optional[str] = None
    help: Optional[str] = None
    default_value: Optional[str] = Field(default=None, alias="defaultValue")

    class Config:
        allow_population_by_field_name = True


class DataQualityTestType(BaseModel):
    test_type: str = Field(alias="testType")
    rule_type: str = Field(alias="ruleType")
    id: Optional[str] = None
    name_short: Optional[str] = Field(default=None, alias="nameShort")
    name_long: Optional[str] = Field(default=None, alias="nameLong")
    description: Optional[str] = None
    usage_notes: Optional[str] = Field(default=None, alias="usageNotes")
    dq_dimension: Optional[str] = Field(default=None, alias="dqDimension")
    run_type: Optional[str] = Field(default=None, alias="runType")
    test_scope: Optional[str] = Field(default=None, alias="testScope")
    default_severity: Optional[str] = Field(default=None, alias="defaultSeverity")
    column_prompt: Optional[str] = Field(default=None, alias="columnPrompt")
    column_help: Optional[str] = Field(default=None, alias="columnHelp")
    sql_flavors: List[str] = Field(default_factory=list, alias="sqlFlavors")
    parameters: List[DataQualityTestTypeParameter] = Field(default_factory=list)
    source_file: Optional[str] = Field(default=None, alias="sourceFile")

    class Config:
        allow_population_by_field_name = True
