from datetime import datetime
from enum import Enum
from typing import Any, Optional
from uuid import UUID

from pydantic import BaseModel, Field, root_validator


class TimestampSchema(BaseModel):
    created_at: datetime
    updated_at: datetime

    class Config:
        orm_mode = True


class ProjectBase(BaseModel):
    name: str = Field(..., max_length=200)
    description: Optional[str] = None
    status: str = Field("planned", max_length=50)


class ProjectCreate(ProjectBase):
    pass


class ProjectUpdate(BaseModel):
    name: Optional[str] = Field(None, max_length=200)
    description: Optional[str] = None
    status: Optional[str] = Field(None, max_length=50)


class ProjectRead(ProjectBase, TimestampSchema):
    id: UUID


class ReleaseBase(BaseModel):
    project_id: UUID
    name: str = Field(..., max_length=200)
    description: Optional[str] = None
    status: str = Field("planned", max_length=50)


class ReleaseCreate(ReleaseBase):
    pass


class ReleaseUpdate(BaseModel):
    project_id: Optional[UUID] = None
    name: Optional[str] = Field(None, max_length=200)
    description: Optional[str] = None
    status: Optional[str] = Field(None, max_length=50)


class ReleaseRead(ReleaseBase, TimestampSchema):
    id: UUID


class ReleaseDataObjectBase(BaseModel):
    release_id: UUID
    data_object_id: UUID


class ReleaseDataObjectCreate(ReleaseDataObjectBase):
    pass


class ReleaseDataObjectRead(ReleaseDataObjectBase, TimestampSchema):
    id: UUID


class MockCycleBase(BaseModel):
    release_id: UUID
    name: str = Field(..., max_length=200)
    description: Optional[str] = None
    status: str = Field("planned", max_length=50)


class MockCycleCreate(MockCycleBase):
    pass


class MockCycleUpdate(BaseModel):
    release_id: Optional[UUID] = None
    name: Optional[str] = Field(None, max_length=200)
    description: Optional[str] = None
    status: Optional[str] = Field(None, max_length=50)


class MockCycleRead(MockCycleBase, TimestampSchema):
    id: UUID


class ExecutionContextBase(BaseModel):
    mock_cycle_id: UUID
    name: str = Field(..., max_length=200)
    description: Optional[str] = None
    status: str = Field("planned", max_length=50)


class ExecutionContextCreate(ExecutionContextBase):
    pass


class ExecutionContextUpdate(BaseModel):
    mock_cycle_id: Optional[UUID] = None
    name: Optional[str] = Field(None, max_length=200)
    description: Optional[str] = None
    status: Optional[str] = Field(None, max_length=50)


class ExecutionContextRead(ExecutionContextBase, TimestampSchema):
    id: UUID


class RoleBase(BaseModel):
    name: str = Field(..., max_length=100)
    description: Optional[str] = None
    is_active: bool = True


class RoleCreate(RoleBase):
    pass


class RoleUpdate(BaseModel):
    name: Optional[str] = Field(None, max_length=100)
    description: Optional[str] = None
    is_active: Optional[bool] = None


class RoleRead(RoleBase, TimestampSchema):
    id: UUID


class UserBase(BaseModel):
    name: str = Field(..., max_length=200)
    email: Optional[str] = Field(None, max_length=200)
    status: str = Field("active", max_length=50)


class UserCreate(UserBase):
    pass


class UserUpdate(BaseModel):
    name: Optional[str] = Field(None, max_length=200)
    email: Optional[str] = Field(None, max_length=200)
    status: Optional[str] = Field(None, max_length=50)


class UserRead(UserBase, TimestampSchema):
    id: UUID


class DependencyType(str, Enum):
    PRECEDENCE = "precedence"
    REFERENCE = "reference"


class DependencyApprovalDecision(str, Enum):
    PENDING = "pending"
    APPROVED = "approved"
    REJECTED = "rejected"


class DataObjectDependencyBase(BaseModel):
    predecessor_id: UUID
    successor_id: UUID
    dependency_type: DependencyType = DependencyType.PRECEDENCE
    notes: Optional[str] = None


class DataObjectDependencyCreate(DataObjectDependencyBase):
    pass


class DataObjectDependencyUpdate(BaseModel):
    predecessor_id: Optional[UUID] = None
    successor_id: Optional[UUID] = None
    dependency_type: Optional[DependencyType] = None
    notes: Optional[str] = None


class DataObjectDependencyRead(DataObjectDependencyBase, TimestampSchema):
    id: UUID


class TableDependencyBase(BaseModel):
    predecessor_id: UUID
    successor_id: UUID
    dependency_type: DependencyType = DependencyType.PRECEDENCE
    notes: Optional[str] = None


class TableDependencyCreate(TableDependencyBase):
    pass


class TableDependencyUpdate(BaseModel):
    predecessor_id: Optional[UUID] = None
    successor_id: Optional[UUID] = None
    dependency_type: Optional[DependencyType] = None
    notes: Optional[str] = None


class TableDependencyRead(TableDependencyBase, TimestampSchema):
    id: UUID


class DependencyApprovalBase(BaseModel):
    data_object_dependency_id: Optional[UUID] = None
    table_dependency_id: Optional[UUID] = None
    approver_id: UUID
    decision: DependencyApprovalDecision = DependencyApprovalDecision.PENDING
    comments: Optional[str] = None

    @root_validator
    def validate_target(cls, values):
        data_object_dependency_id = values.get("data_object_dependency_id")
        table_dependency_id = values.get("table_dependency_id")
        if bool(data_object_dependency_id) == bool(table_dependency_id):
            raise ValueError("Exactly one dependency target must be provided")
        return values


class DependencyApprovalCreate(DependencyApprovalBase):
    pass


class DependencyApprovalUpdate(BaseModel):
    data_object_dependency_id: Optional[UUID] = None
    table_dependency_id: Optional[UUID] = None
    approver_id: Optional[UUID] = None
    decision: Optional[DependencyApprovalDecision] = None
    comments: Optional[str] = None

    @root_validator
    def validate_target(cls, values):
        data_object_dependency_id = values.get("data_object_dependency_id")
        table_dependency_id = values.get("table_dependency_id")
        if data_object_dependency_id is not None and table_dependency_id is not None:
            raise ValueError("Cannot set both dependency identifiers")
        return values


class DependencyApprovalRead(DependencyApprovalBase, TimestampSchema):
    id: UUID
    decided_at: datetime


class DependencyOrderItem(BaseModel):
    id: UUID
    name: str
    order: int


class TableLoadOrderBase(BaseModel):
    data_object_id: UUID
    table_id: UUID
    sequence: int = Field(..., ge=1)
    notes: Optional[str] = None


class TableLoadOrderCreate(TableLoadOrderBase):
    pass


class TableLoadOrderUpdate(BaseModel):
    table_id: Optional[UUID] = None
    sequence: Optional[int] = Field(None, ge=1)
    notes: Optional[str] = None


class TableLoadOrderRead(TableLoadOrderBase, TimestampSchema):
    id: UUID


class TableLoadOrderApprovalRole(str, Enum):
    SME = "sme"
    DATA_OWNER = "data_owner"
    APPROVER = "approver"
    ADMIN = "admin"


class TableLoadOrderApprovalDecision(str, Enum):
    APPROVED = "approved"
    REJECTED = "rejected"


class TableLoadOrderApprovalBase(BaseModel):
    table_load_order_id: UUID
    approver_id: UUID
    role: TableLoadOrderApprovalRole
    decision: TableLoadOrderApprovalDecision
    comments: Optional[str] = None


class TableLoadOrderApprovalCreate(TableLoadOrderApprovalBase):
    pass


class TableLoadOrderApprovalUpdate(BaseModel):
    table_load_order_id: Optional[UUID] = None
    approver_id: Optional[UUID] = None
    role: Optional[TableLoadOrderApprovalRole] = None
    decision: Optional[TableLoadOrderApprovalDecision] = None
    comments: Optional[str] = None


class TableLoadOrderApprovalRead(TableLoadOrderApprovalBase, TimestampSchema):
    id: UUID
    approved_at: datetime


class ConstructedTableStatus(str, Enum):
    DRAFT = "draft"
    PENDING_APPROVAL = "pending_approval"
    APPROVED = "approved"
    REJECTED = "rejected"


class ConstructedTableBase(BaseModel):
    execution_context_id: Optional[UUID] = None
    data_definition_id: Optional[UUID] = None
    data_definition_table_id: Optional[UUID] = None
    name: str = Field(..., max_length=200)
    description: Optional[str] = None
    purpose: Optional[str] = None
    status: ConstructedTableStatus = ConstructedTableStatus.DRAFT


class ConstructedTableCreate(ConstructedTableBase):
    execution_context_id: UUID


class ConstructedTableUpdate(BaseModel):
    execution_context_id: Optional[UUID] = None
    data_definition_id: Optional[UUID] = None
    data_definition_table_id: Optional[UUID] = None
    name: Optional[str] = Field(None, max_length=200)
    description: Optional[str] = None
    purpose: Optional[str] = None
    status: Optional[ConstructedTableStatus] = None


class ConstructedTableRead(ConstructedTableBase, TimestampSchema):
    id: UUID


class ConstructedFieldBase(BaseModel):
    constructed_table_id: UUID
    name: str = Field(..., max_length=200)
    data_type: str = Field(..., max_length=100)
    is_nullable: bool = True
    default_value: Optional[str] = None
    description: Optional[str] = None


class ConstructedFieldCreate(ConstructedFieldBase):
    pass


class ConstructedFieldUpdate(BaseModel):
    constructed_table_id: Optional[UUID] = None
    name: Optional[str] = Field(None, max_length=200)
    data_type: Optional[str] = Field(None, max_length=100)
    is_nullable: Optional[bool] = None
    default_value: Optional[str] = None
    description: Optional[str] = None


class ConstructedFieldRead(ConstructedFieldBase, TimestampSchema):
    id: UUID


class ConstructedDataBase(BaseModel):
    constructed_table_id: UUID
    row_identifier: Optional[str] = Field(None, max_length=200)
    payload: dict[str, Any]


class ConstructedDataCreate(ConstructedDataBase):
    pass


class ConstructedDataUpdate(BaseModel):
    constructed_table_id: Optional[UUID] = None
    row_identifier: Optional[str] = Field(None, max_length=200)
    payload: Optional[dict[str, Any]] = None


class ConstructedDataRead(ConstructedDataBase, TimestampSchema):
    id: UUID


class ValidationErrorDetail(BaseModel):
    """Details of a single validation error."""
    rowIndex: int
    fieldName: Optional[str] = None
    message: str
    ruleId: UUID


class ConstructedDataBatchSaveRequest(BaseModel):
    """Request to save multiple rows of constructed data with validation."""
    rows: list[dict[str, Any]]
    validateOnly: bool = False  # If true, validate but don't save


class ConstructedDataBatchSaveResponse(BaseModel):
    """Response from batch save operation."""
    success: bool
    rowsSaved: int
    errors: list[ValidationErrorDetail]


class ConstructedTableApprovalRole(str, Enum):
    DATA_STEWARD = "data_steward"
    BUSINESS_OWNER = "business_owner"
    TECHNICAL_LEAD = "technical_lead"
    ADMIN = "admin"


class ConstructedTableApprovalDecision(str, Enum):
    APPROVED = "approved"
    REJECTED = "rejected"


class ConstructedTableApprovalBase(BaseModel):
    constructed_table_id: UUID
    approver_id: UUID
    role: ConstructedTableApprovalRole
    decision: ConstructedTableApprovalDecision
    comments: Optional[str] = None


class ConstructedTableApprovalCreate(ConstructedTableApprovalBase):
    pass


class ConstructedTableApprovalUpdate(BaseModel):
    constructed_table_id: Optional[UUID] = None
    approver_id: Optional[UUID] = None
    role: Optional[ConstructedTableApprovalRole] = None
    decision: Optional[ConstructedTableApprovalDecision] = None
    comments: Optional[str] = None


class ConstructedTableApprovalRead(ConstructedTableApprovalBase, TimestampSchema):
    id: UUID
    approved_at: datetime


class ValidationRuleType(str, Enum):
    REQUIRED = "required"
    UNIQUE = "unique"
    RANGE = "range"
    PATTERN = "pattern"
    CUSTOM = "custom"
    CROSS_FIELD = "cross_field"


class ConstructedDataValidationRuleBase(BaseModel):
    constructed_table_id: UUID
    name: str = Field(..., max_length=200)
    description: Optional[str] = None
    rule_type: ValidationRuleType
    field_id: Optional[UUID] = None
    configuration: dict = Field(default_factory=dict)
    error_message: str = "Validation failed"
    is_active: bool = True
    applies_to_new_only: bool = False


class ConstructedDataValidationRuleCreate(ConstructedDataValidationRuleBase):
    pass


class ConstructedDataValidationRuleUpdate(BaseModel):
    name: Optional[str] = Field(None, max_length=200)
    description: Optional[str] = None
    rule_type: Optional[ValidationRuleType] = None
    field_id: Optional[UUID] = None
    configuration: Optional[dict] = None
    error_message: Optional[str] = None
    is_active: Optional[bool] = None
    applies_to_new_only: Optional[bool] = None


class ConstructedDataValidationRuleRead(ConstructedDataValidationRuleBase, TimestampSchema):
    id: UUID


class ProcessAreaBase(BaseModel):
    name: str = Field(..., max_length=200)
    description: Optional[str] = None
    status: str = Field("draft", max_length=50)


class ProcessAreaCreate(ProcessAreaBase):
    pass


class ProcessAreaUpdate(BaseModel):
    name: Optional[str] = Field(None, max_length=200)
    description: Optional[str] = None
    status: Optional[str] = Field(None, max_length=50)


class ProcessAreaRead(ProcessAreaBase, TimestampSchema):
    id: UUID


class ProcessAreaRoleAssignmentBase(BaseModel):
    process_area_id: UUID
    user_id: UUID
    role_id: UUID
    granted_by: Optional[UUID] = None
    notes: Optional[str] = None


class ProcessAreaRoleAssignmentCreate(ProcessAreaRoleAssignmentBase):
    pass


class ProcessAreaRoleAssignmentUpdate(BaseModel):
    process_area_id: Optional[UUID] = None
    user_id: Optional[UUID] = None
    role_id: Optional[UUID] = None
    granted_by: Optional[UUID] = None
    notes: Optional[str] = None


class ProcessAreaRoleAssignmentRead(ProcessAreaRoleAssignmentBase, TimestampSchema):
    id: UUID


class DataObjectBase(BaseModel):
    process_area_id: UUID
    name: str = Field(..., max_length=200)
    description: Optional[str] = None
    status: str = Field("draft", max_length=50)


class DataObjectCreate(DataObjectBase):
    system_ids: list[UUID] = []


class DataObjectUpdate(BaseModel):
    process_area_id: Optional[UUID] = None
    name: Optional[str] = Field(None, max_length=200)
    description: Optional[str] = None
    status: Optional[str] = Field(None, max_length=50)
    system_ids: Optional[list[UUID]] = None


class DataObjectRead(DataObjectBase, TimestampSchema):
    id: UUID
    systems: list["SystemRead"] = []
    process_area: Optional["ProcessAreaRead"] = None


class SystemBase(BaseModel):
    name: str = Field(..., max_length=200)
    physical_name: str = Field(..., max_length=200)
    description: Optional[str] = None
    system_type: Optional[str] = Field(None, max_length=100)
    status: str = Field("active", max_length=50)
    security_classification: Optional[str] = Field(None, max_length=100)


class SystemCreate(SystemBase):
    pass


class SystemUpdate(BaseModel):
    name: Optional[str] = Field(None, max_length=200)
    physical_name: Optional[str] = Field(None, max_length=200)
    description: Optional[str] = None
    system_type: Optional[str] = Field(None, max_length=100)
    status: Optional[str] = Field(None, max_length=50)
    security_classification: Optional[str] = Field(None, max_length=100)


class SystemRead(SystemBase, TimestampSchema):
    id: UUID


class DataObjectSystemBase(BaseModel):
    data_object_id: UUID
    system_id: UUID
    relationship_type: str = Field("source", max_length=50)
    description: Optional[str] = None


class DataObjectSystemCreate(DataObjectSystemBase):
    pass


class DataObjectSystemUpdate(BaseModel):
    data_object_id: Optional[UUID] = None
    system_id: Optional[UUID] = None
    relationship_type: Optional[str] = Field(None, max_length=50)
    description: Optional[str] = None


class DataObjectSystemRead(DataObjectSystemBase, TimestampSchema):
    id: UUID


DataObjectRead.update_forward_refs()


class TableBase(BaseModel):
    system_id: UUID
    name: str = Field(..., max_length=200)
    physical_name: str = Field(..., max_length=200)
    schema_name: Optional[str] = Field(None, max_length=120)
    description: Optional[str] = None
    table_type: Optional[str] = Field(None, max_length=50)
    status: str = Field("active", max_length=50)
    security_classification: Optional[str] = Field(None, max_length=100)


class TableCreate(TableBase):
    pass


class TableUpdate(BaseModel):
    system_id: Optional[UUID] = None
    name: Optional[str] = Field(None, max_length=200)
    physical_name: Optional[str] = Field(None, max_length=200)
    schema_name: Optional[str] = Field(None, max_length=120)
    description: Optional[str] = None
    table_type: Optional[str] = Field(None, max_length=50)
    status: Optional[str] = Field(None, max_length=50)
    security_classification: Optional[str] = Field(None, max_length=100)


class TableRead(TableBase, TimestampSchema):
    id: UUID


class FieldBase(BaseModel):
    table_id: UUID
    name: str = Field(..., max_length=200)
    description: Optional[str] = None
    application_usage: Optional[str] = None
    business_definition: Optional[str] = None
    enterprise_attribute: Optional[str] = Field(None, max_length=200)
    field_type: str = Field(..., max_length=100)
    field_length: Optional[int] = Field(None, ge=0)
    decimal_places: Optional[int] = Field(None, ge=0)
    system_required: bool = False
    business_process_required: bool = False
    suppressed_field: bool = False
    active: bool = True
    legal_regulatory_implications: Optional[str] = None
    security_classification: Optional[str] = Field(None, max_length=100)
    data_validation: Optional[str] = None
    reference_table: Optional[str] = Field(None, max_length=200)
    grouping_tab: Optional[str] = Field(None, max_length=200)


class FieldCreate(FieldBase):
    pass


class FieldUpdate(BaseModel):
    table_id: Optional[UUID] = None
    name: Optional[str] = Field(None, max_length=200)
    description: Optional[str] = None
    application_usage: Optional[str] = None
    business_definition: Optional[str] = None
    enterprise_attribute: Optional[str] = Field(None, max_length=200)
    field_type: Optional[str] = Field(None, max_length=100)
    field_length: Optional[int] = Field(None, ge=0)
    decimal_places: Optional[int] = Field(None, ge=0)
    system_required: Optional[bool] = None
    business_process_required: Optional[bool] = None
    suppressed_field: Optional[bool] = None
    active: Optional[bool] = None
    legal_regulatory_implications: Optional[str] = None
    security_classification: Optional[str] = Field(None, max_length=100)
    data_validation: Optional[str] = None
    reference_table: Optional[str] = Field(None, max_length=200)
    grouping_tab: Optional[str] = Field(None, max_length=200)


class FieldRead(FieldBase, TimestampSchema):
    id: UUID


class DataDefinitionFieldInput(BaseModel):
    field_id: UUID
    notes: Optional[str] = None


class DataDefinitionTableInput(BaseModel):
    table_id: UUID
    alias: Optional[str] = Field(None, max_length=200)
    description: Optional[str] = None
    load_order: Optional[int] = Field(None, ge=1)
    is_construction: bool = False
    fields: list[DataDefinitionFieldInput] = []


class DataDefinitionCreate(BaseModel):
    data_object_id: UUID
    system_id: UUID
    description: Optional[str] = None
    tables: list[DataDefinitionTableInput] = []


class DataDefinitionUpdate(BaseModel):
    description: Optional[str] = None
    tables: Optional[list[DataDefinitionTableInput]] = None


class DataDefinitionFieldRead(TimestampSchema):
    id: UUID
    definition_table_id: UUID
    field_id: UUID
    notes: Optional[str] = None
    field: FieldRead


class DataDefinitionTableRead(TimestampSchema):
    id: UUID
    data_definition_id: UUID
    table_id: UUID
    alias: Optional[str] = None
    description: Optional[str] = None
    load_order: Optional[int] = None
    is_construction: bool
    table: TableRead
    fields: list[DataDefinitionFieldRead] = []
    constructed_table_id: Optional[UUID] = Field(
        None,
        alias="constructedTableId",
    )
    constructed_table_name: Optional[str] = Field(
        None,
        alias="constructedTableName",
    )
    constructed_table_status: Optional[str] = Field(
        None,
        alias="constructedTableStatus",
    )

    class Config:
        orm_mode = True
        allow_population_by_field_name = True


class DataDefinitionJoinType(str, Enum):
    INNER = "inner"
    LEFT = "left"
    RIGHT = "right"


class DataDefinitionRelationshipBase(BaseModel):
    primary_field_id: UUID
    foreign_field_id: UUID
    join_type: DataDefinitionJoinType = DataDefinitionJoinType.INNER
    notes: Optional[str] = None

    @root_validator
    def validate_fields(cls, values):
        primary = values.get("primary_field_id")
        foreign = values.get("foreign_field_id")
        if primary == foreign:
            raise ValueError("Primary and foreign fields must be different")
        return values


class DataDefinitionRelationshipCreate(DataDefinitionRelationshipBase):
    pass


class DataDefinitionRelationshipUpdate(BaseModel):
    primary_field_id: Optional[UUID] = None
    foreign_field_id: Optional[UUID] = None
    join_type: Optional[DataDefinitionJoinType] = None
    notes: Optional[str] = None

    @root_validator
    def validate_fields(cls, values):
        primary = values.get("primary_field_id")
        foreign = values.get("foreign_field_id")
        if primary is not None and foreign is not None and primary == foreign:
            raise ValueError("Primary and foreign fields must be different")
        return values


class DataDefinitionRelationshipRead(TimestampSchema):
    id: UUID
    data_definition_id: UUID
    primary_table_id: UUID
    primary_field_id: UUID
    foreign_table_id: UUID
    foreign_field_id: UUID
    join_type: DataDefinitionJoinType
    notes: Optional[str] = None
    primary_field: DataDefinitionFieldRead
    foreign_field: DataDefinitionFieldRead


class DataDefinitionRead(TimestampSchema):
    id: UUID
    data_object_id: UUID
    system_id: UUID
    description: Optional[str] = None
    system: Optional[SystemRead] = None
    data_object: Optional[DataObjectRead] = None
    tables: list[DataDefinitionTableRead] = []
    relationships: list[DataDefinitionRelationshipRead] = []


class FieldLoadBase(BaseModel):
    release_id: UUID
    field_id: UUID
    load_flag: bool = True
    notes: Optional[str] = None
    created_by: Optional[UUID] = None


class FieldLoadCreate(FieldLoadBase):
    pass


class FieldLoadUpdate(BaseModel):
    release_id: Optional[UUID] = None
    field_id: Optional[UUID] = None
    load_flag: Optional[bool] = None
    notes: Optional[str] = None
    created_by: Optional[UUID] = None


class FieldLoadRead(FieldLoadBase, TimestampSchema):
    id: UUID


class SystemConnectionType(str, Enum):
    JDBC = "jdbc"
    ODBC = "odbc"
    API = "api"
    FILE = "file"
    SAPRFC = "saprfc"
    OTHER = "other"


class SystemConnectionAuthMethod(str, Enum):
    USERNAME_PASSWORD = "username_password"
    OAUTH = "oauth"
    KEY_VAULT_REFERENCE = "key_vault_reference"


class SystemConnectionBase(BaseModel):
    system_id: UUID
    connection_type: SystemConnectionType = SystemConnectionType.JDBC
    connection_string: str
    auth_method: SystemConnectionAuthMethod = SystemConnectionAuthMethod.USERNAME_PASSWORD
    active: bool = True
    ingestion_enabled: bool = True
    notes: Optional[str] = None


class SystemConnectionCreate(SystemConnectionBase):
    pass


class SystemConnectionUpdate(BaseModel):
    system_id: Optional[UUID] = None
    connection_type: Optional[SystemConnectionType] = None
    connection_string: Optional[str] = None
    auth_method: Optional[SystemConnectionAuthMethod] = None
    active: Optional[bool] = None
    ingestion_enabled: Optional[bool] = None
    notes: Optional[str] = None


class SystemConnectionRead(SystemConnectionBase, TimestampSchema):
    id: UUID


class SystemConnectionTestRequest(BaseModel):
    connection_type: SystemConnectionType = SystemConnectionType.JDBC
    connection_string: str


class SystemConnectionTestResult(BaseModel):
    success: bool
    message: str
    duration_ms: float | None = None
    connection_summary: str | None = None


class ConnectionCatalogTable(BaseModel):
    schema_name: str
    table_name: str
    table_type: Optional[str] = None
    column_count: Optional[int] = None
    estimated_rows: Optional[int] = None
    selected: bool = False
    available: bool = True
    selection_id: Optional[UUID] = None


class ConnectionTablePreview(BaseModel):
    columns: list[str]
    rows: list[dict[str, Any]]


class ConnectionTableIdentifier(BaseModel):
    schema_name: str
    table_name: str
    table_type: Optional[str] = None
    column_count: Optional[int] = None
    estimated_rows: Optional[int] = None


class ConnectionCatalogSelectionUpdate(BaseModel):
    selected_tables: list[ConnectionTableIdentifier]


class IngestionJobStatus(str, Enum):
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"


class IngestionJobBase(BaseModel):
    execution_context_id: UUID
    table_id: UUID
    status: IngestionJobStatus = IngestionJobStatus.PENDING
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    row_count: Optional[int] = Field(None, ge=0)
    notes: Optional[str] = None


class IngestionJobCreate(IngestionJobBase):
    pass


class IngestionJobUpdate(BaseModel):
    execution_context_id: Optional[UUID] = None
    table_id: Optional[UUID] = None
    status: Optional[IngestionJobStatus] = None
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    row_count: Optional[int] = Field(None, ge=0)
    notes: Optional[str] = None


class IngestionJobRead(IngestionJobBase, TimestampSchema):
    id: UUID


class IngestionJobRunRequest(BaseModel):
    replace: bool = False
    batch_size: Optional[int] = Field(default=None, ge=1, le=100_000)


class IngestionLoadStrategy(str, Enum):
    TIMESTAMP = "timestamp"
    NUMERIC_KEY = "numeric_key"
    FULL = "full"


class IngestionScheduleBase(BaseModel):
    connection_table_selection_id: UUID
    schedule_expression: str = Field(..., max_length=120)
    timezone: Optional[str] = Field(None, max_length=60)
    load_strategy: IngestionLoadStrategy = IngestionLoadStrategy.TIMESTAMP
    watermark_column: Optional[str] = Field(None, max_length=120)
    primary_key_column: Optional[str] = Field(None, max_length=120)
    target_schema: Optional[str] = Field(None, max_length=120)
    target_table_name: Optional[str] = Field(None, max_length=200)
    batch_size: int = Field(5_000, ge=1, le=100_000)
    is_active: bool = True


class IngestionScheduleCreate(IngestionScheduleBase):
    pass


class IngestionScheduleUpdate(BaseModel):
    schedule_expression: Optional[str] = Field(None, max_length=120)
    timezone: Optional[str] = Field(None, max_length=60)
    load_strategy: Optional[IngestionLoadStrategy] = None
    watermark_column: Optional[str] = Field(None, max_length=120)
    primary_key_column: Optional[str] = Field(None, max_length=120)
    target_schema: Optional[str] = Field(None, max_length=120)
    target_table_name: Optional[str] = Field(None, max_length=200)
    batch_size: Optional[int] = Field(None, ge=1, le=100_000)
    is_active: Optional[bool] = None


class IngestionScheduleRead(IngestionScheduleBase, TimestampSchema):
    id: UUID
    last_watermark_timestamp: Optional[datetime]
    last_watermark_id: Optional[int]
    last_run_started_at: Optional[datetime]
    last_run_completed_at: Optional[datetime]
    last_run_status: Optional[str]
    last_run_error: Optional[str]
    total_runs: int
    total_rows_loaded: int


class IngestionRunStatus(str, Enum):
    SCHEDULED = "scheduled"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"


class IngestionRunRead(TimestampSchema):
    id: UUID
    ingestion_schedule_id: UUID
    status: IngestionRunStatus
    started_at: Optional[datetime]
    completed_at: Optional[datetime]
    rows_loaded: Optional[int]
    watermark_timestamp_before: Optional[datetime]
    watermark_timestamp_after: Optional[datetime]
    watermark_id_before: Optional[int]
    watermark_id_after: Optional[int]
    query_text: Optional[str]
    error_message: Optional[str]


class PreLoadValidationResultBase(BaseModel):
    release_id: UUID
    name: str = Field(..., max_length=200)
    status: str = Field("pending", max_length=50)
    total_checks: int = Field(0, ge=0)
    passed_checks: int = Field(0, ge=0)
    failed_checks: int = Field(0, ge=0)
    executed_at: Optional[datetime] = None
    notes: Optional[str] = None


class PreLoadValidationResultCreate(PreLoadValidationResultBase):
    pass


class PreLoadValidationResultUpdate(BaseModel):
    release_id: Optional[UUID] = None
    name: Optional[str] = Field(None, max_length=200)
    status: Optional[str] = Field(None, max_length=50)
    total_checks: Optional[int] = Field(None, ge=0)
    passed_checks: Optional[int] = Field(None, ge=0)
    failed_checks: Optional[int] = Field(None, ge=0)
    executed_at: Optional[datetime] = None
    notes: Optional[str] = None


class PreLoadValidationResultRead(PreLoadValidationResultBase, TimestampSchema):
    id: UUID


class PreLoadValidationIssueBase(BaseModel):
    validation_result_id: UUID
    description: str
    severity: str = Field("medium", max_length=50)
    status: str = Field("open", max_length=50)
    record_identifier: Optional[str] = Field(None, max_length=200)
    resolution_notes: Optional[str] = None


class PreLoadValidationIssueCreate(PreLoadValidationIssueBase):
    pass


class PreLoadValidationIssueUpdate(BaseModel):
    validation_result_id: Optional[UUID] = None
    description: Optional[str] = None
    severity: Optional[str] = Field(None, max_length=50)
    status: Optional[str] = Field(None, max_length=50)
    record_identifier: Optional[str] = Field(None, max_length=200)
    resolution_notes: Optional[str] = None


class PreLoadValidationIssueRead(PreLoadValidationIssueBase, TimestampSchema):
    id: UUID


class PostLoadValidationResultBase(BaseModel):
    release_id: UUID
    name: str = Field(..., max_length=200)
    status: str = Field("pending", max_length=50)
    total_checks: int = Field(0, ge=0)
    passed_checks: int = Field(0, ge=0)
    failed_checks: int = Field(0, ge=0)
    executed_at: Optional[datetime] = None
    notes: Optional[str] = None


class PostLoadValidationResultCreate(PostLoadValidationResultBase):
    pass


class PostLoadValidationResultUpdate(BaseModel):
    release_id: Optional[UUID] = None
    name: Optional[str] = Field(None, max_length=200)
    status: Optional[str] = Field(None, max_length=50)
    total_checks: Optional[int] = Field(None, ge=0)
    passed_checks: Optional[int] = Field(None, ge=0)
    failed_checks: Optional[int] = Field(None, ge=0)
    executed_at: Optional[datetime] = None
    notes: Optional[str] = None


class PostLoadValidationResultRead(PostLoadValidationResultBase, TimestampSchema):
    id: UUID


class PostLoadValidationIssueBase(BaseModel):
    validation_result_id: UUID
    description: str
    severity: str = Field("medium", max_length=50)
    status: str = Field("open", max_length=50)
    record_identifier: Optional[str] = Field(None, max_length=200)
    resolution_notes: Optional[str] = None


class PostLoadValidationIssueCreate(PostLoadValidationIssueBase):
    pass


class PostLoadValidationIssueUpdate(BaseModel):
    validation_result_id: Optional[UUID] = None
    description: Optional[str] = None
    severity: Optional[str] = Field(None, max_length=50)
    status: Optional[str] = Field(None, max_length=50)
    record_identifier: Optional[str] = Field(None, max_length=200)
    resolution_notes: Optional[str] = None


class PostLoadValidationIssueRead(PostLoadValidationIssueBase, TimestampSchema):
    id: UUID


class ValidationApprovalRole(str, Enum):
    SME = "sme"
    DATA_OWNER = "data_owner"
    APPROVER = "approver"
    ADMIN = "admin"


class ValidationApprovalDecision(str, Enum):
    APPROVED = "approved"
    REJECTED = "rejected"


class PreLoadValidationApprovalBase(BaseModel):
    pre_load_validation_result_id: UUID
    approver_id: UUID
    role: ValidationApprovalRole
    decision: ValidationApprovalDecision
    comments: Optional[str] = None
    approved_at: Optional[datetime] = None


class PreLoadValidationApprovalCreate(PreLoadValidationApprovalBase):
    pass


class PreLoadValidationApprovalUpdate(BaseModel):
    pre_load_validation_result_id: Optional[UUID] = None
    approver_id: Optional[UUID] = None
    role: Optional[ValidationApprovalRole] = None
    decision: Optional[ValidationApprovalDecision] = None
    comments: Optional[str] = None
    approved_at: Optional[datetime] = None


class PreLoadValidationApprovalRead(PreLoadValidationApprovalBase, TimestampSchema):
    id: UUID


class PostLoadValidationApprovalBase(BaseModel):
    post_load_validation_result_id: UUID
    approver_id: UUID
    role: ValidationApprovalRole
    decision: ValidationApprovalDecision
    comments: Optional[str] = None
    approved_at: Optional[datetime] = None


class PostLoadValidationApprovalCreate(PostLoadValidationApprovalBase):
    pass


class PostLoadValidationApprovalUpdate(BaseModel):
    post_load_validation_result_id: Optional[UUID] = None
    approver_id: Optional[UUID] = None
    role: Optional[ValidationApprovalRole] = None
    decision: Optional[ValidationApprovalDecision] = None
    comments: Optional[str] = None
    approved_at: Optional[datetime] = None


class PostLoadValidationApprovalRead(PostLoadValidationApprovalBase, TimestampSchema):
    id: UUID


class MappingSetStatus(str, Enum):
    DRAFT = "draft"
    ACTIVE = "active"
    SUPERSEDED = "superseded"
    ARCHIVED = "archived"


class MappingSetBase(BaseModel):
    release_id: UUID
    version: int = Field(..., ge=1)
    status: MappingSetStatus = MappingSetStatus.DRAFT
    notes: Optional[str] = None
    created_by: Optional[UUID] = None


class MappingSetCreate(MappingSetBase):
    pass


class MappingSetUpdate(BaseModel):
    status: Optional[MappingSetStatus] = None
    notes: Optional[str] = None
    created_by: Optional[UUID] = None


class MappingSetRead(MappingSetBase, TimestampSchema):
    id: UUID


class MappingStatus(str, Enum):
    DRAFT = "draft"
    APPROVED = "approved"
    REJECTED = "rejected"


class MappingBase(BaseModel):
    mapping_set_id: UUID
    source_field_id: Optional[UUID] = None
    target_field_id: UUID
    transformation_rule: Optional[str] = None
    default_value: Optional[str] = None
    notes: Optional[str] = None
    status: MappingStatus = MappingStatus.DRAFT
    created_by: Optional[UUID] = None


class MappingCreate(MappingBase):
    pass


class MappingUpdate(BaseModel):
    source_field_id: Optional[UUID] = None
    target_field_id: Optional[UUID] = None
    transformation_rule: Optional[str] = None
    default_value: Optional[str] = None
    notes: Optional[str] = None
    status: Optional[MappingStatus] = None
    created_by: Optional[UUID] = None


class MappingRead(MappingBase, TimestampSchema):
    id: UUID
