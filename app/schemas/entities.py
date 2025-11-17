import base64
import binascii
import re
from datetime import datetime
from enum import Enum
from typing import Any, Optional
from uuid import UUID

from pydantic import BaseModel, EmailStr, Field, root_validator, validator


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
    display_order: int = 0


class ConstructedFieldCreate(ConstructedFieldBase):
    pass


class ConstructedFieldUpdate(BaseModel):
    constructed_table_id: Optional[UUID] = None
    name: Optional[str] = Field(None, max_length=200)
    data_type: Optional[str] = Field(None, max_length=100)
    is_nullable: Optional[bool] = None
    default_value: Optional[str] = None
    description: Optional[str] = None
    display_order: Optional[int] = None


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
    ruleName: Optional[str] = None
    ruleType: Optional[str] = None


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
    is_system_generated: bool


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


class LegalRequirementBase(BaseModel):
    name: str = Field(..., max_length=200)
    description: Optional[str] = None
    status: str = Field("active", max_length=50)
    display_order: Optional[int] = Field(None, ge=0)


class LegalRequirementCreate(LegalRequirementBase):
    pass


class LegalRequirementUpdate(BaseModel):
    name: Optional[str] = Field(None, max_length=200)
    description: Optional[str] = None
    status: Optional[str] = Field(None, max_length=50)
    display_order: Optional[int] = Field(None, ge=0)


class LegalRequirementRead(LegalRequirementBase, TimestampSchema):
    id: UUID


class SecurityClassificationBase(BaseModel):
    name: str = Field(..., max_length=120)
    description: Optional[str] = None
    status: str = Field("active", max_length=50)
    display_order: Optional[int] = Field(None, ge=0)


class SecurityClassificationCreate(SecurityClassificationBase):
    pass


class SecurityClassificationUpdate(BaseModel):
    name: Optional[str] = Field(None, max_length=120)
    description: Optional[str] = None
    status: Optional[str] = Field(None, max_length=50)
    display_order: Optional[int] = Field(None, ge=0)


class SecurityClassificationRead(SecurityClassificationBase, TimestampSchema):
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
    legal_requirement_id: Optional[UUID] = None
    security_classification_id: Optional[UUID] = None
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
    legal_requirement_id: Optional[UUID] = None
    security_classification_id: Optional[UUID] = None
    data_validation: Optional[str] = None
    reference_table: Optional[str] = Field(None, max_length=200)
    grouping_tab: Optional[str] = Field(None, max_length=200)


class FieldRead(FieldBase, TimestampSchema):
    id: UUID
    legal_requirement: Optional[LegalRequirementRead] = None
    security_classification: Optional[SecurityClassificationRead] = None


class DataDefinitionFieldInput(BaseModel):
    field_id: UUID
    notes: Optional[str] = None
    display_order: Optional[int] = None
    is_unique: Optional[bool] = False


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
    display_order: int
    is_unique: bool = False
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


LEGACY_RELATIONSHIP_TO_JOIN_TYPE = {
    "one_to_one": DataDefinitionJoinType.INNER,
    "one_to_many": DataDefinitionJoinType.LEFT,
    "many_to_one": DataDefinitionJoinType.RIGHT,
    "many_to_many": DataDefinitionJoinType.INNER,
}


JOIN_TYPE_TO_LEGACY_RELATIONSHIP = {
    DataDefinitionJoinType.INNER: "one_to_one",
    DataDefinitionJoinType.LEFT: "one_to_many",
    DataDefinitionJoinType.RIGHT: "many_to_one",
}


def _normalize_join_type(value: Any) -> DataDefinitionJoinType | None:
    if value is None:
        return None
    if isinstance(value, DataDefinitionJoinType):
        return value
    if isinstance(value, str):
        lowered = value.lower()
        if lowered in DataDefinitionJoinType._value2member_map_:
            return DataDefinitionJoinType(lowered)
        legacy = LEGACY_RELATIONSHIP_TO_JOIN_TYPE.get(lowered)
        if legacy is not None:
            return legacy
    raise ValueError("Invalid relationship type. Expected inner/left/right or legacy cardinality values.")


class DataDefinitionRelationshipBase(BaseModel):
    primary_field_id: UUID
    foreign_field_id: UUID
    join_type: DataDefinitionJoinType = Field(
        DataDefinitionJoinType.INNER, alias="relationship_type"
    )
    notes: Optional[str] = None

    class Config:
        allow_population_by_field_name = True

    @validator("join_type", pre=True)
    def _translate_join_type(cls, value):
        return _normalize_join_type(value)

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
    join_type: Optional[DataDefinitionJoinType] = Field(
        None, alias="relationship_type"
    )
    notes: Optional[str] = None

    @validator("join_type", pre=True)
    def _translate_join_type(cls, value):
        return _normalize_join_type(value)

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
    relationship_type: str = Field(default=None, alias="relationship_type")
    notes: Optional[str] = None
    primary_field: DataDefinitionFieldRead
    foreign_field: DataDefinitionFieldRead

    class Config:
        orm_mode = True
        allow_population_by_field_name = True

    @root_validator(pre=True)
    def _populate_relationship_type(cls, values):
        values = dict(values)
        join_value = values.get("join_type")
        if join_value is not None and values.get("relationship_type") is None:
            join_enum = _normalize_join_type(join_value)
            values["join_type"] = join_enum
            values["relationship_type"] = JOIN_TYPE_TO_LEGACY_RELATIONSHIP.get(
                join_enum,
                join_enum.value if join_enum else None,
            )
        return values


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
    connection_string: Optional[str] = Field(None, min_length=1)
    use_databricks_managed_connection: bool = False

    @root_validator
    def _require_connection(cls, values: dict) -> dict:
        use_managed = values.get("use_databricks_managed_connection")
        connection_string = values.get("connection_string")
        if not use_managed and not connection_string:
            raise ValueError(
                "connection_string is required unless use_databricks_managed_connection is true."
            )
        return values


class SystemConnectionUpdate(BaseModel):
    system_id: Optional[UUID] = None
    connection_type: Optional[SystemConnectionType] = None
    connection_string: Optional[str] = Field(None, min_length=1)
    auth_method: Optional[SystemConnectionAuthMethod] = None
    active: Optional[bool] = None
    ingestion_enabled: Optional[bool] = None
    notes: Optional[str] = None
    use_databricks_managed_connection: Optional[bool] = None


class SystemConnectionRead(SystemConnectionBase, TimestampSchema):
    id: UUID
    uses_databricks_managed_connection: bool = False

    @root_validator(pre=True)
    def _detect_managed_databricks(cls, values: dict) -> dict:
        connection_string = values.get("connection_string") or ""
        values = dict(values)
        values["uses_databricks_managed_connection"] = connection_string.startswith("jdbc:databricks://token:@")
        return values


class SystemConnectionTestRequest(BaseModel):
    connection_type: SystemConnectionType = SystemConnectionType.JDBC
    connection_string: str


class SystemConnectionTestResult(BaseModel):
    success: bool
    message: str
    duration_ms: float | None = None
    connection_summary: str | None = None


class DatabricksSqlSettingBase(BaseModel):
    display_name: str = Field("Primary Warehouse", max_length=120)
    workspace_host: str = Field(..., min_length=3, max_length=255)
    http_path: str = Field(..., min_length=3, max_length=400)
    catalog: Optional[str] = Field(None, max_length=120)
    schema_name: Optional[str] = Field(None, max_length=120)
    constructed_schema: Optional[str] = Field(None, max_length=120)
    data_quality_schema: Optional[str] = Field(None, max_length=120)
    data_quality_storage_format: str = Field(
        "delta",
        regex=r"^(delta|hudi)$",
        description="Storage format for data quality metadata tables.",
    )
    data_quality_auto_manage_tables: bool = Field(
        True,
        description="Automatically manage data quality metadata tables when true.",
    )
    ingestion_method: str = Field(
        "sql",
        regex=r"^(sql|spark)$",
        description="Preferred ingestion method for constructed and ingestion data.",
    )
    @validator("ingestion_method", pre=True, always=True)
    def _normalize_ingestion_method(cls, value: str | None) -> str:
        if not value:
            return "sql"
        lowered = value.strip().lower()
        if lowered not in {"sql", "spark"}:
            raise ValueError("ingestion method must be 'sql' or 'spark'")
        return lowered
    @validator("data_quality_storage_format", pre=True, always=True)
    def _normalize_data_quality_format(cls, value: str | None) -> str:
        if not value:
            return "delta"
        lowered = value.strip().lower()
        if lowered not in {"delta", "hudi"}:
            raise ValueError("data quality storage format must be 'delta' or 'hudi'")
        return lowered
    warehouse_name: Optional[str] = Field(None, max_length=180)
    ingestion_batch_rows: Optional[int] = Field(
        None,
        ge=1,
        le=100_000,
        description="Maximum number of rows to include in each Databricks insert batch.",
    )
    spark_compute: Optional[str] = Field(
        None,
        regex=r"^(classic|serverless)$",
        description="Compute mode to use for Spark Connect ingestion (classic cluster vs serverless).",
    )

    @validator("workspace_host", "http_path", pre=True)
    def _strip_value(cls, value: str | None) -> str | None:
        if isinstance(value, str):
            return value.strip()
        return value

    @validator(
        "catalog",
        "schema_name",
        "constructed_schema",
        "data_quality_schema",
        "warehouse_name",
        "spark_compute",
        pre=True,
    )
    def _strip_optional_value(cls, value: str | None) -> str | None:
        if isinstance(value, str):
            return value.strip()
        return value

    @validator("spark_compute", pre=True, always=False)
    def _normalize_compute(cls, value: str | None) -> str | None:
        if value is None:
            return None
        lowered = value.strip().lower()
        if lowered not in {"classic", "serverless"}:
            raise ValueError("spark compute must be 'classic' or 'serverless'")
        return lowered


class DatabricksSqlSettingCreate(DatabricksSqlSettingBase):
    access_token: str = Field(..., min_length=10)


class DatabricksSqlSettingUpdate(BaseModel):
    display_name: Optional[str] = Field(None, max_length=120)
    workspace_host: Optional[str] = Field(None, min_length=3, max_length=255)
    http_path: Optional[str] = Field(None, min_length=3, max_length=400)
    access_token: Optional[str | None] = Field(None, min_length=10)
    catalog: Optional[str | None] = Field(None, max_length=120)
    schema_name: Optional[str | None] = Field(None, max_length=120)
    constructed_schema: Optional[str | None] = Field(None, max_length=120)
    data_quality_schema: Optional[str | None] = Field(None, max_length=120)
    data_quality_storage_format: Optional[str | None] = Field(None, regex=r"^(delta|hudi)$")
    data_quality_auto_manage_tables: Optional[bool] = None
    ingestion_method: Optional[str | None] = Field(
        None,
        regex=r"^(sql|spark)$",
    )
    @validator("ingestion_method")
    def _normalize_update_method(cls, value: str | None) -> str | None:
        if value is None:
            return None
        lowered = value.strip().lower()
        if lowered not in {"sql", "spark"}:
            raise ValueError("ingestion method must be 'sql' or 'spark'")
        return lowered
    @validator("data_quality_storage_format")
    def _normalize_update_data_quality_format(cls, value: str | None) -> str | None:
        if value is None:
            return None
        lowered = value.strip().lower()
        if lowered not in {"delta", "hudi"}:
            raise ValueError("data quality storage format must be 'delta' or 'hudi'")
        return lowered
    warehouse_name: Optional[str | None] = Field(None, max_length=180)
    ingestion_batch_rows: Optional[int | None] = Field(
        None,
        ge=1,
        le=100_000,
    )
    spark_compute: Optional[str | None] = Field(None, regex=r"^(classic|serverless)$")
    is_active: Optional[bool] = None

    @validator(
        "workspace_host",
        "http_path",
        "display_name",
        "catalog",
        "schema_name",
        "constructed_schema",
        "data_quality_schema",
        "data_quality_storage_format",
        "warehouse_name",
        "spark_compute",
        pre=True,
    )
    def _strip_optional(cls, value: str | None) -> str | None:
        if isinstance(value, str):
            return value.strip()
        return value


class DatabricksSqlSettingRead(DatabricksSqlSettingBase, TimestampSchema):
    id: UUID
    is_active: bool = True
    has_access_token: bool = Field(False, description="True when an access token is stored server-side.")


class DatabricksSqlSettingTestRequest(BaseModel):
    workspace_host: str = Field(..., min_length=3, max_length=255)
    http_path: str = Field(..., min_length=3, max_length=400)
    access_token: str = Field(..., min_length=10)
    catalog: Optional[str] = Field(None, max_length=120)
    schema_name: Optional[str] = Field(None, max_length=120)
    constructed_schema: Optional[str] = Field(None, max_length=120)
    data_quality_schema: Optional[str] = Field(None, max_length=120)
    data_quality_storage_format: Optional[str] = Field(None, regex=r"^(delta|hudi)$")
    data_quality_auto_manage_tables: Optional[bool] = None
    ingestion_method: Optional[str] = Field(None, regex=r"^(sql|spark)$")
    ingestion_batch_rows: Optional[int] = Field(None, ge=1, le=100_000)
    spark_compute: Optional[str] = Field(None, regex=r"^(classic|serverless)$")


class DatabricksSqlSettingTestResult(BaseModel):
    success: bool
    message: str
    duration_ms: float | None = None


class SapHanaSettingBase(BaseModel):
    display_name: str = Field("SAP HANA Warehouse", max_length=120)
    host: str = Field(..., min_length=3, max_length=255)
    port: int = Field(30015, ge=1, le=65_535)
    database_name: str = Field(..., min_length=1, max_length=120)
    username: str = Field(..., min_length=1, max_length=120)
    schema_name: Optional[str] = Field(None, max_length=120)
    tenant: Optional[str] = Field(None, max_length=120)
    use_ssl: bool = True
    ingestion_batch_rows: Optional[int] = Field(None, ge=1, le=100_000)

    @validator(
        "display_name",
        "host",
        "database_name",
        "username",
        "schema_name",
        "tenant",
        pre=True,
    )
    def _strip_strings(cls, value: str | None) -> str | None:
        if isinstance(value, str):
            return value.strip()
        return value


class SapHanaSettingCreate(SapHanaSettingBase):
    password: str = Field(..., min_length=4)


class SapHanaSettingUpdate(BaseModel):
    display_name: Optional[str] = Field(None, max_length=120)
    host: Optional[str] = Field(None, min_length=3, max_length=255)
    port: Optional[int] = Field(None, ge=1, le=65_535)
    database_name: Optional[str] = Field(None, min_length=1, max_length=120)
    username: Optional[str] = Field(None, min_length=1, max_length=120)
    password: Optional[str | None] = Field(None, min_length=4)
    schema_name: Optional[str | None] = Field(None, max_length=120)
    tenant: Optional[str | None] = Field(None, max_length=120)
    use_ssl: Optional[bool] = None
    ingestion_batch_rows: Optional[int | None] = Field(None, ge=1, le=100_000)
    is_active: Optional[bool] = None

    @validator(
        "display_name",
        "host",
        "database_name",
        "username",
        "schema_name",
        "tenant",
        pre=True,
    )
    def _strip_optional_strings(cls, value: str | None) -> str | None:
        if isinstance(value, str):
            return value.strip()
        return value


class SapHanaSettingRead(SapHanaSettingBase, TimestampSchema):
    id: UUID
    is_active: bool = True
    has_password: bool = Field(False, description="True when credentials are stored server-side.")


class SapHanaSettingTestRequest(BaseModel):
    host: str = Field(..., min_length=3, max_length=255)
    port: int = Field(30015, ge=1, le=65_535)
    database_name: str = Field(..., min_length=1, max_length=120)
    username: str = Field(..., min_length=1, max_length=120)
    password: str = Field(..., min_length=4)
    schema_name: Optional[str] = Field(None, max_length=120)
    tenant: Optional[str] = Field(None, max_length=120)
    use_ssl: bool = True
    ingestion_batch_rows: Optional[int] = Field(None, ge=1, le=100_000)


class SapHanaSettingTestResult(BaseModel):
    success: bool
    message: str
    duration_ms: float | None = None


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


class DataWarehouseTarget(str, Enum):
    DATABRICKS_SQL = "databricks_sql"


class IngestionScheduleBase(BaseModel):
    connection_table_selection_id: UUID
    schedule_expression: str = Field(..., max_length=120)
    timezone: Optional[str] = Field(None, max_length=60)
    load_strategy: IngestionLoadStrategy = IngestionLoadStrategy.TIMESTAMP
    watermark_column: Optional[str] = Field(None, max_length=120)
    primary_key_column: Optional[str] = Field(None, max_length=120)
    target_schema: Optional[str] = Field(None, max_length=120)
    target_table_name: Optional[str] = Field(None, max_length=200)
    target_warehouse: DataWarehouseTarget = DataWarehouseTarget.DATABRICKS_SQL
    sap_hana_setting_id: Optional[UUID] = None
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
    target_warehouse: Optional[DataWarehouseTarget] = None
    sap_hana_setting_id: Optional[UUID | None] = None
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
    rows_expected: Optional[int]
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


class ApplicationDatabaseEngine(str, Enum):
    DEFAULT_POSTGRES = "default_postgres"
    CUSTOM_POSTGRES = "custom_postgres"
    SQLSERVER = "sqlserver"


class ApplicationDatabaseConnectionInput(BaseModel):
    host: Optional[str] = Field(None, max_length=255)
    port: Optional[int] = Field(None, ge=1, le=65535)
    database: Optional[str] = Field(None, max_length=255)
    username: Optional[str] = Field(None, max_length=255)
    password: Optional[str] = Field(None, max_length=255)
    options: Optional[dict[str, str]] = None
    use_ssl: Optional[bool] = None


class ApplicationDatabaseTestRequest(BaseModel):
    engine: ApplicationDatabaseEngine
    connection: Optional[ApplicationDatabaseConnectionInput] = None

    @root_validator
    def _validate_connection(cls, values):
        engine = values.get("engine")
        connection = values.get("connection")
        if engine in (ApplicationDatabaseEngine.CUSTOM_POSTGRES, ApplicationDatabaseEngine.SQLSERVER):
            if not connection:
                raise ValueError("Connection details are required for custom database engines.")
            required_fields = ["host", "database", "username", "password"]
            missing = [field for field in required_fields if not getattr(connection, field)]
            if missing:
                raise ValueError(f"Missing required connection fields: {', '.join(missing)}")
        return values


class ApplicationDatabaseTestResult(BaseModel):
    success: bool
    message: str
    latency_ms: Optional[float] = None


class ApplicationDatabaseApplyRequest(ApplicationDatabaseTestRequest):
    display_name: Optional[str] = Field(None, max_length=200)


class ApplicationDatabaseSettingRead(TimestampSchema):
    id: UUID
    engine: ApplicationDatabaseEngine
    connection_display: Optional[str]
    applied_at: datetime
    display_name: Optional[str] = None


class ApplicationDatabaseStatus(BaseModel):
    configured: bool
    setting: Optional[ApplicationDatabaseSettingRead] = None
    admin_email: Optional[EmailStr] = None


class AdminEmailSetting(BaseModel):
    email: Optional[EmailStr] = None


class AdminEmailUpdate(BaseModel):
    email: EmailStr


TRANSPARENT_SAFE_LOGO_MIME_TYPES = {"image/png", "image/svg+xml", "image/webp"}
MAX_LOGO_BYTES = 350_000
DEFAULT_THEME_MODE = "light"
DEFAULT_ACCENT_COLOR = "#1e88e5"
SUPPORTED_THEME_MODES = {"light", "dark"}
ACCENT_COLOR_PATTERN = re.compile(r"^#(?:[0-9a-fA-F]{6})$")


class CompanySettings(BaseModel):
    site_title: Optional[str] = Field(None, max_length=120)
    # Base64-encoded payload expands the raw bytes by roughly 4/3, so allow extra headroom here.
    logo_data_url: Optional[str] = Field(None, max_length=MAX_LOGO_BYTES * 2)
    theme_mode: Optional[str] = Field(None)
    accent_color: Optional[str] = Field(None)

    @validator("site_title", pre=True)
    def _normalize_site_title(cls, value: Optional[str]) -> Optional[str]:
        if value is None:
            return None
        trimmed = value.strip()
        return trimmed or None

    @validator("logo_data_url", pre=True)
    def _normalize_logo(cls, value: Optional[str]) -> Optional[str]:
        if value is None:
            return None
        trimmed = value.strip()
        return trimmed or None

    @validator("logo_data_url")
    def _validate_logo(cls, value: Optional[str]) -> Optional[str]:
        if value is None:
            return None
        match = re.match(r"^data:([^;]+);base64,", value, flags=re.IGNORECASE)
        if not match:
            raise ValueError(
                "Logo must be provided as a base64-encoded data URL (e.g. data:image/png;base64,...)."
            )

        mime_type = match.group(1).lower()
        if mime_type not in TRANSPARENT_SAFE_LOGO_MIME_TYPES:
            allowed = ", ".join(sorted(TRANSPARENT_SAFE_LOGO_MIME_TYPES))
            raise ValueError(f"Logo image format '{mime_type}' is not supported. Allowed formats: {allowed}.")

        encoded_payload = value[match.end() :]
        try:
            decoded_bytes = base64.b64decode(encoded_payload, validate=True)
        except binascii.Error as exc:  # pragma: no cover - defensive guard
            raise ValueError("Logo data must contain valid base64 content.") from exc

        if len(decoded_bytes) > MAX_LOGO_BYTES:
            raise ValueError("Logo data exceeds the maximum allowed size (350 KB).")

        return value

    @validator("theme_mode")
    def _validate_theme_mode(cls, value: Optional[str]) -> Optional[str]:
        if value is None:
            return None
        normalized = value.strip().lower()
        if normalized not in SUPPORTED_THEME_MODES:
            allowed = ", ".join(sorted(SUPPORTED_THEME_MODES))
            raise ValueError(f"Theme mode must be one of: {allowed}.")
        return normalized

    @validator("accent_color")
    def _validate_accent_color(cls, value: Optional[str]) -> Optional[str]:
        if value is None:
            return None
        normalized = value.strip().lower()
        if not ACCENT_COLOR_PATTERN.match(normalized):
            raise ValueError("Accent color must be a 6-digit hex code (e.g. #1e88e5).")
        return normalized


class CompanySettingsRead(CompanySettings):
    pass


class CompanySettingsUpdate(CompanySettings):
    class Config:
        extra = "forbid"
