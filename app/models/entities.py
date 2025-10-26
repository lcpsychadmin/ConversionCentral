import uuid
from datetime import datetime
from typing import Optional

import sqlalchemy as sa
from sqlalchemy import BigInteger, Boolean, DateTime, ForeignKey, Integer, JSON, String, Text
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.database import Base


class TimestampMixin:
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=datetime.utcnow
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=datetime.utcnow, onupdate=datetime.utcnow
    )


class User(Base, TimestampMixin):
    __tablename__ = "users"

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True, default=uuid.uuid4
    )
    name: Mapped[str] = mapped_column(String(200), nullable=False)
    email: Mapped[str | None] = mapped_column(String(200), nullable=True, unique=True)
    status: Mapped[str] = mapped_column(String(50), nullable=False, default="active")

    field_loads: Mapped[list["FieldLoad"]] = relationship(
        "FieldLoad",
        back_populates="creator",
        cascade="all, delete-orphan",
        passive_deletes=True,
    )
    mapping_sets: Mapped[list["MappingSet"]] = relationship(
        "MappingSet",
        back_populates="creator",
        cascade="all, delete-orphan",
        passive_deletes=True,
    )
    mappings: Mapped[list["Mapping"]] = relationship(
        "Mapping",
        back_populates="creator",
        cascade="all, delete-orphan",
        passive_deletes=True,
    )
    pre_load_validation_approvals: Mapped[list["PreLoadValidationApproval"]] = relationship(
        "PreLoadValidationApproval",
        back_populates="approver",
        cascade="all, delete-orphan",
        passive_deletes=True,
    )
    post_load_validation_approvals: Mapped[list["PostLoadValidationApproval"]] = relationship(
        "PostLoadValidationApproval",
        back_populates="approver",
        cascade="all, delete-orphan",
        passive_deletes=True,
    )
    constructed_table_approvals: Mapped[list["ConstructedTableApproval"]] = relationship(
        "ConstructedTableApproval",
        back_populates="approver",
        cascade="all, delete-orphan",
        passive_deletes=True,
    )
    process_area_assignments: Mapped[list["ProcessAreaRoleAssignment"]] = relationship(
        "ProcessAreaRoleAssignment",
        back_populates="user",
        cascade="all, delete-orphan",
        passive_deletes=True,
        foreign_keys="[ProcessAreaRoleAssignment.user_id]",
    )
    dependency_approvals: Mapped[list["DependencyApproval"]] = relationship(
        "DependencyApproval",
        back_populates="approver",
        cascade="all, delete-orphan",
        passive_deletes=True,
        foreign_keys="[DependencyApproval.approver_id]",
    )
    table_load_order_approvals: Mapped[list["TableLoadOrderApproval"]] = relationship(
        "TableLoadOrderApproval",
        back_populates="approver",
        cascade="all, delete-orphan",
        passive_deletes=True,
        foreign_keys="[TableLoadOrderApproval.approver_id]",
    )


class Project(Base, TimestampMixin):
    __tablename__ = "projects"

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True, default=uuid.uuid4
    )
    name: Mapped[str] = mapped_column(String(200), unique=True, nullable=False)
    description: Mapped[str | None] = mapped_column(Text, nullable=True)
    status: Mapped[str] = mapped_column(String(50), nullable=False, default="planned")

    releases: Mapped[list["Release"]] = relationship(
        "Release",
        back_populates="project",
        cascade="all, delete-orphan",
        passive_deletes=True,
    )
class Release(Base, TimestampMixin):
    __tablename__ = "releases"

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True, default=uuid.uuid4
    )
    project_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("projects.id", ondelete="CASCADE"), nullable=False
    )
    name: Mapped[str] = mapped_column(String(200), nullable=False)
    description: Mapped[str | None] = mapped_column(Text, nullable=True)
    status: Mapped[str] = mapped_column(String(50), nullable=False, default="planned")

    project: Mapped[Project] = relationship("Project", back_populates="releases")
    mock_cycles: Mapped[list["MockCycle"]] = relationship(
        "MockCycle",
        back_populates="release",
        cascade="all, delete-orphan",
        passive_deletes=True,
    )
    field_loads: Mapped[list["FieldLoad"]] = relationship(
        "FieldLoad",
        back_populates="release",
        cascade="all, delete-orphan",
        passive_deletes=True,
    )
    mapping_sets: Mapped[list["MappingSet"]] = relationship(
        "MappingSet",
        back_populates="release",
        cascade="all, delete-orphan",
        passive_deletes=True,
    )
    release_data_objects: Mapped[list["ReleaseDataObject"]] = relationship(
        "ReleaseDataObject",
        back_populates="release",
        cascade="all, delete-orphan",
        passive_deletes=True,
        overlaps="data_objects",
    )
    data_objects: Mapped[list["DataObject"]] = relationship(
        "DataObject",
        secondary="release_data_objects",
        back_populates="releases",
        viewonly=True,
        overlaps="release_data_objects,release_links",
    )
    pre_load_validation_results: Mapped[list["PreLoadValidationResult"]] = relationship(
        "PreLoadValidationResult",
        back_populates="release",
        cascade="all, delete-orphan",
        passive_deletes=True,
    )
    post_load_validation_results: Mapped[list["PostLoadValidationResult"]] = relationship(
        "PostLoadValidationResult",
        back_populates="release",
        cascade="all, delete-orphan",
        passive_deletes=True,
    )


class MockCycle(Base, TimestampMixin):
    __tablename__ = "mock_cycles"

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True, default=uuid.uuid4
    )
    release_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("releases.id", ondelete="CASCADE"), nullable=False
    )
    name: Mapped[str] = mapped_column(String(200), nullable=False)
    description: Mapped[str | None] = mapped_column(Text, nullable=True)
    status: Mapped[str] = mapped_column(String(50), nullable=False, default="planned")

    release: Mapped[Release] = relationship("Release", back_populates="mock_cycles")
    execution_contexts: Mapped[list["ExecutionContext"]] = relationship(
        "ExecutionContext",
        back_populates="mock_cycle",
        cascade="all, delete-orphan",
        passive_deletes=True,
    )


class ExecutionContext(Base, TimestampMixin):
    __tablename__ = "execution_contexts"

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True, default=uuid.uuid4
    )
    mock_cycle_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("mock_cycles.id", ondelete="CASCADE"), nullable=False
    )
    name: Mapped[str] = mapped_column(String(200), nullable=False)
    description: Mapped[str | None] = mapped_column(Text, nullable=True)
    status: Mapped[str] = mapped_column(String(50), nullable=False, default="planned")

    mock_cycle: Mapped[MockCycle] = relationship("MockCycle", back_populates="execution_contexts")
    ingestion_jobs: Mapped[list["IngestionJob"]] = relationship(
        "IngestionJob",
        back_populates="execution_context",
        cascade="all, delete-orphan",
        passive_deletes=True,
    )
    constructed_tables: Mapped[list["ConstructedTable"]] = relationship(
        "ConstructedTable",
        back_populates="execution_context",
        cascade="all, delete-orphan",
        passive_deletes=True,
    )


class ConstructedTable(Base, TimestampMixin):
    __tablename__ = "constructed_tables"

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True, default=uuid.uuid4
    )
    execution_context_id: Mapped[uuid.UUID | None] = mapped_column(
        UUID(as_uuid=True), ForeignKey("execution_contexts.id", ondelete="CASCADE"), nullable=True
    )
    data_definition_id: Mapped[uuid.UUID | None] = mapped_column(
        UUID(as_uuid=True), ForeignKey("data_definitions.id", ondelete="CASCADE"), nullable=True
    )
    data_definition_table_id: Mapped[uuid.UUID | None] = mapped_column(
        UUID(as_uuid=True), ForeignKey("data_definition_tables.id", ondelete="CASCADE"), nullable=True, unique=True
    )
    name: Mapped[str] = mapped_column(String(200), nullable=False)
    description: Mapped[str | None] = mapped_column(Text, nullable=True)
    purpose: Mapped[str | None] = mapped_column(Text, nullable=True)
    status: Mapped[str] = mapped_column(
        sa.Enum(
            "draft",
            "pending_approval",
            "approved",
            "rejected",
            name="constructed_table_status_enum",
        ),
        nullable=False,
        default="draft",
    )

    execution_context: Mapped[Optional["ExecutionContext"]] = relationship(
        "ExecutionContext", back_populates="constructed_tables"
    )
    data_definition: Mapped[Optional["DataDefinition"]] = relationship(
        "DataDefinition", back_populates="constructed_tables"
    )
    data_definition_table: Mapped[Optional["DataDefinitionTable"]] = relationship(
        "DataDefinitionTable", back_populates="constructed_table"
    )
    fields: Mapped[list["ConstructedField"]] = relationship(
        "ConstructedField",
        back_populates="constructed_table",
        cascade="all, delete-orphan",
        passive_deletes=True,
    )
    data_rows: Mapped[list["ConstructedData"]] = relationship(
        "ConstructedData",
        back_populates="constructed_table",
        cascade="all, delete-orphan",
        passive_deletes=True,
    )
    approvals: Mapped[list["ConstructedTableApproval"]] = relationship(
        "ConstructedTableApproval",
        back_populates="constructed_table",
        cascade="all, delete-orphan",
        passive_deletes=True,
    )
    validation_rules: Mapped[list["ConstructedDataValidationRule"]] = relationship(
        "ConstructedDataValidationRule",
        cascade="all, delete-orphan",
        passive_deletes=True,
    )


class ConstructedField(Base, TimestampMixin):
    __tablename__ = "constructed_fields"

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True, default=uuid.uuid4
    )
    constructed_table_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("constructed_tables.id", ondelete="CASCADE"), nullable=False
    )
    name: Mapped[str] = mapped_column(String(200), nullable=False)
    data_type: Mapped[str] = mapped_column(String(100), nullable=False)
    is_nullable: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)
    default_value: Mapped[str | None] = mapped_column(Text, nullable=True)
    description: Mapped[str | None] = mapped_column(Text, nullable=True)

    constructed_table: Mapped[ConstructedTable] = relationship(
        "ConstructedTable", back_populates="fields"
    )


class ConstructedData(Base, TimestampMixin):
    __tablename__ = "constructed_data"

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True, default=uuid.uuid4
    )
    constructed_table_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("constructed_tables.id", ondelete="CASCADE"), nullable=False
    )
    row_identifier: Mapped[str | None] = mapped_column(String(200), nullable=True)
    payload: Mapped[dict[str, object]] = mapped_column(JSON, nullable=False)

    constructed_table: Mapped[ConstructedTable] = relationship(
        "ConstructedTable", back_populates="data_rows"
    )


class ConstructedTableApproval(Base, TimestampMixin):
    __tablename__ = "constructed_table_approvals"

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True, default=uuid.uuid4
    )
    constructed_table_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("constructed_tables.id", ondelete="CASCADE"), nullable=False
    )
    approver_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("users.id", ondelete="CASCADE"), nullable=False
    )
    role: Mapped[str] = mapped_column(
        sa.Enum(
            "data_steward",
            "business_owner",
            "technical_lead",
            "admin",
            name="constructed_table_approval_role_enum",
        ),
        nullable=False,
    )
    decision: Mapped[str] = mapped_column(
        sa.Enum(
            "approved",
            "rejected",
            name="constructed_table_approval_decision_enum",
        ),
        nullable=False,
    )
    comments: Mapped[str | None] = mapped_column(Text, nullable=True)
    approved_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=datetime.utcnow
    )

    constructed_table: Mapped[ConstructedTable] = relationship(
        "ConstructedTable", back_populates="approvals"
    )
    approver: Mapped[User] = relationship(
        "User", back_populates="constructed_table_approvals"
    )


class ConstructedDataValidationRule(Base, TimestampMixin):
    """Validation rules for constructed table data."""
    __tablename__ = "constructed_data_validation_rules"

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True, default=uuid.uuid4
    )
    constructed_table_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("constructed_tables.id", ondelete="CASCADE"), nullable=False
    )
    name: Mapped[str] = mapped_column(String(200), nullable=False)
    description: Mapped[str | None] = mapped_column(Text, nullable=True)
    rule_type: Mapped[str] = mapped_column(
        sa.Enum(
            "required",  # Field must not be empty
            "unique",  # Value must be unique across all rows
            "range",  # Numeric value between min and max
            "pattern",  # String matches regex pattern
            "custom",  # Custom expression/formula
            "cross_field",  # Validation across multiple fields
            name="validation_rule_type_enum",
        ),
        nullable=False,
    )
    field_id: Mapped[uuid.UUID | None] = mapped_column(
        UUID(as_uuid=True), ForeignKey("constructed_fields.id", ondelete="CASCADE"), nullable=True
    )
    # Rule configuration stored as JSON for flexibility
    # Examples:
    # Required: { "fieldName": "FirstName" }
    # Unique: { "fieldName": "Email" }
    # Range: { "fieldName": "Age", "min": 0, "max": 150 }
    # Pattern: { "fieldName": "Phone", "pattern": "^\\d{3}-\\d{3}-\\d{4}$" }
    # Custom: { "expression": "field1 > 100 AND field2 < 50" }
    # CrossField: { "fields": ["StartDate", "EndDate"], "rule": "StartDate <= EndDate" }
    configuration: Mapped[dict[str, object]] = mapped_column(JSON, nullable=False)
    error_message: Mapped[str] = mapped_column(
        Text, nullable=False, default="Validation failed"
    )
    is_active: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)
    applies_to_new_only: Mapped[bool] = mapped_column(
        Boolean, nullable=False, default=False
    )

    constructed_table: Mapped[ConstructedTable] = relationship(
        "ConstructedTable", passive_deletes=True
    )
    field: Mapped[Optional["ConstructedField"]] = relationship(
        "ConstructedField", passive_deletes=True
    )


class ProcessArea(Base, TimestampMixin):
    __tablename__ = "process_areas"

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True, default=uuid.uuid4
    )
    name: Mapped[str] = mapped_column(String(200), nullable=False)
    description: Mapped[str | None] = mapped_column(Text, nullable=True)
    status: Mapped[str] = mapped_column(String(50), nullable=False, default="draft")
    data_objects: Mapped[list["DataObject"]] = relationship(
        "DataObject",
        back_populates="process_area",
        cascade="all, delete-orphan",
        passive_deletes=True,
    )
    role_assignments: Mapped[list["ProcessAreaRoleAssignment"]] = relationship(
        "ProcessAreaRoleAssignment",
        back_populates="process_area",
        cascade="all, delete-orphan",
        passive_deletes=True,
    )


class Role(Base, TimestampMixin):
    __tablename__ = "roles"

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True, default=uuid.uuid4
    )
    name: Mapped[str] = mapped_column(String(100), nullable=False, unique=True)
    description: Mapped[str | None] = mapped_column(Text, nullable=True)
    is_active: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)

    assignments: Mapped[list["ProcessAreaRoleAssignment"]] = relationship(
        "ProcessAreaRoleAssignment",
        back_populates="role",
        cascade="all, delete-orphan",
        passive_deletes=True,
    )


class ProcessAreaRoleAssignment(Base, TimestampMixin):
    __tablename__ = "process_area_role_assignments"
    __table_args__ = (
        sa.UniqueConstraint(
            "process_area_id",
            "user_id",
            "role_id",
            name="uq_process_area_role_assignment",
        ),
    )

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True, default=uuid.uuid4
    )
    process_area_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("process_areas.id", ondelete="CASCADE"), nullable=False
    )
    user_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("users.id", ondelete="CASCADE"), nullable=False
    )
    role_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("roles.id", ondelete="CASCADE"), nullable=False
    )
    granted_by: Mapped[uuid.UUID | None] = mapped_column(
        UUID(as_uuid=True), ForeignKey("users.id", ondelete="SET NULL"), nullable=True
    )
    notes: Mapped[str | None] = mapped_column(Text, nullable=True)

    process_area: Mapped[ProcessArea] = relationship(
        "ProcessArea", back_populates="role_assignments"
    )
    user: Mapped[User] = relationship(
        "User", foreign_keys=[user_id], back_populates="process_area_assignments"
    )
    role: Mapped[Role] = relationship("Role", back_populates="assignments")
    granter: Mapped[User | None] = relationship(
        "User", foreign_keys=[granted_by]
    )


class DataObject(Base, TimestampMixin):
    __tablename__ = "data_objects"

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True, default=uuid.uuid4
    )
    process_area_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("process_areas.id", ondelete="CASCADE"), nullable=False
    )
    name: Mapped[str] = mapped_column(String(200), nullable=False)
    description: Mapped[str | None] = mapped_column(Text, nullable=True)
    status: Mapped[str] = mapped_column(String(50), nullable=False, default="draft")

    process_area: Mapped[ProcessArea] = relationship("ProcessArea", back_populates="data_objects")
    system_links: Mapped[list["DataObjectSystem"]] = relationship(
        "DataObjectSystem",
        back_populates="data_object",
        cascade="all, delete-orphan",
        passive_deletes=True,
    )
    outgoing_dependencies: Mapped[list["DataObjectDependency"]] = relationship(
        "DataObjectDependency",
        back_populates="predecessor",
        cascade="all, delete-orphan",
        passive_deletes=True,
        foreign_keys="[DataObjectDependency.predecessor_id]",
    )
    incoming_dependencies: Mapped[list["DataObjectDependency"]] = relationship(
        "DataObjectDependency",
        back_populates="successor",
        cascade="all, delete-orphan",
        passive_deletes=True,
        foreign_keys="[DataObjectDependency.successor_id]",
    )
    table_load_orders: Mapped[list["TableLoadOrder"]] = relationship(
        "TableLoadOrder",
        back_populates="data_object",
        cascade="all, delete-orphan",
        passive_deletes=True,
    )
    data_definitions: Mapped[list["DataDefinition"]] = relationship(
        "DataDefinition",
        back_populates="data_object",
        cascade="all, delete-orphan",
        passive_deletes=True,
    )
    release_links: Mapped[list["ReleaseDataObject"]] = relationship(
        "ReleaseDataObject",
        back_populates="data_object",
        cascade="all, delete-orphan",
        passive_deletes=True,
        overlaps="releases",
    )
    releases: Mapped[list[Release]] = relationship(
        "Release",
        secondary="release_data_objects",
        back_populates="data_objects",
        viewonly=True,
        overlaps="release_links,release_data_objects",
    )
    systems: Mapped[list["System"]] = relationship(
        "System",
        secondary="data_object_systems",
        viewonly=True,
        overlaps="system_links,data_object_links",
    )



class ReleaseDataObject(Base, TimestampMixin):
    __tablename__ = "release_data_objects"

    __table_args__ = (
        sa.UniqueConstraint("release_id", "data_object_id", name="uq_release_data_object"),
    )

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True, default=uuid.uuid4
    )
    release_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("releases.id", ondelete="CASCADE"), nullable=False
    )
    data_object_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("data_objects.id", ondelete="CASCADE"), nullable=False
    )

    release: Mapped[Release] = relationship("Release", back_populates="release_data_objects")
    data_object: Mapped[DataObject] = relationship("DataObject", back_populates="release_links")


class System(Base, TimestampMixin):
    __tablename__ = "systems"

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True, default=uuid.uuid4
    )
    name: Mapped[str] = mapped_column(String(200), unique=True, nullable=False)
    physical_name: Mapped[str] = mapped_column(String(200), nullable=False)
    description: Mapped[str | None] = mapped_column(Text, nullable=True)
    system_type: Mapped[str | None] = mapped_column(String(100), nullable=True)
    status: Mapped[str] = mapped_column(String(50), nullable=False, default="active")
    security_classification: Mapped[str | None] = mapped_column(String(100), nullable=True)

    tables: Mapped[list["Table"]] = relationship(
        "Table",
        back_populates="system",
        cascade="all, delete-orphan",
        passive_deletes=True,
    )
    data_object_links: Mapped[list["DataObjectSystem"]] = relationship(
        "DataObjectSystem",
        back_populates="system",
        cascade="all, delete-orphan",
        passive_deletes=True,
    )
    data_objects: Mapped[list[DataObject]] = relationship(
        "DataObject",
        secondary="data_object_systems",
        viewonly=True,
        overlaps="data_object_links,system_links",
    )
    connections: Mapped[list["SystemConnection"]] = relationship(
        "SystemConnection",
        back_populates="system",
        cascade="all, delete-orphan",
        passive_deletes=True,
    )
    data_definitions: Mapped[list["DataDefinition"]] = relationship(
        "DataDefinition",
        back_populates="system",
        cascade="all, delete-orphan",
        passive_deletes=True,
    )


class DataDefinition(Base, TimestampMixin):
    __tablename__ = "data_definitions"
    __table_args__ = (
        sa.UniqueConstraint(
            "data_object_id",
            "system_id",
            name="uq_data_definition_object_system",
        ),
    )

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True, default=uuid.uuid4
    )
    data_object_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("data_objects.id", ondelete="CASCADE"), nullable=False
    )
    system_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("systems.id", ondelete="CASCADE"), nullable=False
    )
    description: Mapped[str | None] = mapped_column(Text, nullable=True)

    data_object: Mapped[DataObject] = relationship("DataObject", back_populates="data_definitions")
    system: Mapped[System] = relationship("System", back_populates="data_definitions")
    tables: Mapped[list["DataDefinitionTable"]] = relationship(
        "DataDefinitionTable",
        back_populates="data_definition",
        cascade="all, delete-orphan",
        passive_deletes=True,
        order_by="DataDefinitionTable.load_order",
    )
    relationships: Mapped[list["DataDefinitionRelationship"]] = relationship(
        "DataDefinitionRelationship",
        back_populates="data_definition",
        cascade="all, delete-orphan",
        passive_deletes=True,
    )
    constructed_tables: Mapped[list["ConstructedTable"]] = relationship(
        "ConstructedTable",
        back_populates="data_definition",
        cascade="all, delete-orphan",
        passive_deletes=True,
    )


class DataDefinitionTable(Base, TimestampMixin):
    __tablename__ = "data_definition_tables"
    __table_args__ = (
        sa.UniqueConstraint(
            "data_definition_id",
            "table_id",
            name="uq_data_definition_table",
        ),
    )

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True, default=uuid.uuid4
    )
    data_definition_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("data_definitions.id", ondelete="CASCADE"), nullable=False
    )
    table_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("tables.id", ondelete="CASCADE"), nullable=False
    )
    alias: Mapped[str | None] = mapped_column(String(200), nullable=True)
    description: Mapped[str | None] = mapped_column(Text, nullable=True)
    load_order: Mapped[int | None] = mapped_column(Integer, nullable=True)
    is_construction: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)

    data_definition: Mapped[DataDefinition] = relationship("DataDefinition", back_populates="tables")
    table: Mapped["Table"] = relationship("Table", back_populates="definition_tables")
    fields: Mapped[list["DataDefinitionField"]] = relationship(
        "DataDefinitionField",
        back_populates="definition_table",
        cascade="all, delete-orphan",
        passive_deletes=True,
    )
    relationships_as_primary: Mapped[list["DataDefinitionRelationship"]] = relationship(
        "DataDefinitionRelationship",
        back_populates="primary_table",
        passive_deletes=True,
        foreign_keys="[DataDefinitionRelationship.primary_table_id]",
    )
    relationships_as_foreign: Mapped[list["DataDefinitionRelationship"]] = relationship(
        "DataDefinitionRelationship",
        back_populates="foreign_table",
        passive_deletes=True,
        foreign_keys="[DataDefinitionRelationship.foreign_table_id]",
    )
    constructed_table: Mapped[Optional["ConstructedTable"]] = relationship(
        "ConstructedTable", back_populates="data_definition_table", uselist=False
    )

    @property
    def constructed_table_id(self) -> uuid.UUID | None:
        return self.constructed_table.id if self.constructed_table else None

    @property
    def constructed_table_name(self) -> str | None:
        return self.constructed_table.name if self.constructed_table else None

    @property
    def constructed_table_status(self) -> str | None:
        return self.constructed_table.status if self.constructed_table else None


class DataDefinitionField(Base, TimestampMixin):
    __tablename__ = "data_definition_fields"
    __table_args__ = (
        sa.UniqueConstraint(
            "definition_table_id",
            "field_id",
            name="uq_data_definition_field",
        ),
    )

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True, default=uuid.uuid4
    )
    definition_table_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("data_definition_tables.id", ondelete="CASCADE"), nullable=False
    )
    field_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("fields.id", ondelete="CASCADE"), nullable=False
    )
    notes: Mapped[str | None] = mapped_column(Text, nullable=True)

    definition_table: Mapped[DataDefinitionTable] = relationship(
        "DataDefinitionTable", back_populates="fields"
    )
    field: Mapped["Field"] = relationship("Field", back_populates="definition_fields")
    relationships_as_primary: Mapped[list["DataDefinitionRelationship"]] = relationship(
        "DataDefinitionRelationship",
        back_populates="primary_field",
        passive_deletes=True,
        foreign_keys="[DataDefinitionRelationship.primary_field_id]",
    )
    relationships_as_foreign: Mapped[list["DataDefinitionRelationship"]] = relationship(
        "DataDefinitionRelationship",
        back_populates="foreign_field",
        passive_deletes=True,
        foreign_keys="[DataDefinitionRelationship.foreign_field_id]",
    )


class DataDefinitionRelationship(Base, TimestampMixin):
    __tablename__ = "data_definition_relationships"
    __table_args__ = (
        sa.UniqueConstraint(
            "data_definition_id",
            "primary_field_id",
            "foreign_field_id",
            name="uq_data_definition_relationship_fields",
        ),
        sa.CheckConstraint(
            "primary_field_id <> foreign_field_id",
            name="ck_data_definition_relationship_distinct_fields",
        ),
        sa.CheckConstraint(
            "primary_table_id <> foreign_table_id",
            name="ck_data_definition_relationship_distinct_tables",
        ),
    )

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True, default=uuid.uuid4
    )
    data_definition_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("data_definitions.id", ondelete="CASCADE"), nullable=False
    )
    primary_table_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("data_definition_tables.id", ondelete="CASCADE"), nullable=False
    )
    primary_field_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("data_definition_fields.id", ondelete="CASCADE"), nullable=False
    )
    foreign_table_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("data_definition_tables.id", ondelete="CASCADE"), nullable=False
    )
    foreign_field_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("data_definition_fields.id", ondelete="CASCADE"), nullable=False
    )
    join_type: Mapped[str] = mapped_column(
        sa.Enum(
            "inner",
            "left",
            "right",
            name="data_definition_join_type_enum",
        ),
        nullable=False,
        default="inner",
    )
    notes: Mapped[str | None] = mapped_column(Text, nullable=True)

    data_definition: Mapped[DataDefinition] = relationship(
        "DataDefinition", back_populates="relationships"
    )
    primary_table: Mapped[DataDefinitionTable] = relationship(
        "DataDefinitionTable",
        back_populates="relationships_as_primary",
        foreign_keys=[primary_table_id],
    )
    foreign_table: Mapped[DataDefinitionTable] = relationship(
        "DataDefinitionTable",
        back_populates="relationships_as_foreign",
        foreign_keys=[foreign_table_id],
    )
    primary_field: Mapped[DataDefinitionField] = relationship(
        "DataDefinitionField",
        back_populates="relationships_as_primary",
        foreign_keys=[primary_field_id],
    )
    foreign_field: Mapped[DataDefinitionField] = relationship(
        "DataDefinitionField",
        back_populates="relationships_as_foreign",
        foreign_keys=[foreign_field_id],
    )


class DataObjectSystem(Base, TimestampMixin):
    __tablename__ = "data_object_systems"

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True, default=uuid.uuid4
    )
    data_object_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("data_objects.id", ondelete="CASCADE"), nullable=False
    )
    system_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("systems.id", ondelete="CASCADE"), nullable=False
    )
    relationship_type: Mapped[str] = mapped_column(String(50), nullable=False, default="source")
    description: Mapped[str | None] = mapped_column(Text, nullable=True)

    data_object: Mapped[DataObject] = relationship("DataObject", back_populates="system_links")
    system: Mapped[System] = relationship("System", back_populates="data_object_links")


class DataObjectDependency(Base, TimestampMixin):
    __tablename__ = "data_object_dependencies"
    __table_args__ = (
        sa.UniqueConstraint(
            "predecessor_id",
            "successor_id",
            name="uq_data_object_dependency",
        ),
    )

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True, default=uuid.uuid4
    )
    predecessor_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("data_objects.id", ondelete="CASCADE"), nullable=False
    )
    successor_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("data_objects.id", ondelete="CASCADE"), nullable=False
    )
    dependency_type: Mapped[str] = mapped_column(
        sa.Enum(
            "precedence",
            "reference",
            name="data_object_dependency_type_enum",
        ),
        nullable=False,
        default="precedence",
    )
    notes: Mapped[str | None] = mapped_column(Text, nullable=True)

    predecessor: Mapped[DataObject] = relationship(
        "DataObject",
        back_populates="outgoing_dependencies",
        foreign_keys=[predecessor_id],
    )
    successor: Mapped[DataObject] = relationship(
        "DataObject",
        back_populates="incoming_dependencies",
        foreign_keys=[successor_id],
    )
    approvals: Mapped[list["DependencyApproval"]] = relationship(
        "DependencyApproval",
        back_populates="data_object_dependency",
        cascade="all, delete-orphan",
        passive_deletes=True,
        foreign_keys="[DependencyApproval.data_object_dependency_id]",
    )


class Table(Base, TimestampMixin):
    __tablename__ = "tables"

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True, default=uuid.uuid4
    )
    system_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("systems.id", ondelete="CASCADE"), nullable=False
    )
    name: Mapped[str] = mapped_column(String(200), nullable=False)
    physical_name: Mapped[str] = mapped_column(String(200), nullable=False)
    schema_name: Mapped[str | None] = mapped_column(String(120), nullable=True)
    description: Mapped[str | None] = mapped_column(Text, nullable=True)
    table_type: Mapped[str | None] = mapped_column(String(50), nullable=True)
    status: Mapped[str] = mapped_column(String(50), nullable=False, default="active")
    security_classification: Mapped[str | None] = mapped_column(String(100), nullable=True)

    system: Mapped[System] = relationship("System", back_populates="tables")
    fields: Mapped[list["Field"]] = relationship(
        "Field",
        back_populates="table",
        cascade="all, delete-orphan",
        passive_deletes=True,
    )
    definition_tables: Mapped[list["DataDefinitionTable"]] = relationship(
        "DataDefinitionTable",
        back_populates="table",
        cascade="all, delete-orphan",
        passive_deletes=True,
    )
    ingestion_jobs: Mapped[list["IngestionJob"]] = relationship(
        "IngestionJob",
        back_populates="table",
        cascade="all, delete-orphan",
        passive_deletes=True,
    )
    upstream_dependencies: Mapped[list["TableDependency"]] = relationship(
        "TableDependency",
        back_populates="predecessor",
        cascade="all, delete-orphan",
        passive_deletes=True,
        foreign_keys="[TableDependency.predecessor_id]",
    )
    downstream_dependencies: Mapped[list["TableDependency"]] = relationship(
        "TableDependency",
        back_populates="successor",
        cascade="all, delete-orphan",
        passive_deletes=True,
        foreign_keys="[TableDependency.successor_id]",
    )
    load_orders: Mapped[list["TableLoadOrder"]] = relationship(
        "TableLoadOrder",
        back_populates="table",
        cascade="all, delete-orphan",
        passive_deletes=True,
    )


class TableLoadOrder(Base, TimestampMixin):
    __tablename__ = "table_load_orders"
    __table_args__ = (
        sa.UniqueConstraint("data_object_id", "table_id", name="uq_table_load_order_table"),
        sa.UniqueConstraint("data_object_id", "sequence", name="uq_table_load_order_sequence"),
        sa.CheckConstraint("sequence > 0", name="ck_table_load_order_sequence_positive"),
    )

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True, default=uuid.uuid4
    )
    data_object_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("data_objects.id", ondelete="CASCADE"), nullable=False
    )
    table_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("tables.id", ondelete="CASCADE"), nullable=False
    )
    sequence: Mapped[int] = mapped_column(Integer, nullable=False)
    notes: Mapped[str | None] = mapped_column(Text, nullable=True)

    data_object: Mapped[DataObject] = relationship(
        "DataObject", back_populates="table_load_orders"
    )
    table: Mapped[Table] = relationship("Table", back_populates="load_orders")
    approvals: Mapped[list["TableLoadOrderApproval"]] = relationship(
        "TableLoadOrderApproval",
        back_populates="table_load_order",
        cascade="all, delete-orphan",
        passive_deletes=True,
    )


class TableLoadOrderApproval(Base, TimestampMixin):
    __tablename__ = "table_load_order_approvals"
    __table_args__ = (
        sa.UniqueConstraint(
            "table_load_order_id",
            "approver_id",
            "role",
            name="uq_table_load_order_approval_unique",
        ),
    )

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True, default=uuid.uuid4
    )
    table_load_order_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("table_load_orders.id", ondelete="CASCADE"), nullable=False
    )
    approver_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("users.id", ondelete="CASCADE"), nullable=False
    )
    role: Mapped[str] = mapped_column(
        sa.Enum(
            "sme",
            "data_owner",
            "approver",
            "admin",
            name="table_load_order_approval_role_enum",
        ),
        nullable=False,
    )
    decision: Mapped[str] = mapped_column(
        sa.Enum(
            "approved",
            "rejected",
            name="table_load_order_approval_decision_enum",
        ),
        nullable=False,
    )
    comments: Mapped[str | None] = mapped_column(Text, nullable=True)
    approved_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=datetime.utcnow
    )

    table_load_order: Mapped[TableLoadOrder] = relationship(
        "TableLoadOrder", back_populates="approvals"
    )
    approver: Mapped[User] = relationship(
        "User",
        back_populates="table_load_order_approvals",
        foreign_keys=[approver_id],
    )


class TableDependency(Base, TimestampMixin):
    __tablename__ = "table_dependencies"
    __table_args__ = (
        sa.UniqueConstraint(
            "predecessor_id",
            "successor_id",
            name="uq_table_dependency",
        ),
    )

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True, default=uuid.uuid4
    )
    predecessor_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("tables.id", ondelete="CASCADE"), nullable=False
    )
    successor_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("tables.id", ondelete="CASCADE"), nullable=False
    )
    dependency_type: Mapped[str] = mapped_column(
        sa.Enum(
            "precedence",
            "reference",
            name="table_dependency_type_enum",
        ),
        nullable=False,
        default="precedence",
    )
    notes: Mapped[str | None] = mapped_column(Text, nullable=True)

    predecessor: Mapped[Table] = relationship(
        "Table",
        back_populates="upstream_dependencies",
        foreign_keys=[predecessor_id],
    )
    successor: Mapped[Table] = relationship(
        "Table",
        back_populates="downstream_dependencies",
        foreign_keys=[successor_id],
    )
    approvals: Mapped[list["DependencyApproval"]] = relationship(
        "DependencyApproval",
        back_populates="table_dependency",
        cascade="all, delete-orphan",
        passive_deletes=True,
        foreign_keys="[DependencyApproval.table_dependency_id]",
    )


class DependencyApproval(Base, TimestampMixin):
    __tablename__ = "dependency_approvals"
    __table_args__ = (
        sa.CheckConstraint(
            "(data_object_dependency_id IS NOT NULL) != (table_dependency_id IS NOT NULL)",
            name="ck_dependency_approval_single_target",
        ),
    )

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True, default=uuid.uuid4
    )
    data_object_dependency_id: Mapped[uuid.UUID | None] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("data_object_dependencies.id", ondelete="CASCADE"),
        nullable=True,
    )
    table_dependency_id: Mapped[uuid.UUID | None] = mapped_column(
        UUID(as_uuid=True), ForeignKey("table_dependencies.id", ondelete="CASCADE"), nullable=True
    )
    approver_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("users.id", ondelete="CASCADE"), nullable=False
    )
    decision: Mapped[str] = mapped_column(
        sa.Enum("pending", "approved", "rejected", name="dependency_approval_decision_enum"),
        nullable=False,
        default="pending",
    )
    comments: Mapped[str | None] = mapped_column(Text, nullable=True)
    decided_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=datetime.utcnow
    )

    data_object_dependency: Mapped[DataObjectDependency | None] = relationship(
        "DataObjectDependency",
        back_populates="approvals",
        foreign_keys=[data_object_dependency_id],
    )
    table_dependency: Mapped[TableDependency | None] = relationship(
        "TableDependency",
        back_populates="approvals",
        foreign_keys=[table_dependency_id],
    )
    approver: Mapped[User] = relationship(
        "User",
        back_populates="dependency_approvals",
        foreign_keys=[approver_id],
    )


class Field(Base, TimestampMixin):
    __tablename__ = "fields"

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True, default=uuid.uuid4
    )
    table_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("tables.id", ondelete="CASCADE"), nullable=False
    )
    name: Mapped[str] = mapped_column(String(200), nullable=False)
    description: Mapped[str | None] = mapped_column(Text, nullable=True)
    application_usage: Mapped[str | None] = mapped_column(Text, nullable=True)
    business_definition: Mapped[str | None] = mapped_column(Text, nullable=True)
    enterprise_attribute: Mapped[str | None] = mapped_column(String(200), nullable=True)
    field_type: Mapped[str] = mapped_column(String(100), nullable=False)
    field_length: Mapped[int | None] = mapped_column(Integer, nullable=True)
    decimal_places: Mapped[int | None] = mapped_column(Integer, nullable=True)
    system_required: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    business_process_required: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    suppressed_field: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    active: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)
    legal_regulatory_implications: Mapped[str | None] = mapped_column(Text, nullable=True)
    security_classification: Mapped[str | None] = mapped_column(String(100), nullable=True)
    data_validation: Mapped[str | None] = mapped_column(Text, nullable=True)
    reference_table: Mapped[str | None] = mapped_column(String(200), nullable=True)
    grouping_tab: Mapped[str | None] = mapped_column(String(200), nullable=True)

    table: Mapped[Table] = relationship("Table", back_populates="fields")
    field_loads: Mapped[list["FieldLoad"]] = relationship(
        "FieldLoad",
        back_populates="field",
        cascade="all, delete-orphan",
        passive_deletes=True,
    )
    source_mappings: Mapped[list["Mapping"]] = relationship(
        "Mapping",
        back_populates="source_field",
        foreign_keys="[Mapping.source_field_id]",
        cascade="all, delete-orphan",
        passive_deletes=True,
    )
    target_mappings: Mapped[list["Mapping"]] = relationship(
        "Mapping",
        back_populates="target_field",
        foreign_keys="[Mapping.target_field_id]",
        cascade="all, delete-orphan",
        passive_deletes=True,
    )
    definition_fields: Mapped[list["DataDefinitionField"]] = relationship(
        "DataDefinitionField",
        back_populates="field",
        cascade="all, delete-orphan",
        passive_deletes=True,
    )


class FieldLoad(Base, TimestampMixin):
    __tablename__ = "field_loads"
    __table_args__ = (
        sa.UniqueConstraint("release_id", "field_id", name="uq_field_load_release_field"),
    )

    id: Mapped[uuid.UUID] = mapped_column(
        "field_load_id",
        UUID(as_uuid=True),
        primary_key=True,
        default=uuid.uuid4,
    )
    release_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("releases.id", ondelete="CASCADE"), nullable=False
    )
    field_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("fields.id", ondelete="CASCADE"), nullable=False
    )
    load_flag: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)
    notes: Mapped[str | None] = mapped_column(Text, nullable=True)
    created_by: Mapped[uuid.UUID | None] = mapped_column(
        UUID(as_uuid=True), ForeignKey("users.id", ondelete="SET NULL"), nullable=True
    )

    release: Mapped[Release] = relationship("Release", back_populates="field_loads")
    field: Mapped[Field] = relationship("Field", back_populates="field_loads")
    creator: Mapped[User | None] = relationship("User", back_populates="field_loads")


class PreLoadValidationResult(Base, TimestampMixin):
    __tablename__ = "pre_load_validation_results"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    release_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("releases.id", ondelete="CASCADE"), nullable=False
    )
    name: Mapped[str] = mapped_column(String(200), nullable=False)
    status: Mapped[str] = mapped_column(String(50), nullable=False, default="pending")
    total_checks: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    passed_checks: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    failed_checks: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    executed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    notes: Mapped[str | None] = mapped_column(Text, nullable=True)

    release: Mapped[Release] = relationship("Release", back_populates="pre_load_validation_results")
    issues: Mapped[list["PreLoadValidationIssue"]] = relationship(
        "PreLoadValidationIssue",
        back_populates="validation_result",
        cascade="all, delete-orphan",
        passive_deletes=True,
    )
    approvals: Mapped[list["PreLoadValidationApproval"]] = relationship(
        "PreLoadValidationApproval",
        back_populates="validation_result",
        cascade="all, delete-orphan",
        passive_deletes=True,
    )


class PreLoadValidationApproval(Base, TimestampMixin):
    __tablename__ = "pre_load_validation_approvals"

    id: Mapped[uuid.UUID] = mapped_column(
        "preload_validation_approval_id",
        UUID(as_uuid=True),
        primary_key=True,
        default=uuid.uuid4,
    )
    pre_load_validation_result_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("pre_load_validation_results.id", ondelete="CASCADE"),
        nullable=False,
    )
    approver_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("users.id", ondelete="CASCADE"), nullable=False
    )
    role: Mapped[str] = mapped_column(
        sa.Enum(
            "sme",
            "data_owner",
            "approver",
            "admin",
            name="validation_approval_role_enum",
        ),
        nullable=False,
    )
    decision: Mapped[str] = mapped_column(
        sa.Enum("approved", "rejected", name="validation_approval_decision_enum"),
        nullable=False,
    )
    comments: Mapped[str | None] = mapped_column(Text, nullable=True)
    approved_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=datetime.utcnow
    )

    validation_result: Mapped["PreLoadValidationResult"] = relationship(
        "PreLoadValidationResult", back_populates="approvals"
    )
    approver: Mapped[User] = relationship("User", back_populates="pre_load_validation_approvals")


class PostLoadValidationApproval(Base, TimestampMixin):
    __tablename__ = "post_load_validation_approvals"

    id: Mapped[uuid.UUID] = mapped_column(
        "postload_validation_approval_id",
        UUID(as_uuid=True),
        primary_key=True,
        default=uuid.uuid4,
    )
    post_load_validation_result_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("post_load_validation_results.id", ondelete="CASCADE"),
        nullable=False,
    )
    approver_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("users.id", ondelete="CASCADE"), nullable=False
    )
    role: Mapped[str] = mapped_column(
        sa.Enum(
            "sme",
            "data_owner",
            "approver",
            "admin",
            name="validation_approval_role_enum",
        ),
        nullable=False,
    )
    decision: Mapped[str] = mapped_column(
        sa.Enum("approved", "rejected", name="validation_approval_decision_enum"),
        nullable=False,
    )
    comments: Mapped[str | None] = mapped_column(Text, nullable=True)
    approved_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=datetime.utcnow
    )

    validation_result: Mapped["PostLoadValidationResult"] = relationship(
        "PostLoadValidationResult", back_populates="approvals"
    )
    approver: Mapped[User] = relationship("User", back_populates="post_load_validation_approvals")


class PreLoadValidationIssue(Base, TimestampMixin):
    __tablename__ = "pre_load_validation_issues"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    validation_result_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("pre_load_validation_results.id", ondelete="CASCADE"),
        nullable=False,
    )
    description: Mapped[str] = mapped_column(Text, nullable=False)
    severity: Mapped[str] = mapped_column(String(50), nullable=False, default="medium")
    status: Mapped[str] = mapped_column(String(50), nullable=False, default="open")
    record_identifier: Mapped[str | None] = mapped_column(String(200), nullable=True)
    resolution_notes: Mapped[str | None] = mapped_column(Text, nullable=True)

    validation_result: Mapped[PreLoadValidationResult] = relationship(
        "PreLoadValidationResult", back_populates="issues"
    )


class PostLoadValidationResult(Base, TimestampMixin):
    __tablename__ = "post_load_validation_results"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    release_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("releases.id", ondelete="CASCADE"), nullable=False
    )
    name: Mapped[str] = mapped_column(String(200), nullable=False)
    status: Mapped[str] = mapped_column(String(50), nullable=False, default="pending")
    total_checks: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    passed_checks: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    failed_checks: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    executed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    notes: Mapped[str | None] = mapped_column(Text, nullable=True)

    release: Mapped[Release] = relationship("Release", back_populates="post_load_validation_results")
    issues: Mapped[list["PostLoadValidationIssue"]] = relationship(
        "PostLoadValidationIssue",
        back_populates="validation_result",
        cascade="all, delete-orphan",
        passive_deletes=True,
    )
    approvals: Mapped[list["PostLoadValidationApproval"]] = relationship(
        "PostLoadValidationApproval",
        back_populates="validation_result",
        cascade="all, delete-orphan",
        passive_deletes=True,
    )


class PostLoadValidationIssue(Base, TimestampMixin):
    __tablename__ = "post_load_validation_issues"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    validation_result_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("post_load_validation_results.id", ondelete="CASCADE"),
        nullable=False,
    )
    description: Mapped[str] = mapped_column(Text, nullable=False)
    severity: Mapped[str] = mapped_column(String(50), nullable=False, default="medium")
    status: Mapped[str] = mapped_column(String(50), nullable=False, default="open")
    record_identifier: Mapped[str | None] = mapped_column(String(200), nullable=True)
    resolution_notes: Mapped[str | None] = mapped_column(Text, nullable=True)

    validation_result: Mapped[PostLoadValidationResult] = relationship(
        "PostLoadValidationResult", back_populates="issues"
    )


class SystemConnection(Base, TimestampMixin):
    __tablename__ = "system_connections"

    id: Mapped[uuid.UUID] = mapped_column(
        "system_connection_id",
        UUID(as_uuid=True),
        primary_key=True,
        default=uuid.uuid4,
    )
    system_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("systems.id", ondelete="CASCADE"), nullable=False
    )
    connection_type: Mapped[str] = mapped_column(
        sa.Enum(
            "jdbc",
            "odbc",
            "api",
            "file",
            "saprfc",
            "other",
            name="system_connection_type_enum",
        ),
        nullable=False,
        default="jdbc",
    )
    connection_string: Mapped[str] = mapped_column(Text, nullable=False)
    auth_method: Mapped[str] = mapped_column(
        sa.Enum(
            "username_password",
            "oauth",
            "key_vault_reference",
            name="system_connection_auth_method_enum",
        ),
        nullable=False,
        default="username_password",
    )
    active: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)
    ingestion_enabled: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)
    notes: Mapped[str | None] = mapped_column(Text, nullable=True)

    system: Mapped[System] = relationship("System", back_populates="connections")
    catalog_selections: Mapped[list["ConnectionTableSelection"]] = relationship(
        "ConnectionTableSelection",
        back_populates="system_connection",
        cascade="all, delete-orphan",
        passive_deletes=True,
    )


class ConnectionTableSelection(Base, TimestampMixin):
    __tablename__ = "connection_table_selections"
    __table_args__ = (
        sa.UniqueConstraint(
            "system_connection_id",
            "schema_name",
            "table_name",
            name="uq_connection_table_selection",
        ),
    )

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True, default=uuid.uuid4
    )
    system_connection_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("system_connections.system_connection_id", ondelete="CASCADE"), nullable=False
    )
    schema_name: Mapped[str] = mapped_column(String(120), nullable=False)
    table_name: Mapped[str] = mapped_column(String(200), nullable=False)
    table_type: Mapped[str | None] = mapped_column(String(50), nullable=True)
    column_count: Mapped[int | None] = mapped_column(Integer, nullable=True)
    estimated_rows: Mapped[int | None] = mapped_column(BigInteger, nullable=True)
    last_seen_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)

    system_connection: Mapped[SystemConnection] = relationship(
        "SystemConnection", back_populates="catalog_selections"
    )
    ingestion_schedules: Mapped[list["IngestionSchedule"]] = relationship(
        "IngestionSchedule",
        back_populates="table_selection",
        cascade="all, delete-orphan",
        passive_deletes=True,
    )


class IngestionJob(Base, TimestampMixin):
    __tablename__ = "ingestion_jobs"

    id: Mapped[uuid.UUID] = mapped_column(
        "ingestion_job_id",
        UUID(as_uuid=True),
        primary_key=True,
        default=uuid.uuid4,
    )
    execution_context_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("execution_contexts.id", ondelete="CASCADE"), nullable=False
    )
    table_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("tables.id", ondelete="CASCADE"), nullable=False
    )
    status: Mapped[str] = mapped_column(
        sa.Enum("pending", "running", "completed", "failed", name="ingestion_job_status_enum"),
        nullable=False,
        default="pending",
    )
    started_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    completed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    row_count: Mapped[int | None] = mapped_column(Integer, nullable=True)
    notes: Mapped[str | None] = mapped_column(Text, nullable=True)

    execution_context: Mapped[ExecutionContext] = relationship(
        "ExecutionContext", back_populates="ingestion_jobs"
    )
    table: Mapped[Table] = relationship("Table", back_populates="ingestion_jobs")


class IngestionSchedule(Base, TimestampMixin):
    __tablename__ = "ingestion_schedules"

    id: Mapped[uuid.UUID] = mapped_column(
        "ingestion_schedule_id",
        UUID(as_uuid=True),
        primary_key=True,
        default=uuid.uuid4,
    )
    connection_table_selection_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("connection_table_selections.id", ondelete="CASCADE"),
        nullable=False,
    )
    schedule_expression: Mapped[str] = mapped_column(String(120), nullable=False)
    timezone: Mapped[str | None] = mapped_column(String(60), nullable=True)
    load_strategy: Mapped[str] = mapped_column(
        sa.Enum(
            "timestamp",
            "numeric_key",
            "full",
            name="ingestion_load_strategy_enum",
        ),
        nullable=False,
        default="timestamp",
    )
    watermark_column: Mapped[str | None] = mapped_column(String(120), nullable=True)
    primary_key_column: Mapped[str | None] = mapped_column(String(120), nullable=True)
    target_schema: Mapped[str | None] = mapped_column(String(120), nullable=True)
    target_table_name: Mapped[str | None] = mapped_column(String(200), nullable=True)
    batch_size: Mapped[int] = mapped_column(Integer, nullable=False, default=5_000)
    is_active: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)
    last_watermark_timestamp: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    last_watermark_id: Mapped[int | None] = mapped_column(BigInteger, nullable=True)
    last_run_started_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    last_run_completed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    last_run_status: Mapped[str | None] = mapped_column(String(20), nullable=True)
    last_run_error: Mapped[str | None] = mapped_column(Text, nullable=True)
    total_runs: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    total_rows_loaded: Mapped[int] = mapped_column(BigInteger, nullable=False, default=0)

    table_selection: Mapped[ConnectionTableSelection] = relationship(
        "ConnectionTableSelection", back_populates="ingestion_schedules"
    )
    runs: Mapped[list["IngestionRun"]] = relationship(
        "IngestionRun",
        back_populates="schedule",
        cascade="all, delete-orphan",
        passive_deletes=True,
    )


class IngestionRun(Base, TimestampMixin):
    __tablename__ = "ingestion_runs"

    id: Mapped[uuid.UUID] = mapped_column(
        "ingestion_run_id",
        UUID(as_uuid=True),
        primary_key=True,
        default=uuid.uuid4,
    )
    ingestion_schedule_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("ingestion_schedules.ingestion_schedule_id", ondelete="CASCADE"),
        nullable=False,
    )
    status: Mapped[str] = mapped_column(
        sa.Enum(
            "scheduled",
            "running",
            "completed",
            "failed",
            name="ingestion_run_status_enum",
        ),
        nullable=False,
        default="scheduled",
    )
    started_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    completed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    rows_loaded: Mapped[int | None] = mapped_column(Integer, nullable=True)
    watermark_timestamp_before: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    watermark_timestamp_after: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    watermark_id_before: Mapped[int | None] = mapped_column(BigInteger, nullable=True)
    watermark_id_after: Mapped[int | None] = mapped_column(BigInteger, nullable=True)
    query_text: Mapped[str | None] = mapped_column(Text, nullable=True)
    error_message: Mapped[str | None] = mapped_column(Text, nullable=True)

    schedule: Mapped[IngestionSchedule] = relationship(
        "IngestionSchedule", back_populates="runs"
    )


class MappingSet(Base, TimestampMixin):
    __tablename__ = "mapping_sets"
    __table_args__ = (
        sa.UniqueConstraint("release_id", "version", name="uq_mapping_set_release_version"),
    )

    id: Mapped[uuid.UUID] = mapped_column(
        "mapping_set_id",
        UUID(as_uuid=True),
        primary_key=True,
        default=uuid.uuid4,
    )
    release_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("releases.id", ondelete="CASCADE"), nullable=False
    )
    version: Mapped[int] = mapped_column(Integer, nullable=False)
    status: Mapped[str] = mapped_column(
        sa.Enum("draft", "active", "superseded", "archived", name="mapping_set_status_enum"),
        nullable=False,
        default="draft",
    )
    notes: Mapped[str | None] = mapped_column(Text, nullable=True)
    created_by: Mapped[uuid.UUID | None] = mapped_column(
        UUID(as_uuid=True), ForeignKey("users.id", ondelete="SET NULL"), nullable=True
    )

    release: Mapped[Release] = relationship("Release", back_populates="mapping_sets")
    creator: Mapped[User | None] = relationship("User", back_populates="mapping_sets")
    mappings: Mapped[list["Mapping"]] = relationship(
        "Mapping",
        back_populates="mapping_set",
        cascade="all, delete-orphan",
        passive_deletes=True,
    )


class Mapping(Base, TimestampMixin):
    __tablename__ = "mappings"

    id: Mapped[uuid.UUID] = mapped_column(
        "mapping_id",
        UUID(as_uuid=True),
        primary_key=True,
        default=uuid.uuid4,
    )
    mapping_set_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("mapping_sets.mapping_set_id", ondelete="CASCADE"), nullable=False
    )
    source_field_id: Mapped[uuid.UUID | None] = mapped_column(
        UUID(as_uuid=True), ForeignKey("fields.id", ondelete="SET NULL"), nullable=True
    )
    target_field_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("fields.id", ondelete="CASCADE"), nullable=False
    )
    transformation_rule: Mapped[str | None] = mapped_column(Text, nullable=True)
    default_value: Mapped[str | None] = mapped_column(Text, nullable=True)
    notes: Mapped[str | None] = mapped_column(Text, nullable=True)
    status: Mapped[str] = mapped_column(
        sa.Enum("draft", "approved", "rejected", name="mapping_status_enum"),
        nullable=False,
        default="draft",
    )
    created_by: Mapped[uuid.UUID | None] = mapped_column(
        UUID(as_uuid=True), ForeignKey("users.id", ondelete="SET NULL"), nullable=True
    )

    mapping_set: Mapped[MappingSet] = relationship("MappingSet", back_populates="mappings")
    source_field: Mapped[Field | None] = relationship(
        "Field",
        back_populates="source_mappings",
        foreign_keys=[source_field_id],
    )
    target_field: Mapped[Field] = relationship(
        "Field",
        back_populates="target_mappings",
        foreign_keys=[target_field_id],
    )
    creator: Mapped[User | None] = relationship("User", back_populates="mappings")
