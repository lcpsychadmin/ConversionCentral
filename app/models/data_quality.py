from __future__ import annotations

from datetime import datetime

from sqlalchemy import Boolean, DateTime, Float, ForeignKey, Integer, String, Text
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.database import Base
from app.models.entities import TimestampMixin, utcnow


class DataQualityProject(Base, TimestampMixin):
    __tablename__ = "dq_projects"

    project_key: Mapped[str] = mapped_column(String(255), primary_key=True)
    name: Mapped[str | None] = mapped_column(String(255), nullable=True)
    description: Mapped[str | None] = mapped_column(Text, nullable=True)
    sql_flavor: Mapped[str | None] = mapped_column(String(50), nullable=True)

    connections: Mapped[list["DataQualityConnection"]] = relationship(
        "DataQualityConnection",
        back_populates="project",
        cascade="all, delete-orphan",
        passive_deletes=True,
    )


class DataQualityConnection(Base, TimestampMixin):
    __tablename__ = "dq_connections"

    connection_id: Mapped[str] = mapped_column(String(255), primary_key=True)
    project_key: Mapped[str] = mapped_column(
        String(255), ForeignKey("dq_projects.project_key", ondelete="CASCADE"), nullable=False
    )
    name: Mapped[str | None] = mapped_column(String(255), nullable=True)
    catalog: Mapped[str | None] = mapped_column(String(255), nullable=True)
    schema_name: Mapped[str | None] = mapped_column(String(255), nullable=True)
    http_path: Mapped[str | None] = mapped_column(String(400), nullable=True)
    managed_credentials_ref: Mapped[str | None] = mapped_column(String(255), nullable=True)
    is_active: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)

    project: Mapped[DataQualityProject] = relationship("DataQualityProject", back_populates="connections")
    table_groups: Mapped[list["DataQualityTableGroup"]] = relationship(
        "DataQualityTableGroup",
        back_populates="connection",
        cascade="all, delete-orphan",
        passive_deletes=True,
    )


class DataQualityTableGroup(Base, TimestampMixin):
    __tablename__ = "dq_table_groups"

    table_group_id: Mapped[str] = mapped_column(String(255), primary_key=True)
    connection_id: Mapped[str] = mapped_column(
        String(255), ForeignKey("dq_connections.connection_id", ondelete="CASCADE"), nullable=False
    )
    name: Mapped[str | None] = mapped_column(String(255), nullable=True)
    description: Mapped[str | None] = mapped_column(Text, nullable=True)
    profiling_include_mask: Mapped[str | None] = mapped_column(String(255), nullable=True)
    profiling_exclude_mask: Mapped[str | None] = mapped_column(String(255), nullable=True)
    profiling_job_id: Mapped[str | None] = mapped_column(String(255), nullable=True)

    connection: Mapped[DataQualityConnection] = relationship("DataQualityConnection", back_populates="table_groups")
    tables: Mapped[list["DataQualityTable"]] = relationship(
        "DataQualityTable",
        back_populates="table_group",
        cascade="all, delete-orphan",
        passive_deletes=True,
    )


class DataQualityTable(Base):
    __tablename__ = "dq_tables"

    table_id: Mapped[str] = mapped_column(String(255), primary_key=True)
    table_group_id: Mapped[str] = mapped_column(
        String(255), ForeignKey("dq_table_groups.table_group_id", ondelete="CASCADE"), nullable=False
    )
    schema_name: Mapped[str | None] = mapped_column(String(255), nullable=True)
    table_name: Mapped[str | None] = mapped_column(String(255), nullable=True)
    source_table_id: Mapped[str | None] = mapped_column(String(255), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, default=utcnow)

    table_group: Mapped[DataQualityTableGroup] = relationship("DataQualityTableGroup", back_populates="tables")


class DataQualityProfile(Base):
    __tablename__ = "dq_profiles"

    profile_run_id: Mapped[str] = mapped_column(String(255), primary_key=True)
    table_group_id: Mapped[str] = mapped_column(
        String(255), ForeignKey("dq_table_groups.table_group_id", ondelete="CASCADE"), nullable=False
    )
    status: Mapped[str | None] = mapped_column(String(50), nullable=True)
    started_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    completed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    row_count: Mapped[int | None] = mapped_column(Integer, nullable=True)
    table_count: Mapped[int | None] = mapped_column(Integer, nullable=True)
    column_count: Mapped[int | None] = mapped_column(Integer, nullable=True)
    anomaly_count: Mapped[int | None] = mapped_column(Integer, nullable=True)
    dq_score_profiling: Mapped[float | None] = mapped_column(Float, nullable=True)
    dq_score_testing: Mapped[float | None] = mapped_column(Float, nullable=True)
    profile_mode: Mapped[str | None] = mapped_column(String(50), nullable=True)
    profile_version: Mapped[str | None] = mapped_column(String(50), nullable=True)
    payload_path: Mapped[str | None] = mapped_column(String(500), nullable=True)
    databricks_run_id: Mapped[str | None] = mapped_column(String(255), nullable=True)


class DataQualityProfileAnomalyType(Base, TimestampMixin):
    __tablename__ = "dq_profile_anomaly_types"

    anomaly_type_id: Mapped[str] = mapped_column(String(120), primary_key=True)
    name: Mapped[str | None] = mapped_column(String(255), nullable=True)
    category: Mapped[str | None] = mapped_column(String(120), nullable=True)
    default_severity: Mapped[str | None] = mapped_column(String(50), nullable=True)
    default_likelihood: Mapped[str | None] = mapped_column(String(50), nullable=True)
    description: Mapped[str | None] = mapped_column(Text, nullable=True)


class DataQualityProfileAnomaly(Base):
    __tablename__ = "dq_profile_anomalies"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    profile_run_id: Mapped[str] = mapped_column(
        String(255), ForeignKey("dq_profiles.profile_run_id", ondelete="CASCADE"), nullable=False
    )
    table_name: Mapped[str | None] = mapped_column(String(255), nullable=True)
    column_name: Mapped[str | None] = mapped_column(String(255), nullable=True)
    anomaly_type: Mapped[str | None] = mapped_column(String(120), nullable=True)
    severity: Mapped[str | None] = mapped_column(String(50), nullable=True)
    description: Mapped[str | None] = mapped_column(Text, nullable=True)
    detected_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)


class DataQualityProfileColumn(Base):
    __tablename__ = "dq_profile_columns"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    profile_run_id: Mapped[str] = mapped_column(
        String(255), ForeignKey("dq_profiles.profile_run_id", ondelete="CASCADE"), nullable=False
    )
    schema_name: Mapped[str | None] = mapped_column(String(255), nullable=True)
    table_name: Mapped[str] = mapped_column(String(255), nullable=False)
    column_name: Mapped[str] = mapped_column(String(255), nullable=False)
    qualified_name: Mapped[str | None] = mapped_column(String(500), nullable=True)
    data_type: Mapped[str | None] = mapped_column(String(255), nullable=True)
    general_type: Mapped[str | None] = mapped_column(String(50), nullable=True)
    ordinal_position: Mapped[int | None] = mapped_column(Integer, nullable=True)
    row_count: Mapped[int | None] = mapped_column(Integer, nullable=True)
    null_count: Mapped[int | None] = mapped_column(Integer, nullable=True)
    non_null_count: Mapped[int | None] = mapped_column(Integer, nullable=True)
    distinct_count: Mapped[int | None] = mapped_column(Integer, nullable=True)
    min_value: Mapped[str | None] = mapped_column(String(255), nullable=True)
    max_value: Mapped[str | None] = mapped_column(String(255), nullable=True)
    avg_value: Mapped[float | None] = mapped_column(Float, nullable=True)
    stddev_value: Mapped[float | None] = mapped_column(Float, nullable=True)
    median_value: Mapped[float | None] = mapped_column(Float, nullable=True)
    p95_value: Mapped[float | None] = mapped_column(Float, nullable=True)
    true_count: Mapped[int | None] = mapped_column(Integer, nullable=True)
    false_count: Mapped[int | None] = mapped_column(Integer, nullable=True)
    min_length: Mapped[int | None] = mapped_column(Integer, nullable=True)
    max_length: Mapped[int | None] = mapped_column(Integer, nullable=True)
    avg_length: Mapped[float | None] = mapped_column(Float, nullable=True)
    non_ascii_ratio: Mapped[float | None] = mapped_column(Float, nullable=True)
    min_date: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    max_date: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    date_span_days: Mapped[int | None] = mapped_column(Integer, nullable=True)
    metrics_json: Mapped[str | None] = mapped_column(Text, nullable=True)
    generated_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)


class DataQualityProfileColumnValue(Base):
    __tablename__ = "dq_profile_column_values"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    profile_run_id: Mapped[str] = mapped_column(
        String(255), ForeignKey("dq_profiles.profile_run_id", ondelete="CASCADE"), nullable=False
    )
    schema_name: Mapped[str | None] = mapped_column(String(255), nullable=True)
    table_name: Mapped[str] = mapped_column(String(255), nullable=False)
    column_name: Mapped[str] = mapped_column(String(255), nullable=False)
    value: Mapped[str | None] = mapped_column(String(500), nullable=True)
    value_hash: Mapped[str | None] = mapped_column(String(255), nullable=True)
    frequency: Mapped[int | None] = mapped_column(Integer, nullable=True)
    relative_freq: Mapped[float | None] = mapped_column(Float, nullable=True)
    rank: Mapped[int | None] = mapped_column(Integer, nullable=True)
    bucket_label: Mapped[str | None] = mapped_column(String(255), nullable=True)
    bucket_lower_bound: Mapped[float | None] = mapped_column(Float, nullable=True)
    bucket_upper_bound: Mapped[float | None] = mapped_column(Float, nullable=True)
    generated_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)


class DataQualityProfileResult(Base):
    __tablename__ = "dq_profile_results"

    result_id: Mapped[str] = mapped_column(String(255), primary_key=True)
    profile_run_id: Mapped[str | None] = mapped_column(
        String(255), ForeignKey("dq_profiles.profile_run_id", ondelete="CASCADE"), nullable=True
    )
    table_id: Mapped[str | None] = mapped_column(String(255), nullable=True)
    column_id: Mapped[str | None] = mapped_column(String(255), nullable=True)
    schema_name: Mapped[str | None] = mapped_column(String(255), nullable=True)
    table_name: Mapped[str | None] = mapped_column(String(255), nullable=True)
    column_name: Mapped[str | None] = mapped_column(String(255), nullable=True)
    data_type: Mapped[str | None] = mapped_column(String(255), nullable=True)
    general_type: Mapped[str | None] = mapped_column(String(50), nullable=True)
    record_count: Mapped[int | None] = mapped_column(Integer, nullable=True)
    null_count: Mapped[int | None] = mapped_column(Integer, nullable=True)
    distinct_count: Mapped[int | None] = mapped_column(Integer, nullable=True)
    min_value: Mapped[str | None] = mapped_column(String(255), nullable=True)
    max_value: Mapped[str | None] = mapped_column(String(255), nullable=True)
    avg_value: Mapped[float | None] = mapped_column(Float, nullable=True)
    stddev_value: Mapped[float | None] = mapped_column(Float, nullable=True)
    percentiles_json: Mapped[str | None] = mapped_column(Text, nullable=True)
    top_values_json: Mapped[str | None] = mapped_column(Text, nullable=True)
    metrics_json: Mapped[str | None] = mapped_column(Text, nullable=True)
    generated_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)


class DataQualityProfileAnomalyResult(Base):
    __tablename__ = "dq_profile_anomaly_results"

    anomaly_result_id: Mapped[str] = mapped_column(String(255), primary_key=True)
    profile_run_id: Mapped[str | None] = mapped_column(
        String(255), ForeignKey("dq_profiles.profile_run_id", ondelete="CASCADE"), nullable=True
    )
    table_id: Mapped[str | None] = mapped_column(String(255), nullable=True)
    column_id: Mapped[str | None] = mapped_column(String(255), nullable=True)
    table_name: Mapped[str | None] = mapped_column(String(255), nullable=True)
    column_name: Mapped[str | None] = mapped_column(String(255), nullable=True)
    anomaly_type_id: Mapped[str | None] = mapped_column(
        String(120), ForeignKey("dq_profile_anomaly_types.anomaly_type_id", ondelete="SET NULL"), nullable=True
    )
    severity: Mapped[str | None] = mapped_column(String(50), nullable=True)
    likelihood: Mapped[str | None] = mapped_column(String(50), nullable=True)
    detail: Mapped[str | None] = mapped_column(Text, nullable=True)
    pii_risk: Mapped[str | None] = mapped_column(String(50), nullable=True)
    dq_dimension: Mapped[str | None] = mapped_column(String(50), nullable=True)
    detected_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    dismissed: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    dismissed_by: Mapped[str | None] = mapped_column(String(255), nullable=True)
    dismissed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)


class DataQualityProfileOperation(Base):
    __tablename__ = "dq_profile_operations"

    operation_id: Mapped[str] = mapped_column(String(255), primary_key=True)
    profile_run_id: Mapped[str | None] = mapped_column(
        String(255), ForeignKey("dq_profiles.profile_run_id", ondelete="CASCADE"), nullable=True
    )
    target_table: Mapped[str | None] = mapped_column(String(255), nullable=True)
    rows_written: Mapped[int | None] = mapped_column(Integer, nullable=True)
    duration_ms: Mapped[int | None] = mapped_column(Integer, nullable=True)
    status: Mapped[str | None] = mapped_column(String(50), nullable=True)
    error_payload: Mapped[str | None] = mapped_column(Text, nullable=True)
    started_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    completed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)


class DataQualityDataTableCharacteristic(Base, TimestampMixin):
    __tablename__ = "dq_data_table_chars"

    table_id: Mapped[str] = mapped_column(String(255), primary_key=True)
    table_group_id: Mapped[str | None] = mapped_column(
        String(255), ForeignKey("dq_table_groups.table_group_id", ondelete="SET NULL"), nullable=True
    )
    schema_name: Mapped[str | None] = mapped_column(String(255), nullable=True)
    table_name: Mapped[str | None] = mapped_column(String(255), nullable=True)
    record_count: Mapped[int | None] = mapped_column(Integer, nullable=True)
    column_count: Mapped[int | None] = mapped_column(Integer, nullable=True)
    data_point_count: Mapped[int | None] = mapped_column(Integer, nullable=True)
    critical_data_element: Mapped[bool | None] = mapped_column(Boolean, nullable=True)
    data_source: Mapped[str | None] = mapped_column(String(255), nullable=True)
    source_system: Mapped[str | None] = mapped_column(String(255), nullable=True)
    source_process: Mapped[str | None] = mapped_column(String(255), nullable=True)
    business_domain: Mapped[str | None] = mapped_column(String(255), nullable=True)
    stakeholder_group: Mapped[str | None] = mapped_column(String(255), nullable=True)
    transform_level: Mapped[str | None] = mapped_column(String(255), nullable=True)
    data_product: Mapped[str | None] = mapped_column(String(255), nullable=True)
    dq_score_profiling: Mapped[float | None] = mapped_column(Float, nullable=True)
    dq_score_testing: Mapped[float | None] = mapped_column(Float, nullable=True)
    last_complete_profile_run_id: Mapped[str | None] = mapped_column(String(255), nullable=True)
    latest_anomaly_ct: Mapped[int | None] = mapped_column(Integer, nullable=True)
    latest_run_completed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)


class DataQualityDataColumnCharacteristic(Base, TimestampMixin):
    __tablename__ = "dq_data_column_chars"

    column_id: Mapped[str] = mapped_column(String(255), primary_key=True)
    table_id: Mapped[str | None] = mapped_column(
        String(255), ForeignKey("dq_tables.table_id", ondelete="SET NULL"), nullable=True
    )
    schema_name: Mapped[str | None] = mapped_column(String(255), nullable=True)
    table_name: Mapped[str | None] = mapped_column(String(255), nullable=True)
    column_name: Mapped[str | None] = mapped_column(String(255), nullable=True)
    data_type: Mapped[str | None] = mapped_column(String(255), nullable=True)
    functional_data_type: Mapped[str | None] = mapped_column(String(255), nullable=True)
    critical_data_element: Mapped[bool | None] = mapped_column(Boolean, nullable=True)
    pii_risk: Mapped[str | None] = mapped_column(String(50), nullable=True)
    dq_dimension: Mapped[str | None] = mapped_column(String(50), nullable=True)
    tags_json: Mapped[str | None] = mapped_column(Text, nullable=True)
    dq_score_profiling: Mapped[float | None] = mapped_column(Float, nullable=True)
    dq_score_testing: Mapped[float | None] = mapped_column(Float, nullable=True)
    last_complete_profile_run_id: Mapped[str | None] = mapped_column(String(255), nullable=True)
    latest_anomaly_ct: Mapped[int | None] = mapped_column(Integer, nullable=True)
    latest_run_completed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)


class DataQualityTestSuite(Base, TimestampMixin):
    __tablename__ = "dq_test_suites"

    test_suite_key: Mapped[str] = mapped_column(String(255), primary_key=True)
    project_key: Mapped[str | None] = mapped_column(
        String(255), ForeignKey("dq_projects.project_key", ondelete="SET NULL"), nullable=True
    )
    name: Mapped[str | None] = mapped_column(String(255), nullable=True)
    description: Mapped[str | None] = mapped_column(Text, nullable=True)
    severity: Mapped[str | None] = mapped_column(String(50), nullable=True)
    product_team_id: Mapped[str | None] = mapped_column(String(255), nullable=True)
    application_id: Mapped[str | None] = mapped_column(String(255), nullable=True)
    data_object_id: Mapped[str | None] = mapped_column(String(255), nullable=True)
    data_definition_id: Mapped[str | None] = mapped_column(String(255), nullable=True)


class DataQualityTest(Base, TimestampMixin):
    __tablename__ = "dq_tests"

    test_id: Mapped[str] = mapped_column(String(255), primary_key=True)
    table_group_id: Mapped[str] = mapped_column(
        String(255), ForeignKey("dq_table_groups.table_group_id", ondelete="CASCADE"), nullable=False
    )
    test_suite_key: Mapped[str | None] = mapped_column(
        String(255), ForeignKey("dq_test_suites.test_suite_key", ondelete="SET NULL"), nullable=True
    )
    name: Mapped[str | None] = mapped_column(String(255), nullable=True)
    rule_type: Mapped[str | None] = mapped_column(String(120), nullable=True)
    definition: Mapped[str | None] = mapped_column(Text, nullable=True)


class DataQualityTestRun(Base):
    __tablename__ = "dq_test_runs"

    test_run_id: Mapped[str] = mapped_column(String(255), primary_key=True)
    test_suite_key: Mapped[str | None] = mapped_column(
        String(255), ForeignKey("dq_test_suites.test_suite_key", ondelete="SET NULL"), nullable=True
    )
    project_key: Mapped[str | None] = mapped_column(String(255), nullable=True)
    status: Mapped[str | None] = mapped_column(String(50), nullable=True)
    started_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    completed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    duration_ms: Mapped[int | None] = mapped_column(Integer, nullable=True)
    total_tests: Mapped[int | None] = mapped_column(Integer, nullable=True)
    failed_tests: Mapped[int | None] = mapped_column(Integer, nullable=True)
    trigger_source: Mapped[str | None] = mapped_column(String(120), nullable=True)


class DataQualityTestResult(Base):
    __tablename__ = "dq_test_results"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    test_run_id: Mapped[str] = mapped_column(
        String(255), ForeignKey("dq_test_runs.test_run_id", ondelete="CASCADE"), nullable=False
    )
    test_id: Mapped[str] = mapped_column(
        String(255), ForeignKey("dq_tests.test_id", ondelete="CASCADE"), nullable=False
    )
    table_name: Mapped[str | None] = mapped_column(String(255), nullable=True)
    column_name: Mapped[str | None] = mapped_column(String(255), nullable=True)
    result_status: Mapped[str | None] = mapped_column(String(50), nullable=True)
    expected_value: Mapped[str | None] = mapped_column(String(255), nullable=True)
    actual_value: Mapped[str | None] = mapped_column(String(255), nullable=True)
    message: Mapped[str | None] = mapped_column(Text, nullable=True)
    detected_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)


class DataQualityAlert(Base):
    __tablename__ = "dq_alerts"

    alert_id: Mapped[str] = mapped_column(String(255), primary_key=True)
    source_type: Mapped[str | None] = mapped_column(String(120), nullable=True)
    source_ref: Mapped[str | None] = mapped_column(String(255), nullable=True)
    severity: Mapped[str | None] = mapped_column(String(50), nullable=True)
    title: Mapped[str | None] = mapped_column(String(255), nullable=True)
    details: Mapped[str | None] = mapped_column(Text, nullable=True)
    acknowledged: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    acknowledged_by: Mapped[str | None] = mapped_column(String(255), nullable=True)
    acknowledged_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, default=utcnow)


class DataQualitySetting(Base):
    __tablename__ = "dq_settings"

    key: Mapped[str] = mapped_column(String(255), primary_key=True)
    value: Mapped[str | None] = mapped_column(Text, nullable=True)
    updated_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)


__all__ = [
    "DataQualityProject",
    "DataQualityConnection",
    "DataQualityTableGroup",
    "DataQualityTable",
    "DataQualityProfile",
    "DataQualityProfileAnomalyType",
    "DataQualityProfileAnomaly",
    "DataQualityProfileColumn",
    "DataQualityProfileColumnValue",
    "DataQualityProfileResult",
    "DataQualityProfileAnomalyResult",
    "DataQualityProfileOperation",
    "DataQualityDataTableCharacteristic",
    "DataQualityDataColumnCharacteristic",
    "DataQualityTestSuite",
    "DataQualityTest",
    "DataQualityTestRun",
    "DataQualityTestResult",
    "DataQualityAlert",
    "DataQualitySetting",
]
