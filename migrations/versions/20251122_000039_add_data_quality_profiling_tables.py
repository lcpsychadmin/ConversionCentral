"""add data quality profiling tables

Revision ID: 20251122_000039
Revises: 20251119_000038
Create Date: 2025-11-22 10:00:00.000000
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa


revision = "20251122_000039"
down_revision = "20251119_000038"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "dq_projects",
        sa.Column("project_key", sa.String(length=255), primary_key=True, nullable=False),
        sa.Column("name", sa.String(length=255), nullable=True),
        sa.Column("description", sa.Text(), nullable=True),
        sa.Column("sql_flavor", sa.String(length=50), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),
    )

    op.create_table(
        "dq_connections",
        sa.Column("connection_id", sa.String(length=255), primary_key=True, nullable=False),
        sa.Column("project_key", sa.String(length=255), nullable=False),
        sa.Column("system_id", sa.String(length=255), nullable=True),
        sa.Column("name", sa.String(length=255), nullable=True),
        sa.Column("catalog", sa.String(length=255), nullable=True),
        sa.Column("schema_name", sa.String(length=255), nullable=True),
        sa.Column("http_path", sa.String(length=400), nullable=True),
        sa.Column("managed_credentials_ref", sa.String(length=255), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),
        sa.Column("is_active", sa.Boolean(), nullable=False, server_default=sa.text("true")),
        sa.ForeignKeyConstraint(["project_key"], ["dq_projects.project_key"], ondelete="CASCADE"),
    )

    op.create_table(
        "dq_table_groups",
        sa.Column("table_group_id", sa.String(length=255), primary_key=True, nullable=False),
        sa.Column("connection_id", sa.String(length=255), nullable=False),
        sa.Column("name", sa.String(length=255), nullable=True),
        sa.Column("description", sa.Text(), nullable=True),
        sa.Column("profiling_include_mask", sa.String(length=255), nullable=True),
        sa.Column("profiling_exclude_mask", sa.String(length=255), nullable=True),
        sa.Column("profiling_job_id", sa.String(length=255), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),
        sa.ForeignKeyConstraint(["connection_id"], ["dq_connections.connection_id"], ondelete="CASCADE"),
    )

    op.create_table(
        "dq_tables",
        sa.Column("table_id", sa.String(length=255), primary_key=True, nullable=False),
        sa.Column("table_group_id", sa.String(length=255), nullable=False),
        sa.Column("schema_name", sa.String(length=255), nullable=True),
        sa.Column("table_name", sa.String(length=255), nullable=True),
        sa.Column("source_table_id", sa.String(length=255), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),
        sa.ForeignKeyConstraint(["table_group_id"], ["dq_table_groups.table_group_id"], ondelete="CASCADE"),
    )

    op.create_table(
        "dq_profiles",
        sa.Column("profile_run_id", sa.String(length=255), primary_key=True, nullable=False),
        sa.Column("table_group_id", sa.String(length=255), nullable=False),
        sa.Column("status", sa.String(length=50), nullable=True),
        sa.Column("started_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("completed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("row_count", sa.BigInteger(), nullable=True),
        sa.Column("table_count", sa.BigInteger(), nullable=True),
        sa.Column("column_count", sa.BigInteger(), nullable=True),
        sa.Column("anomaly_count", sa.Integer(), nullable=True),
        sa.Column("dq_score_profiling", sa.Float(), nullable=True),
        sa.Column("dq_score_testing", sa.Float(), nullable=True),
        sa.Column("profile_mode", sa.String(length=50), nullable=True),
        sa.Column("profile_version", sa.String(length=50), nullable=True),
        sa.Column("payload_path", sa.String(length=500), nullable=True),
        sa.Column("databricks_run_id", sa.String(length=255), nullable=True),
        sa.ForeignKeyConstraint(["table_group_id"], ["dq_table_groups.table_group_id"], ondelete="CASCADE"),
    )
    op.create_index(
        "ix_dq_profiles_table_group",
        "dq_profiles",
        ["table_group_id"],
    )

    op.create_table(
        "dq_profile_anomaly_types",
        sa.Column("anomaly_type_id", sa.String(length=120), primary_key=True, nullable=False),
        sa.Column("name", sa.String(length=255), nullable=True),
        sa.Column("category", sa.String(length=120), nullable=True),
        sa.Column("default_severity", sa.String(length=50), nullable=True),
        sa.Column("default_likelihood", sa.String(length=50), nullable=True),
        sa.Column("description", sa.Text(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),
    )

    op.create_table(
        "dq_profile_anomalies",
        sa.Column("id", sa.BigInteger(), primary_key=True, autoincrement=True),
        sa.Column("profile_run_id", sa.String(length=255), nullable=False),
        sa.Column("table_name", sa.String(length=255), nullable=True),
        sa.Column("column_name", sa.String(length=255), nullable=True),
        sa.Column("anomaly_type", sa.String(length=120), nullable=True),
        sa.Column("severity", sa.String(length=50), nullable=True),
        sa.Column("description", sa.Text(), nullable=True),
        sa.Column("detected_at", sa.DateTime(timezone=True), nullable=True),
        sa.ForeignKeyConstraint(["profile_run_id"], ["dq_profiles.profile_run_id"], ondelete="CASCADE"),
    )

    op.create_table(
        "dq_profile_columns",
        sa.Column("id", sa.BigInteger(), primary_key=True, autoincrement=True),
        sa.Column("profile_run_id", sa.String(length=255), nullable=False),
        sa.Column("schema_name", sa.String(length=255), nullable=True),
        sa.Column("table_name", sa.String(length=255), nullable=False),
        sa.Column("column_name", sa.String(length=255), nullable=False),
        sa.Column("qualified_name", sa.String(length=500), nullable=True),
        sa.Column("data_type", sa.String(length=255), nullable=True),
        sa.Column("general_type", sa.String(length=50), nullable=True),
        sa.Column("ordinal_position", sa.Integer(), nullable=True),
        sa.Column("row_count", sa.BigInteger(), nullable=True),
        sa.Column("null_count", sa.BigInteger(), nullable=True),
        sa.Column("non_null_count", sa.BigInteger(), nullable=True),
        sa.Column("distinct_count", sa.BigInteger(), nullable=True),
        sa.Column("min_value", sa.String(length=255), nullable=True),
        sa.Column("max_value", sa.String(length=255), nullable=True),
        sa.Column("avg_value", sa.Float(), nullable=True),
        sa.Column("stddev_value", sa.Float(), nullable=True),
        sa.Column("median_value", sa.Float(), nullable=True),
        sa.Column("p95_value", sa.Float(), nullable=True),
        sa.Column("true_count", sa.BigInteger(), nullable=True),
        sa.Column("false_count", sa.BigInteger(), nullable=True),
        sa.Column("min_length", sa.Integer(), nullable=True),
        sa.Column("max_length", sa.Integer(), nullable=True),
        sa.Column("avg_length", sa.Float(), nullable=True),
        sa.Column("non_ascii_ratio", sa.Float(), nullable=True),
        sa.Column("min_date", sa.Date(), nullable=True),
        sa.Column("max_date", sa.Date(), nullable=True),
        sa.Column("date_span_days", sa.Integer(), nullable=True),
        sa.Column("metrics_json", sa.Text(), nullable=True),
        sa.Column("generated_at", sa.DateTime(timezone=True), nullable=True),
        sa.ForeignKeyConstraint(["profile_run_id"], ["dq_profiles.profile_run_id"], ondelete="CASCADE"),
    )
    op.create_index(
        "ix_dq_profile_columns_run_table_col",
        "dq_profile_columns",
        ["profile_run_id", "table_name", "column_name"],
    )

    op.create_table(
        "dq_profile_column_values",
        sa.Column("id", sa.BigInteger(), primary_key=True, autoincrement=True),
        sa.Column("profile_run_id", sa.String(length=255), nullable=False),
        sa.Column("schema_name", sa.String(length=255), nullable=True),
        sa.Column("table_name", sa.String(length=255), nullable=False),
        sa.Column("column_name", sa.String(length=255), nullable=False),
        sa.Column("value", sa.String(length=500), nullable=True),
        sa.Column("value_hash", sa.String(length=255), nullable=True),
        sa.Column("frequency", sa.BigInteger(), nullable=True),
        sa.Column("relative_freq", sa.Float(), nullable=True),
        sa.Column("rank", sa.Integer(), nullable=True),
        sa.Column("bucket_label", sa.String(length=255), nullable=True),
        sa.Column("bucket_lower_bound", sa.Float(), nullable=True),
        sa.Column("bucket_upper_bound", sa.Float(), nullable=True),
        sa.Column("generated_at", sa.DateTime(timezone=True), nullable=True),
        sa.ForeignKeyConstraint(["profile_run_id"], ["dq_profiles.profile_run_id"], ondelete="CASCADE"),
    )
    op.create_index(
        "ix_dq_profile_column_values_run_table_col",
        "dq_profile_column_values",
        ["profile_run_id", "table_name", "column_name"],
    )

    op.create_table(
        "dq_profile_results",
        sa.Column("result_id", sa.String(length=255), primary_key=True, nullable=False),
        sa.Column("profile_run_id", sa.String(length=255), nullable=True),
        sa.Column("table_id", sa.String(length=255), nullable=True),
        sa.Column("column_id", sa.String(length=255), nullable=True),
        sa.Column("schema_name", sa.String(length=255), nullable=True),
        sa.Column("table_name", sa.String(length=255), nullable=True),
        sa.Column("column_name", sa.String(length=255), nullable=True),
        sa.Column("data_type", sa.String(length=255), nullable=True),
        sa.Column("general_type", sa.String(length=50), nullable=True),
        sa.Column("record_count", sa.BigInteger(), nullable=True),
        sa.Column("null_count", sa.BigInteger(), nullable=True),
        sa.Column("distinct_count", sa.BigInteger(), nullable=True),
        sa.Column("min_value", sa.String(length=255), nullable=True),
        sa.Column("max_value", sa.String(length=255), nullable=True),
        sa.Column("avg_value", sa.Float(), nullable=True),
        sa.Column("stddev_value", sa.Float(), nullable=True),
        sa.Column("percentiles_json", sa.Text(), nullable=True),
        sa.Column("top_values_json", sa.Text(), nullable=True),
        sa.Column("metrics_json", sa.Text(), nullable=True),
        sa.Column("generated_at", sa.DateTime(timezone=True), nullable=True),
        sa.ForeignKeyConstraint(["profile_run_id"], ["dq_profiles.profile_run_id"], ondelete="CASCADE"),
    )
    op.create_index(
        "ix_dq_profile_results_run_table_col",
        "dq_profile_results",
        ["profile_run_id", "table_name", "column_name"],
    )

    op.create_table(
        "dq_profile_anomaly_results",
        sa.Column("anomaly_result_id", sa.String(length=255), primary_key=True, nullable=False),
        sa.Column("profile_run_id", sa.String(length=255), nullable=True),
        sa.Column("table_id", sa.String(length=255), nullable=True),
        sa.Column("column_id", sa.String(length=255), nullable=True),
        sa.Column("table_name", sa.String(length=255), nullable=True),
        sa.Column("column_name", sa.String(length=255), nullable=True),
        sa.Column("anomaly_type_id", sa.String(length=120), nullable=True),
        sa.Column("severity", sa.String(length=50), nullable=True),
        sa.Column("likelihood", sa.String(length=50), nullable=True),
        sa.Column("detail", sa.Text(), nullable=True),
        sa.Column("pii_risk", sa.String(length=50), nullable=True),
        sa.Column("dq_dimension", sa.String(length=50), nullable=True),
        sa.Column("detected_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("dismissed", sa.Boolean(), nullable=False, server_default=sa.text("false")),
        sa.Column("dismissed_by", sa.String(length=255), nullable=True),
        sa.Column("dismissed_at", sa.DateTime(timezone=True), nullable=True),
        sa.ForeignKeyConstraint(["profile_run_id"], ["dq_profiles.profile_run_id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["anomaly_type_id"], ["dq_profile_anomaly_types.anomaly_type_id"], ondelete="SET NULL"),
    )
    op.create_index(
        "ix_dq_profile_anomaly_results_run",
        "dq_profile_anomaly_results",
        ["profile_run_id"],
    )

    op.create_table(
        "dq_profile_operations",
        sa.Column("operation_id", sa.String(length=255), primary_key=True, nullable=False),
        sa.Column("profile_run_id", sa.String(length=255), nullable=True),
        sa.Column("target_table", sa.String(length=255), nullable=True),
        sa.Column("rows_written", sa.BigInteger(), nullable=True),
        sa.Column("duration_ms", sa.BigInteger(), nullable=True),
        sa.Column("status", sa.String(length=50), nullable=True),
        sa.Column("error_payload", sa.Text(), nullable=True),
        sa.Column("started_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("completed_at", sa.DateTime(timezone=True), nullable=True),
        sa.ForeignKeyConstraint(["profile_run_id"], ["dq_profiles.profile_run_id"], ondelete="CASCADE"),
    )

    op.create_table(
        "dq_data_table_chars",
        sa.Column("table_id", sa.String(length=255), primary_key=True, nullable=False),
        sa.Column("table_group_id", sa.String(length=255), nullable=True),
        sa.Column("schema_name", sa.String(length=255), nullable=True),
        sa.Column("table_name", sa.String(length=255), nullable=True),
        sa.Column("record_count", sa.BigInteger(), nullable=True),
        sa.Column("column_count", sa.Integer(), nullable=True),
        sa.Column("data_point_count", sa.BigInteger(), nullable=True),
        sa.Column("critical_data_element", sa.Boolean(), nullable=True),
        sa.Column("data_source", sa.String(length=255), nullable=True),
        sa.Column("source_system", sa.String(length=255), nullable=True),
        sa.Column("source_process", sa.String(length=255), nullable=True),
        sa.Column("business_domain", sa.String(length=255), nullable=True),
        sa.Column("stakeholder_group", sa.String(length=255), nullable=True),
        sa.Column("transform_level", sa.String(length=255), nullable=True),
        sa.Column("data_product", sa.String(length=255), nullable=True),
        sa.Column("dq_score_profiling", sa.Float(), nullable=True),
        sa.Column("dq_score_testing", sa.Float(), nullable=True),
        sa.Column("last_complete_profile_run_id", sa.String(length=255), nullable=True),
        sa.Column("latest_anomaly_ct", sa.Integer(), nullable=True),
        sa.Column("latest_run_completed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),
        sa.ForeignKeyConstraint(["table_group_id"], ["dq_table_groups.table_group_id"], ondelete="SET NULL"),
        sa.ForeignKeyConstraint(["table_id"], ["dq_tables.table_id"], ondelete="CASCADE"),
    )
    op.create_index(
        "ix_dq_data_table_chars_group",
        "dq_data_table_chars",
        ["table_group_id"],
    )

    op.create_table(
        "dq_data_column_chars",
        sa.Column("column_id", sa.String(length=255), primary_key=True, nullable=False),
        sa.Column("table_id", sa.String(length=255), nullable=True),
        sa.Column("schema_name", sa.String(length=255), nullable=True),
        sa.Column("table_name", sa.String(length=255), nullable=True),
        sa.Column("column_name", sa.String(length=255), nullable=True),
        sa.Column("data_type", sa.String(length=255), nullable=True),
        sa.Column("functional_data_type", sa.String(length=255), nullable=True),
        sa.Column("critical_data_element", sa.Boolean(), nullable=True),
        sa.Column("pii_risk", sa.String(length=50), nullable=True),
        sa.Column("dq_dimension", sa.String(length=50), nullable=True),
        sa.Column("tags_json", sa.Text(), nullable=True),
        sa.Column("dq_score_profiling", sa.Float(), nullable=True),
        sa.Column("dq_score_testing", sa.Float(), nullable=True),
        sa.Column("last_complete_profile_run_id", sa.String(length=255), nullable=True),
        sa.Column("latest_anomaly_ct", sa.Integer(), nullable=True),
        sa.Column("latest_run_completed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),
        sa.ForeignKeyConstraint(["table_id"], ["dq_tables.table_id"], ondelete="SET NULL"),
    )
    op.create_index(
        "ix_dq_data_column_chars_table",
        "dq_data_column_chars",
        ["table_id"],
    )

    op.create_table(
        "dq_test_suites",
        sa.Column("test_suite_key", sa.String(length=255), primary_key=True, nullable=False),
        sa.Column("project_key", sa.String(length=255), nullable=True),
        sa.Column("name", sa.String(length=255), nullable=True),
        sa.Column("description", sa.Text(), nullable=True),
        sa.Column("severity", sa.String(length=50), nullable=True),
        sa.Column("product_team_id", sa.String(length=255), nullable=True),
        sa.Column("application_id", sa.String(length=255), nullable=True),
        sa.Column("data_object_id", sa.String(length=255), nullable=True),
        sa.Column("data_definition_id", sa.String(length=255), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),
        sa.ForeignKeyConstraint(["project_key"], ["dq_projects.project_key"], ondelete="SET NULL"),
    )

    op.create_table(
        "dq_tests",
        sa.Column("test_id", sa.String(length=255), primary_key=True, nullable=False),
        sa.Column("table_group_id", sa.String(length=255), nullable=False),
        sa.Column("test_suite_key", sa.String(length=255), nullable=True),
        sa.Column("name", sa.String(length=255), nullable=True),
        sa.Column("rule_type", sa.String(length=120), nullable=True),
        sa.Column("definition", sa.Text(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),
        sa.ForeignKeyConstraint(["table_group_id"], ["dq_table_groups.table_group_id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["test_suite_key"], ["dq_test_suites.test_suite_key"], ondelete="SET NULL"),
    )

    op.create_table(
        "dq_test_runs",
        sa.Column("test_run_id", sa.String(length=255), primary_key=True, nullable=False),
        sa.Column("test_suite_key", sa.String(length=255), nullable=True),
        sa.Column("project_key", sa.String(length=255), nullable=True),
        sa.Column("status", sa.String(length=50), nullable=True),
        sa.Column("started_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("completed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("duration_ms", sa.BigInteger(), nullable=True),
        sa.Column("total_tests", sa.Integer(), nullable=True),
        sa.Column("failed_tests", sa.Integer(), nullable=True),
        sa.Column("trigger_source", sa.String(length=120), nullable=True),
        sa.ForeignKeyConstraint(["test_suite_key"], ["dq_test_suites.test_suite_key"], ondelete="SET NULL"),
    )

    op.create_table(
        "dq_test_results",
        sa.Column("id", sa.BigInteger(), primary_key=True, autoincrement=True),
        sa.Column("test_run_id", sa.String(length=255), nullable=False),
        sa.Column("test_id", sa.String(length=255), nullable=False),
        sa.Column("table_name", sa.String(length=255), nullable=True),
        sa.Column("column_name", sa.String(length=255), nullable=True),
        sa.Column("result_status", sa.String(length=50), nullable=True),
        sa.Column("expected_value", sa.String(length=255), nullable=True),
        sa.Column("actual_value", sa.String(length=255), nullable=True),
        sa.Column("message", sa.Text(), nullable=True),
        sa.Column("detected_at", sa.DateTime(timezone=True), nullable=True),
        sa.ForeignKeyConstraint(["test_run_id"], ["dq_test_runs.test_run_id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["test_id"], ["dq_tests.test_id"], ondelete="CASCADE"),
    )

    op.create_table(
        "dq_alerts",
        sa.Column("alert_id", sa.String(length=255), primary_key=True, nullable=False),
        sa.Column("source_type", sa.String(length=120), nullable=True),
        sa.Column("source_ref", sa.String(length=255), nullable=True),
        sa.Column("severity", sa.String(length=50), nullable=True),
        sa.Column("title", sa.String(length=255), nullable=True),
        sa.Column("details", sa.Text(), nullable=True),
        sa.Column("acknowledged", sa.Boolean(), nullable=False, server_default=sa.text("false")),
        sa.Column("acknowledged_by", sa.String(length=255), nullable=True),
        sa.Column("acknowledged_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),
    )

    op.create_table(
        "dq_settings",
        sa.Column("key", sa.String(length=255), primary_key=True, nullable=False),
        sa.Column("value", sa.Text(), nullable=True),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=True),
    )


def downgrade() -> None:
    op.drop_table("dq_settings")
    op.drop_table("dq_alerts")
    op.drop_table("dq_test_results")
    op.drop_table("dq_test_runs")
    op.drop_table("dq_tests")
    op.drop_table("dq_test_suites")
    op.drop_index("ix_dq_data_column_chars_table", table_name="dq_data_column_chars")
    op.drop_table("dq_data_column_chars")
    op.drop_index("ix_dq_data_table_chars_group", table_name="dq_data_table_chars")
    op.drop_table("dq_data_table_chars")
    op.drop_table("dq_profile_operations")
    op.drop_index("ix_dq_profile_anomaly_results_run", table_name="dq_profile_anomaly_results")
    op.drop_table("dq_profile_anomaly_results")
    op.drop_index("ix_dq_profile_results_run_table_col", table_name="dq_profile_results")
    op.drop_table("dq_profile_results")
    op.drop_index("ix_dq_profile_column_values_run_table_col", table_name="dq_profile_column_values")
    op.drop_table("dq_profile_column_values")
    op.drop_index("ix_dq_profile_columns_run_table_col", table_name="dq_profile_columns")
    op.drop_table("dq_profile_columns")
    op.drop_table("dq_profile_anomalies")
    op.drop_table("dq_profile_anomaly_types")
    op.drop_index("ix_dq_profiles_table_group", table_name="dq_profiles")
    op.drop_table("dq_profiles")
    op.drop_table("dq_tables")
    op.drop_table("dq_table_groups")
    op.drop_table("dq_connections")
    op.drop_table("dq_projects")