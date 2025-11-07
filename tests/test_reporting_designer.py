import os
import tempfile
from types import SimpleNamespace

import sqlalchemy as sa

from app.ingestion.engine import get_ingestion_engine, reset_ingestion_engine
from app.models import (
    ConstructedField,
    ConstructedTable,
    DataDefinition,
    DataDefinitionTable,
    DataObject,
    Field,
    ProcessArea,
    SystemConnection,
    System,
    Table,
)
from app.schemas import SystemConnectionType
from app.schemas.reporting import ReportDesignerDefinition, ReportJoinType, ReportSortDirection
from app.services.databricks_sql import DatabricksConnectionParams
from app.services.reporting_designer import generate_report_preview


def _build_report_definition(table: Table, fields: list[Field]) -> ReportDesignerDefinition:
    return ReportDesignerDefinition(
        tables=[
            {
                "tableId": table.id,
                "label": table.name,
                "physicalName": table.physical_name,
                "schemaName": table.schema_name,
                "alias": None,
                "position": {"x": 0, "y": 0},
            }
        ],
        joins=[],
        columns=[
            {
                "order": index,
                "fieldId": field.id,
                "fieldName": field.name,
                "fieldDescription": field.description,
                "tableId": table.id,
                "tableName": table.name,
                "show": True,
                "sort": ReportSortDirection.NONE,
                "aggregate": None,
                "criteria": [],
            }
            for index, field in enumerate(fields)
        ],
        criteriaRowCount=0,
        groupingEnabled=False,
    )


def test_constructed_table_preview_uses_managed_warehouse(db_session):
    reset_ingestion_engine()

    process_area = ProcessArea(name="Customer Experience")
    data_object = DataObject(process_area=process_area, name="Customer", description="Test object")
    system = System(name="Constructed Source", physical_name="constructed_source")
    table = Table(system=system, name="Constructed Table", physical_name="constructed_table")
    field = Field(
        table=table,
        name="value",
        description="Test value",
        field_type="integer",
        field_length=None,
        decimal_places=None,
    )
    audit_field = Field(
        table=table,
        name="Created By",
        description="Audit field",
        field_type="string",
        field_length=255,
        decimal_places=None,
    )

    definition = DataDefinition(data_object=data_object, system=system)
    constructed_link = DataDefinitionTable(data_definition=definition, table=table, is_construction=True)
    constructed_table = ConstructedTable(
        data_definition=definition,
        data_definition_table=constructed_link,
        name="constructed_table",
    )
    constructed_link.constructed_table = constructed_table

    constructed_field_value = ConstructedField(
        constructed_table=constructed_table,
        name="value",
        data_type="integer",
        is_nullable=True,
        display_order=0,
    )
    constructed_field_created_by = ConstructedField(
        constructed_table=constructed_table,
        name="Created By",
        data_type="string",
        is_nullable=True,
        display_order=1,
    )

    db_session.add_all(
        [
            process_area,
            data_object,
            system,
            table,
            field,
            audit_field,
            definition,
            constructed_link,
            constructed_table,
            constructed_field_value,
            constructed_field_created_by,
        ]
    )
    db_session.commit()

    # The preview should read from the ingestion engine, so only create the physical table there.
    engine = get_ingestion_engine()
    with engine.begin() as connection:
        connection.execute(sa.text('DROP TABLE IF EXISTS "constructed_table"'))
        connection.execute(sa.text('CREATE TABLE "constructed_table" (value INTEGER, created_by TEXT)'))
        connection.execute(
            sa.text('INSERT INTO "constructed_table" (value, created_by) VALUES (42, :created_by)'),
            {"created_by": "system"},
        )

    definition_payload = _build_report_definition(table, [field, audit_field])

    response = generate_report_preview(db_session, definition_payload, limit=5)

    assert response.row_count == 1
    assert response.rows == [{"value": 42, "Created By": "system"}]
    assert response.columns == ["value", "Created By"]
    assert response.limit == 5

    reset_ingestion_engine()


def test_preview_collapses_multiple_databricks_connections(db_session, monkeypatch):
    reset_ingestion_engine()

    temp_dir = tempfile.TemporaryDirectory()
    ingestion_path = os.path.join(temp_dir.name, "ingestion.db")
    constructed_path = os.path.join(temp_dir.name, "constructed_data.db")
    inner_engine = sa.create_engine(f"sqlite:///{ingestion_path}", future=True)

    class _DatabricksProxy:
        def __init__(self, inner, schema_path: str):
            self._inner = inner
            self._schema_path = schema_path
            self.dialect = SimpleNamespace(name="databricks")

        def connect(self):
            connection = self._inner.connect()
            escaped_path = self._schema_path.replace("'", "''")
            try:
                connection.exec_driver_sql(
                    f"ATTACH DATABASE '{escaped_path}' AS constructed_data"
                )
            except Exception as exc:  # pragma: no cover - defensive guard
                if "already in use" not in str(exc).lower():
                    raise
            return connection

        def dispose(self):
            return self._inner.dispose()

        def __getattr__(self, item):
            return getattr(self._inner, item)

    fake_engine = _DatabricksProxy(inner_engine, constructed_path)

    params = DatabricksConnectionParams(
        workspace_host="example.cloud.databricks.com",
        http_path="/sql/1/2",
        access_token="token",
        catalog=None,
        schema_name=None,
        constructed_schema="constructed_data",
    )

    monkeypatch.setattr("app.services.reporting_designer.get_ingestion_engine", lambda: fake_engine)
    monkeypatch.setattr("app.services.reporting_designer.get_ingestion_connection_params", lambda: params)
    monkeypatch.setattr("app.services.connection_resolver.get_ingestion_connection_params", lambda: params)

    process_area = ProcessArea(name="Analytics")
    data_object = DataObject(process_area=process_area, name="Constructed Customers", description="Test object")

    constructed_system = System(name="Constructed", physical_name="constructed")
    constructed_table_entity = Table(
        system=constructed_system,
        name="Constructed Customers",
        physical_name="constructed_customers",
    )
    constructed_field = Field(
        table=constructed_table_entity,
        name="customer_id",
        description="Customer identifier",
        field_type="string",
        field_length=50,
        decimal_places=None,
    )

    definition = DataDefinition(data_object=data_object, system=constructed_system)
    constructed_link = DataDefinitionTable(
        data_definition=definition,
        table=constructed_table_entity,
        is_construction=True,
    )
    constructed_table = ConstructedTable(
        data_definition=definition,
        data_definition_table=constructed_link,
        name="constructed_customers",
    )
    constructed_field_model = ConstructedField(
        constructed_table=constructed_table,
        name="customer_id",
        data_type="string",
        is_nullable=False,
        display_order=0,
    )
    constructed_link.constructed_table = constructed_table

    remote_system = System(name="Demo_Data", physical_name="demo_data")
    remote_connection = SystemConnection(
        system=remote_system,
        connection_type=SystemConnectionType.JDBC,
        connection_string="jdbc:databricks://example.cloud.databricks.com:443/default",
    )
    remote_table = Table(
        system=remote_system,
        name="Demo Customers",
        physical_name="demo_customers",
    )
    remote_join_field = Field(
        table=remote_table,
        name="customer_id",
        description="Remote customer id",
        field_type="string",
        field_length=50,
        decimal_places=None,
    )
    remote_status_field = Field(
        table=remote_table,
        name="status",
        description="Status",
        field_type="string",
        field_length=20,
        decimal_places=None,
    )

    db_session.add_all(
        [
            process_area,
            data_object,
            constructed_system,
            constructed_table_entity,
            constructed_field,
            definition,
            constructed_link,
            constructed_table,
            constructed_field_model,
            remote_system,
            remote_connection,
            remote_table,
            remote_join_field,
            remote_status_field,
        ]
    )
    db_session.commit()

    with fake_engine.connect() as connection:
        connection.execute(sa.text('DROP TABLE IF EXISTS "constructed_customers"'))
        connection.execute(sa.text('DROP TABLE IF EXISTS constructed_data."constructed_customers"'))
        connection.execute(
            sa.text('CREATE TABLE constructed_data."constructed_customers" (customer_id TEXT)')
        )
        connection.execute(
            sa.text('INSERT INTO constructed_data."constructed_customers" (customer_id) VALUES (:cid)'),
            {"cid": "CUST-001"},
        )
        connection.commit()
        connection.execute(sa.text('DROP TABLE IF EXISTS "demo_customers"'))
        connection.execute(sa.text('CREATE TABLE "demo_customers" (customer_id TEXT, status TEXT)'))
        connection.execute(
            sa.text('INSERT INTO "demo_customers" (customer_id, status) VALUES (:cid, :status)'),
            {"cid": "CUST-001", "status": "active"},
        )
        connection.commit()

    definition_payload = ReportDesignerDefinition(
        tables=[
            {
                "tableId": constructed_table_entity.id,
                "label": constructed_table_entity.name,
                "physicalName": constructed_table_entity.physical_name,
                "schemaName": constructed_table_entity.schema_name,
                "alias": None,
                "position": {"x": 0, "y": 0},
            },
            {
                "tableId": remote_table.id,
                "label": remote_table.name,
                "physicalName": remote_table.physical_name,
                "schemaName": remote_table.schema_name,
                "alias": None,
                "position": {"x": 200, "y": 0},
            },
        ],
        joins=[
            {
                "id": "join-1",
                "sourceTableId": constructed_table_entity.id,
                "targetTableId": remote_table.id,
                "sourceFieldId": constructed_field.id,
                "targetFieldId": remote_join_field.id,
                "joinType": ReportJoinType.INNER,
            }
        ],
        columns=[
            {
                "order": 0,
                "fieldId": constructed_field.id,
                "fieldName": constructed_field.name,
                "fieldDescription": constructed_field.description,
                "tableId": constructed_table_entity.id,
                "tableName": constructed_table_entity.name,
                "show": True,
                "sort": ReportSortDirection.NONE,
                "aggregate": None,
                "criteria": [],
            },
            {
                "order": 1,
                "fieldId": remote_status_field.id,
                "fieldName": remote_status_field.name,
                "fieldDescription": remote_status_field.description,
                "tableId": remote_table.id,
                "tableName": remote_table.name,
                "show": True,
                "sort": ReportSortDirection.NONE,
                "aggregate": None,
                "criteria": [],
            },
        ],
        criteriaRowCount=0,
        groupingEnabled=False,
    )

    response = generate_report_preview(db_session, definition_payload, limit=10)

    assert response.row_count == 1
    assert response.columns == ["customer_id", "status"]
    assert response.rows == [{"customer_id": "CUST-001", "status": "active"}]
    assert response.limit == 10

    reset_ingestion_engine()
    fake_engine.dispose()
    temp_dir.cleanup()
