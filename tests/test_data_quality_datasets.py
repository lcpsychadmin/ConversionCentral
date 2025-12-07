from __future__ import annotations

from contextlib import contextmanager
from uuid import uuid4

from app.main import app
from app.routers.data_quality import get_profiling_service
from app.models.entities import (
    ConnectionTableSelection,
    ConstructedTable,
    DataDefinition,
    DataDefinitionField,
    DataDefinitionTable,
    DataObject,
    Field,
    ProcessArea,
    System,
    SystemConnection,
    Table,
)
from app.services.data_quality_keys import build_table_group_id
from app.services.data_quality_profiling import (
    PreparedProfileRun,
    ProfilingLaunchResult,
    ProfilingTarget,
)


class StubProfilingService:
    def __init__(self) -> None:
        self.calls: list[str] = []
        self.launched: list[str] = []
        self.sequence = 0

    def prepare_profile_run(self, table_group_id: str, **_) -> PreparedProfileRun:
        self.sequence += 1
        run_id = f"profile-run-{self.sequence}"
        self.calls.append(table_group_id)
        target = ProfilingTarget(
            table_group_id=table_group_id,
            table_group_name=None,
            connection_id="conn",
            connection_name=None,
            catalog=None,
            schema_name=None,
            http_path=None,
            project_key=None,
            profiling_job_id=None,
            is_active=True,
        )
        return PreparedProfileRun(
            target=target,
            profile_run_id=run_id,
            callback_url=None,
        )

    def launch_prepared_profile_run(self, prepared_run: PreparedProfileRun) -> ProfilingLaunchResult:
        self.launched.append(prepared_run.target.table_group_id)
        return ProfilingLaunchResult(
            table_group_id=prepared_run.target.table_group_id,
            profile_run_id=prepared_run.profile_run_id,
            job_id=100 + self.sequence,
            databricks_run_id=200 + self.sequence,
        )

    def start_profile_for_table_group(self, table_group_id: str, **kwargs) -> ProfilingLaunchResult:
        prepared = self.prepare_profile_run(table_group_id, **kwargs)
        return self.launch_prepared_profile_run(prepared)


@contextmanager
def override_profiling_service(stub: StubProfilingService):
    def _override():  # pragma: no cover - dependency override hook
        yield stub

    app.dependency_overrides[get_profiling_service] = _override
    try:
        yield
    finally:
        app.dependency_overrides.pop(get_profiling_service, None)


def _seed_dataset_context(db_session, *, include_connection: bool = True, connection_active: bool = True):
    product_team = ProcessArea(name="Product A", description="Team", status="active")
    system = System(name="Billing", physical_name="billing", description="Billing system")
    data_object = DataObject(name="Invoice", description="Invoices", status="active", process_area=product_team)
    definition = DataDefinition(data_object=data_object, system=system, description="Primary definition")
    table = Table(
        system=system,
        name="invoices",
        physical_name="raw.invoices",
        schema_name="finance",
        description="Invoices table",
        table_type="base",
    )
    definition_table = DataDefinitionTable(
        data_definition=definition,
        table=table,
        alias="Invoices",
        description="Primary invoices table",
        load_order=1,
    )

    field = Field(
        table=table,
        name="invoice_id",
        description="Invoice identifier",
        field_type="string",
        field_length=64,
    )
    definition_field = DataDefinitionField(
        definition_table=definition_table,
        field=field,
        display_order=1,
        is_unique=True,
        notes="Primary key",
    )

    db_session.add_all([product_team, system, data_object, definition, table, definition_table, field, definition_field])

    connection = None
    selection = None

    if include_connection:
        connection = SystemConnection(
            connection_type="jdbc",
            connection_string="jdbc://example",
            auth_method="username_password",
            active=connection_active,
        )
        selection = ConnectionTableSelection(
            system_connection=connection,
            schema_name="finance",
            table_name="invoices",
        )
        db_session.add_all([connection, selection])

    db_session.commit()

    return {
        "data_object_id": data_object.id,
        "definition_table_id": definition_table.id,
        "connection": connection,
    }


def test_dataset_hierarchy_returns_empty_list_when_no_records(client):
    response = client.get("/data-quality/datasets")

    assert response.status_code == 200
    assert response.json() == []


def test_dataset_hierarchy_groups_by_product_team_application_and_data_object(client, db_session):
    product_team = ProcessArea(name="Product A", description="Product team", status="active")
    system = System(name="Billing", physical_name="billing", description="Billing platform")
    data_object = DataObject(
        name="Invoice",
        description="Invoices data object",
        status="active",
        process_area=product_team,
    )
    definition = DataDefinition(
        data_object=data_object,
        system=system,
        description="Invoice data definition",
    )
    table = Table(
        system=system,
        name="invoices",
        physical_name="billing.invoices",
        schema_name="finance",
        description="Invoices staging table",
        table_type="base",
    )
    definition_table = DataDefinitionTable(
        data_definition=definition,
        table=table,
        alias="Invoices",
        description="Primary invoices table",
        load_order=1,
        is_construction=False,
    )
    field = Field(
        table=table,
        name="invoice_id",
        description="Invoice identifier",
        field_type="string",
        field_length=64,
        decimal_places=None,
    )
    definition_field = DataDefinitionField(
        definition_table=definition_table,
        field=field,
        display_order=0,
        is_unique=True,
        notes="Primary key",
    )

    db_session.add_all([product_team, system, data_object, definition, table, definition_table, field, definition_field])
    db_session.commit()

    response = client.get("/data-quality/datasets")

    assert response.status_code == 200
    assert response.json() == [
        {
            "productTeamId": str(product_team.id),
            "name": "Product A",
            "description": "Product team",
            "applications": [
                {
                    "applicationId": str(system.id),
                    "name": "Billing",
                    "description": "Billing platform",
                    "physicalName": "billing",
                    "dataObjects": [
                        {
                            "dataObjectId": str(data_object.id),
                            "name": "Invoice",
                            "description": "Invoices data object",
                            "dataDefinitions": [
                                {
                                    "dataDefinitionId": str(definition.id),
                                    "description": "Invoice data definition",
                                    "tables": [
                                        {
                                            "dataDefinitionTableId": str(definition_table.id),
                                            "tableId": str(table.id),
                                            "schemaName": "finance",
                                            "tableName": "invoices",
                                            "physicalName": "billing.invoices",
                                            "alias": "Invoices",
                                            "description": "Primary invoices table",
                                            "loadOrder": 1,
                                            "isConstructed": False,
                                            "tableType": "base",
                                            "fields": [
                                                {
                                                    "dataDefinitionFieldId": str(definition_field.id),
                                                    "fieldId": str(field.id),
                                                    "name": "invoice_id",
                                                    "description": "Invoice identifier",
                                                    "fieldType": "string",
                                                    "fieldLength": 64,
                                                    "decimalPlaces": None,
                                                    "applicationUsage": None,
                                                    "businessDefinition": None,
                                                    "notes": "Primary key",
                                                    "displayOrder": 0,
                                                    "isUnique": True,
                                                    "referenceTable": None,
                                                }
                                            ],
                                        }
                                    ],
                                }
                            ],
                        }
                    ],
                }
            ],
        }
    ]


def test_start_profile_runs_for_data_object(client, db_session):
    context = _seed_dataset_context(db_session)
    stub = StubProfilingService()
    expected_group = build_table_group_id(context["connection"].id, context["data_object_id"])

    with override_profiling_service(stub):
        response = client.post(f"/data-quality/datasets/{context['data_object_id']}/profile-runs")

    assert response.status_code == 202
    body = response.json()
    assert body["requestedTableCount"] == 1
    assert body["targetedTableGroupCount"] == 1
    assert body["profileRuns"][0]["tableGroupId"] == expected_group
    assert stub.calls == [expected_group]


def test_start_profile_runs_returns_404_when_data_object_missing(client):
    stub = StubProfilingService()
    missing_id = uuid4()

    with override_profiling_service(stub):
        response = client.post(f"/data-quality/datasets/{missing_id}/profile-runs")

    assert response.status_code == 404
    assert stub.calls == []


def test_start_profile_runs_returns_400_when_no_active_connections(client, db_session):
    context = _seed_dataset_context(db_session, include_connection=True, connection_active=False)
    stub = StubProfilingService()

    with override_profiling_service(stub):
        response = client.post(f"/data-quality/datasets/{context['data_object_id']}/profile-runs")

    assert response.status_code == 400
    assert stub.calls == []


def test_start_profile_runs_skips_tables_when_connections_are_ambiguous(client, db_session):
    product_team = ProcessArea(name="Product A", description="Team", status="active")
    system = System(name="Billing", physical_name="billing", description="Billing system")
    data_object = DataObject(name="Invoice", description="Invoices", status="active", process_area=product_team)
    definition = DataDefinition(data_object=data_object, system=system, description="Primary definition")

    matched_table = Table(
        system=system,
        name="invoices",
        physical_name="raw.invoices",
        schema_name="finance",
        description="Invoices table",
        table_type="base",
    )
    unmatched_table = Table(
        system=system,
        name="invoices_backup",
        physical_name="raw.invoices_backup",
        schema_name="archive",
        description="Archive table",
        table_type="base",
    )

    matched_link = DataDefinitionTable(
        data_definition=definition,
        table=matched_table,
        alias="Invoices",
        description="Primary invoices table",
        load_order=1,
    )
    unmatched_link = DataDefinitionTable(
        data_definition=definition,
        table=unmatched_table,
        alias="Archive",
        description="Archive invoices table",
        load_order=2,
    )

    connection_primary = SystemConnection(
        connection_type="jdbc",
        connection_string="jdbc://primary",
        auth_method="username_password",
        active=True,
    )
    primary_selection = ConnectionTableSelection(
        system_connection=connection_primary,
        schema_name="finance",
        table_name="invoices",
    )

    connection_secondary = SystemConnection(
        connection_type="jdbc",
        connection_string="jdbc://secondary",
        auth_method="username_password",
        active=True,
    )
    secondary_selection = ConnectionTableSelection(
        system_connection=connection_secondary,
        schema_name="other",
        table_name="external",
    )

    db_session.add_all(
        [
            product_team,
            system,
            data_object,
            definition,
            matched_table,
            unmatched_table,
            matched_link,
            unmatched_link,
            connection_primary,
            connection_secondary,
            primary_selection,
            secondary_selection,
        ]
    )
    db_session.commit()

    stub = StubProfilingService()

    with override_profiling_service(stub):
        response = client.post(f"/data-quality/datasets/{data_object.id}/profile-runs")

    assert response.status_code == 202
    payload = response.json()
    assert payload["requestedTableCount"] == 2
    assert payload["targetedTableGroupCount"] == 1
    assert payload["skippedTableIds"] == [str(unmatched_link.id)]
    expected_group = build_table_group_id(connection_primary.id, data_object.id)
    expected_run_id = f"profile-run-{stub.sequence}"
    assert payload["profileRuns"] == [
        {
            "tableGroupId": expected_group,
            "profileRunId": expected_run_id,
        }
    ]


def test_get_table_context_returns_table_group(client, db_session):
    context = _seed_dataset_context(db_session)
    expected_group = build_table_group_id(context["connection"].id, context["data_object_id"])

    response = client.get(f"/data-quality/datasets/tables/{context['definition_table_id']}/context")

    assert response.status_code == 200
    payload = response.json()
    assert payload["tableGroupId"] == expected_group
    assert payload["dataDefinitionTableId"] == str(context["definition_table_id"])


def test_table_context_prefers_databricks_connection_for_constructed_table(client, db_session):
    product_team = ProcessArea(name="Product Constructed", description="Team", status="active")
    system = System(name="Analytics", physical_name="analytics", description="Analytics platform")
    data_object = DataObject(name="Constructed", description="Constructed data", status="active", process_area=product_team)
    definition = DataDefinition(data_object=data_object, system=system, description="Constructed definition")
    base_table = Table(
        system=system,
        name="staging_constructed",
        physical_name="raw.staging_constructed",
        schema_name="raw",
        description="Base staging table",
        table_type="base",
    )
    definition_table = DataDefinitionTable(
        data_definition=definition,
        table=base_table,
        alias="Constructed table",
        is_construction=True,
    )
    constructed_table = ConstructedTable(
        data_definition=definition,
        data_definition_table=definition_table,
        name="curated_constructed",
        schema_name="analytics_curated",
        status="approved",
    )

    databricks_connection = SystemConnection(
        connection_type="jdbc",
        connection_string="jdbc:databricks://workspace.cloud.databricks.com/",
        auth_method="username_password",
        active=True,
    )
    fallback_connection = SystemConnection(
        connection_type="jdbc",
        connection_string="jdbc://legacy",
        auth_method="username_password",
        active=True,
    )

    db_session.add_all(
        [
            product_team,
            system,
            data_object,
            definition,
            base_table,
            definition_table,
            constructed_table,
            databricks_connection,
            fallback_connection,
        ]
    )
    db_session.commit()

    response = client.get(f"/data-quality/datasets/tables/{definition_table.id}/context")

    assert response.status_code == 200
    payload = response.json()
    expected_group = build_table_group_id(databricks_connection.id, data_object.id)
    assert payload["tableGroupId"] == expected_group
    assert payload["schemaName"] == "analytics_curated"
    assert payload["tableName"] == "curated_constructed"
    assert payload["physicalName"] == "curated_constructed"


def test_start_profile_runs_returns_404_when_no_tables(client, db_session):
    product_team = ProcessArea(name="Product B", description="Team", status="active")
    system = System(name="Orders", physical_name="orders", description="Orders system")
    data_object = DataObject(name="Order", description="Orders", status="active", process_area=product_team)

    db_session.add_all([product_team, system, data_object])
    db_session.commit()

    stub = StubProfilingService()

    with override_profiling_service(stub):
        response = client.post(f"/data-quality/datasets/{data_object.id}/profile-runs")

    assert response.status_code == 404
    assert stub.calls == []
