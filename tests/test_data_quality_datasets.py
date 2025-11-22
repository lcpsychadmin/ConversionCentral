from __future__ import annotations

from contextlib import contextmanager
from uuid import uuid4

from app.main import app
from app.routers.data_quality import get_profiling_service
from app.models.entities import (
    ConnectionTableSelection,
    DataDefinition,
    DataDefinitionTable,
    DataObject,
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
            system_id=None,
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

    db_session.add_all([product_team, system, data_object, definition, table, definition_table])

    connection = None
    selection = None

    if include_connection:
        connection = SystemConnection(
            system=system,
            connection_type="jdbc",
            connection_string="jdbc://example",
            auth_method="username_password",
            active=connection_active,
            ingestion_enabled=True,
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

    db_session.add_all([product_team, system, data_object, definition, table, definition_table])
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
