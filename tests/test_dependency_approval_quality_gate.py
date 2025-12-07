from __future__ import annotations

from typing import Iterable
from uuid import uuid4

import pytest
from fastapi.testclient import TestClient

from app.models import (
    DataDefinition,
    DataDefinitionTable,
    DataObject,
    DataObjectSystem,
    DependencyApproval,
    ProcessArea,
    System,
    Table,
    TableDependency,
    User,
    Workspace,
)
from app.services.data_quality_keys import build_project_key


def _seed_table_dependency(db_session):
    approver = User(id=uuid4(), name="Approver", email="approver@example.com")
    system = System(id=uuid4(), name="CRM", physical_name="crm_base")
    workspace = Workspace(id=uuid4(), name="Workspace QA", slug="workspace-qa")
    process_area = ProcessArea(id=uuid4(), name="Sales")
    data_object = DataObject(
        id=uuid4(),
        process_area_id=process_area.id,
        workspace_id=workspace.id,
        name="Customers",
    )
    table_upstream = Table(
        id=uuid4(),
        system=system,
        name="customers",
        physical_name="crm_customers",
        schema_name="crm",
    )
    table_downstream = Table(
        id=uuid4(),
        system=system,
        name="orders",
        physical_name="crm_orders",
        schema_name="crm",
    )
    data_definition = DataDefinition(id=uuid4(), data_object_id=data_object.id, system_id=system.id)
    definition_upstream = DataDefinitionTable(
        id=uuid4(),
        data_definition=data_definition,
        table=table_upstream,
    )
    definition_downstream = DataDefinitionTable(
        id=uuid4(),
        data_definition=data_definition,
        table=table_downstream,
    )
    link = DataObjectSystem(id=uuid4(), data_object_id=data_object.id, system_id=system.id)
    dependency = TableDependency(
        id=uuid4(),
        predecessor=table_upstream,
        successor=table_downstream,
        dependency_type="precedence",
    )
    db_session.add_all(
        [
            approver,
            system,
            workspace,
            process_area,
            data_object,
            table_upstream,
            table_downstream,
            data_definition,
            definition_upstream,
            definition_downstream,
            link,
            dependency,
        ]
    )
    db_session.commit()
    return approver, system, dependency, data_object


@pytest.fixture
def seeded_approval(db_session):
    approver, system, dependency, data_object = _seed_table_dependency(db_session)
    approval = DependencyApproval(
        id=uuid4(),
        table_dependency_id=dependency.id,
        approver_id=approver.id,
        decision="pending",
    )
    db_session.add(approval)
    db_session.commit()
    return approval, approver, system, dependency, data_object


def test_update_blocks_when_data_quality_reports_failure(
    client: TestClient,
    monkeypatch: pytest.MonkeyPatch,
    seeded_approval,
):
    approval, _approver, _system, _dependency, _data_object = seeded_approval

    def _fail_guard(_keys: Iterable[str]) -> None:
        from fastapi import HTTPException
        from starlette import status

        raise HTTPException(status.HTTP_409_CONFLICT, detail="Data quality failing")

    monkeypatch.setattr("app.routers.dependency_approval._guard_data_quality", _fail_guard)

    response = client.put(
        f"/dependency-approvals/{approval.id}",
        json={"decision": "approved"},
    )

    assert response.status_code == 409
    assert response.json()["detail"] == "Data quality failing"

    # Ensure decision not updated
    assert approval.decision == "pending"


def test_update_does_not_invoke_gate_when_decision_unchanged(
    client: TestClient,
    monkeypatch: pytest.MonkeyPatch,
    seeded_approval,
):
    approval, _approver, _system, _dependency, _data_object = seeded_approval

    def _fail_guard(_keys: Iterable[str]) -> None:
        from fastapi import HTTPException
        from starlette import status

        raise HTTPException(status.HTTP_409_CONFLICT, detail="Should not be called")

    monkeypatch.setattr("app.routers.dependency_approval._guard_data_quality", _fail_guard)

    response = client.put(
        f"/dependency-approvals/{approval.id}",
        json={"comments": "adding context"},
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["decision"] == "pending"
    assert payload["comments"] == "adding context"


def test_update_collects_project_keys_and_allows_when_gate_passes(
    client: TestClient,
    monkeypatch: pytest.MonkeyPatch,
    seeded_approval,
):
    approval, _approver, system, _dependency, data_object = seeded_approval

    captured = {}

    def _capture_guard(keys: Iterable[str]) -> None:
        captured["keys"] = set(keys)

    monkeypatch.setattr("app.routers.dependency_approval._guard_data_quality", _capture_guard)

    response = client.put(
        f"/dependency-approvals/{approval.id}",
        json={"decision": "approved"},
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["decision"] == "approved"
    expected_key = build_project_key(data_object.workspace_id)
    assert captured["keys"] == {expected_key}


def test_create_invokes_gate_for_immediate_approval(
    client: TestClient,
    monkeypatch: pytest.MonkeyPatch,
    db_session,
):
    approver, system, dependency, data_object = _seed_table_dependency(db_session)

    captured = {}

    def _capture_guard(keys: Iterable[str]) -> None:
        captured["keys"] = set(keys)

    monkeypatch.setattr("app.routers.dependency_approval._guard_data_quality", _capture_guard)

    response = client.post(
        "/dependency-approvals",
        json={
            "table_dependency_id": str(dependency.id),
            "approver_id": str(approver.id),
            "decision": "approved",
        },
    )

    assert response.status_code == 201
    payload = response.json()
    assert payload["decision"] == "approved"
    expected_key = build_project_key(data_object.workspace_id)
    assert captured["keys"] == {expected_key}