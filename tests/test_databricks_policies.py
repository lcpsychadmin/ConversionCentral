from __future__ import annotations

from datetime import datetime, timezone

import pytest
from sqlalchemy import select

import app.routers.databricks_settings as databricks_router
import app.services.databricks_policies as policy_module

from app.models import DatabricksClusterPolicy, DatabricksSqlSetting
from app.services.databricks_policies import DatabricksPolicySyncError, sync_cluster_policies


class StubWorkspaceClient:
    def __init__(self, payloads):
        self._payloads = payloads

    def list_cluster_policies(self):
        return self._payloads


def _create_setting(db_session):
    setting = DatabricksSqlSetting(
        display_name="Primary",
        workspace_host="adb-123.azuredatabricks.net",
        http_path="/sql/1.0/warehouses/abc",
        access_token="token",
        is_active=True,
    )
    db_session.add(setting)
    db_session.flush()
    return setting


def test_sync_cluster_policies_upserts_rows(db_session):
    setting = _create_setting(db_session)
    existing = DatabricksClusterPolicy(
        setting_id=setting.id,
        policy_id="obsolete",
        name="Old",
        is_active=True,
    )
    db_session.add(existing)
    db_session.flush()

    payloads = [
        {"policy_id": "policy-1", "name": "Policy One", "definition": '{"foo": "bar"}', "description": "Primary policy"},
        {"policy_id": "policy-2", "name": "Policy Two"},
    ]
    stub_client = StubWorkspaceClient(payloads)

    records = sync_cluster_policies(db_session, client=stub_client)

    assert [record.policy_id for record in records] == ["policy-1", "policy-2"]
    stored = (
        db_session.execute(
            select(DatabricksClusterPolicy).where(DatabricksClusterPolicy.policy_id == "policy-1")
        )
        .scalars()
        .one()
    )
    assert stored.definition == {"foo": "bar"}
    assert stored.description == "Primary policy"
    obsolete = (
        db_session.execute(
            select(DatabricksClusterPolicy).where(DatabricksClusterPolicy.policy_id == "obsolete")
        )
        .scalars()
        .one()
    )
    assert obsolete.is_active is False


def test_sync_cluster_policies_requires_setting(db_session):
    stub_client = StubWorkspaceClient([])
    with pytest.raises(DatabricksPolicySyncError):
        sync_cluster_policies(db_session, client=stub_client)


def test_list_cluster_policies_endpoint_returns_active_only(client, db_session):
    setting = _create_setting(db_session)
    active = DatabricksClusterPolicy(
        setting_id=setting.id,
        policy_id="policy-active",
        name="Active Policy",
        is_active=True,
    )
    inactive = DatabricksClusterPolicy(
        setting_id=setting.id,
        policy_id="policy-inactive",
        name="Inactive Policy",
        is_active=False,
    )
    db_session.add_all([active, inactive])
    db_session.commit()

    response = client.get("/databricks/settings/policies")
    assert response.status_code == 200
    payload = response.json()
    assert len(payload) == 1
    assert payload[0]["policy_id"] == "policy-active"


def test_sync_cluster_policies_endpoint_returns_serialized_rows(client, db_session, monkeypatch):
    setting = _create_setting(db_session)

    def fake_sync(session, *, settings=None, client=None):
        policy = DatabricksClusterPolicy(
            setting_id=setting.id,
            policy_id="policy-synced",
            name="Synced Policy",
            description="Refreshed from Databricks",
            definition={"spark_version": "13.x"},
            synced_at=datetime.now(timezone.utc),
            is_active=True,
        )
        session.add(policy)
        session.flush()
        return [policy]

    monkeypatch.setattr(policy_module, "sync_cluster_policies", fake_sync)
    monkeypatch.setattr(databricks_router, "sync_cluster_policies", fake_sync)

    response = client.post("/databricks/settings/policies/sync")
    assert response.status_code == 200
    payload = response.json()
    assert len(payload) == 1
    assert payload[0]["name"] == "Synced Policy"
    assert payload[0]["definition"] == {"spark_version": "13.x"}
