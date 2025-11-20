from __future__ import annotations

import app.routers.databricks_settings as databricks_router
from app.models import DatabricksSqlSetting


def _create_setting(db_session):
    setting = DatabricksSqlSetting(
        display_name="Primary",
        workspace_host="adb-123.azuredatabricks.net",
        http_path="/sql/1.0/warehouses/abc",
        access_token="token",
        catalog="workspace",
        schema_name="default",
        is_active=True,
    )
    db_session.add(setting)
    db_session.commit()
    db_session.refresh(setting)
    return setting


def test_update_skips_connection_for_metadata_fields(client, db_session, monkeypatch):
    setting = _create_setting(db_session)

    called = {"status": False}

    def fake_connection_test(*args, **kwargs):
        called["status"] = True
        return 0.0, "databricks://token@adb-123"

    monkeypatch.setattr(databricks_router, "test_databricks_connection", fake_connection_test)
    monkeypatch.setattr(databricks_router, "ensure_databricks_connection", lambda: None)

    response = client.put(
        f"/databricks/settings/{setting.id}",
        json={"profiling_notebook_path": "/Repos/Example/notebook"},
    )
    assert response.status_code == 200
    assert called["status"] is False
    payload = response.json()
    assert payload["profiling_notebook_path"] == "/Repos/Example/notebook"


def test_update_validates_connection_when_core_fields_change(client, db_session, monkeypatch):
    setting = _create_setting(db_session)

    called = {"status": False}

    def fake_connection_test(*args, **kwargs):
        called["status"] = True
        return 0.0, "databricks://token@adb-123"

    monkeypatch.setattr(databricks_router, "test_databricks_connection", fake_connection_test)
    monkeypatch.setattr(databricks_router, "ensure_databricks_connection", lambda: None)

    response = client.put(
        f"/databricks/settings/{setting.id}",
        json={"workspace_host": "adb-456.azuredatabricks.net"},
    )
    assert response.status_code == 200
    assert called["status"] is True
    payload = response.json()
    assert payload["workspace_host"] == "adb-456.azuredatabricks.net"
