from __future__ import annotations

import pytest

from app.models import ApplicationDatabaseSetting


def test_status_not_configured(client):
    response = client.get("/application-database/status")
    assert response.status_code == 200
    assert response.json() == {"configured": False, "setting": None, "admin_email": None}


@pytest.mark.parametrize("engine_value", ["default_postgres"])
def test_apply_default_database(client, db_session, monkeypatch, engine_value):
    monkeypatch.setattr("app.database.refresh_runtime_engine", lambda url: url)
    monkeypatch.setattr("app.services.application_database._run_migrations", lambda url: None)

    response = client.post(
        "/application-database/apply",
        json={"engine": engine_value, "display_name": "Primary"},
    )
    assert response.status_code == 201

    payload = response.json()
    assert payload["engine"] == engine_value
    assert "applied_at" in payload
    assert payload["connection_display"] is not None
    assert payload["display_name"] == "Primary"

    stored_settings = db_session.query(ApplicationDatabaseSetting).all()
    assert len(stored_settings) == 1
    assert stored_settings[0].engine == engine_value
    assert stored_settings[0].display_name == "Primary"
    assert stored_settings[0].connection_url is None


def test_admin_email_roundtrip(client):
    # Initial fetch returns null
    response = client.get("/application-settings/admin-email")
    assert response.status_code == 200
    assert response.json() == {"email": None}

    update_response = client.put(
        "/application-settings/admin-email",
        json={"email": "Admin@ConversionCentral.com"},
    )
    assert update_response.status_code == 200
    assert update_response.json() == {"email": "admin@conversioncentral.com"}

    # Subsequent read returns normalized email
    response = client.get("/application-settings/admin-email")
    assert response.status_code == 200
    assert response.json() == {"email": "admin@conversioncentral.com"}


def test_status_reflects_admin_email(client, monkeypatch):
    monkeypatch.setattr("app.database.refresh_runtime_engine", lambda url: url)
    monkeypatch.setattr("app.services.application_database._run_migrations", lambda url: None)

    apply_response = client.post(
        "/application-database/apply",
        json={"engine": "default_postgres", "display_name": "Primary"},
    )
    assert apply_response.status_code == 201

    client.put(
        "/application-settings/admin-email",
        json={"email": "owner@example.com"},
    )

    status_response = client.get("/application-database/status")
    assert status_response.status_code == 200
    body = status_response.json()
    assert body["configured"] is True
    assert body["admin_email"] == "owner@example.com"


def test_test_endpoint_requires_connection_for_custom(client):
    response = client.post(
        "/application-database/test",
        json={"engine": "custom_postgres"},
    )
    assert response.status_code == 422
    messages = [detail.get("msg", "") for detail in response.json().get("detail", [])]
    assert any("Connection details are required" in msg for msg in messages)
