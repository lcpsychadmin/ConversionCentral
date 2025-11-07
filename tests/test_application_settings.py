from fastapi.testclient import TestClient

LOGO_DATA_URL = "data:image/png;base64,iVBORw0KGgo="


def test_company_settings_defaults(client: TestClient) -> None:
    response = client.get("/application-settings/company")
    assert response.status_code == 200
    assert response.json() == {"site_title": None, "logo_data_url": None}


def test_update_company_settings(client: TestClient) -> None:
    payload = {
        "site_title": "Acme Portal",
        "logo_data_url": LOGO_DATA_URL,
    }
    response = client.put("/application-settings/company", json=payload)
    assert response.status_code == 200
    body = response.json()
    assert body["site_title"] == "Acme Portal"
    assert body["logo_data_url"] == LOGO_DATA_URL

    response = client.get("/application-settings/company")
    assert response.status_code == 200
    assert response.json() == body


def test_clear_company_logo(client: TestClient) -> None:
    # Seed initial values
    client.put(
        "/application-settings/company",
        json={"site_title": "Acme Portal", "logo_data_url": LOGO_DATA_URL},
    )

    # Clear logo while keeping title intact
    response = client.put(
        "/application-settings/company",
        json={"logo_data_url": None},
    )
    assert response.status_code == 200
    body = response.json()
    assert body["site_title"] == "Acme Portal"
    assert body["logo_data_url"] is None

    # Trimmed titles should be persisted without surrounding whitespace
    response = client.put(
        "/application-settings/company",
        json={"site_title": "  Example Corp  "},
    )
    assert response.status_code == 200
    assert response.json()["site_title"] == "Example Corp"