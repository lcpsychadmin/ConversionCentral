from app.services.databricks_sql import DatabricksConnectionParams, build_sqlalchemy_url


def test_build_sqlalchemy_url_uses_databricks_dialect():
    params = DatabricksConnectionParams(
        workspace_host="adb-12345.1.azuredatabricks.net",
        http_path="/sql/1.0/warehouses/abc",
        access_token="secret",
        catalog="sample_catalog",
        schema_name="sample_schema",
        constructed_schema="constructed_workspace",
    )

    url = build_sqlalchemy_url(params)

    assert url.drivername == "databricks"
    assert url.host == "adb-12345.1.azuredatabricks.net"
    assert url.port == 443
    assert url.query == {
        "http_path": "/sql/1.0/warehouses/abc",
        "catalog": "sample_catalog",
        "schema": "sample_schema",
    }
