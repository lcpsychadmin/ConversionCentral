from sqlalchemy.engine import URL

from app.schemas import SystemConnectionType
from app.services.connection_resolver import resolve_sqlalchemy_url, UnsupportedConnectionError


def test_resolve_postgres_jdbc_url():
    url = resolve_sqlalchemy_url(
        SystemConnectionType.JDBC,
        "jdbc:postgresql://analytics.example.com:5432/warehouse",
    )
    assert isinstance(url, URL)
    assert url.drivername == "postgresql+psycopg"
    assert url.host == "analytics.example.com"
    assert url.database == "warehouse"


def test_resolve_sqlserver_jdbc_url_adds_defaults():
    url = resolve_sqlalchemy_url(
        SystemConnectionType.JDBC,
        "jdbc:sqlserver://sql.example.com:1433/datahub",
    )
    assert url.drivername == "mssql+pyodbc"
    assert url.query.get("TrustServerCertificate") == "yes"
    assert "driver" in url.query


def test_unsupported_dialect_raises():
    try:
        resolve_sqlalchemy_url(SystemConnectionType.JDBC, "jdbc:mysql://db/app")
    except UnsupportedConnectionError as exc:
        assert "Unsupported JDBC dialect" in str(exc)
    else:
        raise AssertionError("UnsupportedConnectionError was not raised")
