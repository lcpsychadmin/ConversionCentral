from types import SimpleNamespace

from app.services.ingestion_support import build_ingestion_schema_name, build_ingestion_table_name


def test_build_ingestion_schema_name_prefers_system_name() -> None:
    connection = SimpleNamespace(system=SimpleNamespace(name="CRM Prod"), id="12345678")
    assert build_ingestion_schema_name(connection) == "crm_prod"


def test_build_ingestion_schema_name_handles_missing_metadata() -> None:
    connection = SimpleNamespace(system=None, id=None)
    assert build_ingestion_schema_name(connection) is None


def test_build_ingestion_table_name_prefixes_schema() -> None:
    connection = SimpleNamespace()
    selection = SimpleNamespace(schema_name="Sales", table_name="Opportunities", table_type="base")
    assert build_ingestion_table_name(connection, selection) == "sales_opportunities"


def test_build_ingestion_table_name_skips_constructed_prefix() -> None:
    connection = SimpleNamespace()
    selection = SimpleNamespace(schema_name="Reporting", table_name="Accounts", table_type="constructed")
    assert build_ingestion_table_name(connection, selection) == "accounts"
