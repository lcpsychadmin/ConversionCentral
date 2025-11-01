from types import SimpleNamespace

from sqlalchemy import create_engine, text

from app.services.ingestion_storage import DatabricksIngestionStorage


def _build_table(fields):
    return SimpleNamespace(
        schema_name=None,
        physical_name="ingested_customers",
        name="Customers",
        fields=fields,
    )


def _field(name, field_type, length=None, decimal_places=None, system_required=False):
    return SimpleNamespace(
        name=name,
        field_type=field_type,
        field_length=length,
        decimal_places=decimal_places,
        system_required=system_required,
    )


def test_load_rows_creates_table_and_inserts_data():
    engine = create_engine("sqlite:///:memory:", future=True)
    storage = DatabricksIngestionStorage(engine=engine)

    fields = [
        _field("Customer ID", "int", system_required=True),
        _field("Full Name", "nvarchar", length=120),
        _field("Balance", "decimal", length=10, decimal_places=2),
    ]
    table = _build_table(fields)

    rows = [
        {"Customer ID": 1, "Full Name": "Alice", "Balance": 125.50},
        {"Customer ID": 2, "Full Name": "Bob", "Balance": 99.99},
    ]

    inserted = storage.load_rows(table, rows, replace=True)
    assert inserted == 2

    with engine.connect() as connection:
        result = connection.execute(text("SELECT COUNT(*) FROM ingested_customers"))
        assert result.scalar_one() == 2


def test_replace_uses_delete_on_non_sqlserver():
    engine = create_engine("sqlite:///:memory:", future=True)
    storage = DatabricksIngestionStorage(engine=engine)

    fields = [_field("Identifier", "int", system_required=True)]
    table = _build_table(fields)

    storage.load_rows(table, [{"Identifier": 1}], replace=True)
    storage.load_rows(table, [{"Identifier": 2}], replace=True)

    with engine.connect() as connection:
        result = connection.execute(text("SELECT COUNT(*) FROM ingested_customers"))
        assert result.scalar_one() == 1
        value = connection.execute(text("SELECT Identifier FROM ingested_customers"))
        assert value.scalar_one() == 2
