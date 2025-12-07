from types import SimpleNamespace

from app.services.constructed_schema_resolver import resolve_constructed_schema, sanitize_schema_name


def test_resolver_prefers_table_schema():
    table = SimpleNamespace(schema_name="custom_schema")
    system = SimpleNamespace(constructed_schema_name="system_schema", physical_name="System One")

    result = resolve_constructed_schema(table=table, system=system)

    assert result == "custom_schema"


def test_resolver_uses_system_name_when_missing_table_schema():
    table = SimpleNamespace(schema_name=None)
    system = SimpleNamespace(
        constructed_schema_name=None,
        name="Orders Platform",
        physical_name="orders_physical",
    )

    result = resolve_constructed_schema(table=table, system=system)

    assert result == "orders_platform"


def test_resolver_falls_back_to_physical_name_when_display_name_missing():
    table = SimpleNamespace(schema_name=None)
    system = SimpleNamespace(constructed_schema_name=None, name=None, physical_name="Orders DB")

    result = resolve_constructed_schema(table=table, system=system)

    assert result == "orders_db"


def test_resolver_falls_back_to_default_schema():
    table = SimpleNamespace(schema_name=None)
    system = SimpleNamespace(constructed_schema_name=None, physical_name=None)

    result = resolve_constructed_schema(table=table, system=system)

    assert result == "constructed_data"


def test_sanitize_schema_name_normalizes_invalid_characters():
    assert sanitize_schema_name(" 123-Orders! ") == "_123_orders"
