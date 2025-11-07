from types import SimpleNamespace

from app.models import ConstructedField, ConstructedTable
from app.services.data_construction_sync import _sync_validation_rules


def _create_constructed_table_with_field(db_session, name: str, data_type: str) -> tuple[ConstructedTable, ConstructedField]:
    safe_name = name.lower().replace(" ", "_")
    table = ConstructedTable(name=f"constructed_{safe_name}")
    db_session.add(table)
    db_session.flush()

    field = ConstructedField(
        constructed_table_id=table.id,
        name=name,
        data_type=data_type,
        is_nullable=False,
        display_order=0,
    )
    db_session.add(field)
    db_session.flush()

    if field not in table.fields:
        table.fields.append(field)

    return table, field


def test_sync_validation_rules_creates_required_unique_and_length_rules(db_session):
    table, field = _create_constructed_table_with_field(db_session, "CustomerId", "string")

    source_field = SimpleNamespace(
        name="CustomerId",
        system_required=True,
        field_length=10,
        decimal_places=None,
    )
    definition_field = SimpleNamespace(field=source_field, is_unique=True)
    definition_table = SimpleNamespace(fields=[definition_field])

    _sync_validation_rules(definition_table, table, db_session)
    db_session.flush()

    assert len(table.validation_rules) == 3

    rules_by_type = {}
    for rule in table.validation_rules:
        rules_by_type.setdefault(rule.rule_type, []).append(rule)

    assert "required" in rules_by_type
    assert "unique" in rules_by_type
    assert "pattern" in rules_by_type

    length_rule = rules_by_type["pattern"][0]
    assert length_rule.configuration == {"fieldName": "CustomerId", "pattern": "^.{0,10}$"}
    assert length_rule.is_system_generated is True


def test_sync_validation_rules_skips_audit_fields(db_session):
    table, _ = _create_constructed_table_with_field(db_session, "Created Date", "datetime")

    source_field = SimpleNamespace(
        name="Created Date",
        system_required=True,
        field_length=25,
        decimal_places=None,
    )
    definition_field = SimpleNamespace(field=source_field, is_unique=False)
    definition_table = SimpleNamespace(fields=[definition_field])

    _sync_validation_rules(definition_table, table, db_session)
    db_session.flush()

    assert table.validation_rules == []
