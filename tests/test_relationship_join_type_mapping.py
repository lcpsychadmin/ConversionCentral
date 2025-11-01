from datetime import datetime
from uuid import uuid4

import pytest

from app.schemas.entities import (
    DataDefinitionRelationshipCreate,
    DataDefinitionRelationshipRead,
    DataDefinitionRelationshipUpdate,
    DataDefinitionJoinType,
)


def _read_payload(join_value: str):
    now = datetime.utcnow()
    return {
        "created_at": now,
        "updated_at": now,
        "id": uuid4(),
        "data_definition_id": uuid4(),
        "primary_table_id": uuid4(),
        "primary_field_id": uuid4(),
        "foreign_table_id": uuid4(),
        "foreign_field_id": uuid4(),
        "join_type": join_value,
        "primary_field": {
            "created_at": now,
            "updated_at": now,
            "id": uuid4(),
            "definition_table_id": uuid4(),
            "field_id": uuid4(),
            "display_order": 0,
            "field": {
                "id": uuid4(),
                "created_at": now,
                "updated_at": now,
                "table_id": uuid4(),
                "name": "foo",
                "field_type": "uuid",
                "active": True,
                "suppressed_field": False,
            },
        },
        "foreign_field": {
            "created_at": now,
            "updated_at": now,
            "id": uuid4(),
            "definition_table_id": uuid4(),
            "field_id": uuid4(),
            "display_order": 1,
            "field": {
                "id": uuid4(),
                "created_at": now,
                "updated_at": now,
                "table_id": uuid4(),
                "name": "bar",
                "field_type": "uuid",
                "active": True,
                "suppressed_field": False,
            },
        },
    }


def test_create_accepts_legacy_cardinality_and_translates_to_join_type():
    payload = {
        "primary_field_id": uuid4(),
        "foreign_field_id": uuid4(),
        "relationship_type": "one_to_many",
    }

    result = DataDefinitionRelationshipCreate(**payload)

    assert result.join_type is DataDefinitionJoinType.LEFT


def test_create_accepts_many_to_many_and_maps_to_inner_join():
    payload = {
        "primary_field_id": uuid4(),
        "foreign_field_id": uuid4(),
        "relationship_type": "many_to_many",
    }

    result = DataDefinitionRelationshipCreate(**payload)

    assert result.join_type is DataDefinitionJoinType.INNER


def test_create_accepts_direct_join_type_string():
    payload = {
        "primary_field_id": uuid4(),
        "foreign_field_id": uuid4(),
        "relationship_type": "left",
    }

    result = DataDefinitionRelationshipCreate(**payload)

    assert result.join_type is DataDefinitionJoinType.LEFT


def test_update_accepts_cardinality_and_join_type():
    update_payload = DataDefinitionRelationshipUpdate(relationship_type="many_to_one")
    assert update_payload.join_type is DataDefinitionJoinType.RIGHT

    update_payload = DataDefinitionRelationshipUpdate(relationship_type="right")
    assert update_payload.join_type is DataDefinitionJoinType.RIGHT


def test_read_populates_legacy_relationship_type_from_join_type():
    read_model = DataDefinitionRelationshipRead(**_read_payload("left"))
    assert read_model.relationship_type == "one_to_many"

    read_model = DataDefinitionRelationshipRead(**_read_payload("right"))
    assert read_model.relationship_type == "many_to_one"

    read_model = DataDefinitionRelationshipRead(**_read_payload("inner"))
    assert read_model.relationship_type == "one_to_one"


@pytest.mark.parametrize("invalid", ["outer", "", None])
def test_invalid_values_raise_validation_error(invalid):
    payload = {
        "primary_field_id": uuid4(),
        "foreign_field_id": uuid4(),
        "relationship_type": invalid,
    }

    with pytest.raises(Exception):
        DataDefinitionRelationshipCreate(**payload)
