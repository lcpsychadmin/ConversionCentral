"""Reference metadata for Databricks SQL Warehouse data types."""

from __future__ import annotations

WAREHOUSE_DATA_TYPES: list[dict[str, object]] = [
    {"name": "BOOLEAN", "category": "boolean", "supports_decimal_places": False},
    {"name": "BYTE", "category": "numeric", "supports_decimal_places": False},
    {"name": "SHORT", "category": "numeric", "supports_decimal_places": False},
    {"name": "INT", "category": "numeric", "supports_decimal_places": False},
    {"name": "LONG", "category": "numeric", "supports_decimal_places": False},
    {"name": "FLOAT", "category": "numeric", "supports_decimal_places": True},
    {"name": "DOUBLE", "category": "numeric", "supports_decimal_places": True},
    {"name": "DECIMAL", "category": "numeric", "supports_decimal_places": True},
    {"name": "NUMERIC", "category": "numeric", "supports_decimal_places": True},
    {"name": "BIGDECIMAL", "category": "numeric", "supports_decimal_places": True},
    {"name": "STRING", "category": "string", "supports_decimal_places": False},
    {"name": "VARCHAR", "category": "string", "supports_decimal_places": False},
    {"name": "CHAR", "category": "string", "supports_decimal_places": False},
    {"name": "BINARY", "category": "binary", "supports_decimal_places": False},
    {"name": "DATE", "category": "temporal", "supports_decimal_places": False},
    {"name": "TIMESTAMP", "category": "temporal", "supports_decimal_places": False},
    {"name": "TIMESTAMP_NTZ", "category": "temporal", "supports_decimal_places": False},
    {"name": "TIMESTAMP_LTZ", "category": "temporal", "supports_decimal_places": False},
    {"name": "ARRAY", "category": "complex", "supports_decimal_places": False},
    {"name": "MAP", "category": "complex", "supports_decimal_places": False},
    {"name": "STRUCT", "category": "complex", "supports_decimal_places": False},
]

DECIMAL_COMPATIBLE_TYPES: set[str] = {
    entry["name"]
    for entry in WAREHOUSE_DATA_TYPES
    if entry.get("supports_decimal_places")
}
