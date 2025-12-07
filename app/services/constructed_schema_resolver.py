from __future__ import annotations

import re
from typing import Iterable

from app.models import ConstructedTable, System

DEFAULT_CONSTRUCTED_SCHEMA = "constructed_data"
_MAX_SCHEMA_LENGTH = 120
_SCHEMA_VALID_CHARS = re.compile(r"[^A-Za-z0-9_]")
_MULTIPLE_UNDERSCORES = re.compile(r"_+")


class ConstructedSchemaResolutionError(RuntimeError):
    """Raised when a constructed table schema cannot be determined."""


def sanitize_schema_name(value: str | None) -> str | None:
    """Return a normalized schema identifier or None when invalid/empty."""

    if not value:
        return None

    normalized = value.strip()
    if not normalized:
        return None

    normalized = _SCHEMA_VALID_CHARS.sub("_", normalized)
    normalized = _MULTIPLE_UNDERSCORES.sub("_", normalized)
    normalized = normalized.strip("_")

    if not normalized:
        return None

    if normalized[0].isdigit():
        normalized = f"_{normalized}"

    if len(normalized) > _MAX_SCHEMA_LENGTH:
        normalized = normalized[:_MAX_SCHEMA_LENGTH].rstrip("_") or normalized[:_MAX_SCHEMA_LENGTH]

    return normalized.lower()


def _iter_candidates(
    *,
    overrides: Iterable[str | None] | None = None,
    table: ConstructedTable | None = None,
    system: System | None = None,
    fallback_schema: str | None = None,
) -> Iterable[str | None]:
    if overrides:
        for candidate in overrides:
            yield candidate

    if table is not None:
        yield getattr(table, "schema_name", None)

    if system is not None:
        yield getattr(system, "constructed_schema_name", None)
        yield getattr(system, "name", None)
        yield getattr(system, "physical_name", None)

    yield fallback_schema
    yield DEFAULT_CONSTRUCTED_SCHEMA


def resolve_constructed_schema(
    *,
    overrides: Iterable[str | None] | None = None,
    table: ConstructedTable | None = None,
    system: System | None = None,
    fallback_schema: str | None = None,
) -> str:
    """Determine the schema to use for a constructed table."""

    for candidate in _iter_candidates(
        overrides=overrides,
        table=table,
        system=system,
        fallback_schema=fallback_schema,
    ):
        normalized = sanitize_schema_name(candidate)
        if normalized:
            return normalized

    raise ConstructedSchemaResolutionError("Unable to derive constructed table schema")


def validate_schema_input(value: str | None) -> str | None:
    """Validate user-provided schema names before persisting them."""

    if value is None:
        return None

    normalized = sanitize_schema_name(value)
    if not normalized:
        raise ValueError("Schema name must contain alphanumeric characters")

    if normalized != value:
        raise ValueError("Schema name contains invalid characters")

    return value
