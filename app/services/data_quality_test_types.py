"""Load and normalize DataKitchen TestGen test type templates."""
from __future__ import annotations

import csv
import json
import logging
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path
from typing import Any, Iterable

import yaml

logger = logging.getLogger(__name__)

PROJECT_ROOT = Path(__file__).resolve().parents[2]
CATALOG_ROOT = PROJECT_ROOT / "docs" / "reference" / "datakitchen"
INDEX_PATH = CATALOG_ROOT / "test_types_index.json"
TEMPLATE_DIR = CATALOG_ROOT / "dbsetup_test_types"


@dataclass(frozen=True)
class TestTypeParameter:
    name: str
    prompt: str | None
    help_text: str | None
    default_value: str | None


@dataclass(frozen=True)
class TestTypeMetadata:
    test_type: str
    rule_type: str
    identifier: str | None
    name_short: str | None
    name_long: str | None
    description: str | None
    dq_dimension: str | None
    run_type: str | None
    test_scope: str | None
    default_severity: str | None
    usage_notes: str | None
    column_prompt: str | None
    column_help: str | None
    sql_flavors: tuple[str, ...]
    parameters: tuple[TestTypeParameter, ...]
    source_file: str | None


def list_test_types() -> list[dict[str, Any]]:
    """Return the catalog of available test types as API-ready dictionaries."""

    return list(_iter_api_metadata())


def get_test_type(rule_type: str) -> dict[str, Any] | None:
    """Return a single test type metadata dictionary by its lowercase rule type."""

    normalized = _slugify(rule_type)
    for item in _iter_api_metadata():
        if _slugify(item.get("ruleType", "")) == normalized:
            return item
    return None


def clear_test_type_cache() -> None:
    """Clear cached metadata to reflect on-disk updates."""

    _load_test_type_metadata.cache_clear()


def _iter_api_metadata() -> Iterable[dict[str, Any]]:
    for meta in _load_test_type_metadata():
        yield _to_api_dict(meta)


@lru_cache(maxsize=1)
def _load_test_type_metadata() -> tuple[TestTypeMetadata, ...]:
    index_payload = _read_index()
    if not index_payload:
        return tuple()

    results: list[TestTypeMetadata] = []
    for key, entry in sorted(index_payload.items()):
        try:
            metadata = _build_metadata(key, entry)
        except Exception:  # noqa: BLE001 - defensive guard around third-party data
            logger.exception("Unable to parse test type definition for %s", key)
            continue
        if metadata is not None:
            results.append(metadata)
    return tuple(results)


def _read_index() -> dict[str, Any]:
    if not INDEX_PATH.exists():
        logger.warning("DataKitchen test type index not found at %s", INDEX_PATH)
        return {}
    try:
        return json.loads(INDEX_PATH.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        logger.warning("Unable to parse test type index %s: %s", INDEX_PATH, exc)
        return {}


def _build_metadata(default_key: str, summary: dict[str, Any]) -> TestTypeMetadata | None:
    source_file = summary.get("source_file")
    template_payload = _read_template(source_file) if source_file else {}
    test_type_payload = template_payload.get("test_types") or {}

    test_type_value = _to_str(test_type_payload.get("test_type") or default_key)
    if not test_type_value:
        return None

    rule_type = _slugify(test_type_value)
    parameters = _build_parameters(test_type_payload)

    metadata = TestTypeMetadata(
        test_type=test_type_value,
        rule_type=rule_type,
        identifier=_to_optional_str(summary.get("id") or test_type_payload.get("id")),
        name_short=_to_optional_str(summary.get("test_name_short") or test_type_payload.get("test_name_short")),
        name_long=_to_optional_str(summary.get("test_name_long") or test_type_payload.get("test_name_long")),
        description=_clean_text(test_type_payload.get("test_description")),
        dq_dimension=_to_optional_str(summary.get("dq_dimension") or test_type_payload.get("dq_dimension")),
        run_type=_to_optional_str(summary.get("run_type") or test_type_payload.get("run_type")),
        test_scope=_to_optional_str(test_type_payload.get("test_scope")),
        default_severity=_to_optional_str(summary.get("default_severity") or test_type_payload.get("default_severity")),
        usage_notes=_clean_text(summary.get("usage_notes") or test_type_payload.get("usage_notes")),
        column_prompt=_clean_text(test_type_payload.get("column_name_prompt")),
        column_help=_clean_text(test_type_payload.get("column_name_help")),
        sql_flavors=_collect_sql_flavors(template_payload),
        parameters=tuple(parameters),
        source_file=source_file,
    )
    return metadata


def _read_template(file_name: str | None) -> dict[str, Any]:
    if not file_name:
        return {}
    template_path = TEMPLATE_DIR / file_name
    if not template_path.exists():
        logger.warning("Missing DataKitchen template: %s", template_path)
        return {}
    try:
        return yaml.safe_load(template_path.read_text(encoding="utf-8")) or {}
    except yaml.YAMLError as exc:  # pragma: no cover - defensive guard for third-party data
        logger.warning("Failed to parse template %s: %s", template_path, exc)
        return {}


def _build_parameters(payload: dict[str, Any]) -> list[TestTypeParameter]:
    columns_raw = _to_str(payload.get("default_parm_columns"))
    if not columns_raw:
        return []

    names = _split_columns(columns_raw)
    prompts = _split_prompts(_to_str(payload.get("default_parm_prompts")), len(names))
    helps = _split_help(_to_str(payload.get("default_parm_help")), len(names))
    defaults = _split_defaults(_to_str(payload.get("default_parm_values")), len(names))

    parameters: list[TestTypeParameter] = []
    for index, name in enumerate(names):
        prompt = prompts[index] if index < len(prompts) else None
        help_text = helps[index] if index < len(helps) else None
        default_value = defaults[index] if index < len(defaults) else None
        parameters.append(TestTypeParameter(name=name, prompt=prompt, help_text=help_text, default_value=default_value))
    return parameters


def _split_columns(value: str) -> list[str]:
    sanitized = value.replace("\n", "").replace("\r", "")
    return [item.strip() for item in sanitized.split(",") if item.strip()]


def _split_prompts(value: str, expected: int) -> list[str]:
    if not value:
        return []
    lines = [item.strip() for item in value.replace("\r", "\n").splitlines() if item.strip()]
    if expected and len(lines) == expected:
        return lines

    csv_values = _parse_csv(value)
    if expected and len(csv_values) == expected:
        return csv_values
    return lines or csv_values


def _split_help(value: str, expected: int) -> list[str]:
    if not value:
        return []
    parts: list[str] = []
    for fragment in value.replace("\r", "\n").splitlines():
        for segment in fragment.split("|"):
            cleaned = segment.strip()
            if cleaned:
                parts.append(cleaned)
    if expected and len(parts) == expected:
        return parts

    fallback = [item.strip() for item in value.replace("\r", "\n").splitlines() if item.strip()]
    return parts or fallback


def _split_defaults(value: str, expected: int) -> list[str | None]:
    if not value:
        return []
    csv_values = _parse_csv(value)
    results: list[str | None] = []
    for item in csv_values:
        normalized = item.strip()
        if normalized.lower() in {"", "null", "none"}:
            results.append(None)
        else:
            results.append(normalized)
    if expected and len(results) != expected:
        return results[:expected]
    return results


def _parse_csv(value: str) -> list[str]:
    reader = csv.reader([value.replace("\n", " ")], skipinitialspace=True)
    for row in reader:
        return [item.strip() for item in row if item is not None and item.strip()]
    return []


def _collect_sql_flavors(payload: dict[str, Any]) -> tuple[str, ...]:
    flavors: set[str] = set()
    for key in ("cat_test_conditions", "target_data_lookups", "test_templates"):
        for item in payload.get(key) or []:
            flavor = _to_optional_str(item.get("sql_flavor"))
            if flavor:
                flavors.add(flavor)
    return tuple(sorted(flavors))


def _slugify(value: str) -> str:
    return _to_str(value).strip().lower().replace(" ", "_")


def _clean_text(value: Any) -> str | None:
    text = _to_optional_str(value)
    if not text:
        return None
    return " ".join(text.split())


def _to_optional_str(value: Any) -> str | None:
    text = _to_str(value)
    return text if text else None


def _to_str(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, (int, float)):
        return str(value)
    if isinstance(value, str):
        return value.strip()
    return str(value).strip()


def _to_api_dict(metadata: TestTypeMetadata) -> dict[str, Any]:
    return {
        "testType": metadata.test_type,
        "ruleType": metadata.rule_type,
        "id": metadata.identifier,
        "nameShort": metadata.name_short,
        "nameLong": metadata.name_long,
        "description": metadata.description,
        "dqDimension": metadata.dq_dimension,
        "runType": metadata.run_type,
        "testScope": metadata.test_scope,
        "defaultSeverity": metadata.default_severity,
        "usageNotes": metadata.usage_notes,
        "columnPrompt": metadata.column_prompt,
        "columnHelp": metadata.column_help,
        "sqlFlavors": list(metadata.sql_flavors),
        "parameters": [
            {
                "name": parameter.name,
                "prompt": parameter.prompt,
                "help": parameter.help_text,
                "defaultValue": parameter.default_value,
            }
            for parameter in metadata.parameters
        ],
        "sourceFile": metadata.source_file,
    }


__all__ = [
    "list_test_types",
    "get_test_type",
    "clear_test_type_cache",
]
