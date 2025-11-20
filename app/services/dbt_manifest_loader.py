from __future__ import annotations

import json
from json import JSONDecodeError
from pathlib import Path
from typing import Any, Mapping, Union

from app.schemas.dbt_manifest import DbtManifest

ManifestInput = Union[str, bytes, Mapping[str, Any], Path]


def load_manifest_from_str(content: Union[str, bytes]) -> DbtManifest:
    """Parse a manifest JSON string into a ``DbtManifest`` model."""

    if isinstance(content, bytes):
        payload = json.loads(content.decode("utf-8"))
    else:
        payload = json.loads(content)
    return DbtManifest.parse_obj(payload)


def load_manifest_from_mapping(payload: Mapping[str, Any]) -> DbtManifest:
    """Build a manifest model from an in-memory mapping."""

    return DbtManifest.parse_obj(payload)


def load_manifest_path(path: Union[str, Path]) -> DbtManifest:
    """Load a manifest JSON file from disk."""

    json_path = Path(path)
    content = json_path.read_text(encoding="utf-8")
    return load_manifest_from_str(content)


def load_manifest(manifest: ManifestInput) -> DbtManifest:
    """Generic manifest loader accepting JSON strings, bytes, mappings, or Paths."""

    if isinstance(manifest, Mapping):
        return load_manifest_from_mapping(manifest)
    if isinstance(manifest, (Path, str)):
        path = Path(manifest)
        if path.exists():
            return load_manifest_path(path)
    if isinstance(manifest, (str, bytes)):
        try:
            return load_manifest_from_str(manifest)
        except JSONDecodeError:
            if isinstance(manifest, str):
                path = Path(manifest)
                if path.exists():
                    return load_manifest_path(path)
            raise
    if isinstance(manifest, Path):
        return load_manifest_path(manifest)
    raise TypeError(f"Unsupported manifest input type: {type(manifest)!r}")
