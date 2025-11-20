"""Fetch DataKitchen TestGen test type templates.

This utility downloads every YAML template from the
DataKitchen/dataops-testgen repository under
`testgen/template/dbsetup_test_types/` and stores them locally for further
analysis. It also generates a compact index with the key metadata we care
about so the application can consume a normalized view without parsing every
YAML at runtime.
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

import httpx
import yaml

# GitHub endpoints to enumerate and download the template files.
LISTING_URL = (
    "https://api.github.com/repos/DataKitchen/dataops-testgen/contents/"
    "testgen/template/dbsetup_test_types?ref=main"
)
USER_AGENT = "ConversionCentral-TestTypeSync"

# Destination folders inside the repository.
OUTPUT_ROOT = Path(__file__).resolve().parent.parent / "docs" / "reference" / "datakitchen"
TEMPLATE_DIR = OUTPUT_ROOT / "dbsetup_test_types"
INDEX_PATH = OUTPUT_ROOT / "test_types_index.json"


def ensure_output_dirs() -> None:
    """Create the destination directories if they do not already exist."""
    TEMPLATE_DIR.mkdir(parents=True, exist_ok=True)
    OUTPUT_ROOT.mkdir(parents=True, exist_ok=True)


def fetch_listing(client: httpx.Client) -> list[dict]:
    """Return the GitHub directory listing for the template files."""
    response = client.get(LISTING_URL)
    response.raise_for_status()
    entries = response.json()
    return [entry for entry in entries if entry.get("type") == "file"]


def download_templates(client: httpx.Client, entries: list[dict]) -> list[Path]:
    """Download each template file and persist it locally."""
    saved_paths: list[Path] = []
    for entry in entries:
        download_url = entry.get("download_url")
        if not download_url:
            continue
        target_path = TEMPLATE_DIR / entry["name"]
        response = client.get(download_url)
        response.raise_for_status()
        target_path.write_text(response.text, encoding="utf-8")
        saved_paths.append(target_path)
    return saved_paths


def build_index(saved_paths: list[Path]) -> None:
    """Parse the YAML templates and write a normalized summary index."""
    index: dict[str, dict] = {}
    for file_path in saved_paths:
        data = yaml.safe_load(file_path.read_text(encoding="utf-8")) or {}
        test_type = data.get("test_types", {})
        identifier = test_type.get("test_type")
        if not identifier:
            continue
        index[identifier] = {
            "id": test_type.get("id"),
            "test_name_short": test_type.get("test_name_short"),
            "test_name_long": test_type.get("test_name_long"),
            "dq_dimension": test_type.get("dq_dimension"),
            "run_type": test_type.get("run_type"),
            "default_parm_columns": test_type.get("default_parm_columns"),
            "default_severity": test_type.get("default_severity"),
            "usage_notes": test_type.get("usage_notes"),
            "source_file": file_path.name,
        }
    INDEX_PATH.write_text(
        json.dumps(index, indent=2, sort_keys=True),
        encoding="utf-8",
    )


def main() -> int:
    ensure_output_dirs()
    with httpx.Client(headers={"User-Agent": USER_AGENT}, timeout=30.0) as client:
        entries = fetch_listing(client)
        if not entries:
            print("No templates found in the upstream repository.")
            return 1
        saved_paths = download_templates(client, entries)
    build_index(saved_paths)
    print(f"Downloaded {len(saved_paths)} templates to {TEMPLATE_DIR.relative_to(Path.cwd())}")
    print(f"Generated index file at {INDEX_PATH.relative_to(Path.cwd())}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
