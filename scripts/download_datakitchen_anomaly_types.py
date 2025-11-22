"""Fetch DataKitchen TestGen profile anomaly templates.

This helper mirrors the TestGen metadata that defines canonical profiling
anomalies (name, description, likelihood, etc.). We download every YAML file
from the upstream repository and persist them locally under
`docs/reference/datakitchen/dbsetup_anomaly_types/`. While downloading we also
build a compact JSON index that captures the subset of fields the application
and migrations need, including derived attributes such as the default category
(PII vs hygiene) and default severity.
"""
from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import Any

import httpx
import yaml

LISTING_URL = (
    "https://api.github.com/repos/DataKitchen/dataops-testgen/contents/"
    "testgen/template/dbsetup_anomaly_types?ref=main"
)
USER_AGENT = "ConversionCentral-AnomalyTypeSync"

OUTPUT_ROOT = Path(__file__).resolve().parent.parent / "docs" / "reference" / "datakitchen"
TEMPLATE_DIR = OUTPUT_ROOT / "dbsetup_anomaly_types"
INDEX_PATH = OUTPUT_ROOT / "anomaly_types_index.json"

IDENTIFIER_PII_KEYWORDS = {
    "pii",
    "email",
    "zip",
    "address",
    "name",
    "state",
    "ssn",
    "phone",
}

LABEL_PII_KEYWORDS = {
    "pii",
    "email",
    "zip",
    "address",
    "state",
    "ssn",
    "phone",
}


def ensure_output_dirs() -> None:
    TEMPLATE_DIR.mkdir(parents=True, exist_ok=True)
    OUTPUT_ROOT.mkdir(parents=True, exist_ok=True)


def fetch_listing(client: httpx.Client) -> list[dict[str, Any]]:
    response = client.get(LISTING_URL)
    response.raise_for_status()
    entries = response.json()
    return [entry for entry in entries if entry.get("type") == "file"]


def download_templates(client: httpx.Client, entries: list[dict[str, Any]]) -> list[Path]:
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


def derive_category(record: dict[str, Any]) -> str:
    identifier = str(record.get("anomaly_type", "")).lower()
    label = str(record.get("anomaly_name", "")).lower()
    if any(keyword in identifier for keyword in IDENTIFIER_PII_KEYWORDS):
        return "pii"
    if any(keyword in label for keyword in LABEL_PII_KEYWORDS):
        return "pii"
    return "hygiene"


def derive_severity(record: dict[str, Any]) -> str:
    risk_raw = record.get("dq_score_risk_factor")
    severity: str | None = None
    if isinstance(risk_raw, (int, float)):
        numeric_risk = float(risk_raw)
    else:
        try:
            numeric_risk = float(str(risk_raw))
        except (TypeError, ValueError):
            numeric_risk = None
    if numeric_risk is not None:
        if numeric_risk >= 0.9:
            severity = "High"
        elif numeric_risk >= 0.6:
            severity = "Medium"
        else:
            severity = "Low"
    else:
        likelihood = (record.get("issue_likelihood") or "").lower()
        if "definite" in likelihood or "certain" in likelihood or "pii" in likelihood:
            severity = "High"
        elif "likely" in likelihood:
            severity = "Medium"
        else:
            severity = "Low"
    return severity


def normalize_text(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    return " ".join(text.split())


def build_index(saved_paths: list[Path]) -> None:
    index: dict[str, dict[str, Any]] = {}
    for file_path in saved_paths:
        data = yaml.safe_load(file_path.read_text(encoding="utf-8")) or {}
        anomaly = data.get("profile_anomaly_types", {})
        identifier = anomaly.get("anomaly_type")
        if not identifier:
            continue
        index[identifier] = {
            "id": anomaly.get("id"),
            "name": anomaly.get("anomaly_name"),
            "data_object": anomaly.get("data_object"),
            "description": normalize_text(anomaly.get("anomaly_description")),
            "default_likelihood": anomaly.get("issue_likelihood"),
            "dq_dimension": anomaly.get("dq_dimension"),
            "dq_score_risk_factor": anomaly.get("dq_score_risk_factor"),
            "suggested_action": normalize_text(anomaly.get("suggested_action")),
            "category": derive_category(anomaly),
            "default_severity": derive_severity(anomaly),
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
            print("No anomaly templates found in the upstream repository.")
            return 1
        saved_paths = download_templates(client, entries)
    build_index(saved_paths)
    print(f"Downloaded {len(saved_paths)} templates to {TEMPLATE_DIR.relative_to(Path.cwd())}")
    print(f"Generated index file at {INDEX_PATH.relative_to(Path.cwd())}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
