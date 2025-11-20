from __future__ import annotations

import argparse
import os
import subprocess
from pathlib import Path
from typing import Iterable, Optional

SUPPORTED_ADAPTERS = {
    "duckdb": "dbt-duckdb==1.7.2",
    "databricks": "dbt-databricks==1.7.2",
}

DEFAULT_DBT_VERSION = "dbt-core==1.7.6"


def _parse_args(argv: Optional[Iterable[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Configure a dbt environment for the Canvas exporter.")
    parser.add_argument("--adapter", choices=SUPPORTED_ADAPTERS.keys(), required=True)
    parser.add_argument("--profiles-dir", type=Path, default=Path.home() / ".dbt")
    parser.add_argument("--host")
    parser.add_argument("--http-path")
    parser.add_argument("--token")
    parser.add_argument("--catalog", default="main")
    parser.add_argument("--schema", default="analytics")
    parser.add_argument("--profile-name", default="default")
    return parser.parse_args(argv)


def _run(cmd: list[str]) -> None:
    subprocess.run(cmd, check=True)


def _install_packages(adapter: str) -> None:
    packages = [DEFAULT_DBT_VERSION, SUPPORTED_ADAPTERS[adapter]]
    for package in packages:
        _run(["python", "-m", "pip", "install", package])


def _write_profiles(args: argparse.Namespace) -> Path:
    profiles_dir = args.profiles_dir
    profiles_dir.mkdir(parents=True, exist_ok=True)

    if args.adapter == "duckdb":
        profile_content = f"""
{args.profile_name}:
  target: dev
  outputs:
    dev:
      type: duckdb
      path: {profiles_dir}/canvas-exporter.duckdb
"""
    else:
        missing = [
            name
            for name in ("host", "http_path", "token")
            if not getattr(args, name)
        ]
        if missing:
            raise ValueError(
                "Missing required Databricks parameters: " + ", ".join(missing)
            )
        profile_content = f"""
{args.profile_name}:
  target: dev
  outputs:
    dev:
      type: databricks
      catalog: {args.catalog}
      schema: {args.schema}
      host: {args.host}
      http_path: {args.http_path}
      token: {args.token}
"""

    profiles_path = profiles_dir / "profiles.yml"
    profiles_path.write_text(profile_content.strip() + "\n", encoding="utf-8")
    return profiles_path


def main(argv: Optional[Iterable[str]] = None) -> int:
    try:
        args = _parse_args(argv)
        _install_packages(args.adapter)
        profile_path = _write_profiles(args)
    except subprocess.CalledProcessError as exc:
        print(f"Command failed: {exc}", file=os.sys.stderr)
        return exc.returncode or 1
    except ValueError as exc:
        print(str(exc), file=os.sys.stderr)
        return 1

    print("dbt environment ready.")
    print(f"Profiles directory: {profile_path.parent}")
    print(f"Profile name: {args.profile_name}")
    return 0


if __name__ == "__main__":  # pragma: no cover - CLI entry point
    raise SystemExit(main())
