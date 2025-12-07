"""tie data quality projects to workspaces

Revision ID: 20251212_000052
Revises: 20251211_000051
Create Date: 2025-12-07 00:00:00.000000
"""

from __future__ import annotations

import uuid

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql
from sqlalchemy.sql import text

# revision identifiers, used by Alembic.
revision = "20251212_000052"
down_revision = "20251211_000051"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.drop_constraint("dq_connections_project_key_fkey", "dq_connections", type_="foreignkey")
    op.drop_constraint("dq_test_suites_project_key_fkey", "dq_test_suites", type_="foreignkey")

    op.add_column(
        "dq_projects",
        sa.Column("workspace_id", postgresql.UUID(as_uuid=True), nullable=True),
    )

    conn = op.get_bind()
    workspace_lookup = _load_workspace_metadata(conn)
    default_workspace_id = _determine_default_workspace(conn, workspace_lookup)
    data_object_map = _load_data_object_workspaces(conn)

    projects = conn.execute(text("SELECT project_key FROM dq_projects ORDER BY project_key")).fetchall()
    canonical_keys: dict[str, str] = {}

    for (project_key,) in projects:
        workspace_id = _resolve_workspace_id(
            project_key,
            data_object_map=data_object_map,
            workspace_lookup=workspace_lookup,
            default_workspace_id=default_workspace_id,
        )
        if workspace_id is None:
            workspace_id = default_workspace_id

        normalized_workspace_id = _normalize_uuid(workspace_id)
        if normalized_workspace_id is None:
            normalized_workspace_id = default_workspace_id

        canonical_key = f"workspace:{normalized_workspace_id}"
        existing = canonical_keys.get(normalized_workspace_id)
        if existing is None:
            canonical_keys[normalized_workspace_id] = canonical_key
            _migrate_project(
                conn,
                old_key=project_key,
                canonical_key=canonical_key,
                workspace_id=normalized_workspace_id,
                workspace_lookup=workspace_lookup,
            )
        else:
            _repoint_project(
                conn,
                old_key=project_key,
                canonical_key=existing,
            )

    conn.execute(
        text("UPDATE dq_projects SET workspace_id = :workspace WHERE workspace_id IS NULL"),
        {"workspace": default_workspace_id},
    )

    op.alter_column("dq_projects", "workspace_id", existing_type=postgresql.UUID(as_uuid=True), nullable=False)
    op.create_unique_constraint("uq_dq_projects_workspace_id", "dq_projects", ["workspace_id"])
    op.create_foreign_key(
        "fk_dq_projects_workspace",
        "dq_projects",
        "workspaces",
        ["workspace_id"],
        ["id"],
        ondelete="CASCADE",
    )

    op.create_foreign_key(
        "dq_connections_project_key_fkey",
        "dq_connections",
        "dq_projects",
        ["project_key"],
        ["project_key"],
        ondelete="CASCADE",
    )
    op.create_foreign_key(
        "dq_test_suites_project_key_fkey",
        "dq_test_suites",
        "dq_projects",
        ["project_key"],
        ["project_key"],
        ondelete="SET NULL",
    )


def downgrade() -> None:
    op.drop_constraint("fk_dq_projects_workspace", "dq_projects", type_="foreignkey")
    op.drop_constraint("uq_dq_projects_workspace_id", "dq_projects", type_="unique")
    op.drop_column("dq_projects", "workspace_id")


# ---------------------------------------------------------------------------
# Migration helpers


def _load_data_object_workspaces(conn) -> dict[str, str]:
    mapping: dict[str, str] = {}
    rows = conn.execute(text("SELECT id, workspace_id FROM data_objects")).fetchall()
    for row in rows:
        data_object_id = _normalize_uuid(row[0])
        workspace_id = _normalize_uuid(row[1])
        if data_object_id and workspace_id:
            mapping[data_object_id] = workspace_id
    return mapping


def _load_workspace_metadata(conn) -> dict[str, dict[str, str | None]]:
    lookup: dict[str, dict[str, str | None]] = {}
    rows = conn.execute(text("SELECT id, name, description FROM workspaces")).fetchall()
    for row in rows:
        workspace_id = _normalize_uuid(row[0])
        if not workspace_id:
            continue
        lookup[workspace_id] = {
            "name": row[1],
            "description": row[2],
        }
    return lookup


def _determine_default_workspace(conn, workspace_lookup: dict[str, dict[str, str | None]]) -> str:
    row = conn.execute(
        text("SELECT id FROM workspaces WHERE is_default = true ORDER BY created_at LIMIT 1")
    ).fetchone()
    if row:
        normalized = _normalize_uuid(row[0])
        if normalized:
            return normalized
    if workspace_lookup:
        return next(iter(workspace_lookup.keys()))
    raise RuntimeError("At least one workspace is required to migrate data quality projects.")


def _resolve_workspace_id(
    project_key: str,
    *,
    data_object_map: dict[str, str],
    workspace_lookup: dict[str, dict[str, str | None]],
    default_workspace_id: str,
) -> str:
    if not project_key:
        return default_workspace_id
    parts = project_key.split(":")
    if parts and parts[0] == "workspace" and len(parts) >= 2:
        candidate = _normalize_uuid(parts[1])
        if candidate and candidate in workspace_lookup:
            return candidate
    try:
        object_index = parts.index("object")
        if object_index + 1 < len(parts):
            data_object_id = _normalize_uuid(parts[object_index + 1])
            if data_object_id:
                workspace = data_object_map.get(data_object_id)
                if workspace:
                    return workspace
    except ValueError:
        pass
    return default_workspace_id


def _migrate_project(conn, old_key: str, canonical_key: str, workspace_id: str, workspace_lookup: dict[str, dict[str, str | None]]) -> None:
    _update_references(conn, old_key, canonical_key)
    _update_project_key(conn, old_key, canonical_key)
    _update_project_metadata(conn, canonical_key, workspace_id, workspace_lookup)


def _repoint_project(conn, old_key: str, canonical_key: str) -> None:
    if old_key == canonical_key:
        return
    _update_references(conn, old_key, canonical_key)
    conn.execute(text("DELETE FROM dq_projects WHERE project_key = :project_key"), {"project_key": old_key})


def _update_references(conn, old_key: str, new_key: str) -> None:
    if old_key == new_key:
        return
    params = {"old_key": old_key, "new_key": new_key}
    conn.execute(
        text("UPDATE dq_connections SET project_key = :new_key WHERE project_key = :old_key"),
        params,
    )
    conn.execute(
        text("UPDATE dq_test_suites SET project_key = :new_key WHERE project_key = :old_key"),
        params,
    )
    conn.execute(
        text("UPDATE dq_test_runs SET project_key = :new_key WHERE project_key = :old_key"),
        params,
    )


def _update_project_key(conn, old_key: str, new_key: str) -> None:
    if old_key == new_key:
        return
    conn.execute(
        text("UPDATE dq_projects SET project_key = :new_key WHERE project_key = :old_key"),
        {"old_key": old_key, "new_key": new_key},
    )


def _update_project_metadata(
    conn,
    project_key: str,
    workspace_id: str,
    workspace_lookup: dict[str, dict[str, str | None]],
) -> None:
    conn.execute(
        text("UPDATE dq_projects SET workspace_id = :workspace_id WHERE project_key = :project_key"),
        {"workspace_id": workspace_id, "project_key": project_key},
    )
    workspace = workspace_lookup.get(workspace_id)
    if workspace:
        conn.execute(
            text("UPDATE dq_projects SET name = :name WHERE project_key = :project_key"),
            {"name": workspace.get("name") or "Workspace", "project_key": project_key},
        )
        description = workspace.get("description")
        if description is not None:
            conn.execute(
                text("UPDATE dq_projects SET description = :description WHERE project_key = :project_key"),
                {"description": description, "project_key": project_key},
            )


def _normalize_uuid(value) -> str | None:
    if value is None:
        return None
    try:
        return str(uuid.UUID(str(value)))
    except (TypeError, ValueError, AttributeError):
        text_value = str(value).strip()
        return text_value or None