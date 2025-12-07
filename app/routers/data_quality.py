from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any, Dict, List
from uuid import UUID

from fastapi import APIRouter, BackgroundTasks, Depends, HTTPException, Query, Request, status
from sqlalchemy.orm import Session, joinedload

from app.database import get_db
from app.models import DataObject, ProcessArea, System, SystemConnection
from app.routers.data_quality_testgen import get_testgen_client
from app.schemas.data_quality import (
    DataQualityBulkProfileRunResponse,
    DataQualityDatasetProfilingStatsResponse,
    DataQualityDatasetProductTeam,
    DataQualityDatasetTableContext,
    DataQualityProfileRunDeleteRequest,
    DataQualityProfileRunDeleteResponse,
    DataQualityProfileRunEntry,
    DataQualityProfileRunListResponse,
    DataQualityProfileRunTableGroup,
    DataQualityProfileRunSummary,
    DataQualityProfileRunResultResponse,
    ProfileRunStartResponse,
    TestGenProfileAnomaly,
)
from app.services.data_quality_keys import parse_connection_id, parse_table_group_id
from app.services.data_quality_datasets import build_dataset_hierarchy
from app.services.data_quality_dataset_stats import build_dataset_profiling_stats
from app.services.data_quality_profiling import (
    CALLBACK_URL_PLACEHOLDER,
    DataQualityProfilingService,
    ProfilingConfigurationError,
    ProfilingServiceError,
    ProfilingTargetNotFound,
)
from app.services.data_quality_table_context import resolve_table_context, resolve_table_contexts_for_data_object
from app.services.data_quality_testgen import TestGenClient, TestGenClientError
from app.services.workspace_scope import resolve_workspace_id

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/data-quality", tags=["Data Quality"])


def _launch_profile_run_background(
    profiling_service: DataQualityProfilingService,
    prepared_run,
) -> None:
    try:
        profiling_service.launch_prepared_profile_run(prepared_run)
    except ProfilingServiceError as exc:  # pragma: no cover - defensive logging
        logger.error(
            "Deferred profiling launch failed for table_group_id=%s profile_run_id=%s: %s",
            getattr(prepared_run.target, "table_group_id", "unknown"),
            getattr(prepared_run, "profile_run_id", "unknown"),
            exc,
        )


def _try_parse_uuid(value: str | None) -> UUID | None:
    if not value:
        return None
    try:
        return UUID(str(value))
    except (TypeError, ValueError):
        return None


def _extract_ids_from_table_group(table_group_id: str | None) -> tuple[UUID | None, UUID | None]:
    connection_id_text, data_object_id_text = parse_table_group_id(table_group_id)
    return _try_parse_uuid(connection_id_text), _try_parse_uuid(data_object_id_text)


def _extract_ids_from_connection_identifier(connection_identifier: str | None) -> tuple[UUID | None, UUID | None]:
    connection_id_text, data_object_id_text = parse_connection_id(connection_identifier)
    return _try_parse_uuid(connection_id_text), _try_parse_uuid(data_object_id_text)


def _compute_duration_ms(
    started_at: datetime | None,
    completed_at: datetime | None,
    status: str | None,
    *,
    reference: datetime | None = None,
) -> int | None:
    if not started_at:
        return None
    if completed_at:
        delta = completed_at - started_at
    else:
        normalized = (status or "").lower()
        if normalized not in {"running", "pending"}:
            return None
        current = reference or datetime.now(timezone.utc)
        delta = current - started_at
    milliseconds = int(delta.total_seconds() * 1000)
    return milliseconds if milliseconds >= 0 else None


def _resolve_connection_uuid(
    connection_identifier: str | None,
    table_group_id: str | None,
) -> UUID | None:
    connection_uuid, _ = _extract_ids_from_connection_identifier(connection_identifier)
    if connection_uuid:
        return connection_uuid
    connection_uuid, _ = _extract_ids_from_table_group(table_group_id)
    return connection_uuid


@router.get("/datasets", response_model=List[DataQualityDatasetProductTeam])
def get_dataset_hierarchy(
    workspace_id: UUID | None = Query(default=None, alias="workspaceId"),
    db: Session = Depends(get_db),
) -> List[DataQualityDatasetProductTeam]:
    resolved_workspace_id = resolve_workspace_id(db, workspace_id)
    return build_dataset_hierarchy(db, resolved_workspace_id)


@router.get(
    "/datasets/profiling-stats",
    response_model=DataQualityDatasetProfilingStatsResponse,
)
def get_dataset_profiling_stats(
    workspace_id: UUID | None = Query(default=None, alias="workspaceId"),
    db: Session = Depends(get_db),
    client: TestGenClient = Depends(get_testgen_client),
) -> DataQualityDatasetProfilingStatsResponse:
    resolved_workspace_id = resolve_workspace_id(db, workspace_id)
    try:
        return build_dataset_profiling_stats(db, client, resolved_workspace_id)
    except TestGenClientError as exc:  # pragma: no cover - defensive
        raise HTTPException(status.HTTP_502_BAD_GATEWAY, detail=str(exc)) from exc


@router.get(
    "/datasets/tables/{data_definition_table_id}/context",
    response_model=DataQualityDatasetTableContext,
)
def get_dataset_table_context(
    data_definition_table_id: UUID,
    workspace_id: UUID | None = Query(default=None, alias="workspaceId"),
    db: Session = Depends(get_db)
) -> DataQualityDatasetTableContext:
    resolved_workspace_id = resolve_workspace_id(db, workspace_id)
    context = resolve_table_context(db, data_definition_table_id, workspace_id=resolved_workspace_id)
    return DataQualityDatasetTableContext(
        data_definition_table_id=context.data_definition_table_id,
        data_definition_id=context.data_definition_id,
        data_object_id=context.data_object_id,
        workspace_id=context.workspace_id,
        application_id=context.application_id,
        product_team_id=context.product_team_id,
        table_group_id=context.table_group_id,
        table_id=context.table_id,
        schema_name=context.schema_name,
        table_name=context.table_name,
        physical_name=context.physical_name,
    )


def get_profiling_service(client: TestGenClient = Depends(get_testgen_client)) -> DataQualityProfilingService:
    return DataQualityProfilingService(client)


@router.post(
    "/datasets/{data_object_id}/profile-runs",
    response_model=DataQualityBulkProfileRunResponse,
    status_code=status.HTTP_202_ACCEPTED,
)
def start_profile_runs_for_data_object(
    data_object_id: UUID,
    request: Request,
    background_tasks: BackgroundTasks,
    workspace_id: UUID | None = Query(default=None, alias="workspaceId"),
    db: Session = Depends(get_db),
    profiling_service: DataQualityProfilingService = Depends(get_profiling_service),
) -> DataQualityBulkProfileRunResponse:
    resolved_workspace_id = resolve_workspace_id(db, workspace_id)

    data_object_exists = (
        db.query(DataObject.id)
        .filter(DataObject.id == data_object_id, DataObject.workspace_id == resolved_workspace_id)
        .one_or_none()
    )
    if not data_object_exists:
        raise HTTPException(status.HTTP_404_NOT_FOUND, detail="Data object not found")

    contexts, skipped_table_ids = resolve_table_contexts_for_data_object(
        db,
        data_object_id,
        workspace_id=resolved_workspace_id,
    )

    if not contexts and not skipped_table_ids:
        raise HTTPException(status.HTTP_404_NOT_FOUND, detail="Data object not found or has no tables")

    if not contexts:
        raise HTTPException(
            status.HTTP_400_BAD_REQUEST,
            detail="No tables with valid connections are available for profiling",
        )

    requested_table_count = len(contexts) + len(skipped_table_ids)

    table_group_ids = sorted({context.table_group_id for context in contexts})
    profile_runs: List[DataQualityProfileRunSummary] = []

    callback_template = str(
        request.url_for(
            "complete_profile_run",
            profile_run_id=CALLBACK_URL_PLACEHOLDER,
        )
    )

    for table_group_id in table_group_ids:
        try:
            prepared = profiling_service.prepare_profile_run(
                table_group_id,
                callback_url_template=callback_template,
            )
        except ProfilingTargetNotFound as exc:
            raise HTTPException(status.HTTP_404_NOT_FOUND, detail=str(exc)) from exc
        except ProfilingConfigurationError as exc:
            logger.warning(
                "Profiling configuration error for table_group_id=%s: %s",
                table_group_id,
                exc,
            )
            raise HTTPException(status.HTTP_503_SERVICE_UNAVAILABLE, detail=str(exc)) from exc
        except ProfilingServiceError as exc:  # pragma: no cover - defensive
            logger.exception(
                "Profiling service error for table_group_id=%s",
                table_group_id,
            )
            raise HTTPException(status.HTTP_502_BAD_GATEWAY, detail=str(exc)) from exc

        profile_runs.append(
            DataQualityProfileRunSummary(
                table_group_id=table_group_id,
                profile_run_id=prepared.profile_run_id,
            )
        )
        background_tasks.add_task(_launch_profile_run_background, profiling_service, prepared)

    return DataQualityBulkProfileRunResponse(
        requested_table_count=requested_table_count,
        targeted_table_group_count=len(table_group_ids),
        profile_runs=profile_runs,
        skipped_table_ids=skipped_table_ids,
    )


@router.post(
    "/table-groups/{table_group_id}/profile-runs",
    response_model=ProfileRunStartResponse,
    status_code=status.HTTP_202_ACCEPTED,
)
def start_profile_run_for_table_group(
    table_group_id: str,
    request: Request,
    background_tasks: BackgroundTasks,
    workspace_id: UUID | None = Query(default=None, alias="workspaceId"),
    db: Session = Depends(get_db),
    profiling_service: DataQualityProfilingService = Depends(get_profiling_service),
) -> ProfileRunStartResponse:
    resolved_workspace_id = resolve_workspace_id(db, workspace_id)

    _, data_object_uuid = _extract_ids_from_table_group(table_group_id)
    if not data_object_uuid:
        raise HTTPException(status.HTTP_400_BAD_REQUEST, detail="Table group is not linked to a data object")

    data_object_exists = (
        db.query(DataObject.id)
        .filter(DataObject.id == data_object_uuid, DataObject.workspace_id == resolved_workspace_id)
        .one_or_none()
    )
    if not data_object_exists:
        raise HTTPException(status.HTTP_404_NOT_FOUND, detail="Data object not found")

    callback_template = str(
        request.url_for(
            "complete_profile_run",
            profile_run_id=CALLBACK_URL_PLACEHOLDER,
        )
    )

    try:
        prepared = profiling_service.prepare_profile_run(
            table_group_id,
            callback_url_template=callback_template,
        )
    except ProfilingTargetNotFound as exc:
        raise HTTPException(status.HTTP_404_NOT_FOUND, detail=str(exc)) from exc
    except ProfilingConfigurationError as exc:
        logger.warning(
            "Profiling configuration error for table_group_id=%s: %s",
            table_group_id,
            exc,
        )
        raise HTTPException(status.HTTP_503_SERVICE_UNAVAILABLE, detail=str(exc)) from exc
    except ProfilingServiceError as exc:  # pragma: no cover - defensive
        logger.exception(
            "Profiling service error for table_group_id=%s",
            table_group_id,
        )
        raise HTTPException(status.HTTP_502_BAD_GATEWAY, detail=str(exc)) from exc

    background_tasks.add_task(_launch_profile_run_background, profiling_service, prepared)

    return ProfileRunStartResponse(profile_run_id=prepared.profile_run_id)


def _build_entity_maps(
    db: Session,
    *,
    connection_ids: set[UUID],
    data_object_ids: set[UUID],
    workspace_id: UUID | None = None,
) -> tuple[Dict[UUID, SystemConnection], Dict[UUID, DataObject]]:
    connections: Dict[UUID, SystemConnection] = {}
    data_objects: Dict[UUID, DataObject] = {}

    if connection_ids:
        rows = (
            db.query(SystemConnection)
            .options(joinedload(SystemConnection.system))
            .filter(SystemConnection.id.in_(connection_ids))
            .all()
        )
        connections = {row.id: row for row in rows}

    if data_object_ids:
        query = (
            db.query(DataObject)
            .options(joinedload(DataObject.process_area))
            .filter(DataObject.id.in_(data_object_ids))
        )
        if workspace_id:
            query = query.filter(DataObject.workspace_id == workspace_id)
        rows = query.all()
        data_objects = {row.id: row for row in rows}

    return connections, data_objects


def _derive_workspace_table_groups(
    db: Session,
    *,
    workspace_id: UUID,
    data_objects_map: Dict[UUID, DataObject],
    connections_map: Dict[UUID, SystemConnection],
) -> List[DataQualityProfileRunTableGroup]:
    if not data_objects_map:
        return []

    accumulators: Dict[str, Dict[str, Any]] = {}
    required_connection_ids: set[UUID] = set()

    for data_object in data_objects_map.values():
        if getattr(data_object, "workspace_id", None) != workspace_id:
            continue
        if not data_object.id:
            continue
        try:
            contexts, _ = resolve_table_contexts_for_data_object(
                db,
                data_object.id,
                workspace_id=workspace_id,
            )
        except HTTPException:
            continue
        for context in contexts:
            table_group_id = context.table_group_id
            if not table_group_id:
                continue
            connection_uuid, _ = _extract_ids_from_table_group(table_group_id)
            if connection_uuid:
                required_connection_ids.add(connection_uuid)
            bucket = accumulators.setdefault(
                table_group_id,
                {
                    "data_object_id": context.data_object_id,
                    "application_id": context.application_id,
                    "product_team_id": context.product_team_id,
                    "connection_id": connection_uuid,
                    "table_names": set(),
                    "schema_names": set(),
                },
            )
            if connection_uuid and bucket.get("connection_id") is None:
                bucket["connection_id"] = connection_uuid
            if context.table_name:
                bucket["table_names"].add(context.table_name)
            if context.schema_name:
                bucket["schema_names"].add(context.schema_name)

    missing_connections = [cid for cid in required_connection_ids if cid not in connections_map]
    if missing_connections:
        rows = (
            db.query(SystemConnection)
            .options(joinedload(SystemConnection.system))
            .filter(SystemConnection.id.in_(missing_connections))
            .all()
        )
        for row in rows:
            connections_map[row.id] = row

    fallback_groups: List[DataQualityProfileRunTableGroup] = []
    for table_group_id, accumulator in accumulators.items():
        data_object = data_objects_map.get(accumulator.get("data_object_id"))
        if not data_object:
            continue
        process_area: ProcessArea | None = getattr(data_object, "process_area", None)
        connection_uuid = accumulator.get("connection_id")
        connection = connections_map.get(connection_uuid)
        system: System | None = getattr(connection, "system", None) if connection else None
        table_group_name = f"{data_object.name} Tables" if getattr(data_object, "name", None) else None
        schema_name = next(iter(accumulator["schema_names"]), None) if accumulator["schema_names"] else None

        fallback_groups.append(
            DataQualityProfileRunTableGroup(
                table_group_id=table_group_id,
                table_group_name=table_group_name,
                connection_id=connection_uuid,
                connection_name=getattr(connection, "display_name", None),
                catalog=getattr(connection, "databricks_catalog", None),
                schema_name=schema_name or getattr(connection, "databricks_schema", None),
                data_object_id=getattr(data_object, "id", None),
                data_object_name=getattr(data_object, "name", None),
                application_id=getattr(system, "id", None) or accumulator.get("application_id"),
                application_name=getattr(system, "name", None),
                application_description=getattr(system, "description", None),
                product_team_id=getattr(process_area, "id", None),
                product_team_name=getattr(process_area, "name", None),
                table_count=len(accumulator["table_names"]) or None,
                field_count=None,
                profiling_job_id=None,
            )
        )

    return fallback_groups


@router.get("/profile-runs", response_model=DataQualityProfileRunListResponse)
def list_profile_runs(
    table_group_id: str | None = Query(default=None, alias="tableGroupId"),
    limit: int = Query(default=50, ge=1, le=500),
    include_groups: bool = Query(default=True, alias="includeGroups"),
    workspace_id: UUID | None = Query(default=None, alias="workspaceId"),
    db: Session = Depends(get_db),
    client: TestGenClient = Depends(get_testgen_client),
) -> DataQualityProfileRunListResponse:
    resolved_workspace_id = resolve_workspace_id(db, workspace_id)
    try:
        runs_raw = client.list_profile_runs_overview(table_group_id=table_group_id, limit=limit)
    except TestGenClientError as exc:  # pragma: no cover - defensive
        raise HTTPException(status.HTTP_502_BAD_GATEWAY, detail=str(exc)) from exc

    group_rows: List[dict] = []
    if include_groups:
        try:
            group_rows = client.list_table_groups_with_connections()
        except TestGenClientError as exc:  # pragma: no cover - defensive
            raise HTTPException(status.HTTP_502_BAD_GATEWAY, detail=str(exc)) from exc

    connection_ids: set[UUID] = set()
    data_object_ids: set[UUID] = set()

    def _register_row(row: dict) -> None:
        conn_uuid, obj_uuid = _extract_ids_from_connection_identifier(row.get("connection_id"))
        if conn_uuid:
            connection_ids.add(conn_uuid)
        if obj_uuid:
            data_object_ids.add(obj_uuid)
        conn_uuid, obj_uuid = _extract_ids_from_table_group(row.get("table_group_id"))
        if conn_uuid:
            connection_ids.add(conn_uuid)
        if obj_uuid:
            data_object_ids.add(obj_uuid)

    for row in runs_raw:
        _register_row(row)

    for row in group_rows:
        _register_row(row)

    if resolved_workspace_id:
        workspace_object_rows = (
            db.query(DataObject.id)
            .filter(DataObject.workspace_id == resolved_workspace_id)
            .all()
        )
        for row in workspace_object_rows:
            data_object_id = row[0]
            if data_object_id:
                data_object_ids.add(data_object_id)

    connections_map, data_objects_map = _build_entity_maps(
        db,
        connection_ids=connection_ids,
        data_object_ids=data_object_ids,
        workspace_id=resolved_workspace_id,
    )

    def _data_object_uuid_for_row(row: dict) -> UUID | None:
        _, data_object_uuid = _extract_ids_from_table_group(row.get("table_group_id"))
        if data_object_uuid:
            return data_object_uuid
        _, data_object_uuid = _extract_ids_from_connection_identifier(row.get("connection_id"))
        return data_object_uuid

    def _row_in_workspace(row: dict) -> bool:
        data_object_uuid = _data_object_uuid_for_row(row)
        return bool(data_object_uuid and data_object_uuid in data_objects_map)

    runs_filtered = [row for row in runs_raw if _row_in_workspace(row)]

    group_rows_filtered = [
        row
        for row in group_rows
        if _row_in_workspace(row) and _resolve_connection_uuid(row.get("connection_id"), row.get("table_group_id"))
    ]

    filtered_run_ids = [row.get("profile_run_id") for row in runs_filtered if row.get("profile_run_id")]
    try:
        severity_map = client.profile_run_anomaly_counts(filtered_run_ids)
    except TestGenClientError as exc:  # pragma: no cover - defensive
        raise HTTPException(status.HTTP_502_BAD_GATEWAY, detail=str(exc)) from exc

    table_group_ids: set[str] = set()
    for row in runs_filtered:
        if row.get("table_group_id"):
            table_group_ids.add(row["table_group_id"])
    for row in group_rows_filtered:
        if row.get("table_group_id"):
            table_group_ids.add(row["table_group_id"])

    table_group_counts: Dict[str, Dict[str, int]] = {}
    if table_group_ids:
        try:
            table_characteristics = client.fetch_table_characteristics(table_group_ids=tuple(table_group_ids))
        except TestGenClientError as exc:  # pragma: no cover - defensive
            raise HTTPException(status.HTTP_502_BAD_GATEWAY, detail=str(exc)) from exc
        for entry in table_characteristics:
            table_group = entry.get("table_group_id")
            if not table_group:
                continue
            bucket = table_group_counts.setdefault(table_group, {"table_count": 0, "field_count": 0})
            bucket["table_count"] += 1
            try:
                column_count = int(entry.get("column_count") or 0)
            except (TypeError, ValueError):
                column_count = 0
            bucket["field_count"] += max(column_count, 0)

    reference_time = datetime.now(timezone.utc)
    runs_payload: List[DataQualityProfileRunEntry] = []
    for row in runs_filtered:
        profile_run_id = row.get("profile_run_id")
        table_group = row.get("table_group_id")
        connection_uuid = _resolve_connection_uuid(row.get("connection_id"), table_group)
        _, data_object_uuid = _extract_ids_from_table_group(table_group)

        connection = connections_map.get(connection_uuid)
        system: System | None = getattr(connection, "system", None) if connection else None
        data_object = data_objects_map.get(data_object_uuid)
        process_area: ProcessArea | None = data_object.process_area if data_object else None
        severity_counts = severity_map.get(profile_run_id, {}) if profile_run_id else {}
        counts = table_group_counts.get(table_group or "", {})

        runs_payload.append(
            DataQualityProfileRunEntry(
                profile_run_id=profile_run_id or "",
                table_group_id=table_group or "",
                table_group_name=row.get("table_group_name"),
                connection_id=connection_uuid,
                connection_name=row.get("connection_name") or (system.name if system else None),
                catalog=row.get("catalog"),
                schema_name=row.get("schema_name"),
                data_object_id=data_object_uuid,
                data_object_name=data_object.name if data_object else None,
                application_id=system.id if system else None,
                application_name=system.name if system else None,
                application_description=getattr(system, "description", None),
                product_team_id=process_area.id if process_area else None,
                product_team_name=process_area.name if process_area else None,
                table_count=counts.get("table_count"),
                field_count=counts.get("field_count"),
                status=row.get("status") or "unknown",
                started_at=row.get("started_at"),
                completed_at=row.get("completed_at"),
                duration_ms=_compute_duration_ms(
                    row.get("started_at"),
                    row.get("completed_at"),
                    row.get("status"),
                    reference=reference_time,
                ),
                row_count=row.get("row_count"),
                anomaly_count=row.get("anomaly_count"),
                databricks_run_id=row.get("databricks_run_id"),
                anomalies_by_severity=severity_counts,
                profiling_score=row.get("dq_score_profiling"),
            )
        )

    table_group_payload: List[DataQualityProfileRunTableGroup] = []
    for row in group_rows_filtered:
        table_group = row.get("table_group_id")
        connection_uuid = _resolve_connection_uuid(row.get("connection_id"), table_group)
        _, data_object_uuid = _extract_ids_from_table_group(table_group)
        connection = connections_map.get(connection_uuid)
        system: System | None = getattr(connection, "system", None) if connection else None
        data_object = data_objects_map.get(data_object_uuid)
        process_area: ProcessArea | None = data_object.process_area if data_object else None
        counts = table_group_counts.get(table_group or "", {})

        table_group_payload.append(
            DataQualityProfileRunTableGroup(
                table_group_id=table_group or "",
                table_group_name=row.get("table_group_name"),
                connection_id=connection_uuid,
                connection_name=row.get("connection_name") or (system.name if system else None),
                catalog=row.get("catalog"),
                schema_name=row.get("schema_name"),
                data_object_id=data_object_uuid,
                data_object_name=data_object.name if data_object else None,
                application_id=system.id if system else None,
                application_name=system.name if system else None,
                application_description=getattr(system, "description", None),
                product_team_id=process_area.id if process_area else None,
                product_team_name=process_area.name if process_area else None,
                table_count=counts.get("table_count"),
                field_count=counts.get("field_count"),
                profiling_job_id=row.get("profiling_job_id"),
            )
        )

    if include_groups and resolved_workspace_id:
        fallback_groups = _derive_workspace_table_groups(
            db,
            workspace_id=resolved_workspace_id,
            data_objects_map=data_objects_map,
            connections_map=connections_map,
        )
        if fallback_groups:
            existing_ids = {entry.table_group_id for entry in table_group_payload}
            for entry in fallback_groups:
                if entry.table_group_id not in existing_ids:
                    table_group_payload.append(entry)

    table_group_payload.sort(
        key=lambda entry: (
            (entry.product_team_name or "").lower(),
            (entry.application_name or "").lower(),
            (entry.data_object_name or "").lower(),
            (entry.table_group_name or entry.table_group_id or "").lower(),
        )
    )

    return DataQualityProfileRunListResponse(runs=runs_payload, table_groups=table_group_payload)


@router.get(
    "/profile-runs/{profile_run_id}/anomalies",
    response_model=List[TestGenProfileAnomaly],
)
def get_profile_run_anomalies(
    profile_run_id: str,
    client: TestGenClient = Depends(get_testgen_client),
) -> List[TestGenProfileAnomaly]:
    try:
        anomalies = client.list_profile_run_anomalies(profile_run_id)
    except TestGenClientError as exc:  # pragma: no cover - defensive
        raise HTTPException(status.HTTP_502_BAD_GATEWAY, detail=str(exc)) from exc
    return anomalies


@router.get(
    "/profile-runs/{profile_run_id}/results",
    response_model=DataQualityProfileRunResultResponse,
)
def get_profile_run_results(
    profile_run_id: str,
    table_group_id: str = Query(..., alias="tableGroupId"),
    client: TestGenClient = Depends(get_testgen_client),
) -> DataQualityProfileRunResultResponse:
    try:
        payload = client.export_profiling_payload(table_group_id, profile_run_id=profile_run_id)
    except TestGenClientError as exc:  # pragma: no cover - defensive
        raise HTTPException(status.HTTP_502_BAD_GATEWAY, detail=str(exc)) from exc
    if not payload:
        raise HTTPException(status.HTTP_404_NOT_FOUND, detail="Profiling results not found.")
    return payload


@router.delete(
    "/profile-runs",
    response_model=DataQualityProfileRunDeleteResponse,
)
def delete_profile_runs(
    payload: DataQualityProfileRunDeleteRequest,
    client: TestGenClient = Depends(get_testgen_client),
) -> DataQualityProfileRunDeleteResponse:
    try:
        deleted = client.delete_profile_runs(payload.profile_run_ids)
    except TestGenClientError as exc:  # pragma: no cover - defensive
        raise HTTPException(status.HTTP_502_BAD_GATEWAY, detail=str(exc)) from exc
    return DataQualityProfileRunDeleteResponse(deleted_count=deleted)
