from __future__ import annotations

from collections.abc import Callable, Generator, Sequence

from fastapi import APIRouter, Body, Depends, HTTPException, Query, Response, status

from app.ingestion.engine import get_ingestion_connection_params
from app.schemas import (
    AlertAcknowledgeRequest,
    AlertCreateRequest,
    AlertCreateResponse,
    ProfileAnomalyPayload,
    ProfileRunCompleteRequest,
    ProfileRunStartRequest,
    ProfileRunStartResponse,
    TestGenAlert,
    TestGenConnection,
    TestGenProfileRun,
    TestGenProject,
    TestGenTable,
    TestGenTableGroup,
    TestGenTestRun,
    TestResultBatchRequest,
    TestResultPayload,
    TestRunCompleteRequest,
    TestRunStartRequest,
    TestRunStartResponse,
)
from app.services.data_quality_testgen import (
    AlertRecord,
    ProfileAnomaly,
    TestGenClient,
    TestGenClientError,
    TestResultRecord,
)

router = APIRouter(prefix="/data-quality/testgen", tags=["Data Quality TestGen"])


def _ensure_schema(schema: str | None) -> str:
    value = (schema or "").strip()
    if not value:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Databricks data quality schema is not configured.",
        )
    return value


def get_testgen_client() -> Generator[TestGenClient, None, None]:
    params = get_ingestion_connection_params()
    schema = _ensure_schema(params.data_quality_schema)
    client = TestGenClient(params, schema=schema)
    try:
        yield client
    finally:
        client.close()


def _invoke(call: Callable[[], object]):
    try:
        return call()
    except TestGenClientError as exc:  # pragma: no cover - defensive guard
        raise HTTPException(status.HTTP_502_BAD_GATEWAY, detail=str(exc)) from exc


def _build_anomalies(payloads: Sequence[ProfileAnomalyPayload]) -> list[ProfileAnomaly]:
    return [
        ProfileAnomaly(
            table_name=item.table_name,
            column_name=item.column_name,
            anomaly_type=item.anomaly_type,
            severity=item.severity,
            description=item.description,
            detected_at=item.detected_at,
        )
        for item in payloads
    ]


def _build_test_results(payload: TestResultBatchRequest) -> list[TestResultRecord]:
    return [
        TestResultRecord(
            test_id=item.test_id,
            table_name=item.table_name,
            column_name=item.column_name,
            result_status=item.result_status,
            expected_value=item.expected_value,
            actual_value=item.actual_value,
            message=item.message,
            detected_at=item.detected_at,
        )
        for item in payload.results
    ]


def _build_alert_record(payload: AlertCreateRequest) -> AlertRecord:
    acknowledged = bool(payload.acknowledged) if payload.acknowledged is not None else False
    return AlertRecord(
        source_type=payload.source_type,
        source_ref=payload.source_ref,
        severity=payload.severity,
        title=payload.title,
        details=payload.details,
        alert_id=payload.alert_id,
        acknowledged=acknowledged,
        acknowledged_by=payload.acknowledged_by,
        acknowledged_at=payload.acknowledged_at,
    )


@router.get("/projects", response_model=list[TestGenProject])
def list_projects(client: TestGenClient = Depends(get_testgen_client)):
    return _invoke(client.list_projects)


@router.get(
    "/projects/{project_key}/connections",
    response_model=list[TestGenConnection],
)
def list_connections(project_key: str, client: TestGenClient = Depends(get_testgen_client)):
    return _invoke(lambda: client.list_connections(project_key))


@router.get(
    "/connections/{connection_id}/table-groups",
    response_model=list[TestGenTableGroup],
)
def list_table_groups(connection_id: str, client: TestGenClient = Depends(get_testgen_client)):
    return _invoke(lambda: client.list_table_groups(connection_id))


@router.get(
    "/table-groups/{table_group_id}/tables",
    response_model=list[TestGenTable],
)
def list_tables(table_group_id: str, client: TestGenClient = Depends(get_testgen_client)):
    return _invoke(lambda: client.list_tables(table_group_id))


@router.get(
    "/table-groups/{table_group_id}/profile-runs",
    response_model=list[TestGenProfileRun],
)
def recent_profile_runs(
    table_group_id: str,
    limit: int = Query(20, ge=1, le=200),
    client: TestGenClient = Depends(get_testgen_client),
):
    return _invoke(lambda: client.recent_profile_runs(table_group_id, limit=limit))


@router.get(
    "/projects/{project_key}/test-runs",
    response_model=list[TestGenTestRun],
)
def recent_test_runs(
    project_key: str,
    limit: int = Query(20, ge=1, le=200),
    client: TestGenClient = Depends(get_testgen_client),
):
    return _invoke(lambda: client.recent_test_runs(project_key, limit=limit))


@router.get("/alerts", response_model=list[TestGenAlert])
def recent_alerts(
    limit: int = Query(50, ge=1, le=500),
    include_acknowledged: bool = Query(False),
    client: TestGenClient = Depends(get_testgen_client),
):
    return _invoke(
        lambda: client.recent_alerts(limit=limit, include_acknowledged=include_acknowledged)
    )


@router.post("/alerts", response_model=AlertCreateResponse, status_code=status.HTTP_201_CREATED)
def create_alert(alert: AlertCreateRequest, client: TestGenClient = Depends(get_testgen_client)):
    alert_id = _invoke(lambda: client.create_alert(_build_alert_record(alert)))
    return AlertCreateResponse(alert_id=alert_id)


@router.post(
    "/alerts/{alert_id}/acknowledge",
    status_code=status.HTTP_200_OK,
    response_class=Response,
)
def acknowledge_alert(
    alert_id: str,
    payload: AlertAcknowledgeRequest = Body(default_factory=AlertAcknowledgeRequest),
    client: TestGenClient = Depends(get_testgen_client),
) -> None:
    _invoke(
        lambda: client.acknowledge_alert(
            alert_id,
            acknowledged=payload.acknowledged,
            acknowledged_by=payload.acknowledged_by,
            acknowledged_at=payload.acknowledged_at,
        )
    )


@router.delete(
    "/alerts/{alert_id}",
    status_code=status.HTTP_204_NO_CONTENT,
    response_class=Response,
)
def delete_alert(alert_id: str, client: TestGenClient = Depends(get_testgen_client)) -> Response:
    _invoke(lambda: client.delete_alert(alert_id))
    return Response(status_code=status.HTTP_204_NO_CONTENT)


@router.post("/profile-runs", response_model=ProfileRunStartResponse, status_code=status.HTTP_201_CREATED)
def start_profile_run(request: ProfileRunStartRequest, client: TestGenClient = Depends(get_testgen_client)):
    profile_run_id = _invoke(
        lambda: client.start_profile_run(
            request.table_group_id,
            status=request.status,
            started_at=request.started_at,
            payload_path=request.payload_path,
        )
    )
    return ProfileRunStartResponse(profile_run_id=profile_run_id)


@router.post(
    "/profile-runs/{profile_run_id}/complete",
    status_code=status.HTTP_204_NO_CONTENT,
    response_class=Response,
)
def complete_profile_run(
    profile_run_id: str,
    request: ProfileRunCompleteRequest,
    client: TestGenClient = Depends(get_testgen_client),
) -> Response:
    anomalies = _build_anomalies(request.anomalies)
    _invoke(
        lambda: client.complete_profile_run(
            profile_run_id,
            status=request.status,
            row_count=request.row_count,
            anomaly_count=request.anomaly_count,
            anomalies=anomalies,
        )
    )
    return Response(status_code=status.HTTP_204_NO_CONTENT)


@router.post("/test-runs", response_model=TestRunStartResponse, status_code=status.HTTP_201_CREATED)
def start_test_run(request: TestRunStartRequest, client: TestGenClient = Depends(get_testgen_client)):
    test_run_id = _invoke(
        lambda: client.start_test_run(
            project_key=request.project_key,
            test_suite_key=request.test_suite_key,
            total_tests=request.total_tests,
            trigger_source=request.trigger_source,
            status=request.status,
            started_at=request.started_at,
        )
    )
    return TestRunStartResponse(test_run_id=test_run_id)


@router.post(
    "/test-runs/{test_run_id}/results",
    status_code=status.HTTP_202_ACCEPTED,
    response_class=Response,
)
def record_test_results(
    test_run_id: str,
    request: TestResultBatchRequest,
    client: TestGenClient = Depends(get_testgen_client),
) -> Response:
    results = _build_test_results(request)
    _invoke(lambda: client.record_test_results(test_run_id, results))
    return Response(status_code=status.HTTP_202_ACCEPTED)


@router.post(
    "/test-runs/{test_run_id}/complete",
    status_code=status.HTTP_204_NO_CONTENT,
    response_class=Response,
)
def complete_test_run(
    test_run_id: str,
    request: TestRunCompleteRequest,
    client: TestGenClient = Depends(get_testgen_client),
) -> Response:
    _invoke(
        lambda: client.complete_test_run(
            test_run_id,
            status=request.status,
            failed_tests=request.failed_tests,
            duration_ms=request.duration_ms,
        )
    )
    return Response(status_code=status.HTTP_204_NO_CONTENT)
