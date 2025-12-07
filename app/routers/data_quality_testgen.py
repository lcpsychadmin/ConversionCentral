from __future__ import annotations

import json
from collections.abc import Callable, Generator, Sequence
from typing import Any, Dict
from uuid import UUID

from fastapi import APIRouter, Body, Depends, HTTPException, Query, Response, status
from sqlalchemy.orm import Session

from app.database import get_db

from app.ingestion.engine import get_ingestion_connection_params
from app.schemas import (
    AlertAcknowledgeRequest,
    AlertCreateRequest,
    AlertCreateResponse,
    ProfileAnomalyPayload,
    ProfileRunCompleteRequest,
    ProfileRunStartRequest,
    ProfileRunStartResponse,
    TestGenColumnProfile,
    TestGenAlert,
    TestGenConnection,
    TestGenProfileRun,
    TestGenProject,
    TestGenTable,
    TestGenTableGroup,
    TestGenSuiteTest,
    TestGenTestRun,
    TestGenTestSuite,
    TestResultBatchRequest,
    TestRunCompleteRequest,
    TestRunStartRequest,
    TestRunStartResponse,
    TestSuiteCreateRequest,
    TestSuiteSeverity,
    TestSuiteUpdateRequest,
    TestDefinitionCreateRequest,
    TestDefinitionUpdateRequest,
    DataQualityTestType,
)
from app.services.data_quality_backend import LOCAL_BACKEND, get_metadata_backend
from app.services.data_quality_local_client import LocalTestGenClient
from app.services.data_quality_testgen import (
    AlertRecord,
    ProfileAnomaly,
    TestGenClient,
    TestGenClientError,
    TestResultRecord,
)
from app.services.data_quality_test_types import (
    get_test_type as load_test_type_metadata,
    list_test_types as list_test_type_metadata,
)
from app.services.data_quality_keys import project_keys_for_data_object
from app.services.data_quality_table_context import TableContext, resolve_table_context

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
    backend = get_metadata_backend()
    if backend == LOCAL_BACKEND:
        client: TestGenClient = LocalTestGenClient()
    else:
        params = get_ingestion_connection_params()
        schema = _ensure_schema(params.data_quality_schema)
        client = TestGenClient(params, schema=schema)
    try:
        yield client
    finally:
        try:
            client.close()
        except Exception:  # pragma: no cover - defensive guard
            pass


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
    "/test-suites",
    response_model=list[TestGenTestSuite],
)
def list_test_suites(
    project_key: str | None = Query(default=None),
    data_object_id: str | None = Query(default=None, alias="dataObjectId"),
    client: TestGenClient = Depends(get_testgen_client),
):
    suites = _invoke(
        lambda: client.list_test_suites(project_key=project_key, data_object_id=data_object_id)
    )
    return [TestGenTestSuite(**suite) for suite in suites]


@router.get(
    "/test-suites/{test_suite_key}",
    response_model=TestGenTestSuite,
)
def get_test_suite(test_suite_key: str, client: TestGenClient = Depends(get_testgen_client)):
    suite = _invoke(lambda: client.get_test_suite(test_suite_key))
    if not suite:
        raise HTTPException(status.HTTP_404_NOT_FOUND, detail="Test suite not found")
    return TestGenTestSuite(**suite)


@router.get(
    "/test-types",
    response_model=list[DataQualityTestType],
)
def list_test_types_endpoint():
    return list_test_type_metadata()


@router.get(
    "/test-types/{rule_type}",
    response_model=DataQualityTestType,
)
def get_test_type_endpoint(rule_type: str):
    metadata = load_test_type_metadata(rule_type)
    if not metadata:
        raise HTTPException(status.HTTP_404_NOT_FOUND, detail="Test type not found")
    return metadata


def _resolve_project_key(
    *,
    explicit_project_key: str | None,
    data_object_id: UUID | str | None,
    db: Session,
) -> str | None:
    if explicit_project_key:
        return explicit_project_key
    if not data_object_id:
        return None
    keys = project_keys_for_data_object(db, data_object_id)
    if len(keys) == 1:
        return next(iter(keys))
    if not keys:
        return None
    raise HTTPException(
        status.HTTP_400_BAD_REQUEST,
        detail="Multiple project keys match the selected data object; specify projectKey explicitly.",
    )


def _severity_value(severity: TestSuiteSeverity | None) -> str | None:
    if severity is None:
        return None
    if isinstance(severity, TestSuiteSeverity):
        return severity.value
    return str(severity)


def _uuid_to_str(value):
    return str(value) if value is not None else None


def _try_parse_uuid(value) -> UUID | None:
    if value is None:
        return None
    try:
        return UUID(str(value))
    except (TypeError, ValueError):
        return None


def _definition_payload(
    *,
    context: TableContext,
    column_name: str | None,
    parameters: Dict[str, Any],
    base: Dict[str, Any] | None = None,
) -> Dict[str, Any]:
    payload: Dict[str, Any] = dict(base or {})
    payload.update(
        {
            "dataDefinitionTableId": str(context.data_definition_table_id),
            "tableGroupId": context.table_group_id,
            "tableId": context.table_id,
            "schemaName": context.schema_name,
            "tableName": context.table_name,
            "physicalName": context.physical_name,
            "columnName": column_name,
            "parameters": parameters,
        }
    )
    return payload


def _parse_definition(raw: Any) -> Dict[str, Any]:
    if raw is None:
        return {}
    if isinstance(raw, dict):
        return raw
    if isinstance(raw, str):
        try:
            return json.loads(raw)
        except json.JSONDecodeError:  # pragma: no cover - defensive
            return {}
    return {}


def _build_suite_test_response(row: Dict[str, Any]) -> TestGenSuiteTest:
    definition_payload = _parse_definition(row.get("definition"))
    data_definition_table_id = _try_parse_uuid(definition_payload.get("dataDefinitionTableId"))

    return TestGenSuiteTest(
        testId=row.get("test_id", ""),
        testSuiteKey=row.get("test_suite_key", ""),
        tableGroupId=row.get("table_group_id", ""),
        tableId=definition_payload.get("tableId"),
        dataDefinitionTableId=data_definition_table_id,
        schemaName=definition_payload.get("schemaName"),
        tableName=definition_payload.get("tableName"),
        physicalName=definition_payload.get("physicalName"),
        columnName=definition_payload.get("columnName"),
        ruleType=row.get("rule_type", ""),
        name=row.get("name", ""),
        definition=definition_payload,
        createdAt=row.get("created_at"),
        updatedAt=row.get("updated_at"),
    )
@router.post(
    "/test-suites",
    response_model=TestGenTestSuite,
    status_code=status.HTTP_201_CREATED,
)
def create_test_suite(
    request: TestSuiteCreateRequest,
    client: TestGenClient = Depends(get_testgen_client),
    db: Session = Depends(get_db),
):


    project_key = _resolve_project_key(
        explicit_project_key=request.project_key,
        data_object_id=request.data_object_id,
        db=db,
    )

    test_suite_key = _invoke(
        lambda: client.create_test_suite(
            project_key=project_key,
            name=request.name,
            description=request.description,
            severity=_severity_value(request.severity),
            product_team_id=_uuid_to_str(request.product_team_id),
            application_id=_uuid_to_str(request.application_id),
            data_object_id=_uuid_to_str(request.data_object_id),
            data_definition_id=_uuid_to_str(request.data_definition_id),
        )
    )

    created = _invoke(lambda: client.get_test_suite(test_suite_key))
    if not created:
        raise HTTPException(status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Failed to load created test suite")
    return TestGenTestSuite(**created)


@router.get(
    "/table-groups/{table_group_id}/column-profile",
    response_model=TestGenColumnProfile,
)
def get_column_profile(
    table_group_id: str,
    column_name: str = Query(..., alias="columnName"),
    table_name: str | None = Query(default=None, alias="tableName"),
    physical_name: str | None = Query(default=None, alias="physicalName"),
    client: TestGenClient = Depends(get_testgen_client),
):
    normalized = (column_name or "").strip()
    if not normalized:
        raise HTTPException(status.HTTP_400_BAD_REQUEST, detail="columnName is required")

    profile = _invoke(
        lambda: client.column_profile(
            table_group_id,
            column_name=normalized,
            table_name=table_name,
            physical_name=physical_name,
        )
    )

    if not profile:
        raise HTTPException(status.HTTP_404_NOT_FOUND, detail="Column profile not found")

    return profile


@router.put(
    "/test-suites/{test_suite_key}",
    response_model=TestGenTestSuite,
)
def update_test_suite(
    test_suite_key: str,
    request: TestSuiteUpdateRequest,
    client: TestGenClient = Depends(get_testgen_client),
    db: Session = Depends(get_db),
):
    provided_fields = request.__fields_set__

    update_kwargs: dict[str, object] = {}

    if "project_key" in provided_fields:
        update_kwargs["project_key"] = request.project_key
    elif "data_object_id" in provided_fields:
        resolved_key = _resolve_project_key(
            explicit_project_key=request.project_key,
            data_object_id=request.data_object_id,
            db=db,
        )
        if resolved_key is not None:
            update_kwargs["project_key"] = resolved_key

    if "name" in provided_fields:
        update_kwargs["name"] = request.name

    if "description" in provided_fields:
        update_kwargs["description"] = request.description

    if "severity" in provided_fields:
        update_kwargs["severity"] = _severity_value(request.severity)

    if "product_team_id" in provided_fields:
        update_kwargs["product_team_id"] = _uuid_to_str(request.product_team_id)

    if "application_id" in provided_fields:
        update_kwargs["application_id"] = _uuid_to_str(request.application_id)

    if "data_object_id" in provided_fields:
        update_kwargs["data_object_id"] = _uuid_to_str(request.data_object_id)

    if "data_definition_id" in provided_fields:
        update_kwargs["data_definition_id"] = _uuid_to_str(request.data_definition_id)

    _invoke(lambda: client.update_test_suite(test_suite_key, **update_kwargs))

    updated = _invoke(lambda: client.get_test_suite(test_suite_key))
    if not updated:
        raise HTTPException(status.HTTP_404_NOT_FOUND, detail="Test suite not found")
    return TestGenTestSuite(**updated)


@router.delete(
    "/test-suites/{test_suite_key}",
    status_code=status.HTTP_204_NO_CONTENT,
    response_class=Response,
)
def delete_test_suite(test_suite_key: str, client: TestGenClient = Depends(get_testgen_client)) -> Response:
    _invoke(lambda: client.delete_test_suite(test_suite_key))
    return Response(status_code=status.HTTP_204_NO_CONTENT)


@router.get(
    "/test-suites/{test_suite_key}/tests",
    response_model=list[TestGenSuiteTest],
)
def list_suite_tests_endpoint(
    test_suite_key: str,
    client: TestGenClient = Depends(get_testgen_client),
):
    tests = _invoke(lambda: client.list_suite_tests(test_suite_key))
    return [_build_suite_test_response(row) for row in tests]


@router.post(
    "/test-suites/{test_suite_key}/tests",
    response_model=TestGenSuiteTest,
    status_code=status.HTTP_201_CREATED,
)
def create_suite_test(
    test_suite_key: str,
    request: TestDefinitionCreateRequest,
    client: TestGenClient = Depends(get_testgen_client),
    db: Session = Depends(get_db),
):
    context = resolve_table_context(db, request.data_definition_table_id)
    parameters = request.definition or {}

    if not isinstance(parameters, dict):
        raise HTTPException(status.HTTP_400_BAD_REQUEST, detail="definition must be an object")

    column_name = request.column_name or None
    definition_payload = _definition_payload(
        context=context,
        column_name=column_name,
        parameters=parameters,
    )

    test_id = _invoke(
        lambda: client.create_test(
            table_group_id=context.table_group_id,
            test_suite_key=test_suite_key,
            name=request.name,
            rule_type=request.rule_type,
            definition=definition_payload,
        )
    )

    created = _invoke(lambda: client.get_test(test_id))
    if not created:
        raise HTTPException(status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Failed to load created test definition")
    return _build_suite_test_response(created)


@router.put(
    "/tests/{test_id}",
    response_model=TestGenSuiteTest,
)
def update_suite_test(
    test_id: str,
    request: TestDefinitionUpdateRequest,
    client: TestGenClient = Depends(get_testgen_client),
    db: Session = Depends(get_db),
):
    existing = _invoke(lambda: client.get_test(test_id))
    if not existing:
        raise HTTPException(status.HTTP_404_NOT_FOUND, detail="Test definition not found")

    fields_set = request.__fields_set__ or set()
    definition_payload = _parse_definition(existing.get("definition"))
    table_group_id = existing.get("table_group_id")

    update_kwargs: Dict[str, Any] = {}

    if "name" in fields_set and request.name is not None:
        update_kwargs["name"] = request.name

    if "rule_type" in fields_set and request.rule_type is not None:
        update_kwargs["rule_type"] = request.rule_type

    updated_definition = definition_payload.copy()

    if request.definition is not None:
        if not isinstance(request.definition, dict):
            raise HTTPException(status.HTTP_400_BAD_REQUEST, detail="definition must be an object")
        updated_definition["parameters"] = request.definition

    if "column_name" in fields_set:
        updated_definition["columnName"] = request.column_name or None

    if "data_definition_table_id" in fields_set and request.data_definition_table_id is not None:
        context = resolve_table_context(db, request.data_definition_table_id)
        parameters = updated_definition.get("parameters")
        if not isinstance(parameters, dict):
            parameters = {}
        column_name = updated_definition.get("columnName")
        updated_definition = _definition_payload(
            context=context,
            column_name=column_name,
            parameters=parameters,
        )
        table_group_id = context.table_group_id

    if table_group_id:
        update_kwargs["table_group_id"] = table_group_id

    updated_definition["tableGroupId"] = table_group_id

    update_kwargs["definition"] = updated_definition

    _invoke(lambda: client.update_test(test_id, **update_kwargs))

    refreshed = _invoke(lambda: client.get_test(test_id))
    if not refreshed:
        raise HTTPException(status.HTTP_404_NOT_FOUND, detail="Test definition not found")
    return _build_suite_test_response(refreshed)


@router.delete(
    "/tests/{test_id}",
    status_code=status.HTTP_204_NO_CONTENT,
    response_class=Response,
)
def delete_suite_test(test_id: str, client: TestGenClient = Depends(get_testgen_client)) -> Response:
    _invoke(lambda: client.delete_test(test_id))
    return Response(status_code=status.HTTP_204_NO_CONTENT)


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
