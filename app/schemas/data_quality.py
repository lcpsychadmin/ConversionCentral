from __future__ import annotations

from datetime import datetime
from typing import List, Optional

from pydantic import BaseModel, Field


class TestGenProject(BaseModel):
    project_key: str
    name: str
    description: Optional[str] = None
    sql_flavor: Optional[str] = None


class TestGenConnection(BaseModel):
    connection_id: str
    project_key: str
    system_id: Optional[str] = None
    name: str
    catalog: Optional[str] = None
    schema_name: Optional[str] = None
    http_path: Optional[str] = None
    managed_credentials_ref: Optional[str] = None
    is_active: Optional[bool] = None


class TestGenTableGroup(BaseModel):
    table_group_id: str
    connection_id: str
    name: str
    description: Optional[str] = None
    profiling_include_mask: Optional[str] = None
    profiling_exclude_mask: Optional[str] = None


class TestGenTable(BaseModel):
    table_id: str
    table_group_id: str
    schema_name: Optional[str] = None
    table_name: str
    source_table_id: Optional[str] = None


class TestGenProfileRun(BaseModel):
    profile_run_id: str
    table_group_id: str
    status: str
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    row_count: Optional[int] = None
    anomaly_count: Optional[int] = None
    payload_path: Optional[str] = None


class TestGenTestRun(BaseModel):
    test_run_id: str
    test_suite_key: Optional[str] = None
    project_key: str
    status: str
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    duration_ms: Optional[int] = None
    total_tests: Optional[int] = None
    failed_tests: Optional[int] = None
    trigger_source: Optional[str] = None


class TestGenAlert(BaseModel):
    alert_id: str
    source_type: str
    source_ref: str
    severity: str
    title: str
    details: str
    acknowledged: bool
    acknowledged_by: Optional[str] = None
    acknowledged_at: Optional[datetime] = None
    created_at: Optional[datetime] = None


class ProfileAnomalyPayload(BaseModel):
    table_name: str
    column_name: Optional[str] = None
    anomaly_type: str
    severity: str
    description: str
    detected_at: Optional[datetime] = None


class ProfileRunStartRequest(BaseModel):
    table_group_id: str
    status: str = "running"
    started_at: Optional[datetime] = None
    payload_path: Optional[str] = None


class ProfileRunStartResponse(BaseModel):
    profile_run_id: str


class ProfileRunCompleteRequest(BaseModel):
    status: str
    row_count: Optional[int] = None
    anomaly_count: Optional[int] = None
    anomalies: List[ProfileAnomalyPayload] = Field(default_factory=list)


class TestRunStartRequest(BaseModel):
    project_key: str
    test_suite_key: Optional[str] = None
    total_tests: Optional[int] = None
    trigger_source: Optional[str] = None
    status: str = "running"
    started_at: Optional[datetime] = None


class TestRunStartResponse(BaseModel):
    test_run_id: str


class TestRunCompleteRequest(BaseModel):
    status: str
    failed_tests: Optional[int] = None
    duration_ms: Optional[int] = None


class TestResultPayload(BaseModel):
    test_id: str
    table_name: Optional[str] = None
    column_name: Optional[str] = None
    result_status: str
    expected_value: Optional[str] = None
    actual_value: Optional[str] = None
    message: Optional[str] = None
    detected_at: Optional[datetime] = None


class TestResultBatchRequest(BaseModel):
    results: List[TestResultPayload] = Field(default_factory=list)


class AlertCreateRequest(BaseModel):
    source_type: str
    source_ref: str
    severity: str
    title: str
    details: str
    acknowledged: Optional[bool] = None
    acknowledged_by: Optional[str] = None
    acknowledged_at: Optional[datetime] = None
    alert_id: Optional[str] = None


class AlertCreateResponse(BaseModel):
    alert_id: str


class AlertAcknowledgeRequest(BaseModel):
    acknowledged: bool = True
    acknowledged_by: Optional[str] = None
    acknowledged_at: Optional[datetime] = None
