from __future__ import annotations

import json
import logging
import uuid
from collections.abc import Iterable, Mapping, Sequence
from datetime import datetime, timezone
from typing import Any, Callable

from sqlalchemy import func
from sqlalchemy.orm import Session

from app.database import SessionLocal
from app.models.data_quality import (
    DataQualityAlert,
    DataQualityConnection,
    DataQualityDataTableCharacteristic,
    DataQualityProfile,
    DataQualityProfileAnomaly,
    DataQualityProfileAnomalyResult,
    DataQualityProfileColumnValue,
    DataQualityProfileOperation,
    DataQualityProfileResult,
    DataQualityProject,
    DataQualityTable,
    DataQualityTableGroup,
    DataQualityTest,
    DataQualityTestResult,
    DataQualityTestRun,
    DataQualityTestSuite,
)
from app.services.data_quality_testgen import (
    AlertRecord,
    ProfileAnomaly,
    SuiteFailureStatus,
    TestResultRecord,
    TestGenClient,
    TestGenClientError,
    _UNSET,
)

logger = logging.getLogger(__name__)


class LocalTestGenClient:
    """TestGen client that proxies metadata operations to the local database."""

    schema = "local"

    def __init__(self, session_factory: Callable[[], Session] | None = None) -> None:
        self._session_factory = session_factory or SessionLocal
        # Reuse the Databricks client helper methods that only operate on dict payloads.
        self._helper = object.__new__(TestGenClient)

    def close(self) -> None:  # pragma: no cover - parity with databricks client
        return None

    def list_projects(self) -> list[dict[str, Any]]:
        with self._session_factory() as session:
            rows = (
                session.query(DataQualityProject)
                .order_by(DataQualityProject.name.asc().nullslast(), DataQualityProject.project_key.asc())
                .all()
            )
            return [self._project_payload(row) for row in rows]

    def list_connections(self, project_key: str) -> list[dict[str, Any]]:
        with self._session_factory() as session:
            rows = (
                session.query(DataQualityConnection)
                .filter(DataQualityConnection.project_key == project_key)
                .order_by(DataQualityConnection.name.asc().nullslast(), DataQualityConnection.connection_id.asc())
                .all()
            )
            return [self._connection_payload(row) for row in rows]

    def list_table_groups(self, connection_id: str) -> list[dict[str, Any]]:
        with self._session_factory() as session:
            rows = (
                session.query(DataQualityTableGroup)
                .filter(DataQualityTableGroup.connection_id == connection_id)
                .order_by(DataQualityTableGroup.name.asc().nullslast(), DataQualityTableGroup.table_group_id.asc())
                .all()
            )
            return [self._table_group_payload(row) for row in rows]

    def get_table_group_details(self, table_group_id: str) -> dict[str, Any] | None:
        with self._session_factory() as session:
            group = session.get(DataQualityTableGroup, table_group_id)
            if group is None:
                return None
            connection = session.get(DataQualityConnection, group.connection_id)
            if connection is None:
                return None
            payload = self._table_group_payload(group)
            payload.update(
                {
                    "connection_name": connection.name,
                    "project_key": connection.project_key,
                    "catalog": connection.catalog,
                    "schema_name": connection.schema_name,
                    "http_path": connection.http_path,
                    "managed_credentials_ref": connection.managed_credentials_ref,
                    "is_active": connection.is_active,
                }
            )
            return payload

    def list_tables(self, table_group_id: str) -> list[dict[str, Any]]:
        with self._session_factory() as session:
            rows = (
                session.query(DataQualityTable)
                .filter(DataQualityTable.table_group_id == table_group_id)
                .order_by(DataQualityTable.table_name.asc().nullslast(), DataQualityTable.table_id.asc())
                .all()
            )
            return [self._table_payload(row) for row in rows]

    def list_table_groups_with_connections(self) -> list[dict[str, Any]]:
        with self._session_factory() as session:
            rows = (
                session.query(DataQualityTableGroup, DataQualityConnection)
                .join(DataQualityConnection, DataQualityConnection.connection_id == DataQualityTableGroup.connection_id, isouter=True)
                .order_by(DataQualityTableGroup.name.asc().nullslast(), DataQualityTableGroup.table_group_id.asc())
                .all()
            )
            result: list[dict[str, Any]] = []
            for group, connection in rows:
                payload = self._table_group_payload(group)
                if connection is not None:
                    payload.update(
                        {
                            "connection_name": connection.name,
                            "catalog": connection.catalog,
                            "schema_name": connection.schema_name,
                            "project_key": connection.project_key,
                        }
                    )
                result.append(payload)
            return result

    def _project_payload(self, project: DataQualityProject) -> dict[str, Any]:
        return {
            "project_key": project.project_key,
            "name": project.name,
            "description": project.description,
            "sql_flavor": project.sql_flavor,
        }

    def _connection_payload(self, connection: DataQualityConnection) -> dict[str, Any]:
        return {
            "connection_id": connection.connection_id,
            "project_key": connection.project_key,
            "name": connection.name,
            "catalog": connection.catalog,
            "schema_name": connection.schema_name,
            "http_path": connection.http_path,
            "managed_credentials_ref": connection.managed_credentials_ref,
            "is_active": connection.is_active,
        }

    def _table_group_payload(self, group: DataQualityTableGroup) -> dict[str, Any]:
        return {
            "table_group_id": group.table_group_id,
            "connection_id": group.connection_id,
            "name": group.name,
            "description": group.description,
            "profiling_include_mask": group.profiling_include_mask,
            "profiling_exclude_mask": group.profiling_exclude_mask,
            "profiling_job_id": group.profiling_job_id,
        }

    def _table_payload(self, table: DataQualityTable) -> dict[str, Any]:
        return {
            "table_id": table.table_id,
            "table_group_id": table.table_group_id,
            "schema_name": table.schema_name,
            "table_name": table.table_name,
            "source_table_id": table.source_table_id,
        }

    def fetch_table_characteristics(
        self,
        *,
        table_ids: Sequence[str] | None = None,
        table_group_ids: Sequence[str] | None = None,
    ) -> list[dict[str, Any]]:
        with self._session_factory() as session:
            query = session.query(DataQualityDataTableCharacteristic)
            if table_ids:
                query = query.filter(DataQualityDataTableCharacteristic.table_id.in_(tuple(table_ids)))
            if table_group_ids:
                query = query.filter(DataQualityDataTableCharacteristic.table_group_id.in_(tuple(table_group_ids)))
            rows = query.all()
            return [
                {
                    "table_id": row.table_id,
                    "table_group_id": row.table_group_id,
                    "schema_name": row.schema_name,
                    "table_name": row.table_name,
                    "record_count": row.record_count,
                    "column_count": row.column_count,
                    "latest_anomaly_ct": row.latest_anomaly_ct,
                    "dq_score_profiling": row.dq_score_profiling,
                    "latest_run_completed_at": row.latest_run_completed_at,
                }
                for row in rows
            ]

    def list_profile_runs_overview(
        self,
        *,
        table_group_id: str | None = None,
        limit: int = 50,
    ) -> list[dict[str, Any]]:
        with self._session_factory() as session:
            query = (
                session.query(
                    DataQualityProfile,
                    DataQualityTableGroup,
                    DataQualityConnection,
                )
                .join(DataQualityTableGroup, DataQualityTableGroup.table_group_id == DataQualityProfile.table_group_id, isouter=True)
                .join(DataQualityConnection, DataQualityConnection.connection_id == DataQualityTableGroup.connection_id, isouter=True)
            )
            if table_group_id:
                query = query.filter(DataQualityProfile.table_group_id == table_group_id)
            rows = (
                query.order_by(
                    func.coalesce(DataQualityProfile.completed_at, DataQualityProfile.started_at).desc(),
                    DataQualityProfile.started_at.desc(),
                )
                .limit(limit)
                .all()
            )
            result: list[dict[str, Any]] = []
            for profile, group, connection in rows:
                payload = self._profile_payload(profile)
                if group is not None:
                    payload.update(
                        {
                            "table_group_name": group.name,
                            "table_group_description": group.description,
                            "connection_id": group.connection_id,
                        }
                    )
                if connection is not None:
                    payload.update(
                        {
                            "connection_name": connection.name,
                            "catalog": connection.catalog,
                            "schema_name": connection.schema_name,
                            "project_key": connection.project_key,
                        }
                    )
                result.append(payload)
            return result

    def recent_profile_runs(self, table_group_id: str, *, limit: int = 20) -> list[dict[str, Any]]:
        with self._session_factory() as session:
            rows = (
                session.query(DataQualityProfile)
                .filter(DataQualityProfile.table_group_id == table_group_id)
                .order_by(DataQualityProfile.started_at.desc(), DataQualityProfile.profile_run_id.desc())
                .limit(limit)
                .all()
            )
            return [self._profile_payload(row) for row in rows]

    def profile_run_anomaly_counts(self, profile_run_ids: Sequence[str]) -> dict[str, dict[str, int]]:
        normalized = [run_id for run_id in profile_run_ids if run_id]
        if not normalized:
            return {}
        with self._session_factory() as session:
            rows = (
                session.query(
                    DataQualityProfileAnomaly.profile_run_id,
                    DataQualityProfileAnomaly.severity,
                    func.count(DataQualityProfileAnomaly.id),
                )
                .filter(DataQualityProfileAnomaly.profile_run_id.in_(tuple(normalized)))
                .group_by(DataQualityProfileAnomaly.profile_run_id, DataQualityProfileAnomaly.severity)
                .all()
            )
            buckets: dict[str, dict[str, int]] = {}
            for run_id, severity, count in rows:
                if not run_id:
                    continue
                bucket = buckets.setdefault(run_id, {})
                bucket[str(severity or "unknown")] = int(count or 0)
            return buckets

    def _profile_payload(self, profile: DataQualityProfile) -> dict[str, Any]:
        return {
            "profile_run_id": profile.profile_run_id,
            "table_group_id": profile.table_group_id,
            "status": profile.status,
            "started_at": profile.started_at,
            "completed_at": profile.completed_at,
            "row_count": profile.row_count,
            "anomaly_count": profile.anomaly_count,
            "dq_score_profiling": profile.dq_score_profiling,
            "databricks_run_id": profile.databricks_run_id,
        }

    def list_test_suites(
        self,
        *,
        project_key: str | None = None,
        data_object_id: str | None = None,
    ) -> list[dict[str, Any]]:
        with self._session_factory() as session:
            query = session.query(DataQualityTestSuite)
            if project_key:
                query = query.filter(DataQualityTestSuite.project_key == project_key)
            if data_object_id:
                query = query.filter(DataQualityTestSuite.data_object_id == data_object_id)
            rows = query.order_by(DataQualityTestSuite.name.asc().nullslast(), DataQualityTestSuite.test_suite_key.asc()).all()
            return [self._test_suite_payload(row) for row in rows]

    def get_test_suite(self, test_suite_key: str) -> dict[str, Any] | None:
        with self._session_factory() as session:
            suite = session.get(DataQualityTestSuite, test_suite_key)
            if suite is None:
                return None
            return self._test_suite_payload(suite)

    def create_test_suite(
        self,
        *,
        project_key: str | None,
        name: str,
        description: str | None,
        severity: str | None,
        product_team_id: str | None,
        application_id: str | None,
        data_object_id: str | None,
        data_definition_id: str | None,
    ) -> str:
        suite = DataQualityTestSuite(
            test_suite_key=str(uuid.uuid4()),
            project_key=project_key,
            name=name,
            description=description,
            severity=severity,
            product_team_id=product_team_id,
            application_id=application_id,
            data_object_id=data_object_id,
            data_definition_id=data_definition_id,
            created_at=datetime.now(timezone.utc),
            updated_at=datetime.now(timezone.utc),
        )
        with self._session_factory() as session:
            session.add(suite)
            session.commit()
        return suite.test_suite_key

    def update_test_suite(
        self,
        test_suite_key: str,
        *,
        project_key: Any = _UNSET,
        name: Any = _UNSET,
        description: Any = _UNSET,
        severity: Any = _UNSET,
        product_team_id: Any = _UNSET,
        application_id: Any = _UNSET,
        data_object_id: Any = _UNSET,
        data_definition_id: Any = _UNSET,
    ) -> None:
        with self._session_factory() as session:
            suite = session.get(DataQualityTestSuite, test_suite_key)
            if suite is None:
                raise TestGenClientError("Test suite not found")
            updates = {
                "project_key": project_key,
                "name": name,
                "description": description,
                "severity": severity,
                "product_team_id": product_team_id,
                "application_id": application_id,
                "data_object_id": data_object_id,
                "data_definition_id": data_definition_id,
            }
            for field, value in updates.items():
                if value is _UNSET:
                    continue
                setattr(suite, field, value)
            suite.updated_at = datetime.now(timezone.utc)
            session.add(suite)
            session.commit()

    def delete_test_suite(self, test_suite_key: str) -> None:
        with self._session_factory() as session:
            suite = session.get(DataQualityTestSuite, test_suite_key)
            if suite is None:
                return
            session.delete(suite)
            session.commit()

    def list_suite_tests(self, test_suite_key: str) -> list[dict[str, Any]]:
        with self._session_factory() as session:
            rows = (
                session.query(DataQualityTest)
                .filter(DataQualityTest.test_suite_key == test_suite_key)
                .order_by(DataQualityTest.name.asc().nullslast(), DataQualityTest.test_id.asc())
                .all()
            )
            return [self._test_payload(row) for row in rows]

    def get_test(self, test_id: str) -> dict[str, Any] | None:
        with self._session_factory() as session:
            test = session.get(DataQualityTest, test_id)
            if test is None:
                return None
            return self._test_payload(test)

    def create_test(
        self,
        *,
        table_group_id: str,
        test_suite_key: str,
        name: str,
        rule_type: str,
        definition: dict[str, Any] | str | None = None,
    ) -> str:
        payload = definition if isinstance(definition, str) else json.dumps(definition or {})
        record = DataQualityTest(
            test_id=str(uuid.uuid4()),
            table_group_id=table_group_id,
            test_suite_key=test_suite_key,
            name=name,
            rule_type=rule_type,
            definition=payload,
            created_at=datetime.now(timezone.utc),
            updated_at=datetime.now(timezone.utc),
        )
        with self._session_factory() as session:
            session.add(record)
            session.commit()
        return record.test_id

    def start_test_run(
        self,
        *,
        project_key: str,
        test_suite_key: str | None = None,
        total_tests: int | None = None,
        trigger_source: str | None = None,
        status: str = "running",
        started_at: datetime | None = None,
    ) -> str:
        record = DataQualityTestRun(
            test_run_id=str(uuid.uuid4()),
            project_key=project_key,
            test_suite_key=test_suite_key,
            status=status,
            started_at=started_at or datetime.now(timezone.utc),
            total_tests=total_tests,
            trigger_source=trigger_source,
        )
        with self._session_factory() as session:
            session.add(record)
            session.commit()
        return record.test_run_id

    def complete_test_run(
        self,
        test_run_id: str,
        *,
        status: str,
        failed_tests: int | None = None,
        duration_ms: int | None = None,
    ) -> None:
        with self._session_factory() as session:
            record = session.get(DataQualityTestRun, test_run_id)
            if record is None:
                raise TestGenClientError("Test run not found")
            record.status = status
            record.completed_at = datetime.now(timezone.utc)
            record.failed_tests = failed_tests
            record.duration_ms = duration_ms
            session.add(record)
            session.commit()

    def record_test_results(self, test_run_id: str, results: Iterable[TestResultRecord]) -> None:
        payload = tuple(results)
        if not payload:
            return
        with self._session_factory() as session:
            for entry in payload:
                session.add(
                    DataQualityTestResult(
                        test_run_id=test_run_id,
                        test_id=entry.test_id,
                        table_name=entry.table_name,
                        column_name=entry.column_name,
                        result_status=entry.result_status,
                        expected_value=entry.expected_value,
                        actual_value=entry.actual_value,
                        message=entry.message,
                        detected_at=entry.detected_at or datetime.now(timezone.utc),
                    )
                )
            session.commit()

    def recent_test_runs(self, project_key: str, *, limit: int = 20) -> list[dict[str, Any]]:
        with self._session_factory() as session:
            rows = (
                session.query(DataQualityTestRun)
                .filter(DataQualityTestRun.project_key == project_key)
                .order_by(DataQualityTestRun.started_at.desc(), DataQualityTestRun.test_run_id.desc())
                .limit(limit)
                .all()
            )
            return [
                {
                    "test_run_id": row.test_run_id,
                    "test_suite_key": row.test_suite_key,
                    "project_key": row.project_key,
                    "status": row.status,
                    "started_at": row.started_at,
                    "completed_at": row.completed_at,
                    "duration_ms": row.duration_ms,
                    "total_tests": row.total_tests,
                    "failed_tests": row.failed_tests,
                    "trigger_source": row.trigger_source,
                }
                for row in rows
            ]

    def recent_alerts(self, *, limit: int = 50, include_acknowledged: bool = False) -> list[dict[str, Any]]:
        with self._session_factory() as session:
            query = session.query(DataQualityAlert)
            if not include_acknowledged:
                query = query.filter(DataQualityAlert.acknowledged.is_(False))
            rows = (
                query.order_by(DataQualityAlert.created_at.desc(), DataQualityAlert.alert_id.desc())
                .limit(limit)
                .all()
            )
            return [
                {
                    "alert_id": row.alert_id,
                    "source_type": row.source_type,
                    "source_ref": row.source_ref,
                    "severity": row.severity,
                    "title": row.title,
                    "details": row.details,
                    "acknowledged": row.acknowledged,
                    "acknowledged_by": row.acknowledged_by,
                    "acknowledged_at": row.acknowledged_at,
                    "created_at": row.created_at,
                }
                for row in rows
            ]

    def create_alert(self, alert: AlertRecord) -> str:
        record = DataQualityAlert(
            alert_id=alert.alert_id or str(uuid.uuid4()),
            source_type=alert.source_type,
            source_ref=alert.source_ref,
            severity=alert.severity,
            title=alert.title,
            details=alert.details,
            acknowledged=bool(alert.acknowledged),
            acknowledged_by=alert.acknowledged_by,
            acknowledged_at=alert.acknowledged_at,
        )
        with self._session_factory() as session:
            session.add(record)
            session.commit()
        return record.alert_id

    def acknowledge_alert(
        self,
        alert_id: str,
        *,
        acknowledged: bool = True,
        acknowledged_by: str | None = None,
        acknowledged_at: datetime | None = None,
    ) -> None:
        with self._session_factory() as session:
            record = session.get(DataQualityAlert, alert_id)
            if record is None:
                raise TestGenClientError("Alert not found")
            record.acknowledged = bool(acknowledged)
            record.acknowledged_by = acknowledged_by
            record.acknowledged_at = acknowledged_at or (datetime.now(timezone.utc) if acknowledged else None)
            session.add(record)
            session.commit()

    def delete_alert(self, alert_id: str) -> None:
        with self._session_factory() as session:
            record = session.get(DataQualityAlert, alert_id)
            if record is None:
                return
            session.delete(record)
            session.commit()

    def delete_alert_by_source(self, *, source_type: str, source_ref: str) -> None:
        with self._session_factory() as session:
            session.query(DataQualityAlert).filter(
                DataQualityAlert.source_type == source_type,
                DataQualityAlert.source_ref == source_ref,
            ).delete(synchronize_session=False)
            session.commit()

    def suite_failure_statuses(self, project_key: str) -> tuple[SuiteFailureStatus, ...]:
        with self._session_factory() as session:
            rows = (
                session.query(DataQualityTestRun)
                .filter(DataQualityTestRun.project_key == project_key)
                .order_by(
                    DataQualityTestRun.test_suite_key.asc().nullslast(),
                    func.coalesce(DataQualityTestRun.completed_at, DataQualityTestRun.started_at).desc(),
                )
                .all()
            )
            latest: dict[str | None, DataQualityTestRun] = {}
            for row in rows:
                key = row.test_suite_key or "__default__"
                if key not in latest:
                    latest[key] = row
            result: list[SuiteFailureStatus] = []
            for row in latest.values():
                result.append(
                    SuiteFailureStatus(
                        test_suite_key=row.test_suite_key,
                        status=row.status or "unknown",
                        acknowledged=False,
                        acknowledged_by=None,
                        acknowledged_at=None,
                    )
                )
            return tuple(result)

    def unresolved_failed_test_suites(self, project_key: str) -> tuple[str | None, ...]:
        with self._session_factory() as session:
            rows = (
                session.query(DataQualityTestRun)
                .filter(DataQualityTestRun.project_key == project_key)
                .order_by(
                    DataQualityTestRun.test_suite_key.asc().nullslast(),
                    func.coalesce(DataQualityTestRun.completed_at, DataQualityTestRun.started_at).desc(),
                )
                .all()
            )
            unresolved: list[str | None] = []
            seen: set[str | None] = set()
            for row in rows:
                key = row.test_suite_key or "__default__"
                if key in seen:
                    continue
                seen.add(key)
                if (row.status or "").lower() == "failed":
                    unresolved.append(row.test_suite_key)
            return tuple(unresolved)

    def update_test(
        self,
        test_id: str,
        *,
        table_group_id: Any = _UNSET,
        test_suite_key: Any = _UNSET,
        name: Any = _UNSET,
        rule_type: Any = _UNSET,
        definition: Any = _UNSET,
    ) -> None:
        with self._session_factory() as session:
            record = session.get(DataQualityTest, test_id)
            if record is None:
                raise TestGenClientError("Test definition not found")
            updates = {
                "table_group_id": table_group_id,
                "test_suite_key": test_suite_key,
                "name": name,
                "rule_type": rule_type,
            }
            for field, value in updates.items():
                if value is _UNSET:
                    continue
                setattr(record, field, value)
            if definition is not _UNSET:
                record.definition = definition if isinstance(definition, str) else json.dumps(definition or {})
            record.updated_at = datetime.now(timezone.utc)
            session.add(record)
            session.commit()

    def delete_test(self, test_id: str) -> None:
        with self._session_factory() as session:
            record = session.get(DataQualityTest, test_id)
            if record is None:
                return
            session.delete(record)
            session.commit()

    def _test_suite_payload(self, suite: DataQualityTestSuite) -> dict[str, Any]:
        return {
            "test_suite_key": suite.test_suite_key,
            "project_key": suite.project_key,
            "name": suite.name,
            "description": suite.description,
            "severity": suite.severity,
            "product_team_id": suite.product_team_id,
            "application_id": suite.application_id,
            "data_object_id": suite.data_object_id,
            "data_definition_id": suite.data_definition_id,
            "created_at": suite.created_at,
            "updated_at": suite.updated_at,
        }

    def _test_payload(self, test: DataQualityTest) -> dict[str, Any]:
        return {
            "test_id": test.test_id,
            "table_group_id": test.table_group_id,
            "test_suite_key": test.test_suite_key,
            "name": test.name,
            "rule_type": test.rule_type,
            "definition": test.definition,
            "created_at": test.created_at,
            "updated_at": test.updated_at,
        }

    def column_profile(
        self,
        table_group_id: str,
        *,
        column_name: str,
        table_name: str | None = None,
        physical_name: str | None = None,
    ) -> dict[str, Any] | None:
        normalized_column = (column_name or "").strip()
        if not normalized_column:
            raise ValueError("column_name is required to build a column profile")

        with self._session_factory() as session:
            run = self._latest_completed_profile_run(session, table_group_id)
            if run is None:
                return None

            table_catalog = self._fetch_table_catalog(session, table_group_id)
            result_rows = self._fetch_profile_result_rows(session, run.profile_run_id)
            value_rows = self._fetch_profile_value_rows(session, run.profile_run_id)
            anomaly_rows = self._fetch_profile_anomaly_rows(session, run.profile_run_id)
            tables = self._build_tables_payload(table_group_id, table_catalog, result_rows, value_rows, anomaly_rows)

            match = self._locate_column_payload(tables, normalized_column, table_name, physical_name)
            if match is None:
                return None

            table_entry, column_entry = match
            metrics = self._helper._build_column_metrics(column_entry)
            top_values = self._helper._build_top_values(column_entry)
            histogram = self._helper._build_histogram(column_entry)

            return {
                "table_group_id": table_group_id,
                "profile_run_id": run.profile_run_id,
                "status": run.status,
                "started_at": run.started_at,
                "completed_at": run.completed_at,
                "row_count": run.row_count,
                "table_name": self._helper._extract_table_name(table_entry) or table_name or physical_name,
                "column_name": self._helper._extract_column_name(column_entry) or normalized_column,
                "data_type": self._helper._extract_data_type(column_entry),
                "metrics": metrics,
                "top_values": top_values,
                "histogram": histogram,
                "anomalies": column_entry.get("anomalies", []),
            }

    def export_profiling_payload(
        self,
        table_group_id: str,
        *,
        profile_run_id: str | None = None,
    ) -> dict[str, Any] | None:
        normalized = (table_group_id or "").strip()
        if not normalized:
            raise ValueError("table_group_id is required to export a profiling payload")

        with self._session_factory() as session:
            run = self._resolve_profile_run(session, normalized, profile_run_id)
            if run is None:
                return None

            table_catalog = self._fetch_table_catalog(session, normalized)
            result_rows = self._fetch_profile_result_rows(session, run.profile_run_id)
            value_rows = self._fetch_profile_value_rows(session, run.profile_run_id)
            anomaly_rows = self._fetch_profile_anomaly_rows(session, run.profile_run_id)
            tables = self._build_tables_payload(normalized, table_catalog, result_rows, value_rows, anomaly_rows)
            if not tables:
                return None

            summary = self._build_profile_summary_payload(run)
            return {
                "table_group_id": normalized,
                "profile_run_id": run.profile_run_id,
                "summary": summary,
                "tables": tables,
            }

    def list_profile_run_anomalies(self, profile_run_id: str) -> list[dict[str, Any]]:
        with self._session_factory() as session:
            rows = self._fetch_profile_anomaly_rows(session, profile_run_id)
            return rows

    def delete_profile_runs(self, profile_run_ids: Sequence[str]) -> int:
        normalized = tuple(sorted({run_id for run_id in profile_run_ids if run_id}))
        if not normalized:
            return 0
        with self._session_factory() as session:
            deleted = 0
            for model in (
                DataQualityProfileAnomalyResult,
                DataQualityProfileAnomaly,
                DataQualityProfileResult,
                DataQualityProfileColumnValue,
                DataQualityProfileOperation,
            ):
                session.query(model).filter(model.profile_run_id.in_(normalized)).delete(synchronize_session=False)
            deleted = (
                session.query(DataQualityProfile)
                .filter(DataQualityProfile.profile_run_id.in_(normalized))
                .delete(synchronize_session=False)
            )
            session.commit()
            return int(deleted or 0)

    def _latest_completed_profile_run(self, session: Session, table_group_id: str) -> DataQualityProfile | None:
        return (
            session.query(DataQualityProfile)
            .filter(DataQualityProfile.table_group_id == table_group_id)
            .filter(
                func.coalesce(DataQualityProfile.completed_at, DataQualityProfile.started_at).isnot(None)
            )
            .order_by(
                func.coalesce(DataQualityProfile.completed_at, DataQualityProfile.started_at).desc(),
                DataQualityProfile.started_at.desc(),
            )
            .first()
        )

    def _resolve_profile_run(
        self,
        session: Session,
        table_group_id: str,
        profile_run_id: str | None,
    ) -> DataQualityProfile | None:
        if profile_run_id:
            row = (
                session.query(DataQualityProfile)
                .filter(DataQualityProfile.profile_run_id == profile_run_id)
                .filter(DataQualityProfile.table_group_id == table_group_id)
                .one_or_none()
            )
            if row:
                return row
        return self._latest_completed_profile_run(session, table_group_id)

    def _fetch_profile_result_rows(self, session: Session, profile_run_id: str) -> list[dict[str, Any]]:
        rows = (
            session.query(DataQualityProfileResult)
            .filter(DataQualityProfileResult.profile_run_id == profile_run_id)
            .order_by(DataQualityProfileResult.schema_name.asc().nullslast(), DataQualityProfileResult.table_name.asc(), DataQualityProfileResult.column_name.asc())
            .all()
        )
        return [
            {
                "result_id": row.result_id,
                "profile_run_id": row.profile_run_id,
                "table_id": row.table_id,
                "column_id": row.column_id,
                "schema_name": row.schema_name,
                "table_name": row.table_name,
                "column_name": row.column_name,
                "data_type": row.data_type,
                "general_type": row.general_type,
                "record_count": row.record_count,
                "null_count": row.null_count,
                "distinct_count": row.distinct_count,
                "min_value": row.min_value,
                "max_value": row.max_value,
                "avg_value": row.avg_value,
                "stddev_value": row.stddev_value,
                "percentiles_json": row.percentiles_json,
                "top_values_json": row.top_values_json,
                "metrics_json": row.metrics_json,
            }
            for row in rows
        ]

    def _fetch_profile_value_rows(self, session: Session, profile_run_id: str) -> list[dict[str, Any]]:
        rows = (
            session.query(DataQualityProfileColumnValue)
            .filter(DataQualityProfileColumnValue.profile_run_id == profile_run_id)
            .order_by(
                DataQualityProfileColumnValue.schema_name.asc().nullslast(),
                DataQualityProfileColumnValue.table_name.asc(),
                DataQualityProfileColumnValue.column_name.asc(),
                DataQualityProfileColumnValue.rank.asc().nullslast(),
            )
            .all()
        )
        return [
            {
                "profile_run_id": row.profile_run_id,
                "schema_name": row.schema_name,
                "table_name": row.table_name,
                "column_name": row.column_name,
                "value": row.value,
                "frequency": row.frequency,
                "relative_freq": row.relative_freq,
                "rank": row.rank,
                "bucket_label": row.bucket_label,
                "bucket_lower_bound": row.bucket_lower_bound,
                "bucket_upper_bound": row.bucket_upper_bound,
            }
            for row in rows
        ]

    def _fetch_profile_anomaly_rows(self, session: Session, profile_run_id: str) -> list[dict[str, Any]]:
        results = (
            session.query(DataQualityProfileAnomalyResult)
            .filter(DataQualityProfileAnomalyResult.profile_run_id == profile_run_id)
            .all()
        )
        if results:
            return [
                {
                    "profile_run_id": row.profile_run_id,
                    "table_id": row.table_id,
                    "column_id": row.column_id,
                    "table_name": row.table_name,
                    "column_name": row.column_name,
                    "anomaly_type_id": row.anomaly_type_id,
                    "severity": row.severity,
                    "likelihood": row.likelihood,
                    "detail": row.detail,
                    "pii_risk": row.pii_risk,
                    "dq_dimension": row.dq_dimension,
                    "detected_at": row.detected_at,
                }
                for row in results
            ]
        legacy = (
            session.query(DataQualityProfileAnomaly)
            .filter(DataQualityProfileAnomaly.profile_run_id == profile_run_id)
            .all()
        )
        return [
            {
                "profile_run_id": row.profile_run_id,
                "table_id": None,
                "column_id": None,
                "table_name": row.table_name,
                "column_name": row.column_name,
                "anomaly_type_id": row.anomaly_type,
                "severity": row.severity,
                "likelihood": None,
                "detail": row.description,
                "pii_risk": None,
                "dq_dimension": None,
                "detected_at": row.detected_at,
            }
            for row in legacy
        ]

    def _fetch_table_catalog(self, session: Session, table_group_id: str) -> dict[str, dict[str, Mapping[str, Any]]]:
        rows = (
            session.query(DataQualityTable)
            .filter(DataQualityTable.table_group_id == table_group_id)
            .all()
        )
        by_id: dict[str, Mapping[str, Any]] = {}
        by_name: dict[str, Mapping[str, Any]] = {}
        for row in rows:
            payload = {
                "table_id": row.table_id,
                "table_group_id": row.table_group_id,
                "schema_name": row.schema_name,
                "table_name": row.table_name,
            }
            by_id[row.table_id] = payload
            normalized = self._normalize_table_name(row.schema_name, row.table_name)
            if normalized:
                by_name[normalized] = payload
        return {"by_id": by_id, "by_name": by_name}

    def _build_tables_payload(
        self,
        table_group_id: str,
        table_catalog: Mapping[str, Mapping[str, Any]],
        result_rows: Sequence[Mapping[str, Any]],
        value_rows: Sequence[Mapping[str, Any]],
        anomaly_rows: Sequence[Mapping[str, Any]],
    ) -> list[dict[str, Any]]:
        helper = self._helper
        grouped_values = helper._group_value_rows(value_rows)
        column_anomalies, table_anomalies = helper._group_anomaly_rows(anomaly_rows)
        tables: dict[tuple[str, str], dict[str, Any]] = {}

        for row in result_rows:
            table_entry = helper._ensure_table_entry(tables, row, table_catalog, table_group_id)
            if table_entry is None:
                continue
            column_entry = helper._build_column_entry(row, grouped_values, column_anomalies)
            if column_entry is None:
                continue
            table_entry.setdefault("columns", []).append(column_entry)
            row_count = column_entry.get("row_count")
            if row_count is not None and table_entry.get("row_count") is None:
                table_entry["row_count"] = row_count
                table_entry.setdefault("metrics", {})["row_count"] = row_count

        for key, anomalies in table_anomalies.items():
            table_entry = tables.get(key)
            if table_entry is None:
                metadata = helper._lookup_table_metadata(key, table_catalog)
                if not metadata:
                    continue
                schema_name, table_name = helper._split_schema_table(
                    metadata.get("schema_name"), metadata.get("table_name")
                )
                table_entry = {
                    "table_id": metadata.get("table_id"),
                    "table_group_id": table_group_id,
                    "schema_name": schema_name,
                    "table_name": table_name,
                    "columns": [],
                    "anomalies": [],
                }
                tables[key] = table_entry
            table_entry["anomalies"] = anomalies

        for table_entry in tables.values():
            table_entry.setdefault("columns", [])
            table_entry.setdefault("anomalies", [])
            metrics = table_entry.setdefault("metrics", {})
            metrics["column_count"] = len(table_entry["columns"])

        return list(tables.values())

    def _build_profile_summary_payload(self, row: DataQualityProfile) -> dict[str, Any]:
        summary: dict[str, Any] = {
            "profile_run_id": row.profile_run_id,
            "table_group_id": row.table_group_id,
            "status": row.status,
            "started_at": self._format_timestamp(row.started_at),
            "completed_at": self._format_timestamp(row.completed_at),
            "databricks_run_id": row.databricks_run_id,
        }
        if row.row_count is not None:
            summary["row_count"] = row.row_count
        if row.anomaly_count is not None:
            summary["anomaly_count"] = row.anomaly_count
        return summary

    @staticmethod
    def _format_timestamp(value: datetime | None) -> str | None:
        if value is None:
            return None
        if value.tzinfo is None:
            return value.replace(tzinfo=timezone.utc).isoformat()
        return value.astimezone(timezone.utc).isoformat()

    def _locate_column_payload(
        self,
        tables: Sequence[Mapping[str, Any]],
        column_name: str,
        table_name: str | None,
        physical_name: str | None,
    ) -> tuple[Mapping[str, Any], Mapping[str, Any]] | None:
        normalized_column = column_name.strip().lower()
        normalized_tables: set[str] = set()
        if table_name:
            normalized_tables.add(self._normalize_table_name(None, table_name))
        if physical_name:
            normalized_tables.add(self._normalize_table_name(None, physical_name))

        for table in tables:
            table_normalized = self._normalize_table_name(table.get("schema_name"), table.get("table_name"))
            if normalized_tables and table_normalized not in normalized_tables:
                continue
            for column in table.get("columns", []):
                current = (column.get("column_name") or "").strip().lower()
                if current == normalized_column:
                    return table, column
        return None

    @staticmethod
    def _normalize_table_name(schema_name: Any, table_name: Any) -> str:
        schema, table = TestGenClient._split_schema_table(schema_name, table_name)
        normalized_table = (table or "").strip().lower()
        if not normalized_table:
            return ""
        normalized_schema = (schema or "").strip().lower()
        return f"{normalized_schema}.{normalized_table}" if normalized_schema else normalized_table

    def start_profile_run(
        self,
        table_group_id: str,
        *,
        status: str = "running",
        started_at: datetime | None = None,
    ) -> str:
        record = DataQualityProfile(
            profile_run_id=str(uuid.uuid4()),
            table_group_id=table_group_id,
            status=status,
            started_at=started_at or datetime.now(timezone.utc),
        )
        with self._session_factory() as session:
            session.add(record)
            session.flush()
            profile_run_id = record.profile_run_id
            session.commit()
        return profile_run_id

    def update_table_group_profiling_job(self, table_group_id: str, profiling_job_id: str | None) -> None:
        with self._session_factory() as session:
            group = session.get(DataQualityTableGroup, table_group_id)
            if group is None:
                raise TestGenClientError("Table group not found")
            group.profiling_job_id = profiling_job_id
            session.add(group)
            session.commit()

    def update_profile_run_databricks_run(
        self,
        profile_run_id: str,
        *,
        databricks_run_id: str | None,
    ) -> None:
        with self._session_factory() as session:
            record = session.get(DataQualityProfile, profile_run_id)
            if record is None:
                raise TestGenClientError("Profile run not found")
            record.databricks_run_id = databricks_run_id
            session.add(record)
            session.commit()

    def complete_profile_run(
        self,
        profile_run_id: str,
        *,
        status: str,
        row_count: int | None = None,
        anomaly_count: int | None = None,
        anomalies: Sequence[ProfileAnomaly] | None = None,
    ) -> None:
        anomalies = tuple(anomalies or ())
        with self._session_factory() as session:
            record = session.get(DataQualityProfile, profile_run_id)
            if record is None:
                raise TestGenClientError("Profile run not found")
            record.status = status
            record.completed_at = datetime.now(timezone.utc)
            record.row_count = row_count
            record.anomaly_count = anomaly_count if anomaly_count is not None else len(anomalies)
            session.add(record)
            if anomalies:
                for entry in anomalies:
                    session.add(
                        DataQualityProfileAnomaly(
                            profile_run_id=profile_run_id,
                            table_name=entry.table_name,
                            column_name=entry.column_name,
                            anomaly_type=entry.anomaly_type,
                            severity=entry.severity,
                            description=entry.description,
                            detected_at=entry.detected_at or datetime.now(timezone.utc),
                        )
                    )
            session.commit()
