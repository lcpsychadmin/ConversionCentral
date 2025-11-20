import logging
from typing import Optional, Set
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.orm import Session

from app.database import get_db
from app.ingestion.engine import get_ingestion_connection_params
from app.models import DataObjectDependency, DependencyApproval, TableDependency, User
from app.schemas import (
    DependencyApprovalCreate,
    DependencyApprovalDecision,
    DependencyApprovalRead,
    DependencyApprovalUpdate,
)
from app.services.data_quality_keys import (
    project_keys_for_data_object,
    project_keys_for_table,
)
from app.services.data_quality_testgen import TestGenClient, TestGenClientError

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/dependency-approvals", tags=["Dependency Approvals"])


def _get_approval_or_404(approval_id: UUID, db: Session) -> DependencyApproval:
    approval = db.get(DependencyApproval, approval_id)
    if not approval:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Approval not found")
    return approval


def _ensure_dependency_exists(
    data_object_dependency_id: UUID | None,
    table_dependency_id: UUID | None,
    db: Session,
) -> None:
    if data_object_dependency_id:
        if not db.get(DataObjectDependency, data_object_dependency_id):
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Data object dependency not found")
    if table_dependency_id:
        if not db.get(TableDependency, table_dependency_id):
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Table dependency not found")


def _ensure_user_exists(user_id: UUID, db: Session) -> None:
    if not db.get(User, user_id):
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="User not found")


def _collect_project_keys(
    db: Session,
    *,
    data_object_dependency_id: Optional[UUID] = None,
    table_dependency_id: Optional[UUID] = None,
) -> Set[str]:
    project_keys: set[str] = set()

    if data_object_dependency_id:
        dependency = db.get(DataObjectDependency, data_object_dependency_id)
        if dependency:
            project_keys.update(project_keys_for_data_object(db, dependency.predecessor_id))
            project_keys.update(project_keys_for_data_object(db, dependency.successor_id))

    if table_dependency_id:
        dependency = db.get(TableDependency, table_dependency_id)
        if dependency:
            project_keys.update(project_keys_for_table(db, dependency.predecessor_id))
            project_keys.update(project_keys_for_table(db, dependency.successor_id))

    return project_keys


def _guard_data_quality(project_keys: Set[str]) -> None:
    if not project_keys:
        return

    client = _get_testgen_client()
    if client is None:
        return

    blocking: dict[str, tuple[str | None, ...]] = {}
    try:
        for project_key in project_keys:
            try:
                failures = client.unresolved_failed_test_suites(project_key)
            except TestGenClientError as exc:  # pragma: no cover - defensive guard
                logger.warning(
                    "Unable to check data quality status for project %s: %s",
                    project_key,
                    exc,
                )
                continue
            if failures:
                blocking[project_key] = failures
    finally:
        try:
            client.close()
        except Exception:  # pragma: no cover - defensive guard
            logger.debug("Ignoring TestGen client close error", exc_info=True)

    if not blocking:
        return

    parts: list[str] = []
    for key, suites in blocking.items():
        formatted = ", ".join(suite or "default" for suite in suites)
        parts.append(f"{key} ({formatted})")
    detail = "Data quality validations are failing for " + "; ".join(parts)
    raise HTTPException(status.HTTP_409_CONFLICT, detail=detail)


def _get_testgen_client() -> Optional[TestGenClient]:
    try:
        params = get_ingestion_connection_params()
    except RuntimeError:
        return None

    schema = (getattr(params, "data_quality_schema", "") or "").strip()
    if not schema:
        return None

    try:
        return TestGenClient(params, schema=schema)
    except (ValueError, TestGenClientError):  # pragma: no cover - defensive guard
        logger.warning("Unable to initialize TestGen client for dependency approval gate", exc_info=True)
        return None


def _ensure_quality_gate(
    *,
    decision: Optional[str],
    previous_decision: Optional[str],
    project_keys: Set[str],
) -> None:
    if (
        decision == DependencyApprovalDecision.APPROVED.value
        and previous_decision != DependencyApprovalDecision.APPROVED.value
    ):
        _guard_data_quality(project_keys)


@router.post("", response_model=DependencyApprovalRead, status_code=status.HTTP_201_CREATED)
def create_approval(
    payload: DependencyApprovalCreate, db: Session = Depends(get_db)
) -> DependencyApprovalRead:
    _ensure_dependency_exists(payload.data_object_dependency_id, payload.table_dependency_id, db)
    _ensure_user_exists(payload.approver_id, db)

    project_keys = _collect_project_keys(
        db,
        data_object_dependency_id=payload.data_object_dependency_id,
        table_dependency_id=payload.table_dependency_id,
    )
    decision_value = (
        payload.decision.value if isinstance(payload.decision, DependencyApprovalDecision) else str(payload.decision)
    )
    _ensure_quality_gate(decision=decision_value, previous_decision=None, project_keys=project_keys)

    approval = DependencyApproval(**payload.dict())
    db.add(approval)
    db.commit()
    db.refresh(approval)
    return approval


@router.get("", response_model=list[DependencyApprovalRead])
def list_approvals(db: Session = Depends(get_db)) -> list[DependencyApprovalRead]:
    return db.query(DependencyApproval).all()


@router.get("/{approval_id}", response_model=DependencyApprovalRead)
def get_approval(
    approval_id: UUID, db: Session = Depends(get_db)
) -> DependencyApprovalRead:
    return _get_approval_or_404(approval_id, db)


@router.put("/{approval_id}", response_model=DependencyApprovalRead)
def update_approval(
    approval_id: UUID,
    payload: DependencyApprovalUpdate,
    db: Session = Depends(get_db),
) -> DependencyApprovalRead:
    approval = _get_approval_or_404(approval_id, db)
    update_data = payload.dict(exclude_unset=True)

    data_object_dependency_id = update_data.get("data_object_dependency_id", approval.data_object_dependency_id)
    table_dependency_id = update_data.get("table_dependency_id", approval.table_dependency_id)

    if data_object_dependency_id and table_dependency_id:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Approval can only target one dependency",
        )

    _ensure_dependency_exists(data_object_dependency_id, table_dependency_id, db)

    approver_id = update_data.get("approver_id")
    if approver_id:
        _ensure_user_exists(approver_id, db)

    decision_value = update_data.get("decision")
    if isinstance(decision_value, DependencyApprovalDecision):
        decision_value = decision_value.value

    project_keys = _collect_project_keys(
        db,
        data_object_dependency_id=data_object_dependency_id,
        table_dependency_id=table_dependency_id,
    )
    final_decision = decision_value or approval.decision
    _ensure_quality_gate(
        decision=final_decision,
        previous_decision=approval.decision,
        project_keys=project_keys,
    )

    for field, value in update_data.items():
        if field == "decision" and isinstance(value, DependencyApprovalDecision):
            value = value.value
        setattr(approval, field, value)

    db.commit()
    db.refresh(approval)
    return approval


@router.delete("/{approval_id}", status_code=status.HTTP_204_NO_CONTENT)
def delete_approval(
    approval_id: UUID, db: Session = Depends(get_db)
) -> None:
    approval = _get_approval_or_404(approval_id, db)
    db.delete(approval)
    db.commit()
