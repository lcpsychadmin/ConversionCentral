from sqlalchemy import func, select
from sqlalchemy.orm import Session
from fastapi import APIRouter, Depends

from app.database import get_db
from app.models.entities import (
    PostLoadValidationApproval,
    PostLoadValidationIssue,
    PreLoadValidationApproval,
    PreLoadValidationIssue,
    Project,
    Release,
)

router = APIRouter(prefix="/dashboard", tags=["Dashboard"])


@router.get("/summary")
def get_dashboard_summary(db: Session = Depends(get_db)) -> dict[str, int]:
    projects = db.scalar(select(func.count()).select_from(Project)) or 0
    releases = db.scalar(select(func.count()).select_from(Release)) or 0

    pre_issues = db.scalar(select(func.count()).select_from(PreLoadValidationIssue)) or 0
    post_issues = db.scalar(select(func.count()).select_from(PostLoadValidationIssue)) or 0
    validation_issues = pre_issues + post_issues

    pending_pre = db.scalar(
        select(func.count()).select_from(PreLoadValidationApproval).where(
            PreLoadValidationApproval.decision != "approved"
        )
    ) or 0
    pending_post = db.scalar(
        select(func.count()).select_from(PostLoadValidationApproval).where(
            PostLoadValidationApproval.decision != "approved"
        )
    ) or 0
    pending_approvals = pending_pre + pending_post

    return {
        "projects": projects,
        "releases": releases,
        "validationIssues": validation_issues,
        "pendingApprovals": pending_approvals,
    }
