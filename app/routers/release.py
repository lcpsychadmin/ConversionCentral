from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.orm import Session

from app.database import get_db
from app.models import Project, Release
from app.schemas import ReleaseCreate, ReleaseRead, ReleaseUpdate

router = APIRouter(prefix="/releases", tags=["Releases"])


def _get_release_or_404(release_id: UUID, db: Session) -> Release:
    release = db.get(Release, release_id)
    if not release:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Release not found")
    return release


@router.post("", response_model=ReleaseRead, status_code=status.HTTP_201_CREATED)
def create_release(payload: ReleaseCreate, db: Session = Depends(get_db)) -> ReleaseRead:
    if not db.get(Project, payload.project_id):
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Project not found")

    release = Release(**payload.dict())
    db.add(release)
    db.commit()
    db.refresh(release)
    return release


@router.get("", response_model=list[ReleaseRead])
def list_releases(db: Session = Depends(get_db)) -> list[ReleaseRead]:
    return db.query(Release).all()


@router.get("/{release_id}", response_model=ReleaseRead)
def get_release(release_id: UUID, db: Session = Depends(get_db)) -> ReleaseRead:
    return _get_release_or_404(release_id, db)


@router.put("/{release_id}", response_model=ReleaseRead)
def update_release(
    release_id: UUID, payload: ReleaseUpdate, db: Session = Depends(get_db)
) -> ReleaseRead:
    release = _get_release_or_404(release_id, db)

    update_data = payload.dict(exclude_unset=True)
    project_id = update_data.get("project_id")
    if project_id and not db.get(Project, project_id):
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Project not found")

    for field, value in update_data.items():
        setattr(release, field, value)

    db.commit()
    db.refresh(release)
    return release


@router.delete("/{release_id}", status_code=status.HTTP_204_NO_CONTENT)
def delete_release(release_id: UUID, db: Session = Depends(get_db)) -> None:
    release = _get_release_or_404(release_id, db)
    db.delete(release)
    db.commit()
