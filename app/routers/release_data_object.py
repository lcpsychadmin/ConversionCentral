from typing import Optional
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, Query, status
from sqlalchemy import String, cast
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from app.database import get_db
from app.models import DataObject, Release, ReleaseDataObject
from app.schemas import ReleaseDataObjectCreate, ReleaseDataObjectRead

router = APIRouter(prefix="/release-data-objects", tags=["Release Data Objects"])


@router.post("", response_model=ReleaseDataObjectRead, status_code=status.HTTP_201_CREATED)
def create_release_data_object(
    payload: ReleaseDataObjectCreate, db: Session = Depends(get_db)
) -> ReleaseDataObjectRead:
    if not db.get(Release, payload.release_id):
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Release not found")
    if not db.get(DataObject, payload.data_object_id):
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Data object not found")

    existing_link = (
        db.query(ReleaseDataObject)
        .filter(ReleaseDataObject.release_id == payload.release_id)
        .filter(cast(ReleaseDataObject.data_object_id, String) == str(payload.data_object_id))
        .one_or_none()
    )
    if existing_link:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="Data object is already linked to this release",
        )

    link = ReleaseDataObject(**payload.dict())
    db.add(link)

    try:
        db.commit()
    except IntegrityError:
        db.rollback()
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="Data object is already linked to this release",
        )

    db.refresh(link)
    return link


@router.get("", response_model=list[ReleaseDataObjectRead])
def list_release_data_objects(
    release_id: Optional[UUID] = Query(None),
    data_object_id: Optional[UUID] = Query(None),
    db: Session = Depends(get_db),
) -> list[ReleaseDataObjectRead]:
    query = db.query(ReleaseDataObject)
    if release_id:
        query = query.filter(ReleaseDataObject.release_id == release_id)
    if data_object_id:
        query = query.filter(
            cast(ReleaseDataObject.data_object_id, String) == str(data_object_id)
        )
    return query.all()


@router.delete("/{link_id}", status_code=status.HTTP_204_NO_CONTENT)
def delete_release_data_object(
    link_id: UUID,
    release_id: Optional[UUID] = Query(None),
    data_object_id: Optional[UUID] = Query(None),
    db: Session = Depends(get_db),
) -> None:
    link = next(
        (
            candidate
            for candidate in db.query(ReleaseDataObject).all()
            if str(candidate.id) == str(link_id)
        ),
        None,
    )

    if not link:
        return

    if release_id and str(link.release_id) != str(release_id):
        return

    if data_object_id and str(link.data_object_id) != str(data_object_id):
        return

    db.delete(link)
    db.commit()
