from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, Query, status
from sqlalchemy import func
from sqlalchemy.orm import Session, selectinload

from app.database import get_db
from app.models import DataDefinition, DataObject, DataObjectSystem, ProcessArea, System
from app.schemas import DataObjectCreate, DataObjectRead, DataObjectUpdate
from app.services.workspace_scope import resolve_workspace_id

router = APIRouter(prefix="/data-objects", tags=["Data Objects"])


router = APIRouter(prefix="/data-objects", tags=["Data Objects"])


def _get_data_object_or_404(data_object_id: UUID, db: Session) -> DataObject:
    data_object = (
        db.query(DataObject)
        .options(selectinload(DataObject.systems), selectinload(DataObject.workspace))
        .filter(DataObject.id == data_object_id)
        .one_or_none()
    )
    if not data_object:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Data object not found")
    return data_object


@router.post("", response_model=DataObjectRead, status_code=status.HTTP_201_CREATED)
def create_data_object(
    payload: DataObjectCreate, db: Session = Depends(get_db)
) -> DataObjectRead:
    if not db.get(ProcessArea, payload.process_area_id):
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Process area not found")

    workspace_id = resolve_workspace_id(db, payload.workspace_id)

    system_ids = list(dict.fromkeys(payload.system_ids))
    if system_ids:
        existing_count = (
            db.query(func.count(System.id))
            .filter(System.id.in_(system_ids))
            .scalar()
        )
        if existing_count != len(system_ids):
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="One or more systems not found")

    data_object_payload = payload.dict(exclude={"system_ids", "workspace_id"})
    data_object = DataObject(**data_object_payload, workspace_id=workspace_id)
    db.add(data_object)
    db.flush()

    for system_id in system_ids:
        db.add(
            DataObjectSystem(
                data_object_id=data_object.id,
                system_id=system_id,
                relationship_type="source",
            )
        )

    db.commit()
    return _get_data_object_or_404(data_object.id, db)


@router.get("", response_model=list[DataObjectRead])
def list_data_objects(
    workspace_id: UUID | None = Query(default=None),
    process_area_id: UUID | None = Query(default=None),
    db: Session = Depends(get_db),
) -> list[DataObjectRead]:
    resolved_workspace_id = resolve_workspace_id(db, workspace_id)

    query = (
        db.query(DataObject)
        .options(selectinload(DataObject.systems), selectinload(DataObject.workspace))
        .filter(DataObject.workspace_id == resolved_workspace_id)
    )

    if process_area_id:
        query = query.filter(DataObject.process_area_id == process_area_id)

    return query.all()


@router.get("/{data_object_id}", response_model=DataObjectRead)
def get_data_object(
    data_object_id: UUID, db: Session = Depends(get_db)
) -> DataObjectRead:
    return _get_data_object_or_404(data_object_id, db)


@router.put("/{data_object_id}", response_model=DataObjectRead)
def update_data_object(
    data_object_id: UUID, payload: DataObjectUpdate, db: Session = Depends(get_db)
) -> DataObjectRead:
    data_object = _get_data_object_or_404(data_object_id, db)

    update_data = payload.dict(exclude_unset=True)
    system_ids = update_data.pop("system_ids", None)
    workspace_provided = "workspace_id" in payload.__fields_set__
    requested_workspace_id = update_data.pop("workspace_id", None)
    process_area_id = update_data.get("process_area_id")
    if process_area_id and not db.get(ProcessArea, process_area_id):
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Process area not found")

    for field, value in update_data.items():
        setattr(data_object, field, value)

    if workspace_provided:
        resolved_workspace_id = resolve_workspace_id(db, requested_workspace_id)
        data_object.workspace_id = resolved_workspace_id
        db.query(DataDefinition).filter(DataDefinition.data_object_id == data_object.id).update(
            {DataDefinition.workspace_id: resolved_workspace_id}, synchronize_session=False
        )

    if system_ids is not None:
        unique_system_ids = list(dict.fromkeys(system_ids))
        if unique_system_ids:
            existing_count = (
                db.query(func.count(System.id))
                .filter(System.id.in_(unique_system_ids))
                .scalar()
            )
            if existing_count != len(unique_system_ids):
                raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="One or more systems not found")

        # sync links
        current_links = db.query(DataObjectSystem).filter(DataObjectSystem.data_object_id == data_object.id).all()
        current_ids = {link.system_id for link in current_links}
        desired_ids = set(unique_system_ids)

        for link in current_links:
            if link.system_id not in desired_ids:
                db.delete(link)

        for system_id in desired_ids - current_ids:
            db.add(
                DataObjectSystem(
                    data_object_id=data_object.id,
                    system_id=system_id,
                    relationship_type="source",
                )
            )

    db.commit()
    return _get_data_object_or_404(data_object.id, db)


@router.delete("/{data_object_id}", status_code=status.HTTP_204_NO_CONTENT)
def delete_data_object(data_object_id: UUID, db: Session = Depends(get_db)) -> None:
    data_object = _get_data_object_or_404(data_object_id, db)
    db.delete(data_object)
    db.commit()
