from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.orm import Session

from app.database import get_db
from app.models import Table, TableDependency
from app.schemas import TableDependencyCreate, TableDependencyRead, TableDependencyUpdate

router = APIRouter(prefix="/table-dependencies", tags=["Table Dependencies"])


def _get_dependency_or_404(dependency_id: UUID, db: Session) -> TableDependency:
    dependency = db.get(TableDependency, dependency_id)
    if not dependency:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Dependency not found")
    return dependency


def _ensure_table_exists(table_id: UUID, db: Session) -> None:
    if not db.get(Table, table_id):
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Table not found")


def _ensure_unique_pair(
    predecessor_id: UUID,
    successor_id: UUID,
    db: Session,
    current_id: UUID | None = None,
) -> None:
    query = db.query(TableDependency).filter(
        TableDependency.predecessor_id == predecessor_id,
        TableDependency.successor_id == successor_id,
    )
    if current_id:
        query = query.filter(TableDependency.id != current_id)
    if db.query(query.exists()).scalar():
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Dependency already exists",
        )


@router.post("", response_model=TableDependencyRead, status_code=status.HTTP_201_CREATED)
def create_dependency(
    payload: TableDependencyCreate, db: Session = Depends(get_db)
) -> TableDependencyRead:
    if payload.predecessor_id == payload.successor_id:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Predecessor and successor must differ",
        )

    _ensure_table_exists(payload.predecessor_id, db)
    _ensure_table_exists(payload.successor_id, db)
    _ensure_unique_pair(payload.predecessor_id, payload.successor_id, db)

    dependency = TableDependency(**payload.dict())
    db.add(dependency)
    db.commit()
    db.refresh(dependency)
    return dependency


@router.get("", response_model=list[TableDependencyRead])
def list_dependencies(db: Session = Depends(get_db)) -> list[TableDependencyRead]:
    return db.query(TableDependency).all()


@router.get("/{dependency_id}", response_model=TableDependencyRead)
def get_dependency(
    dependency_id: UUID, db: Session = Depends(get_db)
) -> TableDependencyRead:
    return _get_dependency_or_404(dependency_id, db)


@router.put("/{dependency_id}", response_model=TableDependencyRead)
def update_dependency(
    dependency_id: UUID,
    payload: TableDependencyUpdate,
    db: Session = Depends(get_db),
) -> TableDependencyRead:
    dependency = _get_dependency_or_404(dependency_id, db)
    update_data = payload.dict(exclude_unset=True)

    predecessor_id = update_data.get("predecessor_id", dependency.predecessor_id)
    successor_id = update_data.get("successor_id", dependency.successor_id)

    if predecessor_id == successor_id:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Predecessor and successor must differ",
        )

    _ensure_table_exists(predecessor_id, db)
    _ensure_table_exists(successor_id, db)
    _ensure_unique_pair(predecessor_id, successor_id, db, current_id=dependency_id)

    for field, value in update_data.items():
        setattr(dependency, field, value)

    db.commit()
    db.refresh(dependency)
    return dependency


@router.delete("/{dependency_id}", status_code=status.HTTP_204_NO_CONTENT)
def delete_dependency(
    dependency_id: UUID, db: Session = Depends(get_db)
) -> None:
    dependency = _get_dependency_or_404(dependency_id, db)
    db.delete(dependency)
    db.commit()
