from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.orm import Session

from app.database import get_db
from app.models import Role
from app.schemas import RoleCreate, RoleRead, RoleUpdate

router = APIRouter(prefix="/roles", tags=["Roles"])


def _get_role_or_404(role_id: UUID, db: Session) -> Role:
    role = db.get(Role, role_id)
    if not role:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Role not found")
    return role


def _ensure_unique_name(name: str, db: Session, role_id: UUID | None = None) -> None:
    query = db.query(Role).filter(Role.name == name)
    if role_id:
        query = query.filter(Role.id != role_id)
    if db.query(query.exists()).scalar():
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Role name must be unique",
        )


@router.post("", response_model=RoleRead, status_code=status.HTTP_201_CREATED)
def create_role(payload: RoleCreate, db: Session = Depends(get_db)) -> RoleRead:
    _ensure_unique_name(payload.name, db)

    role = Role(**payload.dict())
    db.add(role)
    db.commit()
    db.refresh(role)
    return role


@router.get("", response_model=list[RoleRead])
def list_roles(db: Session = Depends(get_db)) -> list[RoleRead]:
    return db.query(Role).order_by(Role.name).all()


@router.get("/{role_id}", response_model=RoleRead)
def get_role(role_id: UUID, db: Session = Depends(get_db)) -> RoleRead:
    return _get_role_or_404(role_id, db)


@router.put("/{role_id}", response_model=RoleRead)
def update_role(role_id: UUID, payload: RoleUpdate, db: Session = Depends(get_db)) -> RoleRead:
    role = _get_role_or_404(role_id, db)

    update_data = payload.dict(exclude_unset=True)
    name = update_data.get("name")
    if name:
        _ensure_unique_name(name, db, role_id=role_id)

    for field, value in update_data.items():
        setattr(role, field, value)

    db.commit()
    db.refresh(role)
    return role


@router.delete("/{role_id}", status_code=status.HTTP_204_NO_CONTENT)
def delete_role(role_id: UUID, db: Session = Depends(get_db)) -> None:
    role = _get_role_or_404(role_id, db)
    db.delete(role)
    db.commit()
