from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.orm import Session

from app.database import get_db
from app.models import User
from app.schemas import UserCreate, UserRead, UserUpdate

router = APIRouter(prefix="/users", tags=["Users"])


def _get_user_or_404(user_id: UUID, db: Session) -> User:
    user = db.get(User, user_id)
    if not user:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="User not found")
    return user


def _ensure_unique_email(email: str | None, db: Session, user_id: UUID | None = None) -> None:
    if not email:
        return
    query = db.query(User).filter(User.email == email)
    if user_id:
        query = query.filter(User.id != user_id)
    if db.query(query.exists()).scalar():
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Email already in use",
        )


@router.post("", response_model=UserRead, status_code=status.HTTP_201_CREATED)
def create_user(payload: UserCreate, db: Session = Depends(get_db)) -> UserRead:
    _ensure_unique_email(payload.email, db)

    user = User(**payload.dict())
    db.add(user)
    db.commit()
    db.refresh(user)
    return user


@router.get("", response_model=list[UserRead])
def list_users(db: Session = Depends(get_db)) -> list[UserRead]:
    return db.query(User).order_by(User.name).all()


@router.get("/{user_id}", response_model=UserRead)
def get_user(user_id: UUID, db: Session = Depends(get_db)) -> UserRead:
    return _get_user_or_404(user_id, db)


@router.put("/{user_id}", response_model=UserRead)
def update_user(user_id: UUID, payload: UserUpdate, db: Session = Depends(get_db)) -> UserRead:
    user = _get_user_or_404(user_id, db)

    update_data = payload.dict(exclude_unset=True)
    if "email" in update_data:
        _ensure_unique_email(update_data["email"], db, user_id=user_id)

    for field, value in update_data.items():
        setattr(user, field, value)

    db.commit()
    db.refresh(user)
    return user


@router.delete("/{user_id}", status_code=status.HTTP_204_NO_CONTENT)
def delete_user(user_id: UUID, db: Session = Depends(get_db)) -> None:
    user = _get_user_or_404(user_id, db)
    db.delete(user)
    db.commit()
