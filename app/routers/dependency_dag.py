from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.orm import Session

from app.database import get_db
from app.schemas import DependencyOrderItem
from app.services import DAGBuilder

router = APIRouter(prefix="/dependency-dag", tags=["Dependency DAG"])


def _build_response(items: list[dict]) -> list[DependencyOrderItem]:
    return [DependencyOrderItem(**item) for item in items]


@router.get("/data-objects", response_model=list[DependencyOrderItem])
def get_data_object_order(db: Session = Depends(get_db)) -> list[DependencyOrderItem]:
    builder = DAGBuilder(db)
    try:
        order = builder.build_data_object_order()
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc
    return _build_response(order)


@router.get("/tables", response_model=list[DependencyOrderItem])
def get_table_order(db: Session = Depends(get_db)) -> list[DependencyOrderItem]:
    builder = DAGBuilder(db)
    try:
        order = builder.build_table_order()
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc
    return _build_response(order)
