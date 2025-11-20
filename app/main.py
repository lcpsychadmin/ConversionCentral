import asyncio
import logging

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from app.config import get_settings
from app.routers import api_router
from app.services.databricks_bootstrap import ensure_databricks_connection
from app.services.data_quality_provisioning import data_quality_provisioner
from app.services.scheduled_ingestion import scheduled_ingestion_engine

settings = get_settings()
app = FastAPI(title=settings.app_name)

logger = logging.getLogger(__name__)

app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.frontend_origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)
app.include_router(api_router, prefix="/api")


@app.get("/health", tags=["Health"])
def health_check() -> dict[str, str]:
    return {"status": "ok"}


async def _run_startup_task(func, label: str) -> None:
    try:
        await asyncio.to_thread(func)
    except Exception:
        logger.exception("%s failed", label)


@app.on_event("startup")
async def startup_scheduler() -> None:
    asyncio.create_task(_run_startup_task(ensure_databricks_connection, "Databricks bootstrap"))
    scheduled_ingestion_engine.start()


@app.on_event("shutdown")
async def shutdown_scheduler() -> None:
    scheduled_ingestion_engine.shutdown()
    data_quality_provisioner.shutdown()
