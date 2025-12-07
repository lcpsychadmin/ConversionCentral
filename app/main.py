import logging

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from app.config import get_settings
from app.routers import api_router
from app.services.data_quality_provisioning import data_quality_provisioner
from app.services.scheduled_profiling import scheduled_profiling_engine
from app.services.local_profiling_runner import local_profiling_runner
from app.services.table_observability import table_observability_engine

settings = get_settings()
log_level_name = (settings.log_level or "INFO").upper()
log_level = getattr(logging, log_level_name, logging.INFO)
logging.getLogger("app").setLevel(log_level)

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


@app.on_event("startup")
async def startup_scheduler() -> None:
    scheduled_profiling_engine.start()
    table_observability_engine.start()
    if settings.profiling_execution_mode.lower() == "local":
        local_profiling_runner.start()


@app.on_event("shutdown")
async def shutdown_scheduler() -> None:
    scheduled_profiling_engine.shutdown()
    table_observability_engine.shutdown()
    data_quality_provisioner.shutdown()
    if settings.profiling_execution_mode.lower() == "local":
        local_profiling_runner.stop()
