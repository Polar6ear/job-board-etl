"""
api/main.py — FastAPI application entry point.

Features:
  - Lifespan: DB table check on startup
  - CORS configured for local dev + production origins
  - /health endpoint
  - Auto-generated docs at /docs and /redoc
  - Global exception handler for clean JSON errors
"""

from __future__ import annotations

import os
from contextlib import asynccontextmanager

import structlog
from fastapi import FastAPI, Request, status
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from sqlalchemy import text

from api.db.session import get_async_engine
from api.routers import jobs
from api.schemas.job import HealthResponse

logger = structlog.get_logger(__name__)


# ---------------------------------------------------------------------------
# Lifespan — runs on startup / shutdown
# ---------------------------------------------------------------------------

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup
    logger.info("api_starting")
    engine = get_async_engine()
    async with engine.connect() as conn:
        await conn.execute(text("SELECT 1"))
    logger.info("db_connection_ok")
    yield
    # Shutdown
    await engine.dispose()
    logger.info("api_shutdown")


# ---------------------------------------------------------------------------
# App
# ---------------------------------------------------------------------------

app = FastAPI(
    title="Job Board ETL — API",
    description=(
        "Search and filter job listings extracted from Adzuna, RemoteOK, and LinkedIn. "
        "Powered by Apache Airflow ETL pipeline."
    ),
    version="1.0.0",
    docs_url="/docs",
    redoc_url="/redoc",
    lifespan=lifespan,
)


# ---------------------------------------------------------------------------
# CORS
# ---------------------------------------------------------------------------

ALLOWED_ORIGINS = [
    "http://localhost:3000",
    "http://localhost:5173",
    "http://localhost:8000",
    *os.environ.get("EXTRA_CORS_ORIGINS", "").split(","),
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=[o for o in ALLOWED_ORIGINS if o],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# ---------------------------------------------------------------------------
# Global exception handler
# ---------------------------------------------------------------------------

@app.exception_handler(Exception)
async def global_exception_handler(request: Request, exc: Exception) -> JSONResponse:
    logger.error("unhandled_exception", path=request.url.path, error=str(exc))
    return JSONResponse(
        status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
        content={"detail": "Internal server error. Please try again later."},
    )


# ---------------------------------------------------------------------------
# Health check
# ---------------------------------------------------------------------------

@app.get("/health", response_model=HealthResponse, tags=["system"])
async def health_check() -> HealthResponse:
    """Quick liveness + DB connectivity check."""
    from sqlalchemy import func, select
    from api.db.models import Job
    from api.db.session import AsyncSessionLocal

    try:
        async with AsyncSessionLocal() as session:
            total = (
                await session.execute(
                    select(func.count()).where(Job.is_active == True)  # noqa: E712
                )
            ).scalar_one()
        db_status = "ok"
    except Exception as exc:
        logger.error("health_check_db_failed", error=str(exc))
        db_status = "error"
        total = 0

    return HealthResponse(
        status="ok" if db_status == "ok" else "degraded",
        db=db_status,
        total_jobs=total,
    )


# ---------------------------------------------------------------------------
# Routers
# ---------------------------------------------------------------------------

app.include_router(jobs.router)


# ---------------------------------------------------------------------------
# Dev entrypoint
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import uvicorn

    uvicorn.run(
        "api.main:app",
        host=os.environ.get("API_HOST", "0.0.0.0"),
        port=int(os.environ.get("API_PORT", 8000)),
        reload=True,
        log_level="info",
    )