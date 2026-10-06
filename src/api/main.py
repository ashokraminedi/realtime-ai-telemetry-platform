import logging

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

from src.api.audit import audit_middleware
from src.api.auth import AuthorizationError
from src.api.routers import (
    applications,
    guardrails,
    models,
    platform,
)
from src.api.service import InsufficientAggregationError


logging.basicConfig(level=logging.INFO)


app = FastAPI(
    title="AI Telemetry Data Access API",
    description=(
        "Governed aggregate access to the real-time "
        "AI telemetry platform."
    ),
    version="1.0.0",
)


app.middleware("http")(audit_middleware)


@app.exception_handler(AuthorizationError)
async def authorization_exception_handler(
    request: Request,
    exc: AuthorizationError,
):
    return JSONResponse(
        status_code=403,
        content={
            "detail": "Insufficient permissions."
        },
    )


@app.exception_handler(InsufficientAggregationError)
async def aggregation_exception_handler(
    request: Request,
    exc: InsufficientAggregationError,
):
    return JSONResponse(
        status_code=403,
        content={
            "detail": (
                "Insufficient data for this aggregation."
            )
        },
    )


@app.get(
    "/health",
    tags=["Health"],
)
def health():
    return {
        "status": "healthy",
        "service": "ai-telemetry-api",
    }


app.include_router(platform.router)
app.include_router(models.router)
app.include_router(applications.router)
app.include_router(guardrails.router)