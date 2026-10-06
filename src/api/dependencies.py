import os

from fastapi import Depends, HTTPException, Security, status
from fastapi.security import APIKeyHeader

from config.spark_config import get_spark_session
from src.api.auth import Principal
from src.api.repository import TelemetryRepository
from src.api.service import TelemetryService


api_key_header = APIKeyHeader(
    name="X-API-Key",
    auto_error=False,
)


def _build_api_keys() -> dict[str, Principal]:
    api_keys: dict[str, Principal] = {}

    performance_key = os.getenv("PERFORMANCE_AGENT_API_KEY")
    security_key = os.getenv("SECURITY_AGENT_API_KEY")
    dashboard_key = os.getenv("DASHBOARD_API_KEY")

    if performance_key:
        api_keys[performance_key] = Principal(
            subject="performance-agent",
            scopes={
                "telemetry:summary:read",
                "telemetry:model:read",
                "telemetry:app:read",
            },
        )

    if security_key:
        api_keys[security_key] = Principal(
            subject="security-agent",
            scopes={
                "telemetry:summary:read",
                "telemetry:guardrail:read",
            },
        )

    if dashboard_key:
        api_keys[dashboard_key] = Principal(
            subject="dashboard-service",
            scopes={
                "telemetry:summary:read",
                "telemetry:model:read",
                "telemetry:app:read",
                "telemetry:guardrail:read",
            },
        )

    return api_keys


def get_current_principal(
    api_key: str | None = Security(api_key_header),
) -> Principal:

    if not api_key:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Missing API key.",
        )

    principal = _build_api_keys().get(api_key)

    if principal is None:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid API key.",
        )

    return principal


def get_telemetry_service() -> TelemetryService:

    spark = get_spark_session()

    repository = TelemetryRepository(spark)

    return TelemetryService(repository)