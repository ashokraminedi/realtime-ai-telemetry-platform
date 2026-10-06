from fastapi import APIRouter, Depends

from src.api.auth import Principal, require_scope
from src.api.dependencies import (
    get_current_principal,
    get_telemetry_service,
)
from src.api.models import PlatformSummary, TimeWindow
from src.api.service import TelemetryService


router = APIRouter(
    prefix="/v1/platform",
    tags=["Platform"],
)


@router.get(
    "/summary",
    response_model=PlatformSummary,
)
def platform_summary(
    window: TimeWindow = TimeWindow.ONE_HOUR,
    principal: Principal = Depends(
        get_current_principal
    ),
    service: TelemetryService = Depends(
        get_telemetry_service
    ),
):
    require_scope(
        principal,
        "telemetry:summary:read",
    )

    return service.get_platform_summary(window)