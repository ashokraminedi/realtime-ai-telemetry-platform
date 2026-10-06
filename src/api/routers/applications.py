from fastapi import APIRouter, Depends

from src.api.auth import Principal, require_scope
from src.api.dependencies import (
    get_current_principal,
    get_telemetry_service,
)
from src.api.models import (
    ApplicationHealthResponse,
    TimeWindow,
)
from src.api.service import TelemetryService


router = APIRouter(
    prefix="/v1/apps",
    tags=["Applications"],
)


@router.get(
    "/{app_id}/health",
    response_model=ApplicationHealthResponse,
)
def application_health(
    app_id: str,
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
        "telemetry:app:read",
    )

    return service.get_application_health(
        app_id,
        window,
    )