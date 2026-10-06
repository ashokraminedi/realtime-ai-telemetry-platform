from fastapi import APIRouter, Depends

from src.api.auth import Principal, require_scope
from src.api.dependencies import (
    get_current_principal,
    get_telemetry_service,
)
from src.api.models import (
    ModelLatencyResponse,
    ModelUsageResponse,
    TimeWindow,
)
from src.api.service import TelemetryService


router = APIRouter(
    prefix="/v1/models",
    tags=["Models"],
)


@router.get(
    "/{model_name}/latency",
    response_model=ModelLatencyResponse,
)
def model_latency(
    model_name: str,
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
        "telemetry:model:read",
    )

    return service.get_model_latency(
        model_name,
        window,
    )


@router.get(
    "/{model_name}/usage",
    response_model=ModelUsageResponse,
)
def model_usage(
    model_name: str,
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
        "telemetry:model:read",
    )

    return service.get_model_usage(
        model_name,
        window,
    )