from datetime import datetime
from enum import Enum

from pydantic import BaseModel, Field


class TimeWindow(str, Enum):
    ONE_HOUR = "1h"
    SIX_HOURS = "6h"
    TWENTY_FOUR_HOURS = "24h"
    SEVEN_DAYS = "7d"


class PlatformSummary(BaseModel):
    window: TimeWindow

    total_events: int = Field(ge=0)
    avg_latency_ms: float = Field(ge=0)

    anomaly_count: int = Field(ge=0)
    pii_detection_count: int = Field(ge=0)
    policy_violation_count: int = Field(ge=0)

    generated_at: datetime


class ModelLatencyResponse(BaseModel):
    model: str
    window: TimeWindow

    request_count: int = Field(ge=0)

    avg_latency_ms: float = Field(ge=0)
    p50_latency_ms: float = Field(ge=0)
    p95_latency_ms: float = Field(ge=0)
    p99_latency_ms: float = Field(ge=0)

    anomaly_count: int = Field(ge=0)

    generated_at: datetime


class ModelUsageResponse(BaseModel):
    model: str
    window: TimeWindow

    request_count: int = Field(ge=0)

    prompt_tokens: int = Field(ge=0)
    completion_tokens: int = Field(ge=0)
    total_tokens: int = Field(ge=0)

    generated_at: datetime


class ApplicationHealthResponse(BaseModel):
    app_id: str
    window: TimeWindow

    request_count: int = Field(ge=0)
    avg_latency_ms: float = Field(ge=0)
    p95_latency_ms: float = Field(ge=0)

    anomaly_count: int = Field(ge=0)
    pii_detection_count: int = Field(ge=0)
    policy_violation_count: int = Field(ge=0)

    generated_at: datetime


class GuardrailSummaryResponse(BaseModel):
    window: TimeWindow

    total_events: int = Field(ge=0)
    pii_detection_count: int = Field(ge=0)
    policy_violation_count: int = Field(ge=0)

    pii_detection_rate: float = Field(ge=0, le=1)
    policy_violation_rate: float = Field(ge=0, le=1)

    generated_at: datetime