from datetime import datetime, timedelta, timezone

from src.api.models import (
    ApplicationHealthResponse,
    GuardrailSummaryResponse,
    ModelLatencyResponse,
    ModelUsageResponse,
    PlatformSummary,
    TimeWindow,
)


WINDOWS = {
    TimeWindow.ONE_HOUR: timedelta(hours=1),
    TimeWindow.SIX_HOURS: timedelta(hours=6),
    TimeWindow.TWENTY_FOUR_HOURS: timedelta(hours=24),
    TimeWindow.SEVEN_DAYS: timedelta(days=7),
}


MIN_AGGREGATION_SIZE = 10


class InsufficientAggregationError(Exception):
    pass


class TelemetryService:

    def __init__(self, repository):
        self.repository = repository

    @staticmethod
    def _since(window: TimeWindow) -> datetime:
        return datetime.now(timezone.utc) - WINDOWS[window]

    @staticmethod
    def _check_minimum(count: int) -> None:
        if count < MIN_AGGREGATION_SIZE:
            raise InsufficientAggregationError(
                "Insufficient data for this aggregation."
            )

    def get_platform_summary(
        self,
        window: TimeWindow,
    ) -> PlatformSummary:

        row = self.repository.get_platform_summary(
            self._since(window)
        ).first()

        total = row.total_events or 0

        self._check_minimum(total)

        return PlatformSummary(
            window=window,
            total_events=total,
            avg_latency_ms=row.avg_latency_ms or 0,
            anomaly_count=row.anomaly_count or 0,
            pii_detection_count=row.pii_detection_count or 0,
            policy_violation_count=(
                row.policy_violation_count or 0
            ),
            generated_at=datetime.now(timezone.utc),
        )

    def get_model_latency(
        self,
        model_name: str,
        window: TimeWindow,
    ) -> ModelLatencyResponse:

        row = self.repository.get_model_latency(
            model_name,
            self._since(window),
        ).first()

        count = row.request_count or 0

        self._check_minimum(count)

        return ModelLatencyResponse(
            model=model_name,
            window=window,
            request_count=count,
            avg_latency_ms=row.avg_latency_ms or 0,
            p50_latency_ms=row.p50_latency_ms or 0,
            p95_latency_ms=row.p95_latency_ms or 0,
            p99_latency_ms=row.p99_latency_ms or 0,
            anomaly_count=row.anomaly_count or 0,
            generated_at=datetime.now(timezone.utc),
        )

    def get_model_usage(
        self,
        model_name: str,
        window: TimeWindow,
    ) -> ModelUsageResponse:

        row = self.repository.get_model_usage(
            model_name,
            self._since(window),
        ).first()

        count = row.request_count or 0

        self._check_minimum(count)

        prompt_tokens = row.prompt_tokens or 0
        completion_tokens = row.completion_tokens or 0

        return ModelUsageResponse(
            model=model_name,
            window=window,
            request_count=count,
            prompt_tokens=prompt_tokens,
            completion_tokens=completion_tokens,
            total_tokens=prompt_tokens + completion_tokens,
            generated_at=datetime.now(timezone.utc),
        )

    def get_application_health(
        self,
        app_id: str,
        window: TimeWindow,
    ) -> ApplicationHealthResponse:

        row = self.repository.get_application_health(
            app_id,
            self._since(window),
        ).first()

        count = row.request_count or 0

        self._check_minimum(count)

        return ApplicationHealthResponse(
            app_id=app_id,
            window=window,
            request_count=count,
            avg_latency_ms=row.avg_latency_ms or 0,
            p95_latency_ms=row.p95_latency_ms or 0,
            anomaly_count=row.anomaly_count or 0,
            pii_detection_count=row.pii_detection_count or 0,
            policy_violation_count=(
                row.policy_violation_count or 0
            ),
            generated_at=datetime.now(timezone.utc),
        )

    def get_guardrail_summary(
        self,
        window: TimeWindow,
    ) -> GuardrailSummaryResponse:

        row = self.repository.get_guardrail_summary(
            self._since(window)
        ).first()

        total = row.total_events or 0

        self._check_minimum(total)

        pii_count = row.pii_detection_count or 0
        policy_count = row.policy_violation_count or 0

        return GuardrailSummaryResponse(
            window=window,
            total_events=total,
            pii_detection_count=pii_count,
            policy_violation_count=policy_count,
            pii_detection_rate=pii_count / total,
            policy_violation_rate=policy_count / total,
            generated_at=datetime.now(timezone.utc),
        )