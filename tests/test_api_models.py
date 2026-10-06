from datetime import datetime, timezone

import pytest
from pydantic import ValidationError

from src.api.models import (
    GuardrailSummaryResponse,
    PlatformSummary,
    TimeWindow,
)


def test_time_window_values():
    assert TimeWindow.ONE_HOUR.value == "1h"
    assert TimeWindow.SIX_HOURS.value == "6h"
    assert TimeWindow.TWENTY_FOUR_HOURS.value == "24h"
    assert TimeWindow.SEVEN_DAYS.value == "7d"


def test_platform_summary_accepts_valid_values():
    summary = PlatformSummary(
        window=TimeWindow.ONE_HOUR,
        total_events=100,
        avg_latency_ms=250.5,
        anomaly_count=5,
        pii_detection_count=2,
        policy_violation_count=1,
        generated_at=datetime.now(timezone.utc),
    )

    assert summary.total_events == 100
    assert summary.window == TimeWindow.ONE_HOUR


def test_platform_summary_rejects_negative_counts():
    with pytest.raises(ValidationError):
        PlatformSummary(
            window=TimeWindow.ONE_HOUR,
            total_events=-1,
            avg_latency_ms=250,
            anomaly_count=0,
            pii_detection_count=0,
            policy_violation_count=0,
            generated_at=datetime.now(timezone.utc),
        )


def test_guardrail_summary_rejects_rate_above_one():
    with pytest.raises(ValidationError):
        GuardrailSummaryResponse(
            window=TimeWindow.ONE_HOUR,
            total_events=100,
            pii_detection_count=5,
            policy_violation_count=2,
            pii_detection_rate=1.1,
            policy_violation_rate=0.02,
            generated_at=datetime.now(timezone.utc),
        )