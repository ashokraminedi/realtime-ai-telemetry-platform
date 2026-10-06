import os
from datetime import datetime, timezone

from fastapi.testclient import TestClient

os.environ["PERFORMANCE_AGENT_API_KEY"] = "test-performance-key"
os.environ["SECURITY_AGENT_API_KEY"] = "test-security-key"
os.environ["DASHBOARD_API_KEY"] = "test-dashboard-key"

from src.api.dependencies import get_telemetry_service
from src.api.main import app
from src.api.models import PlatformSummary, TimeWindow


class FakeTelemetryService:
    def get_platform_summary(
        self,
        window: TimeWindow,
    ) -> PlatformSummary:
        return PlatformSummary(
            window=window,
            total_events=100,
            avg_latency_ms=250.5,
            anomaly_count=5,
            pii_detection_count=2,
            policy_violation_count=1,
            generated_at=datetime.now(timezone.utc),
        )


def override_telemetry_service():
    return FakeTelemetryService()


app.dependency_overrides[get_telemetry_service] = (
    override_telemetry_service
)

client = TestClient(app)


def test_health_endpoint_is_public():
    response = client.get("/health")

    assert response.status_code == 200
    assert response.json() == {
        "status": "healthy",
        "service": "ai-telemetry-api",
    }


def test_platform_summary_with_valid_credentials():
    response = client.get(
        "/v1/platform/summary?window=1h",
        headers={
            "X-API-Key": "test-performance-key",
        },
    )

    assert response.status_code == 200

    body = response.json()

    assert body["window"] == "1h"
    assert body["total_events"] == 100
    assert body["avg_latency_ms"] == 250.5
    assert body["anomaly_count"] == 5


def test_platform_summary_supports_allowed_window():
    response = client.get(
        "/v1/platform/summary?window=24h",
        headers={
            "X-API-Key": "test-performance-key",
        },
    )

    assert response.status_code == 200
    assert response.json()["window"] == "24h"


def test_invalid_time_window_returns_422():
    response = client.get(
        "/v1/platform/summary?window=30d",
        headers={
            "X-API-Key": "test-performance-key",
        },
    )

    assert response.status_code == 422


def test_raw_telemetry_endpoint_does_not_exist():
    response = client.get(
        "/v1/telemetry/events",
        headers={
            "X-API-Key": "test-dashboard-key",
        },
    )

    assert response.status_code == 404


def test_sql_endpoint_does_not_exist():
    response = client.post(
        "/v1/sql",
        headers={
            "X-API-Key": "test-dashboard-key",
        },
        json={
            "query": "SELECT * FROM telemetry",
        },
    )

    assert response.status_code == 404


def test_query_endpoint_does_not_exist():
    response = client.post(
        "/v1/query",
        headers={
            "X-API-Key": "test-dashboard-key",
        },
        json={
            "query": "anything",
        },
    )

    assert response.status_code == 404