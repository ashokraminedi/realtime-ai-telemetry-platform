import os

from fastapi.testclient import TestClient

# Set test credentials before importing the application.
os.environ["PERFORMANCE_AGENT_API_KEY"] = "test-performance-key"
os.environ["SECURITY_AGENT_API_KEY"] = "test-security-key"
os.environ["DASHBOARD_API_KEY"] = "test-dashboard-key"

from src.api.main import app


client = TestClient(app)


def test_missing_api_key_returns_401():
    response = client.get("/v1/platform/summary")

    assert response.status_code == 401
    assert response.json()["detail"] == "Missing API key."


def test_invalid_api_key_returns_401():
    response = client.get(
        "/v1/platform/summary",
        headers={
            "X-API-Key": "invalid-key",
        },
    )

    assert response.status_code == 401
    assert response.json()["detail"] == "Invalid API key."


def test_performance_agent_cannot_access_guardrails():
    response = client.get(
        "/v1/guardrails/summary",
        headers={
            "X-API-Key": "test-performance-key",
        },
    )

    assert response.status_code == 403
    assert response.json()["detail"] == "Insufficient permissions."


def test_security_agent_cannot_access_model_metrics():
    response = client.get(
        "/v1/models/gpt-4o/latency",
        headers={
            "X-API-Key": "test-security-key",
        },
    )

    assert response.status_code == 403
    assert response.json()["detail"] == "Insufficient permissions."


def test_security_agent_cannot_access_application_metrics():
    response = client.get(
        "/v1/apps/search-agent-v2/health",
        headers={
            "X-API-Key": "test-security-key",
        },
    )

    assert response.status_code == 403
    assert response.json()["detail"] == "Insufficient permissions."