import pytest

from src.api.auth import (
    AuthorizationError,
    Principal,
    require_scope,
)


def test_principal_with_required_scope_is_authorized():
    principal = Principal(
        subject="performance-agent",
        scopes={
            "telemetry:summary:read",
            "telemetry:model:read",
        },
    )

    # Should not raise.
    require_scope(
        principal,
        "telemetry:model:read",
    )


def test_principal_without_required_scope_is_rejected():
    principal = Principal(
        subject="performance-agent",
        scopes={
            "telemetry:summary:read",
            "telemetry:model:read",
        },
    )

    with pytest.raises(
        AuthorizationError,
        match="Missing required scope",
    ):
        require_scope(
            principal,
            "telemetry:guardrail:read",
        )


def test_empty_scope_set_is_rejected():
    principal = Principal(
        subject="unknown-agent",
        scopes=set(),
    )

    with pytest.raises(AuthorizationError):
        require_scope(
            principal,
            "telemetry:summary:read",
        )


def test_scope_matching_is_exact():
    principal = Principal(
        subject="performance-agent",
        scopes={
            "telemetry:model:read",
        },
    )

    with pytest.raises(AuthorizationError):
        require_scope(
            principal,
            "telemetry:model",
        )