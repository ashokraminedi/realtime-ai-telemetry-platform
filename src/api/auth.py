from pydantic import BaseModel


class Principal(BaseModel):
    subject: str
    scopes: set[str]


class AuthorizationError(Exception):
    pass


def require_scope(principal: Principal, required_scope: str) -> None:
    if required_scope not in principal.scopes:
        raise AuthorizationError(
            f"Missing required scope: {required_scope}"
        )