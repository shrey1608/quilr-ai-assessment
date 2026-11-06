"""Bearer-token verification and local demo token issuance."""

import argparse
import time
from dataclasses import dataclass
from typing import Literal

import jwt

from .config import Settings
from .errors import GatewayError


@dataclass(frozen=True)
class Identity:
    tenant: str
    role: Literal["admin", "viewer"]


def authenticate(header: str | None, settings: Settings) -> Identity:
    try:
        scheme, token = (header or "").split()
        if scheme.lower() != "bearer":
            raise ValueError
        claims = jwt.decode(
            token,
            settings.jwt_secret.get_secret_value(),
            algorithms=["HS256"],
            issuer=settings.jwt_issuer,
            audience=settings.jwt_audience,
            options={"require": ["exp", "iat", "sub", "role", "iss", "aud"]},
        )
        if claims["role"] not in ("admin", "viewer"):
            raise ValueError
        if not isinstance(claims["sub"], str) or not 1 <= len(claims["sub"]) <= 128:
            raise ValueError
        return Identity(claims["sub"], claims["role"])
    except (ValueError, jwt.PyJWTError, TypeError):
        raise GatewayError(401, "unauthorized", "A valid bearer token is required") from None


def issue_token(settings: Settings, role: str, tenant: str, ttl: int = 3600) -> str:
    now = int(time.time())
    return jwt.encode(
        {
            "sub": tenant,
            "role": role,
            "iat": now,
            "exp": now + ttl,
            "iss": settings.jwt_issuer,
            "aud": settings.jwt_audience,
        },
        settings.jwt_secret.get_secret_value(),
        algorithm="HS256",
    )


def main() -> None:
    parser = argparse.ArgumentParser(description="Issue a local demonstration bearer token")
    parser.add_argument("--role", choices=["admin", "viewer"], default="viewer")
    parser.add_argument("--tenant", default="demo-tenant")
    args = parser.parse_args()
    print(issue_token(Settings(), args.role, args.tenant))
