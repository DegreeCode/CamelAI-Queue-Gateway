from __future__ import annotations

import asyncio
import hmac

from starlette.requests import Request

from .db import Database, GatewayKey


class AuthenticationError(Exception):
    pass


def _candidate_keys(request: Request) -> list[str]:
    candidates: list[str] = []
    for raw_name, raw_value in request.headers.raw:
        name = raw_name.lower()
        value = raw_value.decode("latin-1").strip()
        if name == b"authorization":
            scheme, separator, token = value.partition(" ")
            if separator and scheme.lower() == "bearer" and token.strip():
                candidates.append(token.strip())
            elif value:
                raise AuthenticationError("Authorization must use the Bearer scheme")
        elif name == b"x-api-key" and value:
            candidates.append(value)
    return candidates


async def authenticate_request(request: Request, database: Database) -> GatewayKey:
    candidates = _candidate_keys(request)
    if not candidates:
        raise AuthenticationError("Missing gateway API key")

    selected = candidates[0]
    for candidate in candidates[1:]:
        if not hmac.compare_digest(selected, candidate):
            raise AuthenticationError("Conflicting gateway API keys")

    key = await asyncio.to_thread(database.authenticate_key, selected)
    if key is None:
        raise AuthenticationError("Invalid or disabled gateway API key")
    return key
