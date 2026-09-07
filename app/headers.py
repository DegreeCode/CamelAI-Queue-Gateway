from __future__ import annotations

import json
from collections import defaultdict
from collections.abc import Iterable

HOP_BY_HOP_HEADERS = {
    b"connection",
    b"keep-alive",
    b"proxy-authenticate",
    b"proxy-authorization",
    b"proxy-connection",
    b"te",
    b"trailer",
    b"transfer-encoding",
    b"upgrade",
}

REQUEST_AUTH_HEADERS = {b"authorization", b"x-api-key"}
GATEWAY_RESPONSE_HEADERS = {
    b"x-camel-gateway-request-id",
    b"x-camel-gateway-queue-ms",
}


def _connection_tokens(raw_headers: Iterable[tuple[bytes, bytes]]) -> set[bytes]:
    tokens: set[bytes] = set()
    for name, value in raw_headers:
        if name.lower() != b"connection":
            continue
        for token in value.split(b","):
            normalized = token.strip().lower()
            if normalized:
                tokens.add(normalized)
    return tokens


def build_upstream_headers(
    raw_headers: list[tuple[bytes, bytes]],
    *,
    endpoint: str,
    camel_api_key: str,
    body_size: int | None,
) -> list[tuple[bytes, bytes]]:
    connection_tokens = _connection_tokens(raw_headers)
    blocked = (
        HOP_BY_HOP_HEADERS
        | connection_tokens
        | REQUEST_AUTH_HEADERS
        | GATEWAY_RESPONSE_HEADERS
        | {b"host", b"content-length", b"expect", b"accept-encoding"}
    )

    result: list[tuple[bytes, bytes]] = []
    for name, value in raw_headers:
        lower = name.lower()
        if lower in blocked:
            continue
        result.append((name, value))

    # Keep raw logs and token-usage parsing byte-faithful without recompression.
    result.append((b"accept-encoding", b"identity"))

    secret = camel_api_key.encode("utf-8")
    if endpoint in {"/v1/messages", "/v1/messages/count_tokens"}:
        result.append((b"x-api-key", secret))
    else:
        result.append((b"authorization", b"Bearer " + secret))

    if body_size is not None:
        result.append((b"content-length", str(body_size).encode("ascii")))
    return result


def filter_response_headers(
    raw_headers: list[tuple[bytes, bytes]],
) -> list[tuple[bytes, bytes]]:
    connection_tokens = _connection_tokens(raw_headers)
    blocked = HOP_BY_HOP_HEADERS | connection_tokens | GATEWAY_RESPONSE_HEADERS
    return [
        (name, value)
        for name, value in raw_headers
        if name.lower() not in blocked
    ]


def _is_sensitive_header(name: bytes) -> bool:
    lower = name.decode("latin-1").lower()
    if lower in {
        "authorization",
        "proxy-authorization",
        "x-api-key",
        "api-key",
        "cookie",
        "set-cookie",
    }:
        return True
    return any(
        marker in lower
        for marker in (
            "authorization",
            "api-key",
            "apikey",
            "secret",
            "token",
            "cookie",
            "credential",
            "jwt",
            "session",
            "signature",
        )
    )


def redact_headers(raw_headers: Iterable[tuple[bytes, bytes]]) -> dict[str, object]:
    grouped: defaultdict[str, list[str]] = defaultdict(list)
    for name, value in raw_headers:
        decoded_name = name.decode("latin-1")
        decoded_value = (
            "[REDACTED]" if _is_sensitive_header(name) else value.decode("latin-1")
        )
        grouped[decoded_name].append(decoded_value)

    output: dict[str, object] = {}
    for name, values in grouped.items():
        output[name] = values[0] if len(values) == 1 else values
    return output


def redacted_headers_json(raw_headers: Iterable[tuple[bytes, bytes]]) -> str:
    return json.dumps(redact_headers(raw_headers), ensure_ascii=False, separators=(",", ":"))
