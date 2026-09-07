from __future__ import annotations

import httpx
import pytest

from .conftest import gateway_harness

pytestmark = pytest.mark.asyncio


async def test_bearer_and_x_api_key_authentication_and_upstream_replacement(tmp_path):
    seen: list[
        tuple[
            str,
            str | None,
            str | None,
            str | None,
            str | None,
            str | None,
        ]
    ] = []

    async def handler(request: httpx.Request) -> httpx.Response:
        seen.append(
            (
                request.url.path,
                request.headers.get("authorization"),
                request.headers.get("x-api-key"),
                request.headers.get("anthropic-version"),
                request.headers.get("accept-encoding"),
                request.headers.get("connection"),
            )
        )
        return httpx.Response(200, json={"ok": True})

    async with gateway_harness(tmp_path, handler) as harness:
        bearer = await harness.client.post(
            "/v1/chat/completions",
            headers={"Authorization": f"Bearer {harness.keys['hermes']}"},
            json={"model": "auto", "messages": []},
        )
        anthropic = await harness.client.post(
            "/v1/messages",
            headers={
                "x-api-key": harness.keys["hermes"],
                "anthropic-version": "2023-06-01",
            },
            json={"model": "auto", "max_tokens": 1, "messages": []},
        )

    assert bearer.status_code == 200
    assert anthropic.status_code == 200
    assert seen[0] == (
        "/v1/chat/completions",
        "Bearer qaml_live_test_secret",
        None,
        None,
        "identity",
        None,
    )
    assert seen[1] == (
        "/v1/messages",
        None,
        "qaml_live_test_secret",
        "2023-06-01",
        "identity",
        None,
    )


async def test_invalid_missing_and_conflicting_keys_are_rejected(tmp_path):
    calls = 0

    async def handler(request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        return httpx.Response(200, json={})

    async with gateway_harness(tmp_path, handler) as harness:
        missing = await harness.client.post("/v1/responses", json={"model": "auto"})
        invalid = await harness.client.post(
            "/v1/responses",
            headers={"Authorization": "Bearer cgk_not_valid"},
            json={"model": "auto"},
        )
        conflicting = await harness.client.post(
            "/v1/responses",
            headers=[
                ("Authorization", f"Bearer {harness.keys['hermes']}"),
                ("x-api-key", "cgk_different"),
            ],
            json={"model": "auto"},
        )

    assert missing.status_code == 401
    assert invalid.status_code == 401
    assert conflicting.status_code == 401
    assert missing.headers["x-camel-gateway-request-id"]
    assert invalid.headers["x-camel-gateway-request-id"]
    assert conflicting.headers["x-camel-gateway-request-id"]
    assert calls == 0


async def test_allowlist_blocks_management_and_unknown_endpoints(tmp_path):
    async def handler(request: httpx.Request) -> httpx.Response:
        raise AssertionError("blocked endpoints must not reach upstream")

    async with gateway_harness(tmp_path, handler) as harness:
        management = await harness.client.post(
            "/v1/keys",
            headers={"Authorization": f"Bearer {harness.keys['hermes']}"},
            json={},
        )
        unknown = await harness.client.post(
            "/v1/unknown",
            headers={"Authorization": f"Bearer {harness.keys['hermes']}"},
            json={},
        )

    assert management.status_code == 404
    assert unknown.status_code == 404


async def test_health_does_not_expose_secrets(tmp_path):
    async def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json={})

    async with gateway_harness(tmp_path, handler) as harness:
        response = await harness.client.get("/healthz")

    assert response.status_code == 200
    assert response.json() == {
        "status": "ok",
        "queue_depth": 0,
        "upstream_busy": False,
    }
    assert "qaml" not in response.text
    assert "cgk_" not in response.text


async def test_hop_by_hop_headers_are_removed_in_both_directions(tmp_path):
    async def handler(request: httpx.Request) -> httpx.Response:
        assert request.headers.get("connection") is None
        assert request.headers.get("x-client-hop") is None
        assert request.headers["x-end-to-end"] == "keep"
        return httpx.Response(
            200,
            headers=[
                ("content-type", "application/json"),
                ("connection", "x-upstream-hop"),
                ("x-upstream-hop", "drop"),
                ("x-end-to-end-response", "keep"),
            ],
            content=b"{}",
        )

    async with gateway_harness(tmp_path, handler) as harness:
        response = await harness.client.post(
            "/v1/responses",
            headers={
                "Authorization": f"Bearer {harness.keys['hermes']}",
                "Connection": "x-client-hop",
                "X-Client-Hop": "drop",
                "X-End-To-End": "keep",
            },
            json={"model": "auto"},
        )

    assert response.status_code == 200
    assert response.headers.get("connection") is None
    assert response.headers.get("x-upstream-hop") is None
    assert response.headers["x-end-to-end-response"] == "keep"
