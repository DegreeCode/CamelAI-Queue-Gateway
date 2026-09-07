from __future__ import annotations

import httpx
import pytest

from app.db import Database
from .conftest import gateway_harness, make_settings

pytestmark = pytest.mark.asyncio


async def test_upstream_status_body_and_headers_are_preserved(tmp_path):
    raw = b'{"error":{"message":"teapot"}}'

    async def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            418,
            headers={
                "content-type": "application/json",
                "retry-after": "9",
                "x-camel-queue-ms": "23",
            },
            content=raw,
        )

    async with gateway_harness(tmp_path, handler) as harness:
        response = await harness.client.post(
            "/v1/chat/completions",
            headers={"Authorization": f"Bearer {harness.keys['hermes']}"},
            json={"model": "auto"},
        )

    assert response.status_code == 418
    assert response.content == raw
    assert response.headers["retry-after"] == "9"
    assert response.headers["x-camel-queue-ms"] == "23"
    assert response.headers["x-camel-gateway-request-id"]


async def test_count_tokens_retries_503_and_respects_retry_after(tmp_path):
    attempts = 0

    async def handler(request: httpx.Request) -> httpx.Response:
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            return httpx.Response(
                503,
                headers={"retry-after": "0", "content-type": "application/json"},
                json={"error": "busy"},
            )
        return httpx.Response(200, json={"input_tokens": 42})

    async with gateway_harness(tmp_path, handler) as harness:
        response = await harness.client.post(
            "/v1/messages/count_tokens",
            headers={
                "x-api-key": harness.keys["hermes"],
                "anthropic-version": "2023-06-01",
            },
            json={"model": "auto", "messages": []},
        )
        row = harness.database.get_request(
            response.headers["x-camel-gateway-request-id"]
        )

    assert response.status_code == 200
    assert attempts == 2
    assert row is not None
    assert row["retry_count"] == 1
    assert row["counted_input_tokens"] == 42


async def test_generation_does_not_retry_by_default_and_preserves_429_retry_after(tmp_path):
    attempts = 0
    raw = b'{"error":{"message":"rate limited"}}'

    async def handler(request: httpx.Request) -> httpx.Response:
        nonlocal attempts
        attempts += 1
        return httpx.Response(
            429,
            headers={"retry-after": "3", "content-type": "application/json"},
            content=raw,
        )

    async with gateway_harness(tmp_path, handler) as harness:
        response = await harness.client.post(
            "/v1/responses",
            headers={"Authorization": f"Bearer {harness.keys['hermes']}"},
            json={"model": "auto"},
        )
        row = harness.database.get_request(
            response.headers["x-camel-gateway-request-id"]
        )

    assert response.status_code == 429
    assert response.content == raw
    assert response.headers["retry-after"] == "3"
    assert attempts == 1
    assert row is not None
    assert row["retry_count"] == 0
    assert row["retry_allowed"] == 0


async def test_generation_retry_can_be_explicitly_enabled(tmp_path):
    attempts = 0

    async def handler(request: httpx.Request) -> httpx.Response:
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            return httpx.Response(503, headers={"retry-after": "0"}, json={})
        return httpx.Response(
            200,
            json={
                "usage": {
                    "input_tokens": 1,
                    "output_tokens": 1,
                    "total_tokens": 2,
                }
            },
        )

    async with gateway_harness(
        tmp_path, handler, retry_post_generation=True
    ) as harness:
        response = await harness.client.post(
            "/v1/responses",
            headers={"Authorization": f"Bearer {harness.keys['hermes']}"},
            json={"model": "auto"},
        )
        row = harness.database.get_request(
            response.headers["x-camel-gateway-request-id"]
        )

    assert response.status_code == 200
    assert attempts == 2
    assert row is not None
    assert row["retry_count"] == 1
    assert row["retry_allowed"] == 1


async def test_revoke_keeps_historical_usage_and_plaintext_is_not_stored(tmp_path):
    async def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            json={
                "usage": {
                    "prompt_tokens": 2,
                    "completion_tokens": 3,
                    "total_tokens": 5,
                }
            },
        )

    async with gateway_harness(tmp_path, handler) as harness:
        plaintext = harness.keys["hermes"]
        response = await harness.client.post(
            "/v1/chat/completions",
            headers={"Authorization": f"Bearer {plaintext}"},
            json={"model": "auto"},
        )
        assert response.status_code == 200
        harness.database.revoke_key("hermes")
        stats = harness.database.stats(key_name="hermes")
        rejected = await harness.client.post(
            "/v1/chat/completions",
            headers={"Authorization": f"Bearer {plaintext}"},
            json={"model": "auto"},
        )

    assert rejected.status_code == 401
    assert stats["keys"][0]["reported_total_tokens"] == 5
    key = harness.database.list_keys()[0]
    assert key.enabled is False
    assert key.revoked_at is not None

    database_bytes = harness.settings.db_path.read_bytes()
    assert plaintext.encode() not in database_bytes


async def test_startup_recovery_marks_inflight_rows_interrupted(tmp_path):
    settings = make_settings(tmp_path)
    database = Database(settings.db_path)
    database.initialize()
    plaintext, key = database.create_key("hermes")
    assert plaintext.startswith("cgk_")
    database.insert_request(
        {
            "request_id": "inflight",
            "gateway_key_id": key.id,
            "gateway_key_name": key.name,
            "endpoint": "/v1/responses",
            "method": "POST",
            "stream": 1,
            "received_at": "2026-08-29T00:00:00.000Z",
            "retry_count": 0,
            "retry_allowed": 0,
            "client_disconnected": 0,
            "usage_source": "unavailable",
            "token_count_query": 0,
            "request_bytes": 0,
            "response_bytes": 0,
            "state": "running",
        }
    )
    changed = database.mark_inflight_interrupted()
    row = database.get_request("inflight")
    assert changed == 1
    assert row is not None
    assert row["state"] == "interrupted"
    assert "gateway_restarted" in row["error"]
