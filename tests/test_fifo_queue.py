from __future__ import annotations

import asyncio
import json

import httpx
import pytest

from app.queue import ClientDisconnectedBeforeUpstream, GlobalFIFOQueue
from .conftest import gateway_harness

pytestmark = pytest.mark.asyncio


async def test_concurrent_inference_requests_are_strict_fifo_and_max_one(tmp_path):
    active = 0
    max_active = 0
    order: list[int] = []
    lock = asyncio.Lock()

    async def handler(request: httpx.Request) -> httpx.Response:
        nonlocal active, max_active
        payload = json.loads((await request.aread()).decode("utf-8"))
        async with lock:
            active += 1
            max_active = max(max_active, active)
            order.append(payload["sequence"])
        try:
            await asyncio.sleep(0.04)
            return httpx.Response(
                200,
                json={
                    "id": payload["sequence"],
                    "usage": {
                        "prompt_tokens": 1,
                        "completion_tokens": 1,
                        "total_tokens": 2,
                    },
                },
            )
        finally:
            async with lock:
                active -= 1

    async with gateway_harness(tmp_path, handler) as harness:
        headers = {"Authorization": f"Bearer {harness.keys['hermes']}"}

        async def send(sequence: int, delay: float):
            await asyncio.sleep(delay)
            return await harness.client.post(
                "/v1/chat/completions",
                headers=headers,
                json={"model": "auto", "sequence": sequence},
            )

        responses = await asyncio.gather(
            send(1, 0.00),
            send(2, 0.01),
            send(3, 0.02),
        )

    assert [response.status_code for response in responses] == [200, 200, 200]
    assert max_active == 1
    assert order == [1, 2, 3]


async def test_queued_disconnect_removes_ticket_before_upstream():
    queue = GlobalFIFOQueue(poll_seconds=0.005)

    async def connected() -> bool:
        return False

    first = await queue.acquire("first", connected)
    checks = 0

    async def disconnect_soon() -> bool:
        nonlocal checks
        checks += 1
        return checks >= 2

    waiting = asyncio.create_task(queue.acquire("second", disconnect_soon))
    with pytest.raises(ClientDisconnectedBeforeUpstream):
        await waiting

    depth, busy, active = await queue.snapshot()
    assert depth == 0
    assert busy is True
    assert active == "first"

    await first.release()
    depth, busy, active = await queue.snapshot()
    assert (depth, busy, active) == (0, False, None)


async def test_models_bypasses_generation_queue(tmp_path):
    generation_started = asyncio.Event()
    release_generation = asyncio.Event()
    models_seen = asyncio.Event()

    async def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/v1/chat/completions":
            generation_started.set()
            await release_generation.wait()
            return httpx.Response(200, json={"usage": {"total_tokens": 0}})
        if request.url.path == "/v1/models":
            models_seen.set()
            return httpx.Response(200, json={"object": "list", "data": []})
        raise AssertionError(request.url.path)

    async with gateway_harness(tmp_path, handler) as harness:
        headers = {"Authorization": f"Bearer {harness.keys['hermes']}"}
        generation_task = asyncio.create_task(
            harness.client.post(
                "/v1/chat/completions",
                headers=headers,
                json={"model": "auto", "messages": []},
            )
        )
        await asyncio.wait_for(generation_started.wait(), timeout=1)

        models_response = await asyncio.wait_for(
            harness.client.get("/v1/models", headers=headers), timeout=1
        )
        assert models_seen.is_set()
        assert models_response.status_code == 200

        release_generation.set()
        generation_response = await generation_task
        assert generation_response.status_code == 200


async def test_all_four_inference_endpoints_share_the_same_global_queue(tmp_path):
    active = 0
    max_active = 0
    order: list[int] = []

    async def handler(request: httpx.Request) -> httpx.Response:
        nonlocal active, max_active
        payload = json.loads((await request.aread()).decode("utf-8"))
        active += 1
        max_active = max(max_active, active)
        order.append(payload["sequence"])
        try:
            await asyncio.sleep(0.02)
            if request.url.path == "/v1/messages/count_tokens":
                return httpx.Response(200, json={"input_tokens": payload["sequence"]})
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
        finally:
            active -= 1

    async with gateway_harness(tmp_path, handler) as harness:
        bearer = {"Authorization": f"Bearer {harness.keys['hermes']}"}
        anthropic = {
            "x-api-key": harness.keys["hermes"],
            "anthropic-version": "2023-06-01",
        }
        requests = [
            ("/v1/chat/completions", bearer, {"model": "auto", "sequence": 1}),
            ("/v1/responses", bearer, {"model": "auto", "sequence": 2}),
            (
                "/v1/messages",
                anthropic,
                {"model": "auto", "max_tokens": 1, "messages": [], "sequence": 3},
            ),
            (
                "/v1/messages/count_tokens",
                anthropic,
                {"model": "auto", "messages": [], "sequence": 4},
            ),
        ]

        async def send(item, delay):
            await asyncio.sleep(delay)
            endpoint, headers, body = item
            return await harness.client.post(endpoint, headers=headers, json=body)

        responses = await asyncio.gather(
            *(send(item, index * 0.005) for index, item in enumerate(requests))
        )

    assert all(response.status_code == 200 for response in responses)
    assert max_active == 1
    assert order == [1, 2, 3, 4]


async def test_gateway_drops_disconnected_queued_request_without_upstream(tmp_path):
    from starlette.requests import Request

    from app.db import Database
    from app.proxy import GatewayProxy
    from app.queue import GlobalFIFOQueue
    from .conftest import make_settings

    upstream_calls = 0

    async def handler(request: httpx.Request) -> httpx.Response:
        nonlocal upstream_calls
        upstream_calls += 1
        return httpx.Response(200, json={})

    settings = make_settings(tmp_path)
    settings.prepare_directories()
    database = Database(settings.db_path)
    database.initialize()
    gateway_key, _ = database.create_key("queued-client")
    queue = GlobalFIFOQueue(poll_seconds=0.005)

    async def connected() -> bool:
        return False

    blocker = await queue.acquire("blocker", connected)
    upstream_client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    proxy = GatewayProxy(
        settings=settings,
        database=database,
        queue=queue,
        client=upstream_client,
    )

    body = b'{"model":"auto","messages":[]}'
    receive_events = [
        {"type": "http.request", "body": body, "more_body": False},
        {"type": "http.disconnect"},
    ]

    async def receive():
        if receive_events:
            return receive_events.pop(0)
        await asyncio.sleep(3600)
        raise AssertionError("unreachable")

    scope = {
        "type": "http",
        "asgi": {"version": "3.0"},
        "http_version": "1.1",
        "method": "POST",
        "scheme": "http",
        "path": "/v1/chat/completions",
        "raw_path": b"/v1/chat/completions",
        "query_string": b"",
        "root_path": "",
        "headers": [
            (b"authorization", f"Bearer {gateway_key}".encode("ascii")),
            (b"content-type", b"application/json"),
            (b"content-length", str(len(body)).encode("ascii")),
        ],
        "client": ("127.0.0.1", 12345),
        "server": ("gateway.test", 80),
    }

    try:
        response = await asyncio.wait_for(
            proxy.handle(Request(scope, receive)), timeout=1
        )
        depth, busy, active = await queue.snapshot()
    finally:
        await blocker.release()
        await upstream_client.aclose()

    assert response.status_code == 499
    assert upstream_calls == 0
    assert (depth, busy, active) == (0, True, "blocker")
