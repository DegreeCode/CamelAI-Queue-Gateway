from __future__ import annotations

import asyncio

import httpx
import pytest

from app import proxy as proxy_module
from app.db import Database
from app.proxy import _read_request_metadata
from .conftest import gateway_harness


def test_request_metadata_scanner_does_not_load_large_json_body(tmp_path, monkeypatch):
    body_path = tmp_path / "large-request.json"
    body_path.write_bytes(
        b'{"messages":[{"role":"user","content":"'
        + (b"x" * (2 * 1024 * 1024))
        + b'"}],"model":"test-model","stream":true}'
    )

    def forbidden_json_load(*args, **kwargs):
        raise AssertionError("large request metadata must not use json.load")

    monkeypatch.setattr(proxy_module.json, "load", forbidden_json_load)
    assert _read_request_metadata(body_path) == ("test-model", True)


@pytest.mark.asyncio
async def test_transient_audit_update_failure_does_not_wedge_fifo(
    tmp_path, monkeypatch
):
    calls: list[int] = []

    async def handler(request: httpx.Request) -> httpx.Response:
        calls.append(1)
        await asyncio.sleep(0.01)
        return httpx.Response(200, json={"usage": {"total_tokens": 0}})

    original_update = Database.update_request
    failed_once = False

    def flaky_update(self: Database, request_id: str, **values):
        nonlocal failed_once
        if values.get("state") == "running" and not failed_once:
            failed_once = True
            raise RuntimeError("simulated audit write failure")
        return original_update(self, request_id, **values)

    monkeypatch.setattr(Database, "update_request", flaky_update)

    async with gateway_harness(tmp_path, handler) as harness:
        headers = {"Authorization": f"Bearer {harness.keys['hermes']}"}
        responses = await asyncio.gather(
            harness.client.post(
                "/v1/chat/completions",
                headers=headers,
                json={"model": "auto", "sequence": 1},
            ),
            harness.client.post(
                "/v1/chat/completions",
                headers=headers,
                json={"model": "auto", "sequence": 2},
            ),
        )
        depth, busy, active = await harness.client._transport.app.state.queue.snapshot()

    assert failed_once is True
    assert len(calls) == 2
    assert [response.status_code for response in responses] == [200, 200]
    assert (depth, busy, active) == (0, False, None)


def test_receiving_request_is_inflight_and_visible_in_queue_stats(tmp_path):
    database = Database(tmp_path / "gateway.db")
    database.initialize()
    _, key = database.create_key("hermes")
    database.insert_request(
        {
            "request_id": "receiving-1",
            "gateway_key_id": key.id,
            "gateway_key_name": key.name,
            "endpoint": "/v1/chat/completions",
            "method": "POST",
            "received_at": "2026-08-29T00:00:00.000Z",
            "queue_started_at": "2026-08-29T00:00:00.000Z",
            "state": "receiving",
        }
    )

    stats = database.stats()
    total = stats["total"]
    assert stats["queue_depth"] == 1
    assert total["request_count"] == 1
    assert total["inflight_count"] == 1
    assert total["failure_count"] == 0
    assert total["usage_unknown_requests"] == 0
    assert database.queue_rows()[0]["state"] == "receiving"


@pytest.mark.asyncio
async def test_runtime_log_directory_failure_does_not_break_proxy_stream(tmp_path):
    import json
    import shutil

    raw = b'{"ok":true,"usage":{"input_tokens":1,"output_tokens":2}}'

    async def handler(request: httpx.Request) -> httpx.Response:
        await request.aread()
        return httpx.Response(
            200,
            headers={"content-type": "application/json"},
            content=raw,
        )

    async with gateway_harness(tmp_path, handler) as harness:
        shutil.rmtree(harness.settings.log_dir)
        harness.settings.log_dir.write_text("not a directory", encoding="utf-8")

        response = await harness.client.post(
            "/v1/responses",
            headers={"Authorization": f"Bearer {harness.keys['hermes']}"},
            json={"model": "auto", "input": "hello"},
        )
        row = harness.database.get_request(
            response.headers["x-camel-gateway-request-id"]
        )

    assert response.status_code == 200
    assert response.content == raw
    assert row is not None
    assert row["state"] == "completed"
    assert "request_log_directory_error" in (row["error"] or "")
    assert "response_log_open_error" in (row["error"] or "")
    assert row["usage_source"] == "unavailable"
    assert json.loads(row["response_headers_json"])["content-type"] == (
        "application/json"
    )


@pytest.mark.asyncio
async def test_cancelled_request_upload_releases_fifo_reservation(tmp_path):
    from starlette.requests import Request

    from app.proxy import GatewayProxy
    from app.queue import GlobalFIFOQueue
    from .conftest import make_settings

    settings = make_settings(tmp_path)
    settings.prepare_directories()
    database = Database(settings.db_path)
    database.initialize()
    plaintext, _ = database.create_key("cancel-upload")
    queue = GlobalFIFOQueue(poll_seconds=0.005)
    upstream_calls = 0

    async def upstream_handler(request: httpx.Request) -> httpx.Response:
        nonlocal upstream_calls
        upstream_calls += 1
        return httpx.Response(200, json={})

    client = httpx.AsyncClient(transport=httpx.MockTransport(upstream_handler))
    proxy = GatewayProxy(
        settings=settings,
        database=database,
        queue=queue,
        client=client,
    )

    first_chunk_seen = asyncio.Event()
    never = asyncio.Event()
    receive_calls = 0

    async def receive():
        nonlocal receive_calls
        receive_calls += 1
        if receive_calls == 1:
            first_chunk_seen.set()
            return {
                "type": "http.request",
                "body": b'{"model":"auto",',
                "more_body": True,
            }
        await never.wait()
        raise AssertionError("unreachable")

    scope = {
        "type": "http",
        "asgi": {"version": "3.0", "spec_version": "2.3"},
        "http_version": "1.1",
        "method": "POST",
        "scheme": "http",
        "path": "/v1/chat/completions",
        "raw_path": b"/v1/chat/completions",
        "query_string": b"",
        "root_path": "",
        "headers": [
            (b"authorization", f"Bearer {plaintext}".encode("ascii")),
            (b"content-type", b"application/json"),
        ],
        "client": ("127.0.0.1", 12345),
        "server": ("gateway.test", 80),
    }
    request = Request(scope, receive)

    task = asyncio.create_task(proxy.handle(request))
    await asyncio.wait_for(first_chunk_seen.wait(), timeout=0.2)
    await asyncio.sleep(0)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task

    depth, busy, active = await queue.snapshot()
    rows = database.history_rows(limit=10)
    await client.aclose()

    assert (depth, busy, active) == (0, False, None)
    assert upstream_calls == 0
    assert rows[0]["state"] == "interrupted"
    assert rows[0]["client_disconnected"] == 0


@pytest.mark.asyncio
async def test_cancelled_finalize_caller_cannot_leak_fifo_lease(tmp_path):
    import time

    from app.proxy import ResponseSession
    from app.queue import GlobalFIFOQueue

    queue = GlobalFIFOQueue(poll_seconds=0.005)

    async def connected() -> bool:
        return False

    first_lease = await queue.acquire("first", connected)
    second_waiter = asyncio.create_task(queue.acquire("second", connected))

    close_started = asyncio.Event()
    allow_close = asyncio.Event()

    class SlowCloseStream(httpx.AsyncByteStream):
        async def __aiter__(self):
            if False:
                yield b""

        async def aclose(self) -> None:
            close_started.set()
            await allow_close.wait()

    class ConnectedRequest:
        async def is_disconnected(self) -> bool:
            return False

    database = Database(tmp_path / "finalize.db")
    database.initialize()
    upstream = httpx.Response(200, stream=SlowCloseStream())
    session = ResponseSession(
        database=database,
        request=ConnectedRequest(),
        request_id="first",
        endpoint="/v1/responses",
        upstream_response=upstream,
        response_path=tmp_path / "missing.response",
        lease=first_lease,
        received_monotonic=time.monotonic(),
        upstream_started_monotonic=time.monotonic(),
        queue_wait_ms=0.0,
        retry_count=0,
        upstream_queue_ms=None,
        camel_stream_limit=None,
        poll_seconds=0.005,
        initial_error=None,
    )

    caller = asyncio.create_task(session.finalize())
    await asyncio.wait_for(close_started.wait(), timeout=0.2)
    caller.cancel()
    with pytest.raises(asyncio.CancelledError):
        await caller

    assert second_waiter.done() is False
    allow_close.set()
    second_lease = await asyncio.wait_for(second_waiter, timeout=0.2)
    await second_lease.release()
    depth, busy, active = await queue.snapshot()

    assert session._finalized is True
    assert (depth, busy, active) == (0, False, None)
