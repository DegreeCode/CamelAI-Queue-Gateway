from __future__ import annotations

import json
from pathlib import Path

import httpx
import pytest

from .conftest import ChunkedStream, gateway_harness

pytestmark = pytest.mark.asyncio


def split_bytes(data: bytes, widths: tuple[int, ...] = (1, 5, 2, 13, 3, 8)) -> list[bytes]:
    chunks: list[bytes] = []
    index = 0
    width_index = 0
    while index < len(data):
        width = widths[width_index % len(widths)]
        chunks.append(data[index : index + width])
        index += width
        width_index += 1
    return chunks


async def test_openai_and_anthropic_sse_are_raw_passthrough_with_split_events(tmp_path):
    payloads = {
        "/v1/chat/completions": (
            b'data: {"id":"chat","choices":[]}\n\n'
            b'data: {"id":"chat","choices":[],"usage":{"prompt_tokens":5,'
            b'"completion_tokens":2,"total_tokens":7}}\n\n'
            b"data: [DONE]\n\n"
        ),
        "/v1/responses": (
            b"event: response.completed\n"
            b'data: {"type":"response.completed","response":{"usage":{'
            b'"input_tokens":6,"output_tokens":3,"total_tokens":9,'
            b'"input_tokens_details":{"cached_tokens":2},'
            b'"output_tokens_details":{"reasoning_tokens":1}}}}\n\n'
        ),
        "/v1/messages": (
            b"event: message_start\n"
            b'data: {"type":"message_start","message":{"usage":{'
            b'"input_tokens":4,"output_tokens":0,"cache_read_input_tokens":1}}}\n\n'
            b"event: message_delta\n"
            b'data: {"type":"message_delta","usage":{"output_tokens":5}}\n\n'
        ),
    }

    async def handler(request: httpx.Request) -> httpx.Response:
        raw = payloads[request.url.path]
        return httpx.Response(
            200,
            headers={
                "content-type": "text/event-stream; charset=utf-8",
                "x-camel-queue-ms": "17",
                "x-camel-stream-limit": "1",
            },
            stream=ChunkedStream(split_bytes(raw)),
        )

    async with gateway_harness(tmp_path, handler) as harness:
        for endpoint, raw in payloads.items():
            if endpoint == "/v1/messages":
                headers = {
                    "x-api-key": harness.keys["hermes"],
                    "anthropic-version": "2023-06-01",
                }
                body = {
                    "model": "auto",
                    "max_tokens": 10,
                    "messages": [],
                    "stream": True,
                }
            else:
                headers = {
                    "Authorization": f"Bearer {harness.keys['hermes']}"
                }
                body = {"model": "auto", "stream": True}

            response = await harness.client.post(endpoint, headers=headers, json=body)
            assert response.status_code == 200
            assert response.content == raw
            assert response.headers["content-type"].startswith("text/event-stream")
            assert response.headers["x-camel-queue-ms"] == "17"
            assert response.headers["x-camel-stream-limit"] == "1"
            assert response.headers["x-camel-gateway-request-id"]

            row = harness.database.get_request(
                response.headers["x-camel-gateway-request-id"]
            )
            assert row is not None
            assert Path(row["response_path"]).read_bytes() == raw
            assert row["upstream_queue_ms"] == 17.0
            assert row["camel_stream_limit"] == 1
            assert row["state"] == "completed"

        rows = harness.database.history_rows(limit=10)

    by_endpoint = {row["endpoint"]: row for row in rows}
    assert (
        by_endpoint["/v1/chat/completions"]["input_tokens"],
        by_endpoint["/v1/chat/completions"]["output_tokens"],
        by_endpoint["/v1/chat/completions"]["total_tokens"],
    ) == (5, 2, 7)
    assert (
        by_endpoint["/v1/responses"]["input_tokens"],
        by_endpoint["/v1/responses"]["output_tokens"],
        by_endpoint["/v1/responses"]["total_tokens"],
    ) == (6, 3, 9)
    assert (
        by_endpoint["/v1/messages"]["input_tokens"],
        by_endpoint["/v1/messages"]["output_tokens"],
        by_endpoint["/v1/messages"]["total_tokens"],
    ) == (4, 5, 9)


async def test_key_usage_totals_unknown_usage_and_count_tokens_are_separate(tmp_path):
    async def handler(request: httpx.Request) -> httpx.Response:
        payload = json.loads((await request.aread()).decode("utf-8") or "{}")
        case = payload.get("case")
        if request.url.path == "/v1/messages/count_tokens":
            return httpx.Response(200, json={"input_tokens": 123})
        if case == "openai":
            return httpx.Response(
                200,
                json={
                    "id": "a",
                    "usage": {
                        "prompt_tokens": 10,
                        "completion_tokens": 4,
                        "total_tokens": 14,
                        "prompt_tokens_details": {"cached_tokens": 2},
                    },
                },
            )
        if case == "anthropic":
            return httpx.Response(
                200,
                json={
                    "id": "b",
                    "usage": {
                        "input_tokens": 7,
                        "output_tokens": 3,
                        "cache_read_input_tokens": 1,
                    },
                },
            )
        return httpx.Response(200, json={"id": "no-usage"})

    async with gateway_harness(
        tmp_path, handler, key_names=("hermes", "codex")
    ) as harness:
        await harness.client.post(
            "/v1/chat/completions",
            headers={"Authorization": f"Bearer {harness.keys['hermes']}"},
            json={"model": "auto", "case": "openai"},
        )
        await harness.client.post(
            "/v1/responses",
            headers={"Authorization": f"Bearer {harness.keys['hermes']}"},
            json={"model": "auto", "case": "missing"},
        )
        await harness.client.post(
            "/v1/messages",
            headers={
                "x-api-key": harness.keys["codex"],
                "anthropic-version": "2023-06-01",
            },
            json={"model": "auto", "case": "anthropic", "messages": []},
        )
        count_response = await harness.client.post(
            "/v1/messages/count_tokens",
            headers={
                "x-api-key": harness.keys["codex"],
                "anthropic-version": "2023-06-01",
            },
            json={"model": "auto", "messages": []},
        )
        stats = harness.database.stats()
        count_row = harness.database.get_request(
            count_response.headers["x-camel-gateway-request-id"]
        )

    by_key = {item["key_name"]: item for item in stats["keys"]}
    assert by_key["hermes"]["reported_input_tokens"] == 10
    assert by_key["hermes"]["reported_output_tokens"] == 4
    assert by_key["hermes"]["reported_total_tokens"] == 14
    assert by_key["hermes"]["usage_reported_requests"] == 1
    assert by_key["hermes"]["usage_unknown_requests"] == 1
    assert by_key["hermes"]["usage_coverage_percent"] == 50.0

    assert by_key["codex"]["reported_input_tokens"] == 7
    assert by_key["codex"]["reported_output_tokens"] == 3
    assert by_key["codex"]["reported_total_tokens"] == 10
    assert by_key["codex"]["token_count_queries"] == 1
    assert by_key["codex"]["counted_input_tokens"] == 123

    total = stats["total"]
    assert total["reported_input_tokens"] == 17
    assert total["reported_output_tokens"] == 7
    assert total["reported_total_tokens"] == 24
    assert total["usage_reported_requests"] == 2
    assert total["usage_unknown_requests"] == 1
    assert total["usage_coverage_percent"] == 66.67
    assert total["token_count_queries"] == 1
    assert total["counted_input_tokens"] == 123
    assert count_row is not None
    assert count_row["counted_input_tokens"] == 123
    assert count_row["input_tokens"] is None
    assert count_row["total_tokens"] is None


async def test_request_response_logging_and_secret_redaction(tmp_path):
    upstream_body = b'{"ok":true,"usage":{"input_tokens":1,"output_tokens":2}}'

    seen_request_bodies: list[bytes] = []

    async def handler(request: httpx.Request) -> httpx.Response:
        seen_request_bodies.append(await request.aread())
        return httpx.Response(
            200,
            headers={"content-type": "application/json"},
            content=upstream_body,
        )

    async with gateway_harness(tmp_path, handler) as harness:
        gateway_key = harness.keys["hermes"]
        request_body = b'{"model":"auto","input":"hello"}'
        response = await harness.client.post(
            "/v1/responses",
            headers={
                "Authorization": f"Bearer {gateway_key}",
                "Content-Type": "application/json",
                "X-Secret-Token": "client-side-secret",
                "Cf-Access-Jwt-Assertion": "jwt-secret",
            },
            content=request_body,
        )
        row = harness.database.get_request(
            response.headers["x-camel-gateway-request-id"]
        )

    assert response.content == upstream_body
    assert seen_request_bodies == [request_body]
    assert row is not None
    assert Path(row["request_path"]).read_bytes() == request_body
    assert Path(row["response_path"]).read_bytes() == upstream_body
    assert gateway_key not in row["request_headers_json"]
    assert "client-side-secret" not in row["request_headers_json"]
    assert "jwt-secret" not in row["request_headers_json"]
    assert "[REDACTED]" in row["request_headers_json"]
    assert "qaml_live_test_secret" not in row["request_headers_json"]

    database_bytes = b"".join(
        path.read_bytes()
        for path in row_path_candidates(harness.settings.db_path)
        if path.exists()
    )
    assert gateway_key.encode() not in database_bytes
    assert b"qaml_live_test_secret" not in database_bytes


def row_path_candidates(database_path: Path) -> list[Path]:
    return [
        database_path,
        Path(str(database_path) + "-wal"),
        Path(str(database_path) + "-shm"),
    ]


async def test_response_session_yields_first_upstream_chunk_without_buffering(tmp_path):
    import asyncio
    import time

    from app.db import Database
    from app.proxy import ResponseSession

    gate = asyncio.Event()

    class GatedStream(httpx.AsyncByteStream):
        async def __aiter__(self):
            yield b'data: {"part":1}\n\n'
            await gate.wait()
            yield b'data: {"part":2}\n\n'

        async def aclose(self) -> None:
            return None

    class ConnectedRequest:
        async def is_disconnected(self) -> bool:
            return False

    database = Database(tmp_path / "gateway.db")
    database.initialize()
    _, key = database.create_key("stream-client")
    database.insert_request(
        {
            "request_id": "stream-1",
            "gateway_key_id": key.id,
            "gateway_key_name": key.name,
            "endpoint": "/v1/responses",
            "method": "POST",
            "received_at": "2026-08-29T00:00:00.000Z",
            "state": "running",
        }
    )
    upstream = httpx.Response(
        200,
        headers={"content-type": "text/event-stream"},
        stream=GatedStream(),
    )
    session = ResponseSession(
        database=database,
        request=ConnectedRequest(),
        request_id="stream-1",
        endpoint="/v1/responses",
        upstream_response=upstream,
        response_path=tmp_path / "stream.response",
        lease=None,
        received_monotonic=time.monotonic(),
        upstream_started_monotonic=time.monotonic(),
        queue_wait_ms=0.0,
        retry_count=0,
        upstream_queue_ms=None,
        camel_stream_limit=None,
        poll_seconds=0.005,
        initial_error=None,
    )

    iterator = session.iter_body().__aiter__()
    first = await asyncio.wait_for(anext(iterator), timeout=0.2)
    assert first == b'data: {"part":1}\n\n'

    pending_second = asyncio.create_task(anext(iterator))
    await asyncio.sleep(0.02)
    assert pending_second.done() is False
    gate.set()
    assert await asyncio.wait_for(pending_second, timeout=0.2) == (
        b'data: {"part":2}\n\n'
    )
    with pytest.raises(StopAsyncIteration):
        await anext(iterator)


async def test_response_log_open_failure_does_not_break_passthrough(tmp_path):
    import time

    from app.db import Database
    from app.proxy import ResponseSession

    class ConnectedRequest:
        async def is_disconnected(self) -> bool:
            return False

    database = Database(tmp_path / "gateway-log-failure.db")
    database.initialize()
    _, key = database.create_key("log-failure-client")
    database.insert_request(
        {
            "request_id": "stream-log-failure",
            "gateway_key_id": key.id,
            "gateway_key_name": key.name,
            "endpoint": "/v1/chat/completions",
            "method": "POST",
            "received_at": "2026-08-29T00:00:00.000Z",
            "state": "running",
        }
    )

    blocked_parent = tmp_path / "not-a-directory"
    blocked_parent.write_bytes(b"file")
    raw = b'data: {"usage":{"prompt_tokens":1,"completion_tokens":2}}\n\n'
    upstream = httpx.Response(
        200,
        headers={"content-type": "text/event-stream"},
        content=raw,
    )
    session = ResponseSession(
        database=database,
        request=ConnectedRequest(),
        request_id="stream-log-failure",
        endpoint="/v1/chat/completions",
        upstream_response=upstream,
        response_path=blocked_parent / "response.log",
        lease=None,
        received_monotonic=time.monotonic(),
        upstream_started_monotonic=time.monotonic(),
        queue_wait_ms=0.0,
        retry_count=0,
        upstream_queue_ms=None,
        camel_stream_limit=None,
        poll_seconds=0.005,
        initial_error=None,
    )

    received = b"".join([chunk async for chunk in session.iter_body()])
    row = database.get_request("stream-log-failure")

    assert received == raw
    assert row is not None
    assert row["state"] == "completed"
    assert "response_log_open_error" in row["error"]


async def test_streaming_disconnect_cancels_upstream_and_releases_fifo(tmp_path):
    import asyncio
    import time

    from app.db import Database
    from app.proxy import ResponseSession
    from app.queue import GlobalFIFOQueue

    queue = GlobalFIFOQueue(poll_seconds=0.005)

    async def connected() -> bool:
        return False

    lease = await queue.acquire("disconnect-stream", connected)
    stream_closed = asyncio.Event()
    never = asyncio.Event()

    class BlockingStream(httpx.AsyncByteStream):
        async def __aiter__(self):
            await never.wait()
            yield b"unreachable"

        async def aclose(self) -> None:
            stream_closed.set()

    class DisconnectedRequest:
        async def is_disconnected(self) -> bool:
            return True

    database = Database(tmp_path / "disconnect-stream.db")
    database.initialize()
    _, key = database.create_key("disconnect-stream-client")
    database.insert_request(
        {
            "request_id": "disconnect-stream",
            "gateway_key_id": key.id,
            "gateway_key_name": key.name,
            "endpoint": "/v1/responses",
            "method": "POST",
            "received_at": "2026-08-29T00:00:00.000Z",
            "state": "running",
        }
    )
    upstream = httpx.Response(
        200,
        headers={"content-type": "text/event-stream"},
        stream=BlockingStream(),
    )
    session = ResponseSession(
        database=database,
        request=DisconnectedRequest(),
        request_id="disconnect-stream",
        endpoint="/v1/responses",
        upstream_response=upstream,
        response_path=tmp_path / "disconnect-stream.response",
        lease=lease,
        received_monotonic=time.monotonic(),
        upstream_started_monotonic=time.monotonic(),
        queue_wait_ms=0.0,
        retry_count=0,
        upstream_queue_ms=None,
        camel_stream_limit=None,
        poll_seconds=0.005,
        initial_error=None,
    )

    received = [chunk async for chunk in session.iter_body()]
    depth, busy, active = await queue.snapshot()
    row = database.get_request("disconnect-stream")

    assert received == []
    assert stream_closed.is_set()
    assert (depth, busy, active) == (0, False, None)
    assert row is not None
    assert row["state"] == "interrupted"
    assert row["client_disconnected"] == 1

async def test_sse_done_stops_hanging_upstream_and_records_normal_completion(tmp_path):
    import asyncio
    import time

    from app.db import Database
    from app.proxy import ResponseSession

    release = asyncio.Event()
    stream_closed = asyncio.Event()

    class HangingAfterDoneStream(httpx.AsyncByteStream):
        async def __aiter__(self):
            yield b'data: {"id":"chat","usage":{"prompt_tokens":2,' \
                b'"completion_tokens":1,"total_tokens":3}}\n\n'
            # Split [DONE] across HTTP chunks to exercise incremental SSE parsing.
            yield b"data: [DO"
            yield b"NE]\n\n"
            await release.wait()
            yield b"unreachable"

        async def aclose(self) -> None:
            stream_closed.set()
            release.set()

    class ConnectedRequest:
        async def is_disconnected(self) -> bool:
            return False

    database = Database(tmp_path / "done-stream.db")
    database.initialize()
    _, key = database.create_key("done-stream-client")
    database.insert_request(
        {
            "request_id": "done-stream",
            "gateway_key_id": key.id,
            "gateway_key_name": key.name,
            "endpoint": "/v1/chat/completions",
            "method": "POST",
            "received_at": "2026-08-29T00:00:00.000Z",
            "state": "running",
        }
    )

    upstream = httpx.Response(
        200,
        headers={"content-type": "text/event-stream"},
        stream=HangingAfterDoneStream(),
    )
    response_path = tmp_path / "done-stream.response"
    session = ResponseSession(
        database=database,
        request=ConnectedRequest(),
        request_id="done-stream",
        endpoint="/v1/chat/completions",
        upstream_response=upstream,
        response_path=response_path,
        lease=None,
        received_monotonic=time.monotonic(),
        upstream_started_monotonic=time.monotonic(),
        queue_wait_ms=0.0,
        retry_count=0,
        upstream_queue_ms=None,
        camel_stream_limit=None,
        poll_seconds=0.005,
        initial_error=None,
    )

    async def collect() -> list[bytes]:
        return [chunk async for chunk in session.iter_body()]

    received = await asyncio.wait_for(collect(), timeout=0.25)
    row = database.get_request("done-stream")

    expected = (
        b'data: {"id":"chat","usage":{"prompt_tokens":2,'
        b'"completion_tokens":1,"total_tokens":3}}\n\n'
        b"data: [DONE]\n\n"
    )
    assert b"".join(received) == expected
    assert response_path.read_bytes() == expected
    assert stream_closed.is_set()
    assert row is not None
    assert row["state"] == "completed"
    assert row["client_disconnected"] == 0
    assert row["error"] is None
    assert row["input_tokens"] == 2
    assert row["output_tokens"] == 1
    assert row["total_tokens"] == 3


async def test_cancel_after_sse_done_is_not_recorded_as_client_disconnect(tmp_path):
    import asyncio
    import time

    from app.db import Database
    from app.proxy import ResponseSession

    class DoneStream(httpx.AsyncByteStream):
        async def __aiter__(self):
            yield b"data: [DONE]\n\n"
            await asyncio.Event().wait()

        async def aclose(self) -> None:
            return None

    class ConnectedRequest:
        async def is_disconnected(self) -> bool:
            return False

    database = Database(tmp_path / "done-cancel.db")
    database.initialize()
    _, key = database.create_key("done-cancel-client")
    database.insert_request(
        {
            "request_id": "done-cancel",
            "gateway_key_id": key.id,
            "gateway_key_name": key.name,
            "endpoint": "/v1/chat/completions",
            "method": "POST",
            "received_at": "2026-08-29T00:00:00.000Z",
            "state": "running",
        }
    )

    upstream = httpx.Response(
        200,
        headers={"content-type": "text/event-stream"},
        stream=DoneStream(),
    )
    session = ResponseSession(
        database=database,
        request=ConnectedRequest(),
        request_id="done-cancel",
        endpoint="/v1/chat/completions",
        upstream_response=upstream,
        response_path=tmp_path / "done-cancel.response",
        lease=None,
        received_monotonic=time.monotonic(),
        upstream_started_monotonic=time.monotonic(),
        queue_wait_ms=0.0,
        retry_count=0,
        upstream_queue_ms=None,
        camel_stream_limit=None,
        poll_seconds=0.005,
        initial_error=None,
    )

    iterator = session.iter_body().__aiter__()
    assert await anext(iterator) == b"data: [DONE]\n\n"

    with pytest.raises(asyncio.CancelledError):
        await iterator.athrow(asyncio.CancelledError())

    row = database.get_request("done-cancel")
    assert row is not None
    assert row["state"] == "completed"
    assert row["client_disconnected"] == 0
    assert row["error"] is None
