from __future__ import annotations

import asyncio
import email.utils
import errno
import json
import os
import shutil
import time
import uuid
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, AsyncIterator

import httpx
from starlette.background import BackgroundTask
from starlette.requests import ClientDisconnect, Request
from starlette.responses import JSONResponse, Response, StreamingResponse

from .auth import AuthenticationError, authenticate_request
from .config import Settings
from .db import Database
from .headers import (
    build_upstream_headers,
    filter_response_headers,
    redacted_headers_json,
)
from .queue import (
    ClientDisconnectedBeforeUpstream,
    GlobalFIFOQueue,
    QueueLease,
    QueueReservation,
)
from .timeutil import elapsed_ms, utc_iso, utc_now
from .usage import SSEUsageParser, UsageResult, parse_json_usage_file


@dataclass(frozen=True, slots=True)
class EndpointPolicy:
    method: str
    queued: bool


ALLOWED_ENDPOINTS: dict[str, EndpointPolicy] = {
    "/v1/chat/completions": EndpointPolicy(method="POST", queued=True),
    "/v1/responses": EndpointPolicy(method="POST", queued=True),
    "/v1/messages": EndpointPolicy(method="POST", queued=True),
    "/v1/messages/count_tokens": EndpointPolicy(method="POST", queued=True),
    "/v1/models": EndpointPolicy(method="GET", queued=False),
}

_RETRYABLE_STATUSES = {429, 502, 503}
_SAFE_RETRY_ENDPOINTS = {"/v1/models", "/v1/messages/count_tokens"}


class UpstreamConnectionFailure(Exception):
    def __init__(self, message: str, retry_count: int) -> None:
        super().__init__(message)
        self.retry_count = retry_count


class ClientDisconnectedDuringRetry(Exception):
    pass


@dataclass(slots=True)
class OpenedUpstream:
    response: httpx.Response
    retry_count: int
    upstream_started_at: str
    upstream_started_monotonic: float


class GatewayProxy:
    def __init__(
        self,
        *,
        settings: Settings,
        database: Database,
        queue: GlobalFIFOQueue,
        client: httpx.AsyncClient,
    ) -> None:
        self.settings = settings
        self.database = database
        self.queue = queue
        self.client = client

    async def handle(self, request: Request) -> Response:
        endpoint = request.url.path
        policy = ALLOWED_ENDPOINTS.get(endpoint)
        if policy is None or request.method != policy.method:
            return JSONResponse({"detail": "Not Found"}, status_code=404)

        request_id = str(uuid.uuid4())
        received_at_dt = utc_now()
        received_at = utc_iso(received_at_dt)
        received_monotonic = time.monotonic()
        queue_started_at = received_at if policy.queued else None
        queue_started_monotonic = received_monotonic
        reservation: QueueReservation | None = (
            await self.queue.reserve(request_id) if policy.queued else None
        )

        try:
            gateway_key = await authenticate_request(request, self.database)
        except AuthenticationError as exc:
            if reservation is not None:
                await reservation.cancel()
            response = _error_response(
                401, str(exc), error_type="authentication_error"
            )
            response.headers["x-camel-gateway-request-id"] = request_id
            return response
        except asyncio.CancelledError:
            if reservation is not None:
                await reservation.cancel()
            raise
        except Exception:
            if reservation is not None:
                await reservation.cancel()
            response = _error_response(
                500,
                "Unable to validate gateway API key",
                error_type="gateway_error",
            )
            response.headers["x-camel-gateway-request-id"] = request_id
            return response

        date_dir = self.settings.log_dir / received_at_dt.strftime("%Y-%m-%d")
        final_request_path = date_dir / f"{request_id}.request"
        response_path = date_dir / f"{request_id}.response"
        spool_path = self.settings.spool_dir / f"{request_id}.body"
        try:
            spool_path.parent.mkdir(parents=True, exist_ok=True)
            await asyncio.to_thread(
                self.database.insert_request,
                {
                    "request_id": request_id,
                    "gateway_key_id": gateway_key.id,
                    "gateway_key_name": gateway_key.name,
                    "endpoint": endpoint,
                    "method": request.method,
                    "stream": 0,
                    "received_at": received_at,
                    "queue_started_at": queue_started_at,
                    "retry_count": 0,
                    "retry_allowed": int(self._retry_allowed(endpoint)),
                    "client_disconnected": 0,
                    "usage_source": "unavailable",
                    "token_count_query": int(endpoint == "/v1/messages/count_tokens"),
                    "request_path": str(spool_path),
                    "response_path": str(response_path),
                    "request_headers_json": redacted_headers_json(request.headers.raw),
                    "request_bytes": 0,
                    "response_bytes": 0,
                    "state": "receiving",
                },
            )
        except asyncio.CancelledError:
            if reservation is not None:
                await reservation.cancel()
            raise
        except Exception:
            if reservation is not None:
                await reservation.cancel()
            response = _error_response(
                500, "Unable to initialize request audit state", error_type="gateway_error"
            )
            response.headers["x-camel-gateway-request-id"] = request_id
            return response

        content_length = _parse_content_length(request)
        if (
            self.settings.max_request_bytes
            and content_length is not None
            and content_length > self.settings.max_request_bytes
        ):
            return await self._local_failure(
                request_id=request_id,
                received_monotonic=received_monotonic,
                status_code=413,
                message="Request body exceeds GATEWAY_MAX_REQUEST_BYTES",
                state="failed",
                spool_path=spool_path,
                final_request_path=final_request_path,
                response_path=response_path,
                reservation=reservation,
            )

        try:
            body_size = await self._spool_request(request, spool_path)
        except asyncio.CancelledError:
            if reservation is not None:
                await reservation.cancel()
            try:
                await asyncio.to_thread(
                    self.database.update_request,
                    request_id,
                    completed_at=utc_iso(),
                    client_disconnected=0,
                    error="request_cancelled_while_uploading",
                    state="interrupted",
                )
            except Exception:
                pass
            raise
        except ClientDisconnect:
            return await self._local_failure(
                request_id=request_id,
                received_monotonic=received_monotonic,
                status_code=499,
                message="Client disconnected while uploading the request body",
                state="interrupted",
                spool_path=spool_path,
                final_request_path=final_request_path,
                response_path=response_path,
                client_disconnected=True,
                reservation=reservation,
            )
        except ValueError as exc:
            return await self._local_failure(
                request_id=request_id,
                received_monotonic=received_monotonic,
                status_code=413,
                message=str(exc),
                state="failed",
                spool_path=spool_path,
                final_request_path=final_request_path,
                response_path=response_path,
                reservation=reservation,
            )
        except OSError as exc:
            return await self._local_failure(
                request_id=request_id,
                received_monotonic=received_monotonic,
                status_code=507,
                message=f"Unable to spool request body: {exc}",
                state="failed",
                spool_path=spool_path,
                final_request_path=final_request_path,
                response_path=response_path,
                reservation=reservation,
            )
        except Exception as exc:
            return await self._local_failure(
                request_id=request_id,
                received_monotonic=received_monotonic,
                status_code=500,
                message=f"Unable to receive request body: {exc.__class__.__name__}",
                state="failed",
                spool_path=spool_path,
                final_request_path=final_request_path,
                response_path=response_path,
                reservation=reservation,
            )

        audit_error: str | None = None
        try:
            model, is_stream = await asyncio.to_thread(
                _read_request_metadata, spool_path
            )
        except asyncio.CancelledError:
            if reservation is not None:
                await reservation.cancel()
            try:
                await asyncio.to_thread(
                    self.database.update_request,
                    request_id,
                    completed_at=utc_iso(),
                    client_disconnected=0,
                    error="request_cancelled_before_queue_wait",
                    state="interrupted",
                )
            except Exception:
                pass
            raise
        except Exception as exc:
            model, is_stream = None, False
            audit_error = _join_error(
                audit_error,
                f"request_metadata_error:{exc.__class__.__name__}",
            )

        try:
            await asyncio.to_thread(
                self.database.update_request,
                request_id,
                model=model,
                stream=int(is_stream),
                request_bytes=body_size,
                state="queued" if policy.queued else "running",
            )
        except asyncio.CancelledError:
            if reservation is not None:
                await reservation.cancel()
            try:
                await asyncio.to_thread(
                    self.database.update_request,
                    request_id,
                    completed_at=utc_iso(),
                    client_disconnected=0,
                    error="request_cancelled_before_queue_wait",
                    state="interrupted",
                )
            except Exception:
                pass
            raise
        except Exception as exc:
            audit_error = _join_error(
                audit_error,
                f"audit_update_error: request_metadata:{exc.__class__.__name__}",
            )

        lease: QueueLease | None = None
        if policy.queued:
            try:
                assert reservation is not None
                lease = await reservation.wait(request.is_disconnected)
            except ClientDisconnectedBeforeUpstream:
                return await self._local_failure(
                    request_id=request_id,
                    received_monotonic=received_monotonic,
                    status_code=499,
                    message="Client disconnected while waiting in the FIFO queue",
                    state="interrupted",
                    spool_path=spool_path,
                    final_request_path=final_request_path,
                    response_path=response_path,
                    client_disconnected=True,
                )
        elif await request.is_disconnected():
            return await self._local_failure(
                request_id=request_id,
                received_monotonic=received_monotonic,
                status_code=499,
                message="Client disconnected before the upstream request started",
                state="interrupted",
                spool_path=spool_path,
                final_request_path=final_request_path,
                response_path=response_path,
                client_disconnected=True,
            )

        upstream_started_monotonic = time.monotonic()
        queue_wait_ms = (
            elapsed_ms(queue_started_monotonic, upstream_started_monotonic)
            if policy.queued
            else 0.0
        )
        try:
            await asyncio.to_thread(
                self.database.update_request,
                request_id,
                state="running",
                upstream_started_at=utc_iso(),
                queue_wait_ms=queue_wait_ms,
            )
        except asyncio.CancelledError:
            if lease is not None:
                await lease.release()
            raise
        except Exception as exc:
            audit_error = _join_error(
                audit_error,
                f"audit_update_error: queue_start:{exc.__class__.__name__}",
            )

        if await request.is_disconnected():
            if lease is not None:
                await lease.release()
            return await self._local_failure(
                request_id=request_id,
                received_monotonic=received_monotonic,
                status_code=499,
                message="Client disconnected before the upstream request started",
                state="interrupted",
                spool_path=spool_path,
                final_request_path=final_request_path,
                response_path=response_path,
                client_disconnected=True,
                queue_wait_ms=queue_wait_ms,
            )

        try:
            opened = await self._open_upstream(
                request=request,
                endpoint=endpoint,
                spool_path=spool_path,
                body_size=body_size if request.method == "POST" else None,
            )
        except ClientDisconnectedDuringRetry:
            if lease is not None:
                await lease.release()
            return await self._local_failure(
                request_id=request_id,
                received_monotonic=received_monotonic,
                status_code=499,
                message="Client disconnected while waiting to retry upstream",
                state="interrupted",
                spool_path=spool_path,
                final_request_path=final_request_path,
                response_path=response_path,
                client_disconnected=True,
                queue_wait_ms=queue_wait_ms,
            )
        except UpstreamConnectionFailure as exc:
            if lease is not None:
                await lease.release()
            return await self._local_failure(
                request_id=request_id,
                received_monotonic=received_monotonic,
                status_code=502,
                message=str(exc),
                state="failed",
                spool_path=spool_path,
                final_request_path=final_request_path,
                response_path=response_path,
                retry_count=exc.retry_count,
                queue_wait_ms=queue_wait_ms,
            )
        except BaseException:
            if lease is not None:
                await lease.release()
            raise

        try:
            return await self._prepare_downstream_response(
                request=request,
                request_id=request_id,
                endpoint=endpoint,
                spool_path=spool_path,
                final_request_path=final_request_path,
                response_path=response_path,
                opened=opened,
                lease=lease,
                received_monotonic=received_monotonic,
                queue_wait_ms=queue_wait_ms,
                audit_error=audit_error,
            )
        except BaseException:
            try:
                await opened.response.aclose()
            finally:
                if lease is not None:
                    await lease.release()
            raise

    async def _prepare_downstream_response(
        self,
        *,
        request: Request,
        request_id: str,
        endpoint: str,
        spool_path: Path,
        final_request_path: Path,
        response_path: Path,
        opened: OpenedUpstream,
        lease: QueueLease | None,
        received_monotonic: float,
        queue_wait_ms: float,
        audit_error: str | None,
    ) -> Response:
        persisted_request_path, persist_error = await asyncio.to_thread(
            _persist_spool, spool_path, final_request_path
        )
        raw_response_headers = list(opened.response.headers.raw)
        upstream_queue_ms = _header_float(opened.response, "x-camel-queue-ms")
        stream_limit = _header_int(opened.response, "x-camel-stream-limit")
        session_error = _join_error(persist_error, audit_error)
        try:
            await asyncio.to_thread(
                self.database.update_request,
                request_id,
                request_path=str(persisted_request_path),
                response_headers_json=redacted_headers_json(raw_response_headers),
                http_status=opened.response.status_code,
                retry_count=opened.retry_count,
                upstream_started_at=opened.upstream_started_at,
                upstream_queue_ms=upstream_queue_ms,
                camel_stream_limit=stream_limit,
                error=session_error,
            )
        except Exception as exc:
            session_error = _join_error(
                session_error,
                f"audit_update_error: upstream_headers:{exc.__class__.__name__}",
            )

        session = ResponseSession(
            database=self.database,
            request=request,
            request_id=request_id,
            endpoint=endpoint,
            upstream_response=opened.response,
            response_path=response_path,
            lease=lease,
            received_monotonic=received_monotonic,
            upstream_started_monotonic=opened.upstream_started_monotonic,
            queue_wait_ms=queue_wait_ms,
            retry_count=opened.retry_count,
            upstream_queue_ms=upstream_queue_ms,
            camel_stream_limit=stream_limit,
            poll_seconds=self.settings.queue_poll_seconds,
            initial_error=session_error,
        )

        downstream_headers = filter_response_headers(raw_response_headers)
        downstream_headers.append(
            (b"x-camel-gateway-request-id", request_id.encode("ascii"))
        )
        downstream_headers.append(
            (
                b"x-camel-gateway-queue-ms",
                str(int(round(queue_wait_ms))).encode("ascii"),
            )
        )
        response = StreamingResponse(
            session.iter_body(),
            status_code=opened.response.status_code,
            background=BackgroundTask(session.finalize),
        )
        response.raw_headers = downstream_headers
        return response

    async def _spool_request(self, request: Request, path: Path) -> int:
        size = 0
        with path.open("wb") as handle:
            async for chunk in request.stream():
                if not chunk:
                    continue
                size += len(chunk)
                if (
                    self.settings.max_request_bytes
                    and size > self.settings.max_request_bytes
                ):
                    raise ValueError(
                        "Request body exceeds GATEWAY_MAX_REQUEST_BYTES"
                    )
                handle.write(chunk)
        return size

    async def _open_upstream(
        self,
        *,
        request: Request,
        endpoint: str,
        spool_path: Path,
        body_size: int | None,
    ) -> OpenedUpstream:
        retry_count = 0
        first_started_at = utc_iso()
        first_started_monotonic = time.monotonic()
        retry_allowed = self._retry_allowed(endpoint)

        while True:
            headers = build_upstream_headers(
                list(request.headers.raw),
                endpoint=endpoint,
                camel_api_key=self.settings.camel_api_key,
                body_size=body_size,
            )
            url = self.settings.upstream_base_url + endpoint
            if request.url.query:
                url += "?" + request.url.query
            content: AsyncIterator[bytes] | None = None
            if request.method == "POST":
                content = _iter_file(spool_path, self.settings.io_chunk_size)

            upstream_request = httpx.Request(
                request.method,
                url,
                headers=headers,
                content=content,
            )
            try:
                response = await self.client.send(upstream_request, stream=True)
            except httpx.TransportError as exc:
                if retry_allowed and retry_count < self.settings.max_retries:
                    delay = min(
                        self.settings.retry_max_delay_seconds,
                        0.5 * (2**retry_count),
                    )
                    retry_count += 1
                    await self._sleep_with_disconnect(request, delay)
                    continue
                raise UpstreamConnectionFailure(
                    f"Unable to reach camelStream: {exc.__class__.__name__}",
                    retry_count,
                ) from exc

            if (
                response.status_code in _RETRYABLE_STATUSES
                and retry_allowed
                and retry_count < self.settings.max_retries
            ):
                delay = _retry_delay_seconds(
                    response.headers.get("retry-after"), retry_count
                )
                if delay is not None and delay <= self.settings.retry_max_delay_seconds:
                    await response.aclose()
                    retry_count += 1
                    await self._sleep_with_disconnect(request, delay)
                    continue

            return OpenedUpstream(
                response=response,
                retry_count=retry_count,
                upstream_started_at=first_started_at,
                upstream_started_monotonic=first_started_monotonic,
            )

    async def _sleep_with_disconnect(self, request: Request, delay: float) -> None:
        deadline = time.monotonic() + max(0.0, delay)
        while True:
            if await request.is_disconnected():
                raise ClientDisconnectedDuringRetry
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                return
            await asyncio.sleep(min(self.settings.queue_poll_seconds, remaining))

    def _retry_allowed(self, endpoint: str) -> bool:
        return endpoint in _SAFE_RETRY_ENDPOINTS or self.settings.retry_post_generation

    async def _local_failure(
        self,
        *,
        request_id: str,
        received_monotonic: float,
        status_code: int,
        message: str,
        state: str,
        spool_path: Path,
        final_request_path: Path,
        response_path: Path,
        client_disconnected: bool = False,
        retry_count: int = 0,
        queue_wait_ms: float | None = None,
        reservation: QueueReservation | None = None,
    ) -> Response:
        if reservation is not None:
            await reservation.cancel()
        persisted_path, persist_error = await asyncio.to_thread(
            _persist_spool, spool_path, final_request_path
        )
        payload = {
            "error": {
                "message": message,
                "type": "gateway_error",
            }
        }
        raw = json.dumps(payload, ensure_ascii=False, separators=(",", ":")).encode(
            "utf-8"
        )
        log_error = persist_error
        try:
            response_path.parent.mkdir(parents=True, exist_ok=True)
            response_path.write_bytes(raw)
        except OSError as exc:
            log_error = _join_error(log_error, f"response_log_error: {exc}")

        completed_monotonic = time.monotonic()
        try:
            await asyncio.to_thread(
                self.database.update_request,
                request_id,
                completed_at=utc_iso(),
                total_duration_ms=elapsed_ms(received_monotonic, completed_monotonic),
                queue_wait_ms=queue_wait_ms,
                http_status=status_code,
                retry_count=retry_count,
                client_disconnected=int(client_disconnected),
                error=_join_error(message, log_error),
                usage_source="unavailable",
                request_path=str(persisted_path),
                response_path=str(response_path),
                response_bytes=len(raw),
                state=state,
            )
        except Exception:
            # A local error response must still reach the client if audit storage fails.
            pass
        response = JSONResponse(payload, status_code=status_code)
        response.headers["x-camel-gateway-request-id"] = request_id
        if queue_wait_ms is not None:
            response.headers["x-camel-gateway-queue-ms"] = str(
                int(round(queue_wait_ms))
            )
        return response


class ResponseSession:
    def __init__(
        self,
        *,
        database: Database,
        request: Request,
        request_id: str,
        endpoint: str,
        upstream_response: httpx.Response,
        response_path: Path,
        lease: QueueLease | None,
        received_monotonic: float,
        upstream_started_monotonic: float,
        queue_wait_ms: float,
        retry_count: int,
        upstream_queue_ms: float | None,
        camel_stream_limit: int | None,
        poll_seconds: float,
        initial_error: str | None,
    ) -> None:
        self.database = database
        self.request = request
        self.request_id = request_id
        self.endpoint = endpoint
        self.upstream_response = upstream_response
        self.response_path = response_path
        self.lease = lease
        self.received_monotonic = received_monotonic
        self.upstream_started_monotonic = upstream_started_monotonic
        self.queue_wait_ms = queue_wait_ms
        self.retry_count = retry_count
        self.upstream_queue_ms = upstream_queue_ms
        self.camel_stream_limit = camel_stream_limit
        self.poll_seconds = poll_seconds
        self.error = initial_error

        content_type = upstream_response.headers.get("content-type", "").lower()
        self.sse_parser = (
            SSEUsageParser(endpoint) if "text/event-stream" in content_type else None
        )
        self.first_byte_at: str | None = None
        self.first_byte_monotonic: float | None = None
        self.response_bytes = 0
        self.client_disconnected = False
        self.completed_normally = False
        self._finalized = False
        self._finalize_lock = asyncio.Lock()
        self._finalize_task: asyncio.Task[None] | None = None

    async def iter_body(self) -> AsyncIterator[bytes]:
        log_handle = None
        if self.upstream_response.is_stream_consumed:
            iterator = _iter_preloaded(self.upstream_response.content).__aiter__()
        else:
            iterator = self.upstream_response.aiter_raw().__aiter__()
        try:
            try:
                self.response_path.parent.mkdir(parents=True, exist_ok=True)
                log_handle = self.response_path.open("wb")
            except OSError as exc:
                self.error = _join_error(
                    self.error, f"response_log_open_error: {exc}"
                )

            while True:
                try:
                    chunk = await self._next_chunk_or_disconnect(iterator)
                except StopAsyncIteration:
                    self.completed_normally = True
                    break
                if chunk is None:
                    if self.sse_parser is not None and self.sse_parser.done_received:
                        self.completed_normally = True
                        break
                    self.client_disconnected = True
                    self.error = _join_error(
                        self.error, "client_disconnected_while_streaming"
                    )
                    break
                if not chunk:
                    continue

                now_monotonic = time.monotonic()
                if self.first_byte_monotonic is None:
                    self.first_byte_monotonic = now_monotonic
                    self.first_byte_at = utc_iso()

                self.response_bytes += len(chunk)
                if self.sse_parser is not None:
                    try:
                        self.sse_parser.feed(chunk)
                    except Exception as exc:  # parsing must never break passthrough
                        self.error = _join_error(
                            self.error,
                            f"usage_parser_error: {exc.__class__.__name__}",
                        )
                        self.sse_parser = None

                if log_handle is not None:
                    try:
                        log_handle.write(chunk)
                    except OSError as exc:
                        self.error = _join_error(
                            self.error, f"response_log_write_error: {exc}"
                        )
                        try:
                            log_handle.close()
                        except OSError:
                            pass
                        log_handle = None

                # SSE [DONE] is the protocol-level terminal marker. Deliver it, then
                # close the upstream stream immediately rather than waiting for a
                # subsequent read/disconnect that could be misclassified as a client
                # interruption.
                sse_done = (
                    self.sse_parser is not None
                    and self.sse_parser.done_received
                )

                yield chunk

                if sse_done:
                    self.completed_normally = True
                    break
        except asyncio.CancelledError:
            if self.sse_parser is not None and self.sse_parser.done_received:
                self.completed_normally = True
            else:
                self.client_disconnected = True
                self.error = _join_error(
                    self.error, "client_disconnected_while_streaming"
                )
            raise
        except httpx.HTTPError as exc:
            self.error = _join_error(
                self.error, f"upstream_stream_error: {exc.__class__.__name__}"
            )
            raise
        finally:
            if log_handle is not None:
                try:
                    log_handle.close()
                except OSError as exc:
                    self.error = _join_error(
                        self.error, f"response_log_close_error: {exc}"
                    )
            await self.finalize()

    async def _next_chunk_or_disconnect(
        self, iterator: AsyncIterator[bytes]
    ) -> bytes | None:
        next_task = asyncio.create_task(anext(iterator))
        try:
            while True:
                done, _ = await asyncio.wait(
                    {next_task}, timeout=self.poll_seconds
                )
                if done:
                    return next_task.result()
                if await self.request.is_disconnected():
                    next_task.cancel()
                    await asyncio.gather(next_task, return_exceptions=True)
                    return None
        except BaseException:
            if not next_task.done():
                next_task.cancel()
                await asyncio.gather(next_task, return_exceptions=True)
            raise

    async def finalize(self) -> None:
        async with self._finalize_lock:
            if self._finalize_task is None:
                self._finalize_task = asyncio.create_task(self._finalize_impl())
            task = self._finalize_task
        await asyncio.shield(task)

    async def _finalize_impl(self) -> None:
        try:
            try:
                await self.upstream_response.aclose()
            except Exception as exc:
                self.error = _join_error(
                    self.error, f"upstream_close_error: {exc.__class__.__name__}"
                )

            try:
                usage = await self._usage_result()
            except Exception as exc:
                self.error = _join_error(
                    self.error, f"usage_finalize_error: {exc.__class__.__name__}"
                )
                usage = UsageResult(
                    input_tokens=None,
                    output_tokens=None,
                    total_tokens=None,
                    usage_source="unavailable",
                    usage_details_json=None,
                )

            completed_monotonic = time.monotonic()
            completed_at = utc_iso()
            if self.first_byte_monotonic is not None:
                ttfb_ms = elapsed_ms(
                    self.upstream_started_monotonic, self.first_byte_monotonic
                )
                generation_ms = elapsed_ms(
                    self.first_byte_monotonic, completed_monotonic
                )
            else:
                ttfb_ms = None
                generation_ms = elapsed_ms(
                    self.upstream_started_monotonic, completed_monotonic
                )

            if self.client_disconnected:
                state = "interrupted"
            elif self.completed_normally:
                state = "completed"
            else:
                state = "failed"
                self.error = _join_error(
                    self.error, "response_stream_did_not_complete"
                )

            try:
                await asyncio.to_thread(
                    self.database.update_request,
                    self.request_id,
                    first_byte_at=self.first_byte_at,
                    completed_at=completed_at,
                    ttfb_ms=ttfb_ms,
                    generation_ms=generation_ms,
                    total_duration_ms=elapsed_ms(
                        self.received_monotonic, completed_monotonic
                    ),
                    http_status=self.upstream_response.status_code,
                    retry_count=self.retry_count,
                    client_disconnected=int(self.client_disconnected),
                    error=self.error,
                    input_tokens=usage.input_tokens,
                    output_tokens=usage.output_tokens,
                    total_tokens=usage.total_tokens,
                    usage_source=usage.usage_source,
                    usage_details_json=usage.usage_details_json,
                    counted_input_tokens=usage.counted_input_tokens,
                    upstream_queue_ms=self.upstream_queue_ms,
                    camel_stream_limit=self.camel_stream_limit,
                    response_path=str(self.response_path),
                    response_bytes=self.response_bytes,
                    state=state,
                )
            except Exception:
                # Audit persistence must not corrupt an otherwise valid client stream.
                pass
        finally:
            try:
                if self.lease is not None:
                    await self.lease.release()
            finally:
                self._finalized = True

    async def _usage_result(self) -> UsageResult:
        if self.sse_parser is not None:
            try:
                return self.sse_parser.finish()
            except Exception:
                pass
        return await asyncio.to_thread(
            parse_json_usage_file, self.response_path, self.endpoint
        )




async def _iter_preloaded(content: bytes) -> AsyncIterator[bytes]:
    if content:
        yield content


async def _iter_file(path: Path, chunk_size: int) -> AsyncIterator[bytes]:
    with path.open("rb") as handle:
        while True:
            chunk = handle.read(chunk_size)
            if not chunk:
                return
            yield chunk


class _ByteReader:
    """Small buffered byte reader with one-byte pushback for JSON scanning."""

    def __init__(self, handle: Any, chunk_size: int = 64 * 1024) -> None:
        self.handle = handle
        self.chunk_size = chunk_size
        self.buffer = b""
        self.index = 0
        self.pushed: int | None = None

    def read(self) -> int | None:
        if self.pushed is not None:
            value = self.pushed
            self.pushed = None
            return value
        if self.index >= len(self.buffer):
            self.buffer = self.handle.read(self.chunk_size)
            self.index = 0
            if not self.buffer:
                return None
        value = self.buffer[self.index]
        self.index += 1
        return value

    def unread(self, value: int) -> None:
        if self.pushed is not None:
            raise RuntimeError("Only one byte of pushback is supported")
        self.pushed = value


def _read_request_metadata(path: Path) -> tuple[str | None, bool]:
    """Extract top-level model/stream fields without loading a large body into RAM."""

    model: str | None = None
    stream = False
    stream_found = False
    try:
        with path.open("rb") as handle:
            reader = _ByteReader(handle)
            if _read_non_whitespace(reader) != ord("{"):
                return None, False

            while True:
                token = _read_non_whitespace(reader)
                if token is None or token == ord("}"):
                    break
                if token != ord('"'):
                    return model, stream

                raw_key = _read_json_string(reader, capture=True)
                if raw_key is None:
                    return model, stream
                key = json.loads(raw_key.decode("utf-8"))
                if _read_non_whitespace(reader) != ord(":"):
                    return model, stream

                first = _read_non_whitespace(reader)
                if first is None:
                    return model, stream
                if key == "model" and first == ord('"'):
                    raw_model = _read_json_string(reader, capture=True)
                    if raw_model is not None:
                        decoded = json.loads(raw_model.decode("utf-8"))
                        model = decoded if isinstance(decoded, str) else None
                elif key == "stream" and first not in (ord("{"), ord("["), ord('"')):
                    literal = _read_json_primitive(reader, first)
                    if literal == b"true":
                        stream = True
                        stream_found = True
                    elif literal == b"false":
                        stream = False
                        stream_found = True
                else:
                    _skip_json_value(reader, first)

                if model is not None and stream_found:
                    return model, stream

                delimiter = _read_non_whitespace(reader)
                if delimiter == ord(","):
                    continue
                if delimiter == ord("}") or delimiter is None:
                    break
                return model, stream
    except (OSError, UnicodeDecodeError, json.JSONDecodeError, ValueError):
        return None, False
    return model, stream


def _read_non_whitespace(reader: _ByteReader) -> int | None:
    while True:
        value = reader.read()
        if value is None or value not in b" \t\r\n":
            return value


def _read_json_string(
    reader: _ByteReader, *, capture: bool, max_capture_bytes: int = 64 * 1024
) -> bytes | None:
    captured = bytearray(b'"') if capture else None
    escaped = False
    while True:
        value = reader.read()
        if value is None:
            return None
        if captured is not None:
            if len(captured) >= max_capture_bytes:
                captured = None
            else:
                captured.append(value)
        if escaped:
            escaped = False
            continue
        if value == ord("\\"):
            escaped = True
            continue
        if value == ord('"'):
            return bytes(captured) if captured is not None else None


def _read_json_primitive(reader: _ByteReader, first: int) -> bytes:
    value = bytearray([first])
    while True:
        current = reader.read()
        if current is None:
            break
        if current in b" \t\r\n":
            break
        if current in (ord(","), ord("}"), ord("]")):
            reader.unread(current)
            break
        value.append(current)
    return bytes(value)


def _skip_json_value(reader: _ByteReader, first: int) -> None:
    if first == ord('"'):
        _read_json_string(reader, capture=False)
        return
    if first not in (ord("{"), ord("[")):
        _read_json_primitive(reader, first)
        return

    stack = [ord("}") if first == ord("{") else ord("]")]
    while stack:
        current = reader.read()
        if current is None:
            return
        if current == ord('"'):
            _read_json_string(reader, capture=False)
        elif current == ord("{"):
            stack.append(ord("}"))
        elif current == ord("["):
            stack.append(ord("]"))
        elif current == stack[-1]:
            stack.pop()


def _persist_spool(source: Path, destination: Path) -> tuple[Path, str | None]:
    if not source.exists():
        return destination, None
    try:
        destination.parent.mkdir(parents=True, exist_ok=True)
    except OSError as exc:
        return source, f"request_log_directory_error: {exc}"
    try:
        os.replace(source, destination)
        return destination, None
    except OSError as exc:
        if exc.errno != errno.EXDEV:
            return source, f"request_log_move_error: {exc}"
    try:
        shutil.copyfile(source, destination)
        source.unlink()
        return destination, None
    except OSError as exc:
        return source, f"request_log_copy_error: {exc}"


def _parse_content_length(request: Request) -> int | None:
    value = request.headers.get("content-length")
    if value is None:
        return None
    try:
        parsed = int(value)
    except ValueError:
        return None
    return parsed if parsed >= 0 else None


def _header_float(response: httpx.Response, name: str) -> float | None:
    value = response.headers.get(name)
    if value is None:
        return None
    try:
        return float(value)
    except ValueError:
        return None


def _header_int(response: httpx.Response, name: str) -> int | None:
    value = response.headers.get(name)
    if value is None:
        return None
    try:
        return int(value)
    except ValueError:
        return None


def _retry_delay_seconds(value: str | None, retry_count: int) -> float | None:
    if value is None:
        return 0.5 * (2**retry_count)
    stripped = value.strip()
    try:
        return max(0.0, float(stripped))
    except ValueError:
        pass
    try:
        parsed = email.utils.parsedate_to_datetime(stripped)
    except (TypeError, ValueError, OverflowError):
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return max(0.0, (parsed.astimezone(UTC) - datetime.now(UTC)).total_seconds())


def _join_error(first: str | None, second: str | None) -> str | None:
    if not first:
        return second
    if not second:
        return first
    return f"{first}; {second}"


def _error_response(status_code: int, message: str, *, error_type: str) -> JSONResponse:
    return JSONResponse(
        {"error": {"message": message, "type": error_type}},
        status_code=status_code,
    )
