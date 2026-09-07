from __future__ import annotations

import json
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

_EVENT_BOUNDARY = re.compile(br"\r?\n\r?\n")
_MAX_SSE_PARSER_BUFFER = 8 * 1024 * 1024
_MAX_USAGE_OBSERVATIONS = 32


def _token_int(value: Any) -> int | None:
    if isinstance(value, bool) or value is None:
        return None
    if isinstance(value, (int, float)):
        converted = int(value)
        return converted if converted >= 0 else None
    return None


@dataclass(slots=True)
class UsageResult:
    input_tokens: int | None
    output_tokens: int | None
    total_tokens: int | None
    usage_source: str
    usage_details_json: str | None
    counted_input_tokens: int | None = None


@dataclass(slots=True)
class UsageAccumulator:
    endpoint: str
    input_tokens: int | None = None
    output_tokens: int | None = None
    total_tokens: int | None = None
    counted_input_tokens: int | None = None
    observations: list[dict[str, Any]] = field(default_factory=list)

    def observe_payload(self, payload: Any) -> None:
        if not isinstance(payload, dict):
            return

        if self.endpoint == "/v1/messages/count_tokens":
            counted = _token_int(payload.get("input_tokens"))
            if counted is not None:
                self.counted_input_tokens = counted
            return

        candidates: list[dict[str, Any]] = []
        direct = payload.get("usage")
        if isinstance(direct, dict):
            candidates.append(direct)

        for container_name in ("response", "message", "delta"):
            container = payload.get(container_name)
            if not isinstance(container, dict):
                continue
            nested = container.get("usage")
            if isinstance(nested, dict):
                candidates.append(nested)

        for usage in candidates:
            self.observe_usage(usage)

    def observe_usage(self, usage: dict[str, Any]) -> None:
        input_value = None
        for key in ("input_tokens", "prompt_tokens"):
            input_value = _token_int(usage.get(key))
            if input_value is not None:
                break

        output_value = None
        for key in ("output_tokens", "completion_tokens"):
            output_value = _token_int(usage.get(key))
            if output_value is not None:
                break

        total_value = _token_int(usage.get("total_tokens"))

        if input_value is not None:
            self.input_tokens = max(self.input_tokens or 0, input_value)
        if output_value is not None:
            self.output_tokens = max(self.output_tokens or 0, output_value)
        if total_value is not None:
            self.total_tokens = max(self.total_tokens or 0, total_value)

        if len(self.observations) < _MAX_USAGE_OBSERVATIONS:
            self.observations.append(usage)

    def result(self) -> UsageResult:
        if self.endpoint == "/v1/messages/count_tokens":
            source = "reported" if self.counted_input_tokens is not None else "unavailable"
            details = (
                json.dumps(
                    {"count_tokens": {"input_tokens": self.counted_input_tokens}},
                    ensure_ascii=False,
                    separators=(",", ":"),
                )
                if self.counted_input_tokens is not None
                else None
            )
            return UsageResult(
                input_tokens=None,
                output_tokens=None,
                total_tokens=None,
                usage_source=source,
                usage_details_json=details,
                counted_input_tokens=self.counted_input_tokens,
            )

        if self.total_tokens is None:
            if self.input_tokens is not None and self.output_tokens is not None:
                self.total_tokens = self.input_tokens + self.output_tokens

        reported = any(
            value is not None
            for value in (self.input_tokens, self.output_tokens, self.total_tokens)
        )
        details = None
        if self.observations:
            details = json.dumps(
                {"reported_usage": self.observations},
                ensure_ascii=False,
                separators=(",", ":"),
            )
        return UsageResult(
            input_tokens=self.input_tokens,
            output_tokens=self.output_tokens,
            total_tokens=self.total_tokens,
            usage_source="reported" if reported else "unavailable",
            usage_details_json=details,
        )


class SSEUsageParser:
    """Incrementally parses SSE records without assuming HTTP chunk boundaries."""

    def __init__(self, endpoint: str) -> None:
        self.accumulator = UsageAccumulator(endpoint)
        self._buffer = bytearray()
        self.disabled = False
        self.done_received = False

    def feed(self, chunk: bytes) -> None:
        if self.disabled or not chunk:
            return
        self._buffer.extend(chunk)
        if len(self._buffer) > _MAX_SSE_PARSER_BUFFER:
            self._buffer.clear()
            self.disabled = True
            return

        while True:
            match = _EVENT_BOUNDARY.search(self._buffer)
            if match is None:
                return
            event = bytes(self._buffer[: match.start()])
            del self._buffer[: match.end()]
            self._parse_event(event)

    def finish(self) -> UsageResult:
        if self._buffer and not self.disabled:
            self._parse_event(bytes(self._buffer))
            self._buffer.clear()
        return self.accumulator.result()

    def _parse_event(self, event: bytes) -> None:
        data_lines: list[bytes] = []
        for line in event.splitlines():
            if not line.startswith(b"data:"):
                continue
            value = line[5:]
            if value.startswith(b" "):
                value = value[1:]
            data_lines.append(value)
        if not data_lines:
            return

        raw_data = b"\n".join(data_lines).strip()
        if not raw_data:
            return
        if raw_data == b"[DONE]":
            self.done_received = True
            return
        try:
            payload = json.loads(raw_data.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError):
            return
        self.accumulator.observe_payload(payload)


def parse_json_usage_file(path: Path, endpoint: str) -> UsageResult:
    accumulator = UsageAccumulator(endpoint)
    try:
        with path.open("rb") as handle:
            payload = json.load(handle)
        accumulator.observe_payload(payload)
    except (OSError, UnicodeDecodeError, json.JSONDecodeError):
        pass
    return accumulator.result()
