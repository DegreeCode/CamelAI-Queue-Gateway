from __future__ import annotations

import hashlib
import hmac
import re
import secrets
import sqlite3
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from .timeutil import utc_iso

_KEY_NAME_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$")
_GENERATION_ENDPOINTS = (
    "/v1/chat/completions",
    "/v1/responses",
    "/v1/messages",
)

_REQUEST_UPDATE_FIELDS = {
    "gateway_key_id",
    "gateway_key_name",
    "endpoint",
    "method",
    "model",
    "stream",
    "received_at",
    "queue_started_at",
    "upstream_started_at",
    "first_byte_at",
    "completed_at",
    "queue_wait_ms",
    "upstream_queue_ms",
    "ttfb_ms",
    "generation_ms",
    "total_duration_ms",
    "http_status",
    "retry_count",
    "retry_allowed",
    "client_disconnected",
    "error",
    "input_tokens",
    "output_tokens",
    "total_tokens",
    "usage_source",
    "usage_details_json",
    "token_count_query",
    "counted_input_tokens",
    "camel_stream_limit",
    "request_path",
    "response_path",
    "request_headers_json",
    "response_headers_json",
    "request_bytes",
    "response_bytes",
    "state",
}


@dataclass(frozen=True, slots=True)
class GatewayKey:
    id: int
    name: str
    key_prefix: str
    enabled: bool
    created_at: str
    last_used_at: str | None
    revoked_at: str | None


class Database:
    def __init__(self, path: Path) -> None:
        self.path = path

    @contextmanager
    def _connect(self) -> Iterator[sqlite3.Connection]:
        connection = sqlite3.connect(self.path, timeout=30.0)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA foreign_keys = ON")
        connection.execute("PRAGMA busy_timeout = 30000")
        try:
            with connection:
                yield connection
        finally:
            connection.close()

    def initialize(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with self._connect() as connection:
            connection.execute("PRAGMA journal_mode = WAL")
            connection.execute("PRAGMA synchronous = NORMAL")
            connection.executescript(
                """
                CREATE TABLE IF NOT EXISTS gateway_keys (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    name TEXT NOT NULL UNIQUE,
                    key_prefix TEXT NOT NULL,
                    key_hash TEXT NOT NULL UNIQUE,
                    enabled INTEGER NOT NULL DEFAULT 1 CHECK (enabled IN (0, 1)),
                    created_at TEXT NOT NULL,
                    last_used_at TEXT,
                    revoked_at TEXT
                );

                CREATE TABLE IF NOT EXISTS requests (
                    request_id TEXT PRIMARY KEY,
                    gateway_key_id INTEGER NOT NULL,
                    gateway_key_name TEXT NOT NULL,
                    endpoint TEXT NOT NULL,
                    method TEXT NOT NULL,
                    model TEXT,
                    stream INTEGER NOT NULL DEFAULT 0 CHECK (stream IN (0, 1)),
                    received_at TEXT NOT NULL,
                    queue_started_at TEXT,
                    upstream_started_at TEXT,
                    first_byte_at TEXT,
                    completed_at TEXT,
                    queue_wait_ms REAL,
                    upstream_queue_ms REAL,
                    ttfb_ms REAL,
                    generation_ms REAL,
                    total_duration_ms REAL,
                    http_status INTEGER,
                    retry_count INTEGER NOT NULL DEFAULT 0,
                    retry_allowed INTEGER NOT NULL DEFAULT 0 CHECK (retry_allowed IN (0, 1)),
                    client_disconnected INTEGER NOT NULL DEFAULT 0 CHECK (client_disconnected IN (0, 1)),
                    error TEXT,
                    input_tokens INTEGER,
                    output_tokens INTEGER,
                    total_tokens INTEGER,
                    usage_source TEXT NOT NULL DEFAULT 'unavailable',
                    usage_details_json TEXT,
                    token_count_query INTEGER NOT NULL DEFAULT 0 CHECK (token_count_query IN (0, 1)),
                    counted_input_tokens INTEGER,
                    camel_stream_limit INTEGER,
                    request_path TEXT,
                    response_path TEXT,
                    request_headers_json TEXT,
                    response_headers_json TEXT,
                    request_bytes INTEGER NOT NULL DEFAULT 0,
                    response_bytes INTEGER NOT NULL DEFAULT 0,
                    state TEXT NOT NULL,
                    FOREIGN KEY (gateway_key_id) REFERENCES gateway_keys(id)
                );

                CREATE INDEX IF NOT EXISTS idx_requests_received_at
                    ON requests(received_at);
                CREATE INDEX IF NOT EXISTS idx_requests_key_received
                    ON requests(gateway_key_id, received_at);
                CREATE INDEX IF NOT EXISTS idx_requests_state_queue
                    ON requests(state, queue_started_at);
                CREATE INDEX IF NOT EXISTS idx_requests_endpoint_received
                    ON requests(endpoint, received_at);

                PRAGMA user_version = 1;
                """
            )

    def mark_inflight_interrupted(self) -> int:
        now = utc_iso()
        with self._connect() as connection:
            cursor = connection.execute(
                """
                UPDATE requests
                   SET state = 'interrupted',
                       completed_at = COALESCE(completed_at, ?),
                       error = CASE
                           WHEN error IS NULL OR error = '' THEN 'gateway_restarted'
                           ELSE error || '; gateway_restarted'
                       END
                 WHERE state IN ('receiving', 'queued', 'running')
                """,
                (now,),
            )
            return cursor.rowcount

    def create_key(self, name: str) -> tuple[str, GatewayKey]:
        if not _KEY_NAME_RE.fullmatch(name):
            raise ValueError(
                "Key name must be 1-64 characters using letters, numbers, '.', '_', or '-'"
            )
        plaintext = "cgk_" + secrets.token_urlsafe(32)
        key_hash = hashlib.sha256(plaintext.encode("utf-8")).hexdigest()
        prefix = plaintext[:12]
        created_at = utc_iso()
        try:
            with self._connect() as connection:
                cursor = connection.execute(
                    """
                    INSERT INTO gateway_keys
                        (name, key_prefix, key_hash, enabled, created_at)
                    VALUES (?, ?, ?, 1, ?)
                    """,
                    (name, prefix, key_hash, created_at),
                )
                key_id = int(cursor.lastrowid)
        except sqlite3.IntegrityError as exc:
            raise ValueError(f"A gateway key named '{name}' already exists") from exc
        return plaintext, GatewayKey(
            id=key_id,
            name=name,
            key_prefix=prefix,
            enabled=True,
            created_at=created_at,
            last_used_at=None,
            revoked_at=None,
        )

    def list_keys(self) -> list[GatewayKey]:
        with self._connect() as connection:
            rows = connection.execute(
                """
                SELECT id, name, key_prefix, enabled, created_at, last_used_at, revoked_at
                  FROM gateway_keys
                 ORDER BY id
                """
            ).fetchall()
        return [self._row_to_key(row) for row in rows]

    def set_key_enabled(self, name: str, enabled: bool) -> None:
        with self._connect() as connection:
            if enabled:
                cursor = connection.execute(
                    """
                    UPDATE gateway_keys
                       SET enabled = 1
                     WHERE name = ? AND revoked_at IS NULL
                    """,
                    (name,),
                )
            else:
                cursor = connection.execute(
                    "UPDATE gateway_keys SET enabled = 0 WHERE name = ?",
                    (name,),
                )
        if cursor.rowcount == 0:
            action = "enable" if enabled else "disable"
            raise ValueError(f"Unable to {action} key '{name}'")

    def revoke_key(self, name: str) -> None:
        with self._connect() as connection:
            cursor = connection.execute(
                """
                UPDATE gateway_keys
                   SET enabled = 0,
                       revoked_at = COALESCE(revoked_at, ?)
                 WHERE name = ?
                """,
                (utc_iso(), name),
            )
        if cursor.rowcount == 0:
            raise ValueError(f"Gateway key '{name}' does not exist")

    def authenticate_key(self, plaintext: str) -> GatewayKey | None:
        candidate_hash = hashlib.sha256(plaintext.encode("utf-8")).hexdigest()
        with self._connect() as connection:
            rows = connection.execute(
                """
                SELECT id, name, key_prefix, key_hash, enabled,
                       created_at, last_used_at, revoked_at
                  FROM gateway_keys
                 WHERE enabled = 1 AND revoked_at IS NULL
                 ORDER BY id
                """
            ).fetchall()

            matched: sqlite3.Row | None = None
            for row in rows:
                if hmac.compare_digest(str(row["key_hash"]), candidate_hash):
                    matched = row
            if matched is None:
                return None

            used_at = utc_iso()
            connection.execute(
                "UPDATE gateway_keys SET last_used_at = ? WHERE id = ?",
                (used_at, matched["id"]),
            )

        return GatewayKey(
            id=int(matched["id"]),
            name=str(matched["name"]),
            key_prefix=str(matched["key_prefix"]),
            enabled=True,
            created_at=str(matched["created_at"]),
            last_used_at=used_at,
            revoked_at=None,
        )

    def insert_request(self, values: dict[str, Any]) -> None:
        columns = list(values)
        unknown = set(columns) - (_REQUEST_UPDATE_FIELDS | {"request_id"})
        if unknown:
            raise ValueError(f"Unknown request fields: {sorted(unknown)}")
        placeholders = ",".join("?" for _ in columns)
        sql = f"INSERT INTO requests ({','.join(columns)}) VALUES ({placeholders})"
        with self._connect() as connection:
            connection.execute(sql, [values[column] for column in columns])

    def update_request(self, request_id: str, **values: Any) -> None:
        if not values:
            return
        unknown = set(values) - _REQUEST_UPDATE_FIELDS
        if unknown:
            raise ValueError(f"Unknown request fields: {sorted(unknown)}")
        assignments = ", ".join(f"{column} = ?" for column in values)
        parameters = list(values.values()) + [request_id]
        with self._connect() as connection:
            connection.execute(
                f"UPDATE requests SET {assignments} WHERE request_id = ?", parameters
            )

    def get_request(self, request_id: str) -> dict[str, Any] | None:
        with self._connect() as connection:
            row = connection.execute(
                "SELECT * FROM requests WHERE request_id = ?", (request_id,)
            ).fetchone()
        return dict(row) if row is not None else None

    def queue_rows(self) -> list[dict[str, Any]]:
        with self._connect() as connection:
            rows = connection.execute(
                """
                SELECT request_id, gateway_key_name, endpoint, model, state,
                       queue_started_at, upstream_started_at
                  FROM requests
                 WHERE state IN ('receiving', 'queued', 'running')
                 ORDER BY CASE state WHEN 'running' THEN 0 WHEN 'receiving' THEN 1 ELSE 2 END,
                          queue_started_at,
                          received_at
                """
            ).fetchall()
        return [dict(row) for row in rows]

    def history_rows(self, limit: int = 20) -> list[dict[str, Any]]:
        with self._connect() as connection:
            rows = connection.execute(
                """
                SELECT request_id, gateway_key_name, endpoint, model, stream, state,
                       received_at, completed_at, queue_wait_ms, generation_ms,
                       http_status, retry_count, client_disconnected,
                       input_tokens, output_tokens, total_tokens, usage_source, error
                  FROM requests
                 ORDER BY received_at DESC
                 LIMIT ?
                """,
                (limit,),
            ).fetchall()
        return [dict(row) for row in rows]

    def stats(
        self,
        *,
        from_iso: str | None = None,
        to_iso: str | None = None,
        key_name: str | None = None,
    ) -> dict[str, Any]:
        keys = self.list_keys()
        if key_name is not None:
            keys = [key for key in keys if key.name == key_name]
            if not keys:
                raise ValueError(f"Gateway key '{key_name}' does not exist")

        per_key = [
            self._aggregate_for_key(key, from_iso=from_iso, to_iso=to_iso)
            for key in keys
        ]
        total = self._aggregate_for_key(None, from_iso=from_iso, to_iso=to_iso)
        with self._connect() as connection:
            queue_depth = int(
                connection.execute(
                    "SELECT COUNT(*) FROM requests WHERE state IN ('receiving', 'queued')"
                ).fetchone()[0]
            )
            running = int(
                connection.execute(
                    "SELECT COUNT(*) FROM requests WHERE state = 'running'"
                ).fetchone()[0]
            )
        return {
            "from": from_iso,
            "to": to_iso,
            "keys": per_key,
            "total": total,
            "queue_depth": queue_depth,
            "running": running,
        }

    def _aggregate_for_key(
        self,
        key: GatewayKey | None,
        *,
        from_iso: str | None,
        to_iso: str | None,
    ) -> dict[str, Any]:
        conditions = [f"endpoint IN ({','.join('?' for _ in _GENERATION_ENDPOINTS)})"]
        parameters: list[Any] = list(_GENERATION_ENDPOINTS)
        if key is not None:
            conditions.append("gateway_key_id = ?")
            parameters.append(key.id)
        if from_iso is not None:
            conditions.append("received_at >= ?")
            parameters.append(from_iso)
        if to_iso is not None:
            conditions.append("received_at < ?")
            parameters.append(to_iso)
        where = " AND ".join(conditions)

        count_conditions = ["endpoint = '/v1/messages/count_tokens'"]
        count_parameters: list[Any] = []
        if key is not None:
            count_conditions.append("gateway_key_id = ?")
            count_parameters.append(key.id)
        if from_iso is not None:
            count_conditions.append("received_at >= ?")
            count_parameters.append(from_iso)
        if to_iso is not None:
            count_conditions.append("received_at < ?")
            count_parameters.append(to_iso)

        with self._connect() as connection:
            generation = connection.execute(
                f"""
                SELECT
                    COUNT(*) AS request_count,
                    SUM(CASE WHEN state = 'completed' AND http_status BETWEEN 200 AND 299
                             THEN 1 ELSE 0 END) AS success_count,
                    SUM(CASE
                        WHEN state NOT IN ('receiving', 'queued', 'running')
                         AND NOT (state = 'completed' AND http_status BETWEEN 200 AND 299)
                        THEN 1 ELSE 0 END) AS failure_count,
                    SUM(CASE WHEN state IN ('receiving', 'queued', 'running') THEN 1 ELSE 0 END)
                        AS inflight_count,
                    COALESCE(SUM(CASE WHEN usage_source = 'reported'
                        THEN COALESCE(input_tokens, 0) ELSE 0 END), 0)
                        AS reported_input_tokens,
                    COALESCE(SUM(CASE WHEN usage_source = 'reported'
                        THEN COALESCE(output_tokens, 0) ELSE 0 END), 0)
                        AS reported_output_tokens,
                    COALESCE(SUM(CASE WHEN usage_source = 'reported'
                        THEN COALESCE(total_tokens, 0) ELSE 0 END), 0)
                        AS reported_total_tokens,
                    SUM(CASE WHEN usage_source = 'reported' THEN 1 ELSE 0 END)
                        AS usage_reported_requests,
                    SUM(CASE WHEN state NOT IN ('receiving', 'queued', 'running')
                              AND usage_source != 'reported' THEN 1 ELSE 0 END)
                        AS usage_unknown_requests,
                    AVG(queue_wait_ms) AS average_queue_wait_ms,
                    AVG(generation_ms) AS average_generation_ms
                FROM requests
                WHERE {where}
                """,
                parameters,
            ).fetchone()
            token_count = connection.execute(
                f"""
                SELECT COUNT(*) AS token_count_queries,
                       COALESCE(SUM(counted_input_tokens), 0) AS counted_input_tokens
                  FROM requests
                 WHERE {' AND '.join(count_conditions)}
                """,
                count_parameters,
            ).fetchone()

        reported = int(generation["usage_reported_requests"] or 0)
        unknown = int(generation["usage_unknown_requests"] or 0)
        denominator = reported + unknown
        coverage = round((reported / denominator * 100.0), 2) if denominator else 0.0
        return {
            "key_id": key.id if key is not None else None,
            "key_name": key.name if key is not None else "TOTAL",
            "enabled": key.enabled if key is not None else None,
            "revoked_at": key.revoked_at if key is not None else None,
            "request_count": int(generation["request_count"] or 0),
            "success_count": int(generation["success_count"] or 0),
            "failure_count": int(generation["failure_count"] or 0),
            "inflight_count": int(generation["inflight_count"] or 0),
            "reported_input_tokens": int(generation["reported_input_tokens"] or 0),
            "reported_output_tokens": int(generation["reported_output_tokens"] or 0),
            "reported_total_tokens": int(generation["reported_total_tokens"] or 0),
            "usage_reported_requests": reported,
            "usage_unknown_requests": unknown,
            "usage_coverage_percent": coverage,
            "average_queue_wait_ms": _rounded_or_none(
                generation["average_queue_wait_ms"]
            ),
            "average_generation_ms": _rounded_or_none(
                generation["average_generation_ms"]
            ),
            "token_count_queries": int(token_count["token_count_queries"] or 0),
            "counted_input_tokens": int(token_count["counted_input_tokens"] or 0),
        }

    @staticmethod
    def _row_to_key(row: sqlite3.Row) -> GatewayKey:
        return GatewayKey(
            id=int(row["id"]),
            name=str(row["name"]),
            key_prefix=str(row["key_prefix"]),
            enabled=bool(row["enabled"]),
            created_at=str(row["created_at"]),
            last_used_at=row["last_used_at"],
            revoked_at=row["revoked_at"],
        )


def _rounded_or_none(value: Any) -> float | None:
    return round(float(value), 3) if value is not None else None
