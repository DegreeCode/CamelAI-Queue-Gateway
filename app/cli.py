from __future__ import annotations

import argparse
import sys
from datetime import UTC, date, datetime, time, timedelta
from pathlib import Path
from typing import Any

from .config import Settings
from .db import Database


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="python -m app.cli")
    parser.add_argument("--db-path", type=Path, help="Override GATEWAY_DB_PATH")
    subcommands = parser.add_subparsers(dest="command", required=True)

    key_parser = subcommands.add_parser("key", help="Manage gateway client keys")
    key_commands = key_parser.add_subparsers(dest="key_command", required=True)
    create = key_commands.add_parser("create")
    create.add_argument("--name", required=True)
    key_commands.add_parser("list")
    for action in ("disable", "enable", "revoke"):
        command = key_commands.add_parser(action)
        command.add_argument("name")

    stats = subcommands.add_parser("stats", help="Show usage statistics")
    stats.add_argument("--key")
    stats.add_argument(
        "--period", choices=("today", "24h", "7d", "all"), default="all"
    )
    stats.add_argument("--from", dest="from_value")
    stats.add_argument("--to", dest="to_value")

    subcommands.add_parser("queue", help="Show queued and running requests")
    history = subcommands.add_parser("history", help="Show recent request history")
    history.add_argument("--limit", type=int, default=20)
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    settings = Settings.from_env(require_camel_key=False)
    database = Database(args.db_path or settings.db_path)
    database.initialize()

    try:
        if args.command == "key":
            return _run_key(database, args)
        if args.command == "stats":
            return _run_stats(database, args)
        if args.command == "queue":
            return _run_queue(database)
        if args.command == "history":
            return _run_history(database, args.limit)
    except ValueError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2
    parser.error("unknown command")
    return 2


def _run_key(database: Database, args: argparse.Namespace) -> int:
    if args.key_command == "create":
        plaintext, key = database.create_key(args.name)
        print(f"Created gateway key '{key.name}' (id={key.id}, prefix={key.key_prefix}).")
        print("The plaintext is shown once; store it securely:")
        print(plaintext)
        return 0

    if args.key_command == "list":
        rows = []
        for key in database.list_keys():
            if key.revoked_at:
                status = "revoked"
            elif key.enabled:
                status = "enabled"
            else:
                status = "disabled"
            rows.append(
                [
                    key.id,
                    key.name,
                    key.key_prefix,
                    status,
                    key.created_at,
                    key.last_used_at or "-",
                ]
            )
        _print_table(
            ["ID", "NAME", "PREFIX", "STATUS", "CREATED_AT", "LAST_USED_AT"],
            rows,
        )
        return 0

    if args.key_command == "disable":
        database.set_key_enabled(args.name, False)
        print(f"Disabled '{args.name}'.")
        return 0
    if args.key_command == "enable":
        database.set_key_enabled(args.name, True)
        print(f"Enabled '{args.name}'.")
        return 0
    if args.key_command == "revoke":
        database.revoke_key(args.name)
        print(f"Revoked '{args.name}'. Historical usage was retained.")
        return 0
    raise ValueError("Unknown key command")


def _run_stats(database: Database, args: argparse.Namespace) -> int:
    from_iso, to_iso, label = _resolve_period(
        args.period, args.from_value, args.to_value
    )
    result = database.stats(
        from_iso=from_iso, to_iso=to_iso, key_name=args.key
    )
    print(f"Period: {label}")
    print(
        f"Current queue depth: {result['queue_depth']} | "
        f"running: {result['running']}"
    )

    rows = [_stats_row(item) for item in result["keys"]]
    rows.append(_stats_row(result["total"]))
    _print_table(
        [
            "KEY",
            "REQ",
            "OK",
            "FAIL",
            "IN",
            "OUT",
            "TOTAL",
            "REPORTED",
            "UNKNOWN",
            "COVERAGE",
            "AVG_QUEUE_MS",
            "AVG_GEN_MS",
            "COUNT_Q",
            "COUNTED_IN",
        ],
        rows,
    )
    return 0


def _stats_row(item: dict[str, Any]) -> list[Any]:
    return [
        item["key_name"],
        item["request_count"],
        item["success_count"],
        item["failure_count"],
        item["reported_input_tokens"],
        item["reported_output_tokens"],
        item["reported_total_tokens"],
        item["usage_reported_requests"],
        item["usage_unknown_requests"],
        f"{item['usage_coverage_percent']:.2f}%",
        _display_number(item["average_queue_wait_ms"]),
        _display_number(item["average_generation_ms"]),
        item["token_count_queries"],
        item["counted_input_tokens"],
    ]


def _run_queue(database: Database) -> int:
    rows = database.queue_rows()
    _print_table(
        ["STATE", "REQUEST_ID", "KEY", "ENDPOINT", "MODEL", "QUEUED_AT"],
        [
            [
                row["state"],
                row["request_id"],
                row["gateway_key_name"],
                row["endpoint"],
                row["model"] or "-",
                row["queue_started_at"] or "-",
            ]
            for row in rows
        ],
    )
    return 0


def _run_history(database: Database, limit: int) -> int:
    if limit < 1 or limit > 1000:
        raise ValueError("--limit must be between 1 and 1000")
    rows = database.history_rows(limit)
    _print_table(
        [
            "RECEIVED_AT",
            "REQUEST_ID",
            "KEY",
            "ENDPOINT",
            "STATUS",
            "STATE",
            "TOKENS",
            "USAGE",
            "ERROR",
        ],
        [
            [
                row["received_at"],
                row["request_id"],
                row["gateway_key_name"],
                row["endpoint"],
                row["http_status"] if row["http_status"] is not None else "-",
                row["state"],
                row["total_tokens"] if row["total_tokens"] is not None else "-",
                row["usage_source"],
                row["error"] or "-",
            ]
            for row in rows
        ],
    )
    return 0


def _resolve_period(
    period: str, from_value: str | None, to_value: str | None
) -> tuple[str | None, str | None, str]:
    if from_value or to_value:
        from_dt = _parse_boundary(from_value, is_to=False) if from_value else None
        to_dt = _parse_boundary(to_value, is_to=True) if to_value else None
        if from_dt and to_dt and from_dt >= to_dt:
            raise ValueError("--from must be earlier than --to")
        return _iso_or_none(from_dt), _iso_or_none(to_dt), (
            f"custom [{_iso_or_none(from_dt) or '-'}, {_iso_or_none(to_dt) or '-'})"
        )

    now = datetime.now(UTC)
    if period == "all":
        return None, None, "all time"
    if period == "24h":
        return _iso_or_none(now - timedelta(hours=24)), _iso_or_none(now), "last 24h"
    if period == "7d":
        return _iso_or_none(now - timedelta(days=7)), _iso_or_none(now), "last 7d"
    if period == "today":
        start = datetime.combine(now.date(), time.min, tzinfo=UTC)
        return _iso_or_none(start), _iso_or_none(now), "today (UTC)"
    raise ValueError(f"Unknown period: {period}")


def _parse_boundary(value: str, *, is_to: bool) -> datetime:
    try:
        parsed_date = date.fromisoformat(value)
    except ValueError:
        parsed_date = None
    if parsed_date is not None and len(value) == 10:
        if is_to:
            parsed_date += timedelta(days=1)
        return datetime.combine(parsed_date, time.min, tzinfo=UTC)

    normalized = value.replace("Z", "+00:00")
    try:
        parsed = datetime.fromisoformat(normalized)
    except ValueError as exc:
        raise ValueError(
            f"Invalid date/time '{value}'; use YYYY-MM-DD or ISO 8601"
        ) from exc
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def _iso_or_none(value: datetime | None) -> str | None:
    if value is None:
        return None
    return value.astimezone(UTC).isoformat(timespec="milliseconds").replace(
        "+00:00", "Z"
    )


def _display_number(value: Any) -> str:
    return "-" if value is None else f"{float(value):.3f}"


def _print_table(headers: list[str], rows: list[list[Any]]) -> None:
    normalized = [[str(value) for value in row] for row in rows]
    widths = [len(header) for header in headers]
    for row in normalized:
        for index, value in enumerate(row):
            widths[index] = max(widths[index], len(value))

    print("  ".join(header.ljust(widths[index]) for index, header in enumerate(headers)))
    print("  ".join("-" * width for width in widths))
    for row in normalized:
        print("  ".join(value.ljust(widths[index]) for index, value in enumerate(row)))


if __name__ == "__main__":
    raise SystemExit(main())
