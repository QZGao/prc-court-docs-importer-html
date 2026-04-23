#!/usr/bin/env python
"""
Export a user's zhwikisource edits for a single calendar day to JSONL.

The script combines normal/page-creation edits from `usercontribs` with page
moves from `logevents`, classifies each event, and writes one JSON object per
line to an output file.
"""

from __future__ import annotations

import argparse
import json
import re
from collections import Counter
from datetime import date, datetime, time, timedelta, timezone
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

import requests

API_URL = "https://zh.wikisource.org/w/api.php"
DEFAULT_USER = "SuperGrey-bot"
DEFAULT_DATE = "2026-04-19"
DEFAULT_TIMEZONE = "UTC"
USER_AGENT = (
    "prc-court-docs-importer-html/1.0"
    "(SuperGrey)"
)
PROJECT_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_OUTPUT_DIR = PROJECT_ROOT / "working" / "output"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Export a user's zhwikisource edits for one day to JSONL."
    )
    parser.add_argument(
        "--user",
        default=DEFAULT_USER,
        help=f"Username without the User: prefix (default: {DEFAULT_USER})",
    )
    parser.add_argument(
        "--date",
        default=DEFAULT_DATE,
        help=f"Calendar date in YYYY-MM-DD format (default: {DEFAULT_DATE})",
    )
    parser.add_argument(
        "--timezone",
        default=DEFAULT_TIMEZONE,
        help=(
            "IANA timezone used to interpret --date before converting to UTC "
            f"(default: {DEFAULT_TIMEZONE})"
        ),
    )
    parser.add_argument(
        "--output",
        type=Path,
        help=(
            "Path to output JSONL file "
            "(default: working/output/<user>_edits_<date>.jsonl)"
        ),
    )
    return parser.parse_args()


def build_utc_window(day_text: str, timezone_name: str) -> tuple[datetime, datetime]:
    day = date.fromisoformat(day_text)
    local_zone = ZoneInfo(timezone_name)
    start_local = datetime.combine(day, time.min, tzinfo=local_zone)
    end_local = start_local + timedelta(days=1)
    return start_local.astimezone(timezone.utc), end_local.astimezone(timezone.utc)


def to_mediawiki_timestamp(value: datetime) -> str:
    return value.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def build_session() -> requests.Session:
    session = requests.Session()
    session.headers.update({"User-Agent": USER_AGENT})
    return session


def fetch_paginated(
    session: requests.Session,
    list_name: str,
    params: dict[str, Any],
) -> list[dict[str, Any]]:
    items: list[dict[str, Any]] = []
    request_params = {
        "action": "query",
        "format": "json",
        "formatversion": "2",
        "list": list_name,
        **params,
    }

    while True:
        response = session.get(API_URL, params=request_params, timeout=30)
        response.raise_for_status()
        payload = response.json()

        if "error" in payload:
            raise RuntimeError(payload["error"])

        items.extend(payload.get("query", {}).get(list_name, []))

        continuation = payload.get("continue")
        if not continuation:
            return items

        request_params.update(continuation)


def fetch_user_contribs(
    session: requests.Session,
    user: str,
    start_utc: datetime,
    end_utc: datetime,
) -> list[dict[str, Any]]:
    return fetch_paginated(
        session,
        "usercontribs",
        {
            "ucuser": user,
            "ucdir": "newer",
            "ucstart": to_mediawiki_timestamp(start_utc),
            "ucend": to_mediawiki_timestamp(end_utc),
            "uclimit": "max",
            "ucprop": "title|timestamp|flags|ids",
        },
    )


def fetch_move_logs(
    session: requests.Session,
    user: str,
    start_utc: datetime,
    end_utc: datetime,
) -> list[dict[str, Any]]:
    return fetch_paginated(
        session,
        "logevents",
        {
            "leuser": user,
            "letype": "move",
            "ledir": "newer",
            "lestart": to_mediawiki_timestamp(start_utc),
            "leend": to_mediawiki_timestamp(end_utc),
            "lelimit": "max",
            "leprop": "title|timestamp|details|ids",
        },
    )


def normalize_contrib(entry: dict[str, Any]) -> dict[str, Any]:
    edit_type = "create" if entry.get("new") else "normal edit"
    return {
        "name": entry["title"],
        "type": edit_type,
        "timestamp": entry["timestamp"],
    }


def normalize_move(entry: dict[str, Any]) -> dict[str, Any]:
    params = entry.get("params") or {}
    record = {
        "name": entry["title"],
        "type": "move",
        "timestamp": entry["timestamp"],
    }
    target_title = params.get("target_title")
    if target_title:
        record["target_name"] = target_title
    return record


def default_output_path(user: str, day_text: str) -> Path:
    slug = re.sub(r"[^A-Za-z0-9_.-]+", "_", user).strip("_") or "user"
    return DEFAULT_OUTPUT_DIR / f"{slug}_edits_{day_text}.jsonl"


def write_jsonl(path: Path, records: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="\n") as handle:
        for record in records:
            handle.write(json.dumps(record, ensure_ascii=False))
            handle.write("\n")


def main() -> int:
    args = parse_args()
    start_utc, end_utc = build_utc_window(args.date, args.timezone)
    output_path = args.output or default_output_path(args.user, args.date)

    with build_session() as session:
        contribs = fetch_user_contribs(session, args.user, start_utc, end_utc)
        moves = fetch_move_logs(session, args.user, start_utc, end_utc)

    records = [normalize_contrib(entry) for entry in contribs]
    records.extend(normalize_move(entry) for entry in moves)
    records.sort(key=lambda record: (record["timestamp"], record["type"], record["name"]))

    write_jsonl(output_path, records)

    counts = Counter(record["type"] for record in records)
    print(f"Wrote {len(records)} records to {output_path}")
    print(
        "Window:",
        f"{to_mediawiki_timestamp(start_utc)} to {to_mediawiki_timestamp(end_utc)}",
        f"(interpreted from {args.date} in {args.timezone})",
    )
    print(
        "Counts:",
        f"create={counts['create']},",
        f"move={counts['move']},",
        f"normal edit={counts['normal edit']}",
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
