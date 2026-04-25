#!/usr/bin/env python
"""
Create case-number redirect pages for uploaded zhwikisource court documents.

The script streams a large JSONL file line by line. For each record, it:
1. Resolves the source page title from `final_title` or `title`.
2. Fetches the current page content from zhwikisource.
3. Parses the `{{Header/裁判文书}}` metadata block.
4. Combines `court`, `案号`, and `type` into a redirect title.
5. Creates a redirect page with
   `[[Category:中华人民共和国法院裁判文书案号重定向]]`.

Records are written to separate JSONL logs for created redirects, missing source
pages, missing metadata, existing redirect pages, invalid input rows, and
unexpected failures.
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import requests

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from dotenv import load_dotenv

load_dotenv(PROJECT_ROOT / ".env")

from rich.console import Console
from rich.progress import (
    BarColumn,
    DownloadColumn,
    Progress,
    SpinnerColumn,
    TaskProgressColumn,
    TextColumn,
    TimeRemainingColumn,
    TotalFileSizeColumn,
    TransferSpeedColumn,
)

from upload.mediawiki import Page, configure_throttle, get_site

console = Console()

API_URL = "https://zh.wikisource.org/w/api.php"
USER_AGENT = (
    "prc-court-docs-importer-html/1.0"
    "(SuperGrey)"
)
DEFAULT_BATCH_SIZE = 20
REDIRECT_CATEGORY = "中华人民共和国法院裁判文书案号重定向"
REQUIRED_METADATA_FIELDS = ("court", "案号", "type")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Create case-number redirect pages for uploaded court-document pages."
        )
    )
    parser.add_argument(
        "input",
        type=Path,
        help="Path to uploaded JSONL log file",
    )
    parser.add_argument(
        "--log-dir",
        type=Path,
        default=None,
        help="Directory for output logs (default: same directory as input)",
    )
    parser.add_argument(
        "--skip",
        type=int,
        default=0,
        help="Number of non-empty JSONL rows to skip before processing (default: 0)",
    )
    parser.add_argument(
        "--max",
        type=int,
        default=None,
        help="Maximum number of non-empty JSONL rows to process",
    )
    parser.add_argument(
        "--interval",
        type=float,
        default=10.0,
        help="Minimum seconds between edits (default: 10)",
    )
    parser.add_argument(
        "--maxlag",
        type=int,
        default=5,
        help="Maxlag parameter for MediaWiki API (default: 5)",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=DEFAULT_BATCH_SIZE,
        help="Titles per read query batch (default: 20)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Resolve and log actions without saving redirect pages",
    )
    return parser.parse_args()


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def default_log_dir(input_path: Path) -> Path:
    return input_path.parent


def build_session() -> requests.Session:
    session = requests.Session()
    session.headers.update({"User-Agent": USER_AGENT})
    return session


def batched(items: list[str], batch_size: int) -> list[list[str]]:
    return [items[index : index + batch_size] for index in range(0, len(items), batch_size)]


def build_log_paths(log_dir: Path) -> dict[str, Path]:
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    return {
        "created": log_dir / f"case_redirect_created_{timestamp}.jsonl",
        "missing_page": log_dir / f"case_redirect_missing_page_{timestamp}.jsonl",
        "missing_metadata": log_dir / f"case_redirect_missing_metadata_{timestamp}.jsonl",
        "redirect_exists": log_dir / f"case_redirect_exists_{timestamp}.jsonl",
        "invalid_input": log_dir / f"case_redirect_invalid_input_{timestamp}.jsonl",
        "failed": log_dir / f"case_redirect_failed_{timestamp}.jsonl",
    }


def append_jsonl(handle, record: dict[str, Any]) -> None:
    handle.write(json.dumps(record, ensure_ascii=False))
    handle.write("\n")


def resolve_source_title(record: dict[str, Any]) -> str:
    for key in ("name", "final_title", "title"):
        value = record.get(key)
        if isinstance(value, str) and value.strip():
            return value.strip()
    return ""


def parse_header_metadata(page_text: str) -> dict[str, str] | None:
    lines = page_text.splitlines()
    start_index: int | None = None

    for index, line in enumerate(lines):
        if line.strip().startswith("{{Header/裁判文书"):
            start_index = index
            break

    if start_index is None:
        return None

    metadata: dict[str, str] = {}
    for line in lines[start_index + 1 :]:
        stripped = line.strip()
        if stripped == "}}":
            return metadata
        if not stripped.startswith("|"):
            continue

        key, separator, value = stripped[1:].partition("=")
        if not separator:
            continue
        metadata[key.strip()] = value.strip()

    return None


def normalize_case_number(case_number: str) -> str:
    return case_number.replace("(", "（").replace(")", "）")


def build_redirect_title(metadata: dict[str, str]) -> str:
    parts: list[str] = []
    for field in REQUIRED_METADATA_FIELDS:
        value = metadata[field].strip()
        if field == "案号":
            value = normalize_case_number(value)
        parts.append(value)
    return "".join(parts)


def build_redirect_text(source_title: str) -> str:
    return (
        f"#REDIRECT [[{source_title}]]\n\n"
        f"[[Category:{REDIRECT_CATEGORY}]]\n"
    )


def build_edit_summary(source_title: str) -> str:
    return f"按案号创建重定向至[[{source_title}]]"


def build_progress_description(
    processed: int,
    created: int,
    skipped: int,
    failed: int,
    dry_run: bool,
) -> str:
    action_label = "would-create" if dry_run else "created"
    return (
        f"[cyan]Processed {processed} rows"
        f" • {action_label} {created}"
        f" • skipped {skipped}"
        f" • failed {failed}"
    )


def truncate_text(value: str, limit: int = 300) -> str:
    if len(value) <= limit:
        return value
    return value[: limit - 1] + "…"


def post_query(
    session: requests.Session,
    data: dict[str, Any],
    maxlag: int,
) -> dict[str, Any]:
    attempt = 0
    while True:
        response = session.post(
            API_URL,
            data={
                "action": "query",
                "format": "json",
                "formatversion": "2",
                "maxlag": maxlag,
                **data,
            },
            timeout=60,
        )
        if response.status_code == 429:
            retry_after = int(response.headers.get("Retry-After", 60))
            attempt += 1
            console.print(
                f"[yellow]Rate limited (429) — waiting {retry_after}s "
                f"(retry #{attempt})…[/yellow]"
            )
            time.sleep(retry_after)
            continue
        response.raise_for_status()
        payload = response.json()
        if "error" in payload:
            raise RuntimeError(payload["error"])
        return payload


def build_title_alias_map(payload: dict[str, Any]) -> dict[str, str]:
    alias_map: dict[str, str] = {}
    query = payload.get("query", {})
    for key in ("normalized", "converted", "redirects"):
        for entry in query.get(key, []):
            source = entry.get("from")
            target = entry.get("to")
            if source and target:
                alias_map[source] = target
    return alias_map


def resolve_canonical_title(title: str, alias_map: dict[str, str]) -> str:
    seen: set[str] = set()
    current = title
    while current in alias_map and current not in seen:
        seen.add(current)
        current = alias_map[current]
    return current


def fetch_page_content_batch(
    session: requests.Session,
    titles: list[str],
    batch_size: int,
    maxlag: int,
) -> dict[str, dict[str, Any]]:
    results: dict[str, dict[str, Any]] = {}

    for title_batch in batched(list(dict.fromkeys(titles)), batch_size):
        payload = post_query(
            session,
            {
                "titles": "|".join(title_batch),
                "redirects": "1",
                "prop": "revisions",
                "rvprop": "content",
                "rvslots": "main",
            },
            maxlag=maxlag,
        )

        alias_map = build_title_alias_map(payload)
        page_map: dict[str, dict[str, Any]] = {}
        for page in payload.get("query", {}).get("pages", []):
            page_title = page.get("title", "")
            entry = {
                "canonical_title": page_title,
                "exists": "missing" not in page,
                "content": "",
            }

            if entry["exists"]:
                revisions = page.get("revisions") or []
                if revisions:
                    slots = revisions[0].get("slots") or {}
                    main_slot = slots.get("main") or {}
                    entry["content"] = main_slot.get("content", "")

            page_map[page_title] = entry

        for requested_title in title_batch:
            canonical_title = resolve_canonical_title(requested_title, alias_map)
            entry = page_map.get(canonical_title) or page_map.get(requested_title)
            if entry is None:
                results[requested_title] = {
                    "canonical_title": canonical_title,
                    "exists": False,
                    "content": "",
                }
            else:
                results[requested_title] = entry

    return results


def fetch_existing_titles_batch(
    session: requests.Session,
    titles: list[str],
    batch_size: int,
    maxlag: int,
) -> set[str]:
    existing_titles: set[str] = set()

    for title_batch in batched(list(dict.fromkeys(titles)), batch_size):
        payload = post_query(
            session,
            {
                "titles": "|".join(title_batch),
            },
            maxlag=maxlag,
        )

        alias_map = build_title_alias_map(payload)
        page_exists_map = {
            page.get("title", ""): "missing" not in page
            for page in payload.get("query", {}).get("pages", [])
        }

        for requested_title in title_batch:
            canonical_title = resolve_canonical_title(requested_title, alias_map)
            if page_exists_map.get(canonical_title) or page_exists_map.get(requested_title):
                existing_titles.add(requested_title)

    return existing_titles


def skipped_total(counts: dict[str, int]) -> int:
    return (
        counts["missing_page"]
        + counts["missing_metadata"]
        + counts["redirect_exists"]
        + counts["invalid_input"]
    )


def update_progress(
    progress: Progress,
    task_id: int,
    bytes_read: int,
    counts: dict[str, int],
    dry_run: bool,
) -> None:
    progress.update(
        task_id,
        completed=bytes_read,
        description=build_progress_description(
            processed=counts["processed"],
            created=counts["created"],
            skipped=skipped_total(counts),
            failed=counts["failed"],
            dry_run=dry_run,
        ),
    )


def process_record_batch(
    *,
    batch_records: list[dict[str, Any]],
    session: requests.Session,
    site: Any,
    dry_run: bool,
    batch_size: int,
    maxlag: int,
    counts: dict[str, int],
    created_f,
    missing_page_f,
    missing_metadata_f,
    redirect_exists_f,
    invalid_input_f,
    failed_f,
) -> None:
    source_titles = [record["source_title"] for record in batch_records]

    try:
        source_pages = fetch_page_content_batch(
            session=session,
            titles=source_titles,
            batch_size=batch_size,
            maxlag=maxlag,
        )
    except KeyboardInterrupt:
        raise
    except Exception as exc:
        for record in batch_records:
            counts["failed"] += 1
            append_jsonl(
                failed_f,
                {
                    "status": "failed",
                    "reason": "source_batch_query_failed",
                    "line_number": record["line_number"],
                    "input_title": record["input_title"],
                    "source_title": record["source_title"],
                    "wenshu_id": record["wenshu_id"],
                    "error": str(exc),
                    "timestamp": utc_now_iso(),
                },
            )
        return

    prepared_records: list[dict[str, Any]] = []
    redirect_titles: list[str] = []

    for record in batch_records:
        source_page = source_pages.get(
            record["source_title"],
            {"exists": False, "content": ""},
        )

        if not source_page.get("exists"):
            counts["missing_page"] += 1
            append_jsonl(
                missing_page_f,
                {
                    "status": "skipped",
                    "reason": "source_page_missing",
                    "line_number": record["line_number"],
                    "input_title": record["input_title"],
                    "source_title": record["source_title"],
                    "wenshu_id": record["wenshu_id"],
                    "timestamp": utc_now_iso(),
                },
            )
            continue

        content_title = source_page.get("canonical_title") or record["source_title"]
        metadata = parse_header_metadata(str(source_page.get("content", "")))
        if metadata is None:
            counts["missing_metadata"] += 1
            append_jsonl(
                missing_metadata_f,
                {
                    "status": "skipped",
                    "reason": "header_missing",
                    "line_number": record["line_number"],
                    "input_title": record["input_title"],
                    "source_title": record["source_title"],
                    "wenshu_id": record["wenshu_id"],
                    "timestamp": utc_now_iso(),
                },
            )
            continue

        missing_fields = [
            field
            for field in REQUIRED_METADATA_FIELDS
            if not metadata.get(field, "").strip()
        ]
        if missing_fields:
            counts["missing_metadata"] += 1
            append_jsonl(
                missing_metadata_f,
                {
                    "status": "skipped",
                    "reason": "required_metadata_missing",
                    "line_number": record["line_number"],
                    "input_title": record["input_title"],
                    "source_title": record["source_title"],
                    "wenshu_id": record["wenshu_id"],
                    "missing_fields": missing_fields,
                    "metadata": {
                        "court": metadata.get("court", ""),
                        "案号": metadata.get("案号", ""),
                        "type": metadata.get("type", ""),
                    },
                    "timestamp": utc_now_iso(),
                },
            )
            continue

        redirect_title = build_redirect_title(metadata)
        if redirect_title == content_title:
            counts["invalid_input"] += 1
            append_jsonl(
                invalid_input_f,
                {
                    "status": "skipped",
                    "reason": "redirect_title_matches_source_title",
                    "line_number": record["line_number"],
                    "input_title": record["input_title"],
                    "source_title": record["source_title"],
                    "content_title": content_title,
                    "redirect_title": redirect_title,
                    "wenshu_id": record["wenshu_id"],
                    "timestamp": utc_now_iso(),
                },
            )
            continue

        prepared_record = {
            **record,
            "content_title": content_title,
            "metadata": metadata,
            "redirect_title": redirect_title,
        }
        prepared_records.append(prepared_record)
        redirect_titles.append(redirect_title)

    if not prepared_records:
        return

    try:
        redirect_titles_existing = fetch_existing_titles_batch(
            session=session,
            titles=redirect_titles,
            batch_size=batch_size,
            maxlag=maxlag,
        )
    except KeyboardInterrupt:
        raise
    except Exception as exc:
        for record in prepared_records:
            counts["failed"] += 1
            append_jsonl(
                failed_f,
                {
                    "status": "failed",
                    "reason": "redirect_batch_query_failed",
                    "line_number": record["line_number"],
                    "input_title": record["input_title"],
                    "source_title": record["source_title"],
                    "redirect_title": record["redirect_title"],
                    "wenshu_id": record["wenshu_id"],
                    "error": str(exc),
                    "timestamp": utc_now_iso(),
                },
            )
        return

    seen_redirect_titles = set(redirect_titles_existing)

    for record in prepared_records:
        metadata = record["metadata"]
        redirect_title = record["redirect_title"]

        if redirect_title in seen_redirect_titles:
            counts["redirect_exists"] += 1
            append_jsonl(
                redirect_exists_f,
                {
                    "status": "skipped",
                    "reason": "redirect_page_exists",
                    "line_number": record["line_number"],
                    "input_title": record["input_title"],
                    "source_title": record["source_title"],
                    "redirect_title": redirect_title,
                    "wenshu_id": record["wenshu_id"],
                    "metadata": {
                        "court": metadata["court"],
                        "案号": metadata["案号"],
                        "type": metadata["type"],
                    },
                    "timestamp": utc_now_iso(),
                },
            )
            continue

        redirect_text = build_redirect_text(record["content_title"])
        try:
            if not dry_run:
                redirect_page = Page(site, redirect_title)
                redirect_page.text = redirect_text
                redirect_page.save(
                    summary=build_edit_summary(record["content_title"]),
                    minor=False,
                    botflag=True,
                )

            counts["created"] += 1
            seen_redirect_titles.add(redirect_title)
            append_jsonl(
                created_f,
                {
                    "status": "would_create" if dry_run else "created",
                    "line_number": record["line_number"],
                    "input_title": record["input_title"],
                    "source_title": record["source_title"],
                    "content_title": record["content_title"],
                    "redirect_title": redirect_title,
                    "wenshu_id": record["wenshu_id"],
                    "metadata": {
                        "court": metadata["court"],
                        "案号": metadata["案号"],
                        "type": metadata["type"],
                    },
                    "timestamp": utc_now_iso(),
                },
            )
        except KeyboardInterrupt:
            raise
        except Exception as exc:
            counts["failed"] += 1
            append_jsonl(
                failed_f,
                {
                    "status": "failed",
                    "reason": "unexpected_error",
                    "line_number": record["line_number"],
                    "input_title": record["input_title"],
                    "source_title": record["source_title"],
                    "content_title": record["content_title"],
                    "redirect_title": redirect_title,
                    "wenshu_id": record["wenshu_id"],
                    "error": str(exc),
                    "timestamp": utc_now_iso(),
                },
            )


def process_input(
    input_path: Path,
    log_paths: dict[str, Path],
    skip_rows: int,
    max_rows: int | None,
    dry_run: bool,
    batch_size: int,
    maxlag: int,
) -> dict[str, int]:
    site = get_site() if not dry_run else None
    input_size = input_path.stat().st_size

    counts = {
        "skipped_rows": 0,
        "processed": 0,
        "created": 0,
        "missing_page": 0,
        "missing_metadata": 0,
        "redirect_exists": 0,
        "invalid_input": 0,
        "failed": 0,
    }
    bytes_read = 0

    with input_path.open("r", encoding="utf-8") as infile, \
         log_paths["created"].open("a", encoding="utf-8", newline="\n", buffering=1) as created_f, \
         log_paths["missing_page"].open("a", encoding="utf-8", newline="\n", buffering=1) as missing_page_f, \
         log_paths["missing_metadata"].open("a", encoding="utf-8", newline="\n", buffering=1) as missing_metadata_f, \
         log_paths["redirect_exists"].open("a", encoding="utf-8", newline="\n", buffering=1) as redirect_exists_f, \
         log_paths["invalid_input"].open("a", encoding="utf-8", newline="\n", buffering=1) as invalid_input_f, \
         log_paths["failed"].open("a", encoding="utf-8", newline="\n", buffering=1) as failed_f:

        with build_session() as session, Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            BarColumn(bar_width=None),
            TaskProgressColumn(),
            TextColumn("•"),
            DownloadColumn(binary_units=True),
            TextColumn("/"),
            TotalFileSizeColumn(),
            TextColumn("•"),
            TransferSpeedColumn(),
            TextColumn("•"),
            TimeRemainingColumn(),
            console=console,
            transient=False,
        ) as progress:
            task_id = progress.add_task(
                build_progress_description(
                    processed=0,
                    created=0,
                    skipped=0,
                    failed=0,
                    dry_run=dry_run,
                ),
                total=input_size,
            )

            batch_records: list[dict[str, Any]] = []

            for line_number, raw_line in enumerate(infile, start=1):
                bytes_read += len(raw_line.encode("utf-8"))
                stripped_line = raw_line.strip()

                if not stripped_line:
                    progress.update(task_id, completed=bytes_read)
                    continue

                if counts["skipped_rows"] < skip_rows:
                    counts["skipped_rows"] += 1
                    progress.update(task_id, completed=bytes_read)
                    continue

                if max_rows is not None and counts["processed"] >= max_rows:
                    break

                counts["processed"] += 1

                try:
                    record = json.loads(stripped_line)
                except json.JSONDecodeError as exc:
                    counts["failed"] += 1
                    append_jsonl(
                        failed_f,
                        {
                            "status": "failed",
                            "reason": "invalid_json",
                            "line_number": line_number,
                            "error": str(exc),
                            "raw_line": truncate_text(stripped_line),
                            "timestamp": utc_now_iso(),
                        },
                    )
                    update_progress(progress, task_id, bytes_read, counts, dry_run)
                    continue

                if not isinstance(record, dict):
                    counts["invalid_input"] += 1
                    append_jsonl(
                        invalid_input_f,
                        {
                            "status": "skipped",
                            "reason": "record_not_object",
                            "line_number": line_number,
                            "record_type": type(record).__name__,
                            "timestamp": utc_now_iso(),
                        },
                    )
                    update_progress(progress, task_id, bytes_read, counts, dry_run)
                    continue

                input_title = str(record.get("title") or "").strip()
                source_title = resolve_source_title(record)
                wenshu_id = str(record.get("wenshu_id") or record.get("wenshuID") or "").strip()

                if not source_title:
                    counts["invalid_input"] += 1
                    append_jsonl(
                        invalid_input_f,
                        {
                            "status": "skipped",
                            "reason": "missing_source_title",
                            "line_number": line_number,
                            "input_title": input_title,
                            "wenshu_id": wenshu_id,
                            "timestamp": utc_now_iso(),
                        },
                    )
                    update_progress(progress, task_id, bytes_read, counts, dry_run)
                    continue

                batch_records.append(
                    {
                        "line_number": line_number,
                        "input_title": input_title,
                        "source_title": source_title,
                        "wenshu_id": wenshu_id,
                    }
                )

                if len(batch_records) >= batch_size:
                    process_record_batch(
                        batch_records=batch_records,
                        session=session,
                        site=site,
                        dry_run=dry_run,
                        batch_size=batch_size,
                        maxlag=maxlag,
                        counts=counts,
                        created_f=created_f,
                        missing_page_f=missing_page_f,
                        missing_metadata_f=missing_metadata_f,
                        redirect_exists_f=redirect_exists_f,
                        invalid_input_f=invalid_input_f,
                        failed_f=failed_f,
                    )
                    batch_records = []
                    update_progress(progress, task_id, bytes_read, counts, dry_run)
                else:
                    progress.update(task_id, completed=bytes_read)

            if batch_records:
                process_record_batch(
                    batch_records=batch_records,
                    session=session,
                    site=site,
                    dry_run=dry_run,
                    batch_size=batch_size,
                    maxlag=maxlag,
                    counts=counts,
                    created_f=created_f,
                    missing_page_f=missing_page_f,
                    missing_metadata_f=missing_metadata_f,
                    redirect_exists_f=redirect_exists_f,
                    invalid_input_f=invalid_input_f,
                    failed_f=failed_f,
                )

            progress.update(
                task_id,
                completed=min(bytes_read, input_size),
                description=build_progress_description(
                    processed=counts["processed"],
                    created=counts["created"],
                    skipped=skipped_total(counts),
                    failed=counts["failed"],
                    dry_run=dry_run,
                ).replace("[cyan]", "[green]"),
            )

    return counts


def main() -> int:
    args = parse_args()

    if args.skip < 0:
        raise ValueError("--skip must be 0 or greater")
    if args.max is not None and args.max <= 0:
        raise ValueError("--max must be greater than 0")
    if args.batch_size <= 0:
        raise ValueError("--batch-size must be greater than 0")

    input_path = args.input.expanduser().resolve()
    if not input_path.exists():
        print(f"Error: Input file not found: {input_path}", file=sys.stderr)
        return 1

    log_dir = (args.log_dir or default_log_dir(input_path)).expanduser().resolve()
    log_dir.mkdir(parents=True, exist_ok=True)
    log_paths = build_log_paths(log_dir)

    logging.getLogger("pywiki").setLevel(logging.WARNING)
    logging.getLogger("pywikibot").setLevel(logging.WARNING)
    configure_throttle(interval=args.interval, maxlag=args.maxlag)

    console.print("=" * 72)
    console.print("[bold cyan]Case-Number Redirect Creator[/bold cyan]")
    console.print("=" * 72)
    console.print(f"Input:              {input_path}")
    console.print(f"Log directory:      {log_dir}")
    console.print(f"Skip rows:          {args.skip or 'none'}")
    console.print(f"Max rows:           {args.max or 'all'}")
    console.print(f"Edit interval:      {args.interval}s")
    console.print(f"Maxlag:             {args.maxlag}")
    console.print(f"Read batch size:    {args.batch_size}")
    console.print(f"Dry run:            {args.dry_run}")
    console.print(f"Created log:        {log_paths['created']}")
    console.print(f"Missing page log:   {log_paths['missing_page']}")
    console.print(f"Missing metadata:   {log_paths['missing_metadata']}")
    console.print(f"Redirect exists:    {log_paths['redirect_exists']}")
    console.print(f"Invalid input:      {log_paths['invalid_input']}")
    console.print(f"Failed log:         {log_paths['failed']}")
    console.print("=" * 72)
    console.print()

    start_time = datetime.now()
    console.print(f"Started at: {start_time.isoformat()}")
    console.print()

    try:
        counts = process_input(
            input_path=input_path,
            log_paths=log_paths,
            skip_rows=args.skip,
            max_rows=args.max,
            dry_run=args.dry_run,
            batch_size=args.batch_size,
            maxlag=args.maxlag,
        )
    except KeyboardInterrupt:
        console.print("\n[yellow]Interrupted by user[/yellow]")
        return 130
    except Exception as exc:
        print(f"\nFatal error: {exc}", file=sys.stderr)
        return 1

    end_time = datetime.now()
    duration = end_time - start_time
    skipped_total = (
        counts["missing_page"]
        + counts["missing_metadata"]
        + counts["redirect_exists"]
        + counts["invalid_input"]
    )

    console.print()
    console.print("=" * 72)
    console.print(
        "[bold green]Dry-Run Complete[/bold green]"
        if args.dry_run
        else "[bold green]Redirect Creation Complete[/bold green]"
    )
    console.print("=" * 72)
    console.print(f"Processed rows:     {counts['processed']}")
    console.print(
        f"{'Would create' if args.dry_run else 'Created'} redirects: "
        f"{counts['created']}"
    )
    console.print(f"Missing source:     {counts['missing_page']}")
    console.print(f"Missing metadata:   {counts['missing_metadata']}")
    console.print(f"Redirect exists:    {counts['redirect_exists']}")
    console.print(f"Invalid input:      {counts['invalid_input']}")
    console.print(f"Failed:             {counts['failed']}")
    console.print(f"Skipped total:      {skipped_total}")
    console.print(f"Duration:           {duration}")
    if duration.total_seconds() > 0:
        console.print(
            f"Rate:               {counts['processed'] / duration.total_seconds():.2f} rows/sec"
        )
    console.print("=" * 72)

    if counts["failed"] > 0:
        console.print(f"\n[yellow]Check {log_paths['failed']} for unexpected errors.[/yellow]")

    if counts["created"] == 0 and counts["failed"] > 0:
        return 1

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
