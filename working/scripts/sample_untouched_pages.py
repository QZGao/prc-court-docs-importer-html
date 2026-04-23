#!/usr/bin/env python
"""
Randomly sample untouched created pages from an edit-export JSONL file.

For each selected page, the script writes a text file named after the
1-based JSONL line number. The first line is the page title wrapped with
`☒`, followed by the full current page content.
"""

from __future__ import annotations

import argparse
import json
import random
from pathlib import Path
from typing import Any

import requests

API_URL = "https://zh.wikisource.org/w/api.php"
USER_AGENT = (
    "prc-court-docs-importer-html/1.0"
    "(SuperGrey)"
)
PROJECT_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_INPUT = PROJECT_ROOT / "working" / "output" / "SuperGrey-bot_edits_2026-04-19.jsonl"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Sample untouched created pages and dump their current content."
    )
    parser.add_argument(
        "--input",
        type=Path,
        default=DEFAULT_INPUT,
        help=f"Input JSONL file (default: {DEFAULT_INPUT})",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        help=(
            "Directory for sampled page text files "
            "(default: working/output/<input_stem>_untouched_samples)"
        ),
    )
    parser.add_argument(
        "--count",
        type=int,
        default=100,
        help="Number of untouched pages to sample (default: 100)",
    )
    parser.add_argument(
        "--seed",
        type=int,
        help="Optional random seed for reproducible sampling",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=20,
        help="Titles per API request when checking/fetching pages (default: 20)",
    )
    return parser.parse_args()


def default_output_dir(input_path: Path) -> Path:
    return input_path.parent / f"{input_path.stem}_untouched_samples"


def build_session() -> requests.Session:
    session = requests.Session()
    session.headers.update({"User-Agent": USER_AGENT})
    return session


def load_candidates(path: Path) -> list[dict[str, Any]]:
    candidates: list[dict[str, Any]] = []
    with path.open("r", encoding="utf-8") as handle:
        for line_number, raw_line in enumerate(handle, start=1):
            line = raw_line.strip()
            if not line:
                continue

            record = json.loads(line)
            if record.get("type") != "create":
                continue

            title = record.get("name")
            if not title:
                continue

            candidates.append({"line_number": line_number, "title": title})

    return candidates


def batched(items: list[dict[str, Any]], batch_size: int) -> list[list[dict[str, Any]]]:
    return [items[index : index + batch_size] for index in range(0, len(items), batch_size)]


def post_query(session: requests.Session, data: dict[str, Any]) -> dict[str, Any]:
    response = session.post(
        API_URL,
        data={
            "action": "query",
            "format": "json",
            "formatversion": "2",
            **data,
        },
        timeout=60,
    )
    response.raise_for_status()
    payload = response.json()
    if "error" in payload:
        raise RuntimeError(payload["error"])
    return payload


def fetch_latest_revisions(
    session: requests.Session,
    titles: list[str],
    include_content: bool,
) -> dict[str, dict[str, Any]]:
    rvprop = "ids|content" if include_content else "ids"
    payload = post_query(
        session,
        {
            "titles": "|".join(titles),
            "prop": "revisions",
            "rvprop": rvprop,
            "rvslots": "main",
        },
    )

    result: dict[str, dict[str, Any]] = {}
    for page in payload.get("query", {}).get("pages", []):
        if "missing" in page:
            continue

        revisions = page.get("revisions") or []
        if not revisions:
            continue

        revision = revisions[0]
        entry = {
            "revid": revision.get("revid"),
            "parentid": revision.get("parentid"),
        }

        if include_content:
            slots = revision.get("slots") or {}
            main_slot = slots.get("main") or {}
            entry["content"] = main_slot.get("content", "")

        result[page["title"]] = entry

    return result


def select_untouched_pages(
    session: requests.Session,
    candidates: list[dict[str, Any]],
    count: int,
    batch_size: int,
    seed: int | None,
) -> list[dict[str, Any]]:
    rng = random.Random(seed)
    shuffled = candidates[:]
    rng.shuffle(shuffled)

    selected: list[dict[str, Any]] = []
    for batch in batched(shuffled, batch_size):
        titles = [item["title"] for item in batch]
        revision_map = fetch_latest_revisions(session, titles, include_content=False)

        for item in batch:
            revision = revision_map.get(item["title"])
            if not revision:
                continue
            if revision.get("parentid") != 0:
                continue

            selected.append(
                {
                    "line_number": item["line_number"],
                    "title": item["title"],
                    "revid": revision.get("revid"),
                }
            )
            if len(selected) >= count:
                return selected

    return selected


def fetch_selected_contents(
    session: requests.Session,
    selected: list[dict[str, Any]],
    batch_size: int,
) -> dict[str, dict[str, Any]]:
    content_map: dict[str, dict[str, Any]] = {}
    for batch in batched(selected, batch_size):
        titles = [item["title"] for item in batch]
        revision_map = fetch_latest_revisions(session, titles, include_content=True)

        for item in batch:
            revision = revision_map.get(item["title"])
            if not revision:
                continue
            if revision.get("parentid") != 0:
                continue

            content_map[item["title"]] = revision

    return content_map


def write_output_files(
    output_dir: Path,
    selected: list[dict[str, Any]],
    content_map: dict[str, dict[str, Any]],
) -> int:
    output_dir.mkdir(parents=True, exist_ok=True)
    written = 0

    for item in selected:
        revision = content_map.get(item["title"])
        if not revision:
            continue

        output_path = output_dir / f"{item['line_number']}.txt"
        page_text = f"☒{item['title']}☒\n{revision.get('content', '')}"
        with output_path.open("w", encoding="utf-8", newline="\n") as handle:
            handle.write(page_text)
        written += 1

    return written


def main() -> int:
    args = parse_args()
    if args.count <= 0:
        raise ValueError("--count must be greater than 0")
    if args.batch_size <= 0:
        raise ValueError("--batch-size must be greater than 0")

    input_path = args.input.resolve()
    output_dir = (args.output_dir or default_output_dir(input_path)).resolve()

    candidates = load_candidates(input_path)
    if not candidates:
        raise RuntimeError(f"No create records found in {input_path}")

    with build_session() as session:
        selected = select_untouched_pages(
            session=session,
            candidates=candidates,
            count=args.count,
            batch_size=args.batch_size,
            seed=args.seed,
        )
        content_map = fetch_selected_contents(
            session=session,
            selected=selected,
            batch_size=args.batch_size,
        )

    written = write_output_files(output_dir, selected, content_map)
    print(f"Input create records: {len(candidates)}")
    print(f"Selected untouched pages: {len(selected)}")
    print(f"Wrote files: {written}")
    print(f"Output directory: {output_dir}")
    if written < args.count:
        print(
            "Warning:",
            f"requested {args.count}, but only wrote {written} currently untouched pages.",
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
