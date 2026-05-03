#!/usr/bin/env python
"""
Convert court-document {{versions}} pages to {{裁判文书消歧义页}}.

For each main-namespace page in Category:版本页, this script looks for old
{{versions}} pages that contain an explicit court category ending in 法院]].
It infers:

  * year from [[Category:YYYY年中华人民共和国TYPE]] or [[Category:YYYY年TYPE]]
  * type from [[Category:中华人民共和国TYPE]] or [[Category:TYPE]]
  * court from [[Category:COURT]]

Then it replaces the {{versions}} template and removes those three explicit
category lines. All wiki operations are done through pywikibot.
"""

from __future__ import annotations

import argparse
import difflib
import json
import logging
import re
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from dotenv import load_dotenv

load_dotenv(PROJECT_ROOT / ".env")

from upload.mediawiki import configure_throttle, get_site

import pywikibot


DEFAULT_CATEGORY = "Category:版本页"
DEFAULT_SUMMARY = "转换为裁判文书消歧义页"

CATEGORY_LINE_RE = re.compile(
    r"^\s*\[\[\s*(?:Category|分类|分類)\s*:\s*([^\]\|\n]+)"
    r"(?:\|[^\]\n]*)?\]\]\s*$",
    re.IGNORECASE,
)
PARAM_RE = re.compile(r"^\s*\|\s*([^=]+?)\s*=\s*(.*)$")
YEAR_TYPE_CATEGORY_RE = re.compile(r"^(\d{4})年(?:中华人民共和国)?(.+)$")
VERSIONS_START_RE = re.compile(r"\{\{\s*versions\b", re.IGNORECASE)


@dataclass(frozen=True)
class InferredMetadata:
    title: str
    court: str
    doc_type: str
    year: str
    category_titles: set[str]


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Convert eligible Category:版本页 entries from {{versions}} to {{裁判文书消歧义页}}."
    )
    parser.add_argument(
        "--category",
        default=DEFAULT_CATEGORY,
        help=f"Category to scan (default: {DEFAULT_CATEGORY})",
    )
    parser.add_argument(
        "--summary",
        default=DEFAULT_SUMMARY,
        help=f"Edit summary (default: {DEFAULT_SUMMARY})",
    )
    parser.add_argument(
        "--skip",
        type=int,
        default=0,
        help="Number of category pages to skip before processing (default: 0)",
    )
    parser.add_argument(
        "--max",
        type=int,
        default=None,
        help="Maximum number of category pages to process after --skip (default: all)",
    )
    parser.add_argument(
        "--log-dir",
        type=Path,
        default=None,
        help="Directory for JSONL logs (default: working/output)",
    )
    parser.add_argument(
        "--interval",
        type=float,
        default=10.0,
        help="Minimum seconds between edits via pywikibot put_throttle (default: 10)",
    )
    parser.add_argument(
        "--maxlag",
        type=int,
        default=5,
        help="pywikibot maxlag setting (default: 5)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show planned edits without saving pages",
    )
    parser.add_argument(
        "--show-diff",
        action="store_true",
        help="In dry-run mode, print unified diffs for changed pages",
    )
    return parser.parse_args(argv)


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def build_log_paths(log_dir: Path) -> dict[str, Path]:
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    return {
        "converted": log_dir / f"court_disambig_converted_{timestamp}.jsonl",
        "skipped": log_dir / f"court_disambig_skipped_{timestamp}.jsonl",
        "failed": log_dir / f"court_disambig_failed_{timestamp}.jsonl",
    }


def append_jsonl(path: Path, record: dict[str, Any]) -> None:
    with path.open("a", encoding="utf-8", newline="\n") as handle:
        handle.write(json.dumps(record, ensure_ascii=False))
        handle.write("\n")


def find_versions_template_span(text: str) -> tuple[int, int] | None:
    """Find the first {{versions ...}} template, allowing nested templates."""
    match = VERSIONS_START_RE.search(text)
    if not match:
        return None

    start = match.start()
    index = start
    depth = 0
    while index < len(text) - 1:
        pair = text[index : index + 2]
        if pair == "{{":
            depth += 1
            index += 2
            continue
        if pair == "}}":
            depth -= 1
            index += 2
            if depth == 0:
                return start, index
            continue
        index += 1

    return None


def parse_template_params(template_text: str) -> dict[str, str]:
    params: dict[str, str] = {}
    for line in template_text.splitlines():
        match = PARAM_RE.match(line)
        if not match:
            continue
        key = match.group(1).strip().lower()
        value = match.group(2).strip()
        params[key] = value
    return params


def extract_category_titles(text: str) -> list[str]:
    titles: list[str] = []
    for line in text.splitlines():
        match = CATEGORY_LINE_RE.match(line)
        if match:
            titles.append(match.group(1).strip())
    return titles


def find_type_category(doc_type: str, categories: set[str]) -> str | None:
    """Return the matching explicit type category, with or without country prefix."""
    for candidate in (f"中华人民共和国{doc_type}", doc_type):
        if candidate in categories:
            return candidate
    return None


def infer_metadata(text: str, fallback_title: str) -> InferredMetadata | None:
    span = find_versions_template_span(text)
    if span is None:
        return None

    template_text = text[span[0] : span[1]]
    params = parse_template_params(template_text)
    title = params.get("title") or fallback_title

    categories = extract_category_titles(text)
    category_set = set(categories)
    year_type_matches: list[tuple[str, str, str]] = []
    courts: list[str] = []

    for category_title in categories:
        year_type_match = YEAR_TYPE_CATEGORY_RE.match(category_title)
        if year_type_match:
            year_type_matches.append((
                year_type_match.group(1),
                year_type_match.group(2),
                category_title,
            ))
            continue

        if category_title.endswith("法院"):
            courts.append(category_title)

    for year, doc_type, year_category in year_type_matches:
        type_category = find_type_category(doc_type, category_set)
        if type_category is None:
            continue
        if not courts:
            return None
        court = courts[0]
        return InferredMetadata(
            title=title,
            court=court,
            doc_type=doc_type,
            year=year,
            category_titles={
                year_category,
                type_category,
                court,
            },
        )

    return None


def build_new_template(metadata: InferredMetadata) -> str:
    return "\n".join([
        "{{裁判文书消歧义页",
        f" | title      = {metadata.title}",
        f" | court      = {metadata.court}",
        f" | type       = {metadata.doc_type}",
        f" | year       = {metadata.year}",
        "}}",
    ])


def remove_inferred_category_lines(text: str, category_titles: set[str]) -> str:
    kept_lines: list[str] = []
    for line in text.splitlines(keepends=True):
        line_without_newline = line.rstrip("\r\n")
        match = CATEGORY_LINE_RE.match(line_without_newline)
        if match and match.group(1).strip() in category_titles:
            continue
        kept_lines.append(line)
    return "".join(kept_lines)


def convert_text(text: str, page_title: str) -> tuple[str, InferredMetadata] | None:
    if "法院]]" not in text:
        return None

    span = find_versions_template_span(text)
    if span is None:
        return None

    metadata = infer_metadata(text, page_title)
    if metadata is None:
        return None

    new_template = build_new_template(metadata)
    converted = text[: span[0]] + new_template + text[span[1] :]
    converted = remove_inferred_category_lines(converted, metadata.category_titles)
    converted = converted.rstrip() + "\n"

    if converted == text:
        return None

    return converted, metadata


def print_diff(title: str, before: str, after: str) -> None:
    diff_lines = difflib.unified_diff(
        before.splitlines(keepends=True),
        after.splitlines(keepends=True),
        fromfile=f"before/{title}",
        tofile=f"after/{title}",
        n=3,
    )
    for line in diff_lines:
        print(line.rstrip("\n"))


def process_page(
    *,
    page: pywikibot.Page,
    summary: str,
    dry_run: bool,
    show_diff: bool,
    log_paths: dict[str, Path],
) -> str:
    title = page.title()

    if not page.exists():
        append_jsonl(log_paths["skipped"], {
            "title": title,
            "reason": "missing",
            "timestamp": utc_now_iso(),
        })
        print(f"SKIP missing: [[{title}]]")
        return "skipped"

    before = page.text
    result = convert_text(before, title)
    if result is None:
        append_jsonl(log_paths["skipped"], {
            "title": title,
            "reason": "not_convertible",
            "timestamp": utc_now_iso(),
        })
        print(f"SKIP not convertible: [[{title}]]")
        return "skipped"

    after, metadata = result
    record = {
        "title": title,
        "court": metadata.court,
        "type": metadata.doc_type,
        "year": metadata.year,
        "status": "would_convert" if dry_run else "converted",
        "timestamp": utc_now_iso(),
    }

    print(f"{'WOULD CONVERT' if dry_run else 'CONVERT'} [[{title}]]")
    print(f"  court={metadata.court} type={metadata.doc_type} year={metadata.year}")

    if dry_run:
        append_jsonl(log_paths["converted"], record)
        if show_diff:
            print_diff(title, before, after)
        return "would_convert"

    page.text = after
    page.save(summary=summary, minor=False, botflag=True)
    append_jsonl(log_paths["converted"], record)
    return "converted"


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)

    if args.skip < 0:
        raise ValueError("--skip must be 0 or greater")
    if args.max is not None and args.max <= 0:
        raise ValueError("--max must be greater than 0")

    log_dir = (args.log_dir or PROJECT_ROOT / "working" / "output").expanduser().resolve()
    log_dir.mkdir(parents=True, exist_ok=True)
    log_paths = build_log_paths(log_dir)

    logging.getLogger("pywiki").setLevel(logging.WARNING)
    logging.getLogger("pywikibot").setLevel(logging.WARNING)
    configure_throttle(interval=args.interval, maxlag=args.maxlag)
    site = get_site()
    category = pywikibot.Category(site, args.category)

    print("=" * 72)
    print("Court Document Versions Converter")
    print("=" * 72)
    print(f"Site:      {site}")
    print(f"Category:  {category.title()}")
    print(f"Skip:      {args.skip}")
    print(f"Max:       {args.max or 'all'}")
    print(f"Log dir:   {log_dir}")
    print(f"Dry run:   {args.dry_run}")
    print("=" * 72)

    counts = {
        "seen": 0,
        "processed": 0,
        "converted": 0,
        "would_convert": 0,
        "skipped": 0,
        "failed": 0,
    }
    start_time = datetime.now()

    for page in category.articles(namespaces=[0], total=None):
        if args.max is not None and counts["processed"] >= args.max:
            break

        counts["seen"] += 1
        if counts["seen"] <= args.skip:
            continue

        counts["processed"] += 1
        try:
            result = process_page(
                page=page,
                summary=args.summary,
                dry_run=args.dry_run,
                show_diff=args.show_diff,
                log_paths=log_paths,
            )
            counts[result] += 1
        except KeyboardInterrupt:
            raise
        except Exception as exc:
            counts["failed"] += 1
            append_jsonl(log_paths["failed"], {
                "title": page.title(),
                "error": str(exc),
                "timestamp": utc_now_iso(),
            })
            print(f"FAILED [[{page.title()}]]: {exc}")

    duration = datetime.now() - start_time
    action_label = "Would convert" if args.dry_run else "Converted"
    action_count = counts["would_convert"] if args.dry_run else counts["converted"]

    print()
    print("=" * 72)
    print("Done")
    print("=" * 72)
    print(f"Category pages seen: {counts['seen']}")
    print(f"Processed:           {counts['processed']}")
    print(f"{action_label}:        {action_count}")
    print(f"Skipped:             {counts['skipped']}")
    print(f"Failed:              {counts['failed']}")
    print(f"Duration:            {duration}")
    print("=" * 72)

    if counts["failed"]:
        print(f"Failure log: {log_paths['failed']}", file=sys.stderr)

    return 1 if counts["failed"] and not action_count else 0


if __name__ == "__main__":
    raise SystemExit(main())
