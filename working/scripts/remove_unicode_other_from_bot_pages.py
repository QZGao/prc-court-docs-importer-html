#!/usr/bin/env python
"""
Remove invisible Unicode "Other" characters from pages edited by a bot.

The default target is the zhwikisource contribution list equivalent to:
Special:Contributions?namespace=all&tagfilter=unicode+other&target=SuperGrey-bot

All wiki reads and writes are performed through pywikibot. Pywikibot's public
Site.usercontribs() wrapper does not expose the tag filter as an argument, so
this script attaches uctag to the pywikibot generator it returns instead of
using raw HTTP/API calls.
"""

from __future__ import annotations

import argparse
import difflib
import json
import logging
import sys
import unicodedata
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from dotenv import load_dotenv

load_dotenv(PROJECT_ROOT / ".env")

from upload.mediawiki import configure_throttle, get_site

import pywikibot


DEFAULT_USER = "SuperGrey-bot"
DEFAULT_TAG = "unicode other"
DEFAULT_SUMMARY = "移除不可見字元"
DEFAULT_ALLOWED_CONTROLS = {"\n", "\r", "\t"}


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Fetch unique pages from tagged zhwikisource user contributions and "
            "remove invisible Unicode Other-category characters from their wikitext."
        )
    )
    parser.add_argument(
        "--user",
        default=DEFAULT_USER,
        help=f"Contribution username to scan (default: {DEFAULT_USER})",
    )
    parser.add_argument(
        "--tag",
        default=DEFAULT_TAG,
        help=f"Contribution tag to filter by (default: {DEFAULT_TAG!r})",
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
        help="Number of unique tagged pages to skip before processing (default: 0)",
    )
    parser.add_argument(
        "--max",
        type=int,
        default=None,
        help="Maximum number of unique tagged pages to process (default: all)",
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
        help="Show what would change without saving pages",
    )
    parser.add_argument(
        "--show-diff",
        action="store_true",
        help="In dry-run mode, print escaped unified diffs for changed pages",
    )
    return parser.parse_args(argv)


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def build_log_paths(log_dir: Path) -> dict[str, Path]:
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    return {
        "edited": log_dir / f"unicode_other_edited_{timestamp}.jsonl",
        "unchanged": log_dir / f"unicode_other_unchanged_{timestamp}.jsonl",
        "skipped": log_dir / f"unicode_other_skipped_{timestamp}.jsonl",
        "failed": log_dir / f"unicode_other_failed_{timestamp}.jsonl",
    }


def append_jsonl(path: Path, record: dict[str, Any]) -> None:
    with path.open("a", encoding="utf-8", newline="\n") as handle:
        handle.write(json.dumps(record, ensure_ascii=False))
        handle.write("\n")


def should_remove_char(char: str) -> bool:
    """Return True for Unicode Other-category chars, preserving line layout."""
    if char in DEFAULT_ALLOWED_CONTROLS:
        return False
    return unicodedata.category(char).startswith("C")


def remove_unicode_other_chars(text: str) -> tuple[str, Counter[str]]:
    removed: Counter[str] = Counter()
    kept_chars: list[str] = []

    for char in text:
        if should_remove_char(char):
            removed[char] += 1
        else:
            kept_chars.append(char)

    return "".join(kept_chars), removed


def describe_removed_chars(removed: Counter[str]) -> list[dict[str, Any]]:
    return [
        {
            "codepoint": f"U+{ord(char):04X}",
            "category": unicodedata.category(char),
            "name": unicodedata.name(char, "<unnamed>"),
            "count": count,
        }
        for char, count in sorted(removed.items(), key=lambda item: ord(item[0]))
    ]


def escaped_lines(text: str) -> list[str]:
    return [
        line.encode("unicode_escape").decode("ascii")
        for line in text.splitlines(keepends=True)
    ]


def print_dry_run_diff(title: str, before: str, after: str) -> None:
    diff_lines = difflib.unified_diff(
        escaped_lines(before),
        escaped_lines(after),
        fromfile=f"before/{title}",
        tofile=f"after/{title}",
        n=2,
    )
    for line in diff_lines:
        print(line.rstrip("\n"))


def tagged_user_contributions(
    site: pywikibot.Site,
    *,
    username: str,
    tag: str,
) -> Iterable[dict[str, Any]]:
    """Yield contribution records filtered by user and tag using pywikibot."""
    generator = site.usercontribs(user=username, total=None)
    generator.request["uctag"] = tag
    return generator


def process_page(
    *,
    site: pywikibot.Site,
    title: str,
    summary: str,
    dry_run: bool,
    show_diff: bool,
    log_paths: dict[str, Path],
) -> str:
    page = pywikibot.Page(site, title)

    if not page.exists():
        append_jsonl(log_paths["skipped"], {
            "reason": "page_missing",
            "title": title,
            "timestamp": utc_now_iso(),
        })
        print(f"SKIP missing: [[{title}]]")
        return "skipped"

    before = page.text
    after, removed = remove_unicode_other_chars(before)

    if not removed:
        append_jsonl(log_paths["unchanged"], {
            "title": title,
            "timestamp": utc_now_iso(),
        })
        print(f"OK unchanged: [[{title}]]")
        return "unchanged"

    removed_details = describe_removed_chars(removed)
    removed_total = sum(removed.values())
    record = {
        "title": title,
        "removed_total": removed_total,
        "removed": removed_details,
        "status": "would_edit" if dry_run else "edited",
        "timestamp": utc_now_iso(),
    }

    print(f"{'WOULD EDIT' if dry_run else 'EDIT'} [[{title}]]: removed {removed_total}")
    for detail in removed_details:
        print(
            "  "
            f"{detail['codepoint']} {detail['category']} "
            f"{detail['name']} x{detail['count']}"
        )

    if dry_run:
        append_jsonl(log_paths["edited"], record)
        if show_diff:
            print_dry_run_diff(title, before, after)
        return "would_edit"

    page.text = after
    page.save(summary=summary, minor=True, botflag=True)
    append_jsonl(log_paths["edited"], record)
    return "edited"


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

    print("=" * 72)
    print("Unicode Other Remover")
    print("=" * 72)
    print(f"Site:        {site}")
    print(f"User:        {args.user}")
    print(f"Tag:         {args.tag}")
    print(f"Skip:        {args.skip}")
    print(f"Max:         {args.max or 'all'}")
    print(f"Log dir:     {log_dir}")
    print(f"Dry run:     {args.dry_run}")
    print("=" * 72)

    counts = {
        "contributions": 0,
        "unique_pages": 0,
        "processed": 0,
        "edited": 0,
        "would_edit": 0,
        "unchanged": 0,
        "skipped": 0,
        "failed": 0,
    }
    seen_titles: set[str] = set()
    start_time = datetime.now()

    for contribution in tagged_user_contributions(site, username=args.user, tag=args.tag):
        if args.max is not None and counts["processed"] >= args.max:
            break

        counts["contributions"] += 1
        title = str(contribution.get("title") or "").strip()
        if not title or title in seen_titles:
            continue

        seen_titles.add(title)
        counts["unique_pages"] += 1

        if counts["unique_pages"] <= args.skip:
            continue

        counts["processed"] += 1
        try:
            result = process_page(
                site=site,
                title=title,
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
                "title": title,
                "contribution": contribution,
                "error": str(exc),
                "timestamp": utc_now_iso(),
            })
            print(f"FAILED [[{title}]]: {exc}")

    duration = datetime.now() - start_time
    action_label = "Would edit" if args.dry_run else "Edited"
    action_count = counts["would_edit"] if args.dry_run else counts["edited"]

    print()
    print("=" * 72)
    print("Done")
    print("=" * 72)
    print(f"Tagged contributions seen: {counts['contributions']}")
    print(f"Unique tagged pages:       {counts['unique_pages']}")
    print(f"Processed pages:           {counts['processed']}")
    print(f"{action_label}:              {action_count}")
    print(f"Unchanged:                 {counts['unchanged']}")
    print(f"Skipped:                   {counts['skipped']}")
    print(f"Failed:                    {counts['failed']}")
    print(f"Duration:                  {duration}")
    print("=" * 72)

    if counts["failed"]:
        print(f"Failure log: {log_paths['failed']}", file=sys.stderr)

    return 1 if counts["failed"] and not action_count else 0


if __name__ == "__main__":
    raise SystemExit(main())
