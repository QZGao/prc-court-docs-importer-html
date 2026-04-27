#!/usr/bin/env python
"""
Normalize bot-created {{versions}} page titles in Category:版本页.

For each main-namespace page in Category:版本页 whose first revision user matches
the configured creator and whose title changes under
normalize_title_redaction_markers:

1. If the normalized title does not exist:
   Move the source versions page to the normalized title without leaving a
   redirect, then update {{Versions}} |title to the normalized page title.

2. If the normalized title exists as a {{Header/裁判文书}} content page:
   2.1 Move that content page to its case-number title without leaving a
       redirect.
   2.2 Move the source versions page to the normalized title without leaving a
       redirect.
   2.3 Update {{Versions}} |title to the normalized page title.
   2.4 Wrap the moved case page's |title field in [[...]] so it links back to
       the versions page.

3. If the normalized title exists as another {{versions}} page:
   3.1 Merge the source page's entry links into the normalized versions page
       and ensure its {{Versions}} |title matches the normalized page title.
   3.2 Prepend {{Sdelete|1=Author's request}} to the source versions page.

All other cases are logged as skips for manual review.
"""

from __future__ import annotations

import argparse
import json
import logging
import re
import sys
from datetime import datetime, timezone
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from dotenv import load_dotenv

load_dotenv(PROJECT_ROOT / ".env")

import pywikibot

from rich.console import Console

from convert.html_normalizer import normalize_title_redaction_markers
from upload.conflict_resolution import add_entry_to_versions_page, add_title_link_to_content
from upload.mediawiki import can_move_over_redirect, configure_throttle, get_site, move_page, post_query, save_page
from upload.page_metadata import build_case_title_from_content, is_header_page, is_versions_page, wikitexts_match

console = Console()

DEFAULT_CATEGORY = "Category:版本页"
DEFAULT_CREATOR = "SuperGrey-bot"
ENTRY_LINE_RE = re.compile(r"\s*\*\s*\[\[([^\]]+)\]\]")
VERSIONS_TITLE_RE = re.compile(r"^(\s*\|\s*title\s*=\s*)(.*)$", re.IGNORECASE)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Normalize bot-created versions-page titles from Category:版本页."
    )
    parser.add_argument(
        "--category",
        default=DEFAULT_CATEGORY,
        help=f"Category title to scan (default: {DEFAULT_CATEGORY})",
    )
    parser.add_argument(
        "--creator",
        default=DEFAULT_CREATOR,
        help=f"Require this first-revision username (default: {DEFAULT_CREATOR})",
    )
    parser.add_argument(
        "--skip",
        type=int,
        default=0,
        help="Number of eligible titles to skip before processing (default: 0)",
    )
    parser.add_argument(
        "--max",
        type=int,
        default=None,
        help="Maximum number of eligible titles to process",
    )
    parser.add_argument(
        "--log-dir",
        type=Path,
        default=None,
        help="Directory for output logs (default: working/output)",
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
        "--dry-run",
        action="store_true",
        help="Resolve and log actions without saving or moving pages",
    )
    return parser.parse_args()


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def canonical_user_name(name: str) -> str:
    name = name.strip()
    if name.startswith("User:"):
        return name.split(":", 1)[1]
    return name


def build_log_paths(log_dir: Path) -> dict[str, Path]:
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    return {
        "moved": log_dir / f"normalize_versions_moved_{ts}.jsonl",
        "absorbed": log_dir / f"normalize_versions_absorbed_{ts}.jsonl",
        "merged": log_dir / f"normalize_versions_merged_{ts}.jsonl",
        "skipped": log_dir / f"normalize_versions_skipped_{ts}.jsonl",
        "failed": log_dir / f"normalize_versions_failed_{ts}.jsonl",
    }


def append_jsonl(path: Path, record: dict[str, object]) -> None:
    with path.open("a", encoding="utf-8", newline="\n") as handle:
        handle.write(json.dumps(record, ensure_ascii=False))
        handle.write("\n")


def fetch_category_titles(category_title: str, maxlag: int) -> list[str]:
    titles: list[str] = []
    continuation: dict[str, object] = {}

    while True:
        payload = post_query(
            {
                "list": "categorymembers",
                "cmtitle": category_title,
                "cmnamespace": "0",
                "cmlimit": "max",
                **continuation,
            },
            maxlag=maxlag,
        )

        members = payload.get("query", {}).get("categorymembers", [])
        titles.extend(
            member.get("title", "").strip()
            for member in members
            if isinstance(member.get("title"), str) and member.get("title", "").strip()
        )

        continuation = payload.get("continue", {})
        if not continuation:
            return titles


def fetch_first_revision_user(title: str, maxlag: int) -> str | None:
    payload = post_query(
        {
            "titles": title,
            "prop": "revisions",
            "rvprop": "user|timestamp",
            "rvlimit": "1",
            "rvdir": "newer",
        },
        maxlag=maxlag,
    )
    pages = payload.get("query", {}).get("pages", [])
    if not pages:
        return None
    revisions = pages[0].get("revisions") or []
    if not revisions:
        return None
    user = revisions[0].get("user")
    return user.strip() if isinstance(user, str) and user.strip() else None


def extract_versions_entries(page_text: str) -> list[str]:
    entries: list[str] = []
    for line in page_text.splitlines():
        match = ENTRY_LINE_RE.match(line)
        if match:
            entries.append(match.group(1).strip())
    return entries


def merge_versions_entries(destination_text: str, source_text: str) -> tuple[str, list[str]]:
    source_entries = extract_versions_entries(source_text)
    updated = destination_text
    for entry_title in source_entries:
        updated = add_entry_to_versions_page(updated, entry_title)
    return updated, source_entries


def prepend_speedy_delete(page_text: str) -> str:
    stripped = page_text.lstrip()
    if stripped.lower().startswith("{{sdelete|1=author's request}}"):
        return page_text
    return "{{Sdelete|1=Author's request}}\n" + page_text


def update_versions_title_field(page_text: str, page_title: str) -> str:
    if not is_versions_page(page_text):
        return page_text

    lines = page_text.splitlines()
    for index, line in enumerate(lines):
        match = VERSIONS_TITLE_RE.match(line)
        if not match:
            continue
        current_value = match.group(2).strip()
        if current_value == page_title:
            return page_text
        lines[index] = f"{match.group(1)}{page_title}"
        return "\n".join(lines)

    return page_text


def move_without_redirect(
    *,
    from_title: str,
    to_title: str,
    reason: str,
    dry_run: bool,
    allow_overwrite_redirect: bool = False,
) -> None:
    if dry_run:
        return

    if allow_overwrite_redirect and can_move_over_redirect(from_title, to_title):
        move_page(
            from_title,
            to_title,
            reason=reason,
            leave_redirect=False,
            ignore_warnings=True,
        )
        return

    move_page(
        from_title,
        to_title,
        reason=reason,
        leave_redirect=False,
    )


def save_if_changed(
    *,
    title: str,
    before: str,
    after: str,
    summary: str,
    dry_run: bool,
) -> bool:
    if wikitexts_match(before, after):
        return False
    if not dry_run:
        save_page(title, after, summary=summary, minor=False, bot=True)
    return True


def classify_existing_page(page_text: str) -> str:
    if is_header_page(page_text):
        return "header"
    if is_versions_page(page_text):
        return "versions"
    return "other"


def process_title(
    *,
    site: pywikibot.Site,
    source_title: str,
    normalized_title: str,
    dry_run: bool,
    counts: dict[str, int],
    log_paths: dict[str, Path],
) -> None:
    source_page = pywikibot.Page(site, source_title)
    if not source_page.exists():
        counts["skipped"] += 1
        append_jsonl(log_paths["skipped"], {
            "reason": "source_missing",
            "source_title": source_title,
            "normalized_title": normalized_title,
            "timestamp": utc_now_iso(),
        })
        return

    source_text = source_page.text
    if not is_versions_page(source_text):
        counts["skipped"] += 1
        append_jsonl(log_paths["skipped"], {
            "reason": "source_not_versions",
            "source_title": source_title,
            "normalized_title": normalized_title,
            "timestamp": utc_now_iso(),
        })
        return

    destination_page = pywikibot.Page(site, normalized_title)
    if not destination_page.exists():
        reason = f"规范化版本页标题：[[{source_title}]] → [[{normalized_title}]]"
        move_without_redirect(
            from_title=source_title,
            to_title=normalized_title,
            reason=reason,
            dry_run=dry_run,
        )
        updated_versions_text = update_versions_title_field(source_text, normalized_title)
        versions_title_updated = save_if_changed(
            title=normalized_title,
            before=source_text,
            after=updated_versions_text,
            summary=f"同步版本页标题至[[{normalized_title}]]",
            dry_run=dry_run,
        )
        counts["moved"] += 1
        append_jsonl(log_paths["moved"], {
            "action": "move_versions_page",
            "source_title": source_title,
            "normalized_title": normalized_title,
            "reason": reason,
            "versions_title_updated": versions_title_updated,
            "status": "would_move" if dry_run else "moved",
            "timestamp": utc_now_iso(),
        })
        return

    destination_text = destination_page.text
    destination_kind = classify_existing_page(destination_text)

    if destination_kind == "header":
        case_title = build_case_title_from_content(destination_text)
        if not case_title:
            counts["skipped"] += 1
            append_jsonl(log_paths["skipped"], {
                "reason": "destination_header_missing_case_title",
                "source_title": source_title,
                "normalized_title": normalized_title,
                "timestamp": utc_now_iso(),
            })
            return

        case_page = pywikibot.Page(site, case_title)
        if case_page.exists() and not (case_page.isRedirectPage() and can_move_over_redirect(normalized_title, case_title)):
            counts["skipped"] += 1
            append_jsonl(log_paths["skipped"], {
                "reason": "case_title_exists",
                "source_title": source_title,
                "normalized_title": normalized_title,
                "case_title": case_title,
                "timestamp": utc_now_iso(),
            })
            return

        move_without_redirect(
            from_title=normalized_title,
            to_title=case_title,
            reason=f"移动至具体案号页面，原标题改为版本页：[[{normalized_title}]]",
            dry_run=dry_run,
            allow_overwrite_redirect=True,
        )
        move_without_redirect(
            from_title=source_title,
            to_title=normalized_title,
            reason=f"规范化版本页标题：[[{source_title}]] → [[{normalized_title}]]",
            dry_run=dry_run,
        )
        updated_versions_text = update_versions_title_field(source_text, normalized_title)
        versions_edit_changed = save_if_changed(
            title=normalized_title,
            before=source_text,
            after=updated_versions_text,
            summary=f"同步版本页标题至[[{normalized_title}]]",
            dry_run=dry_run,
        )

        updated_case_text = add_title_link_to_content(destination_text, normalized_title)
        case_edit_changed = save_if_changed(
            title=case_title,
            before=destination_text,
            after=updated_case_text,
            summary=f"更新标题链接至版本页：[[{normalized_title}]]",
            dry_run=dry_run,
        )

        counts["absorbed"] += 1
        append_jsonl(log_paths["absorbed"], {
            "action": "absorb_header_into_versions",
            "source_title": source_title,
            "normalized_title": normalized_title,
            "case_title": case_title,
            "versions_title_updated": versions_edit_changed,
            "case_title_linked": case_edit_changed,
            "status": "would_absorb" if dry_run else "absorbed",
            "timestamp": utc_now_iso(),
        })
        return

    if destination_kind == "versions":
        merged_text, merged_entries = merge_versions_entries(destination_text, source_text)
        merged_text = update_versions_title_field(merged_text, normalized_title)
        tagged_source_text = prepend_speedy_delete(source_text)

        if not merged_entries:
            counts["skipped"] += 1
            append_jsonl(log_paths["skipped"], {
                "reason": "source_versions_has_no_entries",
                "source_title": source_title,
                "normalized_title": normalized_title,
                "timestamp": utc_now_iso(),
            })
            return

        merged_changed = save_if_changed(
            title=normalized_title,
            before=destination_text,
            after=merged_text,
            summary=f"合并版本页条目自[[{source_title}]]",
            dry_run=dry_run,
        )
        tagged_changed = save_if_changed(
            title=source_title,
            before=source_text,
            after=tagged_source_text,
            summary=f"与[[{normalized_title}]]重复，提请删除",
            dry_run=dry_run,
        )

        counts["merged"] += 1
        append_jsonl(log_paths["merged"], {
            "action": "merge_versions_pages",
            "source_title": source_title,
            "normalized_title": normalized_title,
            "merged_entries": merged_entries,
            "normalized_page_changed": merged_changed,
            "source_tagged_for_delete": tagged_changed,
            "status": "would_merge" if dry_run else "merged",
            "timestamp": utc_now_iso(),
        })
        return

    counts["skipped"] += 1
    append_jsonl(log_paths["skipped"], {
        "reason": "destination_exists_unhandled",
        "source_title": source_title,
        "normalized_title": normalized_title,
        "destination_kind": destination_kind,
        "timestamp": utc_now_iso(),
    })


def main() -> int:
    args = parse_args()

    if args.skip < 0:
        raise ValueError("--skip must be 0 or greater")
    if args.max is not None and args.max <= 0:
        raise ValueError("--max must be greater than 0")

    log_dir = (args.log_dir or (PROJECT_ROOT / "working" / "output")).expanduser().resolve()
    log_dir.mkdir(parents=True, exist_ok=True)
    log_paths = build_log_paths(log_dir)

    logging.getLogger("pywiki").setLevel(logging.WARNING)
    logging.getLogger("pywikibot").setLevel(logging.WARNING)
    configure_throttle(interval=args.interval, maxlag=args.maxlag)

    creator_name = canonical_user_name(args.creator)
    site = get_site()
    category_title = args.category.strip()

    console.print("=" * 72)
    console.print("[bold cyan]Versions Page Title Normalizer[/bold cyan]")
    console.print("=" * 72)
    console.print(f"Category:      {category_title}")
    console.print(f"Creator:       {creator_name}")
    console.print(f"Skip eligible: {args.skip}")
    console.print(f"Max eligible:  {args.max or 'all'}")
    console.print(f"Log dir:       {log_dir}")
    console.print(f"Dry run:       {args.dry_run}")
    console.print("=" * 72)
    console.print()

    all_titles = fetch_category_titles(category_title, maxlag=args.maxlag)

    counts = {
        "scanned": 0,
        "eligible": 0,
        "processed": 0,
        "moved": 0,
        "absorbed": 0,
        "merged": 0,
        "skipped": 0,
        "failed": 0,
    }

    start = datetime.now()

    for source_title in all_titles:
        counts["scanned"] += 1
        normalized_title = normalize_title_redaction_markers(source_title)
        if normalized_title == source_title:
            continue

        try:
            first_user = fetch_first_revision_user(source_title, maxlag=args.maxlag)
        except Exception as exc:
            counts["failed"] += 1
            append_jsonl(log_paths["failed"], {
                "step": "fetch_creator",
                "source_title": source_title,
                "normalized_title": normalized_title,
                "error": str(exc),
                "timestamp": utc_now_iso(),
            })
            continue

        if canonical_user_name(first_user or "") != creator_name:
            continue

        counts["eligible"] += 1
        if counts["eligible"] <= args.skip:
            continue
        if args.max is not None and counts["processed"] >= args.max:
            break

        counts["processed"] += 1
        console.print(
            f"[cyan][{counts['processed']}][/cyan] [[{source_title}]] -> [[{normalized_title}]]"
        )

        try:
            process_title(
                site=site,
                source_title=source_title,
                normalized_title=normalized_title,
                dry_run=args.dry_run,
                counts=counts,
                log_paths=log_paths,
            )
        except KeyboardInterrupt:
            raise
        except Exception as exc:
            counts["failed"] += 1
            append_jsonl(log_paths["failed"], {
                "step": "process_title",
                "source_title": source_title,
                "normalized_title": normalized_title,
                "error": str(exc),
                "timestamp": utc_now_iso(),
            })
            console.print(f"  [red]Failed:[/red] {exc}")

    duration = datetime.now() - start

    console.print()
    console.print("=" * 72)
    console.print("[bold green]Done[/bold green]")
    console.print("=" * 72)
    console.print(f"Scanned titles: {counts['scanned']}")
    console.print(f"Eligible:       {counts['eligible']}")
    console.print(f"Processed:      {counts['processed']}")
    console.print(f"Moved:          {counts['moved']}")
    console.print(f"Absorbed:       {counts['absorbed']}")
    console.print(f"Merged:         {counts['merged']}")
    console.print(f"Skipped:        {counts['skipped']}")
    console.print(f"Failed:         {counts['failed']}")
    console.print(f"Duration:       {duration}")
    console.print("=" * 72)

    if counts["failed"] > 0:
        console.print(f"\n[yellow]Check {log_paths['failed']} for failures.[/yellow]")

    return 1 if counts["failed"] > 0 and (counts["moved"] + counts["absorbed"] + counts["merged"]) == 0 else 0


if __name__ == "__main__":
    raise SystemExit(main())
