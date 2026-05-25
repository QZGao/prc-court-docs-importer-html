#!/usr/bin/env python
"""
Remove unchecked-overwrite category when the overwritten import is obviously bad.

For pages in Category:覆盖版本未检查的裁判文书, this script inspects the standard
overwrite-revert revision shape:

1. current revision: reverted content plus the maintenance category
2. previous revision: overwritten import
3. revision before that: original content restored by the revert

If removing the maintenance category from the current revision exactly restores
the pre-import revision, and the overwritten import is mechanically worse than
the current content, the script saves the current content without the category.
Ambiguous pages are logged and left in the category.
"""

from __future__ import annotations

import argparse
import json
import logging
import os
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

from rich.console import Console

from convert.html_normalizer import REDACTION_SEQUENCE_PATTERN
from upload.mediawiki import configure_throttle, post_query, save_page
from upload.overwrite_quality import (
    PRC_REDACT_TEMPLATE_RE,
    is_safe_formatting_improvement,
    is_safe_header_only_update,
    is_safe_redaction_marker_update,
    is_safe_signature_structure_improvement,
)
from upload.page_metadata import normalize_wikitext_for_comparison
from upload.uploader import UNCHECKED_OVERWRITE_CATEGORY_RE

console = Console()

DEFAULT_CATEGORY = "Category:覆盖版本未检查的裁判文书"
DEFAULT_LOG_DIR = PROJECT_ROOT / "working" / "output"
DEFAULT_LOCK_PATH = PROJECT_ROOT / "working" / "output" / "remove_bad_overwrite_categories.lock"
IMPORT_SUMMARY_PREFIX = "Imported from 裁判文书网"
REVERT_SUMMARY = "回退覆盖导入版本并标记待检查"
MASK_CHAR = "\ue000"


@dataclass(frozen=True)
class RevisionText:
    revid: int
    timestamp: str
    user: str
    comment: str
    text: str


@dataclass(frozen=True)
class Decision:
    action: str
    reason: str
    content: str | None = None


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Remove unchecked-overwrite category when the overwritten import is obviously bad."
    )
    parser.add_argument("--category", default=DEFAULT_CATEGORY)
    parser.add_argument("--skip", type=int, default=0)
    parser.add_argument("--max", type=int, default=None)
    parser.add_argument("--batch-size", type=int, default=50)
    parser.add_argument("--quiet", action="store_true")
    parser.add_argument("--progress-every", type=int, default=100)
    parser.add_argument("--interval", type=float, default=1.0)
    parser.add_argument("--maxlag", type=int, default=5)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--summary", default="")
    parser.add_argument("--log-dir", type=Path, default=DEFAULT_LOG_DIR)
    parser.add_argument("--lock-path", type=Path, default=DEFAULT_LOCK_PATH)
    parser.add_argument("--no-require-overwrite-pair", action="store_true")
    return parser.parse_args()


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def build_log_paths(log_dir: Path) -> dict[str, Path]:
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    return {
        "fixed": log_dir / f"bad_overwrite_category_removed_{timestamp}.jsonl",
        "skipped": log_dir / f"bad_overwrite_category_skipped_{timestamp}.jsonl",
        "failed": log_dir / f"bad_overwrite_category_failed_{timestamp}.jsonl",
    }


def append_jsonl(path: Path, record: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8", newline="\n") as handle:
        handle.write(json.dumps(record, ensure_ascii=False))
        handle.write("\n")


def is_pid_running(pid: int) -> bool:
    try:
        os.kill(pid, 0)
    except OSError:
        return False
    return True


def acquire_lock(path: Path) -> int:
    path.parent.mkdir(parents=True, exist_ok=True)
    flags = os.O_CREAT | os.O_EXCL | os.O_WRONLY
    try:
        fd = os.open(path, flags)
    except FileExistsError as exc:
        pid_text = path.read_text(encoding="ascii", errors="ignore").strip()
        if pid_text.isdigit() and not is_pid_running(int(pid_text)):
            path.unlink()
            fd = os.open(path, flags)
            os.write(fd, str(os.getpid()).encode("ascii"))
            return fd
        raise RuntimeError(f"lock file exists: {path}") from exc
    os.write(fd, str(os.getpid()).encode("ascii"))
    return fd


def release_lock(path: Path, fd: int) -> None:
    try:
        os.close(fd)
    finally:
        try:
            path.unlink()
        except FileNotFoundError:
            pass


def iter_category_title_batches(category_title: str, maxlag: int, batch_size: int) -> Any:
    continuation: dict[str, Any] = {}
    while True:
        payload = post_query(
            {
                "list": "categorymembers",
                "cmtitle": category_title,
                "cmnamespace": "0",
                "cmlimit": str(batch_size),
                **continuation,
            },
            maxlag=maxlag,
        )
        titles = [
            member["title"]
            for member in payload.get("query", {}).get("categorymembers", [])
            if member.get("title")
        ]
        if titles:
            yield titles
        continuation = payload.get("continue", {})
        if not continuation:
            return


def fetch_latest_revisions(title: str, maxlag: int, limit: int = 3) -> list[RevisionText]:
    payload = post_query(
        {
            "titles": title,
            "prop": "revisions",
            "rvprop": "ids|timestamp|user|comment|content",
            "rvslots": "main",
            "rvlimit": str(limit),
        },
        maxlag=maxlag,
    )
    pages = payload.get("query", {}).get("pages", [])
    if not pages or "missing" in pages[0]:
        return []

    revisions: list[RevisionText] = []
    for revision in pages[0].get("revisions") or []:
        revisions.append(
            RevisionText(
                revid=int(revision.get("revid", 0)),
                timestamp=str(revision.get("timestamp", "")),
                user=str(revision.get("user", "")),
                comment=str(revision.get("comment", "")),
                text=str((revision.get("slots") or {}).get("main", {}).get("content", "")),
            )
        )
    return revisions


def remove_unchecked_category(text: str) -> str:
    text = UNCHECKED_OVERWRITE_CATEGORY_RE.sub("", text or "")
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.rstrip()


def equivalent_after_category_removal(left: str, right: str) -> bool:
    return normalize_wikitext_for_comparison(remove_unchecked_category(left)) == normalize_wikitext_for_comparison(
        remove_unchecked_category(right)
    )


def redaction_template_count(text: str) -> int:
    return sum(int(match.group(1)) for match in PRC_REDACT_TEMPLATE_RE.finditer(text or ""))


def raw_marker_count(text: str) -> int:
    return sum(len(match.group(0)) for match in REDACTION_SEQUENCE_PATTERN.finditer(text or ""))


def mask_redactions(text: str) -> str:
    text = normalize_wikitext_for_comparison(remove_unchecked_category(text))
    text = PRC_REDACT_TEMPLATE_RE.sub(lambda match: MASK_CHAR * int(match.group(1)), text)
    text = REDACTION_SEQUENCE_PATTERN.sub(lambda match: MASK_CHAR * len(match.group(0)), text)
    return text


def is_redaction_template_loss(current_text: str, import_text: str) -> bool:
    """Return whether import merely expands existing templates to raw markers."""
    current_template_count = redaction_template_count(current_text)
    import_template_count = redaction_template_count(import_text)
    if current_template_count <= import_template_count:
        return False
    if raw_marker_count(import_text) <= raw_marker_count(current_text):
        return False
    return mask_redactions(current_text) == mask_redactions(import_text)


def bad_import_reason(current_text: str, import_text: str) -> str | None:
    if normalize_wikitext_for_comparison(current_text) == normalize_wikitext_for_comparison(import_text):
        return "import_matches_current"

    if is_redaction_template_loss(current_text, import_text):
        return "redaction_template_loss"

    if is_safe_header_only_update(current_text, import_text):
        return "current_has_safe_header_fix"

    if is_safe_redaction_marker_update(current_text, import_text):
        return "current_has_safe_redaction_fix"

    if is_safe_formatting_improvement(current_text, import_text):
        return "current_has_safe_formatting_fix"

    if is_safe_signature_structure_improvement(current_text, import_text):
        return "current_has_safe_signature_structure_fix"

    return None


def decide(title: str, revisions: list[RevisionText], *, require_overwrite_pair: bool) -> Decision:
    if len(revisions) < 3:
        return Decision("skip", "fewer_than_three_revisions")

    current, import_revision, old_revision = revisions[:3]
    if not UNCHECKED_OVERWRITE_CATEGORY_RE.search(current.text):
        return Decision("skip", "current_page_not_in_unchecked_category")

    if require_overwrite_pair:
        if REVERT_SUMMARY not in current.comment:
            return Decision("skip", "latest_revision_is_not_standard_overwrite_revert")
        if not import_revision.comment.startswith(IMPORT_SUMMARY_PREFIX):
            return Decision("skip", "previous_revision_is_not_standard_import")

    current_without_category = remove_unchecked_category(current.text)
    if not equivalent_after_category_removal(current_without_category, old_revision.text):
        return Decision("skip", "current_without_category_does_not_match_pre_import_revision")

    reason = bad_import_reason(current_without_category, import_revision.text)
    if not reason:
        return Decision("skip", "import_not_obviously_bad")

    return Decision("fix", reason, current_without_category.rstrip() + "\n")


def main() -> int:
    args = parse_args()
    if args.skip < 0:
        raise ValueError("--skip must be non-negative")
    if args.max is not None and args.max <= 0:
        raise ValueError("--max must be greater than 0")
    if args.batch_size <= 0:
        raise ValueError("--batch-size must be greater than 0")
    if args.progress_every <= 0:
        raise ValueError("--progress-every must be greater than 0")

    logging.getLogger("pywikibot").setLevel(logging.WARNING)
    configure_throttle(interval=args.interval, maxlag=args.maxlag)

    lock_fd = acquire_lock(args.lock_path)
    log_paths = build_log_paths(args.log_dir)

    console.print("=" * 72)
    console.print("[bold cyan]Bad Overwrite Category Remover[/bold cyan]")
    console.print("=" * 72)
    console.print(f"Category:       {args.category}")
    console.print("Category scan:  streaming batches")
    console.print(f"Max pages:      {args.max if args.max is not None else 'unlimited'}")
    console.print(f"Batch size:     {args.batch_size}")
    console.print(f"Quiet:          {args.quiet}")
    console.print(f"Dry run:        {args.dry_run}")
    console.print(f"Edit interval:  {args.interval}s")
    console.print(f"Log dir:        {args.log_dir}")
    console.print("=" * 72)

    counts = {"processed": 0, "fixed": 0, "skipped": 0, "failed": 0}
    require_overwrite_pair = not args.no_require_overwrite_pair
    remaining_skip = args.skip

    try:
        for raw_batch in iter_category_title_batches(args.category, args.maxlag, args.batch_size):
            if remaining_skip:
                if remaining_skip >= len(raw_batch):
                    remaining_skip -= len(raw_batch)
                    continue
                raw_batch = raw_batch[remaining_skip:]
                remaining_skip = 0

            if args.max is not None:
                remaining = args.max - counts["processed"]
                if remaining <= 0:
                    break
                raw_batch = raw_batch[:remaining]

            for title in raw_batch:
                counts["processed"] += 1
                if not args.quiet:
                    console.print(f"[cyan][{counts['processed']}][/cyan] {title}")

                try:
                    revisions = fetch_latest_revisions(title, args.maxlag)
                    decision = decide(title, revisions, require_overwrite_pair=require_overwrite_pair)
                    record: dict[str, object] = {
                        "timestamp": utc_now_iso(),
                        "title": title,
                        "decision": decision.action,
                        "reason": decision.reason,
                        "revisions": [
                            {
                                "revid": revision.revid,
                                "timestamp": revision.timestamp,
                                "user": revision.user,
                                "comment": revision.comment,
                            }
                            for revision in revisions[:3]
                        ],
                    }

                    if decision.action != "fix" or decision.content is None:
                        counts["skipped"] += 1
                        append_jsonl(log_paths["skipped"], record)
                        if not args.quiet:
                            console.print(f"  [yellow]skip[/yellow] {decision.reason}")
                        continue

                    if args.dry_run:
                        counts["fixed"] += 1
                        append_jsonl(log_paths["fixed"], {**record, "dry_run": True})
                        if not args.quiet:
                            console.print(f"  [green]would remove[/green] {decision.reason}")
                        continue

                    save_page(title, decision.content, args.summary, minor=False, bot=True)
                    counts["fixed"] += 1
                    append_jsonl(log_paths["fixed"], record)
                    if not args.quiet:
                        console.print(f"  [green]removed[/green] {decision.reason}")
                except Exception as exc:
                    counts["failed"] += 1
                    append_jsonl(
                        log_paths["failed"],
                        {
                            "timestamp": utc_now_iso(),
                            "title": title,
                            "error": repr(exc),
                        },
                    )
                    console.print(f"  [red]failed[/red] {exc}")

                if args.quiet and counts["processed"] % args.progress_every == 0:
                    console.print(
                        f"[cyan]{counts['processed']} processed[/cyan]; "
                        f"removed {counts['fixed']}; skipped {counts['skipped']}; failed {counts['failed']}"
                    )
    finally:
        release_lock(args.lock_path, lock_fd)

    console.print("=" * 72)
    console.print(
        f"Processed {counts['processed']}; removed {counts['fixed']}; "
        f"skipped {counts['skipped']}; failed {counts['failed']}"
    )
    console.print(f"Logs: {args.log_dir}")
    return 1 if counts["failed"] else 0


if __name__ == "__main__":
    raise SystemExit(main())
