#!/usr/bin/env python
"""
Reconcile title and wikitext differences between two JSONL snapshots on zhwikisource.

For each entry where title or wikitext differs between --old-jsonl and --new-jsonl:

  0. Build the case-number redirect title (court + 案号 + type).

  1. Title changes:
     1.1  Landing page == old title
          → Move old-title page to new title.
          → Update case-number redirect to point at new title.
     1.2  Landing page == case-number title (multiple documents share the same canonical
          title due to identical naming; since their case numbers differ, each document
          lives at its case-number page, while the canonical title hosts a {{Versions}}
          disambiguation page listing all versions)
       1.2.1  old-title page exists and is a {{Versions page
              → Move old-title page to new title.
              → Update header |title in newly moved page.
              → Update header |title in case-number page (the landing page).
       1.2.2  old-title page is a redirect (to a new-title versions page)
              → Update header |title in case-number page (the landing page).

  2. Wikitext changes:
     Take the *current* zhwikisource page content (not the new-jsonl wikitext),
     run normalize_redaction_markers over it, and save if it changed.

Changes to the same page are batched into one save call per page.
"""

from __future__ import annotations

import argparse
import difflib
import json
import re
import logging
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from dotenv import load_dotenv

load_dotenv(PROJECT_ROOT / ".env")

import pywikibot

from rich.console import Console
from rich.progress import (
    BarColumn,
    MofNCompleteColumn,
    Progress,
    SpinnerColumn,
    TaskProgressColumn,
    TextColumn,
    TimeRemainingColumn,
)

from upload.mediawiki import configure_throttle, get_site
from convert.html_normalizer import normalize_redaction_markers, normalize_title_redaction_markers

console = Console()

REDIRECT_CATEGORY = "中华人民共和国法院裁判文书案号重定向"


# ── CLI ───────────────────────────────────────────────────────────────────────

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Reconcile zhwikisource pages from two JSONL snapshot diffs."
    )
    parser.add_argument("--old-jsonl", type=Path, required=True, metavar="PATH")
    parser.add_argument("--new-jsonl", type=Path, required=True, metavar="PATH")
    parser.add_argument(
        "--max", type=int, default=55120,
        help="Maximum entries to compare from each file (default: 55120)",
    )
    parser.add_argument(
        "--log-dir", type=Path, default=None,
        help="Directory for output logs (default: same directory as --new-jsonl)",
    )
    parser.add_argument(
        "--interval", type=float, default=10.0,
        help="Minimum seconds between edits (default: 10)",
    )
    parser.add_argument(
        "--maxlag", type=int, default=5,
        help="Maxlag parameter for MediaWiki API (default: 5)",
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Plan actions without saving any changes",
    )
    return parser.parse_args()


# ── Utilities ─────────────────────────────────────────────────────────────────

def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def normalize_case_number(case_number: str) -> str:
    return case_number.replace("(", "（").replace(")", "）")


def get_entry_title(record: dict[str, Any]) -> str:
    for key in ("final_title", "title"):
        v = record.get(key)
        if isinstance(v, str) and v.strip():
            return v.strip()
    return ""


def build_redirect_title(record: dict[str, Any]) -> str:
    """Build the case-number redirect title from a JSONL record."""
    court = record.get("court", "").strip()
    case_num = normalize_case_number(record.get("doc_id", "").strip())
    doc_type = record.get("doc_type", "").strip()
    return court + case_num + doc_type


_ADJACENT_REDACT_RE = re.compile(
    r'(?:[×XxＸｘ*＊∗✱﹡⁎٭※]+|\{\{PRC-redact\|\d+\}\}){2,}'
)
_REDACT_ELEMENT_RE = re.compile(
    r'(?P<chars>[×XxＸｘ*＊∗✱﹡⁎٭※]+)|\{\{PRC-redact\|(?P<n>\d+)\}\}'
)


def combine_adjacent_redactions(text: str) -> str:
    """Merge runs of adjacent raw redaction chars and {{PRC-redact|N}} templates.

    After normalize_redaction_markers runs on content that was already partially
    normalised by the old buggy pattern (e.g. `*{{PRC-redact|1}}`), the lone raw
    char becomes a second template yielding `{{PRC-redact|1}}{{PRC-redact|1}}`.
    This function collapses any such adjacent sequence into one template.
    """
    def _merge(m: re.Match) -> str:
        total = 0
        for em in _REDACT_ELEMENT_RE.finditer(m.group(0)):
            if em.group("chars"):
                total += len(em.group("chars"))
            else:
                total += int(em.group("n"))
        return f"{{{{PRC-redact|{total}}}}}"

    return _ADJACENT_REDACT_RE.sub(_merge, text)


def update_redirect_target(page_text: str, new_target: str) -> str:
    """Replace the redirect target inside #REDIRECT [[...]]."""
    return re.sub(
        r'(#REDIRECT\s*)\[\[.*?\]\]',
        rf'\1[[{new_target}]]',
        page_text,
        count=1,
        flags=re.IGNORECASE,
    )


def update_header_title_field(page_text: str, new_title: str) -> str:
    """
    Update |title = ... inside {{Header/裁判文书.

    Preserves [[...]] wrapper if the existing value is wikilinked.
    """
    def replacer(m: re.Match) -> str:
        existing_val = m.group(1).strip()
        if existing_val.startswith("[[") and existing_val.endswith("]]"):
            return f"|title = [[{new_title}]]"
        return f"|title = {new_title}"

    return re.sub(r'\|title\s*=\s*(.+)', replacer, page_text, count=1)


def print_dry_run_diff(
    index: int,
    old_title: str,
    new_title: str,
    pending_moves: list[tuple[str, str, str]],
    pending_edits: dict[str, tuple[str, str]],
    pending_before: dict[str, str],
) -> None:
    console.rule(f"[bold]Entry {index}[/bold]  {old_title}  →  {new_title}", style="cyan")

    for old_t, new_t, reason in pending_moves:
        console.print(f"  [yellow bold]MOVE[/yellow bold]  [[{old_t}]]  →  [[{new_t}]]")
        console.print(f"         {reason}")

    for page_title, (new_text, summary) in pending_edits.items():
        before = pending_before.get(page_title, "")
        console.print(f"\n  [cyan bold]EDIT[/cyan bold]  [[{page_title}]]  —  {summary}")

        diff_lines = list(difflib.unified_diff(
            before.splitlines(keepends=True),
            new_text.splitlines(keepends=True),
            fromfile=f"before",
            tofile=f"after",
            n=2,
        ))

        if not diff_lines:
            console.print("         [dim](text unchanged)[/dim]")
            continue

        for line in diff_lines:
            stripped = line.rstrip("\n")
            if line.startswith("+++") or line.startswith("---"):
                console.print(f"[dim]{stripped}[/dim]")
            elif line.startswith("+"):
                console.print(f"[green]{stripped}[/green]")
            elif line.startswith("-"):
                console.print(f"[red]{stripped}[/red]")
            elif line.startswith("@@"):
                console.print(f"[cyan]{stripped}[/cyan]")
            else:
                console.print(f"[dim]{stripped}[/dim]")


def append_jsonl(path: Path, record: dict[str, Any]) -> None:
    with path.open("a", encoding="utf-8", newline="\n") as f:
        f.write(json.dumps(record, ensure_ascii=False))
        f.write("\n")


def build_log_paths(log_dir: Path) -> dict[str, Path]:
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    return {
        "moved":      log_dir / f"reconcile_moved_{ts}.jsonl",
        "normalized": log_dir / f"reconcile_normalized_{ts}.jsonl",
        "skipped":    log_dir / f"reconcile_skipped_{ts}.jsonl",
        "failed":     log_dir / f"reconcile_failed_{ts}.jsonl",
    }


# ── JSONL reading ─────────────────────────────────────────────────────────────

def read_jsonl_entries(path: Path, max_count: int) -> list[dict[str, Any]]:
    entries: list[dict[str, Any]] = []
    with path.open("r", encoding="utf-8") as f:
        for line in f:
            stripped = line.strip()
            if not stripped:
                continue
            try:
                entries.append(json.loads(stripped))
            except json.JSONDecodeError:
                continue
            if len(entries) >= max_count:
                break
    return entries


# ── Per-record processing ─────────────────────────────────────────────────────

def process_pair(
    *,
    index: int,
    old_entry: dict[str, Any],
    new_entry: dict[str, Any],
    site: pywikibot.Site,
    dry_run: bool,
    counts: dict[str, int],
    log_paths: dict[str, Path],
) -> None:
    old_title = get_entry_title(old_entry)
    new_title = get_entry_title(new_entry)

    if not old_title or not new_title:
        counts["invalid"] += 1
        return

    title_differs   = old_title != new_title
    wikitext_differs = old_entry.get("wikitext", "") != new_entry.get("wikitext", "")

    if not title_differs and not wikitext_differs:
        counts["unchanged"] += 1
        return

    redirect_title = build_redirect_title(new_entry)

    # pending_edits: page_title → (new_text, summary)
    pending_edits: dict[str, tuple[str, str]] = {}
    # pending_before: page_title → original text (for dry-run diff display)
    pending_before: dict[str, str] = {}
    # pending_moves: list of (old_t, new_t, summary) — executed before edits
    pending_moves: list[tuple[str, str, str]] = []

    # The effective landing page title after any move — used for wikitext step.
    # Initialised from the redirect resolution below; may be overwritten.
    effective_landing_title: str | None = None

    # ── Step 1: resolve landing page and title action ────────────────────────
    # Always resolve the landing page — the wikitext step needs it too, and we
    # want to catch raw redaction chars in the landing title independently of
    # whether the two JSONLs report a title change.
    try:
        redirect_page = pywikibot.Page(site, redirect_title)

        if redirect_page.exists() and redirect_page.isRedirectPage():
            landing_page = redirect_page.getRedirectTarget()
            landing_title = landing_page.title()
            effective_landing_title = landing_title  # refined below if renamed

            normalised_old     = normalize_title_redaction_markers(old_title)
            normalised_landing = normalize_title_redaction_markers(landing_title)

            # Trigger (a): JSONL says rename AND wiki page is at old title
            #   (exact match or already-normalised form)
            jsonl_rename = title_differs and landing_title in (old_title, normalised_old)

            # Trigger (b): landing title itself carries raw redaction chars
            landing_has_raw = normalised_landing != landing_title

            if jsonl_rename:
                desired_title = new_title
            elif landing_has_raw:
                desired_title = normalised_landing
            else:
                desired_title = None

            if desired_title is not None:
                if landing_title != desired_title:
                    pending_moves.append((
                        landing_title, desired_title,
                        f"标题更新：[[{landing_title}]] → [[{desired_title}]]",
                    ))
                new_redirect_text = update_redirect_target(redirect_page.text, desired_title)
                if new_redirect_text != redirect_page.text:
                    pending_before[redirect_title] = redirect_page.text
                    pending_edits[redirect_title] = (
                        new_redirect_text,
                        f"更新重定向目标至[[{desired_title}]]",
                    )
                effective_landing_title = desired_title
                append_jsonl(log_paths["moved"], {
                    "case": "1.1", "index": index,
                    "reason": "jsonl_rename" if jsonl_rename else "landing_normalise",
                    "old_title": old_title, "landing_title": landing_title,
                    "desired_title": desired_title, "redirect_title": redirect_title,
                    "timestamp": utc_now_iso(),
                })

            elif title_differs:
                append_jsonl(log_paths["skipped"], {
                    "reason": "landing_title_unexpected",
                    "index": index, "old_title": old_title, "new_title": new_title,
                    "redirect_title": redirect_title, "landing_title": landing_title,
                    "timestamp": utc_now_iso(),
                })
                counts["skipped"] += 1
                title_differs = False

        elif title_differs and redirect_page.exists() and not redirect_page.isRedirectPage():
            # 1.2 — case-number page IS the document landing page (disambiguation via
            # case number because multiple documents share the same canonical title)
            effective_landing_title = redirect_title
            old_title_page = pywikibot.Page(site, old_title)

            if not old_title_page.exists():
                append_jsonl(log_paths["skipped"], {
                    "reason": "old_title_page_missing",
                    "index": index, "old_title": old_title, "new_title": new_title,
                    "redirect_title": redirect_title, "timestamp": utc_now_iso(),
                })
                counts["skipped"] += 1
                title_differs = False

            elif old_title_page.isRedirectPage():
                # 1.2.2 — T_old is a redirect; update case-number page header
                new_case_text = update_header_title_field(redirect_page.text, new_title)
                pending_before[redirect_title] = redirect_page.text
                pending_edits[redirect_title] = (
                    new_case_text,
                    f"更新标题引用至{new_title}",
                )
                append_jsonl(log_paths["moved"], {
                    "case": "1.2.2", "index": index,
                    "old_title": old_title, "new_title": new_title,
                    "redirect_title": redirect_title,
                    "timestamp": utc_now_iso(),
                })

            elif "{{Versions" in old_title_page.text:
                # 1.2.1 — T_old is a {{Versions page; move, then update both pages.
                # The document itself stays at redirect_title; only the disambiguation
                # page is being renamed. effective_landing_title stays as redirect_title.
                pending_moves.append((
                    old_title, new_title,
                    f"标题更新：[[{old_title}]] → [[{new_title}]]",
                ))
                # After the move the content at new_title will be old_title_page.text
                new_versions_text = update_header_title_field(
                    old_title_page.text, new_title
                )
                pending_before[new_title] = old_title_page.text
                pending_edits[new_title] = (
                    new_versions_text,
                    f"更新标题至{new_title}",
                )
                new_case_text = update_header_title_field(redirect_page.text, new_title)
                pending_before[redirect_title] = redirect_page.text
                pending_edits[redirect_title] = (
                    new_case_text,
                    f"更新标题引用至{new_title}",
                )
                append_jsonl(log_paths["moved"], {
                    "case": "1.2.1", "index": index,
                    "old_title": old_title, "new_title": new_title,
                    "redirect_title": redirect_title,
                    "timestamp": utc_now_iso(),
                })

            else:
                append_jsonl(log_paths["skipped"], {
                    "reason": "old_title_page_not_versions_or_redirect",
                    "index": index, "old_title": old_title, "new_title": new_title,
                    "redirect_title": redirect_title, "timestamp": utc_now_iso(),
                })
                counts["skipped"] += 1
                title_differs = False

        elif title_differs:
            append_jsonl(log_paths["skipped"], {
                "reason": "redirect_page_missing",
                "index": index, "old_title": old_title, "new_title": new_title,
                "redirect_title": redirect_title, "timestamp": utc_now_iso(),
            })
            counts["skipped"] += 1
            title_differs = False

    except Exception as exc:
        counts["failed"] += 1
        append_jsonl(log_paths["failed"], {
            "step": "title", "index": index,
            "old_title": old_title, "new_title": new_title,
            "redirect_title": redirect_title,
            "error": str(exc), "timestamp": utc_now_iso(),
        })
        return

    # ── Step 2: resolve wikitext action ──────────────────────────────────────
    if wikitext_differs:
        try:
            if effective_landing_title is not None:
                landing_page = pywikibot.Page(site, effective_landing_title)

                # For a page that is about to be moved, we read from the old location
                if pending_moves and pending_moves[0][1] == effective_landing_title:
                    landing_page = pywikibot.Page(site, pending_moves[0][0])

                current_text = landing_page.text
                normalized_text = combine_adjacent_redactions(normalize_redaction_markers(current_text))

                if normalized_text != current_text:
                    existing = pending_edits.get(effective_landing_title)
                    if existing:
                        # Re-normalize the already-pending text; keep original as before
                        pending_edits[effective_landing_title] = (
                            combine_adjacent_redactions(normalize_redaction_markers(existing[0])),
                            existing[1] + "；规范化编辑标记",
                        )
                    else:
                        pending_before[effective_landing_title] = current_text
                        pending_edits[effective_landing_title] = (
                            normalized_text,
                            "规范化编辑标记",
                        )
                    append_jsonl(log_paths["normalized"], {
                        "index": index,
                        "page_title": effective_landing_title,
                        "timestamp": utc_now_iso(),
                    })

        except Exception as exc:
            counts["failed"] += 1
            append_jsonl(log_paths["failed"], {
                "step": "wikitext", "index": index,
                "old_title": old_title, "new_title": new_title,
                "redirect_title": redirect_title,
                "error": str(exc), "timestamp": utc_now_iso(),
            })
            return

    if not pending_moves and not pending_edits:
        counts["unchanged"] += 1
        return

    # ── Execute ───────────────────────────────────────────────────────────────
    if dry_run:
        counts["would_act"] += 1
        print_dry_run_diff(
            index=index,
            old_title=old_title,
            new_title=new_title,
            pending_moves=pending_moves,
            pending_edits=pending_edits,
            pending_before=pending_before,
        )
        return

    try:
        for old_t, new_t, reason in pending_moves:
            src = pywikibot.Page(site, old_t)
            src.move(new_t, reason=reason, noredirect=True, movetalk=False)

        for page_title, (new_text, summary) in pending_edits.items():
            page = pywikibot.Page(site, page_title)
            page.text = new_text
            page.save(summary=summary, minor=False, botflag=True)

        counts["acted"] += 1

    except Exception as exc:
        counts["failed"] += 1
        append_jsonl(log_paths["failed"], {
            "step": "execute", "index": index,
            "old_title": old_title, "new_title": new_title,
            "redirect_title": redirect_title,
            "error": str(exc), "timestamp": utc_now_iso(),
        })


# ── Main ──────────────────────────────────────────────────────────────────────

def main() -> int:
    args = parse_args()

    old_path = args.old_jsonl.expanduser().resolve()
    new_path = args.new_jsonl.expanduser().resolve()

    for path in (old_path, new_path):
        if not path.exists():
            print(f"Error: file not found: {path}", file=sys.stderr)
            return 1

    log_dir = (args.log_dir or new_path.parent).expanduser().resolve()
    log_dir.mkdir(parents=True, exist_ok=True)
    log_paths = build_log_paths(log_dir)

    logging.getLogger("pywiki").setLevel(logging.WARNING)
    logging.getLogger("pywikibot").setLevel(logging.WARNING)

    configure_throttle(interval=args.interval, maxlag=args.maxlag)
    site = get_site()

    console.print("=" * 72)
    console.print("[bold cyan]JSONL Diff Reconciler[/bold cyan]")
    console.print("=" * 72)
    console.print(f"Old JSONL:   {old_path}")
    console.print(f"New JSONL:   {new_path}")
    console.print(f"Max entries: {args.max}")
    console.print(f"Log dir:     {log_dir}")
    console.print(f"Dry run:     {args.dry_run}")
    console.print("=" * 72)
    console.print()

    console.print("Reading JSONL files…")
    old_entries = read_jsonl_entries(old_path, args.max)
    new_entries = read_jsonl_entries(new_path, args.max)
    pair_count  = min(len(old_entries), len(new_entries))
    console.print(f"Loaded {len(old_entries)} old / {len(new_entries)} new entries → {pair_count} pairs")
    console.print()

    counts: dict[str, int] = {
        "unchanged": 0,
        "invalid":   0,
        "skipped":   0,
        "acted":     0,
        "would_act": 0,
        "failed":    0,
    }

    start = datetime.now()

    with Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        BarColumn(bar_width=None),
        MofNCompleteColumn(),
        TaskProgressColumn(),
        TextColumn("•"),
        TimeRemainingColumn(),
        console=console,
        transient=False,
    ) as progress:
        task = progress.add_task("Reconciling…", total=pair_count)

        for idx in range(pair_count):
            process_pair(
                index=idx,
                old_entry=old_entries[idx],
                new_entry=new_entries[idx],
                site=site,
                dry_run=args.dry_run,
                counts=counts,
                log_paths=log_paths,
            )
            progress.advance(task)

    duration = datetime.now() - start
    action_label = "Would act" if args.dry_run else "Acted"

    console.print()
    console.print("=" * 72)
    console.print("[bold green]Done[/bold green]")
    console.print("=" * 72)
    console.print(f"Pairs compared: {pair_count}")
    console.print(f"Unchanged:      {counts['unchanged']}")
    console.print(f"Invalid:        {counts['invalid']}")
    console.print(f"Skipped:        {counts['skipped']}")
    console.print(f"{action_label}:       {counts['acted'] if not args.dry_run else counts['would_act']}")
    console.print(f"Failed:         {counts['failed']}")
    console.print(f"Duration:       {duration}")
    console.print("=" * 72)

    if counts["failed"] > 0:
        console.print(f"\n[yellow]Check {log_paths['failed']} for failures.[/yellow]")

    return 1 if counts["failed"] > 0 and counts["acted"] == 0 else 0


if __name__ == "__main__":
    raise SystemExit(main())
