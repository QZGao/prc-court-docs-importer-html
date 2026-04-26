#!/usr/bin/env python
"""
Fix missing 中华人民共和国 in judgment category names for court document pages.

For each page title in working/lista.txt:
1. Fetch the current page content from zhwikisource.
2. [[Category:XXXX年YY判决书]] → [[Category:XXXX年中华人民共和国YY判决书]]
3. [[Category:YY判决书]] (no year prefix) → [[Category:中华人民共和国YY判决书]]
4. Save the page if any change was made.
"""

from __future__ import annotations

import argparse
import re
import sys
import time
from pathlib import Path
from typing import Any

import requests

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from dotenv import load_dotenv

load_dotenv(PROJECT_ROOT / ".env")

from rich.console import Console

from upload.mediawiki import Page, configure_throttle, get_site

console = Console()

API_URL = "https://zh.wikisource.org/w/api.php"
USER_AGENT = "prc-court-docs-importer-html/1.0 (SuperGrey)"
DEFAULT_LISTA = PROJECT_ROOT / "working" / "lista.txt"
INSERT = "中华人民共和国"

# [[Category:2023年民事判决书]] — year + type, INSERT missing after 年
_CAT1 = re.compile(r"\[\[Category:(\d+年)(?!" + INSERT + r")(\S+判决书)\]\]")

# [[Category:民事判决书]] — no year prefix, INSERT missing after Category:
# (?!\d) excludes year-prefixed categories so cat1 fixes aren't re-matched.
_CAT2 = re.compile(r"\[\[Category:(?!\d)(?!" + INSERT + r")(\S+判决书)\]\]")


def fix_categories(text: str) -> tuple[str, list[str]]:
    changes: list[str] = []

    def replace_cat1(m: re.Match) -> str:
        old = m.group(0)
        new = f"[[Category:{m.group(1)}{INSERT}{m.group(2)}]]"
        changes.append(f"{old} → {new}")
        return new

    def replace_cat2(m: re.Match) -> str:
        old = m.group(0)
        new = f"[[Category:{INSERT}{m.group(1)}]]"
        changes.append(f"{old} → {new}")
        return new

    text = _CAT1.sub(replace_cat1, text)
    text = _CAT2.sub(replace_cat2, text)
    return text, changes


def read_titles(lista_path: Path) -> list[str]:
    titles = []
    for line in lista_path.read_text(encoding="utf-8").splitlines():
        title = line.strip()
        if title:
            titles.append(title)
    return titles


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Fix missing 中华人民共和国 in judgment categories on zhwikisource."
    )
    parser.add_argument(
        "--lista",
        type=Path,
        default=DEFAULT_LISTA,
        help=f"Path to page-title list (default: {DEFAULT_LISTA})",
    )
    parser.add_argument(
        "--skip",
        type=int,
        default=0,
        help="Number of titles to skip before processing (default: 0)",
    )
    parser.add_argument(
        "--max",
        type=int,
        default=None,
        help="Maximum number of titles to process",
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
        help="Show what would change without saving",
    )
    return parser.parse_args()


def fetch_page_content(session: requests.Session, title: str, maxlag: int) -> dict[str, Any]:
    attempt = 0
    while True:
        response = session.post(
            API_URL,
            data={
                "action": "query",
                "format": "json",
                "formatversion": "2",
                "titles": title,
                "prop": "revisions",
                "rvprop": "content",
                "rvslots": "main",
                "maxlag": maxlag,
            },
            timeout=60,
        )
        if response.status_code == 429:
            retry_after = int(response.headers.get("Retry-After", 60))
            attempt += 1
            console.print(f"[yellow]Rate limited (429) — waiting {retry_after}s (retry #{attempt})…[/yellow]")
            time.sleep(retry_after)
            continue
        response.raise_for_status()
        payload = response.json()
        if "error" in payload:
            raise RuntimeError(payload["error"])

        pages = payload.get("query", {}).get("pages", [])
        if not pages:
            return {"exists": False, "content": ""}

        page = pages[0]
        if "missing" in page:
            return {"exists": False, "content": ""}

        revisions = page.get("revisions") or []
        content = ""
        if revisions:
            content = revisions[0].get("slots", {}).get("main", {}).get("content", "")
        return {"exists": True, "content": content}


def main() -> int:
    args = parse_args()

    lista_path = args.lista.expanduser().resolve()
    if not lista_path.exists():
        console.print(f"[red]Error: lista file not found: {lista_path}[/red]")
        return 1

    titles = read_titles(lista_path)
    if args.skip:
        titles = titles[args.skip :]
    if args.max is not None:
        titles = titles[: args.max]

    console.print("=" * 72)
    console.print("[bold cyan]Category PRC Fixer[/bold cyan]")
    console.print("=" * 72)
    console.print(f"Lista:          {lista_path}")
    console.print(f"Titles:         {len(titles)}")
    console.print(f"Edit interval:  {args.interval}s")
    console.print(f"Maxlag:         {args.maxlag}")
    console.print(f"Dry run:        {args.dry_run}")
    console.print("=" * 72)
    console.print()

    configure_throttle(interval=args.interval, maxlag=args.maxlag)
    site = None if args.dry_run else get_site()

    session = requests.Session()
    session.headers["User-Agent"] = USER_AGENT

    counts = {"processed": 0, "fixed": 0, "no_change": 0, "missing": 0, "failed": 0}
    last_edit_time = 0.0

    for title in titles:
        counts["processed"] += 1
        console.print(f"[cyan][{counts['processed']}/{len(titles)}][/cyan] {title}")

        try:
            result = fetch_page_content(session, title, args.maxlag)
        except Exception as exc:
            counts["failed"] += 1
            console.print(f"  [red]Fetch failed: {exc}[/red]")
            continue

        if not result["exists"]:
            counts["missing"] += 1
            console.print("  [yellow]Page not found — skipping[/yellow]")
            continue

        content = result["content"]
        new_content, changes = fix_categories(content)

        if not changes:
            counts["no_change"] += 1
            console.print("  [dim]No changes needed[/dim]")
            continue

        for change in changes:
            console.print(f"  [green]{change}[/green]")

        if args.dry_run:
            counts["fixed"] += 1
            console.print("  [dim](dry run — not saved)[/dim]")
            continue

        elapsed = time.monotonic() - last_edit_time
        if elapsed < args.interval:
            time.sleep(args.interval - elapsed)

        try:
            page = Page(site, title)
            page.text = new_content
            page.save(summary="; ".join(changes), minor=True, botflag=True)
            counts["fixed"] += 1
            last_edit_time = time.monotonic()
            console.print("  [bold green]Saved[/bold green]")
        except Exception as exc:
            counts["failed"] += 1
            console.print(f"  [red]Save failed: {exc}[/red]")

    console.print()
    console.print("=" * 72)
    action = "Would fix" if args.dry_run else "Fixed"
    console.print(f"Processed:   {counts['processed']}")
    console.print(f"{action}:     {counts['fixed']}")
    console.print(f"No change:   {counts['no_change']}")
    console.print(f"Missing:     {counts['missing']}")
    console.print(f"Failed:      {counts['failed']}")
    console.print("=" * 72)

    return 1 if counts["failed"] > 0 else 0


if __name__ == "__main__":
    raise SystemExit(main())
