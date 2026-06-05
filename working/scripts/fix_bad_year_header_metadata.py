#!/usr/bin/env python
"""Fix bad Header/裁判文书 year values from known bad year categories."""

from __future__ import annotations

import argparse
import ctypes
import json
import logging
import os
import re
import sys
import time
from dataclasses import asdict, dataclass
from datetime import date, datetime
from pathlib import Path
from typing import Any, Iterable

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from dotenv import load_dotenv

load_dotenv(PROJECT_ROOT / ".env")

from upload.mediawiki import configure_throttle, get_site, post_query

BAD_CATEGORY_YEARS = (2, 6, 7, 8, 9, 10, 12, 17, 18, 19, 20, 21, 22, 23, 24)
MIN_PRC_YEAR = 1949
LOCK_PATH = PROJECT_ROOT / "working" / "bad_year_header_cleanup.lock"
THROTTLE_PATH = PROJECT_ROOT / "throttle.ctrl"
HEADER_RE = re.compile(r"(?ms)^\{\{\s*Header/裁判文书\b.*?^\}\}\s*$")
CASE_NUMBER_FIELD_RE = re.compile(r"(?m)^\s*\|\s*案号\s*=\s*(?P<value>.*?)\s*$")
YEAR_FIELD_RE = re.compile(
    r"(?m)^(?P<prefix>[ \t]*\|[ \t]*year[ \t]*=[ \t]*)(?P<value>[^\n]*?)(?P<suffix>[ \t]*)$"
)
CASE_NUMBER_YEAR_RE = re.compile(r"[（(]\s*(\d{4})\s*[）)]")
SIGNATURE_RE = re.compile(r"(?ms)\{\{裁判文书署名\|1=.*?\n\}\}")
ARABIC_DATE_YEAR_RE = re.compile(r"\b(20\d{2})年")
CHINESE_DATE_YEAR_RE = re.compile(r"([二〇零○O0一二三四五六七八九]{4})年")
CHINESE_YEAR_DIGITS = {
    "〇": "0",
    "零": "0",
    "○": "0",
    "O": "0",
    "0": "0",
    "一": "1",
    "二": "2",
    "三": "3",
    "四": "4",
    "五": "5",
    "六": "6",
    "七": "7",
    "八": "8",
    "九": "9",
}


@dataclass
class Decision:
    title: str
    action: str
    category: str = ""
    current_year: str = ""
    inferred_year: str = ""
    reason: str = ""


@dataclass
class PageText:
    title: str
    exists: bool
    content: str = ""
    timestamp: str = ""


def is_pid_running(pid: int) -> bool:
    if os.name == "nt":
        process_query_limited_information = 0x1000
        handle = ctypes.windll.kernel32.OpenProcess(
            process_query_limited_information,
            False,
            int(pid),
        )
        if handle:
            ctypes.windll.kernel32.CloseHandle(handle)
            return True
        return False

    try:
        os.kill(pid, 0)
    except OSError:
        return False
    return True


def acquire_lock(path: Path) -> int:
    path.parent.mkdir(parents=True, exist_ok=True)
    flags = os.O_CREAT | os.O_EXCL | os.O_WRONLY
    while True:
        try:
            fd = os.open(path, flags)
            os.write(fd, str(os.getpid()).encode("ascii"))
            return fd
        except FileExistsError:
            pid_text = path.read_text(encoding="ascii", errors="ignore").strip()
            if pid_text.isdigit() and not is_pid_running(int(pid_text)):
                path.unlink()
                continue
            raise RuntimeError(f"cleanup lock exists: {path}")


def release_lock(path: Path, fd: int) -> None:
    try:
        os.close(fd)
    finally:
        try:
            path.unlink()
        except FileNotFoundError:
            pass


def valid_prc_year(value: str | None) -> bool:
    if not value:
        return False
    try:
        year = int(value)
    except ValueError:
        return False
    return MIN_PRC_YEAR <= year <= date.today().year


def normalize_category_title(value: str) -> str:
    value = value.strip()
    return value if value.startswith("Category:") else f"Category:{value}"


def default_categories() -> list[str]:
    return [f"Category:{year}年" for year in BAD_CATEGORY_YEARS]


def iter_category_titles(category_title: str, batch_size: int, maxlag: int) -> Iterable[str]:
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
        for member in payload.get("query", {}).get("categorymembers", []):
            title = member.get("title")
            if isinstance(title, str) and title.strip():
                yield title.strip()
        continuation = payload.get("continue", {})
        if not continuation:
            return


def fetch_page_text_batch(titles: list[str], batch_size: int, maxlag: int) -> dict[str, PageText]:
    results: dict[str, PageText] = {}
    for index in range(0, len(titles), batch_size):
        batch = titles[index : index + batch_size]
        payload = post_query(
            {
                "titles": "|".join(batch),
                "prop": "revisions",
                "rvprop": "timestamp|content",
                "rvslots": "main",
            },
            maxlag=maxlag,
        )
        page_map = {page.get("title", ""): page for page in payload.get("query", {}).get("pages", [])}
        for title in batch:
            page = page_map.get(title)
            if not page or "missing" in page:
                results[title] = PageText(title=title, exists=False)
                continue
            revision = (page.get("revisions") or [{}])[0]
            main_slot = (revision.get("slots") or {}).get("main") or {}
            results[title] = PageText(
                title=title,
                exists=True,
                content=str(main_slot.get("content", "")),
                timestamp=str(revision.get("timestamp", "")),
            )
    return results


def collect_titles(
    categories: list[str],
    batch_size: int,
    maxlag: int,
    limit: int | None = None,
) -> dict[str, str]:
    title_category: dict[str, str] = {}
    for category in categories:
        count_before = len(title_category)
        for title in iter_category_titles(category, batch_size=batch_size, maxlag=maxlag):
            title_category.setdefault(title, category)
            if limit is not None and len(title_category) >= limit:
                break
        print(f"{category}: {len(title_category) - count_before} titles")
        if limit is not None and len(title_category) >= limit:
            break
    return title_category


def extract_inferred_case_year(header: str) -> str | None:
    case_match = CASE_NUMBER_FIELD_RE.search(header)
    if not case_match:
        return None

    year_match = CASE_NUMBER_YEAR_RE.search(case_match.group("value"))
    if not year_match:
        return None

    year = year_match.group(1)
    return year if valid_prc_year(year) else None


def extract_inferred_signature_year(text: str) -> str | None:
    signature_match = SIGNATURE_RE.search(text or "")
    if not signature_match:
        return None

    signature = signature_match.group(0)
    arabic_match = ARABIC_DATE_YEAR_RE.search(signature)
    if arabic_match and valid_prc_year(arabic_match.group(1)):
        return arabic_match.group(1)

    chinese_match = CHINESE_DATE_YEAR_RE.search(signature)
    if not chinese_match:
        return None

    year = "".join(CHINESE_YEAR_DIGITS.get(char, "") for char in chinese_match.group(1))
    return year if valid_prc_year(year) else None


def build_fixed_text(title: str, category: str, text: str) -> tuple[str, Decision]:
    header_match = HEADER_RE.search(text or "")
    if not header_match:
        return text, Decision(title=title, category=category, action="skip", reason="missing_header")

    header = header_match.group(0)
    year_match = YEAR_FIELD_RE.search(header)
    if not year_match:
        return text, Decision(title=title, category=category, action="skip", reason="missing_year_field")

    current_year = year_match.group("value").strip()
    if valid_prc_year(current_year):
        return text, Decision(
            title=title,
            category=category,
            action="skip",
            current_year=current_year,
            reason="current_year_already_valid",
        )

    inferred_year = extract_inferred_case_year(header) or extract_inferred_signature_year(text)
    if not inferred_year:
        return text, Decision(
            title=title,
            category=category,
            action="skip",
            current_year=current_year,
            reason="no_valid_case_number_year",
        )

    fixed_header = (
        header[: year_match.start()]
        + f"{year_match.group('prefix')}{inferred_year}{year_match.group('suffix')}"
        + header[year_match.end() :]
    )
    fixed_text = text[: header_match.start()] + fixed_header + text[header_match.end() :]
    return fixed_text, Decision(
        title=title,
        category=category,
        action="fix",
        current_year=current_year,
        inferred_year=inferred_year,
    )


def save_text(site: Any, title: str, text: str, basetimestamp: str, maxlag: int) -> None:
    payload = site.simple_request(
        action="edit",
        title=title,
        text=text,
        summary="",
        minor="1",
        bot="1",
        token=site.tokens["csrf"],
        basetimestamp=basetimestamp,
        format="json",
        formatversion="2",
        maxlag=maxlag,
    ).submit()
    if "error" in payload:
        raise RuntimeError(payload["error"])
    edit = payload.get("edit", {})
    if edit.get("result") != "Success":
        raise RuntimeError(payload)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--category",
        action="append",
        default=None,
        help="Category to scan. Can be repeated. Defaults to the known bad year categories.",
    )
    parser.add_argument("--limit", type=int, default=None, help="Maximum unique titles to process.")
    parser.add_argument("--skip", type=int, default=0, help="Unique titles to skip after collection.")
    parser.add_argument("--batch-size", type=int, default=50, help="Read/query batch size.")
    parser.add_argument("--interval", type=float, default=3.0, help="Pywikibot put throttle interval.")
    parser.add_argument("--maxlag", type=int, default=5, help="MediaWiki maxlag value.")
    parser.add_argument("--save", action="store_true", help="Save changes. Default is dry-run.")
    parser.add_argument("--print-limit", type=int, default=50, help="Maximum individual fixes to print.")
    parser.add_argument(
        "--reset-throttle-file",
        action="store_true",
        help="Remove pywikibot's local throttle.ctrl before starting.",
    )
    parser.add_argument(
        "--log",
        type=Path,
        default=None,
        help="JSONL decision log path.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    lock_fd = acquire_lock(LOCK_PATH)
    last_save_time = 0.0
    logging.getLogger("pywikibot").setLevel(logging.WARNING)
    configure_throttle(interval=args.interval, maxlag=args.maxlag)
    if args.reset_throttle_file:
        try:
            THROTTLE_PATH.unlink()
        except FileNotFoundError:
            pass

    try:
        categories = [normalize_category_title(value) for value in (args.category or default_categories())]
        log_path = args.log or (
            PROJECT_ROOT
            / "working"
            / f"bad_year_header_cleanup_{datetime.now().strftime('%Y%m%d_%H%M%S')}.jsonl"
        )

        print("=" * 72)
        print("Bad Header Year Cleanup")
        print("=" * 72)
        print(f"Mode:       {'save' if args.save else 'dry-run'}")
        print(f"Categories: {len(categories)}")
        print(f"Interval:   {args.interval}s")
        print(f"Log:        {log_path}")
        print("=" * 72)

        collection_limit = args.skip + args.limit if args.limit is not None else None
        title_category = collect_titles(
            categories,
            batch_size=args.batch_size,
            maxlag=args.maxlag,
            limit=collection_limit,
        )
        titles = list(title_category)
        if args.skip:
            titles = titles[args.skip :]
        if args.limit is not None:
            titles = titles[: args.limit]

        print(f"Unique titles to process: {len(titles)}")
        site = get_site() if args.save else None
        if site is not None:
            site.throttle.set_delays(delay=0, writedelay=float(args.interval))
            site.throttle.process_multiplicity = 1

        counts = {"processed": 0, "fix": 0, "saved": 0, "skip": 0, "missing": 0, "failed": 0}
        log_path.parent.mkdir(parents=True, exist_ok=True)

        with log_path.open("w", encoding="utf-8") as log_file:
            for index in range(0, len(titles), args.batch_size):
                batch = titles[index : index + args.batch_size]
                snapshots = fetch_page_text_batch(batch, batch_size=args.batch_size, maxlag=args.maxlag)

                for title in batch:
                    counts["processed"] += 1
                    snapshot = snapshots.get(title)
                    category = title_category.get(title, "")
                    if not snapshot or not snapshot.exists:
                        counts["missing"] += 1
                        decision = Decision(title=title, category=category, action="missing")
                        log_file.write(json.dumps(asdict(decision), ensure_ascii=False) + "\n")
                        continue

                    fixed_text, decision = build_fixed_text(title, category, snapshot.content)
                    log_file.write(json.dumps(asdict(decision), ensure_ascii=False) + "\n")

                    if decision.action != "fix":
                        counts["skip"] += 1
                        continue

                    counts["fix"] += 1
                    if counts["fix"] <= args.print_limit or counts["fix"] % 100 == 0:
                        print(
                            f"[{counts['processed']}/{len(titles)}] {title}: "
                            f"{decision.current_year} -> {decision.inferred_year}"
                        )

                    if not args.save:
                        continue

                    elapsed = time.monotonic() - last_save_time
                    if last_save_time and elapsed < args.interval:
                        time.sleep(args.interval - elapsed)

                    try:
                        save_text(site, title, fixed_text, snapshot.timestamp, args.maxlag)
                        counts["saved"] += 1
                        last_save_time = time.monotonic()
                    except Exception as exc:
                        counts["failed"] += 1
                        decision.action = "failed"
                        decision.reason = str(exc)
                        log_file.write(json.dumps(asdict(decision), ensure_ascii=False) + "\n")
                        print(f"Save failed for {title}: {exc}")

        print("=" * 72)
        for key, value in counts.items():
            print(f"{key}: {value}")
        print("=" * 72)
        return 1 if counts["failed"] else 0
    finally:
        release_lock(LOCK_PATH, lock_fd)


if __name__ == "__main__":
    raise SystemExit(main())
