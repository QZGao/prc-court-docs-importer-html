#!/usr/bin/env python
"""
Rescue overwritten court-document imports into case-number pages.

For each canonical page in PAGE_TITLES:
1. Normalize the current page by moving the stray court/type/案号 lines into
   {{Header/裁判文书}}, and remove [[Category:覆盖版本未检查的裁判文书]].
2. Scan non-create revisions with the import summary. Revisions with a different
   docid from the normalized current page are rescued into new case-number pages.
3. Move the canonical page to its own case-number page, link its header title,
   and replace the canonical title with a {{裁判文书消歧义页}} grouped by court.
"""

from __future__ import annotations

import argparse
import re
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from dotenv import load_dotenv

load_dotenv(PROJECT_ROOT / ".env")

import pywikibot

from upload.mediawiki import can_move_over_redirect, configure_throttle, get_site


PAGE_TITLES = [
    "某财务有限公司、乌鲁木齐某汽车租赁有限公司等金融借款合同纠纷民事一审民事判决书",
    "某金融有限公司、孙某某金融借款合同纠纷民事一审民事判决书",
    "某金融有限公司、张某某金融借款合同纠纷民事一审民事判决书",
    "某金融有限公司、王某某金融借款合同纠纷民事一审民事判决书",
    "某汽车金融有限公司、刘某金融借款合同纠纷民事一审民事判决书",
    "某汽车金融有限公司、周某金融借款合同纠纷民事一审民事判决书",
    "某汽车金融有限公司、张某琳等金融借款合同纠纷民事一审民事判决书",
    "某汽车金融有限公司、李某金融借款合同纠纷民事一审民事判决书",
    "某汽车金融有限公司、杨某金融借款合同纠纷民事一审民事判决书",
    "某汽车金融有限公司、王某金融借款合同纠纷民事一审民事判决书",
    "某某金融有限公司、李某某金融借款合同纠纷民事一审民事判决书",
    "某某银行、某某信用卡纠纷民事一审民事判决书",
]

IMPORT_SUMMARY = "Imported from 裁判文书网 (credit: caseopen.org)"
GAP_MARKER = "{{gap}}文书内容"
UNCHECKED_CATEGORY = "[[Category:覆盖版本未检查的裁判文书]]"
HEADER_START_RE = re.compile(r"^\s*\{\{\s*Header/裁判文书(?=\s|[|\n}])", re.IGNORECASE)
PARAM_RE = re.compile(r"^([ \t]*\|\s*)([^=\n]+?)(\s*=\s*)(.*?)(\r?\n)?$")


@dataclass(frozen=True)
class Metadata:
    court: str
    doc_type: str
    case_number: str


@dataclass(frozen=True)
class HeaderData:
    title: str
    court: str
    doc_type: str
    case_number: str
    docid: str


@dataclass(frozen=True)
class ProcessedText:
    content: str
    metadata: Metadata
    header: HeaderData
    case_title: str
    changed: bool


@dataclass(frozen=True)
class CreatedPage:
    title: str
    source_revid: int | None
    new_revid: int | None


@dataclass(frozen=True)
class RunReport:
    canonical_title: str
    current_case_title: str
    created_pages: list[CreatedPage]
    disambig_entries: list[str]


@dataclass(frozen=True)
class Failure:
    title: str
    reason: str


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Rescue overwritten imports into case-number pages and make disambiguation pages."
    )
    parser.add_argument("--dry-run", action="store_true", help="Plan actions without saving.")
    parser.add_argument("--skip", type=int, default=0, help="Number of page titles to skip.")
    parser.add_argument("--max", type=int, default=None, help="Maximum number of page titles to process.")
    parser.add_argument("--interval", type=float, default=10.0, help="Minimum seconds between writes.")
    parser.add_argument("--maxlag", type=int, default=5, help="MediaWiki maxlag value.")
    return parser.parse_args(argv)


def select_titles(args: argparse.Namespace) -> list[str]:
    if args.skip < 0:
        raise ValueError("--skip must be 0 or greater")
    if args.max is not None and args.max <= 0:
        raise ValueError("--max must be greater than 0")

    titles = PAGE_TITLES[args.skip :]
    if args.max is not None:
        titles = titles[: args.max]
    return titles


def normalize_case_number(value: str) -> str:
    return value.strip().replace("(", "（").replace(")", "）")


def validate_metadata(metadata: Metadata) -> None:
    if not metadata.court.endswith("法院"):
        raise ValueError(f"court line does not end with 法院: {metadata.court}")
    if metadata.doc_type != "民事判决书":
        raise ValueError(f"type line is not 民事判决书: {metadata.doc_type}")
    if not re.search(r"\d", metadata.case_number):
        raise ValueError(f"案号 line does not contain numbers: {metadata.case_number}")


def find_header_span(lines: list[str]) -> tuple[int, int]:
    start = None
    for index, line in enumerate(lines):
        if HEADER_START_RE.match(line):
            start = index
            break

    if start is None:
        raise ValueError("could not find {{Header/裁判文书}}")

    for index in range(start + 1, len(lines)):
        if lines[index].strip() == "}}":
            return start, index

    raise ValueError("could not find closing }} for {{Header/裁判文书}}")


def infer_newline(lines: list[str]) -> str:
    for line in lines:
        if line.endswith("\r\n"):
            return "\r\n"
    return "\n"


def parse_header(text: str) -> HeaderData:
    lines = text.splitlines(keepends=True)
    header_start, header_end = find_header_span(lines)
    params: dict[str, str] = {}

    for line in lines[header_start + 1 : header_end]:
        match = PARAM_RE.match(line)
        if not match:
            continue
        key = match.group(2).strip()
        value = match.group(4).strip()
        params[key] = value

    return HeaderData(
        title=params.get("title", ""),
        court=params.get("court", ""),
        doc_type=params.get("type", ""),
        case_number=normalize_case_number(params.get("案号", "")),
        docid=params.get("docid", ""),
    )


def require_docid(header: HeaderData, context: str) -> str:
    if not header.docid:
        raise ValueError(f"{context}: header docid is empty")
    return header.docid


def extract_metadata_and_remove_intro(text: str) -> tuple[str, Metadata]:
    lines = text.splitlines(keepends=True)
    marker_index = None
    for index, line in enumerate(lines):
        if line.strip() == GAP_MARKER:
            marker_index = index
            break

    if marker_index is None:
        raise ValueError(f"could not find {GAP_MARKER}")

    metadata_indices: list[int] = []
    metadata_values: list[str] = []
    index = marker_index + 1
    while index < len(lines) and len(metadata_values) < 3:
        value = lines[index].strip()
        if value:
            metadata_indices.append(index)
            metadata_values.append(value)
        index += 1

    if len(metadata_values) != 3:
        raise ValueError(f"expected 3 non-empty metadata lines after {GAP_MARKER}, found {len(metadata_values)}")

    remove_end = metadata_indices[-1] + 1
    while remove_end < len(lines) and not lines[remove_end].strip():
        remove_end += 1

    metadata = Metadata(
        court=metadata_values[0],
        doc_type=metadata_values[1],
        case_number=normalize_case_number(metadata_values[2]),
    )
    validate_metadata(metadata)
    return "".join(lines[:marker_index] + lines[remove_end:]), metadata


def remove_unchecked_category(text: str) -> str:
    kept_lines = []
    for line in text.splitlines(keepends=True):
        if line.strip() == UNCHECKED_CATEGORY:
            continue
        kept_lines.append(line)
    return "".join(kept_lines)


def wrap_wikilink(value: str) -> str:
    leading = re.match(r"^\s*", value).group(0)
    trailing = re.search(r"\s*$", value).group(0)
    inner = value[len(leading) : len(value) - len(trailing)]
    if not inner:
        raise ValueError("header title param is empty")
    if inner.startswith("[[") and inner.endswith("]]"):
        return value
    return f"{leading}[[{inner}]]{trailing}"


def format_param_line(prefix: str, key: str, eq: str, value: str, newline: str) -> str:
    return f"{prefix}{key}{eq}{value}{newline}"


def update_header_params(text: str, metadata: Metadata, *, wrap_title: bool) -> str:
    lines = text.splitlines(keepends=True)
    header_start, header_end = find_header_span(lines)
    newline = infer_newline(lines)
    replacements = {
        "court": metadata.court,
        "type": metadata.doc_type,
        "案号": metadata.case_number,
    }
    seen: set[str] = set()
    title_seen = False

    for index in range(header_start + 1, header_end):
        match = PARAM_RE.match(lines[index])
        if not match:
            continue

        prefix, raw_key, eq, raw_value, line_newline = match.groups()
        key = raw_key.strip()
        actual_newline = line_newline or newline

        if key == "title":
            title_seen = True
            if wrap_title:
                lines[index] = format_param_line(prefix, raw_key, eq, wrap_wikilink(raw_value), actual_newline)
            continue

        if key in replacements:
            seen.add(key)
            lines[index] = format_param_line(prefix, raw_key, eq, replacements[key], actual_newline)

    if not title_seen:
        raise ValueError("could not find title param in {{Header/裁判文书}}")

    missing_lines = [
        f"|{key} = {value}{newline}"
        for key, value in replacements.items()
        if key not in seen
    ]
    if missing_lines:
        lines[header_end:header_end] = missing_lines

    return "".join(lines)


def validate_header_state(header: HeaderData, metadata: Metadata, context: str) -> None:
    header_values = [header.court.strip(), header.doc_type.strip(), header.case_number.strip()]
    all_empty = all(not value for value in header_values)
    all_filled = all(header_values)

    if all_empty:
        return
    if not all_filled:
        raise ValueError(f"{context}: header court/type/案号 are neither all empty nor all filled")

    header_metadata = Metadata(header.court, header.doc_type, header.case_number)
    validate_metadata(header_metadata)
    if header_metadata != metadata:
        raise ValueError(
            f"{context}: header metadata differs from stray lines: "
            f"header={header_metadata!r}, stray={metadata!r}"
        )


def metadata_from_filled_header(header: HeaderData, context: str) -> Metadata:
    header_values = [header.court.strip(), header.doc_type.strip(), header.case_number.strip()]
    if not all(header_values):
        raise ValueError(f"{context}: no stray metadata block and header court/type/案号 are not all filled")

    metadata = Metadata(header.court, header.doc_type, header.case_number)
    validate_metadata(metadata)
    return metadata


def process_wikitext(text: str, *, wrap_title: bool, context: str) -> ProcessedText:
    before = text
    header_before = parse_header(text)
    try:
        text, metadata = extract_metadata_and_remove_intro(text)
        validate_header_state(header_before, metadata, context)
    except ValueError as exc:
        if GAP_MARKER not in text:
            metadata = metadata_from_filled_header(header_before, context)
        else:
            raise exc

    text = remove_unchecked_category(text)
    text = update_header_params(text, metadata, wrap_title=wrap_title)
    header_after = parse_header(text)
    require_docid(header_after, context)
    case_title = f"{metadata.court}{metadata.case_number}{metadata.doc_type}"
    return ProcessedText(
        content=text,
        metadata=metadata,
        header=header_after,
        case_title=case_title,
        changed=(text != before),
    )


def wrap_current_title(text: str, metadata: Metadata, context: str) -> str:
    text = update_header_params(text, metadata, wrap_title=True)
    parse_header(text)
    return text


def fetch_page_text(site: pywikibot.Site, title: str) -> str:
    page = pywikibot.Page(site, title)
    if not page.exists():
        raise ValueError(f"page does not exist: [[{title}]]")
    return page.text


def page_exists(site: pywikibot.Site, title: str) -> bool:
    return pywikibot.Page(site, title).exists()


def save_page(site: pywikibot.Site, title: str, text: str, *, dry_run: bool) -> None:
    if dry_run:
        return
    page = pywikibot.Page(site, title)
    page.text = text
    page.save(summary="", minor=False, botflag=True)


def create_page(
    site: pywikibot.Site,
    title: str,
    text: str,
    *,
    dry_run: bool,
    maxlag: int,
) -> int | None:
    if page_exists(site, title):
        raise ValueError(f"destination page already exists: [[{title}]]")
    if dry_run:
        return None

    payload = site.simple_request(
        action="edit",
        format="json",
        formatversion="2",
        title=title,
        text=text,
        summary=IMPORT_SUMMARY,
        createonly="1",
        bot="1",
        token=site.tokens["csrf"],
        maxlag=maxlag,
    ).submit()
    if "error" in payload:
        raise RuntimeError(payload["error"])
    edit = payload.get("edit") or {}
    if edit.get("result") != "Success":
        raise RuntimeError(f"unexpected edit response while creating [[{title}]]: {payload}")
    new_revid = edit.get("newrevid")
    return int(new_revid) if new_revid is not None else None


def move_page(site: pywikibot.Site, from_title: str, to_title: str, *, dry_run: bool, maxlag: int) -> None:
    if page_exists(site, to_title):
        if not can_move_over_redirect(from_title, to_title):
            raise ValueError(f"move destination already exists: [[{to_title}]]")
    if dry_run:
        return

    request = site.simple_request(
        action="move",
        format="json",
        formatversion="2",
        to=to_title,
        reason="",
        token=site.tokens["csrf"],
        maxlag=maxlag,
    )
    request["from"] = from_title
    if can_move_over_redirect(from_title, to_title):
        request["ignorewarnings"] = "1"

    payload = request.submit()
    if "error" in payload:
        raise RuntimeError(payload["error"])
    if "move" not in payload:
        raise RuntimeError(f"unexpected move response: {payload}")


def wait_interval(last_write_time: float, interval: float, *, dry_run: bool) -> float:
    if dry_run:
        return last_write_time

    elapsed = time.monotonic() - last_write_time
    if last_write_time and elapsed < interval:
        time.sleep(interval - elapsed)
    return time.monotonic()


def iter_rescue_revisions(page: pywikibot.Page, current_docid: str) -> list[tuple[int, str]]:
    rescue_revisions: list[tuple[int, str]] = []
    for revision in page.revisions(content=True):
        if revision.parentid == 0:
            continue
        if revision.comment != IMPORT_SUMMARY:
            continue
        if not revision.text:
            raise ValueError(f"revision {revision.revid} has no text")

        revision_docid = require_docid(parse_header(revision.text), f"revision {revision.revid}")
        if revision_docid == current_docid:
            continue
        rescue_revisions.append((int(revision.revid), revision.text))

    return rescue_revisions


def court_from_case_title(title: str) -> str:
    index = title.find("（")
    if index <= 0:
        raise ValueError(f"case-number title does not contain full-width case number start: [[{title}]]")
    court = title[:index]
    if not court.endswith("法院"):
        raise ValueError(f"court inferred from case-number title does not end with 法院: [[{title}]]")
    return court


def build_disambig_page(canonical_title: str, case_titles: list[str]) -> str:
    grouped: dict[str, list[str]] = {}
    for title in case_titles:
        court = court_from_case_title(title)
        grouped.setdefault(court, []).append(title)

    lines = [
        "{{裁判文书消歧义页",
        f"|title={canonical_title}",
        "|type=民事判决书",
        "}}",
    ]
    for court, titles in grouped.items():
        lines.extend([
            f"=={court}==",
            f"[[Category:{court}]]",
        ])
        lines.extend(f"* [[{title}]]" for title in titles)
    return "\n".join(lines) + "\n"


def process_title(
    *,
    site: pywikibot.Site,
    canonical_title: str,
    dry_run: bool,
    interval: float,
    maxlag: int,
    last_write_time: float,
) -> tuple[RunReport, float]:
    page = pywikibot.Page(site, canonical_title)
    if not page.exists():
        raise ValueError(f"page does not exist: [[{canonical_title}]]")

    current_text = page.text
    current_processed = process_wikitext(current_text, wrap_title=False, context=f"current [[{canonical_title}]]")
    current_docid = require_docid(current_processed.header, f"current [[{canonical_title}]]")
    current_case_title = current_processed.case_title

    print(f"  current case title: [[{current_case_title}]]")
    print(f"  current docid: {current_docid}")

    if current_processed.changed:
        print("  STEP 1: normalize current page")
        last_write_time = wait_interval(last_write_time, interval, dry_run=dry_run)
        save_page(site, canonical_title, current_processed.content, dry_run=dry_run)
    else:
        print("  STEP 1: no current-page normalization needed")

    rescue_revisions = iter_rescue_revisions(page, current_docid)
    print(f"  rescue revisions: {len(rescue_revisions)}")

    created_pages: list[CreatedPage] = []
    page_list: list[str] = []

    for revid, revision_text in rescue_revisions:
        processed = process_wikitext(revision_text, wrap_title=True, context=f"revision {revid}")
        print(f"  STEP 3: rescue revision {revid} -> [[{processed.case_title}]]")
        last_write_time = wait_interval(last_write_time, interval, dry_run=dry_run)
        new_revid = create_page(site, processed.case_title, processed.content, dry_run=dry_run, maxlag=maxlag)
        created_pages.append(CreatedPage(processed.case_title, revid, new_revid))
        page_list.append(processed.case_title)

    if not page_list:
        raise ValueError("no rescue pages were created; refusing to replace canonical page with disambiguation")

    print(f"  STEP 4: move canonical page -> [[{current_case_title}]]")
    last_write_time = wait_interval(last_write_time, interval, dry_run=dry_run)
    move_page(site, canonical_title, current_case_title, dry_run=dry_run, maxlag=maxlag)

    page_list.append(current_case_title)
    created_pages.append(CreatedPage(current_case_title, None, None))

    moved_text = wrap_current_title(current_processed.content, current_processed.metadata, f"moved [[{current_case_title}]]")
    print("  STEP 4: link moved page title")
    last_write_time = wait_interval(last_write_time, interval, dry_run=dry_run)
    save_page(site, current_case_title, moved_text, dry_run=dry_run)

    disambig_text = build_disambig_page(canonical_title, page_list)
    print("  STEP 4: replace canonical redirect with disambiguation page")
    last_write_time = wait_interval(last_write_time, interval, dry_run=dry_run)
    save_page(site, canonical_title, disambig_text, dry_run=dry_run)

    return RunReport(
        canonical_title=canonical_title,
        current_case_title=current_case_title,
        created_pages=created_pages,
        disambig_entries=page_list,
    ), last_write_time


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    titles = select_titles(args)

    configure_throttle(interval=args.interval, maxlag=args.maxlag)
    site = get_site()

    print("=" * 72)
    print("Rescue Overwritten Court-Document Imports")
    print("=" * 72)
    print(f"Pages:          {len(titles)}")
    print(f"Dry run:        {args.dry_run}")
    print(f"Edit interval:  {args.interval}s")
    print(f"Maxlag:         {args.maxlag}")
    print(f"Create summary: {IMPORT_SUMMARY}")
    print("=" * 72)

    reports: list[RunReport] = []
    failures: list[Failure] = []
    last_write_time = 0.0

    for index, title in enumerate(titles, start=1):
        print(f"\n[{index}/{len(titles)}] [[{title}]]")
        try:
            report, last_write_time = process_title(
                site=site,
                canonical_title=title,
                dry_run=args.dry_run,
                interval=args.interval,
                maxlag=args.maxlag,
                last_write_time=last_write_time,
            )
            reports.append(report)
        except Exception as exc:
            failures.append(Failure(title, str(exc)))
            print("  !!! FAILED !!!")
            print(f"  {exc}")

    print("\n" + "=" * 72)
    print("RUN REPORT")
    print("=" * 72)
    for report in reports:
        print(f"\n{report.canonical_title}")
        print("case pages:")
        for title in report.disambig_entries:
            print(title)

    if failures:
        print("\n" + "!" * 72)
        print("!!! FAILURES - REVIEW MANUALLY !!!")
        print("!" * 72)
        for failure in failures:
            print(f"[[{failure.title}]]: {failure.reason}")
        return 1

    print("\nNo failures.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
