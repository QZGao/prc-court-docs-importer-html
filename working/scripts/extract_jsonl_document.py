#!/usr/bin/env python
r"""Extract one matching court document from a large JSONL file.

This is intentionally a small one-off utility for inspecting converted
court-document dumps or original unconverted JSONL dumps without loading the
whole file into memory.

Examples:
    python working/scripts/extract_jsonl_document.py "E:\BaiduNetdiskDownload\高速下载- 20241230\2024\202401.jsonl" --title "劳动合同纠纷" --court "人民法院" --case-number "民初.*号"
    python working/scripts/extract_jsonl_document.py converted.jsonl --docid "abcdef..." --output match.json
"""

from __future__ import annotations

import argparse
import json
import re
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable

from rich.console import Console
from rich.progress import (
    BarColumn,
    DownloadColumn,
    Progress,
    TextColumn,
    TimeElapsedColumn,
    TimeRemainingColumn,
    TransferSpeedColumn,
)


PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from upload.page_metadata import (  # noqa: E402
    normalize_case_number,
    normalize_doc_type,
    parse_header_metadata,
)


console = Console(stderr=True)

DICT_CONTAINERS = (
    "data",
    "document",
    "doc",
    "metadata",
    "page_metadata",
    "header",
    "raw_json",
)
WIKITEXT_ALIASES = ("wikitext", "wiki_text", "text", "content")

FIELD_ALIASES = {
    "title": ("title", "case_title", "final_title", "page_title", "s1"),
    "court": ("court", "court_name", "courtName", "noauthor", "s2"),
    "doc_type": ("doc_type", "doctype", "docType", "document_type", "type"),
    "case_number": ("doc_id", "case_number", "caseNumber", "case_no", "caseNo", "案号", "s7"),
    "docid": ("docid", "wenshu_id", "wenshuID", "wenshuId", "wsKey", "wskey", "docId", "docID"),
}

HEADER_ALIASES = {
    "title": ("title",),
    "court": ("court",),
    "doc_type": ("type",),
    "case_number": ("案号",),
    "docid": ("docid",),
}

@dataclass
class MatchResult:
    record: dict[str, Any]
    metadata: dict[str, list[str]]
    source_text: str
    line_number: int
    byte_offset: int


def unique_nonempty(values: Iterable[Any]) -> list[str]:
    seen: set[str] = set()
    result: list[str] = []

    for value in values:
        if value is None:
            continue
        if isinstance(value, (list, tuple, set)):
            nested_values = value
        else:
            nested_values = (value,)

        for nested in nested_values:
            if nested is None:
                continue
            text = str(nested).strip()
            if not text or text in seen:
                continue
            seen.add(text)
            result.append(text)

    return result


def iter_candidate_dicts(record: dict[str, Any]) -> Iterable[dict[str, Any]]:
    yield record

    for key in DICT_CONTAINERS:
        value = record.get(key)
        if isinstance(value, dict):
            yield value


def lookup_values(record: dict[str, Any], aliases: tuple[str, ...]) -> list[str]:
    values: list[Any] = []
    for data in iter_candidate_dicts(record):
        for alias in aliases:
            if alias in data:
                values.append(data[alias])
    return unique_nonempty(values)


def first_lookup(record: dict[str, Any], aliases: tuple[str, ...]) -> str:
    values = lookup_values(record, aliases)
    return values[0] if values else ""


def get_wikitext(record: dict[str, Any]) -> str:
    return first_lookup(record, WIKITEXT_ALIASES)


def parse_header_values(record: dict[str, Any]) -> dict[str, list[str]]:
    wikitext = get_wikitext(record)
    if not wikitext:
        return {}

    header = parse_header_metadata(wikitext) or {}
    values: dict[str, list[str]] = {}

    for field, aliases in HEADER_ALIASES.items():
        values[field] = unique_nonempty(header.get(alias) for alias in aliases)

    docids = [
        value
        for key, value in header.items()
        if key.strip().lower().startswith("docid")
    ]
    if docids:
        values["docid"] = unique_nonempty([*values.get("docid", []), *docids])

    return values


def extract_metadata(record: dict[str, Any]) -> dict[str, list[str]]:
    header_values = parse_header_values(record)
    metadata: dict[str, list[str]] = {}

    for field, aliases in FIELD_ALIASES.items():
        values = lookup_values(record, aliases)
        values.extend(header_values.get(field, []))
        metadata[field] = unique_nonempty(values)

    add_original_dump_metadata(record, metadata)

    return metadata


def add_original_dump_metadata(record: dict[str, Any], metadata: dict[str, list[str]]) -> None:
    """Fill fields that are implicit in original dump rows."""
    hierarchies = lookup_values(record, ("s22",))
    if not hierarchies:
        return

    if not metadata["case_number"]:
        metadata["case_number"] = unique_nonempty(
            normalize_case_number(hierarchy)
            for hierarchy in hierarchies
        )

    if metadata["doc_type"]:
        return

    inferred_types: list[str] = []
    for hierarchy in hierarchies:
        inferred_types.append(infer_doc_type_from_s22(hierarchy, metadata))

    metadata["doc_type"] = unique_nonempty(inferred_types)


def infer_doc_type_from_s22(s22: str, metadata: dict[str, list[str]]) -> str:
    court = metadata.get("court", [""])[0] if metadata.get("court") else ""
    case_number = metadata.get("case_number", [""])[0] if metadata.get("case_number") else ""

    for part in re.split(r"[\r\n]+", s22):
        candidate = part.strip()
        if not candidate:
            continue
        if court:
            candidate = candidate.replace(court, "")
        if case_number:
            candidate = candidate.replace(case_number, "")

        doc_type = normalize_doc_type(candidate)
        if doc_type:
            return doc_type

    compact = re.sub(r"\s+", "", s22)
    if court:
        compact = compact.replace(court, "")
    if case_number:
        compact = compact.replace(case_number, "")
    return normalize_doc_type(compact)


def compile_regex(label: str, pattern: str | None, flags: int) -> re.Pattern[str] | None:
    if not pattern:
        return None
    try:
        return re.compile(pattern, flags)
    except re.error as exc:
        raise SystemExit(f"Invalid regex for {label}: {exc}") from exc


def metadata_matches(
    metadata: dict[str, list[str]],
    regex_criteria: dict[str, re.Pattern[str]],
    docid: str | None,
) -> bool:
    for field, pattern in regex_criteria.items():
        if not any(pattern.search(value) for value in metadata.get(field, [])):
            return False

    if docid is not None and docid not in metadata.get("docid", []):
        return False

    return True


def decode_line(raw_line: bytes) -> str:
    try:
        return raw_line.decode("utf-8-sig")
    except UnicodeDecodeError:
        return raw_line.decode("utf-8", errors="replace")


def parse_json_line(line_text: str, line_number: int, *, strict: bool) -> dict[str, Any] | None:
    if not line_text.strip():
        return None

    try:
        parsed = json.loads(line_text)
    except json.JSONDecodeError as exc:
        if strict:
            raise SystemExit(f"Invalid JSON at line {line_number:,}: {exc}") from exc
        console.print(f"Skipping invalid JSON at line {line_number:,}: {exc}", style="yellow")
        return None

    if not isinstance(parsed, dict):
        if strict:
            raise SystemExit(f"JSON value at line {line_number:,} is not an object")
        return None

    return parsed


def scan_jsonl(
    jsonl_path: Path,
    regex_criteria: dict[str, re.Pattern[str]],
    docid: str | None,
    *,
    strict: bool,
    progress_bytes: int,
) -> MatchResult | None:
    total_size = jsonl_path.stat().st_size
    bytes_read = 0
    pending_progress = 0

    progress = Progress(
        TextColumn("[progress.description]{task.description}"),
        BarColumn(),
        DownloadColumn(binary_units=True),
        TransferSpeedColumn(),
        TimeElapsedColumn(),
        TimeRemainingColumn(),
        TextColumn("{task.fields[line_label]}"),
        console=console,
    )

    with progress, jsonl_path.open("rb") as source:
        task = progress.add_task(
            "Scanning",
            total=total_size,
            line_label="line 0",
        )

        line_number = 0
        for line_number, raw_line in enumerate(source, start=1):
            byte_offset = bytes_read
            line_size = len(raw_line)
            bytes_read += line_size
            pending_progress += line_size

            if pending_progress >= progress_bytes:
                progress.update(
                    task,
                    advance=pending_progress,
                    line_label=f"line {line_number:,}",
                )
                pending_progress = 0

            source_text = decode_line(raw_line)
            record = parse_json_line(source_text, line_number, strict=strict)
            if record is None:
                continue

            metadata = extract_metadata(record)
            if not metadata_matches(metadata, regex_criteria, docid):
                continue

            if pending_progress:
                progress.update(
                    task,
                    advance=pending_progress,
                    line_label=f"line {line_number:,}",
                )
            progress.update(task, description="Matched")
            return MatchResult(
                record=record,
                metadata=metadata,
                source_text=source_text,
                line_number=line_number,
                byte_offset=byte_offset,
            )

        if pending_progress:
            progress.update(task, advance=pending_progress, line_label=f"line {line_number:,}")

    return None


def dump_json(record: dict[str, Any], *, compact: bool) -> str:
    if compact:
        return json.dumps(record, ensure_ascii=False, separators=(",", ":"))
    return json.dumps(record, ensure_ascii=False, indent=2)


def write_outputs(result: MatchResult, args: argparse.Namespace) -> None:
    json_text = result.source_text.rstrip("\r\n") if args.raw_entry else dump_json(result.record, compact=args.compact)

    if args.output:
        output_path = Path(args.output)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text(json_text + "\n", encoding="utf-8", newline="\n")
        console.print(f"Wrote JSON to {output_path}")
    else:
        sys.stdout.write(json_text)
        sys.stdout.write("\n")

    if args.text_output:
        text = get_wikitext(result.record)
        if not text:
            console.print("Matched record has no wikitext to write.", style="yellow")
            return

        text_output_path = Path(args.text_output)
        text_output_path.parent.mkdir(parents=True, exist_ok=True)
        text_output_path.write_text(text, encoding="utf-8", newline="\n")
        console.print(f"Wrote text to {text_output_path}")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Stream a large court-document JSONL file and extract the first matching record.",
    )
    parser.add_argument("jsonl", help="Path to the converted JSONL or original dump JSONL file to scan.")
    parser.add_argument("--title", help="Regex searched against title/s1 and Header title.")
    parser.add_argument("--court", help="Regex searched against court/s2 and Header court.")
    parser.add_argument("--type", "--doc-type", dest="doc_type", help="Regex searched against doc_type, raw s22, and Header type.")
    parser.add_argument(
        "--case-number",
        "--case",
        "--案号",
        dest="case_number",
        help="Regex searched against doc_id/s7/raw s22 and Header 案号.",
    )
    parser.add_argument("--docid", "--wenshu-id", dest="docid", help="Exact docid/wenshu_id/wsKey match.")
    parser.add_argument("--ignore-case", action="store_true", help="Use case-insensitive regex matching.")
    parser.add_argument("--output", "-o", help="Write the matching JSON object to this file. Defaults to stdout.")
    parser.add_argument("--raw-entry", action="store_true", help="Write the matched source JSONL line exactly as read.")
    parser.add_argument(
        "--text-output",
        help="Also write the matching wikitext to this file when present.",
    )
    parser.add_argument("--compact", action="store_true", help="Write compact single-line JSON.")
    parser.add_argument("--strict", action="store_true", help="Stop on invalid JSON instead of skipping bad lines.")
    parser.add_argument(
        "--progress-bytes",
        type=int,
        default=4 * 1024 * 1024,
        help="Minimum bytes between progress updates. Default: 4 MiB.",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    if not any([args.title, args.court, args.doc_type, args.case_number, args.docid]):
        parser.error("provide at least one criterion: --title, --court, --type, --case-number, or --docid")

    jsonl_path = Path(args.jsonl)
    if not jsonl_path.is_file():
        parser.error(f"JSONL file does not exist: {jsonl_path}")

    if args.progress_bytes <= 0:
        parser.error("--progress-bytes must be positive")

    flags = re.IGNORECASE if args.ignore_case else 0
    regex_criteria = {
        key: pattern
        for key, pattern in {
            "title": compile_regex("title", args.title, flags),
            "court": compile_regex("court", args.court, flags),
            "doc_type": compile_regex("type", args.doc_type, flags),
            "case_number": compile_regex("case-number", args.case_number, flags),
        }.items()
        if pattern is not None
    }

    result = scan_jsonl(
        jsonl_path,
        regex_criteria,
        args.docid.strip() if args.docid is not None else None,
        strict=args.strict,
        progress_bytes=args.progress_bytes,
    )
    if result is None:
        console.print("No matching document found.", style="yellow")
        return 1

    summary = {
        key: values[0]
        for key, values in result.metadata.items()
        if values
    }
    console.print(f"Matched line {result.line_number:,} at byte {result.byte_offset:,}.")
    console.print_json(json.dumps(summary, ensure_ascii=False))
    write_outputs(result, args)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
