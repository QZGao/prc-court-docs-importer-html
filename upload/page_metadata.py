"""
Helpers for parsing upload-side page metadata.

These utilities understand the current court-document page format based on
{{Header/裁判文书}}, {{裁判文书消歧义页}}, legacy {{Versions}}, and standard
MediaWiki redirects.
"""

from __future__ import annotations

import re
from typing import Optional


REDIRECT_CATEGORY = "中华人民共和国法院裁判文书案号重定向"
REQUIRED_CASE_TITLE_FIELDS = ("court", "案号", "type")
HEADER_TEMPLATE_PREFIXES = ("{{header/裁判文书",)
COURT_DISAMBIG_TEMPLATE_PREFIXES = ("{{裁判文书消歧义页",)
LEGACY_VERSIONS_TEMPLATE_PREFIXES = ("{{versions",)
VERSIONS_TEMPLATE_PREFIXES = COURT_DISAMBIG_TEMPLATE_PREFIXES + LEGACY_VERSIONS_TEMPLATE_PREFIXES
REDIRECT_PATTERN = re.compile(r"^\s*#redirect\s*\[\[([^\]]+)\]\]", re.IGNORECASE | re.MULTILINE)
LEADING_JUNK_RE = re.compile(r"^[^\u4e00-\u9fff]+")
CASE_NUMBER_RE = re.compile(r"（.*?号(?:之[一二三四五六七八九十百千万〇零]+)?")
NON_CJK_RE = re.compile(r"[^\u4e00-\u9fff]+")
DOC_TYPE_RE = re.compile(r"[\u4e00-\u9fff]*(?:裁定书|判决书|决定书|通知书|调解书|裁决书|支付令)")


def _compact_metadata_text(text: str) -> str:
    if not text:
        return ""

    return re.sub(r"\s+", "", text)


def normalize_court_name(court: str) -> str:
    """Strip junk around a court name and require the value to end at 法院."""
    court = _compact_metadata_text(court)
    if not court:
        return ""

    court = LEADING_JUNK_RE.sub("", court)
    court_end = court.rfind("法院")
    if court_end == -1:
        return ""

    return court[:court_end + len("法院")]


def normalize_case_number(case_number: str) -> str:
    """Extract a case-number span beginning with （ and ending with 号."""
    if not case_number:
        return ""

    case_number = _compact_metadata_text(case_number)
    case_number = case_number.replace("(", "（").replace(")", "）")
    match = CASE_NUMBER_RE.search(case_number)
    if not match:
        return ""

    return match.group(0)


def normalize_doc_type(doc_type: str) -> str:
    """Strip document type metadata to CJK characters only."""
    doc_type = _compact_metadata_text(doc_type)
    if not doc_type:
        return ""

    doc_type = NON_CJK_RE.sub("", doc_type)
    match = DOC_TYPE_RE.search(doc_type)
    if match:
        return match.group(0)

    return doc_type


def _find_template_start(lines: list[str], prefixes: tuple[str, ...]) -> Optional[int]:
    lowered_prefixes = tuple(prefix.lower() for prefix in prefixes)

    for index, line in enumerate(lines):
        stripped = line.strip().lower()
        if any(stripped.startswith(prefix) for prefix in lowered_prefixes):
            return index

    return None


def parse_template_metadata(
    page_text: str,
    prefixes: tuple[str, ...],
) -> Optional[dict[str, str]]:
    """Parse simple one-level template parameters from page text."""
    if not page_text:
        return None

    lines = page_text.splitlines()
    start_index = _find_template_start(lines, prefixes)
    if start_index is None:
        return None

    metadata: dict[str, str] = {}
    for line in lines[start_index + 1 :]:
        stripped = line.strip()
        if stripped == "}}":
            return metadata
        if not stripped.startswith("|"):
            continue

        key, separator, value = stripped[1:].partition("=")
        if not separator:
            continue
        metadata[key.strip()] = value.strip()

    return None


def parse_header_metadata(page_text: str) -> Optional[dict[str, str]]:
    """Parse metadata from {{Header/裁判文书}}."""
    return parse_template_metadata(page_text, HEADER_TEMPLATE_PREFIXES)


def parse_versions_metadata(page_text: str) -> Optional[dict[str, str]]:
    """Parse metadata from {{裁判文书消歧义页}} or legacy {{Versions}}."""
    return parse_template_metadata(page_text, VERSIONS_TEMPLATE_PREFIXES)


def is_header_page(page_text: str) -> bool:
    """Return whether page text looks like a court-document header page."""
    metadata = parse_header_metadata(page_text)
    return bool(metadata and build_case_title_from_metadata(metadata))


def is_versions_page(page_text: str) -> bool:
    """Return whether page text starts with a court-document disambiguation template."""
    if not page_text:
        return False

    stripped = page_text.lstrip().lower()
    return any(stripped.startswith(prefix) for prefix in VERSIONS_TEMPLATE_PREFIXES)


def is_legacy_versions_page(page_text: str) -> bool:
    """Return whether page text starts with legacy {{versions}}."""
    if not page_text:
        return False

    stripped = page_text.lstrip().lower()
    return any(stripped.startswith(prefix) for prefix in LEGACY_VERSIONS_TEMPLATE_PREFIXES)


def build_case_title_from_metadata(metadata: dict[str, str]) -> Optional[str]:
    """Build the canonical case-number title from parsed header metadata."""
    parts = [
        normalize_court_name(metadata.get("court", "")),
        normalize_case_number(metadata.get("案号", "")),
        normalize_doc_type(metadata.get("type", "")),
    ]
    if not all(parts):
        return None

    return "".join(parts)


def build_case_number_from_metadata(metadata: dict[str, str]) -> Optional[str]:
    """Build the normalized case-number identity from parsed header metadata."""
    case_number = normalize_case_number(metadata.get("案号", ""))
    return case_number or None


def build_case_title_from_content(page_text: str) -> Optional[str]:
    """Build the canonical case-number title from header page text."""
    metadata = parse_header_metadata(page_text)
    if not metadata:
        return None
    return build_case_title_from_metadata(metadata)


def build_case_number_from_content(page_text: str) -> Optional[str]:
    """Build the normalized case-number identity from header page text."""
    metadata = parse_header_metadata(page_text)
    if not metadata:
        return None
    return build_case_number_from_metadata(metadata)


def extract_redirect_target(page_text: str) -> Optional[str]:
    """Extract the target from a standard MediaWiki redirect page."""
    if not page_text:
        return None

    match = REDIRECT_PATTERN.search(page_text)
    if not match:
        return None

    return match.group(1).strip()


def build_case_redirect_text(source_title: str) -> str:
    """Build the case-number redirect page body."""
    return (
        f"#REDIRECT [[{source_title}]]\n\n"
        f"[[Category:{REDIRECT_CATEGORY}]]\n"
    )


def build_case_redirect_summary(source_title: str) -> str:
    """Build the edit summary for case-number redirects."""
    return f"按案号创建重定向"


def normalize_wikitext_for_comparison(page_text: Optional[str]) -> str:
    """Normalize insignificant line-ending and trailing-newline differences."""
    if not page_text:
        return ""

    return page_text.replace("\r\n", "\n").replace("\r", "\n").rstrip()


def wikitexts_match(left: Optional[str], right: Optional[str]) -> bool:
    """Return whether two page texts are equivalent for upload-side comparisons."""
    return normalize_wikitext_for_comparison(left) == normalize_wikitext_for_comparison(right)
