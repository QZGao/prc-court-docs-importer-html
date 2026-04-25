"""
Helpers for parsing upload-side page metadata.

These utilities understand the current court-document page format based on
{{Header/裁判文书}}, {{Versions}}, and standard MediaWiki redirects.
"""

from __future__ import annotations

import re
from typing import Optional


REDIRECT_CATEGORY = "中华人民共和国法院裁判文书案号重定向"
REQUIRED_CASE_TITLE_FIELDS = ("court", "案号", "type")
HEADER_TEMPLATE_PREFIXES = ("{{header/裁判文书",)
VERSIONS_TEMPLATE_PREFIXES = ("{{versions",)
REDIRECT_PATTERN = re.compile(r"^\s*#redirect\s*\[\[([^\]]+)\]\]", re.IGNORECASE | re.MULTILINE)


def normalize_case_number(case_number: str) -> str:
    """Normalize case-number parentheses to full-width Chinese ones."""
    if not case_number:
        return case_number

    return case_number.replace("(", "（").replace(")", "）")


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
    """Parse metadata from {{Versions}}."""
    return parse_template_metadata(page_text, VERSIONS_TEMPLATE_PREFIXES)


def is_header_page(page_text: str) -> bool:
    """Return whether page text looks like a court-document header page."""
    metadata = parse_header_metadata(page_text)
    return bool(
        metadata
        and all(metadata.get(field, "").strip() for field in REQUIRED_CASE_TITLE_FIELDS)
    )


def is_versions_page(page_text: str) -> bool:
    """Return whether page text starts with {{Versions}}."""
    if not page_text:
        return False

    stripped = page_text.lstrip().lower()
    return any(stripped.startswith(prefix) for prefix in VERSIONS_TEMPLATE_PREFIXES)


def build_case_title_from_metadata(metadata: dict[str, str]) -> Optional[str]:
    """Build the canonical case-number title from parsed header metadata."""
    parts: list[str] = []

    for field in REQUIRED_CASE_TITLE_FIELDS:
        value = metadata.get(field, "").strip()
        if not value:
            return None
        if field == "案号":
            value = normalize_case_number(value)
        parts.append(value)

    return "".join(parts)


def build_case_title_from_content(page_text: str) -> Optional[str]:
    """Build the canonical case-number title from header page text."""
    metadata = parse_header_metadata(page_text)
    if not metadata:
        return None
    return build_case_title_from_metadata(metadata)


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
    return f"按案号创建重定向至[[{source_title}]]"


def normalize_wikitext_for_comparison(page_text: Optional[str]) -> str:
    """Normalize insignificant line-ending and trailing-newline differences."""
    if not page_text:
        return ""

    return page_text.replace("\r\n", "\n").replace("\r", "\n").rstrip()


def wikitexts_match(left: Optional[str], right: Optional[str]) -> bool:
    """Return whether two page texts are equivalent for upload-side comparisons."""
    return normalize_wikitext_for_comparison(left) == normalize_wikitext_for_comparison(right)
