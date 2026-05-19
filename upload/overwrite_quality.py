"""Normalization and quality rules for overwrite decisions."""

import re
from typing import Optional

from convert.html_normalizer import REDACTION_SEQUENCE_PATTERN, normalize_redaction_markers

from .page_metadata import normalize_wikitext_for_comparison

OS_REDACTION_RE = re.compile(r"\{\{PRC-redact\|\d+\|os=yes\}\}")
PRC_REDACT_TEMPLATE_RE = re.compile(r"\{\{PRC-redact\|(\d+)\}\}")
ADJACENT_PRC_REDACT_TEMPLATE_RE = re.compile(r"(?:\{\{PRC-redact\|\d+\}\}){2,}")
SPLIT_REDACTION_AFTER_TEMPLATE_RE = re.compile(r"\{\{PRC-redact\|\d+\}\}[×XxＸｘ*＊∗✱﹡⁎٭※]+")
SPLIT_REDACTION_BEFORE_TEMPLATE_RE = re.compile(r"[×XxＸｘ*＊∗✱﹡⁎٭※]+\{\{PRC-redact\|\d+\}\}")
MALFORMED_PRC_REDACT_RE = re.compile(r"\{\{PRC-redact(?!\|\d+(?:\|os=yes)?\}\})")
EMPTY_SIGNATURE_TEMPLATE_RE = re.compile(r"\{\{裁判文书署名\|1=\s*\}\}", re.DOTALL)
DOTTED_SIGNATURE_LINE_RE = re.compile(
    r"(?m)^\s*\{\{gap\}\}\.+(?:审判|人民陪审员|书记员|法官助理|二[〇零])"
)
STRAY_DOTTED_CASE_NUMBER_LINE_RE = re.compile(r"(?m)^\s*\.+（[^）\n]+）[^\n]*号\s*$")
MERGED_SIGNATURE_ROLE_RE = re.compile(
    r"(?:法官助理|审判长|审判员|人民陪审员|书记员)[:：]?.*(?:书记员|人民陪审员|法官助理)[:：]?"
)
CONTENT_LINK_RE = re.compile(r"\[\[(?!(?:Category|分类|分類)\s*:)[^\]]+\]\]", re.IGNORECASE)
HEADER_TEMPLATE_START_RE = re.compile(r"^\s*\{\{\s*header/裁判文书\b", re.IGNORECASE)
HEADER_PARAM_LINE_RE = re.compile(r"^(\s*\|\s*)([^=|\n]+?)(\s*=\s*)(.*?)(\s*)$")


def contains_os_redaction(content: str) -> bool:
    """Return whether the existing page has an os=yes redaction template."""
    return bool(OS_REDACTION_RE.search(content or ""))


def normalize_existing_header_safe_fixes(content: str) -> str:
    """Apply safe existing-page cleanups before overwrite classification."""
    text = normalize_wikitext_for_comparison(content)
    lines = text.split("\n")
    span = _find_header_template_span(lines)
    if not span:
        return text

    start, end = span
    changed = False
    for index in range(start, end + 1):
        match = HEADER_PARAM_LINE_RE.match(lines[index])
        if not match:
            continue

        key = match.group(2).strip()
        value = match.group(4)
        new_value = value
        if key == "案号":
            new_value = _normalize_case_number_value_brackets(value)
        elif key == "title":
            new_value = _redaction_templates_to_crosses(value)

        if new_value != value:
            lines[index] = f"{match.group(1)}{match.group(2)}{match.group(3)}{new_value}{match.group(5)}"
            changed = True

    return "\n".join(lines) if changed else text


def body_redaction_penalty(content: str) -> int:
    """Score malformed or split redaction markers in the body."""
    text = _without_header_title_line(content)
    return (
        len(REDACTION_SEQUENCE_PATTERN.findall(text))
        + 3 * len(SPLIT_REDACTION_AFTER_TEMPLATE_RE.findall(text))
        + 3 * len(SPLIT_REDACTION_BEFORE_TEMPLATE_RE.findall(text))
        + 10 * len(MALFORMED_PRC_REDACT_RE.findall(text))
    )


def structural_regression_penalty(content: str) -> int:
    """Score known structural artifacts from lower-quality imports."""
    text = normalize_wikitext_for_comparison(content)
    penalty = (
        12 * len(EMPTY_SIGNATURE_TEMPLATE_RE.findall(text))
        + 10 * len(DOTTED_SIGNATURE_LINE_RE.findall(text))
        + 10 * len(STRAY_DOTTED_CASE_NUMBER_LINE_RE.findall(text))
    )
    for line in text.splitlines():
        if MERGED_SIGNATURE_ROLE_RE.search(line):
            penalty += 8
    return penalty


def content_link_count(content: str) -> int:
    """Count content links, excluding categories and maintenance tags."""
    return len(CONTENT_LINK_RE.findall(normalize_wikitext_for_comparison(content)))


def is_safe_redaction_marker_update(import_wikitext: str, existing_content: str) -> bool:
    """Return whether the import only improves redaction marker formatting."""
    existing_text = normalize_wikitext_for_comparison(existing_content)
    import_text = normalize_wikitext_for_comparison(import_wikitext)
    if existing_text == import_text:
        return False

    if canonicalize_redaction_markers(existing_text) != canonicalize_redaction_markers(import_text):
        return False

    return body_redaction_penalty(import_text) < body_redaction_penalty(existing_text)


def is_safe_header_only_update(import_wikitext: str, existing_content: str) -> bool:
    """Return whether the import only fills header params or normalizes safe values."""
    existing_text = normalize_wikitext_for_comparison(existing_content)
    import_text = normalize_wikitext_for_comparison(import_wikitext)
    if existing_text == import_text:
        return False

    existing_lines = existing_text.split("\n")
    import_lines = import_text.split("\n")
    existing_span = _find_header_template_span(existing_lines)
    import_span = _find_header_template_span(import_lines)
    if not existing_span or not import_span:
        return False

    existing_start, existing_end = existing_span
    import_start, import_end = import_span
    if existing_lines[:existing_start] != import_lines[:import_start]:
        return False
    if existing_lines[existing_end + 1 :] != import_lines[import_end + 1 :]:
        return False

    existing_skeleton, existing_values = _split_header_param_lines(existing_lines[existing_start : existing_end + 1])
    import_skeleton, import_values = _split_header_param_lines(import_lines[import_start : import_end + 1])
    if existing_skeleton != import_skeleton or len(existing_values) != len(import_values):
        return False

    changed = False
    for (existing_key, existing_value), (import_key, import_value) in zip(existing_values, import_values):
        if existing_key != import_key:
            return False
        if existing_value != import_value:
            changed = True
        if not _is_safe_header_param_update(existing_key, existing_value, import_value):
            return False

    return changed


def _find_header_template_span(lines: list[str]) -> Optional[tuple[int, int]]:
    """Return the start/end line indexes for a simple multiline court header."""
    for start_index, line in enumerate(lines):
        if not HEADER_TEMPLATE_START_RE.match(line):
            continue
        for end_index in range(start_index + 1, len(lines)):
            if lines[end_index].strip() == "}}":
                return start_index, end_index
        return None
    return None


def _split_header_param_lines(lines: list[str]) -> Optional[tuple[list[tuple], list[tuple[str, str]]]]:
    """Return a header skeleton and ordered parameter values."""
    skeleton: list[tuple] = []
    values: list[tuple[str, str]] = []

    for line in lines:
        match = HEADER_PARAM_LINE_RE.match(line)
        if not match:
            skeleton.append(("text", line))
            continue

        key = match.group(2).strip()
        skeleton.append(("param", match.group(1), key, match.group(3), match.group(5)))
        values.append((key, match.group(4)))

    return skeleton, values


def _normalize_case_number_brackets(value: str) -> str:
    return re.sub(r"\s+", "", value or "").replace("(", "（").replace(")", "）")


def _normalize_case_number_value_brackets(value: str) -> str:
    return (value or "").replace("(", "（").replace(")", "）")


def _redaction_templates_to_crosses(value: str) -> str:
    return PRC_REDACT_TEMPLATE_RE.sub(lambda match: "×" * int(match.group(1)), value or "")


def _without_header_title_line(content: str) -> str:
    lines = normalize_wikitext_for_comparison(content).split("\n")
    span = _find_header_template_span(lines)
    if not span:
        return "\n".join(lines)

    start, end = span
    filtered: list[str] = []
    for index, line in enumerate(lines):
        if start <= index <= end:
            match = HEADER_PARAM_LINE_RE.match(line)
            if match and match.group(2).strip() == "title":
                continue
        filtered.append(line)
    return "\n".join(filtered)


def _collapse_adjacent_redaction_templates(text: str) -> str:
    def replacer(match: re.Match) -> str:
        count = sum(int(value) for value in re.findall(r"\{\{PRC-redact\|(\d+)\}\}", match.group(0)))
        return f"{{{{PRC-redact|{count}}}}}"

    previous = None
    while previous != text:
        previous = text
        text = ADJACENT_PRC_REDACT_TEMPLATE_RE.sub(replacer, text)
    return text


def _collapse_split_redaction_runs(text: str) -> str:
    def after_replacer(match: re.Match) -> str:
        template_count = int(re.search(r"\{\{PRC-redact\|(\d+)\}\}", match.group(0)).group(1))
        raw_text = re.sub(r"\{\{PRC-redact\|\d+\}\}", "", match.group(0))
        raw_count = len(raw_text)
        return f"{{{{PRC-redact|{template_count + raw_count}}}}}"

    def before_replacer(match: re.Match) -> str:
        template_count = int(re.search(r"\{\{PRC-redact\|(\d+)\}\}", match.group(0)).group(1))
        raw_text = re.sub(r"\{\{PRC-redact\|\d+\}\}", "", match.group(0))
        return f"{{{{PRC-redact|{len(raw_text) + template_count}}}}}"

    previous = None
    while previous != text:
        previous = text
        text = SPLIT_REDACTION_AFTER_TEMPLATE_RE.sub(after_replacer, text)
        text = SPLIT_REDACTION_BEFORE_TEMPLATE_RE.sub(before_replacer, text)
        text = _collapse_adjacent_redaction_templates(text)
    return text


def canonicalize_redaction_markers(content: str) -> str:
    """Normalize equivalent raw/template redaction marker runs for comparison."""
    text = normalize_wikitext_for_comparison(content)
    text = normalize_redaction_markers(text)
    text = _collapse_split_redaction_runs(text)
    return text


def _is_safe_header_param_update(key: str, existing_value: str, import_value: str) -> bool:
    if existing_value == import_value:
        return True

    if not existing_value.strip() and import_value.strip():
        return True

    if key == "案号":
        return _normalize_case_number_brackets(existing_value) == _normalize_case_number_brackets(import_value)

    if key == "title":
        return _redaction_templates_to_crosses(existing_value) == import_value

    return False
