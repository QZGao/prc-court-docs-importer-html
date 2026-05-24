"""Normalization and quality rules for overwrite decisions."""

import re
from typing import Optional

from convert.html_normalizer import (
    find_redaction_marker_runs,
    is_date_text,
    normalize_redaction_markers,
    strip_signature_leading_junk,
)
from convert.wikitext_renderer import parse_signature_entries

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
SPACED_CROSS_MULTIPLICATION_RE = re.compile(r"(?<=[0-9０-９）)元平方米%％])\s*×\s*(?=[0-9０-９（(])")
UNGAPPED_COLON_HEADING_RE = re.compile(r"(?m)^(?!\{\{gap\}\})[\u4e00-\u9fff][^\n]{1,40}：\s*$")
CJK_LINE_START_RE = re.compile(r"^[\u4e00-\u9fff]")
PARTY_LABEL_RE = re.compile(
    r"^(?:"
    r"原告|被告|被告人|上诉人|被上诉人|申请人|被申请人|再审申请人|被执行人|申请执行人|"
    r"第三人|公诉机关|抗诉机关|法定代表人|法定代理人|负责人|经营者|"
    r"委托诉讼代理人|诉讼代理人|委托代理人|代理人|辩护人|指定辩护人|"
    r"(?:[一二三四五六七八九十两0-9０-９]+)?(?:原告|被告|上诉人|被上诉人|申请人|被申请人|第三人)?"
    r"(?:共同)?(?:委托诉讼代理人|诉讼代理人|委托代理人|代理人|辩护人)"
    r")[：:]"
)


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
        len(find_redaction_marker_runs(text))
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


def formatting_regression_penalty(content: str) -> int:
    """Score formatting artifacts that make otherwise equivalent pages worse."""
    text = normalize_wikitext_for_comparison(content)
    penalty = (
        4 * len(UNGAPPED_COLON_HEADING_RE.findall(text))
    )

    lines = text.splitlines()
    for index, line in enumerate(lines):
        stripped = line.strip()
        continuation = _formatting_continuation_text(stripped)
        if not continuation:
            continue
        if not stripped.startswith("{{gap}}") and (index < 1 or lines[index - 1].strip()):
            continue

        previous = _previous_nonempty_line(lines, index)
        if not previous:
            continue

        previous_stripped = previous.strip()
        if _is_gap_continuation_artifact(previous_stripped, continuation):
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


def is_safe_formatting_improvement(import_wikitext: str, existing_content: str) -> bool:
    """Return whether the import only fixes known formatting artifacts."""
    existing_text = normalize_wikitext_for_comparison(existing_content)
    import_text = normalize_wikitext_for_comparison(import_wikitext)
    if existing_text == import_text:
        return False

    if formatting_regression_penalty(import_text) >= formatting_regression_penalty(existing_text):
        return False
    if body_redaction_penalty(import_text) > body_redaction_penalty(existing_text):
        return False
    if structural_regression_penalty(import_text) > structural_regression_penalty(existing_text):
        return False
    if content_link_count(import_text) < content_link_count(existing_text):
        return False

    existing_canon = _canonicalize_formatting_artifacts(canonicalize_redaction_markers(existing_text))
    import_canon = _canonicalize_formatting_artifacts(canonicalize_redaction_markers(import_text))
    return existing_canon == import_canon


def is_safe_signature_structure_improvement(import_wikitext: str, existing_content: str) -> bool:
    """Return whether the import only moves signature artifacts into the signature template."""
    existing_text = normalize_wikitext_for_comparison(existing_content)
    import_text = normalize_wikitext_for_comparison(import_wikitext)
    if existing_text == import_text:
        return False

    if structural_regression_penalty(import_text) >= structural_regression_penalty(existing_text):
        return False
    if body_redaction_penalty(import_text) > body_redaction_penalty(existing_text):
        return False
    if formatting_regression_penalty(import_text) > formatting_regression_penalty(existing_text):
        return False
    if content_link_count(import_text) < content_link_count(existing_text):
        return False

    existing_canon = _canonicalize_signature_structure(
        _canonicalize_formatting_artifacts(canonicalize_redaction_markers(existing_text))
    )
    import_canon = _canonicalize_signature_structure(
        _canonicalize_formatting_artifacts(canonicalize_redaction_markers(import_text))
    )
    return existing_canon == import_canon


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


def _previous_nonempty_line(lines: list[str], index: int) -> Optional[str]:
    for previous_index in range(index - 1, -1, -1):
        if lines[previous_index].strip():
            return lines[previous_index]
    return None


def _is_gap_continuation_artifact(previous_text: str, continuation: str) -> bool:
    return (
        bool(continuation)
        and not _ends_sentence_like(previous_text)
        and not _starts_numbered_or_quoted_block(continuation)
        and not PARTY_LABEL_RE.match(continuation)
    )


def _canonicalize_formatting_artifacts(content: str) -> str:
    text = normalize_wikitext_for_comparison(content)
    text = SPACED_CROSS_MULTIPLICATION_RE.sub("×", text)

    lines = []
    for line in text.split("\n"):
        stripped = line.strip()
        if UNGAPPED_COLON_HEADING_RE.match(line):
            lines.append(f"{{{{gap}}}}{stripped}")
        else:
            lines.append(line)

    merged: list[str] = []
    index = 0
    while index < len(lines):
        if merged and not lines[index].strip() and index + 1 < len(lines):
            previous = merged[-1].strip()
            continuation = _formatting_continuation_text(lines[index + 1].strip())
            if _is_gap_continuation_artifact(previous, continuation):
                merged[-1] = f"{merged[-1]}{continuation}"
                index += 2
                continue

        merged.append(lines[index])
        index += 1

    return "\n".join(merged)


def _formatting_continuation_text(stripped_line: str) -> str:
    if stripped_line.startswith("{{gap}}"):
        return stripped_line.removeprefix("{{gap}}").strip()
    if CJK_LINE_START_RE.match(stripped_line):
        return stripped_line
    return ""


def _canonicalize_signature_structure(content: str) -> str:
    lines = normalize_wikitext_for_comparison(content).split("\n")
    result: list[str] = []
    index = 0

    while index < len(lines):
        if lines[index].strip() != "{{裁判文书署名|1=":
            result.append(lines[index])
            index += 1
            continue

        moved_entries = _pop_trailing_signature_artifacts(result)
        template_entries: list[str] = []
        index += 1
        while index < len(lines) and lines[index].strip() != "}}":
            template_entries.extend(_canonical_signature_entries(lines[index]))
            index += 1

        result.append("{{裁判文书署名|1=")
        result.extend(moved_entries)
        result.extend(template_entries)
        result.append("}}")
        if index < len(lines) and lines[index].strip() == "}}":
            index += 1

    return "\n".join(result)


def _pop_trailing_signature_artifacts(lines: list[str]) -> list[str]:
    entries_reversed: list[str] = []
    trailing_blank_count = 0

    while lines:
        if not lines[-1].strip():
            lines.pop()
            trailing_blank_count += 1
            continue

        entries = _canonical_signature_entries(lines[-1])
        if not entries:
            for _ in range(trailing_blank_count):
                lines.append("")
            break

        lines.pop()
        entries_reversed.extend(reversed(entries))
        trailing_blank_count = 0

    entries_reversed.reverse()
    return entries_reversed


def _canonical_signature_entries(line: str) -> list[str]:
    stripped = line.strip()
    if stripped.startswith("{{gap}}"):
        stripped = stripped.removeprefix("{{gap}}").strip()
    stripped = strip_signature_leading_junk(stripped)
    if not stripped:
        return []
    if is_date_text(stripped):
        return [stripped]

    entries = parse_signature_entries(stripped)
    return [f"{job}：{name}" for job, name in entries]


def _ends_sentence_like(text: str) -> bool:
    return bool(re.search(r"[。！？；：:，,、）)】］》”\"'0-9０-９]$", text))


def _starts_numbered_or_quoted_block(text: str) -> bool:
    return bool(re.match(r"(?:[一二三四五六七八九十百千万]+、|[0-9０-９]+[.．、]|[《（(])", text))


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
