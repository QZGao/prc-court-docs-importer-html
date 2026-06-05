"""
Conflict resolution for duplicate page titles.

Handles two scenarios:
1. Existing page is a {{裁判文书消歧义页}} or legacy {{versions}} page - add new entry to it
2. Existing page is a {{Header/裁判文书}} page from same court - create a
   disambiguation page and move or relocate the existing document to its case title
"""

import re
from typing import Optional, Tuple, Callable, List

from .mediawiki import (
    can_move_over_redirect,
    get_page_content,
    move_page,
    resolve_page,
    save_page,
)
from .page_metadata import (
    build_case_number_from_metadata,
    build_case_title_from_content,
    build_case_title_from_metadata,
    is_header_page,
    is_legacy_versions_page,
    is_versions_page,
    normalize_case_number,
    normalize_court_name,
    normalize_doc_type,
    parse_header_metadata,
    parse_versions_metadata,
    wikitexts_match,
)


# Type for log callback: (message, is_success) -> None
LogCallback = Callable[[str, bool], None]

CATEGORY_LINE_RE = re.compile(
    r"^\s*\[\[\s*(?:Category|分类|分類)\s*:\s*([^\]\|\n]+)"
    r"(?:\|[^\]\n]*)?\]\]\s*$",
    re.IGNORECASE,
)
YEAR_TYPE_CATEGORY_RE = re.compile(r"^(\d{4})年(?:中华人民共和国)?(.+)$")
ENTRY_LINE_RE = re.compile(r"\s*\*+\s*\[\[([^\]|]+)(?:\|[^\]]*)?\]\]")
ENTRY_LINE_WITH_DEPTH_RE = re.compile(r"\s*(\*+)\s*\[\[([^\]|]+)(?:\|[^\]]*)?\]\]")
TOP_LEVEL_ENTRY_LINE_RE = re.compile(r"\s*\*\s*\[\[([^\]|]+)(?:\|[^\]]*)?\]\]")
SECTION_HEADING_RE = re.compile(r"^\s*==\s*(.*?)\s*==\s*$")
CASE_NUMBER_SORT_RE = re.compile(r"^(.*?)(\d+)号(.*)$")


def extract_versions_noauthor(content: str) -> Optional[str]:
    """Extract the court/noauthor field from a disambiguation page."""
    metadata = parse_versions_metadata(content)
    if not metadata:
        return None
    value = normalize_court_name(metadata.get("court", "") or metadata.get("noauthor", ""))
    return value or None


def extract_header_metadata(content: str) -> Optional[dict[str, str]]:
    """Extract current-format header metadata from page content."""
    metadata = parse_header_metadata(content)
    if not metadata:
        return None
    return metadata


def extract_header_title(content: str) -> Optional[str]:
    """Extract the title field from {{Header/裁判文书}}."""
    metadata = extract_header_metadata(content)
    if not metadata:
        return None
    value = metadata.get("title", "").strip()
    return value or None


def extract_header_court(content: str) -> Optional[str]:
    """Extract the court field from {{Header/裁判文书}}."""
    metadata = extract_header_metadata(content)
    if not metadata:
        return None
    value = normalize_court_name(metadata.get("court", ""))
    return value or None


def extract_header_type_from_content(content: str) -> Optional[str]:
    """Extract the type field from {{Header/裁判文书}}."""
    metadata = extract_header_metadata(content)
    if not metadata:
        return None
    value = normalize_doc_type(metadata.get("type", ""))
    return value or None


def extract_header_year_from_content(content: str) -> Optional[str]:
    """Extract the year field from {{Header/裁判文书}}."""
    metadata = extract_header_metadata(content)
    if not metadata:
        return None
    value = metadata.get("year", "").strip()
    return value or None


def add_title_link_to_content(content: str, original_title: str) -> str:
    """Add [[ and ]] around the title field in {{header}} to make it a link."""

    def replacer(match):
        title_value = match.group(1).strip()
        if not title_value.startswith("[["):
            return f"|title = [[{title_value}]]"
        return match.group(0)

    return re.sub(r"\|\s*title\s*=\s*([^\n|]+)", replacer, content, count=1)


def build_versions_page_content(
    title: str,
    noauthor: str,
    entry_titles: List[str],
    year: str = "",
    header_type: str = "",
) -> str:
    """
    Build a {{裁判文书消歧义页}} page content.

    Args:
        title: The original page title
        noauthor: Court name
        entry_titles: List of page titles to link to
        year: Ignored; retained for compatibility with older callers
        header_type: Full type from header
    """
    sorted_titles = sort_versions_entries(entry_titles)
    entries = "\n".join(f"* [[{t}]]" for t in sorted_titles)

    content = f"""{{{{裁判文书消歧义页
 | title      = {title}
 | court      = {noauthor}
 | type       = {header_type}
}}}}
{entries}
"""
    return content


def case_number_sort_key(title: str) -> tuple[str, int, str]:
    """Sort by text before the final case number, numeric case number, then suffix."""
    match = CASE_NUMBER_SORT_RE.match(title)
    if not match:
        return title, -1, ""
    return match.group(1), int(match.group(2)), match.group(3)


def sort_versions_entries(entry_titles: list[str]) -> list[str]:
    """Deduplicate and sort disambiguation entries by case-number syntax."""
    return sorted(set(entry_titles), key=case_number_sort_key)


def infer_court_from_case_title(title: str) -> Optional[str]:
    """Infer court from a case-number page title formatted as court + （case number） + type."""
    court, separator, _ = title.partition("（")
    if not separator:
        return None
    return court.strip() or None


def infer_case_number_from_case_title(title: str) -> Optional[str]:
    """Infer normalized case number from a case-number page title."""
    case_number = normalize_case_number(title)
    return case_number or None


def find_entry_with_same_case_number(entry_titles: list[str], new_entry_title: str) -> Optional[str]:
    """Return the existing entry title with the same normalized case number, if any."""
    new_case_number = infer_case_number_from_case_title(new_entry_title)
    if not new_case_number:
        return None

    for entry_title in entry_titles:
        if infer_case_number_from_case_title(entry_title) == new_case_number:
            return entry_title

    return None


def has_entry_with_same_case_number(entry_titles: list[str], new_entry_title: str) -> bool:
    """Return whether any existing entry has the same normalized case number."""
    return find_entry_with_same_case_number(entry_titles, new_entry_title) is not None


def grouped_entries_have_case_number(
    grouped_entries: dict[str, list[str]],
    new_entry_title: str,
) -> bool:
    """Return whether any grouped entry has the same normalized case number."""
    return has_entry_with_same_case_number(
        [entry for entries in grouped_entries.values() for entry in entries],
        new_entry_title,
    )


def append_entry_if_new_case_number(entry_titles: list[str], new_entry_title: str) -> list[str]:
    """Append new_entry_title unless its normalized case number is already present."""
    if has_entry_with_same_case_number(entry_titles, new_entry_title):
        return entry_titles
    return entry_titles + [new_entry_title]


def build_grouped_versions_page_content(
    *,
    title: str,
    header_type: str,
    court_entries: dict[str, list[str]],
) -> str:
    """Build a cross-court {{裁判文书消歧义页}} page grouped by court."""
    lines = [
        "{{裁判文书消歧义页",
        f" | title      = {title}",
        f" | type       = {header_type}",
        "}}",
    ]

    for court in sorted(court_entries):
        entries = sort_versions_entries(court_entries[court])
        if not court or not entries:
            continue
        lines.extend(
            [
                f"=={court}==",
                f"[[Category:{court}]]",
                *[f"* [[{entry}]]" for entry in entries],
            ]
        )

    return "\n".join(lines) + "\n"


def extract_versions_entries(content: str) -> list[str]:
    """Extract linked entries from a disambiguation page."""
    entries: list[str] = []
    for line in content.splitlines():
        match = ENTRY_LINE_RE.match(line)
        if match:
            entries.append(match.group(1).strip())
    return entries


def has_nested_versions_entries(content: str) -> bool:
    """Return whether a disambiguation page has nested bullet entries."""
    for line in content.splitlines():
        match = ENTRY_LINE_WITH_DEPTH_RE.match(line)
        if match and len(match.group(1)) > 1:
            return True
    return False


def extract_grouped_versions_entries(content: str, default_court: Optional[str] = None) -> dict[str, list[str]]:
    """Extract disambiguation entries grouped by court heading or default court."""
    grouped_entries: dict[str, list[str]] = {}
    current_court = default_court or ""

    for line in content.splitlines():
        heading_match = SECTION_HEADING_RE.match(line)
        if heading_match:
            current_court = heading_match.group(1).strip()
            grouped_entries.setdefault(current_court, [])
            continue

        entry_match = ENTRY_LINE_RE.match(line)
        if entry_match:
            grouped_entries.setdefault(current_court, []).append(entry_match.group(1).strip())

    return grouped_entries


def _find_court_section_bounds(lines: list[str], court: str) -> Optional[tuple[int, int]]:
    """Return the body bounds for a court heading section."""
    for index, line in enumerate(lines):
        heading_match = SECTION_HEADING_RE.match(line)
        if not heading_match or heading_match.group(1).strip() != court:
            continue
        end = len(lines)
        for next_index in range(index + 1, len(lines)):
            if SECTION_HEADING_RE.match(lines[next_index]):
                end = next_index
                break
        return index + 1, end
    return None


def _find_template_body_start(lines: list[str]) -> int:
    for index, line in enumerate(lines):
        stripped = line.strip()
        if stripped == "}}" or stripped.endswith("}}"):
            return index + 1
    return 0


def _find_top_level_entry_insert_index(
    lines: list[str],
    start: int,
    end: int,
    new_entry_title: str,
) -> int:
    """Find an insertion point that keeps top-level entries sorted."""
    new_sort_key = case_number_sort_key(new_entry_title)
    last_entry_block_end: Optional[int] = None

    index = start
    while index < end:
        match = TOP_LEVEL_ENTRY_LINE_RE.match(lines[index])
        if not match:
            index += 1
            continue

        if new_sort_key < case_number_sort_key(match.group(1).strip()):
            return index

        block_end = index + 1
        while block_end < end:
            child_match = ENTRY_LINE_WITH_DEPTH_RE.match(lines[block_end])
            if not child_match or len(child_match.group(1)) <= 1:
                break
            block_end += 1
        last_entry_block_end = block_end
        index = block_end

    return last_entry_block_end if last_entry_block_end is not None else end


def _add_entry_preserving_versions_structure(
    content: str,
    new_entry_title: str,
    new_entry_court: Optional[str],
) -> str:
    """Add an entry without flattening existing nested disambiguation bullets."""
    existing_entries = extract_versions_entries(content)
    if has_entry_with_same_case_number(existing_entries, new_entry_title):
        return content

    lines = content.split("\n")
    insert_index: Optional[int] = None

    if new_entry_court:
        bounds = _find_court_section_bounds(lines, new_entry_court)
        if bounds:
            insert_index = _find_top_level_entry_insert_index(
                lines,
                bounds[0],
                bounds[1],
                new_entry_title,
            )
        elif any(SECTION_HEADING_RE.match(line) for line in lines):
            lines.append(f"=={new_entry_court}==")
            lines.append(f"[[Category:{new_entry_court}]]")
            lines.append(f"* [[{new_entry_title}]]")
            return "\n".join(lines)

    if insert_index is None:
        start = _find_template_body_start(lines)
        end = next(
            (
                index
                for index in range(start, len(lines))
                if lines[index].strip().startswith("[[Category:")
            ),
            len(lines),
        )
        insert_index = _find_top_level_entry_insert_index(lines, start, end, new_entry_title)

    lines.insert(insert_index, f"* [[{new_entry_title}]]")
    return "\n".join(lines)


def extract_category_titles(content: str) -> list[str]:
    """Extract category titles from page text."""
    titles: list[str] = []
    for line in content.splitlines():
        match = CATEGORY_LINE_RE.match(line)
        if match:
            titles.append(match.group(1).strip())
    return titles


def infer_year_type_from_categories(content: str) -> tuple[Optional[str], Optional[str]]:
    """Infer (year, type) from old versions-page category lines."""
    categories = set(extract_category_titles(content))
    for category_title in sorted(categories):
        match = YEAR_TYPE_CATEGORY_RE.match(category_title)
        if not match:
            continue
        year = match.group(1)
        header_type = match.group(2)
        if header_type in categories or f"中华人民共和国{header_type}" in categories:
            return year, header_type
    return None, None


def convert_legacy_versions_page_content(
    *,
    original_title: str,
    existing_content: str,
    fallback_court: str,
    fallback_year: str,
    fallback_type: str,
    entry_titles: list[str],
) -> Optional[str]:
    """Convert legacy {{versions}} content to {{裁判文书消歧义页}} content."""
    if not is_legacy_versions_page(existing_content):
        return None

    metadata = parse_versions_metadata(existing_content) or {}
    title = metadata.get("title", "").strip() or original_title
    court = (
        normalize_court_name(metadata.get("court", "") or metadata.get("noauthor", ""))
        or normalize_court_name(fallback_court)
    )
    inferred_year, inferred_type = infer_year_type_from_categories(existing_content)
    header_type = normalize_doc_type(inferred_type or fallback_type)

    if not court or not header_type:
        return None

    return build_versions_page_content(
        title=title,
        noauthor=court,
        entry_titles=entry_titles,
        header_type=header_type,
    )


def add_entry_to_versions_page(
    content: str,
    new_entry_title: str,
    new_entry_court: Optional[str] = None,
    header_type: Optional[str] = None,
) -> str:
    """Add a new entry to an existing disambiguation page and sort all entries."""
    metadata = parse_versions_metadata(content) or {}
    title = metadata.get("title", "").strip()
    existing_court = normalize_court_name(metadata.get("court", "") or metadata.get("noauthor", ""))
    page_type = normalize_doc_type(metadata.get("type", "") or header_type or "")
    has_nested_entries = has_nested_versions_entries(content)

    if title and page_type:
        if existing_court and new_entry_court and existing_court != new_entry_court:
            grouped_entries = extract_grouped_versions_entries(content, default_court=existing_court)
            if grouped_entries_have_case_number(grouped_entries, new_entry_title):
                return content
            if has_nested_entries:
                return _add_entry_preserving_versions_structure(content, new_entry_title, new_entry_court)
            grouped_entries.setdefault(new_entry_court, []).append(new_entry_title)
            return build_grouped_versions_page_content(
                title=title,
                header_type=page_type,
                court_entries=grouped_entries,
            )

        effective_new_entry_court = new_entry_court or infer_court_from_case_title(new_entry_title)
        if not existing_court and effective_new_entry_court:
            grouped_entries = extract_grouped_versions_entries(content)
            orphan_entries = grouped_entries.pop("", [])
            for entry in orphan_entries:
                inferred_court = infer_court_from_case_title(entry)
                if inferred_court:
                    grouped_entries.setdefault(inferred_court, []).append(entry)
            if grouped_entries_have_case_number(grouped_entries, new_entry_title):
                return content
            if has_nested_entries:
                return _add_entry_preserving_versions_structure(content, new_entry_title, effective_new_entry_court)
            grouped_entries.setdefault(effective_new_entry_court, []).append(new_entry_title)
            return build_grouped_versions_page_content(
                title=title,
                header_type=page_type,
                court_entries=grouped_entries,
            )

        if existing_court:
            if has_nested_entries:
                return _add_entry_preserving_versions_structure(content, new_entry_title, existing_court)
            all_entries = append_entry_if_new_case_number(extract_versions_entries(content), new_entry_title)
            return build_versions_page_content(
                title=title,
                noauthor=existing_court,
                entry_titles=all_entries,
                header_type=page_type,
            )

    lines = content.split("\n")

    # Extract all existing entries
    existing_entries = []
    entry_line_indices = []
    for i, line in enumerate(lines):
        match = ENTRY_LINE_RE.match(line)
        if match:
            existing_entries.append(match.group(1))
            entry_line_indices.append(i)

    # Add new entry, deduplicate, and sort
    new_entry_already_present = has_entry_with_same_case_number(existing_entries, new_entry_title)
    all_entries = existing_entries if new_entry_already_present else existing_entries + [new_entry_title]
    sorted_entries = sort_versions_entries(all_entries)

    if entry_line_indices:
        if (
            new_entry_already_present
            and len(existing_entries) == len(set(existing_entries))
            and existing_entries == sorted_entries
        ):
            return content

        # Remove old entry lines (in reverse order)
        for idx in reversed(entry_line_indices):
            del lines[idx]

        # Find where to insert sorted entries
        insert_idx = None
        for i, line in enumerate(lines):
            if line.strip().startswith("[[Category:"):
                insert_idx = i
                break

        if insert_idx is None:
            for i, line in enumerate(lines):
                if line.strip() == "}}" or line.strip().endswith("}}"):
                    insert_idx = i + 1
                    break

        if insert_idx is None:
            insert_idx = len(lines)

        # Insert sorted entries
        for j, entry in enumerate(sorted_entries):
            lines.insert(insert_idx + j, f"* [[{entry}]]")

        return "\n".join(lines)

    # Fallback: add before first [[Category:
    if has_entry_with_same_case_number(existing_entries, new_entry_title):
        return content

    for i, line in enumerate(lines):
        if line.strip().startswith("[[Category:"):
            lines.insert(i, f"* [[{new_entry_title}]]")
            return "\n".join(lines)

    # Last resort: append before end
    return content.rstrip() + f"\n* [[{new_entry_title}]]\n"


def try_resolve_conflict(
    original_title: str,
    draft_content: str,
    existing_content: str,
    log_callback: Optional[LogCallback] = None,
) -> Tuple[bool, Optional[str], Optional[str]]:
    """
    Attempt to resolve a page title conflict.

    Args:
        original_title: The original page title that already exists
        draft_content: The wikitext content of our draft
        existing_content: The wikitext content of the existing page
        log_callback: Optional callback for logging actions

    Returns:
        (resolved, new_draft_title, error_message)
    """
    draft_metadata = extract_header_metadata(draft_content)
    if not draft_metadata:
        return False, None, "Could not extract {{Header/裁判文书}} metadata from draft content"

    draft_court = normalize_court_name(draft_metadata.get("court", ""))
    if not draft_court:
        return False, None, "Could not extract court from draft header"

    # Check if existing page is a court-document disambiguation page
    if is_versions_page(existing_content):
        return _resolve_versions_page_conflict(
            original_title,
            draft_content,
            existing_content,
            draft_court,
            log_callback,
        )

    # Check if existing page is a header page
    if is_header_page(existing_content):
        return _resolve_header_page_conflict(
            original_title,
            draft_content,
            existing_content,
            log_callback,
        )

    return False, None, "Existing page is neither {{裁判文书消歧义页}} nor {{Header/裁判文书}}"


def _resolve_versions_page_conflict(
    original_title: str,
    draft_content: str,
    existing_content: str,
    draft_noauthor: str,
    log_callback: Optional[LogCallback] = None,
) -> Tuple[bool, Optional[str], Optional[str]]:
    """Resolve conflict when existing page is a court-document disambiguation page."""

    def log(msg: str, ok: bool = True):
        if log_callback:
            log_callback(msg, ok)

    log(f"Detected existing court-document disambiguation page: [[{original_title}]]", True)

    new_draft_title = build_case_title_from_content(draft_content)
    if not new_draft_title:
        return False, None, "Could not extract case-number title from draft"
    log(f"New draft title: [[{new_draft_title}]]", True)

    draft_metadata = extract_header_metadata(draft_content) or {}
    draft_type = normalize_doc_type(draft_metadata.get("type", ""))

    # Update disambiguation page with new entry
    existing_entries = extract_versions_entries(existing_content)
    matching_entry_title = find_entry_with_same_case_number(existing_entries, new_draft_title)
    new_entry_already_present = matching_entry_title is not None
    base_versions = existing_content
    if is_legacy_versions_page(existing_content):
        converted_versions = convert_legacy_versions_page_content(
            original_title=original_title,
            existing_content=existing_content,
            fallback_court=draft_noauthor,
            fallback_year=draft_metadata.get("year", "").strip(),
            fallback_type=draft_type,
            entry_titles=existing_entries,
        )
        if converted_versions:
            base_versions = converted_versions

    updated_versions = add_entry_to_versions_page(
        base_versions,
        new_draft_title,
        new_entry_court=draft_noauthor,
        header_type=draft_type,
    )

    if wikitexts_match(updated_versions, existing_content):
        log(
            f"Disambiguation page [[{original_title}]] already contains case number at [[{matching_entry_title or new_draft_title}]]",
            True,
        )
        return True, matching_entry_title or new_draft_title, None

    try:
        save_page(
            original_title,
            updated_versions,
            summary=(
                "转换为裁判文书消歧义页"
                if is_legacy_versions_page(existing_content) and new_entry_already_present
                else f"转换为裁判文书消歧义页并添加新条目"
                if is_legacy_versions_page(existing_content)
                else f"添加新条目"
            ),
        )
        log(f"Updated disambiguation page [[{original_title}]] with new entry", True)
    except Exception as e:
        return False, None, f"Failed to update disambiguation page: {e}"

    return True, matching_entry_title or new_draft_title, None


def _resolve_header_page_conflict(
    original_title: str,
    draft_content: str,
    existing_content: str,
    log_callback: Optional[LogCallback] = None,
) -> Tuple[bool, Optional[str], Optional[str]]:
    """Resolve conflict when existing page is a {{Header/裁判文书}} page."""

    def log(msg: str, ok: bool = True):
        if log_callback:
            log_callback(msg, ok)

    draft_metadata = extract_header_metadata(draft_content)
    existing_metadata = extract_header_metadata(existing_content)
    if not draft_metadata or not existing_metadata:
        return False, None, "Could not extract current header metadata"

    new_existing_title = build_case_title_from_metadata(existing_metadata)
    new_draft_title = build_case_title_from_metadata(draft_metadata)
    existing_case_number = build_case_number_from_metadata(existing_metadata)
    draft_case_number = build_case_number_from_metadata(draft_metadata)
    existing_court = normalize_court_name(existing_metadata.get("court", ""))
    existing_header_type = normalize_doc_type(existing_metadata.get("type", ""))
    draft_court = normalize_court_name(draft_metadata.get("court", ""))
    draft_header_type = normalize_doc_type(draft_metadata.get("type", ""))

    if existing_court == draft_court:
        log(
            f"Detected existing {{{{Header/裁判文书}}}} page from same court: [[{original_title}]]",
            True,
        )
    else:
        log(
            f"Detected existing {{{{Header/裁判文书}}}} page from different court: [[{original_title}]]",
            True,
        )

    if not new_existing_title or not new_draft_title:
        return False, None, "Could not build case-number title from header metadata"
    if not existing_case_number or not draft_case_number:
        return False, None, "Could not extract case number from header metadata"
    if not existing_court or not existing_header_type:
        return False, None, "Could not extract court/type from existing page header"
    if existing_case_number == draft_case_number:
        return False, None, "Existing page already uses the same case number as the draft"

    log(f"Will move existing page to: [[{new_existing_title}]]", True)
    log(f"New draft title: [[{new_draft_title}]]", True)

    # Step 1: Move existing page to new title, or replace an existing redirect there.
    try:
        target_state = resolve_page(new_existing_title)
        if not target_state.exists:
            move_page(
                original_title,
                new_existing_title,
                reason=f"移动至具体案号页面，原标题改为消歧义页",
                leave_redirect=True,
            )
            log(f"Moved [[{original_title}]] → [[{new_existing_title}]]", True)
        elif target_state.is_redirect:
            if can_move_over_redirect(original_title, new_existing_title):
                move_page(
                    original_title,
                    new_existing_title,
                    reason=f"移动至具体案号页面，原标题改为消歧义页",
                    leave_redirect=True,
                    ignore_warnings=True,
                )
                log(
                    f"Moved [[{original_title}]] → [[{new_existing_title}]] over overwriteable redirect",
                    True,
                )
            else:
                save_page(
                    new_existing_title,
                    existing_content,
                    summary=f"以具体案号页面替换重定向",
                )
                log(
                    f"Replaced redirect [[{new_existing_title}]] with existing document content",
                    True,
                )
        elif target_state.content and is_header_page(target_state.content):
            existing_target_title = build_case_title_from_content(target_state.content)
            if existing_target_title != new_existing_title:
                return False, None, f"Target page already exists at {new_existing_title}"
            log(
                f"Case-number page [[{new_existing_title}]] already exists as a document page",
                True,
            )
        else:
            return False, None, f"Target page already exists at {new_existing_title}"
    except Exception as e:
        return False, None, f"Failed to move existing page: {e}"

    # Step 2: Edit case-specific page to add [[...]] to the title field
    try:
        exists, moved_content = get_page_content(new_existing_title)
        if not exists or not moved_content:
            return (
                False,
                None,
                f"Could not fetch moved page content at {new_existing_title}",
            )

        updated_existing = add_title_link_to_content(moved_content, original_title)
        if not wikitexts_match(updated_existing, moved_content):
            save_page(
                new_existing_title,
                updated_existing,
                summary=f"更新标题链接至消歧义页",
            )
            log(f"Updated [[{new_existing_title}]] with title link to disambiguation page", True)
    except Exception as e:
        return False, None, f"Failed to update moved page: {e}"

    # Step 3: Create disambiguation page at original title
    try:
        versions_content = build_versions_page_content(
            title=original_title,
            noauthor=existing_court,
            entry_titles=[new_existing_title, new_draft_title],
            header_type=existing_header_type,
        ) if existing_court == draft_court else build_grouped_versions_page_content(
            title=original_title,
            header_type=existing_header_type or draft_header_type,
            court_entries={
                existing_court: [new_existing_title],
                draft_court: [new_draft_title],
            },
        )
        save_page(
            original_title,
            versions_content,
            summary=f"创建消歧义页",
        )
        log(f"Created disambiguation page at [[{original_title}]]", True)
    except Exception as e:
        return False, None, f"Failed to create disambiguation page: {e}"

    return True, new_draft_title, None


def update_draft_for_conflict_resolution(
    draft_content: str, original_title: str
) -> str:
    """Update draft content after conflict resolution."""
    return add_title_link_to_content(draft_content, original_title)


def is_conflict_resolvable(
    existing_content: str, draft_content: str
) -> Tuple[bool, str]:
    """
    Check if a page conflict can be resolved.

    Returns:
        (is_resolvable, scenario_description)
    """
    if not existing_content or not draft_content:
        return False, "Missing content"

    draft_metadata = extract_header_metadata(draft_content)
    if not draft_metadata:
        return False, "Could not extract draft {{Header/裁判文书}} metadata"

    draft_court = normalize_court_name(draft_metadata.get("court", ""))
    if not draft_court:
        return False, "Could not extract court from draft header"

    # Check if existing page is a court-document disambiguation page
    if is_versions_page(existing_content):
        existing_noauthor = extract_versions_noauthor(existing_content)
        if existing_noauthor and existing_noauthor != draft_court:
            return True, "Existing page is a {{裁判文书消歧义页}} page from a different court"
        return True, "Existing page is a {{裁判文书消歧义页}} page"

    # Check if existing page is a header page from same court
    if is_header_page(existing_content):
        existing_metadata = extract_header_metadata(existing_content)
        if not existing_metadata:
            return False, "Could not extract existing court info"

        existing_case_number = build_case_number_from_metadata(existing_metadata)
        draft_case_number = build_case_number_from_metadata(draft_metadata)
        if existing_case_number and existing_case_number == draft_case_number:
            return False, "Existing page already uses the same case number as the draft"

        existing_court = normalize_court_name(existing_metadata.get("court", ""))
        if not existing_court:
            return False, "Could not extract existing court info"
        if existing_court != draft_court:
            return True, "Existing page is a {{Header/裁判文书}} page from a different court"
        return True, "Existing page is a {{Header/裁判文书}} page from same court"

    return False, "Existing page is neither {{裁判文书消歧义页}} nor {{Header/裁判文书}}"
