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
    build_case_title_from_content,
    build_case_title_from_metadata,
    is_header_page,
    is_legacy_versions_page,
    is_versions_page,
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
ENTRY_LINE_RE = re.compile(r"\s*\*\s*\[\[([^\]]+)\]\]")


def extract_versions_noauthor(content: str) -> Optional[str]:
    """Extract the court/noauthor field from a disambiguation page."""
    metadata = parse_versions_metadata(content)
    if not metadata:
        return None
    value = (metadata.get("court", "") or metadata.get("noauthor", "")).strip()
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
    value = metadata.get("court", "").strip()
    return value or None


def extract_header_type_from_content(content: str) -> Optional[str]:
    """Extract the type field from {{Header/裁判文书}}."""
    metadata = extract_header_metadata(content)
    if not metadata:
        return None
    value = metadata.get("type", "").strip()
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
    year: str,
    header_type: str,
) -> str:
    """
    Build a {{裁判文书消歧义页}} page content.

    Args:
        title: The original page title
        noauthor: Court name
        entry_titles: List of page titles to link to
        year: Year from header
        header_type: Full type from header
    """
    sorted_titles = sorted(set(entry_titles))
    entries = "\n".join(f"* [[{t}]]" for t in sorted_titles)

    content = f"""{{{{裁判文书消歧义页
 | title      = {title}
 | court      = {noauthor}
 | type       = {header_type}
 | year       = {year}
}}}}
{entries}
"""
    return content


def extract_versions_entries(content: str) -> list[str]:
    """Extract linked entries from a disambiguation page."""
    entries: list[str] = []
    for line in content.splitlines():
        match = ENTRY_LINE_RE.match(line)
        if match:
            entries.append(match.group(1).strip())
    return entries


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
    court = (metadata.get("court", "") or metadata.get("noauthor", "")).strip() or fallback_court
    inferred_year, inferred_type = infer_year_type_from_categories(existing_content)
    year = inferred_year or fallback_year
    header_type = inferred_type or fallback_type

    if not court or not year or not header_type:
        return None

    return build_versions_page_content(
        title=title,
        noauthor=court,
        entry_titles=entry_titles,
        year=year,
        header_type=header_type,
    )


def add_entry_to_versions_page(content: str, new_entry_title: str) -> str:
    """Add a new entry to an existing disambiguation page and sort all entries."""
    lines = content.split("\n")

    # Extract all existing entries
    existing_entries = []
    entry_line_indices = []
    for i, line in enumerate(lines):
        if line.strip().startswith("* [["):
            match = ENTRY_LINE_RE.match(line)
            if match:
                existing_entries.append(match.group(1))
                entry_line_indices.append(i)

    # Add new entry, deduplicate, and sort
    all_entries = existing_entries + [new_entry_title]
    sorted_entries = sorted(set(all_entries))

    if entry_line_indices:
        if (
            new_entry_title in existing_entries
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

    draft_court = draft_metadata.get("court", "").strip()
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
        existing_court = extract_header_court(existing_content)
        if existing_court != draft_court:
            return (
                False,
                None,
                f"Court mismatch: existing='{existing_court}', draft='{draft_court}'",
            )
        return _resolve_header_page_conflict(
            original_title,
            draft_content,
            existing_content,
            draft_court,
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

    existing_noauthor = extract_versions_noauthor(existing_content)
    if existing_noauthor and existing_noauthor != draft_noauthor:
        return (
            False,
            None,
            f"Court mismatch with disambiguation page: existing='{existing_noauthor}', draft='{draft_noauthor}'",
        )

    new_draft_title = build_case_title_from_content(draft_content)
    if not new_draft_title:
        return False, None, "Could not extract case-number title from draft"
    log(f"New draft title: [[{new_draft_title}]]", True)

    # Update disambiguation page with new entry
    existing_entries = extract_versions_entries(existing_content)
    new_entry_already_present = new_draft_title in existing_entries
    updated_versions = add_entry_to_versions_page(existing_content, new_draft_title)
    if is_legacy_versions_page(existing_content):
        draft_metadata = extract_header_metadata(draft_content) or {}
        all_entries = extract_versions_entries(updated_versions)
        converted_versions = convert_legacy_versions_page_content(
            original_title=original_title,
            existing_content=existing_content,
            fallback_court=draft_noauthor,
            fallback_year=draft_metadata.get("year", "").strip(),
            fallback_type=draft_metadata.get("type", "").strip(),
            entry_titles=all_entries,
        )
        if converted_versions:
            updated_versions = converted_versions

    if wikitexts_match(updated_versions, existing_content):
        log(
            f"Disambiguation page [[{original_title}]] already contains [[{new_draft_title}]]",
            True,
        )
        return True, new_draft_title, None

    try:
        save_page(
            original_title,
            updated_versions,
            summary=(
                "转换为裁判文书消歧义页"
                if is_legacy_versions_page(existing_content) and new_entry_already_present
                else f"转换为裁判文书消歧义页并添加新条目：[[{new_draft_title}]]"
                if is_legacy_versions_page(existing_content)
                else f"添加新条目：[[{new_draft_title}]]"
            ),
        )
        log(f"Updated disambiguation page [[{original_title}]] with new entry", True)
    except Exception as e:
        return False, None, f"Failed to update disambiguation page: {e}"

    return True, new_draft_title, None


def _resolve_header_page_conflict(
    original_title: str,
    draft_content: str,
    existing_content: str,
    court: str,
    log_callback: Optional[LogCallback] = None,
) -> Tuple[bool, Optional[str], Optional[str]]:
    """Resolve conflict when existing page is a {{Header/裁判文书}} page."""

    def log(msg: str, ok: bool = True):
        if log_callback:
            log_callback(msg, ok)

    log(
        f"Detected existing {{{{Header/裁判文书}}}} page from same court: [[{original_title}]]",
        True,
    )

    draft_metadata = extract_header_metadata(draft_content)
    existing_metadata = extract_header_metadata(existing_content)
    if not draft_metadata or not existing_metadata:
        return False, None, "Could not extract current header metadata"

    new_existing_title = build_case_title_from_metadata(existing_metadata)
    new_draft_title = build_case_title_from_metadata(draft_metadata)
    existing_court = existing_metadata.get("court", "").strip()
    existing_header_type = existing_metadata.get("type", "").strip()
    existing_year = existing_metadata.get("year", "").strip()

    if not new_existing_title or not new_draft_title:
        return False, None, "Could not build case-number title from header metadata"
    if not existing_court or not existing_header_type or not existing_year:
        return False, None, "Could not extract court/type/year from existing page header"
    if new_existing_title == new_draft_title:
        return False, None, "Existing page already uses the same case-number title as the draft"

    log(f"Will move existing page to: [[{new_existing_title}]]", True)
    log(f"New draft title: [[{new_draft_title}]]", True)

    # Step 1: Move existing page to new title, or replace an existing redirect there.
    try:
        target_state = resolve_page(new_existing_title)
        if not target_state.exists:
            move_page(
                original_title,
                new_existing_title,
                reason=f"移动至具体案号页面，原标题改为消歧义页：[[{original_title}]]",
                leave_redirect=True,
            )
            log(f"Moved [[{original_title}]] → [[{new_existing_title}]]", True)
        elif target_state.is_redirect:
            if can_move_over_redirect(original_title, new_existing_title):
                move_page(
                    original_title,
                    new_existing_title,
                    reason=f"移动至具体案号页面，原标题改为消歧义页：[[{original_title}]]",
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
                    summary=f"以具体案号页面替换重定向：[[{original_title}]]",
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
                summary=f"更新标题链接至消歧义页：[[{original_title}]]",
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
            year=existing_year,
            header_type=existing_header_type,
        )
        save_page(
            original_title,
            versions_content,
            summary=f"创建消歧义页，包含：[[{new_existing_title}]]、[[{new_draft_title}]]",
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

    draft_court = draft_metadata.get("court", "").strip()
    if not draft_court:
        return False, "Could not extract court from draft header"

    # Check if existing page is a court-document disambiguation page
    if is_versions_page(existing_content):
        existing_noauthor = extract_versions_noauthor(existing_content)
        if existing_noauthor and existing_noauthor != draft_court:
            return False, "Court mismatch with disambiguation page"
        return True, "Existing page is a {{裁判文书消歧义页}} page"

    # Check if existing page is a header page from same court
    if is_header_page(existing_content):
        existing_court = extract_header_court(existing_content)
        if not existing_court:
            return False, "Could not extract existing court info"
        if existing_court != draft_court:
            return False, "Court mismatch: different courts"
        return True, "Existing page is a {{Header/裁判文书}} page from same court"

    return False, "Existing page is neither {{裁判文书消歧义页}} nor {{Header/裁判文书}}"
