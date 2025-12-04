"""
Conflict resolution for duplicate page titles.

Handles two scenarios:
1. Existing page is a {{versions}} page - add new entry to it
2. Existing page is a {{header}} page from same court - create versions page and move existing
"""

import re
from typing import Optional, Tuple, Callable, List

from .mediawiki import get_page_content, save_page, move_page


# Type alias for conflict check result
ConflictCheckResult = Tuple[bool, str]

# Type for log callback: (message, is_success) -> None
LogCallback = Callable[[str, bool], None]


def extract_noauthor_from_content(content: str) -> Optional[str]:
    """Extract the noauthor field from wikitext content."""
    match = re.search(r"\|\s*noauthor\s*=\s*([^\n|]+)", content)
    if match:
        return match.group(1).strip()
    return None


def extract_court_and_doctype_from_content(
    content: str,
) -> Tuple[Optional[str], Optional[str]]:
    """
    Extract court name and document type from {{larger}} templates.

    Returns (court_name, doc_type) - doc_type has whitespace removed.
    """
    larger_matches = re.findall(r"\{\{larger\|([^}]+)\}\}", content)
    if len(larger_matches) >= 2:
        court_name = larger_matches[0].strip()
        doc_type = re.sub(r"\s+", "", larger_matches[1].strip())
        return court_name, doc_type
    elif len(larger_matches) == 1:
        return larger_matches[0].strip(), None
    return None, None


def extract_case_number_from_content(content: str) -> Optional[str]:
    """Extract case number from <div align="right">...</div> block."""
    match = re.search(
        r'<div\s+align\s*=\s*["\']?right["\']?\s*>\s*\n?([^<]+)', content, re.IGNORECASE
    )
    if match:
        return match.group(1).strip()
    return None


def extract_header_type_from_content(content: str) -> Optional[str]:
    """Extract the type field from {{header}} template."""
    match = re.search(r"\|\s*type\s*=\s*([^\n|]+)", content)
    if match:
        return match.group(1).strip()
    return None


def extract_header_year_from_content(content: str) -> Optional[str]:
    """Extract the year field from {{header}} template."""
    match = re.search(r"\|\s*year\s*=\s*(\d+)", content)
    if match:
        return match.group(1).strip()
    return None


def build_new_page_title(court: str, case_number: str, doc_type: str) -> str:
    """Build new page title in format: [court][case_number][doc_type]."""
    return f"{court}{case_number}{doc_type}"


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
    Build a {{versions}} page content.

    Args:
        title: The original page title
        noauthor: Court name
        entry_titles: List of page titles to link to
        year: Year from header
        header_type: Full type from header
    """
    sorted_titles = sorted(set(entry_titles))
    entries = "\n".join(f"* [[{t}]]" for t in sorted_titles)

    content = f"""{{{{versions
 | title      = {title}
 | noauthor   = {noauthor}
 | portal     = 
 | notes      = 
}}}}
{entries}

[[Category:{year}年{header_type}]]
[[Category:{header_type}]]
[[Category:{noauthor}]]
"""
    return content


def add_entry_to_versions_page(content: str, new_entry_title: str) -> str:
    """Add a new entry to an existing {{versions}} page and sort all entries."""
    lines = content.split("\n")

    # Extract all existing entries
    existing_entries = []
    entry_line_indices = []
    for i, line in enumerate(lines):
        if line.strip().startswith("* [["):
            match = re.match(r"\s*\*\s*\[\[([^\]]+)\]\]", line)
            if match:
                existing_entries.append(match.group(1))
                entry_line_indices.append(i)

    # Add new entry, deduplicate, and sort
    all_entries = existing_entries + [new_entry_title]
    sorted_entries = sorted(set(all_entries))

    if entry_line_indices:
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
    draft_noauthor = extract_noauthor_from_content(draft_content)
    existing_noauthor = extract_noauthor_from_content(existing_content)

    if not draft_noauthor:
        return False, None, "Could not extract noauthor from draft content"

    # Check if existing page is a versions page
    if existing_content.strip().startswith("{{versions"):
        return _resolve_versions_page_conflict(
            original_title,
            draft_content,
            existing_content,
            draft_noauthor,
            log_callback,
        )

    # Check if existing page is a header page
    if existing_content.strip().startswith("{{header"):
        if existing_noauthor != draft_noauthor:
            return (
                False,
                None,
                f"Court mismatch: existing='{existing_noauthor}', draft='{draft_noauthor}'",
            )
        return _resolve_header_page_conflict(
            original_title,
            draft_content,
            existing_content,
            draft_noauthor,
            log_callback,
        )

    return False, None, "Existing page is neither {{versions}} nor {{header}} page"


def _resolve_versions_page_conflict(
    original_title: str,
    draft_content: str,
    existing_content: str,
    draft_noauthor: str,
    log_callback: Optional[LogCallback] = None,
) -> Tuple[bool, Optional[str], Optional[str]]:
    """Resolve conflict when existing page is a {{versions}} page."""

    def log(msg: str, ok: bool = True):
        if log_callback:
            log_callback(msg, ok)

    log(f"Detected existing {{{{versions}}}} page: [[{original_title}]]", True)

    existing_noauthor = extract_noauthor_from_content(existing_content)
    if existing_noauthor != draft_noauthor:
        return (
            False,
            None,
            f"Court mismatch with versions page: existing='{existing_noauthor}', draft='{draft_noauthor}'",
        )

    # Extract court and doc_type from draft
    court, doc_type = extract_court_and_doctype_from_content(draft_content)
    if not court or not doc_type:
        return (
            False,
            None,
            "Could not extract court/doc_type from draft {{larger}} templates",
        )

    case_number = extract_case_number_from_content(draft_content)
    if not case_number:
        return False, None, "Could not extract case number from draft"

    new_draft_title = build_new_page_title(court, case_number, doc_type)
    log(f"New draft title: [[{new_draft_title}]]", True)

    # Update versions page with new entry
    updated_versions = add_entry_to_versions_page(existing_content, new_draft_title)

    try:
        save_page(
            original_title,
            updated_versions,
            summary=f"添加新条目：[[{new_draft_title}]]",
        )
        log(f"Updated versions page [[{original_title}]] with new entry", True)
    except Exception as e:
        return False, None, f"Failed to update versions page: {e}"

    return True, new_draft_title, None


def _resolve_header_page_conflict(
    original_title: str,
    draft_content: str,
    existing_content: str,
    court: str,
    log_callback: Optional[LogCallback] = None,
) -> Tuple[bool, Optional[str], Optional[str]]:
    """Resolve conflict when existing page is a {{header}} page from same court."""

    def log(msg: str, ok: bool = True):
        if log_callback:
            log_callback(msg, ok)

    log(
        f"Detected existing {{{{header}}}} page from same court: [[{original_title}]]",
        True,
    )

    # Extract info from draft
    draft_court, draft_doc_type = extract_court_and_doctype_from_content(draft_content)
    draft_case_number = extract_case_number_from_content(draft_content)

    if not draft_court or not draft_doc_type or not draft_case_number:
        return False, None, "Could not extract court/doc_type/case_number from draft"

    # Extract info from existing page
    existing_court, existing_doc_type = extract_court_and_doctype_from_content(
        existing_content
    )
    existing_case_number = extract_case_number_from_content(existing_content)
    existing_header_type = extract_header_type_from_content(existing_content)
    existing_year = extract_header_year_from_content(existing_content)

    if not existing_court or not existing_doc_type or not existing_case_number:
        return (
            False,
            None,
            "Could not extract court/doc_type/case_number from existing page",
        )
    if not existing_header_type or not existing_year:
        return False, None, "Could not extract type/year from existing page header"

    # Build new titles
    new_existing_title = build_new_page_title(
        existing_court, existing_case_number, existing_doc_type
    )
    new_draft_title = build_new_page_title(
        draft_court, draft_case_number, draft_doc_type
    )

    log(f"Will move existing page to: [[{new_existing_title}]]", True)
    log(f"New draft title: [[{new_draft_title}]]", True)

    # Step 1: Move existing page to new title
    try:
        move_page(
            original_title,
            new_existing_title,
            reason=f"移动至具体案号页面，原标题改为版本页：[[{original_title}]]",
            leave_redirect=True,
        )
        log(f"Moved [[{original_title}]] → [[{new_existing_title}]]", True)
    except Exception as e:
        return False, None, f"Failed to move existing page: {e}"

    # Step 2: Edit moved page to add [[...]] to title field
    try:
        exists, moved_content = get_page_content(new_existing_title)
        if not exists or not moved_content:
            return (
                False,
                None,
                f"Could not fetch moved page content at {new_existing_title}",
            )

        updated_existing = add_title_link_to_content(moved_content, original_title)
        save_page(
            new_existing_title,
            updated_existing,
            summary=f"更新标题链接至版本页：[[{original_title}]]",
        )
        log(f"Updated [[{new_existing_title}]] with title link to versions page", True)
    except Exception as e:
        return False, None, f"Failed to update moved page: {e}"

    # Step 3: Create versions page at original title
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
            summary=f"创建版本页，包含：[[{new_existing_title}]]、[[{new_draft_title}]]",
        )
        log(f"Created versions page at [[{original_title}]]", True)
    except Exception as e:
        return False, None, f"Failed to create versions page: {e}"

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

    draft_noauthor = extract_noauthor_from_content(draft_content)
    existing_noauthor = extract_noauthor_from_content(existing_content)

    # Check if existing page is a versions page
    if existing_content.strip().startswith("{{versions"):
        if not draft_noauthor:
            return False, "Could not extract court from draft"
        if existing_noauthor and existing_noauthor != draft_noauthor:
            return False, f"Court mismatch with versions page"
        return True, "Existing page is a {{versions}} page"

    # Check if existing page is a header page from same court
    if existing_content.strip().startswith("{{header"):
        if not draft_noauthor or not existing_noauthor:
            return False, "Could not extract court info"
        if existing_noauthor != draft_noauthor:
            return False, f"Court mismatch: different courts"
        return True, "Existing page is a {{header}} page from same court"

    return False, "Existing page is neither {{versions}} nor {{header}}"
