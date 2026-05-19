"""
Uploader for court documents to zhwikisource.

This module handles batch uploading with error handling and conflict resolution.
Rate limiting is handled by pywikibot's built-in throttle.
"""

import json
import logging
import re
import difflib
from dataclasses import dataclass, asdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional, Tuple

from rich.progress import Progress, BarColumn, TextColumn, TimeRemainingColumn, TaskProgressColumn, SpinnerColumn
from rich.console import Console

from .mediawiki import (
    DEFAULT_READ_BATCH_SIZE,
    PageSnapshot,
    check_page_exists,
    fetch_page_content_batch,
    get_page_content,
    resolve_page,
    resolve_pages_batch,
    save_page,
)
from . import conflict_resolution as conflict_resolution_module
from .conflict_resolution import (
    is_conflict_resolvable,
    try_resolve_conflict,
    update_draft_for_conflict_resolution,
)
from .overwrite_quality import (
    body_redaction_penalty,
    canonicalize_redaction_markers,
    content_link_count,
    contains_os_redaction,
    formatting_regression_penalty,
    is_safe_header_only_update,
    is_safe_formatting_improvement,
    is_safe_redaction_marker_update,
    is_safe_signature_structure_improvement,
    normalize_existing_header_safe_fixes,
    structural_regression_penalty,
)
from .page_metadata import (
    build_case_redirect_summary,
    build_case_redirect_text,
    build_case_title_from_content,
    is_header_page,
    normalize_wikitext_for_comparison,
    wikitexts_match,
)

console = Console()
DEFAULT_UPLOAD_QUERY_BATCH_SIZE = DEFAULT_READ_BATCH_SIZE
UNCHECKED_OVERWRITE_CATEGORY = "覆盖版本未检查的裁判文书"
UNCHECKED_OVERWRITE_CATEGORY_LINE = f"[[Category:{UNCHECKED_OVERWRITE_CATEGORY}]]"
UNCHECKED_OVERWRITE_CATEGORY_RE = re.compile(
    rf"\[\[\s*(?:Category|分类|分類)\s*:\s*{re.escape(UNCHECKED_OVERWRITE_CATEGORY)}(?:\|[^\]\n]*)?\]\]",
    re.IGNORECASE,
)


def utc_now_iso() -> str:
    """Return the current UTC timestamp in ISO8601-with-Z format."""
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


@dataclass
class UploadResult:
    """Result of uploading a single document."""
    title: str
    wenshu_id: str
    status: str  # 'uploaded', 'skipped', 'conflict_resolved', 'failed', 'overwritable', 'reverted_overwrite'
    final_title: Optional[str] = None  # Different from title if conflict resolved
    case_title: Optional[str] = None
    redirect_status: Optional[str] = None
    message: str = ""
    timestamp: str = ""
    wikitext: Optional[str] = None  # Included for overwritable entries


@dataclass
class _TrackedWikiWrite:
    """A save/move that verbose and dry-run modes can report."""
    title: str
    before: Optional[str]
    after: str
    summary: str
    action: str = "save"


def build_edit_summary(wenshu_id: str) -> str:
    """Build a standardized edit summary."""
    # return f"Imported from 裁判文书网 (credit: caseopen.org), source: https://wenshu.court.gov.cn/website/wenshu/181107ANFZ0BXSK4/index.html?docId={wenshu_id}"
    return "Imported from 裁判文书网 (credit: caseopen.org)"


def build_manual_revert_summary(wenshu_id: str) -> str:
    """Build the edit summary for restoring pre-existing content after a hidden overwrite."""
    return "回退覆盖导入版本并标记待检查"


def _add_unchecked_overwrite_category(content: str) -> str:
    """Append the unchecked-overwrite review category to existing content."""
    if UNCHECKED_OVERWRITE_CATEGORY_RE.search(content or ""):
        return content

    return f"{(content or '').rstrip()}\n{UNCHECKED_OVERWRITE_CATEGORY_LINE}\n"


def _append_message(message: str, extra: str) -> str:
    """Append a short suffix to an existing status message."""
    if not message:
        return extra
    return f"{message}; {extra}"


def _build_progress_description(action: str, uploaded: int, failed: int, skipped: int) -> str:
    """Build a consistent rich progress description for upload batches."""
    return f"[cyan]{action}: {uploaded} ✓, {failed} ✗, {skipped} ⊘"


def _normalize_diff_text(content: Optional[str]) -> str:
    if content is None:
        return ""
    return content.replace("\r\n", "\n").replace("\r", "\n").rstrip()


def _format_wikitext_diff(write: _TrackedWikiWrite) -> str:
    """Return a unified diff for one tracked write."""
    before_text = _normalize_diff_text(write.before)
    after_text = _normalize_diff_text(write.after)
    if before_text == after_text:
        return ""

    before_label = f"{write.title} (existing)" if write.before is not None else f"{write.title} (new)"
    after_label = f"{write.title} ({write.action})"
    return "\n".join(
        difflib.unified_diff(
            before_text.splitlines(),
            after_text.splitlines(),
            fromfile=before_label,
            tofile=after_label,
            lineterm="",
        )
    )


def _emit_verbose_result(
    *,
    line_num: int,
    result: UploadResult,
    writes: list[_TrackedWikiWrite],
    dry_run: bool,
) -> None:
    """Print one document's decision and any tracked wikitext diffs."""
    prefix = "DRY RUN " if dry_run else ""
    target_title = result.final_title or result.title
    console.print(
        f"{prefix}line {line_num}: {result.status} [[{target_title}]] "
        f"(wenshu_id={result.wenshu_id})",
        markup=False,
    )
    if result.case_title:
        console.print(f"  case_title: [[{result.case_title}]]", markup=False)
    if result.message:
        console.print(f"  message: {result.message}", markup=False)

    if not writes:
        console.print("  wikitext diff: (no page write)", markup=False)
        return

    for write in writes:
        verb = "would save" if dry_run and write.action == "save" else write.action
        console.print(f"  {verb}: [[{write.title}]]", markup=False)
        if write.summary:
            console.print(f"  summary: {write.summary}", markup=False)
        diff_text = _format_wikitext_diff(write)
        if diff_text:
            console.print(diff_text, markup=False)
        else:
            console.print("  wikitext diff: (no change)", markup=False)


def _is_header_landing_page(page_state) -> bool:
    """Return whether a resolved page lands on a real court-document page."""
    return bool(page_state and page_state.exists and page_state.content and is_header_page(page_state.content))


def ensure_case_number_redirect(
    final_title: str,
    wikitext: str,
    existing_case_page: Optional[object] = None,
) -> Tuple[Optional[str], str]:
    """
    Ensure the case-number redirect exists for an uploaded document.

    Returns:
        (case_title, redirect_status)
    """
    case_title = build_case_title_from_content(wikitext)
    if not case_title:
        return None, "missing_case_title"

    if case_title == final_title:
        return case_title, "not_needed"

    page_state = existing_case_page if existing_case_page is not None else resolve_page(case_title)
    if not page_state.exists:
        save_page(
            case_title,
            build_case_redirect_text(final_title),
            build_case_redirect_summary(final_title),
        )
        return case_title, "created"

    if page_state.is_redirect and page_state.resolved_title == final_title:
        return case_title, "existing"

    if _is_header_landing_page(page_state):
        if page_state.resolved_title == final_title:
            return case_title, "existing"
        return case_title, "occupied_by_document"

    if page_state.is_redirect:
        save_page(
            case_title,
            build_case_redirect_text(final_title),
            build_case_redirect_summary(final_title),
        )
        return case_title, "updated"

    return case_title, "occupied"


def _attach_case_redirect(
    result: UploadResult,
    final_title: str,
    wikitext: str,
    existing_case_page: Optional[object] = None,
) -> UploadResult:
    """Populate case-title metadata and create/update redirect when safe."""
    try:
        case_title, redirect_status = ensure_case_number_redirect(
            final_title,
            wikitext,
            existing_case_page=existing_case_page,
        )
    except Exception as e:
        result.case_title = build_case_title_from_content(wikitext)
        result.redirect_status = "failed"
        result.message = _append_message(result.message, f"Case-number redirect failed: {e}")
        return result

    result.case_title = case_title
    result.redirect_status = redirect_status

    if redirect_status == "created":
        result.message = _append_message(result.message, f"Created case-number redirect: {case_title}")
    elif redirect_status == "updated":
        result.message = _append_message(result.message, f"Updated case-number redirect: {case_title}")
    elif redirect_status == "occupied":
        result.message = _append_message(result.message, f"Case-number title occupied: {case_title}")
    elif redirect_status == "occupied_by_document":
        result.message = _append_message(
            result.message,
            f"Case-number title already lands on document page: {case_title}",
        )

    return result


def _build_overwritable_result(
    *,
    source_title: str,
    target_title: str,
    wenshu_id: str,
    wikitext: str,
    case_title: Optional[str],
    message: str,
    timestamp: str,
) -> UploadResult:
    """Build an overwritable-log result retaining the import wikitext."""
    return UploadResult(
        title=source_title,
        wenshu_id=wenshu_id,
        status='overwritable',
        final_title=target_title,
        case_title=case_title,
        message=message,
        timestamp=timestamp,
        wikitext=wikitext,
    )


def _save_safe_existing_update(
    *,
    source_title: str,
    target_title: str,
    wenshu_id: str,
    content: str,
    case_title: Optional[str],
    message: str,
    timestamp: str,
    reason: str,
) -> UploadResult:
    try:
        save_page(target_title, content, build_edit_summary(wenshu_id))
    except Exception as e:
        return UploadResult(
            title=source_title,
            wenshu_id=wenshu_id,
            status='failed',
            final_title=target_title,
            case_title=case_title,
            message=f"Failed to save safe existing-page update: {e}",
            timestamp=timestamp,
        )

    return UploadResult(
        title=source_title,
        wenshu_id=wenshu_id,
        status='uploaded',
        final_title=target_title,
        case_title=case_title,
        message=_append_message(message, reason),
        timestamp=timestamp,
    )


def _build_no_overwrite_result(
    *,
    source_title: str,
    target_title: str,
    wenshu_id: str,
    case_title: Optional[str],
    message: str,
    timestamp: str,
    reason: str,
) -> UploadResult:
    return UploadResult(
        title=source_title,
        wenshu_id=wenshu_id,
        status='skipped',
        final_title=target_title,
        case_title=case_title,
        redirect_status='existing',
        message=_append_message(message, reason),
        timestamp=timestamp,
    )


def _hide_overwrite_revision_for_review(
    *,
    source_title: str,
    target_title: str,
    wenshu_id: str,
    import_wikitext: str,
    existing_content: Optional[str],
    case_title: Optional[str],
    message: str,
    timestamp: str,
) -> UploadResult:
    """
    Save the import once, then restore existing content with a review category.

    This keeps the imported revision in page history for maintainers while
    leaving the live page on the pre-existing content.
    """
    if existing_content is None:
        return UploadResult(
            title=source_title,
            wenshu_id=wenshu_id,
            status='failed',
            final_title=target_title,
            case_title=case_title,
            message="Page exists but content could not be fetched for review revert",
            timestamp=timestamp,
        )

    existing_for_decision = normalize_existing_header_safe_fixes(existing_content)
    existing_changed_safely = not wikitexts_match(existing_for_decision, existing_content)
    import_text = normalize_wikitext_for_comparison(import_wikitext)

    if wikitexts_match(import_text, existing_for_decision):
        if existing_changed_safely:
            return _save_safe_existing_update(
                source_title=source_title,
                target_title=target_title,
                wenshu_id=wenshu_id,
                content=existing_for_decision,
                case_title=case_title,
                message=message,
                timestamp=timestamp,
                reason="Saved existing-page metadata normalization without overwrite",
            )
        return _build_no_overwrite_result(
            source_title=source_title,
            target_title=target_title,
            wenshu_id=wenshu_id,
            case_title=case_title,
            message=message,
            timestamp=timestamp,
            reason="Import matches existing page after safe normalization",
        )

    if (
        is_safe_header_only_update(import_text, existing_for_decision)
        or is_safe_formatting_improvement(import_text, existing_for_decision)
        or is_safe_redaction_marker_update(import_text, existing_for_decision)
        or is_safe_signature_structure_improvement(import_text, existing_for_decision)
    ):
        try:
            save_page(target_title, import_wikitext, build_edit_summary(wenshu_id))
        except Exception as e:
            return UploadResult(
                title=source_title,
                wenshu_id=wenshu_id,
                status='failed',
                final_title=target_title,
                case_title=case_title,
                message=f"Failed to save safe automated update: {e}",
                timestamp=timestamp,
            )

        return UploadResult(
            title=source_title,
            wenshu_id=wenshu_id,
            status='uploaded',
            final_title=target_title,
            case_title=case_title,
            message=_append_message(message, "Saved safe automated update without review category"),
            timestamp=timestamp,
        )

    existing_redaction_penalty = body_redaction_penalty(existing_for_decision)
    import_redaction_penalty = body_redaction_penalty(import_text)
    existing_structural_penalty = structural_regression_penalty(existing_for_decision)
    import_structural_penalty = structural_regression_penalty(import_text)
    existing_formatting_penalty = formatting_regression_penalty(existing_for_decision)
    import_formatting_penalty = formatting_regression_penalty(import_text)
    redaction_canon_matches = (
        canonicalize_redaction_markers(existing_for_decision)
        == canonicalize_redaction_markers(import_text)
    )
    import_loses_links = content_link_count(import_text) < content_link_count(existing_for_decision)

    if redaction_canon_matches and import_redaction_penalty > existing_redaction_penalty:
        if existing_changed_safely:
            return _save_safe_existing_update(
                source_title=source_title,
                target_title=target_title,
                wenshu_id=wenshu_id,
                content=existing_for_decision,
                case_title=case_title,
                message=message,
                timestamp=timestamp,
                reason="Skipped worse redaction formatting and saved existing-page metadata normalization",
            )
        return _build_no_overwrite_result(
            source_title=source_title,
            target_title=target_title,
            wenshu_id=wenshu_id,
            case_title=case_title,
            message=message,
            timestamp=timestamp,
            reason="Skipped worse redaction formatting",
        )

    if (
        import_structural_penalty > existing_structural_penalty
        and import_redaction_penalty >= existing_redaction_penalty
    ) or (import_loses_links and import_redaction_penalty >= existing_redaction_penalty):
        if existing_changed_safely:
            return _save_safe_existing_update(
                source_title=source_title,
                target_title=target_title,
                wenshu_id=wenshu_id,
                content=existing_for_decision,
                case_title=case_title,
                message=message,
                timestamp=timestamp,
                reason="Skipped structurally worse import and saved existing-page metadata normalization",
            )
        return _build_no_overwrite_result(
            source_title=source_title,
            target_title=target_title,
            wenshu_id=wenshu_id,
            case_title=case_title,
            message=message,
            timestamp=timestamp,
            reason="Skipped structurally worse import",
        )

    if (
        import_formatting_penalty > existing_formatting_penalty
        and import_redaction_penalty >= existing_redaction_penalty
    ):
        if existing_changed_safely:
            return _save_safe_existing_update(
                source_title=source_title,
                target_title=target_title,
                wenshu_id=wenshu_id,
                content=existing_for_decision,
                case_title=case_title,
                message=message,
                timestamp=timestamp,
                reason="Skipped formatting-regressed import and saved existing-page metadata normalization",
            )
        return _build_no_overwrite_result(
            source_title=source_title,
            target_title=target_title,
            wenshu_id=wenshu_id,
            case_title=case_title,
            message=message,
            timestamp=timestamp,
            reason="Skipped formatting-regressed import",
        )

    if contains_os_redaction(existing_content):
        return _build_overwritable_result(
            source_title=source_title,
            target_title=target_title,
            wenshu_id=wenshu_id,
            wikitext=import_wikitext,
            case_title=case_title,
            message=_append_message(message, "Existing page contains {{PRC-redact|N|os=yes}}"),
            timestamp=timestamp,
        )

    try:
        save_page(target_title, import_wikitext, build_edit_summary(wenshu_id))
    except Exception as e:
        return UploadResult(
            title=source_title,
            wenshu_id=wenshu_id,
            status='failed',
            final_title=target_title,
            case_title=case_title,
            message=f"Failed to save overwrite revision for review: {e}",
            timestamp=timestamp,
        )

    try:
        save_page(
            target_title,
            _add_unchecked_overwrite_category(existing_for_decision),
            build_manual_revert_summary(wenshu_id),
        )
    except Exception as e:
        return UploadResult(
            title=source_title,
            wenshu_id=wenshu_id,
            status='failed',
            final_title=target_title,
            case_title=case_title,
            message=f"Failed to revert overwrite revision for review: {e}",
            timestamp=timestamp,
        )

    return UploadResult(
        title=source_title,
        wenshu_id=wenshu_id,
        status='reverted_overwrite',
        final_title=target_title,
        case_title=case_title,
        message=_append_message(message, "Saved overwrite revision and restored original content with review category"),
        timestamp=timestamp,
    )


def _handle_existing_case_header_page(
    *,
    source_title: str,
    wenshu_id: str,
    import_wikitext: str,
    case_title: Optional[str],
    page_state: object,
    message: str,
    timestamp: str,
) -> Optional[UploadResult]:
    """Compare an existing case-title header page and handle it as same-document content."""
    if not _is_header_landing_page(page_state):
        return None

    target_title = (
        getattr(page_state, "resolved_title", None)
        or case_title
        or getattr(page_state, "requested_title", None)
        or source_title
    )
    existing_content = getattr(page_state, "content", None)

    if wikitexts_match(import_wikitext, existing_content):
        return UploadResult(
            title=source_title,
            wenshu_id=wenshu_id,
            status='skipped',
            final_title=target_title,
            case_title=case_title,
            redirect_status='existing',
            message=message,
            timestamp=timestamp,
        )

    return _hide_overwrite_revision_for_review(
        source_title=source_title,
        target_title=target_title,
        wenshu_id=wenshu_id,
        import_wikitext=import_wikitext,
        existing_content=existing_content,
        case_title=case_title,
        message=message,
        timestamp=timestamp,
    )


def upload_document(
    title: str,
    wenshu_id: str,
    wikitext: str,
    resolve_conflicts: bool = True,
    force_overwrite: bool = False,
    existing_page: Optional[PageSnapshot] = None,
    case_page: Optional[object] = None,
) -> UploadResult:
    """
    Upload a single document to zhwikisource.
    
    Rate limiting is handled by pywikibot's built-in throttle.
    
    Args:
        title: The page title
        wenshu_id: The wenshu key for tracking
        wikitext: The wikitext content to upload
        resolve_conflicts: Whether to attempt conflict resolution
        
    Returns:
        UploadResult with status and details
    """
    timestamp = utc_now_iso()
    case_title = build_case_title_from_content(wikitext)

    if case_page is None:
        try:
            case_page = resolve_page(case_title) if case_title else None
        except Exception as e:
            return UploadResult(
                title=title,
                wenshu_id=wenshu_id,
                status='failed',
                final_title=title,
                case_title=case_title,
                message=f"Failed to resolve case-number title: {e}",
                timestamp=timestamp,
            )

    # Force-overwrite mode: skip existence check and write directly
    if force_overwrite:
        try:
            save_page(title, wikitext, build_edit_summary(wenshu_id))
            return _attach_case_redirect(UploadResult(
                title=title,
                wenshu_id=wenshu_id,
                status='uploaded',
                final_title=title,
                message="Overwritten successfully",
                timestamp=timestamp,
            ), title, wikitext, existing_case_page=case_page)
        except Exception as e:
            return UploadResult(
                title=title,
                wenshu_id=wenshu_id,
                status='failed',
                final_title=title,
                case_title=case_title,
                message=f"Failed to overwrite: {e}",
                timestamp=timestamp,
            )

    # Check if page exists
    if existing_page is None:
        try:
            exists, _ = check_page_exists(title)
        except Exception as e:
            return UploadResult(
                title=title,
                wenshu_id=wenshu_id,
                status='failed',
                final_title=title,
                case_title=case_title,
                message=f"Failed to check page existence: {e}",
                timestamp=timestamp,
            )
        existing_content = None
    else:
        exists = existing_page.exists
        existing_content = existing_page.content if existing_page.exists else None
    
    if exists:
        # Page exists - check if we should try conflict resolution
        if resolve_conflicts:
            try:
                if existing_content is None:
                    _, existing_content = get_page_content(title)
                if existing_content:
                    # Check if content is identical
                    if wikitexts_match(wikitext, existing_content):
                        return _attach_case_redirect(UploadResult(
                            title=title,
                            wenshu_id=wenshu_id,
                            status='skipped',
                            final_title=title,
                            message="Content identical to existing page",
                            timestamp=timestamp,
                        ), title, wikitext, existing_case_page=case_page)

                    # Check if conflict is resolvable
                    is_resolvable, scenario = is_conflict_resolvable(
                        existing_content, wikitext
                    )
                    
                    if is_resolvable:
                        # Attempt resolution
                        resolved, new_title, error = try_resolve_conflict(
                            title, wikitext, existing_content
                        )
                        
                        if resolved and new_title:
                            # Update draft content for resolution
                            updated_wikitext = update_draft_for_conflict_resolution(
                                wikitext, title
                            )
                            
                            # Check if new title already exists
                            try:
                                if case_title and new_title == case_title and case_page is not None:
                                    new_page = case_page
                                else:
                                    new_page = resolve_page(new_title)
                                if new_page.exists:
                                    case_header_result = _handle_existing_case_header_page(
                                        source_title=title,
                                        wenshu_id=wenshu_id,
                                        import_wikitext=updated_wikitext,
                                        case_title=new_title,
                                        page_state=new_page,
                                        message=f"Case-specific page already exists: {new_page.resolved_title or new_title}",
                                        timestamp=timestamp,
                                    )
                                    if case_header_result:
                                        return case_header_result

                                    # Case-specific page exists but does not land on a real document page.
                                    target_existing_content = new_page.content
                                    if new_page.is_redirect or target_existing_content is None:
                                        _, target_existing_content = get_page_content(new_title)

                                    return _hide_overwrite_revision_for_review(
                                        source_title=title,
                                        target_title=new_title,
                                        wenshu_id=wenshu_id,
                                        import_wikitext=updated_wikitext,
                                        existing_content=target_existing_content,
                                        case_title=new_title,
                                        message=f"Case-specific page already exists: {new_title}",
                                        timestamp=timestamp,
                                    )
                            except Exception as e:
                                return UploadResult(
                                    title=title,
                                    wenshu_id=wenshu_id,
                                    status='failed',
                                    final_title=new_title,
                                    case_title=new_title,
                                    message=f"Failed to check new title existence: {e}",
                                    timestamp=timestamp,
                                )
                            
                            # Save the draft page at new title
                            # (pywikibot handles rate limiting automatically)
                            try:
                                save_page(
                                    new_title,
                                    updated_wikitext,
                                    build_edit_summary(wenshu_id),
                                )
                                return _attach_case_redirect(UploadResult(
                                    title=title,
                                    wenshu_id=wenshu_id,
                                    status='conflict_resolved',
                                    final_title=new_title,
                                    message=f"Resolved: {scenario}",
                                    timestamp=timestamp,
                                ), new_title, updated_wikitext, existing_case_page=case_page)
                            except Exception as e:
                                return UploadResult(
                                    title=title,
                                    wenshu_id=wenshu_id,
                                    status='failed',
                                    final_title=new_title,
                                    case_title=new_title,
                                    message=f"Failed to save after conflict resolution: {e}",
                                    timestamp=timestamp,
                                )
                        else:
                            return UploadResult(
                                title=title,
                                wenshu_id=wenshu_id,
                                status='failed',
                                final_title=title,
                                case_title=case_title,
                                message=f"Conflict resolution failed: {error}",
                                timestamp=timestamp,
                            )
                    else:
                        case_header_result = _handle_existing_case_header_page(
                            source_title=title,
                            wenshu_id=wenshu_id,
                            import_wikitext=wikitext,
                            case_title=case_title,
                            page_state=case_page,
                            message=f"Case-number page already exists: {case_page.resolved_title if case_page else case_title}",
                            timestamp=timestamp,
                        )
                        if case_header_result:
                            return case_header_result
                        # Not resolvable - store the overwrite as a hidden revision for manual review.
                        return _hide_overwrite_revision_for_review(
                            source_title=title,
                            target_title=title,
                            wenshu_id=wenshu_id,
                            import_wikitext=wikitext,
                            existing_content=existing_content,
                            case_title=case_title,
                            message=f"Page exists, conflict not resolvable: {scenario}",
                            timestamp=timestamp,
                        )
                return UploadResult(
                    title=title,
                    wenshu_id=wenshu_id,
                    status='failed',
                    final_title=title,
                    case_title=case_title,
                    message="Page exists but content could not be fetched",
                    timestamp=timestamp,
                )
            except Exception as e:
                return UploadResult(
                    title=title,
                    wenshu_id=wenshu_id,
                    status='failed',
                    final_title=title,
                    case_title=case_title,
                    message=f"Error during conflict check: {e}",
                    timestamp=timestamp,
                )
        else:
            if _is_header_landing_page(case_page):
                return UploadResult(
                    title=title,
                    wenshu_id=wenshu_id,
                    status='skipped',
                    final_title=case_page.resolved_title,
                    case_title=case_title,
                    redirect_status='existing',
                    message=f"Case-number page already exists: {case_page.resolved_title}",
                    timestamp=timestamp,
                )
            # Skip without conflict resolution
            return UploadResult(
                title=title,
                wenshu_id=wenshu_id,
                status='skipped',
                final_title=title,
                case_title=case_title,
                message="Page already exists",
                timestamp=timestamp,
            )
    
    # Page doesn't exist - create it
    case_header_result = _handle_existing_case_header_page(
        source_title=title,
        wenshu_id=wenshu_id,
        import_wikitext=wikitext,
        case_title=case_title,
        page_state=case_page,
        message=f"Case-number page already exists: {case_page.resolved_title if case_page else case_title}",
        timestamp=timestamp,
    )
    if case_header_result:
        return case_header_result

    try:
        save_page(
            title,
            wikitext,
            build_edit_summary(wenshu_id),
        )
        return _attach_case_redirect(UploadResult(
            title=title,
            wenshu_id=wenshu_id,
            status='uploaded',
            final_title=title,
            message="Created successfully",
            timestamp=timestamp,
        ), title, wikitext, existing_case_page=case_page)
    except Exception as e:
        # pywikibot handles maxlag automatically via config.maxlag
        return UploadResult(
            title=title,
            wenshu_id=wenshu_id,
            status='failed',
            final_title=title,
            case_title=case_title,
            message=f"Failed to save: {e}",
            timestamp=timestamp,
        )


def _process_prefetched_upload_batch(
    *,
    batch_docs: list[dict],
    uploaded_f,
    failed_f,
    skipped_f,
    overwritable_f,
    resolve_conflicts: bool,
    force_overwrite: bool,
    dry_run: bool = False,
    verbose: bool = False,
) -> Tuple[int, int, int, int, int]:
    """Process one upload chunk using batched read-side page queries."""
    uploaded_count = 0
    failed_count = 0
    skipped_count = 0
    resolved_count = 0
    overwritable_count = 0

    title_pages: dict[str, PageSnapshot] = {}
    case_pages = {}
    mutated_titles: set[str] = set()
    tracked_page_texts: dict[str, Optional[str]] = {}
    tracked_writes: list[_TrackedWikiWrite] = []
    track_writes = dry_run or verbose

    try:
        title_pages = fetch_page_content_batch(
            [doc["title"] for doc in batch_docs],
            batch_size=DEFAULT_UPLOAD_QUERY_BATCH_SIZE,
        )
    except Exception as exc:
        logging.warning("Falling back to per-title reads after batch title query failure: %s", exc)

    for requested_title, page in title_pages.items():
        tracked_page_texts[requested_title] = page.content if page.exists else None
        if page.canonical_title:
            tracked_page_texts[page.canonical_title] = page.content if page.exists else None

    case_titles = [doc["case_title"] for doc in batch_docs if doc.get("case_title")]
    if case_titles:
        try:
            case_pages = resolve_pages_batch(
                case_titles,
                batch_size=DEFAULT_UPLOAD_QUERY_BATCH_SIZE,
            )
        except Exception as exc:
            logging.warning("Falling back to per-title reads after batch case-title query failure: %s", exc)

    for requested_title, page in case_pages.items():
        tracked_page_texts[requested_title] = page.content if page.exists else None
        if page.resolved_title:
            tracked_page_texts[page.resolved_title] = page.content if page.exists else None

    original_save_page = save_page
    original_conflict_save_page = conflict_resolution_module.save_page
    original_conflict_move_page = conflict_resolution_module.move_page

    def get_before_text(title: str) -> Optional[str]:
        if title in tracked_page_texts:
            return tracked_page_texts[title]
        try:
            exists, content = get_page_content(title)
        except Exception:
            tracked_page_texts[title] = None
            return None
        tracked_page_texts[title] = content if exists else None
        return tracked_page_texts[title]

    def make_tracked_save(real_save_page):
        def tracked_save_page(title: str, content: str, summary: str, *args, **kwargs):
            before = get_before_text(title)
            tracked_writes.append(
                _TrackedWikiWrite(
                    title=title,
                    before=before,
                    after=content,
                    summary=summary,
                )
            )
            tracked_page_texts[title] = content
            if dry_run:
                return True
            return real_save_page(title, content, summary, *args, **kwargs)

        return tracked_save_page

    def tracked_move_page(
        from_title: str,
        to_title: str,
        reason: str = "",
        leave_redirect: bool = True,
        ignore_warnings: bool = False,
    ) -> bool:
        before_from = get_before_text(from_title)
        before_to = get_before_text(to_title)
        moved_text = before_from or ""
        tracked_writes.append(
            _TrackedWikiWrite(
                title=to_title,
                before=before_to,
                after=moved_text,
                summary=reason,
                action=f"move from {from_title}",
            )
        )
        tracked_page_texts[to_title] = moved_text
        if leave_redirect:
            tracked_page_texts[from_title] = build_case_redirect_text(to_title)
        else:
            tracked_page_texts[from_title] = None
        if dry_run:
            return True
        return original_conflict_move_page(
            from_title,
            to_title,
            reason=reason,
            leave_redirect=leave_redirect,
            ignore_warnings=ignore_warnings,
        )

    if track_writes:
        globals()["save_page"] = make_tracked_save(original_save_page)
        conflict_resolution_module.save_page = make_tracked_save(original_conflict_save_page)
        conflict_resolution_module.move_page = tracked_move_page

    try:
        for doc in batch_docs:
            existing_page = None if doc["title"] in mutated_titles else title_pages.get(doc["title"])
            case_page = None
            if doc.get("case_title"):
                case_page = None if doc["case_title"] in mutated_titles else case_pages.get(doc["case_title"])

            write_start = len(tracked_writes)
            result = upload_document(
                title=doc["title"],
                wenshu_id=doc["wenshu_id"],
                wikitext=doc["wikitext"],
                resolve_conflicts=resolve_conflicts,
                force_overwrite=force_overwrite,
                existing_page=existing_page,
                case_page=case_page,
            )
            doc_writes = tracked_writes[write_start:]

            result_dict = asdict(result)
            if dry_run:
                result_dict["dry_run"] = True

            if result.status in {'uploaded', 'reverted_overwrite'}:
                uploaded_f.write(json.dumps(result_dict, ensure_ascii=False) + '\n')
                uploaded_count += 1
            elif result.status == 'conflict_resolved':
                uploaded_f.write(json.dumps(result_dict, ensure_ascii=False) + '\n')
                resolved_count += 1
            elif result.status == 'skipped':
                skipped_f.write(json.dumps(result_dict, ensure_ascii=False) + '\n')
                skipped_count += 1
            elif result.status == 'overwritable':
                overwritable_f.write(json.dumps(result_dict, ensure_ascii=False) + '\n')
                overwritable_count += 1
            else:
                failed_f.write(json.dumps(result_dict, ensure_ascii=False) + '\n')
                failed_count += 1

            if verbose:
                _emit_verbose_result(
                    line_num=doc["line_num"],
                    result=result,
                    writes=doc_writes,
                    dry_run=dry_run,
                )

            if result.status in {'uploaded', 'conflict_resolved', 'reverted_overwrite'} or result.redirect_status in {'created', 'updated'}:
                for touched_title in (
                    doc["title"],
                    result.final_title,
                    result.case_title,
                ):
                    if touched_title:
                        mutated_titles.add(touched_title)
    finally:
        if track_writes:
            globals()["save_page"] = original_save_page
            conflict_resolution_module.save_page = original_conflict_save_page
            conflict_resolution_module.move_page = original_conflict_move_page

    return uploaded_count, failed_count, skipped_count, resolved_count, overwritable_count


def process_upload_batch(
    input_path: Path,
    uploaded_log: Path,
    failed_log: Path,
    skipped_log: Path,
    overwritable_log: Path,
    resolve_conflicts: bool = True,
    force_overwrite: bool = False,
    max_documents: Optional[int] = None,
    skip_lines: int = 0,
    dry_run: bool = False,
    verbose: bool = False,
) -> Tuple[int, int, int, int, int]:
    """
    Process a batch of documents for upload.

    Rate limiting is handled by pywikibot's built-in throttle.
    Configure via mediawiki.configure_throttle() before calling.

    Args:
        input_path: Path to converted JSONL file
        uploaded_log: Path to log successfully uploaded pages
        failed_log: Path to log failed uploads
        skipped_log: Path to log skipped pages
        overwritable_log: Path to log pages that could be overwritten (JSONL with wikitext)
        resolve_conflicts: Whether to attempt conflict resolution
        force_overwrite: Whether to force-overwrite existing pages
        max_documents: Maximum number of documents to process (None = all)
        skip_lines: Number of leading source lines to skip before processing
        dry_run: Simulate writes without editing the wiki
        verbose: Print per-document decisions and wikitext diffs

    Returns:
        Tuple of (uploaded_count, failed_count, skipped_count, resolved_count, overwritable_count)
    """
    uploaded_count = 0
    failed_count = 0
    skipped_count = 0
    resolved_count = 0
    overwritable_count = 0
    doc_num = 0

    # Get file size for progress bar
    file_size = input_path.stat().st_size
    bytes_read = 0

    # Suppress pywikibot's verbose output during upload
    logging.getLogger('pywiki').setLevel(logging.WARNING)
    logging.getLogger('pywikibot').setLevel(logging.WARNING)

    with open(input_path, 'r', encoding='utf-8') as infile, \
         open(uploaded_log, 'a', encoding='utf-8') as uploaded_f, \
         open(failed_log, 'a', encoding='utf-8') as failed_f, \
         open(skipped_log, 'a', encoding='utf-8') as skipped_f, \
         open(overwritable_log, 'a', encoding='utf-8') as overwritable_f:

        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            BarColumn(),
            TaskProgressColumn(),
            TextColumn("[magenta]({task.fields[line_num]:,})[/magenta]"),
            TextColumn("•"),
            TimeRemainingColumn(),
            console=console,
            transient=False,
        ) as progress:
            task_action = "Fast-forwarding" if skip_lines > 0 else "Uploading"
            task = progress.add_task(
                _build_progress_description(task_action, 0, 0, 0),
                total=file_size,
                line_num=0,
            )

            batch_docs: list[dict] = []
            lines_skipped = 0
            current_line_num = 0

            for source_line_num, line in enumerate(infile, start=1):
                current_line_num = source_line_num
                # Track bytes read for progress
                bytes_read += len(line.encode('utf-8'))

                # Skip leading lines before processing
                if lines_skipped < skip_lines:
                    lines_skipped += 1
                    progress.update(task, completed=bytes_read, line_num=current_line_num)
                    continue

                if task_action != "Uploading":
                    task_action = "Uploading"
                    progress.update(
                        task,
                        completed=bytes_read,
                        line_num=current_line_num,
                        description=_build_progress_description(task_action, uploaded_count + resolved_count, failed_count, skipped_count),
                    )

                line = line.strip()
                if not line:
                    progress.update(task, completed=bytes_read, line_num=current_line_num)
                    continue

                doc_num += 1

                # Check max documents limit
                if max_documents and doc_num > max_documents:
                    break
                
                try:
                    doc = json.loads(line)
                except json.JSONDecodeError as e:
                    # Log parse error
                    error_entry = {
                        "line_num": doc_num,
                        "error": f"JSON parse error: {e}",
                        "timestamp": utc_now_iso(),
                    }
                    failed_f.write(json.dumps(error_entry, ensure_ascii=False) + '\n')
                    failed_count += 1
                    progress.update(
                        task, 
                        completed=bytes_read,
                        line_num=current_line_num,
                        description=_build_progress_description(
                            task_action,
                            uploaded_count + resolved_count,
                            failed_count,
                            skipped_count,
                        ),
                    )
                    continue
                
                title = doc.get('title', '')
                wenshu_id = doc.get('wenshu_id', '') or doc.get('wenshuID', '')
                wikitext = doc.get('wikitext', '')
                # When overwriting from an overwritable JSONL, use final_title as the target
                if force_overwrite and doc.get('final_title'):
                    title = doc['final_title']
                
                if not title or not wikitext:
                    error_entry = {
                        "line_num": doc_num,
                        "title": title,
                        "wenshu_id": wenshu_id,
                        "error": "Missing title or wikitext",
                        "timestamp": utc_now_iso(),
                    }
                    failed_f.write(json.dumps(error_entry, ensure_ascii=False) + '\n')
                    failed_count += 1
                    progress.update(
                        task, 
                        completed=bytes_read,
                        line_num=current_line_num,
                        description=_build_progress_description(
                            task_action,
                            uploaded_count + resolved_count,
                            failed_count,
                            skipped_count,
                        ),
                    )
                    continue
                
                batch_docs.append(
                    {
                        "line_num": doc_num,
                        "title": title,
                        "wenshu_id": wenshu_id,
                        "wikitext": wikitext,
                        "case_title": build_case_title_from_content(wikitext),
                    }
                )

                if len(batch_docs) >= DEFAULT_UPLOAD_QUERY_BATCH_SIZE:
                    batch_uploaded, batch_failed, batch_skipped, batch_resolved, batch_overwritable = _process_prefetched_upload_batch(
                        batch_docs=batch_docs,
                        uploaded_f=uploaded_f,
                        failed_f=failed_f,
                        skipped_f=skipped_f,
                        overwritable_f=overwritable_f,
                        resolve_conflicts=resolve_conflicts,
                        force_overwrite=force_overwrite,
                        dry_run=dry_run,
                        verbose=verbose,
                    )
                    uploaded_count += batch_uploaded
                    failed_count += batch_failed
                    skipped_count += batch_skipped
                    resolved_count += batch_resolved
                    overwritable_count += batch_overwritable
                    batch_docs = []

                    progress.update(
                        task,
                        completed=bytes_read,
                        line_num=current_line_num,
                        description=_build_progress_description(
                            task_action,
                            uploaded_count + resolved_count,
                            failed_count,
                            skipped_count + overwritable_count,
                        ),
                    )

            if batch_docs:
                batch_uploaded, batch_failed, batch_skipped, batch_resolved, batch_overwritable = _process_prefetched_upload_batch(
                    batch_docs=batch_docs,
                    uploaded_f=uploaded_f,
                    failed_f=failed_f,
                    skipped_f=skipped_f,
                    overwritable_f=overwritable_f,
                    resolve_conflicts=resolve_conflicts,
                    force_overwrite=force_overwrite,
                    dry_run=dry_run,
                    verbose=verbose,
                )
                uploaded_count += batch_uploaded
                failed_count += batch_failed
                skipped_count += batch_skipped
                resolved_count += batch_resolved
                overwritable_count += batch_overwritable
            
            # Final update with green color
            progress.update(
                task,
                completed=file_size,
                line_num=current_line_num,
                description=f"[green]Complete: {uploaded_count + resolved_count} ✓, {failed_count} ✗, {skipped_count + overwritable_count} ⊘"
            )
    
    return uploaded_count, failed_count, skipped_count, resolved_count, overwritable_count
