"""
Uploader for court documents to zhwikisource.

This module handles batch uploading with error handling and conflict resolution.
Rate limiting is handled by pywikibot's built-in throttle.
"""

import json
import logging
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
from .conflict_resolution import (
    is_conflict_resolvable,
    try_resolve_conflict,
    update_draft_for_conflict_resolution,
)
from .page_metadata import (
    build_case_redirect_summary,
    build_case_redirect_text,
    build_case_title_from_content,
    is_header_page,
    wikitexts_match,
)

console = Console()
DEFAULT_UPLOAD_QUERY_BATCH_SIZE = DEFAULT_READ_BATCH_SIZE


def utc_now_iso() -> str:
    """Return the current UTC timestamp in ISO8601-with-Z format."""
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


@dataclass
class UploadResult:
    """Result of uploading a single document."""
    title: str
    wenshu_id: str
    status: str  # 'uploaded', 'skipped', 'conflict_resolved', 'failed', 'overwritable'
    final_title: Optional[str] = None  # Different from title if conflict resolved
    case_title: Optional[str] = None
    redirect_status: Optional[str] = None
    message: str = ""
    timestamp: str = ""
    wikitext: Optional[str] = None  # Included for overwritable entries


def build_edit_summary(wenshu_id: str) -> str:
    """Build a standardized edit summary."""
    # return f"Imported from 裁判文书网 (credit: caseopen.org), source: https://wenshu.court.gov.cn/website/wenshu/181107ANFZ0BXSK4/index.html?docId={wenshu_id}"
    return "Imported from 裁判文书网 (credit: caseopen.org)"


def _append_message(message: str, extra: str) -> str:
    """Append a short suffix to an existing status message."""
    if not message:
        return extra
    return f"{message}; {extra}"


def _is_header_landing_page(page_state) -> bool:
    """Return whether a resolved page lands on a real court-document page."""
    return bool(page_state and page_state.exists and page_state.content and is_header_page(page_state.content))


def _same_case_document(existing_content: str, draft_content: str) -> bool:
    """Return whether two header pages resolve to the same canonical case title."""
    existing_case_title = build_case_title_from_content(existing_content)
    draft_case_title = build_case_title_from_content(draft_content)
    return bool(existing_case_title and existing_case_title == draft_case_title)


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

                    if is_header_page(existing_content) and _same_case_document(existing_content, wikitext):
                        return _attach_case_redirect(UploadResult(
                            title=title,
                            wenshu_id=wenshu_id,
                            status='skipped',
                            final_title=title,
                            message="Case number already exists at the original title",
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
                                    if _is_header_landing_page(new_page):
                                        return UploadResult(
                                            title=title,
                                            wenshu_id=wenshu_id,
                                            status='skipped',
                                            final_title=new_page.resolved_title,
                                            case_title=case_title or new_title,
                                            redirect_status='existing',
                                            message=f"Case-specific page already exists: {new_page.resolved_title}",
                                            timestamp=timestamp,
                                        )

                                    # Case-specific page exists but does not land on a real document page.
                                    return UploadResult(
                                        title=title,
                                        wenshu_id=wenshu_id,
                                        status='overwritable',
                                        final_title=new_title,
                                        case_title=case_title or new_title,
                                        message=f"Case-specific page already exists: {new_title}",
                                        timestamp=timestamp,
                                        wikitext=updated_wikitext,
                                    )
                            except Exception as e:
                                return UploadResult(
                                    title=title,
                                    wenshu_id=wenshu_id,
                                    status='failed',
                                    final_title=new_title,
                                    case_title=case_title or new_title,
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
                                    case_title=case_title or new_title,
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
                        # Not resolvable - mark as overwritable for manual review
                        return UploadResult(
                            title=title,
                            wenshu_id=wenshu_id,
                            status='overwritable',
                            final_title=title,
                            case_title=case_title,
                            message=f"Page exists, conflict not resolvable: {scenario}",
                            timestamp=timestamp,
                            wikitext=wikitext,
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

    try:
        title_pages = fetch_page_content_batch(
            [doc["title"] for doc in batch_docs],
            batch_size=DEFAULT_UPLOAD_QUERY_BATCH_SIZE,
        )
    except Exception as exc:
        logging.warning("Falling back to per-title reads after batch title query failure: %s", exc)

    case_titles = [doc["case_title"] for doc in batch_docs if doc.get("case_title")]
    if case_titles:
        try:
            case_pages = resolve_pages_batch(
                case_titles,
                batch_size=DEFAULT_UPLOAD_QUERY_BATCH_SIZE,
            )
        except Exception as exc:
            logging.warning("Falling back to per-title reads after batch case-title query failure: %s", exc)

    for doc in batch_docs:
        existing_page = None if doc["title"] in mutated_titles else title_pages.get(doc["title"])
        case_page = None
        if doc.get("case_title"):
            case_page = None if doc["case_title"] in mutated_titles else case_pages.get(doc["case_title"])

        result = upload_document(
            title=doc["title"],
            wenshu_id=doc["wenshu_id"],
            wikitext=doc["wikitext"],
            resolve_conflicts=resolve_conflicts,
            force_overwrite=force_overwrite,
            existing_page=existing_page,
            case_page=case_page,
        )

        result_dict = asdict(result)

        if result.status == 'uploaded':
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

        if result.status in {'uploaded', 'conflict_resolved'} or result.redirect_status in {'created', 'updated'}:
            for touched_title in (
                doc["title"],
                result.final_title,
                result.case_title,
            ):
                if touched_title:
                    mutated_titles.add(touched_title)

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
        max_documents: Maximum number of documents to process (None = all)
        skip_lines: Number of leading source lines to skip before processing

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
            TextColumn("•"),
            TextColumn("[magenta]line {task.fields[line_num]:,}[/magenta]"),
            TextColumn("•"),
            TimeRemainingColumn(),
            console=console,
            transient=False,
        ) as progress:
            task = progress.add_task(
                "[cyan]Uploading: 0 ✓, 0 ✗, 0 ⊘",
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
                        description=f"[cyan]Uploading: {uploaded_count + resolved_count} ✓, {failed_count} ✗, {skipped_count} ⊘"
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
                        description=f"[cyan]Uploading: {uploaded_count + resolved_count} ✓, {failed_count} ✗, {skipped_count} ⊘"
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
                        description=f"[cyan]Uploading: {uploaded_count + resolved_count} ✓, {failed_count} ✗, {skipped_count + overwritable_count} ⊘"
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
