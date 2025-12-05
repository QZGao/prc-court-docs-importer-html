"""
Uploader for court documents to zhwikisource.

This module handles batch uploading with error handling and conflict resolution.
Rate limiting is handled by pywikibot's built-in throttle.
"""

import json
import logging
from dataclasses import dataclass, asdict
from datetime import datetime
from pathlib import Path
from typing import Optional, Tuple

from rich.progress import Progress, BarColumn, TextColumn, TimeRemainingColumn, TaskProgressColumn, SpinnerColumn
from rich.console import Console
from rich.logging import RichHandler

from .mediawiki import (
    get_site,
    check_page_exists,
    get_page_content,
    save_page,
)
from .conflict_resolution import (
    is_conflict_resolvable,
    try_resolve_conflict,
    update_draft_for_conflict_resolution,
)

console = Console()


@dataclass
class UploadResult:
    """Result of uploading a single document."""
    title: str
    wenshu_id: str
    status: str  # 'uploaded', 'skipped', 'conflict_resolved', 'failed'
    final_title: Optional[str] = None  # Different from title if conflict resolved
    message: str = ""
    timestamp: str = ""


def build_edit_summary(wenshu_id: str) -> str:
    """Build a standardized edit summary."""
    return f"Imported from 裁判文书网 (credit: caseopen.org), source: https://wenshu.court.gov.cn/website/wenshu/181107ANFZ0BXSK4/index.html?docId={wenshu_id}"


def upload_document(
    title: str,
    wenshu_id: str,
    wikitext: str,
    resolve_conflicts: bool = True,
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
    timestamp = datetime.utcnow().isoformat() + "Z"
    
    # Check if page exists
    try:
        exists, page_id = check_page_exists(title)
    except Exception as e:
        return UploadResult(
            title=title,
            wenshu_id=wenshu_id,
            status='failed',
            message=f"Failed to check page existence: {e}",
            timestamp=timestamp,
        )
    
    if exists:
        # Page exists - check if we should try conflict resolution
        if resolve_conflicts:
            try:
                _, existing_content = get_page_content(title)
                if existing_content:
                    # Check if content is identical
                    if wikitext.strip() == existing_content.strip():
                        return UploadResult(
                            title=title,
                            wenshu_id=wenshu_id,
                            status='skipped',
                            message="Content identical to existing page",
                            timestamp=timestamp,
                        )
                    
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
                            
                            # Save the draft page at new title
                            # (pywikibot handles rate limiting automatically)
                            try:
                                save_page(
                                    new_title,
                                    updated_wikitext,
                                    build_edit_summary(wenshu_id),
                                )
                                return UploadResult(
                                    title=title,
                                    wenshu_id=wenshu_id,
                                    status='conflict_resolved',
                                    final_title=new_title,
                                    message=f"Resolved: {scenario}",
                                    timestamp=timestamp,
                                )
                            except Exception as e:
                                return UploadResult(
                                    title=title,
                                    wenshu_id=wenshu_id,
                                    status='failed',
                                    message=f"Failed to save after conflict resolution: {e}",
                                    timestamp=timestamp,
                                )
                        else:
                            return UploadResult(
                                title=title,
                                wenshu_id=wenshu_id,
                                status='failed',
                                message=f"Conflict resolution failed: {error}",
                                timestamp=timestamp,
                            )
                    else:
                        # Not resolvable - skip
                        return UploadResult(
                            title=title,
                            wenshu_id=wenshu_id,
                            status='skipped',
                            message=f"Page exists, conflict not resolvable: {scenario}",
                            timestamp=timestamp,
                        )
            except Exception as e:
                return UploadResult(
                    title=title,
                    wenshu_id=wenshu_id,
                    status='failed',
                    message=f"Error during conflict check: {e}",
                    timestamp=timestamp,
                )
        else:
            # Skip without conflict resolution
            return UploadResult(
                title=title,
                wenshu_id=wenshu_id,
                status='skipped',
                message="Page already exists",
                timestamp=timestamp,
            )
    
    # Page doesn't exist - create it
    try:
        save_page(
            title,
            wikitext,
            build_edit_summary(wenshu_id),
        )
        return UploadResult(
            title=title,
            wenshu_id=wenshu_id,
            status='uploaded',
            final_title=title,
            message="Created successfully",
            timestamp=timestamp,
        )
    except Exception as e:
        # pywikibot handles maxlag automatically via config.maxlag
        return UploadResult(
            title=title,
            wenshu_id=wenshu_id,
            status='failed',
            message=f"Failed to save: {e}",
            timestamp=timestamp,
        )


def process_upload_batch(
    input_path: Path,
    uploaded_log: Path,
    failed_log: Path,
    skipped_log: Path,
    resolve_conflicts: bool = True,
    max_documents: Optional[int] = None,
) -> Tuple[int, int, int, int]:
    """
    Process a batch of documents for upload.
    
    Rate limiting is handled by pywikibot's built-in throttle.
    Configure via mediawiki.configure_throttle() before calling.
    
    Args:
        input_path: Path to converted JSONL file
        uploaded_log: Path to log successfully uploaded pages
        failed_log: Path to log failed uploads
        skipped_log: Path to log skipped pages
        resolve_conflicts: Whether to attempt conflict resolution
        max_documents: Maximum number of documents to process (None = all)
        
    Returns:
        Tuple of (uploaded_count, failed_count, skipped_count, resolved_count)
    """
    uploaded_count = 0
    failed_count = 0
    skipped_count = 0
    resolved_count = 0
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
         open(skipped_log, 'a', encoding='utf-8') as skipped_f:
        
        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            BarColumn(),
            TaskProgressColumn(),
            TextColumn("•"),
            TimeRemainingColumn(),
            console=console,
            transient=False,
        ) as progress:
            task = progress.add_task(
                "[cyan]Uploading: 0 ✓, 0 ✗, 0 ⊘",
                total=file_size
            )
            
            for line in infile:
                # Track bytes read for progress
                bytes_read += len(line.encode('utf-8'))
                
                line = line.strip()
                if not line:
                    progress.update(task, completed=bytes_read)
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
                        "timestamp": datetime.utcnow().isoformat() + "Z",
                    }
                    failed_f.write(json.dumps(error_entry, ensure_ascii=False) + '\n')
                    failed_count += 1
                    progress.update(
                        task, 
                        completed=bytes_read,
                        description=f"[cyan]Uploading: {uploaded_count + resolved_count} ✓, {failed_count} ✗, {skipped_count} ⊘"
                    )
                    continue
                
                title = doc.get('title', '')
                wenshu_id = doc.get('wenshu_id', '') or doc.get('wenshuID', '')
                wikitext = doc.get('wikitext', '')
                
                if not title or not wikitext:
                    error_entry = {
                        "line_num": doc_num,
                        "title": title,
                        "wenshu_id": wenshu_id,
                        "error": "Missing title or wikitext",
                        "timestamp": datetime.utcnow().isoformat() + "Z",
                    }
                    failed_f.write(json.dumps(error_entry, ensure_ascii=False) + '\n')
                    failed_count += 1
                    progress.update(
                        task, 
                        completed=bytes_read,
                        description=f"[cyan]Uploading: {uploaded_count + resolved_count} ✓, {failed_count} ✗, {skipped_count} ⊘"
                    )
                    continue
                
                # Upload the document
                result = upload_document(
                    title=title,
                    wenshu_id=wenshu_id,
                    wikitext=wikitext,
                    resolve_conflicts=resolve_conflicts,
                )
                
                # Log result
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
                else:  # failed
                    failed_f.write(json.dumps(result_dict, ensure_ascii=False) + '\n')
                    failed_count += 1
                
                # Update progress bar
                progress.update(
                    task, 
                    completed=bytes_read,
                    description=f"[cyan]Uploading: {uploaded_count + resolved_count} ✓, {failed_count} ✗, {skipped_count} ⊘"
                )
            
            # Final update with green color
            progress.update(
                task,
                completed=file_size,
                description=f"[green]Complete: {uploaded_count + resolved_count} ✓, {failed_count} ✗, {skipped_count} ⊘"
            )
    
    return uploaded_count, failed_count, skipped_count, resolved_count
