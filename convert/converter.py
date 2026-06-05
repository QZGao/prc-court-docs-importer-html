"""
Streaming converter for court documents.

This module provides line-by-line JSONL processing with proper error handling
and failure logging.
"""

import json
import re
from dataclasses import dataclass, asdict
from datetime import datetime, timezone
from typing import Optional, Tuple, Generator, Dict, Any, Callable
from pathlib import Path

from rich.progress import Progress, BarColumn, TextColumn, TimeRemainingColumn, TaskProgressColumn, SpinnerColumn
from rich.console import Console

from .html_normalizer import (
    normalize_html,
    normalize_case_number_parentheses,
    normalize_redaction_markers,
    normalize_title_redaction_markers,
    remove_cjk_spaces,
    remove_unicode_other_chars,
)
from .date_metadata import coerce_valid_prc_date_components
from .wikitext_renderer import render_wikitext

console = Console()
LEADING_JUNK_RE = re.compile(r'^[^\u4e00-\u9fff]+')
CASE_NUMBER_RE = re.compile(r'（.*?号(?:之[一二三四五六七八九十百千万〇零]+)?')
NON_CJK_RE = re.compile(r'[^\u4e00-\u9fff]+')
DOC_TYPE_RE = re.compile(r'[\u4e00-\u9fff]*(?:裁定书|判决书|决定书|通知书|调解书|裁决书|支付令)')
COURT_COMMITTEE_SUFFIX_RE = re.compile(r'^[\u4e00-\u9fff]{1,12}委员会')
TITLE_DOC_TYPE_SUFFIXES = (
    '不予受理支付令申请通知书',
    '不予暂予监外执行决定书',
    '刑事附带民事公益诉讼判决书',
    '终结本次执行程序执行裁定书',
    '财产保全告知事项通知书',
    '申请执行案件结案通知书',
    '刑事附带民事判决书',
    '刑事附带民事裁定书',
    '强制清算与破产裁定书',
    '执行案件结案通知书',
    '执行案件执行裁定书',
    '执行保全结案通知书',
    '诉讼保全结案通知书',
    '保全案件结案通知书',
    '案件执行结束通知书',
    '执行案件完毕通知书',
    '执行案件受理通知书',
    '暂予监外执行决定书',
    '非诉保全审查裁定书',
    '非诉行政执行裁定书',
    '国家司法救助决定书',
    '国家赔偿决定书',
    '行政赔偿判决书',
    '行政赔偿裁定书',
    '行政执行裁定书',
    '保全结案通知书',
    '保全情况通知书',
    '执行结案通知书',
    '执行完毕通知书',
    '终结结案通知书',
    '受理案件通知书',
    '协助执行通知书',
    '恢复执行通知书',
    '指定管辖决定书',
    '执行协调决定书',
    '司法救助决定书',
    '收监执行决定书',
    '民事判决书',
    '刑事判决书',
    '行政判决书',
    '民事裁定书',
    '刑事裁定书',
    '行政裁定书',
    '执行裁定书',
    '民事调解书',
    '刑事调解书',
    '行政调解书',
    '民事决定书',
    '刑事决定书',
    '执行决定书',
    '拘留决定书',
    '罚款决定书',
    '复议决定书',
    '再审决定书',
    '执行通知书',
    '结案通知书',
    '受理通知书',
    '应诉通知书',
    '执结通知书',
    '销案通知书',
    '民事支付令',
    '民事令',
    '刑事令',
    '行政令',
    '支付令',
    '判决书',
    '裁定书',
    '决定书',
    '通知书',
    '调解书',
    '案件移送函',
    '保全复函',
)


class ConversionInterrupted(KeyboardInterrupt):
    """Raised when a conversion run is interrupted after partial progress."""

    def __init__(self, success_count: int, error_count: int, skipped_count: int, last_doc_num: int):
        super().__init__("Conversion interrupted")
        self.success_count = success_count
        self.error_count = error_count
        self.skipped_count = skipped_count
        self.last_doc_num = last_doc_num


@dataclass
class ConversionResult:
    """Result of converting a single document."""
    title: str
    wenshu_id: str
    court: str
    doc_type: str
    doc_id: str
    wikitext: str


@dataclass
class ConversionError:
    """Error information for a failed conversion."""
    error_stage: str
    error_message: str
    raw_json: dict
    timestamp: str


def _compact_metadata_text(text: str) -> str:
    """Remove metadata whitespace after prior Unicode cleanup."""
    if not text:
        return ""

    return re.sub(r'\s+', '', text)


def _remove_once(text: str, fragment: str) -> str:
    """Remove one exact metadata fragment from text when present."""
    if not text or not fragment:
        return text

    return text.replace(fragment, '', 1)


def normalize_court_name(court: str) -> str:
    """Strip junk around a court name and allow immediate 法院...委员会 suffixes."""
    court = _compact_metadata_text(court)
    if not court:
        return ""

    court = LEADING_JUNK_RE.sub('', court)
    court_end = court.rfind('法院')
    if court_end == -1:
        return ""

    end = court_end + len('法院')
    committee_match = COURT_COMMITTEE_SUFFIX_RE.match(court[end:])
    if committee_match:
        end += committee_match.end()

    return court[:end]


def normalize_case_number_value(case_number: str) -> str:
    """Extract a case number span beginning with （ and ending with 号."""
    case_number = _compact_metadata_text(case_number)
    if not case_number:
        return ""

    case_number = normalize_case_number_parentheses(case_number)
    match = CASE_NUMBER_RE.search(case_number)
    if not match:
        return ""

    return match.group(0)


def normalize_doc_type(doc_type: str) -> str:
    """Strip document type metadata to CJK characters only."""
    doc_type = _compact_metadata_text(doc_type)
    if not doc_type:
        return ""

    doc_type = NON_CJK_RE.sub('', doc_type)
    match = DOC_TYPE_RE.search(doc_type)
    if match:
        return match.group(0)

    return doc_type


def extract_case_number_from_s22(s22: str) -> str:
    """Extract a normalized case number from the s22 fallback field."""
    s22 = normalize_case_number_parentheses(_compact_metadata_text(s22))
    if not s22:
        return ""

    return normalize_case_number_value(s22)


def extract_doc_type_from_s22(s22: str, court: str, doc_id: str) -> str:
    """
    Extract document type from s22 field by removing court and docID.
    
    s22 often looks like "Court\nDocType\nDocID", but may omit the type or
    move fields. Remove the already-deduced court and case number first, then
    normalize whatever remains as the type.
    
    Args:
        s22: The s22 field value
        court: The court name (from parsed HTML or s2)
        doc_id: The document ID (s7)
        
    Returns:
        The extracted document type
    """
    remainder = normalize_case_number_parentheses(_compact_metadata_text(s22))
    if not remainder:
        return ""

    remainder = _remove_once(remainder, normalize_court_name(remainder))
    remainder = _remove_once(remainder, normalize_court_name(court))
    remainder = CASE_NUMBER_RE.sub('', remainder, count=1)
    remainder = _remove_once(remainder, normalize_case_number_value(doc_id))

    return normalize_doc_type(remainder)


def extract_doc_type_from_title(title: str) -> str:
    """Extract a document type from known type suffixes in the title."""
    title = _compact_metadata_text(title)
    if not title:
        return ""

    for doc_type in TITLE_DOC_TYPE_SUFFIXES:
        if title.endswith(doc_type):
            return doc_type

    return ""


def extract_date_components_from_s31(s31: str) -> Tuple[Optional[str], Optional[str], Optional[str]]:
    """
    Extract year, month, and day from the s31 metadata field.

    s31 is expected to be an ISO-like date string: "yyyy-mm-dd".
    """
    if not s31:
        return None, None, None

    match = re.fullmatch(r'\s*(\d{4})-(\d{1,2})-(\d{1,2})\s*', s31)
    if not match:
        return None, None, None

    year, month, day = match.groups()
    return coerce_valid_prc_date_components((year, str(int(month)), str(int(day))))


def infer_court_with_province(s2: str, s22: str) -> str:
    """
    Infer the full court name with province from s2 and s22.
    
    s2 may omit the province prefix, but s22 should have the full court name.
    
    Args:
        s2: Court name (possibly without province)
        s22: Full hierarchy string containing court with province
        
    Returns:
        Court name with province prefix
    """
    full_court = normalize_court_name(s22)
    if full_court:
        return full_court
    
    return normalize_court_name(s2)


def convert_document(raw_json: dict) -> Tuple[Optional[ConversionResult], Optional[ConversionError]]:
    """
    Convert a single document from raw JSON to wikitext.
    
    Args:
        raw_json: The parsed JSON object from a JSONL line
        
    Returns:
        Tuple of (result, error) - one will be None
    """
    timestamp = datetime.now(timezone.utc).isoformat()
    
    try:
        # 1. Extract required fields
        title = normalize_title_redaction_markers(
            remove_cjk_spaces(remove_unicode_other_chars(raw_json.get('s1', '').strip()))
        )
        # Normalize middle dot variants to standard middle dot (·)
        title = re.sub(r'[．‧•･・]', '·', title)
        wenshu_id = remove_unicode_other_chars(raw_json.get('wsKey', '').strip())
        court_s2 = normalize_redaction_markers(
            remove_cjk_spaces(remove_unicode_other_chars(raw_json.get('s2', '').strip()))
        )
        doc_id = normalize_case_number_value(
            normalize_redaction_markers(remove_unicode_other_chars(raw_json.get('s7', '').strip()))
        )
        s22 = normalize_redaction_markers(
            remove_cjk_spaces(remove_unicode_other_chars(raw_json.get('s22', '').strip()))
        )
        s31 = remove_unicode_other_chars(raw_json.get('s31', '').strip())
        html_content = raw_json.get('qwContent', '')
        
        if not title:
            return None, ConversionError(
                error_stage="field_extraction",
                error_message="Missing title (s1 field)",
                raw_json=raw_json,
                timestamp=timestamp,
            )
        
        if not html_content:
            return None, ConversionError(
                error_stage="field_extraction",
                error_message="Missing HTML content (qwContent field)",
                raw_json=raw_json,
                timestamp=timestamp,
            )
        
    except Exception as e:
        return None, ConversionError(
            error_stage="field_extraction",
            error_message=str(e),
            raw_json=raw_json,
            timestamp=timestamp,
        )
    
    # 2. Parse HTML
    try:
        doc = normalize_html(html_content)
    except Exception as e:
        return None, ConversionError(
            error_stage="html_parse",
            error_message=str(e),
            raw_json=raw_json,
            timestamp=timestamp,
        )
    
    # 3. Determine court name (prefer parsed value, fallback to s22/s2)
    try:
        court = normalize_court_name(doc.court_name)
        if not court:
            court = infer_court_with_province(court_s2, s22)
    except Exception as e:
        return None, ConversionError(
            error_stage="block_detect",
            error_message=f"Failed to determine court name: {e}",
            raw_json=raw_json,
            timestamp=timestamp,
        )
    
    # 4. Determine case number and date metadata fallbacks
    try:
        case_number = normalize_case_number_value(doc.doc_id)
        if not case_number:
            case_number = doc_id
        if not case_number:
            case_number = extract_case_number_from_s22(s22)

        fallback_date = extract_date_components_from_s31(s31)
    except Exception as e:
        return None, ConversionError(
            error_stage="block_detect",
            error_message=f"Failed to determine case/date metadata: {e}",
            raw_json=raw_json,
            timestamp=timestamp,
        )

    # 5. Determine document type after court and case number are known
    try:
        doc_type = normalize_doc_type(doc.doc_type)
        if not doc_type:
            doc_type = extract_doc_type_from_s22(s22, court, case_number)
        if not doc_type:
            doc_type = extract_doc_type_from_title(title)
    except Exception as e:
        return None, ConversionError(
            error_stage="block_detect",
            error_message=f"Failed to determine document type: {e}",
            raw_json=raw_json,
            timestamp=timestamp,
        )

    # Keep rendered metadata aligned with the cleaned converter result so
    # upload-side case-title redirects and conflict resolution see the same court.
    doc.court_name = court
    doc.doc_type = doc_type
    doc.doc_id = case_number
    
    # 6. Render wikitext
    try:
        wikitext = render_wikitext(
            doc,
            title,
            docid=wenshu_id,
            date_fallback=fallback_date,
        )
    except Exception as e:
        return None, ConversionError(
            error_stage="render",
            error_message=str(e),
            raw_json=raw_json,
            timestamp=timestamp,
        )
    
    # Success
    return ConversionResult(
        title=title,
        wenshu_id=wenshu_id,
        court=court,
        doc_type=doc_type,
        doc_id=case_number,
        wikitext=wikitext,
    ), None


def iter_json_objects(infile, progress_callback: Optional[Callable[[int], None]] = None):
    """
    Iterate over JSON objects from a file, supporting both:
    - JSONL format (one object per line)
    - Pretty-printed format (multi-line objects)
    
    Args:
        infile: File object to read from
        progress_callback: Optional callback called with bytes read after each object
    
    Yields each parsed JSON object.
    """
    buffer = []
    bytes_read = 0
    
    for line in infile:
        stripped = line.strip()
        bytes_read += len(line.encode('utf-8'))
        
        if not stripped:
            continue
        
        buffer.append(line)
        
        # A single-line '}' indicates end of a pretty-printed object
        # Or if the line itself is a complete JSON object (JSONL format)
        if stripped == '}' or (stripped.startswith('{') and stripped.endswith('}')):
            # Try to parse what we have
            chunk = ''.join(buffer)
            try:
                obj = json.loads(chunk)
                yield obj
                buffer = []
                if progress_callback:
                    progress_callback(bytes_read)
            except json.JSONDecodeError:
                # Not complete yet, keep buffering (for nested structures)
                if stripped == '}':
                    # Try harder - might be end of object
                    pass
                continue


def process_jsonl_stream(
    input_path: Path,
    output_path: Path,
    error_path: Path,
    doc_filter: Optional[Callable] = None,
    start_from: int = 0,
    max_success: Optional[int] = None,
    original_path: Optional[Path] = None,
    append_output: bool = False,
) -> Tuple[int, int, int, int]:
    """
    Process a JSON file in streaming mode.
    
    Supports both JSONL (one object per line) and pretty-printed JSON.
    Reads and parses one object at a time, never loads entire file into memory.
    
    Args:
        input_path: Path to input JSON/JSONL file
        output_path: Path to output JSONL file (converted documents)
        error_path: Path to error JSONL file (failed conversions)
        doc_filter: Optional filter function that takes raw_json and returns True to process
        start_from: Skip documents until this document number (1-indexed)
        max_success: Stop after this many successful conversions (None = no limit)
        original_path: Optional path to save original JSON for processed documents
        append_output: Append to existing output files instead of overwriting them.
            Intended for checkpoint-based resume runs.
        
    Returns:
        Tuple of (success_count, error_count, skipped_count, last_doc_num)
    """
    success_count = 0
    error_count = 0
    skipped_count = 0
    doc_num = 0
    
    # Get file size for progress bar
    file_size = input_path.stat().st_size
    
    # Open original file if requested
    orig_file = None
    file_mode = 'a' if append_output else 'w'
    if original_path:
        orig_file = open(original_path, file_mode, encoding='utf-8')

    try:
        try:
            with open(input_path, 'r', encoding='utf-8') as infile, \
                 open(output_path, file_mode, encoding='utf-8') as outfile, \
                 open(error_path, file_mode, encoding='utf-8') as errfile:

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
                        f"[cyan]0 docs: 0 success, 0 errors, 0 skipped",
                        total=file_size
                    )

                    current_bytes = 0

                    def update_bytes(bytes_read: int):
                        nonlocal current_bytes
                        current_bytes = bytes_read

                    for raw_json in iter_json_objects(infile, update_bytes):
                        doc_num += 1

                        # Update progress bar
                        progress.update(
                            task,
                            completed=current_bytes,
                            description=f"[cyan]{doc_num} docs: {success_count} success, {error_count} errors, {skipped_count} skipped"
                        )

                        # Skip documents if resuming
                        if doc_num <= start_from:
                            continue

                        # Apply filter if provided
                        if doc_filter and not doc_filter(raw_json):
                            skipped_count += 1
                            continue

                        # Save original JSON if requested
                        if orig_file:
                            orig_file.write(json.dumps(raw_json, ensure_ascii=False) + '\n')

                        # Convert document
                        result, error = convert_document(raw_json)

                        if result:
                            outfile.write(json.dumps(asdict(result), ensure_ascii=False) + '\n')
                            success_count += 1
                        else:
                            errfile.write(json.dumps(asdict(error), ensure_ascii=False) + '\n')
                            error_count += 1

                        # Check if we've reached the limit
                        if max_success and success_count >= max_success:
                            console.print(f"\n[yellow]Reached limit of {max_success} successful conversions.[/yellow]")
                            break

                    # Final update
                    progress.update(
                        task,
                        completed=file_size,
                        description=f"[green]{doc_num} docs: {success_count} success, {error_count} errors, {skipped_count} skipped"
                    )
        except KeyboardInterrupt as exc:
            raise ConversionInterrupted(success_count, error_count, skipped_count, doc_num) from exc
    finally:
        if orig_file:
            orig_file.close()
    
    return success_count, error_count, skipped_count, doc_num


def convert_single(raw_json: dict) -> dict:
    """
    Convert a single document and return as dict.
    
    Convenience function for testing and single-document conversion.
    
    Args:
        raw_json: The parsed JSON object
        
    Returns:
        Dict with conversion result or error info
    """
    result, error = convert_document(raw_json)
    
    if result:
        return {
            "success": True,
            "data": asdict(result)
        }
    else:
        return {
            "success": False,
            "error": asdict(error)
        }
