"""
Streaming converter for court documents.

This module provides line-by-line JSONL processing with proper error handling
and failure logging.
"""

import json
import re
from dataclasses import dataclass, asdict
from datetime import datetime, timezone
from typing import Optional, Tuple, Generator, Dict, Any
from pathlib import Path

from .html_normalizer import normalize_html, remove_cjk_spaces
from .wikitext_renderer import render_wikitext


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


def extract_doc_type_from_s22(s22: str, court: str, doc_id: str) -> str:
    """
    Extract document type from s22 field by removing court and docID.
    
    s22 format: "Court\nDocType\nDocID"
    
    Args:
        s22: The s22 field value
        court: The court name (from parsed HTML or s2)
        doc_id: The document ID (s7)
        
    Returns:
        The extracted document type
    """
    if not s22:
        return ""
    
    lines = s22.strip().split('\n')
    
    # The document type is typically the second line
    if len(lines) >= 2:
        doc_type = lines[1].strip()
        # Clean up any extra whitespace
        doc_type = re.sub(r'\s+', '', doc_type)
        return doc_type
    
    return ""


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
    if not s22:
        return s2
    
    lines = s22.strip().split('\n')
    if lines:
        # The first line should be the full court name
        full_court = re.sub(r'\s+', '', lines[0])
        if full_court:
            return full_court
    
    return s2


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
        title = remove_cjk_spaces(raw_json.get('s1', '').strip())
        wenshu_id = raw_json.get('wsKey', '').strip()
        court_s2 = remove_cjk_spaces(raw_json.get('s2', '').strip())
        doc_id = raw_json.get('s7', '').strip()
        s22 = remove_cjk_spaces(raw_json.get('s22', '').strip())
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
        if doc.court_name:
            court = doc.court_name
        else:
            court = infer_court_with_province(court_s2, s22)
        
        # Clean up whitespace
        court = re.sub(r'\s+', '', court)
    except Exception as e:
        return None, ConversionError(
            error_stage="block_detect",
            error_message=f"Failed to determine court name: {e}",
            raw_json=raw_json,
            timestamp=timestamp,
        )
    
    # 4. Determine document type
    try:
        if doc.doc_type:
            doc_type = re.sub(r'\s+', '', doc.doc_type)
        else:
            doc_type = extract_doc_type_from_s22(s22, court, doc_id)
    except Exception as e:
        return None, ConversionError(
            error_stage="block_detect",
            error_message=f"Failed to determine document type: {e}",
            raw_json=raw_json,
            timestamp=timestamp,
        )
    
    # 5. Render wikitext
    try:
        wikitext = render_wikitext(doc, title)
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
        doc_id=doc_id,
        wikitext=wikitext,
    ), None


def iter_json_objects(infile):
    """
    Iterate over JSON objects from a file, supporting both:
    - JSONL format (one object per line)
    - Pretty-printed format (multi-line objects)
    
    Yields each parsed JSON object.
    """
    buffer = []
    
    for line in infile:
        stripped = line.strip()
        
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
) -> Tuple[int, int]:
    """
    Process a JSON file in streaming mode.
    
    Supports both JSONL (one object per line) and pretty-printed JSON.
    Reads and parses one object at a time, never loads entire file into memory.
    
    Args:
        input_path: Path to input JSON/JSONL file
        output_path: Path to output JSONL file (converted documents)
        error_path: Path to error JSONL file (failed conversions)
        
    Returns:
        Tuple of (success_count, error_count)
    """
    success_count = 0
    error_count = 0
    doc_num = 0
    
    with open(input_path, 'r', encoding='utf-8') as infile, \
         open(output_path, 'a', encoding='utf-8') as outfile, \
         open(error_path, 'a', encoding='utf-8') as errfile:
        
        for raw_json in iter_json_objects(infile):
            doc_num += 1
            
            # Convert document
            result, error = convert_document(raw_json)
            
            if result:
                outfile.write(json.dumps(asdict(result), ensure_ascii=False) + '\n')
                success_count += 1
            else:
                errfile.write(json.dumps(asdict(error), ensure_ascii=False) + '\n')
                error_count += 1
            
            # Progress logging every 1000 documents
            if doc_num % 1000 == 0:
                print(f"Processed {doc_num} documents: {success_count} success, {error_count} errors")
    
    return success_count, error_count
    
    return success_count, error_count


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
