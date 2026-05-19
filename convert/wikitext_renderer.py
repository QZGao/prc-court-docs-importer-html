"""
Wikitext Renderer for court documents.

This module converts ParsedDocument objects into zhwikisource-friendly wikitext.
"""

import re
from dataclasses import replace
from typing import Optional, Tuple
from .html_normalizer import (
    ParsedDocument,
    ContentBlock,
    BlockType,
    extract_date_components,
    strip_signature_leading_junk,
)
from .location import infer_location_from_court


def escape_wikitext(text: str) -> str:
    """
    Escape special wikitext characters in text content.
    
    Note: We preserve most characters but handle problematic ones.
    """
    # Replace pipe characters that could break templates
    # But only outside of already-escaped contexts
    # For now, we'll be conservative
    return text


def format_paragraph(block: ContentBlock, force_gap: bool = False) -> str:
    """
    Format a paragraph block as wikitext.
    
    Indented paragraphs use {{gap}} template (which provides its own spacing).
    """
    text = escape_wikitext(block.text)
    
    if block.indent or force_gap:
        return f"{{{{gap}}}}{text}"
    return text


def format_table(block: ContentBlock) -> str:
    """
    Format a table block as standard wikitext table syntax.
    """
    rows = block.metadata.get('rows', [])
    if not rows:
        return ""
    
    lines = ['{| class="wikitable"']
    
    for row in rows:
        lines.append('|-')
        for cell in row:
            text = escape_wikitext(cell['text'])
            colspan = cell.get('colspan', 1)
            rowspan = cell.get('rowspan', 1)
            
            # Build cell attributes
            attrs = []
            if colspan > 1:
                attrs.append(f'colspan="{colspan}"')
            if rowspan > 1:
                attrs.append(f'rowspan="{rowspan}"')
            
            if attrs:
                lines.append(f'| {" ".join(attrs)} | {text}')
            else:
                lines.append(f'| {text}')
    
    lines.append('|}')
    return '\n'.join(lines)


def format_list(block: ContentBlock) -> str:
    """
    Format a list block as wikitext list syntax.
    
    Uses * for unordered lists and # for ordered lists.
    """
    list_type = block.metadata.get('list_type', 'unordered')
    items = block.metadata.get('items', [])
    
    if not items:
        return ""
    
    marker = '#' if list_type == 'ordered' else '*'
    lines = [f'{marker} {escape_wikitext(item)}' for item in items]
    
    return '\n'.join(lines)


def render_header_template(
    title: str,
    court: str,
    doc_type: str,
    case_number: str,
    year: Optional[str],
    month: Optional[str],
    day: Optional[str],
    location: Optional[str],
    docid: Optional[str] = None,
) -> str:
    """
    Render the {{Header/裁判文书}} template for the document.
    """
    lines = [
        "{{Header/裁判文书",
        f"|title = {title}",
        f"|court = {court}",
        f"|type = {doc_type}",
        f"|案号 = {case_number}",
        f"|year = {year or ''}",
        f"|month = {month or ''}",
        f"|day = {day or ''}",
        f"|loc = {location or ''}",
        f"|docid = {docid or ''}",
        "}}",
    ]

    return '\n'.join(lines)


def render_title_section(court: str, doc_type: str) -> str:
    """
    Render the centered title section with court name and document type.
    """
    lines = [
        "",
        "<center><b>",
        f"{{{{larger|{court}}}}}",
        "",
        f"{{{{larger|{doc_type}}}}}",
        "</b></center>",
        "",
    ]
    return '\n'.join(lines)


def render_doc_id_section(doc_id: str) -> str:
    """
    Render the right-aligned document ID section.
    """
    return f'''<div align="right">
{doc_id}
</div>
'''


# Pattern for junk paragraphs to filter out
# Unicode hyphens/dashes: - (hyphen-minus), ‐ (hyphen), ‑ (non-breaking hyphen),
# – (en dash), — (em dash), ― (horizontal bar), － (fullwidth hyphen-minus)
DASH_CHARS = r'[\-\u2010\u2011\u2012\u2013\u2014\u2015\uff0d]'
JUNK_PARAGRAPH_PATTERN = re.compile(
    rf'^('
    rf'{DASH_CHARS}?\d+{DASH_CHARS}?'  # Page numbers like -1-, –2–, 1, 2, etc.
    rf'|\?'                             # Single question mark (OCR artifact)
    rf'|第\s*\d+\s*页'                  # "第X页" page markers
    rf')$'
)
SENTENCE_LIKE_END_RE = re.compile(r'[。！？；;：:，,、）)】］》”"\'0-9０-９]$')
CONTINUATION_START_RE = re.compile(r'^[\u4e00-\u9fff]')
SECTION_START_RE = re.compile(
    r'^(?:[一二三四五六七八九十百千万]+、|[0-9０-９]+[.．、]|[（(][一二三四五六七八九十0-9０-９]+[）)]|第[一二三四五六七八九十百千万0-9０-９]+[章节条款])'
)
PARTY_LABEL_RE = re.compile(
    r'^(?:'
    r'原告|被告|被告人|上诉人|被上诉人|申请人|被申请人|再审申请人|被执行人|申请执行人|'
    r'第三人|公诉机关|抗诉机关|法定代表人|法定代理人|负责人|经营者|'
    r'委托诉讼代理人|诉讼代理人|委托代理人|代理人|辩护人|指定辩护人|'
    r'(?:[一二三四五六七八九十两0-9０-９]+)?(?:原告|被告|上诉人|被上诉人|申请人|被申请人|第三人)?'
    r'(?:共同)?(?:委托诉讼代理人|诉讼代理人|委托代理人|代理人|辩护人)'
    r')[：:]'
)
BODY_COLON_HEADING_RE = re.compile(r'^[\u4e00-\u9fff][^。！？；；\n]{1,50}：$')
SIGNATURE_JOB_TITLES = [
    '人民陪审员',
    '校对责任人', '打印责任人',
    '法官助理',
    '审判长', '审判员',
    '书记员', '执行员',
    '校对人',
]


def is_junk_paragraph(text: str) -> bool:
    """
    Check if a paragraph is junk content that should be filtered out.
    
    Examples of junk:
        - Page numbers: "-1-", "-2-", "1", "2"
        - OCR artifacts: "?"
        - Page markers: "第1页"
    """
    return bool(JUNK_PARAGRAPH_PATTERN.match(text.strip()))


def should_merge_paragraph_continuation(previous: ContentBlock, current: ContentBlock) -> bool:
    """Return whether two parsed paragraphs are likely one OCR-split paragraph."""
    if previous.block_type != BlockType.PARAGRAPH or current.block_type != BlockType.PARAGRAPH:
        return False

    previous_text = previous.text.strip()
    current_text = current.text.strip()
    if len(previous_text) < 20 or not current_text:
        return False
    if SENTENCE_LIKE_END_RE.search(previous_text):
        return False
    if SECTION_START_RE.match(current_text):
        return False
    if PARTY_LABEL_RE.match(current_text):
        return False
    return bool(CONTINUATION_START_RE.match(current_text))


def merge_continuation_paragraphs(blocks: list) -> list:
    """Join obvious paragraph fragments caused by source line wrapping."""
    merged = []
    for block in blocks:
        if merged and should_merge_paragraph_continuation(merged[-1], block):
            previous = merged[-1]
            merged[-1] = replace(
                previous,
                text=f"{previous.text}{block.text}",
                indent=previous.indent or block.indent,
            )
            continue
        merged.append(block)
    return merged


def should_force_body_gap(block: ContentBlock) -> bool:
    """Return whether an unindented body paragraph still needs {{gap}}."""
    if block.block_type != BlockType.PARAGRAPH:
        return False
    return bool(BODY_COLON_HEADING_RE.match(block.text.strip()))


def render_body_paragraphs(blocks: list) -> str:
    """
    Render body paragraphs with proper spacing.
    Filters out junk paragraphs like page numbers and OCR artifacts.
    """
    result_lines = []
    
    for block in merge_continuation_paragraphs(blocks):
        if block.block_type == BlockType.TABLE:
            result_lines.append(format_table(block))
            result_lines.append("")  # Blank line after table
        elif block.block_type == BlockType.LIST:
            result_lines.append(format_list(block))
            result_lines.append("")  # Blank line after list
        else:
            # Filter out junk paragraphs
            if is_junk_paragraph(block.text):
                continue
            result_lines.append(format_paragraph(block, force_gap=should_force_body_gap(block)))
            result_lines.append("")  # Blank line between paragraphs
    
    return '\n'.join(result_lines)


def parse_signature_line(text: str) -> Tuple[Optional[str], Optional[str]]:
    """
    Parse a signature line into job title and name.
    
    Examples:
        "审判员　　章辉" -> ("审判员", "章辉")
        "审 判 员　：杨清" -> ("审判员", "杨清")
        "人民陪审员　刘瑞璐" -> ("人民陪审员", "刘瑞璐")
    
    Returns:
        Tuple of (job_title, name) with spaces normalized
    """
    job_titles = []
    for job in SIGNATURE_JOB_TITLES:
        job_titles.append(f'代理{job}')
        job_titles.append(job)
    
    # Clean up spaces within the text (normalize all kinds of spaces)
    # But preserve the structure
    cleaned = strip_signature_leading_junk(text)
    
    # Remove extra spaces and colons between job title and name
    # Pattern: job_title + spaces/colons + name
    for job in job_titles:
        # Create pattern that matches job with optional internal spaces
        spaced_job = r'\s*'.join(job)
        pattern = rf'^({spaced_job})\s*[：:\s]*(.+)$'
        match = re.match(pattern, cleaned, re.IGNORECASE)
        if match:
            # Return the canonical job title (without spaces) and the name
            name = match.group(2).strip()
            # Clean up spaces in name but preserve structure
            name = re.sub(r'\s+', '', name)  # Remove all spaces in name first
            return job, name
    
    return None, None


def parse_signature_entries(text: str) -> list[Tuple[str, str]]:
    """Parse one physical signature line, splitting merged role entries."""
    cleaned = strip_signature_leading_junk(text)
    compact = re.sub(r'\s+', '', cleaned).replace('：', ':')
    if not compact:
        return []

    job_titles = []
    for job in SIGNATURE_JOB_TITLES:
        job_titles.append(f'代理{job}')
        job_titles.append(job)
    job_pattern = "|".join(re.escape(job) for job in sorted(job_titles, key=len, reverse=True))
    matches = list(re.finditer(job_pattern, compact))
    if not matches or matches[0].start() != 0:
        return []

    entries: list[Tuple[str, str]] = []
    for index, match in enumerate(matches):
        name_start = match.end()
        name_end = matches[index + 1].start() if index + 1 < len(matches) else len(compact)
        name = compact[name_start:name_end].lstrip(':')
        if not name:
            return []
        entries.append((match.group(0), name))
    return entries


def render_signature_section(blocks: list) -> str:
    """
    Render the {{裁判文书署名}} section.

    Signature lines are reformatted as '职务：姓名' (fullwidth colon, no spaces).
    Date lines and unrecognised lines are passed through unchanged.
    """
    from .html_normalizer import is_date_text
    lines = ["{{裁判文书署名|1="]
    for block in blocks:
        text = strip_signature_leading_junk(block.text.strip())
        if not text:
            continue
        if is_date_text(text):
            lines.append(text)
        else:
            entries = parse_signature_entries(text)
            if entries:
                for job, name in entries:
                    lines.append(f"{job}：{name}")
            else:
                lines.append(text)
    lines.append("}}")
    return '\n'.join(lines)


def render_further_notes(blocks: list) -> str:
    """
    Render any content after the signature section.
    Filters out junk paragraphs like page numbers and OCR artifacts.
    """
    if not blocks:
        return ""
    
    result_lines = [""]
    for block in merge_continuation_paragraphs(blocks):
        if block.block_type == BlockType.TABLE:
            result_lines.append(format_table(block))
            result_lines.append("")
        else:
            # Filter out junk paragraphs
            if is_junk_paragraph(block.text):
                continue
            result_lines.append(format_paragraph(block, force_gap=should_force_body_gap(block)))
            result_lines.append("")
    
    return '\n'.join(result_lines)


def render_footer() -> str:
    """
    Render the footer with PD template.
    """
    return "{{PD-PRC-exempt}}\n"


def render_wikitext(
    doc: ParsedDocument,
    title: str,
    docid: Optional[str] = None,
    date_fallback: Optional[Tuple[Optional[str], Optional[str], Optional[str]]] = None,
) -> str:
    """
    Render a ParsedDocument as complete wikitext for zhwikisource.
    
    Args:
        doc: The parsed document structure
        title: The document title (usually from s1 field)
        
    Returns:
        Complete wikitext string ready for upload
    """
    # Extract date components, falling back to metadata when no parseable date
    # was found in the HTML.
    year, month, day = date_fallback or (None, None, None)
    if doc.date_block:
        parsed_year, parsed_month, parsed_day = extract_date_components(doc.date_block.text)
        year = parsed_year or year
        month = parsed_month or month
        day = parsed_day or day

    # Infer location from court name
    location = infer_location_from_court(doc.court_name)

    # Build the complete wikitext
    parts = []

    # 1. Header template
    parts.append(render_header_template(
        title=title,
        court=doc.court_name,
        doc_type=doc.doc_type,
        case_number=doc.doc_id,
        year=year,
        month=month,
        day=day,
        location=location,
        docid=docid,
    ))

    # 2. Body paragraphs
    parts.append(render_body_paragraphs(doc.body_blocks))

    # 3. Signature section
    parts.append(render_signature_section(doc.signature_blocks))
    
    # 6. Further notes (if any)
    if doc.further_notes:
        parts.append(render_further_notes(doc.further_notes))
    
    # 7. Footer
    parts.append(render_footer())
    
    return '\n'.join(parts)


def convert_html_to_wikitext(html_content: str, title: str) -> str:
    """
    High-level function to convert HTML content to wikitext.
    
    Args:
        html_content: Raw HTML from qwContent field
        title: Document title from s1 field
        
    Returns:
        Complete wikitext string
    """
    from .html_normalizer import normalize_html
    
    doc = normalize_html(html_content)
    return render_wikitext(doc, title)
