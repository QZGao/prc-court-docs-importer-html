"""
Wikitext Renderer for court documents.

This module converts ParsedDocument objects into zhwikisource-friendly wikitext.
"""

import re
from typing import Optional, Tuple
from .html_normalizer import (
    ParsedDocument,
    ContentBlock,
    BlockType,
    extract_date_components,
    extract_judges_and_clerks,
)
from .location import infer_location_from_court

# Special Unicode characters for formatting signatures
EN_QUAD = '\u3000'  # 宽空格，用于分隔字符和职务与名字之间
THREE_PER_EM = '\u2004'  # 窄空格，用于4字职务对齐


def escape_wikitext(text: str) -> str:
    """
    Escape special wikitext characters in text content.
    
    Note: We preserve most characters but handle problematic ones.
    """
    # Replace pipe characters that could break templates
    # But only outside of already-escaped contexts
    # For now, we'll be conservative
    return text


def format_paragraph(block: ContentBlock) -> str:
    """
    Format a paragraph block as wikitext.
    
    Indented paragraphs use {{gap}} template (which provides its own spacing).
    """
    text = escape_wikitext(block.text)
    
    if block.indent:
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


def render_header_template(
    title: str,
    court: str,
    doc_type: str,
    year: Optional[str],
    month: Optional[str],
    day: Optional[str],
    location: Optional[str],
) -> str:
    """
    Render the {{header}} template for the document.
    """
    # Build type field with "中华人民共和国" prefix
    type_field = f"中华人民共和国{doc_type}" if doc_type else ""
    
    lines = [
        "{{header",
        f"|title = {title}",
        f"|noauthor = {court}",
        f"|type = {type_field}",
        "|lawmaker = ",
        "|section = ",
        "|previous = ",
        "|next = ",
        f"|year = {year or ''}",
        f"|month = {month or ''}",
        f"|day = {day or ''}",
        f"|loc = {location or ''}",
        "|from = ",
        "|notes = ",
        "|edition = ",
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


def is_junk_paragraph(text: str) -> bool:
    """
    Check if a paragraph is junk content that should be filtered out.
    
    Examples of junk:
        - Page numbers: "-1-", "-2-", "1", "2"
        - OCR artifacts: "?"
        - Page markers: "第1页"
    """
    return bool(JUNK_PARAGRAPH_PATTERN.match(text.strip()))


def render_body_paragraphs(blocks: list) -> str:
    """
    Render body paragraphs with proper spacing.
    Filters out junk paragraphs like page numbers and OCR artifacts.
    """
    result_lines = []
    
    for block in blocks:
        if block.block_type == BlockType.TABLE:
            result_lines.append(format_table(block))
            result_lines.append("")  # Blank line after table
        else:
            # Filter out junk paragraphs
            if is_junk_paragraph(block.text):
                continue
            result_lines.append(format_paragraph(block))
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
    # Known job titles (longest first for greedy matching)
    # Include 代理 prefix variants
    base_jobs = [
        '人民陪审员',
        '校对责任人', '打印责任人',
        '法官助理',
        '审判长', '审判员',
        '书记员', '执行员',
        '校对人',
    ]
    
    job_titles = []
    for job in base_jobs:
        job_titles.append(f'代理{job}')
        job_titles.append(job)
    
    # Clean up spaces within the text (normalize all kinds of spaces)
    # But preserve the structure
    cleaned = text.strip()
    
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


def format_job_title(job: str) -> str:
    """
    Format a job title with proper spacing according to the convention.
    
    Rules:
        - 3字职务：逐字用 En Quad 隔开，例如：`审　判　长`
        - 4字职务：用 Three-Per-Em Space 隔开，例如：`法 官 助 理`
        - 5字职务：保持原样，例如：`人民陪审员`
    """
    char_count = len(job)
    
    if char_count == 3:
        # 3字职务：逐字用 En Quad 隔开
        formatted_base = EN_QUAD.join(job)
    elif char_count == 4:
        # 4字职务：用 Three-Per-Em Space 隔开
        formatted_base = THREE_PER_EM.join(job)
    else:
        # 5+字职务：保持原样
        formatted_base = job
    
    return formatted_base


def format_name(name: str) -> str:
    """
    Format a name with proper spacing according to the convention.
    
    Rules:
        - 2字名字：在中间插入一个 En Quad，例如：`王　杨`
        - 3字名字：保持原样，例如：`周海龙`
        - 4+字名字：保持原样，例如：`哈里木拉提`
    """
    if len(name) == 2:
        return EN_QUAD.join(name)
    return name


def format_signature(text: str) -> str:
    """
    Format a single signature line with proper spacing.
    
    Input: "审判员　　章辉" or "审 判 员：杨清"
    Output: "审　判　员　　章　辉" (with proper En Quad spacing)
    """
    job, name = parse_signature_line(text)
    
    if job and name:
        formatted_job = format_job_title(job)
        formatted_name = format_name(name)
        # Job and name separated by two En Quads
        return f'{formatted_job}{EN_QUAD}{EN_QUAD}{formatted_name}'
    
    # If parsing failed, return original text cleaned up
    return text


def render_signature_section(
    judges: list,
    court: str,
    date_text: Optional[str],
    clerks: list,
) -> str:
    """
    Render the {{署名}} section with judges, court seal, date, and clerks.
    
    Formatting follows the convention:
        - Job titles and names are separated by two En Quads
        - 3-char job titles have En Quad between each char
        - 4-char job titles have Three-Per-Em Space between each char
        - 5+ char job titles have no spacing
        - 2-char names have En Quad in the middle
        - 3+ char names have no spacing
    """
    lines = ["{{署名|"]
    
    # Add judges with proper formatting
    for judge in judges:
        lines.append(format_signature(judge))
        lines.append("")
    
    # Add court seal
    lines.append(f"{{{{印|{court}|center=国徽}}}}")
    lines.append("")
    
    # Add date
    if date_text:
        lines.append(date_text)
        lines.append("")
    
    # Add clerks with proper formatting
    for i, clerk in enumerate(clerks):
        lines.append(format_signature(clerk))
        if i < len(clerks) - 1:
            lines.append("")
    
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
    for block in blocks:
        if block.block_type == BlockType.TABLE:
            result_lines.append(format_table(block))
            result_lines.append("")
        else:
            # Filter out junk paragraphs
            if is_junk_paragraph(block.text):
                continue
            result_lines.append(format_paragraph(block))
            result_lines.append("")
    
    return '\n'.join(result_lines)


def render_footer() -> str:
    """
    Render the footer with PD template.
    """
    return "{{PD-PRC-exempt}}\n"


def render_wikitext(doc: ParsedDocument, title: str) -> str:
    """
    Render a ParsedDocument as complete wikitext for zhwikisource.
    
    Args:
        doc: The parsed document structure
        title: The document title (usually from s1 field)
        
    Returns:
        Complete wikitext string ready for upload
    """
    # Extract date components
    year, month, day = None, None, None
    date_text = ""
    if doc.date_block:
        date_text = doc.date_block.text
        year, month, day = extract_date_components(date_text)
    
    # Infer location from court name
    location = infer_location_from_court(doc.court_name)
    
    # Extract judges and clerks (using date as splitter)
    judges, clerks = extract_judges_and_clerks(doc.signature_blocks, doc.date_block)
    
    # Build the complete wikitext
    parts = []
    
    # 1. Header template
    parts.append(render_header_template(
        title=title,
        court=doc.court_name,
        doc_type=doc.doc_type,
        year=year,
        month=month,
        day=day,
        location=location,
    ))
    
    # 2. Title section (centered court name and doc type)
    parts.append(render_title_section(doc.court_name, doc.doc_type))
    
    # 3. Document ID (right-aligned)
    if doc.doc_id:
        parts.append(render_doc_id_section(doc.doc_id))
    
    # 4. Body paragraphs
    parts.append(render_body_paragraphs(doc.body_blocks))
    
    # 5. Signature section
    parts.append(render_signature_section(
        judges=judges,
        court=doc.court_name,
        date_text=date_text,
        clerks=clerks,
    ))
    
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
