"""
HTML Normalizer for court documents.

This module provides deterministic HTML-to-block-segmentation conversion:
- Parse HTML using BeautifulSoup
- Detect meaningful structural cues (center, right alignment, indentation)
- Extract clean text blocks preserving document structure
- Handle tables with merged cells
"""

import re
from dataclasses import dataclass, field
from enum import Enum, auto
from typing import List, Optional, Tuple
from bs4 import BeautifulSoup, NavigableString, Tag


# Regex pattern for CJK characters and common CJK punctuation
# This matches Chinese, Japanese characters and their punctuation
CJK_PATTERN = re.compile(
    r'[\u4e00-\u9fff'           # CJK Unified Ideographs
    r'\u3400-\u4dbf'            # CJK Unified Ideographs Extension A
    r'\U00020000-\U0002a6df'    # CJK Unified Ideographs Extension B
    r'\U0002a700-\U0002b73f'    # CJK Unified Ideographs Extension C
    r'\U0002b740-\U0002b81f'    # CJK Unified Ideographs Extension D
    r'\U0002b820-\U0002ceaf'    # CJK Unified Ideographs Extension E
    r'\U0002ceb0-\U0002ebef'    # CJK Unified Ideographs Extension F
    r'\U0002ebf0-\U0002ee5f'    # CJK Unified Ideographs Extension I
    r'\U00030000-\U0003134f'    # CJK Unified Ideographs Extension G
    r'\U00031350-\U000323af'    # CJK Unified Ideographs Extension H
    r'\U000323b0-\U0003347f'    # CJK Unified Ideographs Extension J
    r'\u2f00-\u2fdf'            # Kangxi Radicals
    r'\u2e80-\u2eff'            # CJK Radicals Supplement
    r'\uf900-\ufaff'            # CJK Compatibility Ideographs
    r'\U0002f800-\U0002fa1f'    # CJK Compatibility Ideographs Supplement
    r'\u3200-\u32ff'            # Enclosed CJK Letters and Months (㈠㈡㈢ etc.)
    r'\u3300-\u33ff'            # CJK Compatibility (㍻ ㎡ etc.)
    r'\u3000-\u303f'            # CJK Symbols and Punctuation (、。〃〈〉《》「」『』【】〒〓〔〕〖〗〘〙〚〛〜〝〞〟〰)
    r'\uff00-\uffef'            # Halfwidth and Fullwidth Forms (，．：；！？等全角标点)
    r'\ufe30-\ufe4f'            # CJK Compatibility Forms (︵︶︷︸ vertical brackets)
    r'\u2000-\u206f'            # General Punctuation
    r']'
)


def remove_cjk_spaces(text: str) -> str:
    """
    Remove spaces that appear between two CJK characters or punctuation.
    
    This cleans up OCR artifacts while preserving spaces between
    CJK and Latin/Arabic characters (e.g., "使用 Python 编程").
    
    Args:
        text: Input text possibly containing spurious spaces
        
    Returns:
        Text with spaces between CJK characters removed
    """
    if not text:
        return text
    
    result = []
    i = 0
    while i < len(text):
        if text[i] == ' ':
            # Check if space is between two CJK characters
            # Look back for the last non-space character
            prev_char = None
            for j in range(len(result) - 1, -1, -1):
                if result[j] != ' ':
                    prev_char = result[j]
                    break
            
            # Look ahead for the next non-space character
            next_char = None
            for j in range(i + 1, len(text)):
                if text[j] != ' ':
                    next_char = text[j]
                    break
            
            # Only keep space if NOT between two CJK characters
            if prev_char and next_char:
                if CJK_PATTERN.match(prev_char) and CJK_PATTERN.match(next_char):
                    # Skip this space (between CJK chars)
                    i += 1
                    continue
            
            result.append(text[i])
        else:
            result.append(text[i])
        i += 1
    
    return ''.join(result)


class BlockType(Enum):
    """Types of content blocks in a court document."""
    TITLE = auto()        # Court name - centered, larger font
    DOC_TYPE = auto()     # Document type (e.g., 执行裁定书) - centered
    DOC_ID = auto()       # Case number - right-aligned
    PARAGRAPH = auto()    # Normal paragraph - indented or default
    SIGNATURE = auto()    # Judge/clerk signatures - right-aligned
    DATE = auto()         # Date - right-aligned, contains 年月日
    TABLE = auto()        # Table content
    LIST = auto()         # List (ul/ol) content
    EMPTY = auto()        # Empty or whitespace-only block


@dataclass
class ContentBlock:
    """Represents a parsed content block from the HTML."""
    block_type: BlockType
    text: str
    raw_html: str = ""
    indent: bool = False  # Whether the block should be indented (text-indent)
    metadata: dict = field(default_factory=dict)


@dataclass
class ParsedDocument:
    """Represents a fully parsed court document."""
    court_name: str = ""
    doc_type: str = ""
    doc_id: str = ""
    body_blocks: List[ContentBlock] = field(default_factory=list)
    signature_blocks: List[ContentBlock] = field(default_factory=list)
    date_block: Optional[ContentBlock] = None
    further_notes: List[ContentBlock] = field(default_factory=list)


def clean_text(text: str) -> str:
    """
    Clean text content by normalizing whitespace and removing control characters.
    Preserves meaningful spaces but collapses multiple spaces.
    Also removes spaces between CJK characters (OCR artifacts).
    """
    if not text:
        return ""
    
    # Replace various whitespace characters with regular spaces
    text = re.sub(r'[\t\r\n\f\v]+', ' ', text)
    # Collapse multiple spaces into one
    text = re.sub(r'  +', ' ', text)
    # Strip leading/trailing whitespace
    text = text.strip()
    # Remove spaces between CJK characters (OCR artifacts)
    text = remove_cjk_spaces(text)
    
    return text


def get_text_align(element: Tag) -> Optional[str]:
    """
    Extract text-align style from an element.
    
    Checks both inline style and common alignment attributes.
    """
    style = element.get('style', '')
    
    # Check inline style for text-align
    align_match = re.search(r'text-align\s*:\s*(\w+)', style, re.IGNORECASE)
    if align_match:
        return align_match.group(1).lower()
    
    # Check align attribute (older HTML)
    align_attr = element.get('align', '').lower()
    if align_attr in ('left', 'center', 'right', 'justify'):
        return align_attr
    
    return None


def has_text_indent(element: Tag) -> bool:
    """
    Check if an element has text-indent style (paragraph indentation).
    """
    style = element.get('style', '')
    indent_match = re.search(r'text-indent\s*:\s*(\d+)', style, re.IGNORECASE)
    if indent_match:
        indent_value = int(indent_match.group(1))
        return indent_value > 0
    return False


def get_font_size(element: Tag) -> Optional[int]:
    """
    Extract font-size from style attribute.
    Returns the numeric value in pt or approximate pt from other units.
    """
    style = element.get('style', '')
    
    # Check for font-size in style
    size_match = re.search(r'font-size\s*:\s*(\d+)(?:pt|px)?', style, re.IGNORECASE)
    if size_match:
        return int(size_match.group(1))
    
    return None


def is_date_text(text: str) -> bool:
    """
    Check if text appears to be a Chinese date string.
    
    Examples:
        "二〇二四年九月二十八日" -> True
        "2024年9月28日" -> True
    """
    # Chinese numerals date pattern
    chinese_pattern = r'[〇一二三四五六七八九零]+年[一二三四五六七八九十]+月[一二三四五六七八九十]+日'
    # Arabic numerals date pattern
    arabic_pattern = r'\d{4}年\d{1,2}月\d{1,2}日'
    
    return bool(re.search(chinese_pattern, text)) or bool(re.search(arabic_pattern, text))


def is_signature_text(text: str) -> bool:
    """
    Check if text appears to be a judge/clerk signature line.
    
    Examples:
        "审判员　　章辉" -> True
        "书记员　　魏凯" -> True
        "审判长　阿勒木江吾米尔江" -> True
        "法官助理　孙雨薇" -> True
        "审 判 员　：杨清" -> True (spaced version)
        "代理审判员　李某" -> True (代理 prefix)
    """
    # Patterns match job titles with optional spaces and optional 代理 prefix
    # The regex handles both exact matches and spaced versions
    signature_patterns = [
        r'(?:代\s*理\s*)?审\s*判\s*长',
        r'(?:代\s*理\s*)?审\s*判\s*员',
        r'(?:代\s*理\s*)?人\s*民\s*陪\s*审\s*员',
        r'(?:代\s*理\s*)?书\s*记\s*员',
        r'(?:代\s*理\s*)?法\s*官\s*助\s*理',
        r'(?:代\s*理\s*)?执\s*行\s*员',
        r'(?:代\s*理\s*)?校\s*对\s*(?:责\s*任\s*)?人',
        r'(?:代\s*理\s*)?打\s*印\s*(?:责\s*任\s*)?人',
    ]
    
    cleaned = clean_text(text)
    
    for pattern in signature_patterns:
        if re.search(pattern, cleaned):
            return True
    
    return False


def parse_div_block(div: Tag) -> ContentBlock:
    """
    Parse a single div element into a ContentBlock.
    
    Determines block type based on styling and content.
    """
    text = clean_text(div.get_text())
    raw_html = str(div)
    
    # Empty block
    if not text:
        return ContentBlock(BlockType.EMPTY, "", raw_html)
    
    # Get styling information
    align = get_text_align(div)
    indent = has_text_indent(div)
    font_size = get_font_size(div)
    
    # Determine block type
    
    # Center-aligned with larger font -> Title or Doc Type
    if align == 'center':
        # Check if it looks like a court name (contains 人民法院 or 法院)
        if '人民法院' in text or text.endswith('法院'):
            return ContentBlock(BlockType.TITLE, text, raw_html)
        
        # Check for document type keywords
        if any(kw in text for kw in ['裁定书', '判决书', '决定书', '通知书', '调解书']):
            return ContentBlock(BlockType.DOC_TYPE, text, raw_html)
        
        # With larger font, likely title or doc type
        if font_size and font_size >= 18:
            # Default to doc type for other centered large text
            return ContentBlock(BlockType.DOC_TYPE, text, raw_html)
        
        # Other centered text - could be part of header
        return ContentBlock(BlockType.PARAGRAPH, text, raw_html, indent=False)
    
    # Right-aligned -> Doc ID, Signature, or Date
    if align == 'right':
        # Check if it's a case number (starts with （ or ( and contains 号)
        if (text.startswith('（') or text.startswith('(')) and '号' in text:
            return ContentBlock(BlockType.DOC_ID, text, raw_html)
        
        # Check if it's a date
        if is_date_text(text):
            return ContentBlock(BlockType.DATE, text, raw_html)
        
        # Check if it's a signature
        if is_signature_text(text):
            return ContentBlock(BlockType.SIGNATURE, text, raw_html)
        
        # Other right-aligned content (could be part of signature)
        return ContentBlock(BlockType.SIGNATURE, text, raw_html)
    
    # Default: Paragraph (check for indentation)
    return ContentBlock(BlockType.PARAGRAPH, text, raw_html, indent=indent)


def parse_table(table: Tag) -> ContentBlock:
    """
    Parse a table element into a ContentBlock with table metadata.
    
    Handles colspan and rowspan for merged cells.
    """
    rows = []
    
    for tr in table.find_all('tr'):
        cells = []
        for td in tr.find_all(['td', 'th']):
            cell_text = clean_text(td.get_text())
            colspan = int(td.get('colspan', 1))
            rowspan = int(td.get('rowspan', 1))
            cells.append({
                'text': cell_text,
                'colspan': colspan,
                'rowspan': rowspan
            })
        if cells:
            rows.append(cells)
    
    return ContentBlock(
        BlockType.TABLE,
        "",  # Text is in metadata
        str(table),
        metadata={'rows': rows}
    )


def parse_list(list_element: Tag) -> ContentBlock:
    """
    Parse a list element (ul/ol) into a ContentBlock with list metadata.
    
    Args:
        list_element: A <ul> or <ol> tag
        
    Returns:
        ContentBlock with LIST type and items in metadata
    """
    list_type = 'ordered' if list_element.name == 'ol' else 'unordered'
    items = []
    
    for li in list_element.find_all('li', recursive=False):
        item_text = clean_text(li.get_text())
        if item_text:
            items.append(item_text)
    
    return ContentBlock(
        BlockType.LIST,
        "",  # Text is in metadata
        str(list_element),
        metadata={'list_type': list_type, 'items': items}
    )


def normalize_html(html_content: str) -> ParsedDocument:
    """
    Parse and normalize HTML content from a court document.
    
    Args:
        html_content: Raw HTML string from qwContent field
        
    Returns:
        ParsedDocument with structured content blocks
    """
    soup = BeautifulSoup(html_content, 'lxml')
    
    # Find the body content
    body = soup.find('body') or soup
    
    # Initialize result
    doc = ParsedDocument()
    all_blocks: List[ContentBlock] = []
    
    # Process all top-level elements
    for element in body.children:
        if isinstance(element, NavigableString):
            text = clean_text(str(element))
            if text:
                all_blocks.append(ContentBlock(BlockType.PARAGRAPH, text, str(element)))
            continue
        
        if not isinstance(element, Tag):
            continue
        
        # Handle different element types
        if element.name == 'table':
            all_blocks.append(parse_table(element))
        elif element.name in ('ul', 'ol'):
            block = parse_list(element)
            if block.metadata.get('items'):
                all_blocks.append(block)
        elif element.name == 'div':
            # Check if div contains p tags - if so, process them individually
            p_tags = element.find_all('p', recursive=False)
            if p_tags:
                for p in p_tags:
                    block = parse_div_block(p)
                    if block.block_type != BlockType.EMPTY:
                        all_blocks.append(block)
            else:
                # Also check for nested divs containing p tags
                nested_p_tags = element.find_all('p')
                if nested_p_tags:
                    for p in nested_p_tags:
                        block = parse_div_block(p)
                        if block.block_type != BlockType.EMPTY:
                            all_blocks.append(block)
                else:
                    block = parse_div_block(element)
                    if block.block_type != BlockType.EMPTY:
                        all_blocks.append(block)
        elif element.name in ('p', 'span'):
            # Treat like div
            block = parse_div_block(element)
            if block.block_type != BlockType.EMPTY:
                all_blocks.append(block)
        # Skip other elements like script, style, etc.
    
    # Now categorize blocks into document sections
    # Header section: Title, Doc Type, Doc ID
    # Body section: Paragraphs until we hit right-aligned content after body
    # Signature section: Right-aligned content at the end
    # Further notes: Anything after signatures
    
    in_header = True
    in_body = False
    in_signature = False
    after_date = False
    
    body_start_idx = 0
    signature_start_idx = len(all_blocks)
    
    for i, block in enumerate(all_blocks):
        if in_header:
            if block.block_type == BlockType.TITLE:
                # Court name: no spaces should exist at all
                doc.court_name = block.text.replace(" ", "")
            elif block.block_type == BlockType.DOC_TYPE:
                # Doc type: no spaces should exist at all
                doc.doc_type = block.text.replace(" ", "")
            elif block.block_type == BlockType.DOC_ID:
                # Doc ID: no spaces should exist at all
                doc.doc_id = block.text.replace(" ", "")
                in_header = False
                in_body = True
                body_start_idx = i + 1
        elif in_body:
            # Check if we've hit the signature section
            # Signature section starts with right-aligned content (signatures) that isn't a doc ID
            if block.block_type in (BlockType.SIGNATURE, BlockType.DATE):
                # Look ahead to confirm this is actually the signature section
                # (not just an isolated right-aligned element in the body)
                remaining = all_blocks[i:]
                non_para_count = sum(1 for b in remaining if b.block_type != BlockType.PARAGRAPH)
                if non_para_count >= 2 or i >= len(all_blocks) - 5:
                    in_body = False
                    in_signature = True
                    signature_start_idx = i
    
    # Assign blocks to sections
    for i, block in enumerate(all_blocks):
        if block.block_type in (BlockType.TITLE, BlockType.DOC_TYPE, BlockType.DOC_ID):
            continue  # Already handled
        
        if i < signature_start_idx:
            doc.body_blocks.append(block)
        else:
            if block.block_type == BlockType.DATE and doc.date_block is None:
                doc.date_block = block
                # Also add to signature_blocks so date-based splitting works
                doc.signature_blocks.append(block)
                after_date = True
            elif block.block_type == BlockType.SIGNATURE or (block.block_type == BlockType.PARAGRAPH and not after_date):
                doc.signature_blocks.append(block)
            elif after_date:
                doc.further_notes.append(block)
    
    return doc


def extract_date_components(date_text: str) -> Tuple[Optional[str], Optional[str], Optional[str]]:
    """
    Extract year, month, day from a Chinese date string.
    
    Args:
        date_text: Date string like "二〇二四年九月二十八日" or "2024年9月28日"
        
    Returns:
        Tuple of (year, month, day) as strings, or None for missing components.
    """
    # Chinese numeral to Arabic mapping
    cn_to_num = {
        '〇': '0', '零': '0', '一': '1', '二': '2', '三': '3',
        '四': '4', '五': '5', '六': '6', '七': '7', '八': '8', '九': '9',
        '十': '10',
    }
    
    def cn_to_arabic(cn_str: str) -> str:
        """Convert Chinese numeral string to Arabic."""
        if not cn_str:
            return ""
        
        # Simple single-digit conversion
        result = ""
        for char in cn_str:
            if char in cn_to_num:
                result += cn_to_num[char]
            elif char.isdigit():
                result += char
        
        # Handle 十 (ten) specially for month/day
        if '十' in cn_str:
            if cn_str == '十':
                return '10'
            elif cn_str.startswith('十'):
                return '1' + cn_to_arabic(cn_str[1:])
            elif cn_str.endswith('十'):
                return cn_to_arabic(cn_str[:-1]) + '0'
            else:
                # Format like 二十八 -> 28
                parts = cn_str.split('十')
                tens = cn_to_arabic(parts[0]) if parts[0] else '1'
                ones = cn_to_arabic(parts[1]) if len(parts) > 1 and parts[1] else '0'
                return tens + ones
        
        return result
    
    year = month = day = None
    
    # Try Chinese pattern first
    cn_pattern = r'([〇零一二三四五六七八九]+)年([一二三四五六七八九十]+)月([一二三四五六七八九十]+)日'
    cn_match = re.search(cn_pattern, date_text)
    if cn_match:
        year = cn_to_arabic(cn_match.group(1))
        month = cn_to_arabic(cn_match.group(2))
        day = cn_to_arabic(cn_match.group(3))
        return year, month, day
    
    # Try Arabic pattern
    ar_pattern = r'(\d{4})年(\d{1,2})月(\d{1,2})日'
    ar_match = re.search(ar_pattern, date_text)
    if ar_match:
        year = ar_match.group(1)
        month = ar_match.group(2)
        day = ar_match.group(3)
        return year, month, day
    
    return None, None, None


def extract_judges_and_clerks(signature_blocks: List[ContentBlock], date_block: Optional[ContentBlock] = None) -> Tuple[List[str], List[str]]:
    """
    Extract judges and clerks from signature blocks.
    
    Uses the date as a splitter: judges appear before the date, clerks after.
    If no date is found, all signatures are treated as clerks.
    
    Args:
        signature_blocks: List of signature ContentBlocks
        date_block: The date block that separates judges from clerks
        
    Returns:
        Tuple of (judges_list, clerks_list)
    """
    judges = []
    clerks = []
    
    # Find the date position in signature blocks
    date_idx = None
    
    # Check if any signature block is actually a date
    for i, block in enumerate(signature_blocks):
        if is_date_text(block.text):
            date_idx = i
            break
    
    # If we have a date, split by it
    if date_idx is not None:
        # Everything before date -> judges
        for block in signature_blocks[:date_idx]:
            text = block.text.strip()
            if text and not is_date_text(text):
                judges.append(text)
        
        # Everything after date -> clerks
        for block in signature_blocks[date_idx + 1:]:
            text = block.text.strip()
            if text and not is_date_text(text):
                clerks.append(text)
    else:
        # No date found - treat everything as clerks
        for block in signature_blocks:
            text = block.text.strip()
            if text:
                clerks.append(text)
    
    return judges, clerks
