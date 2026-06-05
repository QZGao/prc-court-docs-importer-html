"""Helpers for court-document date metadata."""

from datetime import date
import re
from typing import Optional, Tuple


DateComponents = Tuple[Optional[str], Optional[str], Optional[str]]
MIN_PRC_YEAR = 1949
CASE_NUMBER_YEAR_RE = re.compile(r'[（(]\s*(\d{4})\s*[）)]')


def is_valid_prc_year(year: Optional[str]) -> bool:
    """Return True when year is a usable PRC-era year."""
    if not year:
        return False

    try:
        year_number = int(year)
    except (TypeError, ValueError):
        return False

    return MIN_PRC_YEAR <= year_number <= date.today().year


def coerce_valid_prc_date_components(components: DateComponents) -> DateComponents:
    """Blank a date candidate when its year is not valid for PRC documents."""
    year, month, day = components
    if is_valid_prc_year(year):
        return year, month, day
    return None, None, None


def extract_year_from_case_number(case_number: str) -> Optional[str]:
    """Extract a valid PRC-era year from the parenthesized case number prefix."""
    match = CASE_NUMBER_YEAR_RE.search(case_number or "")
    if not match:
        return None

    year = match.group(1)
    return year if is_valid_prc_year(year) else None
