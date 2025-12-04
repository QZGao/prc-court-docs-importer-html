"""
MediaWiki API client for zhwikisource.

This module provides wrappers around pywikibot for page operations.
"""

import time
from typing import Optional, Tuple, Callable, Any
import pywikibot
from pywikibot import Site, Page


# Site configuration
SITE_CODE = 'zh'
SITE_FAMILY = 'wikisource'

# Default rate limits
DEFAULT_EDIT_INTERVAL = 3.0  # seconds between edits
DEFAULT_MAXLAG = 5


_site: Optional[Site] = None


def get_site() -> Site:
    """
    Get or create the pywikibot Site instance.
    
    Returns:
        The configured Site for zhwikisource
    """
    global _site
    if _site is None:
        _site = pywikibot.Site(SITE_CODE, SITE_FAMILY)
        _site.login()
    return _site


def check_page_exists(title: str) -> Tuple[bool, Optional[int]]:
    """
    Check if a page exists on the wiki.
    
    Args:
        title: The page title to check
        
    Returns:
        Tuple of (exists, page_id) where page_id is None if page doesn't exist
    """
    site = get_site()
    page = Page(site, title)
    
    if page.exists():
        return True, page.pageid
    return False, None


def get_page_content(title: str) -> Tuple[bool, Optional[str]]:
    """
    Fetch the wikitext content of a page.
    
    Args:
        title: The page title
        
    Returns:
        Tuple of (exists, content) where content is None if page doesn't exist
    """
    site = get_site()
    page = Page(site, title)
    
    if page.exists():
        return True, page.text
    return False, None


def save_page(
    title: str,
    content: str,
    summary: str,
    minor: bool = False,
    bot: bool = True,
    callback: Optional[Callable[[Page, Optional[Exception]], None]] = None,
) -> bool:
    """
    Save content to a wiki page.
    
    Args:
        title: The page title
        content: The wikitext content to save
        summary: Edit summary
        minor: Whether to mark as minor edit
        bot: Whether to mark as bot edit
        callback: Optional callback function (page, exception) -> None
        
    Returns:
        True if save was successful
    """
    site = get_site()
    page = Page(site, title)
    
    try:
        page.text = content
        page.save(
            summary=summary,
            minor=minor,
            botflag=bot,
        )
        if callback:
            callback(page, None)
        return True
    except Exception as e:
        if callback:
            callback(page, e)
        raise


def move_page(
    from_title: str,
    to_title: str,
    reason: str = "",
    leave_redirect: bool = True,
) -> bool:
    """
    Move (rename) a wiki page.
    
    Args:
        from_title: The current page title
        to_title: The new page title
        reason: Move reason/summary
        leave_redirect: Whether to leave a redirect at old title
        
    Returns:
        True if move was successful
    """
    site = get_site()
    page = Page(site, from_title)
    
    page.move(
        newtitle=to_title,
        reason=reason,
        noredirect=not leave_redirect,
    )
    return True


class RateLimiter:
    """
    Rate limiter for API requests.
    
    Ensures minimum interval between operations and handles maxlag.
    """
    
    def __init__(
        self,
        min_interval: float = DEFAULT_EDIT_INTERVAL,
        maxlag: int = DEFAULT_MAXLAG,
    ):
        self.min_interval = min_interval
        self.maxlag = maxlag
        self.last_request_time = 0.0
    
    def wait(self):
        """Wait until the next request can be made."""
        elapsed = time.time() - self.last_request_time
        if elapsed < self.min_interval:
            time.sleep(self.min_interval - elapsed)
        self.last_request_time = time.time()
    
    def handle_maxlag(self, retry_after: int = 30):
        """
        Handle maxlag response by waiting.
        
        Args:
            retry_after: Seconds to wait (default 30, max 120)
        """
        wait_time = min(retry_after, 120)
        print(f"Maxlag hit, waiting {wait_time} seconds...")
        time.sleep(wait_time)
