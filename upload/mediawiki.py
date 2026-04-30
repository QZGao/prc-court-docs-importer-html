"""
MediaWiki API client for zhwikisource.

This module provides wrappers around pywikibot for page operations.
Credentials are read from environment variables and written to pywikibot config files.

Rate limiting is handled by pywikibot's built-in throttle (put_throttle setting).
"""

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Optional, Tuple, Callable, Any

# Project root directory (where this module's package lives)
# MUST be set before importing pywikibot!
PROJECT_ROOT = Path(__file__).parent.parent

# Set PYWIKIBOT_DIR so pywikibot finds config files in project root
os.environ["PYWIKIBOT_DIR"] = str(PROJECT_ROOT)


def _write_user_config_before_import() -> None:
    """
    Write pywikibot config files BEFORE importing pywikibot.
    
    Pywikibot reads its config at import time, so we must write these files first.
    """
    bot_username = os.getenv("MW_BOT_USERNAME")
    bot_password = os.getenv("MW_BOT_PASSWORD")
    
    if not bot_username or not bot_password:
        # Skip writing - will fail later with clear error message
        return
    
    config_path = PROJECT_ROOT / "user-config.py"
    password_path = PROJECT_ROOT / "user-password.py"
    
    # Write user-config.py
    user_config_content = f"""# Pywikibot configuration file (auto-generated from .env)
family = 'wikisource'
mylang = 'zh'
usernames['wikisource']['zh'] = '{bot_username}'
password_file = 'user-password.py'
"""
    config_path.write_text(user_config_content, encoding='utf-8')
    
    # Write user-password.py
    password_path.write_text(f"('{bot_username}', '{bot_password}')\n", encoding='utf-8')


# Write config files before importing pywikibot
_write_user_config_before_import()

import pywikibot
from pywikibot import Site, Page


# Site configuration
SITE_CODE = 'zh'
SITE_FAMILY = 'wikisource'

# Default rate limits
DEFAULT_EDIT_INTERVAL = 3.0  # seconds between edits
DEFAULT_MAXLAG = 5
DEFAULT_READ_BATCH_SIZE = 20

_site: Optional[Site] = None


@dataclass
class PageSnapshot:
    """Direct page snapshot without following redirects."""
    requested_title: str
    exists: bool
    page_id: Optional[int] = None
    canonical_title: Optional[str] = None
    content: Optional[str] = None


@dataclass
class ResolvedPage:
    """Resolved page state after following redirects."""
    requested_title: str
    exists: bool
    page_id: Optional[int] = None
    is_redirect: bool = False
    redirect_target: Optional[str] = None
    resolved_title: Optional[str] = None
    content: Optional[str] = None


@dataclass
class RedirectInfo:
    """Redirect metadata for move-over-redirect decisions."""
    title: str
    exists: bool
    is_redirect: bool
    target_title: Optional[str] = None
    revision_count: int = 0


def _ensure_credentials() -> Tuple[str, str]:
    """
    Ensure bot credentials are available from environment variables.
    
    Returns:
        Tuple of (bot_username, bot_password)
        
    Raises:
        ValueError: If credentials are not set in environment
    """
    bot_username = os.getenv("MW_BOT_USERNAME")
    bot_password = os.getenv("MW_BOT_PASSWORD")
    
    if not bot_username or not bot_password:
        raise ValueError(
            "Environment variables MW_BOT_USERNAME and MW_BOT_PASSWORD must be set.\n"
            "Create a .env file with:\n"
            "  MW_BOT_USERNAME=YourBot@botname\n"
            "  MW_BOT_PASSWORD=your_bot_password"
        )
    
    return bot_username, bot_password


def get_site() -> Site:
    """
    Get or create the pywikibot Site instance.
    
    Returns:
        The configured Site for zhwikisource
        
    Raises:
        ValueError: If MW_BOT_USERNAME or MW_BOT_PASSWORD env vars are not set
    """
    global _site
    if _site is None:
        # Validate credentials are set
        _ensure_credentials()
        
        _site = pywikibot.Site(SITE_CODE, SITE_FAMILY)
        _site.login()
    return _site


def batched(items: list[str], batch_size: int) -> list[list[str]]:
    """Split a list into fixed-size batches."""
    return [items[index : index + batch_size] for index in range(0, len(items), batch_size)]


def post_query(data: dict[str, Any], maxlag: int = DEFAULT_MAXLAG) -> dict[str, Any]:
    """Perform a batched read query through pywikibot's request layer."""
    payload = get_site().simple_request(
        **{
            "action": "query",
            "format": "json",
            "formatversion": "2",
            "maxlag": maxlag,
            **data,
        }
    ).submit()
    if "error" in payload:
        raise RuntimeError(payload["error"])
    return payload


def build_title_alias_map(payload: dict[str, Any], include_redirects: bool = False) -> dict[str, str]:
    """Build a title alias map from normalized, converted, and optional redirect entries."""
    alias_map: dict[str, str] = {}
    query = payload.get("query", {})

    for key in ("normalized", "converted"):
        for entry in query.get(key, []):
            source = entry.get("from")
            target = entry.get("to")
            if source and target:
                alias_map[source] = target

    if include_redirects:
        for entry in query.get("redirects", []):
            source = entry.get("from")
            target = entry.get("to")
            if source and target:
                alias_map[source] = target

    return alias_map


def resolve_canonical_title(title: str, alias_map: dict[str, str]) -> str:
    """Follow alias mappings until the canonical title is reached."""
    seen: set[str] = set()
    current = title
    while current in alias_map and current not in seen:
        seen.add(current)
        current = alias_map[current]
    return current


def _extract_page_record(page: dict[str, Any]) -> dict[str, Any]:
    """Extract common page fields from an API page object."""
    entry = {
        "title": page.get("title", ""),
        "exists": "missing" not in page,
        "page_id": page.get("pageid"),
        "content": None,
    }

    if entry["exists"]:
        revisions = page.get("revisions") or []
        if revisions:
            slots = revisions[0].get("slots") or {}
            main_slot = slots.get("main") or {}
            entry["content"] = main_slot.get("content", "")

    return entry


def fetch_page_content_batch(
    titles: list[str],
    batch_size: int = DEFAULT_READ_BATCH_SIZE,
    maxlag: int = DEFAULT_MAXLAG,
) -> dict[str, PageSnapshot]:
    """
    Fetch direct page snapshots for titles without following redirects.

    The returned content is the content stored at the requested title itself,
    which may be redirect wikitext.
    """
    results: dict[str, PageSnapshot] = {}

    for title_batch in batched(list(dict.fromkeys(titles)), batch_size):
        payload = post_query(
            {
                "titles": "|".join(title_batch),
                "prop": "revisions",
                "rvprop": "content",
                "rvslots": "main",
            },
            maxlag=maxlag,
        )

        alias_map = build_title_alias_map(payload)
        page_map = {
            page.get("title", ""): _extract_page_record(page)
            for page in payload.get("query", {}).get("pages", [])
        }

        for requested_title in title_batch:
            canonical_title = resolve_canonical_title(requested_title, alias_map)
            page = page_map.get(canonical_title) or page_map.get(requested_title)
            if page is None:
                results[requested_title] = PageSnapshot(
                    requested_title=requested_title,
                    exists=False,
                    canonical_title=canonical_title,
                )
                continue

            results[requested_title] = PageSnapshot(
                requested_title=requested_title,
                exists=bool(page["exists"]),
                page_id=page["page_id"] if page["exists"] else None,
                canonical_title=page["title"] or canonical_title,
                content=page["content"] if page["exists"] else None,
            )

    return results


def resolve_pages_batch(
    titles: list[str],
    batch_size: int = DEFAULT_READ_BATCH_SIZE,
    maxlag: int = DEFAULT_MAXLAG,
) -> dict[str, ResolvedPage]:
    """
    Resolve titles to landing pages in batch, following redirects via the API.

    This is intended for read-side existence checks and redirect landing-page
    inspection before uploads.
    """
    results: dict[str, ResolvedPage] = {}

    for title_batch in batched(list(dict.fromkeys(titles)), batch_size):
        payload = post_query(
            {
                "titles": "|".join(title_batch),
                "redirects": "1",
                "prop": "revisions",
                "rvprop": "content",
                "rvslots": "main",
            },
            maxlag=maxlag,
        )

        normalize_map = build_title_alias_map(payload, include_redirects=False)
        redirect_map = build_title_alias_map(payload, include_redirects=True)
        page_map = {
            page.get("title", ""): _extract_page_record(page)
            for page in payload.get("query", {}).get("pages", [])
        }

        query = payload.get("query", {})
        first_hop_redirects = {
            entry.get("from"): entry.get("to")
            for entry in query.get("redirects", [])
            if entry.get("from") and entry.get("to")
        }

        for requested_title in title_batch:
            normalized_title = resolve_canonical_title(requested_title, normalize_map)
            resolved_title = resolve_canonical_title(normalized_title, redirect_map)
            page = page_map.get(resolved_title) or page_map.get(normalized_title) or page_map.get(requested_title)
            is_redirect = normalized_title in first_hop_redirects
            redirect_target = first_hop_redirects.get(normalized_title)

            if page is None:
                results[requested_title] = ResolvedPage(
                    requested_title=requested_title,
                    exists=False,
                    is_redirect=is_redirect,
                    redirect_target=redirect_target,
                    resolved_title=resolved_title if resolved_title != requested_title else None,
                )
                continue

            if is_redirect and not page["exists"]:
                results[requested_title] = ResolvedPage(
                    requested_title=requested_title,
                    exists=True,
                    is_redirect=True,
                    redirect_target=redirect_target,
                    resolved_title=resolved_title,
                    content=None,
                )
                continue

            results[requested_title] = ResolvedPage(
                requested_title=requested_title,
                exists=bool(page["exists"]),
                page_id=page["page_id"] if page["exists"] else None,
                is_redirect=is_redirect,
                redirect_target=redirect_target,
                resolved_title=(page["title"] or resolved_title) if page["exists"] else resolved_title,
                content=page["content"] if page["exists"] else None,
            )

    return results


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


def resolve_page(title: str, max_redirects: int = 10) -> ResolvedPage:
    """
    Resolve a title to its landing page, following redirects when possible.

    Args:
        title: Title to inspect
        max_redirects: Maximum redirect hops to follow

    Returns:
        ResolvedPage describing the requested title and landing content
    """
    site = get_site()
    page = Page(site, title)

    if not page.exists():
        return ResolvedPage(
            requested_title=title,
            exists=False,
        )

    page_id = page.pageid
    current = page
    seen = {current.title()}
    is_redirect = False
    redirect_target = None

    for _ in range(max_redirects):
        if not current.isRedirectPage():
            return ResolvedPage(
                requested_title=title,
                exists=True,
                page_id=page_id,
                is_redirect=is_redirect,
                redirect_target=redirect_target,
                resolved_title=current.title(),
                content=current.text,
            )

        is_redirect = True
        target = current.getRedirectTarget()
        redirect_target = target.title()
        if target.title() in seen:
            raise ValueError(f"Redirect loop detected while resolving [[{title}]]")
        seen.add(target.title())
        current = target

        if not current.exists():
            return ResolvedPage(
                requested_title=title,
                exists=True,
                page_id=page_id,
                is_redirect=True,
                redirect_target=redirect_target,
                resolved_title=current.title(),
                content=None,
            )

    raise ValueError(f"Too many redirects while resolving [[{title}]]")


def get_redirect_info(title: str) -> RedirectInfo:
    """
    Inspect whether a title is a redirect and gather minimal history metadata.

    Returns redirect target title and up to the first two revisions so callers
    can identify the safe "single-revision redirect back to source" move case.
    """
    site = get_site()
    page = Page(site, title)

    if not page.exists():
        return RedirectInfo(
            title=title,
            exists=False,
            is_redirect=False,
        )

    if not page.isRedirectPage():
        return RedirectInfo(
            title=page.title(),
            exists=True,
            is_redirect=False,
        )

    target = page.getRedirectTarget().title(with_section=False)
    revision_count = sum(1 for _ in page.revisions(total=2))

    return RedirectInfo(
        title=page.title(),
        exists=True,
        is_redirect=True,
        target_title=target,
        revision_count=revision_count,
    )


def can_move_over_redirect(from_title: str, to_title: str) -> bool:
    """
    Return whether MediaWiki should allow moving over the destination redirect.

    This follows the documented safe case: the destination exists as a redirect
    with a single history entry and points back to the source title.
    """
    site = get_site()
    source_title = Page(site, from_title).title(with_section=False)
    redirect_info = get_redirect_info(to_title)

    return bool(
        redirect_info.exists
        and redirect_info.is_redirect
        and redirect_info.target_title == source_title
        and redirect_info.revision_count == 1
    )


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
    ignore_warnings: bool = False,
) -> bool:
    """
    Move (rename) a wiki page.
    
    Args:
        from_title: The current page title
        to_title: The new page title
        reason: Move reason/summary
        leave_redirect: Whether to leave a redirect at old title
        ignore_warnings: Whether to submit the move with ignorewarnings=1
        
    Returns:
        True if move was successful
    """
    site = get_site()
    page = Page(site, from_title)

    if ignore_warnings:
        request = site.simple_request(
            action='move',
            to=to_title,
            reason=reason,
            movetalk=True,
            movesubpages=True,
            noredirect=not leave_redirect,
            ignorewarnings=True,
            token=site.tokens['csrf'],
        )
        request['from'] = page.title(with_section=False)
        result = request.submit()
        if 'move' not in result:
            raise ValueError(f"Unexpected move response: {result}")
        return True

    page.move(
        newtitle=to_title,
        reason=reason,
        noredirect=not leave_redirect,
    )
    return True


def configure_throttle(interval: float = DEFAULT_EDIT_INTERVAL, maxlag: int = DEFAULT_MAXLAG) -> None:
    """
    Configure pywikibot's built-in rate limiting.
    
    Args:
        interval: Minimum seconds between edits (put_throttle)
        maxlag: Server lag threshold for API requests
    """
    pywikibot.config.put_throttle = float(interval)
    pywikibot.config.maxlag = maxlag
