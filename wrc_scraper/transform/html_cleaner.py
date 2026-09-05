"""BeautifulSoup HTML cleaning for the transformation stage.

Verified live across all four bodies (docs/SCRAPY_EXPERIMENTS.md Sec 22):
`div.content` is the universal content anchor (exactly one per page on every
template), but its internals differ sharply -- WRC/Equality content is clean
semantic HTML (no `class`, no `<span>`, no `<img>`), Labour Court content is
presentational (`class="c1".."c5"` wrappers, `<span>`-wrapped text, layout
`<table>`s with width/border/cellpadding/cellspacing/valign attributes, and
1x1 spacer `<img>`s). EAT-import PDF stubs leave `div.content` empty -- their
real content is a PDF, routed through a different `document_type` and never
through this module; the empty-content guard here is defensive, not the normal
EAT path.

One union cleaner handles all four -- normalization is a no-op on already-clean
WRC/Equality content and the real cleanup on Labour Court, rather than
branching per body.
"""

from __future__ import annotations

import dataclasses
import html
import re
from urllib.parse import urljoin

from bs4 import BeautifulSoup
from bs4.element import Tag

# The site's own domain, used only to absolutize a kept <img src> against a
# root-relative path -- not a deployment/operational value (CLAUDE.md: this is
# part of the site's extraction contract, same category as wrc_scraper.bodies).
_SITE_BASE_URL = "https://www.workplacerelations.ie"

_STRIP_TAGS = ("script", "style", "noscript", "iframe")
_PRESENTATIONAL_ATTRS = frozenset(
    {"class", "style", "width", "border", "cellpadding", "cellspacing", "valign", "align"}
)
# A node is "empty" only if it has neither text nor one of these -- a paragraph
# reduced to whitespace-only inline markup (<p><b> </b></p>) is empty, but a
# node that still holds an image or a table is never empty, even textless.
_NON_EMPTY_DESCENDANTS = ("img", "table")

_SIGNATURE_PATTERN = re.compile(
    r"signed on behalf of|for and on behalf of|yours faithfully", re.IGNORECASE
)
_NBSP = "\xa0"


@dataclasses.dataclass(frozen=True)
class CleanedHtml:
    html: bytes  # complete, minimal, UTF-8 document -- doctype + meta charset + title + content
    text_length: int  # visible text length of the cleaned content; drives canonical selection
    has_signature_block: bool  # secondary canonical-selection tiebreaker (Sec 19)


def clean_html(raw_html: str, identifier: str, *, keep_images: bool) -> CleanedHtml | None:
    """Extract and normalize `div.content` from `raw_html`.

    Returns `None` if `div.content` is missing or empty -- the guard for an
    `html_inline` record whose real content is actually a PDF stub (or any
    other malformed page); the caller must log and skip, never emit an empty
    document.
    """
    soup = _parse(raw_html)
    content = soup.select_one("div.content")
    if content is None or not content.get_text(strip=True):
        return None

    _prepend_page_title(content)
    _strip_unwanted_tags(content)
    _strip_presentational_attributes(content)
    _unwrap_spans(content)
    if keep_images:
        _absolutize_image_urls(content)
    else:
        _drop_images(content)
    _drop_empty_nodes(content)

    text_length = len(content.get_text(" ", strip=True))
    has_signature_block = bool(_SIGNATURE_PATTERN.search(content.get_text(" ")))
    document = _wrap_minimal_document(content, identifier)
    return CleanedHtml(
        html=document.encode("utf-8"),
        text_length=text_length,
        has_signature_block=has_signature_block,
    )


def _parse(raw_html: str) -> BeautifulSoup:
    try:
        return BeautifulSoup(raw_html, "lxml")
    except Exception:  # noqa: BLE001 -- lxml's HTML parser is extremely lenient and
        # essentially never raises; this is a last-resort fallback (including when
        # lxml isn't installed), not an expected path.
        return BeautifulSoup(raw_html, "html.parser")


def _prepend_page_title(content: Tag) -> None:
    """Fold the case-identifier heading into the extracted content.

    Verified live: `<h1 class="page-title">` (e.g. "ADJ-00035852") is a
    sibling of `div.content`, not a descendant -- but the assessment's
    "relevant content" screenshot draws its boundary starting at that
    heading, so it is moved into `content` (as the first child) rather than
    left out. A no-op when the template has no such sibling.
    """
    page_title = content.find_previous_sibling("h1", class_="page-title")
    if page_title is not None:
        content.insert(0, page_title.extract())


def _strip_unwanted_tags(content: Tag) -> None:
    for tag_name in _STRIP_TAGS:
        for tag in content.find_all(tag_name):
            tag.decompose()


def _strip_presentational_attributes(content: Tag) -> None:
    for tag in (content, *content.find_all(True)):
        for attr in list(tag.attrs):
            if attr in _PRESENTATIONAL_ATTRS or attr.lower().startswith("on"):
                del tag.attrs[attr]


def _unwrap_spans(content: Tag) -> None:
    for span in content.find_all("span"):
        span.unwrap()


def _drop_images(content: Tag) -> None:
    for img in content.find_all("img"):
        img.decompose()


def _absolutize_image_urls(content: Tag) -> None:
    for img in content.find_all("img"):
        src = img.get("src")
        if src:
            img["src"] = urljoin(_SITE_BASE_URL, src)


def _drop_empty_nodes(content: Tag) -> None:
    changed = True
    while changed:
        changed = False
        for tag in content.find_all(("p", "div")):
            if tag is content or tag.parent is None:
                continue
            if _is_effectively_empty(tag):
                tag.decompose()
                changed = True


def _is_effectively_empty(tag: Tag) -> bool:
    if tag.get_text().replace(_NBSP, " ").strip():
        return False
    return tag.find(_NON_EMPTY_DESCENDANTS) is None


def _wrap_minimal_document(content: Tag, identifier: str) -> str:
    title = html.escape(identifier)
    return (
        "<!DOCTYPE html>\n"
        "<html>\n<head>\n"
        '<meta charset="utf-8">\n'
        f"<title>{title}</title>\n"
        "</head>\n<body>\n"
        f"{content}\n"
        "</body>\n</html>\n"
    )
