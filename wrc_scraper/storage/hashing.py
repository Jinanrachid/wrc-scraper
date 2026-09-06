"""SHA-256 content hashing.

PDF/DOC/DOCX: raw bytes, unmodified -- no volatility evidence exists (static
IIS-served files; docs/SCRAPY_EXPERIMENTS.md Sec 18).

HTML: raw bytes with a known, narrowly-scoped volatile trailing comment
stripped from the hash *input* only (docs/SCRAPY_EXPERIMENTS.md Sec 17). The
stored artifact is always the complete, unmodified `raw_html` -- this
normalization never touches what's persisted, only what's hashed. Confirmed
live: 3 fetches of the same page produced 3 different raw-byte hashes, but a
single identical hash once this exact pattern is stripped.
"""

from __future__ import annotations

import hashlib
import re

# Matches the ASP.NET render-timing/cache-debug comment block observed at the
# tail of every WRC detail-page response, in any of the observed
# combinations (the two cache-debug comments are sometimes present, sometimes
# not; the "Elapsed time" comment was present on every sample).
_VOLATILE_HTML_COMMENT_PATTERN = re.compile(
    rb"(?:<!--\s*cached location\s*-->)?"
    rb"(?:<!--\s*cached or not being index\.aspx page\s*-->)?"
    rb"<!--\s*Elapsed time:.*?-->",
    re.IGNORECASE | re.DOTALL,
)


def normalize_html_for_hashing(raw_html: str) -> bytes:
    """Return raw_html's UTF-8 bytes with the known volatile trailing comment
    removed. Only ever used as hashing input -- never as what's stored.
    """
    raw_bytes = raw_html.encode("utf-8")
    return _VOLATILE_HTML_COMMENT_PATTERN.sub(b"", raw_bytes)


def hash_html(raw_html: str) -> str:
    """SHA-256 hex digest of raw_html, normalized for the known volatile
    trailing comment (Sec 17).
    """
    return hashlib.sha256(normalize_html_for_hashing(raw_html)).hexdigest()


def hash_binary(raw_bytes: bytes) -> str:
    """SHA-256 hex digest of raw binary (PDF/DOC/DOCX) bytes, unmodified."""
    return hashlib.sha256(raw_bytes).hexdigest()
