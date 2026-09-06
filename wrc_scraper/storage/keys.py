"""Deterministic identity for Landing Zone storage.

Identity is `(body_slug, detail_url)` -- never `(body, identifier)`, and never
`partition_date`, so a record's identity is stable no matter which date window
it was (re-)scraped under. (The collision table below predates the slug and so
names the raw numeric `body`; the slug is 1:1 with it, so the counts are
identical -- only the stored key form changed.)

WHY NOT `identifier`: the collision investigation (docs/SCRAPY_EXPERIMENTS.md
Sec 19) tested every candidate against 375 live rows spanning all four bodies:

    (body, ref_no)                  -> 16 colliding groups
    (body, identifier / h2.title)   -> collides too, in a *different* corpus
    (body, identifier, published_date) -> no help (collisions share the date)
    (body, detail_url)              -> 0 collisions
    (body, identifier, detail_url)  -> 0 collisions, identifier adds nothing

Both identifier candidates collide, in different eras: `ref_no` in EAT-import
(one internal id shared by several complaint numbers), `h2.title` in
Equality-import (duplicate/re-published "Full Case Report" pages, sometimes
with genuinely different content -- e.g. DEC-E2003-057 has a complete copy and
a truncated one). `detail_url` was the only field that never collided, so it
is the key; `identifier` stays as metadata and is what the transformation
stage groups by to pick a canonical variant. `ref_no` was measured here but is
not stored at all -- it is identical to `identifier` for modern WRC, and where
it diverges `identifier` is the more meaningful value.

`body_slug` is kept in the key even though `detail_url` alone tested unique
site-wide across the sample -- it costs nothing, and "unique in 375 rows" is
not proof for the whole corpus (exactly the mistake made with `identifier`).
"""

from __future__ import annotations

import re
from pathlib import PurePosixPath
from urllib.parse import urlsplit

_UNSAFE_SLUG_CHARS = re.compile(r"[^A-Za-z0-9._~-]")

# Characters with a dedicated, readable `~word~` token. Escaped one character
# at a time over the *original* string (never by chaining `str.replace` calls
# over a growing output) so the encoding is deterministic and collision-free:
# each input character maps to exactly one output token/passthrough, and `~`
# itself is tokenized too, so a literal `~` already in an identifier can never
# be mistaken for the start of a token produced by this function.
_UNSAFE_CHAR_TOKENS = {
    "/": "slash",
    "\\": "backslash",
    "&": "and",
    "?": "question",
    ":": "colon",
    "~": "tilde",
}


def detail_url_path(detail_url: str) -> str:
    """The URL's path, without scheme/host/query/fragment and without the
    leading slash -- e.g. "en/cases/2024/january/adj-00047352.html".

    Used verbatim (not hashed) so both the Mongo `_id` and the MinIO key stay
    traceable back to their source page by eye, and so MinIO gets a naturally
    browsable body/year/month structure.
    """
    path = urlsplit(detail_url).path.strip()
    if not path or path == "/":
        raise ValueError(f"detail_url {detail_url!r} has no usable path")
    return path.lstrip("/")


def mongo_document_id(body_slug: str, detail_url: str) -> str:
    """`{body slug}:{url path}` -- keyed on the stable body slug (wrc_scraper.bodies),
    not the site's numeric id, so identity survives the site renumbering a body.
    """
    return f"{body_slug}:{detail_url_path(detail_url)}"


def _extension_for(document_type: str) -> str:
    """The stored artifact's file extension for a given `document_type`.

    `html_inline` stores as `.html`; every other document_type ("pdf", "doc",
    "docx") is already a valid extension as-is.
    """
    return "html" if document_type == "html_inline" else document_type


def minio_object_key(body_slug: str, detail_url: str, document_type: str) -> str:
    """`{body slug}/{url path stem}.{ext}`.

    Keyed on the body slug (readable, renumbering-safe -- see wrc_scraper.bodies).
    The extension comes from `document_type`, not the URL: a PDF record's detail
    page is `...rp74_2007.html` but the artifact stored is the PDF, so the key
    must end `.pdf`.
    """
    path = PurePosixPath(detail_url_path(detail_url))
    return f"{body_slug}/{path.with_suffix('')}.{_extension_for(document_type)}"


def transformed_mongo_document_id(body_slug: str, identifier: str) -> str:
    """`{body slug}:{sanitized identifier}` -- the transformed-store key.

    Unlike the landing key (`mongo_document_id`, keyed on `detail_url`), the
    transformed store is keyed on `identifier`: several landing `detail_url`s can
    legitimately share one `identifier` (variant clusters, docs/SCRAPY_EXPERIMENTS.md
    Sec 19), and the transformation stage picks exactly one canonical copy per
    `(body_slug, identifier)` group to rename to `identifier.ext` -- so that pair
    must be the transformed store's identity, not `detail_url`.
    """
    return f"{body_slug}:{sanitize_identifier(identifier)}"


def transformed_minio_object_key(body_slug: str, identifier: str, document_type: str) -> str:
    """`{body slug}/{sanitized identifier}.{ext}` -- the assessment's
    `identifier.ext` renaming requirement, with the body_slug prefix kept
    (matches the landing layout) so the bucket stays browsable and identifiers
    can't collide across bodies.
    """
    return f"{body_slug}/{sanitize_identifier(identifier)}.{_extension_for(document_type)}"


def _encode_unsafe_chars(text: str) -> str:
    """Token-encode every character in `_UNSAFE_CHAR_TOKENS`, one input
    character at a time.

    Collision-resistant by construction: `~` only ever appears in the output
    as part of a `~word~` token (`~` itself is tokenized to `~tilde~`), and
    every other passthrough character comes through unchanged -- so two
    different inputs can never encode to the same output. E.g. `RP74/2007` ->
    `RP74~slash~2007` (never collides with the literal `RP74-2007`), and
    `A~B` -> `A~tilde~B` (never collides with literal text that already
    contains `~slash~`, since that text's own `~`s get tokenized too).
    """
    return "".join(
        f"~{_UNSAFE_CHAR_TOKENS[char]}~" if char in _UNSAFE_CHAR_TOKENS else char for char in text
    )


def sanitize_identifier(identifier: str) -> str:
    """Filename-safe, collision-resistant form of `identifier` (h2.title),
    for the transformation stage's `identifier.ext` renaming and destination
    document id.

    Handles all unsafe patterns observed in live data across all four bodies:
    - `/ \\ & ? : ~`  token-encoded via `_UNSAFE_CHAR_TOKENS` (e.g.
                      `RP74/2007` -> `RP74~slash~2007`) rather than replaced
                      with a lossy separator -- two identifiers differing only
                      by one of these characters must never collide on the
                      same storage key.
    - `,`             compound multi-complaint titles -- stripped
    - en-dash `–`     variant of ` - ` used in IR-SC identifiers -- normalised to `-`
    - spaces          replaced with `_`, then consecutive separators collapsed
    - any other char  raises -- this sanitizes what we have evidence for; it is
                      not a blanket "make any string safe" helper.

    This is the *storage-name* encoding only: it never touches the original
    `identifier` value stored in MongoDB, which callers keep verbatim.
    """
    if not identifier or not identifier.strip():
        raise ValueError("identifier must not be empty")

    slug = identifier.strip()
    # Normalise unicode dashes/hyphens to ASCII hyphen before further processing
    slug = slug.replace("\u2013", "-").replace("\u2014", "-")  # en-dash, em-dash
    slug = _encode_unsafe_chars(slug)
    slug = slug.replace(",", "")
    slug = re.sub(r"\s+", "_", slug)
    slug = re.sub(r"[-_]{2,}", lambda match: match.group(0)[0], slug)
    slug = slug.strip("-_")  # remove leading/trailing separators left by stripping chars

    if not slug:
        raise ValueError(f"identifier {identifier!r} reduced to empty string after sanitization")

    if _UNSAFE_SLUG_CHARS.search(slug):
        raise ValueError(
            f"identifier {identifier!r} sanitized to {slug!r}, which still contains "
            f"characters unsafe for a filename"
        )
    return slug
