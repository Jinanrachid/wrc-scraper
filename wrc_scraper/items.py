"""Item schema for the WRC decisions crawl.

Field names follow the assessment PDF's own screenshot labels: the bold
heading on each search-result row is labeled **identifier** (that's
`h2.title`), the date is labeled **published_date** (`span.date`), and
**description** is the party line (`p.description`). The separate `Ref no:`
line the screenshot leaves unlabeled is *not* kept: for modern WRC it is
identical to `identifier`, and where it diverges (import bodies) `identifier`
is the more meaningful value -- so it would be redundant or misleading.

`identifier` is the site's own labeled reference (`h2.title`) -- for modern WRC
it's e.g. "ADJ-00047352"; for EAT-import it's the business reference
("RP74/2007"). It is *not* the storage key (it collides across corpora -- see
storage/keys.py and docs/SCRAPY_EXPERIMENTS.md Sec 19). Identity is derived from
`(body_slug, detail_url)`; `identifier` is what the transformation stage groups
by to pick a canonical variant when several URLs share one identifier.

`body_slug` is the stable machine key for the deciding body (e.g. "wrc") and
`body_name` its human-readable form (e.g. "Workplace Relations Commission"),
both resolved from wrc_scraper.bodies. Storage identity is keyed on the slug, so
it survives the site renumbering a body; the site's opaque numeric id is only a
crawl query parameter and is not stored.

`not_modified` marks the one case where a record legitimately carries no
bytes: the spider made a conditional GET (binary documents only) and the
server answered 304, meaning the copy already in the Landing Zone is current.
It is never set for html_inline -- those pages expose no validators, so their
downloads can't be skipped (see storage/conditional_get.py).

Date fields are ISO strings (not `datetime.date`) so the item is directly
JSON-serializable via Scrapy's feed exporters without a custom encoder.
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass
class WrcDecisionRecord:
    identifier: str  # h2.title -- the PDF screenshot's "identifier"
    description: str  # p.description; falls back to h2.title when absent (EAT-import)
    published_date: str  # ISO YYYY-MM-DD, parsed from the listing's DD/MM/YYYY
    detail_url: str  # the case detail page URL -- the identity field (never "cleaned")
    document_type: str  # "html_inline" | "pdf" | "doc" | "docx"
    document_url: str  # == detail_url for html_inline; the embedded document URL otherwise
    partition_date: str  # ISO YYYY-MM-DD -- the partition window's label (PDF requirement #3)
    body_slug: str  # stable machine key for the deciding body, e.g. "wrc" (also the storage prefix)
    body_name: str  # human-readable deciding body, e.g. "Workplace Relations Commission"
    scraped_at: str  # ISO 8601 UTC timestamp, provenance
    raw_html: str | None = None  # full, unmodified response.text for html_inline only
    raw_binary: bytes | None = None  # full, unmodified bytes for pdf/doc/docx
    remote_etag: str | None = None  # ETag header from the binary GET response, pdf/doc/docx only
    not_modified: bool = False  # server answered 304 to a conditional GET; raw_binary is absent
