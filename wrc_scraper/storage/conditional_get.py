"""Pre-download ETag check for binary documents (PDF/DOC/DOCX only).

Requirement #9 of the assessment asks that unchanged files not be
re-downloaded. Until now the pipeline only avoided the *write*: every binary
was fetched in full, hashed, and then discarded if the hash matched. This
module supplies the missing half -- the information the spider needs to send
a conditional `If-None-Match` and let the server answer `304 Not Modified`
instead of resending the file.

Two site-specific facts drive the design (both live-verified, see
docs/SCRAPY_EXPERIMENTS.md Sec 18 and Sec 20):

1. The document endpoints emit a **bare, unquoted** ETag --
   `ETag: 635084658003030000` -- which RFC 9110 does not permit (an
   entity-tag must be a quoted-string), and they match on exactly that bare
   token. Re-quoting it gets a full 200, as does the weak `W/"..."` form.
   Sending the value back verbatim is therefore both the correct thing to do
   and the only thing that works; `unquote_etag` below is defensive, a no-op
   against today's headers, so that a stored value that ever does arrive
   quoted is still sent in the matching form. `If-Modified-Since` is ignored
   entirely (the site's `Last-Modified` has no timezone and is itself
   malformed), so `Last-Modified` is not used at all.
2. HTML pages carry no ETag, no Last-Modified and `cache-control: no-cache`,
   so there is nothing to be conditional *about*. `etag_for` refuses
   html_inline outright rather than sending a header that could only ever
   produce a 200 plus a wasted round trip.

Safety rule: this advisor is an optimization and nothing more. It hands back
an ETag only when a full, verified prior version is on hand -- Mongo says
"stored", with a hash, at exactly the object key this record would write, and
that object really is in MinIO. Anything else -- no prior version, a hash-less
record, a key mismatch, a missing object, or *any* error reaching either
store -- returns None, which means a plain unconditional GET followed by the
usual SHA-256 comparison. So the worst case is the behavior we had before,
never a wrong result.
"""

from __future__ import annotations

from typing import Protocol

from wrc_scraper.storage.keys import minio_object_key, mongo_document_id


class MongoLookupPort(Protocol):
    def get(self, doc_id: str) -> dict | None: ...


class MinioLookupPort(Protocol):
    def object_exists(self, key: str) -> bool: ...


def unquote_etag(value: str | None) -> str | None:
    """Strip the `W/` weak marker and the surrounding double quotes an ETag
    header carries, because the WRC endpoints only match on the bare value.

    Returns None for anything empty, so callers get one "no usable ETag"
    answer rather than having to distinguish None from "" from '""'.
    """
    if not value:
        return None
    etag = value.strip()
    if etag.startswith("W/"):
        etag = etag[2:].strip()
    if len(etag) >= 2 and etag.startswith('"') and etag.endswith('"'):
        etag = etag[1:-1]
    return etag or None


class ConditionalGetAdvisor:
    """Answers one question: "may this binary GET be made conditional, and on
    which ETag?" Never raises -- see the module docstring's safety rule.
    """

    def __init__(self, mongo: MongoLookupPort, minio: MinioLookupPort) -> None:
        self._mongo = mongo
        self._minio = minio
        self.last_error: str | None = None

    def etag_for(self, body_slug: str, detail_url: str, document_type: str) -> str | None:
        if document_type == "html_inline":
            return None  # no validators exist on the HTML pages at all
        try:
            return self._etag_for(body_slug, detail_url, document_type)
        except Exception as exc:  # noqa: BLE001 -- an unreachable store must degrade, not crash
            self.last_error = repr(exc)
            return None

    def _etag_for(self, body_slug: str, detail_url: str, document_type: str) -> str | None:
        existing = self._mongo.get(mongo_document_id(body_slug, detail_url))
        if existing is None or existing.get("status") != "stored":
            return None
        if not existing.get("file_hash"):
            return None

        etag = unquote_etag(existing.get("remote_etag"))
        if etag is None:
            return None

        # The prior version must be the one this record would overwrite --
        # not merely *a* stored version under the same id.
        key = minio_object_key(body_slug, detail_url, document_type)
        if existing.get("file_path") != key:
            return None
        if not self._minio.object_exists(key):
            return None  # hash on record but object gone: must re-download

        return etag
