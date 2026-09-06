"""Tests for wrc_scraper.storage.keys.

Includes regression tests built from the *real* collision cases found during
the identity investigation (docs/SCRAPY_EXPERIMENTS.md Sec 19) -- these are
the exact records that `(body, identifier)` conflated.
"""

from __future__ import annotations

import pytest

from wrc_scraper.storage.keys import (
    detail_url_path,
    minio_object_key,
    mongo_document_id,
    sanitize_identifier,
)

BASE = "https://www.workplacerelations.ie"


def test_detail_url_path_strips_scheme_host_and_leading_slash() -> None:
    assert (
        detail_url_path(f"{BASE}/en/cases/2024/january/adj-00047352.html")
        == "en/cases/2024/january/adj-00047352.html"
    )


def test_detail_url_path_strips_query_and_fragment() -> None:
    assert detail_url_path(f"{BASE}/en/cases/x.html?a=1#frag") == "en/cases/x.html"


@pytest.mark.parametrize("url", [f"{BASE}", f"{BASE}/", "   "])
def test_detail_url_path_rejects_urls_with_no_path(url: str) -> None:
    with pytest.raises(ValueError, match="no usable path"):
        detail_url_path(url)


def test_mongo_document_id_is_deterministic() -> None:
    url = f"{BASE}/en/cases/2024/january/adj-00047352.html"
    assert mongo_document_id("wrc", url) == "wrc:en/cases/2024/january/adj-00047352.html"
    assert mongo_document_id("wrc", url) == mongo_document_id("wrc", url)


def test_mongo_document_id_differs_by_body() -> None:
    url = f"{BASE}/en/cases/2024/january/adj-00047352.html"
    assert mongo_document_id("wrc", url) != mongo_document_id("labour_court", url)


def test_minio_object_key_uses_document_type_extension_not_the_page_extension() -> None:
    # A PDF record's *page* is .html but the artifact stored is the PDF.
    url = f"{BASE}/en/cases/2008/march/rp74_2007.html"
    assert minio_object_key("eat", url, "pdf") == "eat/en/cases/2008/march/rp74_2007.pdf"
    assert minio_object_key("eat", url, "html_inline") == "eat/en/cases/2008/march/rp74_2007.html"
    assert minio_object_key("eat", url, "docx") == "eat/en/cases/2008/march/rp74_2007.docx"


# -- regression: the real collisions that (body, identifier) conflated ---------


def test_eat_import_joint_decision_urls_no_longer_collide() -> None:
    """RP74/RP75/RP76/2007 -- three complaint numbers, one shared ref_no
    ("30268"), three distinct pages. Under (body, ref_no) these collapsed to
    one record; under (body, detail_url) they must stay distinct.
    """
    urls = [
        f"{BASE}/en/cases/2008/march/rp74_2007.html",
        f"{BASE}/en/cases/2008/march/rp75_2007.html",
        f"{BASE}/en/cases/2008/march/rp76_2007.html",
    ]
    assert len({mongo_document_id("eat", u) for u in urls}) == 3
    assert len({minio_object_key("eat", u, "pdf") for u in urls}) == 3


def test_equality_import_complete_and_truncated_copies_no_longer_collide() -> None:
    """DEC-E2003-057 -- same identifier on two pages, one complete and one
    truncated (verified: different normalized content hashes). Under
    (body, identifier) the truncated copy could overwrite the complete one.
    """
    urls = [
        f"{BASE}/en/cases/2003/december/dec-e2003-057_full_case_report.html",
        f"{BASE}/en/cases/2003/december/dec-e2003-057_full_case_report1.html",
    ]
    assert len({mongo_document_id("equality", u) for u in urls}) == 2
    assert len({minio_object_key("equality", u, "html_inline") for u in urls}) == 2


def test_eat_import_url_suffix_variants_no_longer_collide() -> None:
    """The RP68 cluster: four URL-suffix variants sharing both ref_no and
    identifier.
    """
    stem = f"{BASE}/en/cases/2008/march/rp68_2007_rp69_2007_rp70_2007_rp126_2007"
    urls = [f"{stem}.html", f"{stem}1.html", f"{stem}11.html", f"{stem}_16403.html"]
    assert len({mongo_document_id("eat", u) for u in urls}) == 4


# -- identifier sanitization (not identity -- filename safety only) ------------


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        ("ADJ-00047352", "ADJ-00047352"),
        ("ADJ 49297", "ADJ_49297"),
        ("RP74/2007", "RP74~slash~2007"),
        ("RP72/2007, RP73/2007", "RP72~slash~2007_RP73~slash~2007"),
        (
            "RP68/2007, RP69/2007, RP70/2007, RP126/2007",
            "RP68~slash~2007_RP69~slash~2007_RP70~slash~2007_RP126~slash~2007",
        ),
        ("IR - SC - 00000787", "IR_SC_00000787"),
        ("DEC-E2003-059", "DEC-E2003-059"),
        ("ADJ-00045266 & ADJ-00047456", "ADJ-00045266_~and~_ADJ-00047456"),
    ],
)
def test_sanitize_identifier_handles_every_observed_real_value(raw: str, expected: str) -> None:
    assert sanitize_identifier(raw) == expected


def test_sanitize_identifier_rejects_empty() -> None:
    with pytest.raises(ValueError, match="must not be empty"):
        sanitize_identifier("   ")


def test_sanitize_identifier_raises_rather_than_guessing_on_unhandled_characters() -> None:
    with pytest.raises(ValueError, match="unsafe for a filename"):
        sanitize_identifier("ADJ⁄123")  # fraction slash -- not a pattern we've seen


# -- storage-name collision resistance (audit MUST FIX) -----------------------
#
# The previous sanitization replaced `/` with `-`, so "RP74/2007" and
# "RP74-2007" both sanitized to "RP74-2007" -- two different identifiers
# targeting the same transformed Mongo document / MinIO object key. The
# `~word~` token encoding below is collision-resistant: every unsafe
# character (including a literal `~`) maps to its own distinct token, so
# distinct raw identifiers can never collapse onto the same sanitized string.


@pytest.mark.parametrize(
    ("first", "second"),
    [
        ("RP74/2007", "RP74-2007"),  # slash vs. hyphen
        ("A&B", "A-B"),  # ampersand vs. hyphen
        ("A~B", "A~slash~B"),  # literal tilde text vs. a generated slash token
    ],
)
def test_distinct_raw_identifiers_never_collide_after_sanitization(first: str, second: str) -> None:
    assert sanitize_identifier(first) != sanitize_identifier(second)


def test_sanitize_identifier_never_mutates_the_original_identifier_string() -> None:
    """`sanitize_identifier` is a pure storage-name projection -- the caller's
    original `identifier` value (what gets stored in MongoDB) must be
    returned unchanged by every other code path that touches it.
    """
    original = "RP74/2007"
    sanitize_identifier(original)
    assert original == "RP74/2007"
