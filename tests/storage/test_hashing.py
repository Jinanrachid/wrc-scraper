"""Tests for wrc_scraper.storage.hashing (Phase 3, Decisions 3/6)."""

from __future__ import annotations

import hashlib

from wrc_scraper.storage.hashing import hash_binary, hash_html

# Real samples captured live (docs/SCRAPY_EXPERIMENTS.md Sec 17): identical
# page content, differing only in the trailing ASP.NET debug comment.
_SAMPLE_BASE = (
    '<html><body><div class="content"><h1>ADJUDICATION OFFICER Recommendation</h1>'
    "<p>Decision text.</p></div></body></html>"
)
_SAMPLE_A = _SAMPLE_BASE + (
    "<!-- cached location --><!-- cached or not being index.aspx page -->"
    "<!-- Elapsed time: 0.0781234 -->"
)
_SAMPLE_B = _SAMPLE_BASE + "<!-- cached or not being index.aspx page --><!-- Elapsed time: 0 -->"
_SAMPLE_C = _SAMPLE_BASE + "<!-- Elapsed time: 12.3456789 -->"
_SAMPLE_DIFFERENT_CONTENT = _SAMPLE_BASE.replace("Decision text.", "Different decision text.") + (
    "<!-- Elapsed time: 0 -->"
)


def test_hash_binary_is_correct_sha256() -> None:
    data = b"%PDF-1.4 some fake pdf bytes"
    assert hash_binary(data) == hashlib.sha256(data).hexdigest()


def test_hash_binary_same_content_same_hash_different_content_different_hash() -> None:
    a = hash_binary(b"content A")
    b = hash_binary(b"content A")
    c = hash_binary(b"content B")
    assert a == b
    assert a != c


def test_hash_html_normalization_converges_despite_differing_trailing_comments() -> None:
    """The core Phase 2/3 finding: raw bytes differ every fetch, but the
    normalized hash must be identical across all observed comment variants.
    """
    hash_a = hash_html(_SAMPLE_A)
    hash_b = hash_html(_SAMPLE_B)
    hash_c = hash_html(_SAMPLE_C)
    assert hash_a == hash_b == hash_c


def test_hash_html_still_detects_genuine_content_changes() -> None:
    normal_hash = hash_html(_SAMPLE_A)
    changed_hash = hash_html(_SAMPLE_DIFFERENT_CONTENT)
    assert normal_hash != changed_hash


def test_hash_html_raw_bytes_would_have_differed_without_normalization() -> None:
    """Sanity check that this test fixture actually reproduces the problem
    normalization solves -- i.e. the raw (unnormalized) samples really are
    byte-different, so the normalized-hash convergence above is meaningful.
    """
    assert (
        hashlib.sha256(_SAMPLE_A.encode()).hexdigest()
        != hashlib.sha256(_SAMPLE_B.encode()).hexdigest()
    )
