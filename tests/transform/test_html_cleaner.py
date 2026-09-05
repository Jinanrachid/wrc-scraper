"""Tests for wrc_scraper.transform.html_cleaner (Phase 4).

Fixtures cover every shape verified live against the real site
(docs/SCRAPY_EXPERIMENTS.md Sec 22): clean WRC, clean Equality, messy Labour
Court (presentational classes/spans/spacer gifs), and the empty-content guard.
"""

from __future__ import annotations

from pathlib import Path

from wrc_scraper.transform.html_cleaner import clean_html

FIXTURES = Path(__file__).parent / "fixtures"


def _fixture(name: str) -> str:
    return (FIXTURES / name).read_text(encoding="utf-8")


def test_wrc_clean_html_preserves_headings_and_data_tables() -> None:
    result = clean_html(_fixture("detail_wrc_clean.html"), "ADJ-00047352", keep_images=False)

    assert result is not None
    text = result.html.decode("utf-8")
    assert "<!doctype html" in text.lower()
    assert "<title>ADJ-00047352</title>" in text
    assert "ADJUDICATION OFFICER DECISION" in text
    assert "<table>" in text  # the parties data table is kept
    assert "Complainant" in text
    assert result.has_signature_block is False


def test_wrc_clean_html_includes_page_title_heading() -> None:
    """The assessment's "relevant content" screenshot starts at the case
    identifier heading, which lives outside div.content as a sibling -- it
    must be folded into the cleaned output, ahead of the rest of the content.
    """
    result = clean_html(_fixture("detail_wrc_clean.html"), "ADJ-00047352", keep_images=False)

    assert result is not None
    text = result.html.decode("utf-8")
    assert "<h1>ADJ-00047352</h1>" in text
    assert text.index("<h1>ADJ-00047352</h1>") < text.index("ADJUDICATION OFFICER DECISION")


def test_equality_clean_html_preserves_headings() -> None:
    result = clean_html(_fixture("detail_equality_clean.html"), "DEC-E2012-001", keep_images=False)

    assert result is not None
    text = result.html.decode("utf-8")
    assert "<h1>DEC-E2012-001</h1>" in text
    assert "The Equality Tribunal" in text
    assert "Noonan Services Limited" in text


def test_labour_court_messy_html_strips_presentational_markup() -> None:
    result = clean_html(_fixture("detail_labour_court_messy.html"), "LCR22706", keep_images=False)

    assert result is not None
    text = result.html.decode("utf-8")
    assert "class=" not in text
    assert "<span" not in text
    assert "ecblank" not in text
    assert 'width="' not in text
    assert "border=" not in text
    # the real content survives the cleanup
    assert "LCR22706" in text
    assert "Signed on behalf of the Labour Court" in text
    assert "Tom Geraghty" in text
    assert result.has_signature_block is True


def test_labour_court_keep_images_true_rewrites_src_to_absolute_url() -> None:
    result = clean_html(_fixture("detail_labour_court_messy.html"), "LCR22706", keep_images=True)

    assert result is not None
    text = result.html.decode("utf-8")
    assert "https://www.workplacerelations.ie/icons/ecblank.gif" in text


def test_empty_div_content_returns_none() -> None:
    """The EAT-import PDF-stub shape: div.content present but empty."""
    result = clean_html(_fixture("detail_empty_content.html"), "MN650/2007", keep_images=False)
    assert result is None


def test_missing_div_content_returns_none() -> None:
    result = clean_html(
        "<html><body><p>no content div here</p></body></html>", "X", keep_images=False
    )
    assert result is None


def test_no_page_title_sibling_is_a_no_op() -> None:
    """Templates without a `h1.page-title` sibling (e.g. hand-built fixtures,
    or a future template shape) must clean normally, with nothing extra added.
    """
    html_no_page_title = (
        '<html><body><div class="content"><p>Just the content, no page-title '
        "sibling.</p></div></body></html>"
    )
    result = clean_html(html_no_page_title, "NO-TITLE-1", keep_images=False)

    assert result is not None
    text = result.html.decode("utf-8")
    assert "<h1>" not in text
    assert "Just the content, no page-title sibling." in text


def test_malformed_html_is_handled_without_raising() -> None:
    malformed = (
        '<html><body><div class="content"><p>Unclosed paragraph'
        "<div>Nested wrongly<span>still open</html>"
    )
    result = clean_html(malformed, "MALFORMED-1", keep_images=False)

    assert result is not None
    assert "Unclosed paragraph" in result.html.decode("utf-8")
