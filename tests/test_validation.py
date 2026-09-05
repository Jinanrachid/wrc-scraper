"""Tests for wrc_scraper.validation (Phase 2, Step 2.2 / Decision 7).

No live requests -- the decision to reject bad input is client-side and
independent of what the server would have done (already observed separately
in docs/SCRAPY_EXPERIMENTS.md).
"""

from __future__ import annotations

from datetime import date

import pytest

from wrc_scraper.validation import (
    KNOWN_BODY_IDS,
    parse_iso_date,
    validate_bodies,
    validate_date_range,
)


def test_parse_iso_date_valid() -> None:
    assert parse_iso_date("2024-01-31") == date(2024, 1, 31)


@pytest.mark.parametrize("raw", ["2024-02-30", "not-a-date", "2024/01/01", ""])
def test_parse_iso_date_rejects_unparseable_or_impossible_dates(raw: str) -> None:
    with pytest.raises(ValueError, match="invalid date"):
        parse_iso_date(raw)


def test_validate_date_range_accepts_normal_range() -> None:
    validate_date_range(date(2024, 1, 1), date(2024, 12, 31))  # no raise


def test_validate_date_range_accepts_equal_start_and_end() -> None:
    validate_date_range(date(2024, 1, 1), date(2024, 1, 1))  # no raise


def test_validate_date_range_rejects_reversed_range() -> None:
    with pytest.raises(ValueError, match="must not be after"):
        validate_date_range(date(2024, 2, 1), date(2024, 1, 1))


def test_validate_date_range_allows_future_dates() -> None:
    # Phase 1: empty windows return HTTP 200 / 0 rows gracefully -- not
    # rejected client-side, per Decision 7.
    validate_date_range(date(2099, 1, 1), date(2099, 12, 31))  # no raise


def test_validate_bodies_accepts_known_ids() -> None:
    validate_bodies(["15376"])
    validate_bodies(list(KNOWN_BODY_IDS))


def test_validate_bodies_rejects_empty_list() -> None:
    with pytest.raises(ValueError, match="must not be empty"):
        validate_bodies([])


def test_validate_bodies_rejects_unknown_numeric_id() -> None:
    with pytest.raises(ValueError, match="unknown body id"):
        validate_bodies(["999"])


def test_validate_bodies_rejects_non_numeric_id() -> None:
    with pytest.raises(ValueError, match="unknown body id"):
        validate_bodies(["abc"])


def test_validate_bodies_rejects_if_any_id_in_list_is_unknown() -> None:
    with pytest.raises(ValueError, match="unknown body id"):
        validate_bodies(["15376", "999"])
