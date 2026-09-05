"""Tests for wrc_scraper.config env readers and settings assembly.

Focus: the readers default cleanly when a variable is absent, and fail with a
clear, variable-named message when a value is present but malformed (rather than
a bare, contextless ValueError from int()/float()).
"""

from __future__ import annotations

import pytest

from wrc_scraper.config import (
    ScrapingSettings,
    TransformSettings,
    env_bool,
    env_float,
    env_int,
    env_int_list,
)


def test_env_int_defaults_when_absent(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("WRC_X", raising=False)
    assert env_int("WRC_X", 24) == 24


def test_env_int_reads_value(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("WRC_X", "8")
    assert env_int("WRC_X", 24) == 8


def test_env_int_malformed_raises_with_variable_name(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("WRC_X", "abc")
    with pytest.raises(ValueError, match="WRC_X must be an integer, got 'abc'"):
        env_int("WRC_X", 24)


def test_env_float_malformed_raises_with_variable_name(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("WRC_D", "fast")
    with pytest.raises(ValueError, match="WRC_D must be a number, got 'fast'"):
        env_float("WRC_D", 0.0)


def test_env_int_list_malformed_raises_with_variable_name(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("WRC_CODES", "429,notacode")
    with pytest.raises(ValueError, match="WRC_CODES must be a comma-separated list of integers"):
        env_int_list("WRC_CODES", [429])


def test_env_bool_recognizes_truthy_and_defaults(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("WRC_F", "TRUE")
    assert env_bool("WRC_F", False) is True
    monkeypatch.setenv("WRC_F", "nope")
    assert env_bool("WRC_F", True) is False  # unrecognized -> not truthy
    monkeypatch.delenv("WRC_F", raising=False)
    assert env_bool("WRC_F", True) is True  # absent -> default


def test_scraping_settings_derives_allowed_domains_from_search_host(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # www. is stripped as a convenience; other subdomains are kept as-is.
    monkeypatch.setenv("WRC_SEARCH_URL", "https://www.example.ie/en/search/")
    monkeypatch.delenv("WRC_ALLOWED_DOMAINS", raising=False)
    cfg = ScrapingSettings.from_env()
    assert cfg.allowed_domains == ["example.ie"]


def test_scraping_settings_autothrottle_target_tracks_concurrency(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("WRC_CONCURRENT_REQUESTS", "8")
    monkeypatch.delenv("WRC_AUTOTHROTTLE_TARGET_CONCURRENCY", raising=False)
    monkeypatch.delenv("WRC_CONCURRENT_REQUESTS_PER_DOMAIN", raising=False)
    cfg = ScrapingSettings.from_env()
    assert cfg.concurrent_requests == 8
    assert cfg.concurrent_requests_per_domain == 8
    assert cfg.autothrottle_target_concurrency == 8.0


def test_transform_settings_concurrency_defaults_to_eight(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("WRC_TRANSFORM_CONCURRENCY", raising=False)
    assert TransformSettings.from_env().concurrency == 8


def test_transform_settings_concurrency_reads_override(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("WRC_TRANSFORM_CONCURRENCY", "1")
    assert TransformSettings.from_env().concurrency == 1


def test_transform_settings_concurrency_malformed_raises_with_variable_name(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("WRC_TRANSFORM_CONCURRENCY", "many")
    with pytest.raises(ValueError, match="WRC_TRANSFORM_CONCURRENCY must be an integer"):
        TransformSettings.from_env()
