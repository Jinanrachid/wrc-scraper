"""Tests that environment variables actually override wrc_scraper.settings
defaults (Phase 2 hardening item 9). One pass per override mechanism, not one
test per trivial field.
"""

from __future__ import annotations

import importlib

import pytest

import wrc_scraper.settings as settings_module


def _reload_with_env(monkeypatch: pytest.MonkeyPatch, **env: str):
    for key, value in env.items():
        monkeypatch.setenv(key, value)
    importlib.reload(settings_module)
    return settings_module


@pytest.fixture(autouse=True)
def _restore_settings_module():
    yield
    importlib.reload(settings_module)  # undo any reload-with-patched-env from the test


def test_defaults_when_no_env_vars_set(monkeypatch: pytest.MonkeyPatch) -> None:
    for key in [
        "WRC_CONCURRENT_REQUESTS",
        "WRC_CONCURRENT_REQUESTS_PER_DOMAIN",
        "WRC_DOWNLOAD_DELAY",
        "WRC_RETRY_TIMES",
        "WRC_DOWNLOAD_TIMEOUT",
        "WRC_ROBOTSTXT_OBEY",
    ]:
        monkeypatch.delenv(key, raising=False)
    settings = _reload_with_env(monkeypatch)

    assert settings.CONCURRENT_REQUESTS == 16
    assert settings.CONCURRENT_REQUESTS_PER_DOMAIN == 16
    assert settings.DOWNLOAD_DELAY == 0.0
    assert settings.RETRY_TIMES == 3
    assert settings.DOWNLOAD_TIMEOUT == 60
    assert settings.ROBOTSTXT_OBEY is False
    # Reactor is pinned (not inherited from Scrapy's version-dependent default).
    assert settings.TWISTED_REACTOR == (
        "twisted.internet.asyncioreactor.AsyncioSelectorReactor"
    )


def test_twisted_reactor_env_override(monkeypatch: pytest.MonkeyPatch) -> None:
    settings = _reload_with_env(
        monkeypatch, WRC_TWISTED_REACTOR="twisted.internet.selectreactor.SelectReactor"
    )
    assert settings.TWISTED_REACTOR == "twisted.internet.selectreactor.SelectReactor"


@pytest.mark.parametrize(
    ("env_var", "env_value", "attr", "expected"),
    [
        ("WRC_CONCURRENT_REQUESTS", "16", "CONCURRENT_REQUESTS", 16),
        ("WRC_DOWNLOAD_DELAY", "0.5", "DOWNLOAD_DELAY", 0.5),
        ("WRC_RETRY_TIMES", "5", "RETRY_TIMES", 5),
        ("WRC_DOWNLOAD_TIMEOUT", "60", "DOWNLOAD_TIMEOUT", 60),
        ("WRC_ROBOTSTXT_OBEY", "true", "ROBOTSTXT_OBEY", True),
    ],
)
def test_env_var_overrides_default(
    monkeypatch: pytest.MonkeyPatch, env_var: str, env_value: str, attr: str, expected: object
) -> None:
    settings = _reload_with_env(monkeypatch, **{env_var: env_value})
    assert getattr(settings, attr) == expected


def test_concurrent_requests_per_domain_inherits_concurrent_requests_when_unset(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("WRC_CONCURRENT_REQUESTS_PER_DOMAIN", raising=False)
    settings = _reload_with_env(monkeypatch, WRC_CONCURRENT_REQUESTS="12")
    # Locked equal to CONCURRENT_REQUESTS unless explicitly overridden (Sec 15's
    # engineering note) -- a single-domain crawl has no reason for them to differ.
    assert settings.CONCURRENT_REQUESTS_PER_DOMAIN == 12


def test_concurrent_requests_per_domain_can_still_be_set_independently(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    settings = _reload_with_env(
        monkeypatch, WRC_CONCURRENT_REQUESTS="12", WRC_CONCURRENT_REQUESTS_PER_DOMAIN="4"
    )
    assert settings.CONCURRENT_REQUESTS == 12
    assert settings.CONCURRENT_REQUESTS_PER_DOMAIN == 4
