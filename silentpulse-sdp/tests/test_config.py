"""Tests for silentpulse_sdp.config module."""

from silentpulse_sdp.config import configure, get_config, reset


def test_configure_from_params():
    reset()
    configure(api_url="https://sp.example.com", api_token="sp_pat_123", timeout=10.0)
    cfg = get_config()
    assert cfg.api_url == "https://sp.example.com"
    assert cfg.api_token == "sp_pat_123"
    assert cfg.enabled is True
    assert cfg.timeout == 10.0
    reset()


def test_configure_strips_trailing_slash():
    reset()
    configure(api_url="https://sp.example.com/", api_token="tok")
    assert get_config().api_url == "https://sp.example.com"
    reset()


def test_configure_from_env(monkeypatch):
    reset()
    monkeypatch.setenv("SILENTPULSE_API_URL", "https://env.example.com")
    monkeypatch.setenv("SILENTPULSE_API_TOKEN", "sp_pat_env")
    configure()
    cfg = get_config()
    assert cfg.api_url == "https://env.example.com"
    assert cfg.api_token == "sp_pat_env"
    reset()


def test_configure_params_override_env(monkeypatch):
    reset()
    monkeypatch.setenv("SILENTPULSE_API_URL", "https://env.example.com")
    configure(api_url="https://param.example.com")
    assert get_config().api_url == "https://param.example.com"
    reset()


def test_configure_disabled():
    reset()
    configure(api_url="https://sp.example.com", api_token="tok", enabled=False)
    assert get_config().enabled is False
    reset()


def test_reset():
    configure(api_url="https://sp.example.com", api_token="tok")
    reset()
    cfg = get_config()
    assert cfg.api_url == ""
    assert cfg.api_token == ""
    assert cfg.enabled is True
