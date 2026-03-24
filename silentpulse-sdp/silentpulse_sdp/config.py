"""Global configuration for SilentPulse SDP telemetry reporting."""

from __future__ import annotations

import os
from dataclasses import dataclass


@dataclass
class _Config:
    """Internal configuration state."""

    api_url: str = ""
    api_token: str = ""
    enabled: bool = True
    timeout: float = 5.0


_current = _Config()


def configure(
    *,
    api_url: str | None = None,
    api_token: str | None = None,
    enabled: bool = True,
    timeout: float = 5.0,
) -> None:
    """Configure SilentPulse SDP telemetry reporting.

    Values fall back to environment variables ``SILENTPULSE_API_URL``
    and ``SILENTPULSE_API_TOKEN`` when not provided explicitly.

    Args:
        api_url: Base URL of the SilentPulse API (e.g. ``https://silentpulse.local``).
        api_token: Personal Access Token (``sp_pat_...``).
        enabled: Set to ``False`` to disable all telemetry reporting.
        timeout: HTTP request timeout in seconds.
    """
    global _current
    _current = _Config(
        api_url=(api_url or os.environ.get("SILENTPULSE_API_URL", "")).rstrip("/"),
        api_token=api_token or os.environ.get("SILENTPULSE_API_TOKEN", ""),
        enabled=enabled,
        timeout=timeout,
    )


def get_config() -> _Config:
    """Return the current configuration."""
    return _current


def reset() -> None:
    """Reset configuration to defaults. Useful for tests."""
    global _current
    _current = _Config()
