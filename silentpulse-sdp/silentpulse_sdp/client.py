"""Non-blocking HTTP client for SilentPulse telemetry ingest."""

from __future__ import annotations

import logging
from concurrent.futures import ThreadPoolExecutor
from typing import Any

import requests

from .config import get_config

logger = logging.getLogger("silentpulse_sdp")

_executor = ThreadPoolExecutor(max_workers=2, thread_name_prefix="sp-telemetry")


def send_telemetry(integration_point: str, events: list[dict[str, Any]]) -> None:
    """Submit telemetry events to SilentPulse (fire-and-forget).

    The actual HTTP call runs in a background thread so the pipeline
    is never blocked by telemetry reporting.

    Args:
        integration_point: SilentPulse integration point name.
        events: List of event dicts with ``type``, ``timestamp``, and ``metadata``.
    """
    cfg = get_config()
    if not cfg.enabled or not cfg.api_url or not cfg.api_token:
        return

    payload = {"integration_point": integration_point, "events": events}
    _executor.submit(_send_payload, payload, cfg.api_url, cfg.api_token, cfg.timeout)


def _send_payload(payload: dict, api_url: str, api_token: str, timeout: float) -> None:
    """Synchronous POST to the telemetry ingest endpoint.

    All exceptions are caught and logged — telemetry failures must
    never crash the data pipeline.
    """
    url = f"{api_url}/api/v1/telemetry/ingest"
    headers = {
        "Authorization": f"Bearer {api_token}",
        "Content-Type": "application/json",
    }
    try:
        resp = requests.post(url, json=payload, headers=headers, timeout=timeout)
        if resp.status_code >= 400:
            logger.warning("SilentPulse ingest returned %d: %s", resp.status_code, resp.text[:200])
    except requests.RequestException as exc:
        logger.warning("SilentPulse ingest failed: %s", exc)
