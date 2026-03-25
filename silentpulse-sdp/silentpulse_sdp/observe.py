"""Observability decorators for SDP pipeline monitoring.

Each decorator wraps an SDP table definition to report telemetry
back to SilentPulse. The decorators are transparent - they do not
modify the pipeline behavior, only observe it.
"""

from __future__ import annotations

import logging
import time
from functools import wraps
from typing import Any, Callable

from . import client

logger = logging.getLogger("silentpulse_sdp")


def _iso_now() -> str:
    """Return current UTC time as ISO 8601 string."""
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


def _is_streaming(result: Any) -> bool:
    """Check if a DataFrame result is a streaming DataFrame."""
    return getattr(result, "isStreaming", False)


def heartbeat(*, integration_point: str, interval_seconds: int = 300) -> Callable:
    """Report periodic heartbeat signals for a pipeline table.

    Always fires regardless of batch or streaming mode. Sends a
    heartbeat event with the function name as the table identifier.

    Args:
        integration_point: SilentPulse integration point identifier.
        interval_seconds: Expected heartbeat interval in seconds.

    Returns:
        Decorator that wraps the table definition function.
    """

    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            result = func(*args, **kwargs)
            try:
                client.send_telemetry(
                    integration_point,
                    [
                        {
                            "type": "heartbeat",
                            "timestamp": _iso_now(),
                            "metadata": {
                                "table_name": func.__name__,
                                "interval_seconds": interval_seconds,
                            },
                        }
                    ],
                )
            except Exception:
                logger.debug("heartbeat telemetry failed for %s", func.__name__, exc_info=True)
            return result

        return wrapper

    return decorator


def completeness(*, integration_point: str, expected_columns: list[str] | None = None) -> Callable:
    """Verify data completeness for a pipeline table.

    For batch DataFrames, computes null counts per ``expected_columns``
    and reports them. Streaming DataFrames are skipped (presence only).

    Args:
        integration_point: SilentPulse integration point identifier.
        expected_columns: Columns to check for null completeness.

    Returns:
        Decorator that wraps the table definition function.
    """

    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            result = func(*args, **kwargs)
            try:
                metadata: dict[str, Any] = {"table_name": func.__name__}

                if not _is_streaming(result) and expected_columns:
                    null_counts: dict[str, int] = {}
                    for col in expected_columns:
                        if col in getattr(result, "columns", []):
                            try:
                                null_counts[col] = result.filter(result[col].isNull()).count()
                            except Exception:
                                pass
                    metadata["null_counts"] = null_counts
                    total = _safe_count(result)
                    if total is not None:
                        metadata["total_rows"] = total

                client.send_telemetry(
                    integration_point, [{"type": "completeness", "timestamp": _iso_now(), "metadata": metadata}]
                )
            except Exception:
                logger.debug("completeness telemetry failed for %s", func.__name__, exc_info=True)
            return result

        return wrapper

    return decorator


def volume(*, integration_point: str, min_rows: int = 0, max_rows: int | None = None) -> Callable:
    """Monitor data volume for a pipeline table.

    For batch DataFrames, counts rows and reports the volume.
    Streaming DataFrames only receive a heartbeat-like presence signal.

    Args:
        integration_point: SilentPulse integration point identifier.
        min_rows: Minimum expected row count per batch.
        max_rows: Maximum expected row count per batch (None for unlimited).

    Returns:
        Decorator that wraps the table definition function.
    """

    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            result = func(*args, **kwargs)
            try:
                metadata: dict[str, Any] = {"table_name": func.__name__}
                event_type = "volume"

                if _is_streaming(result):
                    event_type = "heartbeat"
                    logger.debug("volume: streaming detected, sending heartbeat for %s", func.__name__)
                else:
                    row_count = _safe_count(result)
                    if row_count is not None:
                        metadata["row_count"] = row_count
                        metadata["min_rows"] = min_rows
                        if max_rows is not None:
                            metadata["max_rows"] = max_rows

                client.send_telemetry(
                    integration_point, [{"type": event_type, "timestamp": _iso_now(), "metadata": metadata}]
                )
            except Exception:
                logger.debug("volume telemetry failed for %s", func.__name__, exc_info=True)
            return result

        return wrapper

    return decorator


def freshness(*, integration_point: str, max_delay_seconds: int = 3600) -> Callable:
    """Monitor data freshness for a pipeline table.

    For batch DataFrames, sends a freshness event with the configured
    max delay threshold. Streaming DataFrames only receive a heartbeat.

    Args:
        integration_point: SilentPulse integration point identifier.
        max_delay_seconds: Maximum acceptable delay in seconds before alerting.

    Returns:
        Decorator that wraps the table definition function.
    """

    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            result = func(*args, **kwargs)
            try:
                metadata: dict[str, Any] = {"table_name": func.__name__}

                if _is_streaming(result):
                    event_type = "heartbeat"
                    logger.debug("freshness: streaming detected, sending heartbeat for %s", func.__name__)
                else:
                    event_type = "freshness"
                    metadata["max_delay_seconds"] = max_delay_seconds

                client.send_telemetry(
                    integration_point, [{"type": event_type, "timestamp": _iso_now(), "metadata": metadata}]
                )
            except Exception:
                logger.debug("freshness telemetry failed for %s", func.__name__, exc_info=True)
            return result

        return wrapper

    return decorator


def _safe_count(df: Any) -> int | None:
    """Safely call df.count(), returning None on failure."""
    try:
        return df.count()
    except Exception:
        return None
