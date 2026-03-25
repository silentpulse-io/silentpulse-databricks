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


def heartbeat(*, integration_point: str, interval_seconds: int = 300, asset_column: str | None = None) -> Callable:
    """Report periodic heartbeat signals for a pipeline table.

    Always fires regardless of batch or streaming mode. Sends a
    heartbeat event with the function name as the table identifier.

    Args:
        integration_point: SilentPulse integration point identifier.
        interval_seconds: Expected heartbeat interval in seconds.
        asset_column: Optional DataFrame column with asset identifiers.
            When set, sends one heartbeat per distinct value in this column
            so each asset is individually tracked by the scheduler.

    Returns:
        Decorator that wraps the table definition function.
    """

    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            result = func(*args, **kwargs)
            try:
                now = _iso_now()
                assets = _extract_assets(result, asset_column) if asset_column and not _is_streaming(result) else []
                if assets:
                    events = [
                        {
                            "type": "heartbeat",
                            "timestamp": now,
                            "metadata": {"table_name": a, "interval_seconds": interval_seconds},
                        }
                        for a in assets
                    ]
                else:
                    events = [
                        {
                            "type": "heartbeat",
                            "timestamp": now,
                            "metadata": {"table_name": func.__name__, "interval_seconds": interval_seconds},
                        }
                    ]
                client.send_telemetry(integration_point, events)
            except Exception:
                logger.debug("heartbeat telemetry failed for %s", func.__name__, exc_info=True)
            return result

        return wrapper

    return decorator


def completeness(
    *, integration_point: str, expected_columns: list[str] | None = None, asset_column: str | None = None
) -> Callable:
    """Verify data completeness for a pipeline table.

    For batch DataFrames, computes null counts per ``expected_columns``
    and reports them. Streaming DataFrames are skipped (presence only).

    Args:
        integration_point: SilentPulse integration point identifier.
        expected_columns: Columns to check for null completeness.
        asset_column: Optional DataFrame column with asset identifiers.
            When set, sends per-asset completeness events so each asset
            is individually tracked by the scheduler.

    Returns:
        Decorator that wraps the table definition function.
    """

    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            result = func(*args, **kwargs)
            try:
                now = _iso_now()

                # Compute overall null counts (shared across all assets)
                null_counts: dict[str, int] = {}
                total: int | None = None
                if not _is_streaming(result) and expected_columns:
                    for col in expected_columns:
                        if col in getattr(result, "columns", []):
                            try:
                                null_counts[col] = result.filter(result[col].isNull()).count()
                            except Exception:
                                pass
                    total = _safe_count(result)

                if asset_column and not _is_streaming(result):
                    assets = _extract_assets(result, asset_column)
                    if assets:
                        events = []
                        for a in assets:
                            meta: dict[str, Any] = {"table_name": a}
                            if null_counts:
                                meta["null_counts"] = null_counts
                            if total is not None:
                                meta["total_rows"] = total
                            events.append({"type": "completeness", "timestamp": now, "metadata": meta})
                        client.send_telemetry(integration_point, events)
                        return result

                metadata: dict[str, Any] = {"table_name": func.__name__}
                if null_counts:
                    metadata["null_counts"] = null_counts
                if total is not None:
                    metadata["total_rows"] = total
                client.send_telemetry(
                    integration_point, [{"type": "completeness", "timestamp": now, "metadata": metadata}]
                )
            except Exception:
                logger.debug("completeness telemetry failed for %s", func.__name__, exc_info=True)
            return result

        return wrapper

    return decorator


def volume(
    *, integration_point: str, min_rows: int = 0, max_rows: int | None = None, asset_column: str | None = None
) -> Callable:
    """Monitor data volume for a pipeline table.

    For batch DataFrames, counts rows and reports the volume.
    Streaming DataFrames only receive a heartbeat-like presence signal.

    Args:
        integration_point: SilentPulse integration point identifier.
        min_rows: Minimum expected row count per batch.
        max_rows: Maximum expected row count per batch (None for unlimited).
        asset_column: Optional DataFrame column with asset identifiers.
            When set, sends per-asset row counts via groupBy so the
            scheduler can track volume per individual asset.

    Returns:
        Decorator that wraps the table definition function.
    """

    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            result = func(*args, **kwargs)
            try:
                now = _iso_now()

                if _is_streaming(result):
                    logger.debug("volume: streaming detected, sending heartbeat for %s", func.__name__)
                    client.send_telemetry(
                        integration_point,
                        [{"type": "heartbeat", "timestamp": now, "metadata": {"table_name": func.__name__}}],
                    )
                elif asset_column:
                    asset_counts = _extract_asset_counts(result, asset_column)
                    if asset_counts:
                        events = [
                            {
                                "type": "volume",
                                "timestamp": now,
                                "metadata": {
                                    "table_name": a,
                                    "row_count": cnt,
                                    "min_rows": min_rows,
                                    **({"max_rows": max_rows} if max_rows is not None else {}),
                                },
                            }
                            for a, cnt in asset_counts.items()
                        ]
                        client.send_telemetry(integration_point, events)
                else:
                    metadata: dict[str, Any] = {"table_name": func.__name__}
                    row_count = _safe_count(result)
                    if row_count is not None:
                        metadata["row_count"] = row_count
                        metadata["min_rows"] = min_rows
                        if max_rows is not None:
                            metadata["max_rows"] = max_rows
                    client.send_telemetry(
                        integration_point, [{"type": "volume", "timestamp": now, "metadata": metadata}]
                    )
            except Exception:
                logger.debug("volume telemetry failed for %s", func.__name__, exc_info=True)
            return result

        return wrapper

    return decorator


def freshness(*, integration_point: str, max_delay_seconds: int = 3600, asset_column: str | None = None) -> Callable:
    """Monitor data freshness for a pipeline table.

    For batch DataFrames, sends a freshness event with the configured
    max delay threshold. Streaming DataFrames only receive a heartbeat.

    Args:
        integration_point: SilentPulse integration point identifier.
        max_delay_seconds: Maximum acceptable delay in seconds before alerting.
        asset_column: Optional DataFrame column with asset identifiers.
            When set, sends per-asset freshness events so each asset
            is individually tracked by the scheduler.

    Returns:
        Decorator that wraps the table definition function.
    """

    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            result = func(*args, **kwargs)
            try:
                now = _iso_now()

                if _is_streaming(result):
                    logger.debug("freshness: streaming detected, sending heartbeat for %s", func.__name__)
                    client.send_telemetry(
                        integration_point,
                        [{"type": "heartbeat", "timestamp": now, "metadata": {"table_name": func.__name__}}],
                    )
                elif asset_column:
                    assets = _extract_assets(result, asset_column)
                    if assets:
                        events = [
                            {
                                "type": "freshness",
                                "timestamp": now,
                                "metadata": {"table_name": a, "max_delay_seconds": max_delay_seconds},
                            }
                            for a in assets
                        ]
                        client.send_telemetry(integration_point, events)
                    else:
                        client.send_telemetry(
                            integration_point,
                            [
                                {
                                    "type": "freshness",
                                    "timestamp": now,
                                    "metadata": {"table_name": func.__name__, "max_delay_seconds": max_delay_seconds},
                                }
                            ],
                        )
                else:
                    client.send_telemetry(
                        integration_point,
                        [
                            {
                                "type": "freshness",
                                "timestamp": now,
                                "metadata": {"table_name": func.__name__, "max_delay_seconds": max_delay_seconds},
                            }
                        ],
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


def _extract_assets(df: Any, asset_column: str) -> list[str]:
    """Extract distinct non-null values from a DataFrame column."""
    try:
        rows = df.select(asset_column).distinct().filter(f"{asset_column} IS NOT NULL").collect()
        return [str(row[0]) for row in rows]
    except Exception:
        return []


def _extract_asset_counts(df: Any, asset_column: str) -> dict[str, int]:
    """Extract per-asset row counts via groupBy."""
    try:
        rows = df.groupBy(asset_column).count().collect()
        return {str(row[0]): int(row[1]) for row in rows if row[0] is not None}
    except Exception:
        return {}
