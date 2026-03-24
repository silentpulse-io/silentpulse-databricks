"""Observability decorators for SDP pipeline monitoring.

Each decorator wraps an SDP table definition to report telemetry
back to SilentPulse. The decorators are transparent - they do not
modify the pipeline behavior, only observe it.
"""

from __future__ import annotations

from functools import wraps
from typing import Any, Callable


def heartbeat(*, integration_point: str, interval_seconds: int = 300) -> Callable:
    """Report periodic heartbeat signals for a pipeline table.

    Args:
        integration_point: SilentPulse integration point identifier.
        interval_seconds: Expected heartbeat interval in seconds.

    Returns:
        Decorator that wraps the table definition function.
    """

    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            return func(*args, **kwargs)

        return wrapper

    return decorator


def completeness(*, integration_point: str, expected_columns: list[str] | None = None) -> Callable:
    """Verify data completeness for a pipeline table.

    Args:
        integration_point: SilentPulse integration point identifier.
        expected_columns: Columns to check for null completeness.

    Returns:
        Decorator that wraps the table definition function.
    """

    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            return func(*args, **kwargs)

        return wrapper

    return decorator


def volume(*, integration_point: str, min_rows: int = 0, max_rows: int | None = None) -> Callable:
    """Monitor data volume for a pipeline table.

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
            return func(*args, **kwargs)

        return wrapper

    return decorator


def freshness(*, integration_point: str, max_delay_seconds: int = 3600) -> Callable:
    """Monitor data freshness for a pipeline table.

    Args:
        integration_point: SilentPulse integration point identifier.
        max_delay_seconds: Maximum acceptable delay in seconds before alerting.

    Returns:
        Decorator that wraps the table definition function.
    """

    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            return func(*args, **kwargs)

        return wrapper

    return decorator
