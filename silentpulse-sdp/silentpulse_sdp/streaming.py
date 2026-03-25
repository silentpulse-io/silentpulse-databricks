"""Streaming telemetry via Spark foreachBatch integration.

Provides ``report_batch()`` and ``streaming_sink()`` for reporting
telemetry from structured-streaming micro-batches. Each micro-batch
is a regular DataFrame, so all aggregation operations work.
"""

from __future__ import annotations

import logging
from typing import Any, Callable

from . import client
from .observe import _extract_asset_counts, _iso_now, _safe_count

logger = logging.getLogger("silentpulse_sdp")


def report_batch(
    batch_df: Any,
    batch_id: int,
    *,
    integration_point: str,
    asset_column: str | None = None,
    min_rows: int = 0,
    max_rows: int | None = None,
    expected_columns: list[str] | None = None,
    max_delay_seconds: int | None = None,
) -> None:
    """Report telemetry for a single streaming micro-batch.

    Call this inside a ``foreachBatch`` handler to get per-batch
    heartbeat, volume, completeness, and freshness telemetry.

    All exceptions are swallowed so the pipeline is never interrupted.

    Args:
        batch_df: The micro-batch DataFrame (regular, not streaming).
        batch_id: Spark-assigned batch identifier.
        integration_point: SilentPulse integration point name.
        asset_column: Optional column with asset identifiers for per-asset tracking.
        min_rows: Minimum expected row count per batch.
        max_rows: Maximum expected row count per batch (None for unlimited).
        expected_columns: Columns to check for null completeness.
        max_delay_seconds: Maximum acceptable delay in seconds before alerting.
    """
    try:
        _report_batch_impl(
            batch_df,
            batch_id,
            integration_point=integration_point,
            asset_column=asset_column,
            min_rows=min_rows,
            max_rows=max_rows,
            expected_columns=expected_columns,
            max_delay_seconds=max_delay_seconds,
        )
    except Exception:
        logger.debug("streaming telemetry failed for batch %s", batch_id, exc_info=True)


def streaming_sink(
    *,
    integration_point: str,
    asset_column: str | None = None,
    min_rows: int = 0,
    max_rows: int | None = None,
    expected_columns: list[str] | None = None,
    max_delay_seconds: int | None = None,
) -> Callable[[Any, int], None]:
    """Factory returning a ``foreachBatch`` handler with telemetry.

    Usage::

        sink = streaming_sink(integration_point="zerobus-ingest", asset_column="host")
        query = df.writeStream.foreachBatch(sink).start()

    Args:
        integration_point: SilentPulse integration point name.
        asset_column: Optional column with asset identifiers.
        min_rows: Minimum expected row count per batch.
        max_rows: Maximum expected row count per batch.
        expected_columns: Columns to check for null completeness.
        max_delay_seconds: Maximum acceptable delay in seconds.

    Returns:
        A callable ``(batch_df, batch_id) -> None`` suitable for ``foreachBatch``.
    """

    def handler(batch_df: Any, batch_id: int) -> None:
        report_batch(
            batch_df,
            batch_id,
            integration_point=integration_point,
            asset_column=asset_column,
            min_rows=min_rows,
            max_rows=max_rows,
            expected_columns=expected_columns,
            max_delay_seconds=max_delay_seconds,
        )

    return handler


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _report_batch_impl(
    batch_df: Any,
    batch_id: int,
    *,
    integration_point: str,
    asset_column: str | None,
    min_rows: int,
    max_rows: int | None,
    expected_columns: list[str] | None,
    max_delay_seconds: int | None,
) -> None:
    """Core logic — may raise; caller wraps in try/except."""
    now = _iso_now()
    events: list[dict[str, Any]] = []

    null_counts: dict[str, int] | None = None
    if expected_columns:
        null_counts = _compute_null_counts(batch_df, expected_columns)

    if asset_column:
        asset_counts = _extract_asset_counts(batch_df, asset_column)
        if asset_counts:
            total = sum(asset_counts.values())
            for asset_name, count in asset_counts.items():
                _append_events_for_subject(
                    events,
                    now=now,
                    table_name=asset_name,
                    batch_id=batch_id,
                    row_count=count,
                    min_rows=min_rows,
                    max_rows=max_rows,
                    expected_columns=expected_columns,
                    null_counts=null_counts,
                    total_rows=total,
                    max_delay_seconds=max_delay_seconds,
                )
        else:
            # asset_column extraction failed — fall back to table-level
            row_count = _safe_count(batch_df)
            _append_events_for_subject(
                events,
                now=now,
                table_name=integration_point,
                batch_id=batch_id,
                row_count=row_count if row_count is not None else 0,
                min_rows=min_rows,
                max_rows=max_rows,
                expected_columns=expected_columns,
                null_counts=null_counts,
                total_rows=row_count,
                max_delay_seconds=max_delay_seconds,
            )
    else:
        row_count = _safe_count(batch_df)
        _append_events_for_subject(
            events,
            now=now,
            table_name=integration_point,
            batch_id=batch_id,
            row_count=row_count if row_count is not None else 0,
            min_rows=min_rows,
            max_rows=max_rows,
            expected_columns=expected_columns,
            null_counts=null_counts,
            total_rows=row_count,
            max_delay_seconds=max_delay_seconds,
        )

    if events:
        client.send_telemetry(integration_point, events)


def _append_events_for_subject(
    events: list[dict[str, Any]],
    *,
    now: str,
    table_name: str,
    batch_id: int,
    row_count: int,
    min_rows: int,
    max_rows: int | None,
    expected_columns: list[str] | None,
    null_counts: dict[str, int] | None,
    total_rows: int | None,
    max_delay_seconds: int | None,
) -> None:
    """Append heartbeat + volume + optional completeness/freshness events."""
    # Heartbeat — always
    events.append(
        {
            "type": "heartbeat",
            "timestamp": now,
            "metadata": {"table_name": table_name, "batch_id": batch_id},
        }
    )

    # Volume — always
    vol_meta: dict[str, Any] = {
        "table_name": table_name,
        "row_count": row_count,
        "min_rows": min_rows,
        "batch_id": batch_id,
    }
    if max_rows is not None:
        vol_meta["max_rows"] = max_rows
    events.append({"type": "volume", "timestamp": now, "metadata": vol_meta})

    # Completeness — if expected_columns
    if expected_columns:
        comp_meta: dict[str, Any] = {"table_name": table_name}
        if null_counts:
            comp_meta["null_counts"] = null_counts
        if total_rows is not None:
            comp_meta["total_rows"] = total_rows
        events.append({"type": "completeness", "timestamp": now, "metadata": comp_meta})

    # Freshness — if max_delay_seconds
    if max_delay_seconds is not None:
        events.append(
            {
                "type": "freshness",
                "timestamp": now,
                "metadata": {"table_name": table_name, "max_delay_seconds": max_delay_seconds},
            }
        )


def _compute_null_counts(df: Any, expected_columns: list[str]) -> dict[str, int]:
    """Compute null count per column, silently skipping failures."""
    result: dict[str, int] = {}
    columns = getattr(df, "columns", [])
    for col in expected_columns:
        if col in columns:
            try:
                result[col] = df.filter(df[col].isNull()).count()
            except Exception:
                pass
    return result
