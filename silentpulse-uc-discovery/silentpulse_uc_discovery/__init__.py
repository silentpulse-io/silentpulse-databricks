"""Auto-discover Databricks Unity Catalog assets for SilentPulse monitoring."""

__version__ = "0.1.0"

from .discovery import UCDiscovery

__all__ = ["UCDiscovery"]
