"""
Configuration management for OpenLineage ML.

This module provides centralized configuration handling with support for:
- Environment variables (primary)
- Explicit initialization (override)
- Sensible defaults

Design Decision: Environment Variables First
--------------------------------------------
We prioritize environment variables because:
1. Works with existing deployment patterns (K8s ConfigMaps, Secrets)
2. No code changes required for configuration
3. Matches how MLflow and other ML tools handle config
4. Supports different configs per environment (dev/staging/prod)
"""

import os
from dataclasses import dataclass
from typing import Optional

# Environment variable names
ENV_URL = "OPENLINEAGE_URL"
ENV_NAMESPACE = "OPENLINEAGE_NAMESPACE"
ENV_API_KEY = "OPENLINEAGE_API_KEY"
ENV_TIMEOUT = "OPENLINEAGE_TIMEOUT"
ENV_REGISTRY_URL = "OPENLINEAGE_REGISTRY_URL"
ENV_EMIT_ON_START = "OPENLINEAGE_EMIT_ON_START"
ENV_EMIT_ON_END = "OPENLINEAGE_EMIT_ON_END"
ENV_ENABLED = "OPENLINEAGE_ENABLED"

# Default values
DEFAULT_NAMESPACE = "default"
DEFAULT_TIMEOUT = 5
DEFAULT_PRODUCER = "https://github.com/openlineage-oai"


@dataclass
class OpenLineageConfig:
    """
    Configuration for OpenLineage event emission.

    Attributes:
        url: OpenLineage backend URL (e.g., Marquez API endpoint)
        namespace: Default namespace for jobs and datasets
        api_key: Optional API key for authenticated backends
        timeout_seconds: HTTP timeout for event emission
        registry_url: Optional Dataset Registry URL for canonical IDs
        emit_on_start: Whether to emit START events
        emit_on_end: Whether to emit COMPLETE/FAIL events
        enabled: Master switch to enable/disable all emission
        producer: Producer identifier in OpenLineage events

    Example:
        # From environment
        config = OpenLineageConfig.from_env()

        # Explicit
        config = OpenLineageConfig(
            url="http://marquez:5000",
            namespace="ml-platform",
        )
    """

    url: Optional[str] = None
    namespace: str = DEFAULT_NAMESPACE
    api_key: Optional[str] = None
    timeout_seconds: int = DEFAULT_TIMEOUT
    registry_url: Optional[str] = None
    emit_on_start: bool = True
    emit_on_end: bool = True
    enabled: bool = True
    producer: str = DEFAULT_PRODUCER

    def __post_init__(self):
        """Validate configuration after initialization."""
        if self.timeout_seconds <= 0:
            raise ValueError(f"timeout_seconds must be positive, got {self.timeout_seconds}")

    @classmethod
    def from_env(cls) -> "OpenLineageConfig":
        """
        Create configuration from environment variables.

        Environment Variables:
            OPENLINEAGE_URL: Backend URL (required for emission)
            OPENLINEAGE_NAMESPACE: Job namespace (default: "default")
            OPENLINEAGE_API_KEY: API key for auth
            OPENLINEAGE_TIMEOUT: HTTP timeout in seconds (default: 5)
            OPENLINEAGE_REGISTRY_URL: Dataset registry URL
            OPENLINEAGE_EMIT_ON_START: "true"/"false" (default: true)
            OPENLINEAGE_EMIT_ON_END: "true"/"false" (default: true)
            OPENLINEAGE_ENABLED: "true"/"false" (default: true)

        Returns:
            OpenLineageConfig instance
        """
        return cls(
            url=os.getenv(ENV_URL),
            namespace=os.getenv(ENV_NAMESPACE, DEFAULT_NAMESPACE),
            api_key=os.getenv(ENV_API_KEY),
            timeout_seconds=_parse_int(os.getenv(ENV_TIMEOUT), DEFAULT_TIMEOUT),
            registry_url=os.getenv(ENV_REGISTRY_URL),
            emit_on_start=_parse_bool(os.getenv(ENV_EMIT_ON_START), True),
            emit_on_end=_parse_bool(os.getenv(ENV_EMIT_ON_END), True),
            enabled=_parse_bool(os.getenv(ENV_ENABLED), True),
        )

    @property
    def lineage_url(self) -> str:
        """
        Get the full URL for posting lineage events.

        Returns:
            URL with /api/v1/lineage path appended if needed
        """
        if not self.url:
            return ""

        url = self.url.rstrip("/")

        # If URL already ends with /lineage, use as-is
        if url.endswith("/lineage"):
            return url

        # If URL ends with /api/v1, append /lineage
        if url.endswith("/api/v1"):
            return f"{url}/lineage"

        # Otherwise, append the full path
        return f"{url}/api/v1/lineage"

    def is_valid(self) -> bool:
        """
        Check if configuration is valid for emission.

        Returns:
            True if enabled and URL is configured
        """
        return self.enabled and bool(self.url)

    def merge_with(self, **overrides) -> "OpenLineageConfig":
        """
        Create a new config with specified overrides.

        Args:
            **overrides: Fields to override

        Returns:
            New OpenLineageConfig with overrides applied
        """
        current = {
            "url": self.url,
            "namespace": self.namespace,
            "api_key": self.api_key,
            "timeout_seconds": self.timeout_seconds,
            "registry_url": self.registry_url,
            "emit_on_start": self.emit_on_start,
            "emit_on_end": self.emit_on_end,
            "enabled": self.enabled,
            "producer": self.producer,
        }
        current.update({k: v for k, v in overrides.items() if v is not None})
        return OpenLineageConfig(**current)


def _parse_bool(value: Optional[str], default: bool) -> bool:
    """Parse a boolean from string."""
    if value is None:
        return default
    return value.lower() in ("true", "1", "yes", "on")


def _parse_int(value: Optional[str], default: int) -> int:
    """Parse an integer from string."""
    if value is None:
        return default
    try:
        return int(value)
    except ValueError:
        return default
