"""
Dataset Registry client for canonical dataset identity resolution.

This module provides integration with a central Dataset Registry service
that maps tool-specific dataset references to canonical identifiers.

Design Decision: Graceful Fallback
----------------------------------
The registry is OPTIONAL. When unavailable or not configured:
1. We fall back to using the original namespace/name
2. No exceptions are raised
3. Lineage still works, just without cross-tool dataset correlation

This ensures that:
- Lineage works out-of-the-box without a registry
- Registry outages don't break ML training
- Teams can adopt the registry incrementally
"""

import warnings
from dataclasses import dataclass
from typing import Optional

import requests


@dataclass
class ResolvedDataset:
    """
    Result of dataset resolution.

    Attributes:
        namespace: Canonical namespace (or original if not resolved)
        name: Canonical name (or original if not resolved)
        registry_id: Registry-assigned ID (None if not found in registry)
        resolved: Whether the dataset was found in the registry
    """

    namespace: str
    name: str
    registry_id: Optional[str] = None
    resolved: bool = False


class RegistryClient:
    """
    Client for the Dataset Registry service.

    The registry provides canonical identifiers for datasets, enabling
    cross-tool lineage correlation. For example:
    - MLflow logs input as "iris" with source_type "pandas"
    - Ray reads the same data as "s3://bucket/iris.parquet"
    - The registry maps both to canonical ID "dataset-iris-v1"

    Example:
        client = RegistryClient(url="http://registry:8080")
        result = client.resolve(namespace="pandas", name="iris")
        if result.resolved:
            print(f"Canonical: {result.namespace}/{result.name}")
    """

    def __init__(
        self,
        url: str,
        timeout: int = 5,
        enabled: bool = True,
    ):
        """
        Initialize the registry client.

        Args:
            url: Registry service URL
            timeout: HTTP timeout in seconds
            enabled: Whether to actually query the registry
        """
        self.url = url.rstrip("/")
        self.timeout = timeout
        self.enabled = enabled

    def resolve(
        self,
        namespace: str,
        name: str,
        tool: str = "unknown",
    ) -> ResolvedDataset:
        """
        Resolve a dataset reference to its canonical identifier.

        Args:
            namespace: Tool-specific namespace (e.g., "pandas", "s3")
            name: Tool-specific name (e.g., "iris", "bucket/data.parquet")
            tool: Tool name for context (e.g., "mlflow", "ray")

        Returns:
            ResolvedDataset with canonical namespace/name, or original if not found
        """
        if not self.enabled:
            return ResolvedDataset(namespace=namespace, name=name)

        try:
            response = requests.get(
                f"{self.url}/api/v1/datasets/resolve",
                params={
                    "namespace": namespace,
                    "name": name,
                    "tool": tool,
                },
                timeout=self.timeout,
            )

            if response.status_code == 200:
                data = response.json()
                return ResolvedDataset(
                    namespace=data.get("canonical_namespace", namespace),
                    name=data.get("canonical_name", name),
                    registry_id=data.get("id"),
                    resolved=True,
                )
            elif response.status_code == 404:
                # Dataset not registered - use original
                return ResolvedDataset(namespace=namespace, name=name)
            else:
                warnings.warn(
                    f"Registry returned unexpected status: {response.status_code}",
                    stacklevel=2,
                )
                return ResolvedDataset(namespace=namespace, name=name)

        except requests.exceptions.Timeout:
            warnings.warn(
                f"Registry timeout ({self.timeout}s) - using original dataset ID",
                stacklevel=2,
            )
            return ResolvedDataset(namespace=namespace, name=name)

        except requests.exceptions.ConnectionError:
            warnings.warn(
                f"Registry not reachable at {self.url} - using original dataset ID",
                stacklevel=2,
            )
            return ResolvedDataset(namespace=namespace, name=name)

        except Exception as e:
            warnings.warn(
                f"Registry error: {e} - using original dataset ID",
                stacklevel=2,
            )
            return ResolvedDataset(namespace=namespace, name=name)

    def register(
        self,
        namespace: str,
        name: str,
        canonical_namespace: Optional[str] = None,
        canonical_name: Optional[str] = None,
        tool: str = "unknown",
        metadata: Optional[dict] = None,
    ) -> Optional[str]:
        """
        Register a dataset in the registry.

        Args:
            namespace: Tool-specific namespace
            name: Tool-specific name
            canonical_namespace: Desired canonical namespace (optional)
            canonical_name: Desired canonical name (optional)
            tool: Tool name
            metadata: Additional metadata to store

        Returns:
            Registry-assigned ID, or None if registration failed
        """
        if not self.enabled:
            return None

        try:
            payload = {
                "namespace": namespace,
                "name": name,
                "tool": tool,
            }

            if canonical_namespace:
                payload["canonical_namespace"] = canonical_namespace
            if canonical_name:
                payload["canonical_name"] = canonical_name
            if metadata:
                payload["metadata"] = metadata

            response = requests.post(
                f"{self.url}/api/v1/datasets",
                json=payload,
                timeout=self.timeout,
            )

            if response.status_code in (200, 201):
                data = response.json()
                return data.get("id")
            else:
                warnings.warn(
                    f"Failed to register dataset: HTTP {response.status_code}",
                    stacklevel=2,
                )
                return None

        except Exception as e:
            warnings.warn(f"Failed to register dataset: {e}", stacklevel=2)
            return None


def resolve_with_fallback(
    registry: Optional[RegistryClient],
    namespace: str,
    name: str,
    tool: str = "unknown",
) -> ResolvedDataset:
    """
    Resolve a dataset with fallback when registry is not available.

    This is the recommended function to use - it handles the case where
    no registry is configured.

    Args:
        registry: Optional registry client (can be None)
        namespace: Tool-specific namespace
        name: Tool-specific name
        tool: Tool name

    Returns:
        ResolvedDataset with canonical or original namespace/name
    """
    if registry is None:
        return ResolvedDataset(namespace=namespace, name=name)

    return registry.resolve(namespace=namespace, name=name, tool=tool)
