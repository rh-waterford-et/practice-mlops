"""
URI parsing and normalization utilities.

This module provides functions for parsing the custom OpenLineage tracking URI
format used by adapters, as well as general URI normalization.

URI Format:
-----------
openlineage+<backend>://<backend-connection>?param=value

Examples:
- openlineage+postgresql://user:pass@localhost:5432/mlflow
- openlineage+http://mlflow-server:5000
- openlineage+file:///tmp/mlruns

The "openlineage+" prefix triggers our tracking store plugin.
The backend portion is extracted and used to connect to the real store.
"""

import os
from dataclasses import dataclass
from typing import Optional
from urllib.parse import parse_qs, urlencode, urlparse, urlunparse


@dataclass
class ParsedTrackingURI:
    """
    Parsed components of an OpenLineage tracking URI.

    Attributes:
        backend_uri: The underlying tracking store URI (postgresql://, http://, etc.)
        backend_scheme: Just the scheme of the backend (postgresql, http, file)
        openlineage_url: OpenLineage backend URL (from query param or env)
        openlineage_namespace: Namespace (from query param or env)
        original_uri: The original URI string
    """

    backend_uri: str
    backend_scheme: str
    openlineage_url: Optional[str]
    openlineage_namespace: Optional[str]
    original_uri: str


def parse_tracking_uri(uri: str) -> ParsedTrackingURI:
    """
    Parse an OpenLineage tracking URI into its components.

    Args:
        uri: URI like "openlineage+postgresql://user:pass@host/db?openlineage_url=http://marquez:5000"

    Returns:
        ParsedTrackingURI with extracted components

    Example:
        >>> result = parse_tracking_uri("openlineage+postgresql://localhost/mlflow")
        >>> result.backend_uri
        'postgresql://localhost/mlflow'
        >>> result.backend_scheme
        'postgresql'
    """
    original = uri

    # Check for openlineage+ prefix
    if not uri.startswith("openlineage+"):
        # Not an OpenLineage URI - return as-is
        parsed = urlparse(uri)
        return ParsedTrackingURI(
            backend_uri=uri,
            backend_scheme=parsed.scheme,
            openlineage_url=os.getenv("OPENLINEAGE_URL"),
            openlineage_namespace=os.getenv("OPENLINEAGE_NAMESPACE", "default"),
            original_uri=original,
        )

    # Remove "openlineage+" prefix
    backend_uri = uri[len("openlineage+") :]

    # Parse to extract query parameters
    parsed = urlparse(backend_uri)
    query_params = parse_qs(parsed.query)

    # Extract OpenLineage-specific params
    ol_url = query_params.pop("openlineage_url", [None])[0]
    ol_namespace = query_params.pop("openlineage_namespace", [None])[0]

    # Rebuild backend URI without OpenLineage params
    remaining_query = urlencode(query_params, doseq=True) if query_params else ""
    clean_backend = urlunparse(
        (
            parsed.scheme,
            parsed.netloc,
            parsed.path,
            parsed.params,
            remaining_query,
            parsed.fragment,
        )
    )

    # Fall back to environment variables
    if not ol_url:
        ol_url = os.getenv("OPENLINEAGE_URL")
    if not ol_namespace:
        ol_namespace = os.getenv("OPENLINEAGE_NAMESPACE", "default")

    return ParsedTrackingURI(
        backend_uri=clean_backend,
        backend_scheme=parsed.scheme,
        openlineage_url=ol_url,
        openlineage_namespace=ol_namespace,
        original_uri=original,
    )


def normalize_dataset_uri(uri: str) -> tuple:
    """
    Normalize a dataset URI to (namespace, name) tuple.

    This follows the convention:
    - namespace = URI scheme (s3, file, postgresql, etc.)
    - name = path portion without scheme

    Args:
        uri: Dataset URI like "s3://bucket/path/data.parquet"

    Returns:
        Tuple of (namespace, name)

    Example:
        >>> normalize_dataset_uri("s3://bucket/path/data.parquet")
        ('s3', 'bucket/path/data.parquet')

        >>> normalize_dataset_uri("postgresql://host:5432/db/table")
        ('postgresql', 'host:5432/db/table')

        >>> normalize_dataset_uri("/local/path/data.csv")
        ('file', 'local/path/data.csv')
    """
    # Handle local paths
    if uri.startswith("/"):
        return ("file", uri.lstrip("/"))

    try:
        parsed = urlparse(uri)

        scheme = parsed.scheme or "file"

        # Build name from netloc and path
        if parsed.netloc:
            name = f"{parsed.netloc}{parsed.path}".strip("/")
        else:
            name = parsed.path.strip("/")

        return (scheme, name or "unknown")

    except Exception:
        return ("unknown", uri)


def is_openlineage_uri(uri: str) -> bool:
    """
    Check if a URI is an OpenLineage-prefixed URI.

    Args:
        uri: URI to check

    Returns:
        True if URI starts with "openlineage+"
    """
    return uri.startswith("openlineage+")
