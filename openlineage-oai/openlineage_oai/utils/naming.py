"""
Job naming utilities for consistent naming across tools.

This module provides functions to build job names that follow a consistent
pattern across all tool adapters, enabling better organization in lineage UIs.

Design Decision: Hierarchical Job Names
---------------------------------------
We use a hierarchical naming pattern: {tool}/{context}/{name}

For example:
- mlflow/experiment-123/training-run
- ray/data-pipeline/etl-job
- kfp/my-pipeline/component-train

This provides:
1. Clear grouping by tool in Marquez UI
2. Ability to filter/search by tool or experiment
3. Human-readable job identification
"""

import re
from typing import Optional


def build_job_name(
    tool: str,
    name: str,
    context: Optional[str] = None,
) -> str:
    """
    Build a hierarchical job name.

    Args:
        tool: Tool identifier (e.g., "mlflow", "ray", "kfp")
        name: Job/run name
        context: Optional context (e.g., experiment ID, pipeline name)

    Returns:
        Hierarchical job name like "mlflow/experiment-123/my-run"

    Example:
        >>> build_job_name("mlflow", "training-run", context="experiment-123")
        'mlflow/experiment-123/training-run'

        >>> build_job_name("ray", "etl-job")
        'ray/etl-job'
    """
    # Sanitize components
    tool = sanitize_name(tool)
    name = sanitize_name(name)

    if context:
        context = sanitize_name(context)
        return f"{tool}/{context}/{name}"
    else:
        return f"{tool}/{name}"


def build_mlflow_job_name(
    experiment_id: str,
    run_name: Optional[str] = None,
    run_id: Optional[str] = None,
    experiment_name: Optional[str] = None,
) -> str:
    """
    Build a job name for MLflow runs.

    Uses experiment name when available for a readable Marquez UI label.

    Args:
        experiment_id: MLflow experiment ID
        run_name: Human-readable run name (unused, kept for API compat)
        run_id: MLflow run ID (unused, kept for API compat)
        experiment_name: Human-readable experiment name (preferred)

    Returns:
        Job name like "mlflow-customer-churn-lineage"

    Example:
        >>> build_mlflow_job_name("1", experiment_name="customer_churn_lineage")
        'mlflow-customer-churn-lineage'

        >>> build_mlflow_job_name("1")
        'mlflow-experiment-1'
    """
    label = experiment_name or f"experiment-{experiment_id}"
    return f"mlflow-{sanitize_name(label)}"


def sanitize_name(name: str) -> str:
    """
    Sanitize a name for use in job identifiers.

    - Converts to lowercase
    - Replaces spaces and underscores with hyphens
    - Removes characters that aren't alphanumeric, hyphens, or dots
    - Collapses multiple hyphens
    - Strips leading/trailing hyphens

    Args:
        name: Raw name to sanitize

    Returns:
        Sanitized name safe for use in identifiers

    Example:
        >>> sanitize_name("My Training Run_v2")
        'my-training-run-v2'

        >>> sanitize_name("experiment@#$123")
        'experiment123'
    """
    # Convert to lowercase
    result = name.lower()

    # Replace spaces and underscores with hyphens
    result = result.replace(" ", "-").replace("_", "-")

    # Keep only alphanumeric, hyphens, and dots
    result = re.sub(r"[^a-z0-9\-.]", "", result)

    # Collapse multiple hyphens
    result = re.sub(r"-+", "-", result)

    # Strip leading/trailing hyphens
    result = result.strip("-")

    return result or "unnamed"


def extract_namespace_from_uri(uri: str) -> str:
    """
    Extract a namespace from a URI.

    Args:
        uri: URI like "s3://bucket/path" or "postgresql://host/db"

    Returns:
        Namespace string (scheme portion of URI)

    Example:
        >>> extract_namespace_from_uri("s3://bucket/data.parquet")
        's3'

        >>> extract_namespace_from_uri("postgresql://host:5432/db")
        'postgresql'
    """
    from urllib.parse import urlparse

    try:
        parsed = urlparse(uri)
        return parsed.scheme or "unknown"
    except Exception:
        return "unknown"
