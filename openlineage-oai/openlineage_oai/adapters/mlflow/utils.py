"""
MLflow-specific utility functions.

This module provides utilities for working with MLflow data structures
and converting them to OpenLineage format.
"""

import contextlib
import json
from typing import Any, Optional
from urllib.parse import urlparse


_SCHEME_ALIASES: dict[str, str] = {
    "postgresql": "postgres",
}


def parse_ol_identity(source_uri: str) -> tuple[str, str]:
    """Derive OpenLineage (namespace, name) from a physical source URI.

    Scheme normalisation follows the OpenLineage convention (e.g. the Spark
    ``PostgresJdbcExtractor`` maps ``postgresql`` -> ``postgres``).

    Returns:
        (namespace, name) tuple, e.g.
        "s3://raw-data/customers.csv"          -> ("s3://raw-data", "customers.csv")
        "postgresql://host:5432/db.tbl"        -> ("postgres://host:5432", "db.tbl")
        "jdbc:postgresql://host:5432/db.tbl"   -> ("postgres://host:5432", "db.tbl")
    """
    cleaned = source_uri.strip()
    if cleaned.startswith("jdbc:"):
        cleaned = cleaned[5:]

    parsed = urlparse(cleaned)
    scheme = _SCHEME_ALIASES.get(parsed.scheme, parsed.scheme) or "unknown"
    host = parsed.hostname or "localhost"
    port = parsed.port

    namespace = f"{scheme}://{host}"
    if port:
        namespace += f":{port}"

    path = parsed.path.lstrip("/")
    name = path if path else parsed.netloc
    return namespace, name


def extract_dataset_info(dataset: Any) -> dict[str, Any]:
    """
    Extract OpenLineage-relevant information from an MLflow Dataset.

    MLflow Dataset objects have various attributes depending on the source type.
    This function normalizes them into a consistent format.

    Args:
        dataset: MLflow Dataset object

    Returns:
        Dictionary with extracted information:
        - name: Dataset name
        - source_type: Type of source (pandas, spark, etc.)
        - source: Source URI or description
        - digest: Content hash
        - schema: List of field definitions (if available)
    """
    info: dict[str, Any] = {
        "name": getattr(dataset, "name", "unknown"),
        "source_type": getattr(dataset, "source_type", "unknown"),
        "source": "",
        "digest": getattr(dataset, "digest", ""),
        "schema": None,
    }

    if hasattr(dataset, "source"):
        source = dataset.source
        if hasattr(source, "to_dict"):
            source_dict = source.to_dict()
            info["source_type"] = source_dict.get("type", info["source_type"])
            info["source"] = str(source_dict.get("uri", source_dict.get("path", "")))
        else:
            try:
                source_dict = json.loads(source)
                info["source"] = source_dict.get("uri", source_dict.get("path", str(source)))
            except (json.JSONDecodeError, TypeError):
                info["source"] = str(source)

    # Try to get schema
    if hasattr(dataset, "schema"):
        schema = dataset.schema
        if schema:
            info["schema"] = _convert_mlflow_schema(schema)

            # If still None, it might be a JSON string (from mlflow.entities.Dataset)
            if info["schema"] is None and isinstance(schema, str):
                info["schema"] = _parse_schema_json(schema)

    # Try to get profile information
    if hasattr(dataset, "profile"):
        info["profile"] = getattr(dataset, "profile", {})

    return info


def _parse_schema_json(schema_str: str) -> Optional[list[dict[str, str]]]:
    """
    Parse schema from JSON string (mlflow.entities.Dataset format).

    MLflow entities store schema as JSON like:
    {"mlflow_colspec": [{"type": "double", "name": "x", "required": true}, ...]}

    Args:
        schema_str: JSON string representation of schema

    Returns:
        List of field definitions or None
    """
    import json

    try:
        schema_dict = json.loads(schema_str)

        # Handle mlflow_colspec format
        if isinstance(schema_dict, dict) and "mlflow_colspec" in schema_dict:
            fields = []
            for col in schema_dict["mlflow_colspec"]:
                if isinstance(col, dict):
                    fields.append(
                        {
                            "name": col.get("name", "unknown"),
                            "type": col.get("type", "unknown"),
                        }
                    )
            return fields if fields else None

        # Handle direct list format
        if isinstance(schema_dict, list):
            fields = []
            for col in schema_dict:
                if isinstance(col, dict):
                    fields.append(
                        {
                            "name": col.get("name", "unknown"),
                            "type": col.get("type", "unknown"),
                        }
                    )
            return fields if fields else None

    except (json.JSONDecodeError, TypeError):
        pass

    return None


def _convert_mlflow_schema(schema: Any) -> Optional[list[dict[str, str]]]:
    """
    Convert MLflow schema to OpenLineage field list.

    Args:
        schema: MLflow Schema object

    Returns:
        List of field definitions or None
    """
    if schema is None:
        return None

    fields = []

    # Handle different schema formats
    if hasattr(schema, "to_dict"):
        schema_dict = schema.to_dict()
        if isinstance(schema_dict, list):
            for col in schema_dict:
                if isinstance(col, dict):
                    fields.append(
                        {
                            "name": col.get("name", "unknown"),
                            "type": col.get("type", "unknown"),
                        }
                    )
    elif hasattr(schema, "inputs"):
        # ModelSignature schema
        for col in schema.inputs:
            if hasattr(col, "name") and hasattr(col, "type"):
                fields.append(
                    {
                        "name": col.name,
                        "type": str(col.type),
                    }
                )

    return fields if fields else None


def extract_model_info(
    artifact_path: str,
    run_id: str,
    model_info: Any = None,
) -> dict[str, Any]:
    """
    Extract OpenLineage-relevant information from an MLflow model.

    Args:
        artifact_path: Path where model was logged
        run_id: MLflow run ID
        model_info: Optional MLflow ModelInfo object

    Returns:
        Dictionary with extracted information
    """
    info: dict[str, Any] = {
        "artifact_path": artifact_path,
        "run_id": run_id,
        "flavors": [],
        "model_uuid": "",
        "signature_inputs": "",
        "signature_outputs": "",
    }

    if model_info is None:
        return info

    # Extract flavors
    if hasattr(model_info, "flavors"):
        info["flavors"] = list(model_info.flavors.keys()) if model_info.flavors else []

    # Extract UUID
    if hasattr(model_info, "model_uuid"):
        info["model_uuid"] = model_info.model_uuid or ""

    # Extract signature
    if hasattr(model_info, "signature") and model_info.signature:
        sig = model_info.signature
        if hasattr(sig, "inputs") and sig.inputs:
            with contextlib.suppress(Exception):
                info["signature_inputs"] = sig.inputs.to_json()
        if hasattr(sig, "outputs") and sig.outputs:
            with contextlib.suppress(Exception):
                info["signature_outputs"] = sig.outputs.to_json()

    return info


def build_model_namespace(tracking_uri: str) -> str:
    """
    Build the namespace for model outputs.

    Args:
        tracking_uri: MLflow tracking URI

    Returns:
        Namespace string like "mlflow://localhost:5000"
    """
    # Normalize the URI
    uri = tracking_uri.replace("://", "/")
    return f"mlflow://{uri}"


def build_model_name(run_id: str, artifact_path: str) -> str:
    """
    Build the name for a model output dataset.

    Args:
        run_id: MLflow run ID
        artifact_path: Artifact path where model was logged

    Returns:
        Name string like "runs/abc-123/artifacts/model"
    """
    return f"runs/{run_id}/artifacts/{artifact_path}"
