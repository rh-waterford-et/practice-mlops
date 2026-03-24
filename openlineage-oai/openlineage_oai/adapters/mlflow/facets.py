"""
MLflow-specific OpenLineage facets.

This module provides facet builders for MLflow-specific metadata that extends
the standard OpenLineage facets. These capture MLflow-specific information
like run IDs, experiment IDs, model flavors, etc.

Custom Facets:
- MLflowRunFacet: MLflow run metadata
- MLflowDatasetFacet: MLflow dataset input metadata
- MLflowModelFacet: MLflow model output metadata
"""

from typing import Any, Optional

from openlineage_oai.core.facets import SCHEMA_BASE

# Producer for MLflow-specific facets
MLFLOW_PRODUCER = "https://github.com/openlineage-oai/mlflow-adapter"


def create_mlflow_run_facet(
    run_id: str,
    experiment_id: str,
    experiment_name: str = "",
    run_name: str = "",
    user_id: str = "",
    lifecycle_stage: str = "active",
    params: Optional[dict[str, str]] = None,
    metrics: Optional[dict[str, float]] = None,
    tags: Optional[dict[str, str]] = None,
) -> dict[str, Any]:
    """
    Create an MLflowRunFacet for run metadata.

    This facet captures MLflow-specific run information beyond what's
    in standard OpenLineage facets.

    Args:
        run_id: MLflow run ID
        experiment_id: MLflow experiment ID
        experiment_name: Human-readable experiment name
        run_name: Human-readable run name
        user_id: User who created the run
        lifecycle_stage: Run lifecycle stage (active, deleted)
        params: Run parameters
        metrics: Run metrics (final values)
        tags: Run tags (filtered to remove system tags)

    Returns:
        MLflowRunFacet dictionary
    """
    return {
        "_producer": MLFLOW_PRODUCER,
        "_schemaURL": f"{SCHEMA_BASE}/MLflowRunFacet.json",
        "runId": run_id,
        "experimentId": experiment_id,
        "experimentName": experiment_name,
        "runName": run_name,
        "userId": user_id,
        "lifecycleStage": lifecycle_stage,
        "params": params or {},
        "metrics": metrics or {},
        "tags": tags or {},
    }


def create_mlflow_dataset_facet(
    name: str,
    source: str,
    source_type: str = "unknown",
    digest: str = "",
    context: str = "training",
    profile: Optional[dict[str, Any]] = None,
) -> dict[str, Any]:
    """
    Create an MLflowDatasetFacet for input dataset metadata.

    Args:
        name: Dataset name as registered in MLflow
        source: Dataset source URI
        source_type: Type of source (e.g., "pandas", "spark", "huggingface")
        digest: Content hash/digest
        context: Usage context (training, validation, test)
        profile: Dataset profile (num_rows, num_features, etc.)

    Returns:
        MLflowDatasetFacet dictionary
    """
    return {
        "_producer": MLFLOW_PRODUCER,
        "_schemaURL": f"{SCHEMA_BASE}/MLflowDatasetFacet.json",
        "name": name,
        "source": source,
        "sourceType": source_type,
        "digest": digest,
        "context": context,
        "profile": profile or {},
    }


def create_mlflow_model_facet(
    artifact_path: str,
    run_id: str,
    flavors: Optional[list[str]] = None,
    model_uuid: str = "",
    signature_inputs: str = "",
    signature_outputs: str = "",
    registered_model_name: Optional[str] = None,
    registered_model_version: Optional[str] = None,
) -> dict[str, Any]:
    """
    Create an MLflowModelFacet for model output metadata.

    Args:
        artifact_path: Path where model was logged
        run_id: MLflow run ID
        flavors: Model flavors (sklearn, pytorch, etc.)
        model_uuid: Unique model identifier
        signature_inputs: JSON string of input signature
        signature_outputs: JSON string of output signature
        registered_model_name: Name in model registry (if registered)
        registered_model_version: Version in model registry (if registered)

    Returns:
        MLflowModelFacet dictionary
    """
    facet = {
        "_producer": MLFLOW_PRODUCER,
        "_schemaURL": f"{SCHEMA_BASE}/MLflowModelFacet.json",
        "artifactPath": artifact_path,
        "runId": run_id,
        "flavors": flavors or [],
        "modelUuid": model_uuid,
    }

    if signature_inputs:
        facet["signatureInputs"] = signature_inputs
    if signature_outputs:
        facet["signatureOutputs"] = signature_outputs
    if registered_model_name:
        facet["registeredModelName"] = registered_model_name
    if registered_model_version:
        facet["registeredModelVersion"] = registered_model_version

    return facet


def filter_system_tags(tags: dict[str, str]) -> dict[str, str]:
    """
    Filter out MLflow system tags from user tags.

    System tags start with "mlflow." and are not useful for lineage.
    We keep only user-defined tags.

    Args:
        tags: All tags from MLflow run

    Returns:
        Filtered tags without system tags
    """
    if not tags:
        return {}

    return {key: value for key, value in tags.items() if not key.startswith("mlflow.")}
