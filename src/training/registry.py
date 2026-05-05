"""
STAGE 4  –  MLflow Model Registry helpers.

Uses model aliases (champion/challenger) which is the recommended approach
in MLflow >= 2.9.  Falls back to legacy stage transitions where needed.
"""

import logging

import mlflow
from mlflow.tracking import MlflowClient

logger = logging.getLogger(__name__)


def register_model(
    model_uri: str,
    model_name: str,
    tracking_uri: str,
) -> int:
    """
    Register a model artifact in the MLflow Model Registry.

    Returns the new version number.
    """
    mlflow.set_tracking_uri(tracking_uri)
    result = mlflow.register_model(model_uri=model_uri, name=model_name)
    logger.info("Registered model %s version %s", model_name, result.version)
    return int(result.version)


def promote_to_alias(
    model_name: str,
    version: int,
    alias: str,
    tracking_uri: str,
) -> None:
    """Set a model alias (e.g. 'champion', 'staging') on a specific version."""
    mlflow.set_tracking_uri(tracking_uri)
    client = MlflowClient()
    client.set_registered_model_alias(model_name, alias, str(version))
    logger.info("Model %s v%d → alias '%s'", model_name, version, alias)


def get_model_uri_by_alias(
    model_name: str,
    alias: str,
    tracking_uri: str,
) -> str | None:
    """Return the model URI for a given alias, or None if not set."""
    mlflow.set_tracking_uri(tracking_uri)
    client = MlflowClient()
    try:
        mv = client.get_model_version_by_alias(model_name, alias)
        uri = f"models:/{model_name}@{alias}"
        logger.info("Alias '%s' → %s v%s", alias, model_name, mv.version)
        return uri
    except Exception:
        return None


def get_latest_version(
    model_name: str,
    tracking_uri: str,
) -> int | None:
    """Return the latest version number for a model, or None."""
    mlflow.set_tracking_uri(tracking_uri)
    client = MlflowClient()
    try:
        versions = client.search_model_versions(
            f"name='{model_name}'", order_by=["version_number DESC"], max_results=1,
        )
        if versions:
            return int(versions[0].version)
    except Exception:
        pass
    return None


def archive_version(
    model_name: str,
    version: int,
    tracking_uri: str,
) -> None:
    """Remove all aliases from a version (effectively archiving it)."""
    mlflow.set_tracking_uri(tracking_uri)
    client = MlflowClient()
    mv = client.get_model_version(model_name, str(version))
    for alias in mv.aliases:
        client.delete_registered_model_alias(model_name, alias)
    logger.info("Archived %s v%d (removed aliases)", model_name, version)


def rollback_to_version(
    model_name: str,
    version: int,
    tracking_uri: str,
) -> None:
    """Promote a specific (older) version back to champion."""
    promote_to_alias(model_name, version, "champion", tracking_uri)
    logger.info("Rolled back to %s v%d (champion)", model_name, version)


def transition_stage(
    model_name: str,
    version: int,
    stage: str,
    tracking_uri: str,
) -> None:
    """
    Move a registered model version to a lifecycle stage (e.g. Production, Staging).

    Uses MLflow's registry stages; complements alias-based promotion (:func:`promote_to_alias`).
    """
    mlflow.set_tracking_uri(tracking_uri)
    client = MlflowClient()
    client.transition_model_version_stage(
        name=model_name,
        version=str(version),
        stage=stage,
    )
    logger.info("Model %s v%s → stage %r", model_name, version, stage)
