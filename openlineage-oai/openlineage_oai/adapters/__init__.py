"""
Tool-specific adapters for OpenLineage integration.

Each adapter implements the ToolAdapter interface and provides
tool-specific logic for intercepting operations and emitting lineage.

Available adapters:
- mlflow: MLflow tracking store wrapper
- ray: (planned) Ray Data and Train hooks
- kfp: (planned) Kubeflow Pipelines component wrapper
- llamastack: (planned) LlamaStack inference hooks
"""

from openlineage_oai.adapters.base import ToolAdapter

__all__ = ["ToolAdapter"]
