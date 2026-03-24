"""
MLflow adapter for OpenLineage.

This adapter uses MLflow's native tracking store plugin system to automatically
emit OpenLineage events when MLflow operations are performed.

Usage:
    # Option 1: Programmatic
    import openlineage_oai
    openlineage_oai.init(url="http://marquez:5000", tools=["mlflow"])

    # Option 2: Environment variables
    # MLFLOW_TRACKING_URI=openlineage+postgresql://user:pass@localhost/mlflow
    # OPENLINEAGE_URL=http://marquez:5000

How it works:
    1. When MLFLOW_TRACKING_URI starts with "openlineage+", MLflow loads our
       OpenLineageTrackingStore via Python entry points.
    2. Our store wraps the real tracking store (PostgreSQL, REST, etc.)
    3. All MLflow operations go through our store, which:
       - Delegates to the real store (MLflow works normally)
       - Emits OpenLineage events (lineage is captured)
"""

from openlineage_oai.adapters.base import ToolAdapter


class MLflowAdapter(ToolAdapter):
    """
    MLflow adapter using tracking store wrapper.

    This adapter installs OpenLineage integration by registering
    our tracking store plugin with MLflow. The actual interception
    happens in OpenLineageTrackingStore.

    Note: The tracking store is primarily activated via the URI scheme
    (openlineage+postgresql://...) and Python entry points, not by
    calling install_hooks(). The install_hooks() method is provided
    for completeness and potential future runtime registration.
    """

    def get_tool_name(self) -> str:
        return "mlflow"

    def install_hooks(self) -> None:
        """
        Install MLflow OpenLineage integration.

        Note: The primary installation mechanism is via entry points
        registered in pyproject.toml. When users set:

            MLFLOW_TRACKING_URI=openlineage+postgresql://...

        MLflow automatically discovers and loads our tracking store.

        This method is provided for:
        1. Completeness of the ToolAdapter interface
        2. Potential runtime registration in future MLflow versions
        3. Explicit verification that MLflow is available
        """
        try:
            import mlflow  # noqa: F401

            self._installed = True
        except ImportError as e:
            raise ImportError("MLflow is not installed. Install it with: pip install mlflow") from e

    def uninstall_hooks(self) -> None:
        """
        Uninstall MLflow OpenLineage integration.

        Note: Entry point-based plugins cannot be dynamically unregistered.
        This method marks the adapter as uninstalled but the tracking store
        will remain registered with MLflow.

        To fully disable, users should change MLFLOW_TRACKING_URI to not
        use the "openlineage+" prefix.
        """
        self._installed = False


# Export the tracking store for entry point discovery
from openlineage_oai.adapters.mlflow.tracking_store import OpenLineageTrackingStore  # noqa: E402

__all__ = [
    "MLflowAdapter",
    "OpenLineageTrackingStore",
]
