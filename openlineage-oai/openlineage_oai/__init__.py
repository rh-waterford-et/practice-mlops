"""
OpenLineage OAI - Unified OpenLineage client for OpenShift AI.

This package provides automatic OpenLineage emission for ML tools including:
- MLflow (via tracking store wrapper)
- Ray (planned)
- Kubeflow Pipelines (planned)
- LlamaStack (planned)

Usage:
    import openlineage_oai

    # Initialize with explicit configuration
    openlineage_oai.init(
        url="http://marquez:5000",
        namespace="ml-platform",
        tools=["mlflow"],
    )

    # Or use environment variables:
    # OPENLINEAGE_URL=http://marquez:5000
    # OPENLINEAGE_NAMESPACE=ml-platform
    # MLFLOW_TRACKING_URI=openlineage+postgresql://...

    # Then use tools normally - lineage is automatic!
    import mlflow
    with mlflow.start_run():
        mlflow.log_param("lr", 0.01)
    # OpenLineage events emitted automatically
"""

import warnings
from typing import Optional

from openlineage_oai.adapters.base import ToolAdapter
from openlineage_oai.core.config import OpenLineageConfig
from openlineage_oai.core.emitter import OpenLineageEmitter

__version__ = "0.1.0"

# Global state
_emitter: Optional[OpenLineageEmitter] = None
_adapters: dict = {}
_initialized: bool = False

# Available tool adapters
AVAILABLE_TOOLS = ["mlflow"]  # Will expand: "ray", "kfp", "llamastack"


def init(
    url: str = None,
    namespace: str = "default",
    registry_url: str = None,
    tools: list[str] = None,
    silent: bool = False,
) -> None:
    """
    Initialize OpenLineage for specified tools.

    Args:
        url: Marquez/OpenLineage backend URL. If not provided, reads from
             OPENLINEAGE_URL environment variable.
        namespace: Default namespace for jobs. Defaults to "default".
        registry_url: Optional Dataset Registry URL for canonical dataset IDs.
        tools: List of tools to instrument (e.g., ["mlflow", "ray"]).
               If None, attempts to initialize all available adapters.
        silent: If True, suppress status messages.

    Example:
        >>> import openlineage_oai
        >>> openlineage_oai.init(url="http://marquez:5000", tools=["mlflow"])
        ✓ OpenLineage enabled for mlflow
    """
    global _emitter, _adapters, _initialized

    # Build configuration
    if url:
        config = OpenLineageConfig(
            url=url,
            namespace=namespace,
            registry_url=registry_url,
        )
    else:
        # Load from environment
        config = OpenLineageConfig.from_env()
        if namespace != "default":
            config = config.merge_with(namespace=namespace)
        if registry_url:
            config = config.merge_with(registry_url=registry_url)

    # Warn if no URL configured
    if not config.url:
        warnings.warn(
            "OpenLineage URL not configured. Set OPENLINEAGE_URL or pass url parameter.",
            stacklevel=2,
        )

    # Create emitter
    _emitter = OpenLineageEmitter(config)

    # Determine which tools to initialize
    tools_to_init = tools if tools else AVAILABLE_TOOLS

    # Initialize adapters
    for tool in tools_to_init:
        try:
            adapter = _load_adapter(tool, _emitter, config.namespace)
            adapter.install_hooks()
            _adapters[tool] = adapter
            if not silent:
                print(f"✓ OpenLineage enabled for {tool}")
        except ImportError:
            if not silent:
                print(f"⚠ {tool} not installed, skipping")
        except Exception as e:
            if not silent:
                print(f"⚠ Failed to initialize {tool}: {e}")

    _initialized = True


def shutdown() -> None:
    """
    Disable OpenLineage and restore original tool behavior.

    Call this to cleanly uninstall hooks when shutting down.
    Note: Some adapters (like MLflow's entry-point based plugin) cannot
    be fully uninstalled at runtime.
    """
    global _adapters, _initialized

    for tool, adapter in list(_adapters.items()):
        try:
            adapter.uninstall_hooks()
            print(f"✓ OpenLineage disabled for {tool}")
        except Exception as e:
            print(f"⚠ Failed to disable {tool}: {e}")

    _adapters = {}
    _initialized = False


def get_emitter() -> Optional[OpenLineageEmitter]:
    """
    Get the core OpenLineage emitter for advanced usage.

    Returns:
        The OpenLineageEmitter instance, or None if not initialized.

    Example:
        >>> emitter = openlineage_oai.get_emitter()
        >>> if emitter:
        ...     emitter.emit_create_dataset("my-dataset", "s3")
    """
    return _emitter


def get_adapter(tool: str) -> Optional[ToolAdapter]:
    """
    Get a specific tool adapter.

    Args:
        tool: Tool name (e.g., "mlflow")

    Returns:
        The ToolAdapter instance, or None if not initialized.
    """
    return _adapters.get(tool)


def is_initialized() -> bool:
    """Check if OpenLineage OAI has been initialized."""
    return _initialized


def _load_adapter(tool: str, emitter: OpenLineageEmitter, namespace: str) -> ToolAdapter:
    """
    Dynamically load and instantiate a tool adapter.

    Args:
        tool: Tool name
        emitter: Shared emitter instance
        namespace: Default namespace

    Returns:
        Instantiated ToolAdapter

    Raises:
        ImportError: If tool is not installed
        ValueError: If tool is unknown
    """
    if tool == "mlflow":
        from openlineage_oai.adapters.mlflow import MLflowAdapter

        return MLflowAdapter(emitter, namespace)
    # Future adapters:
    # elif tool == "ray":
    #     from openlineage_oai.adapters.ray import RayAdapter
    #     return RayAdapter(emitter, namespace)
    # elif tool == "kfp":
    #     from openlineage_oai.adapters.kfp import KFPAdapter
    #     return KFPAdapter(emitter, namespace)
    # elif tool == "llamastack":
    #     from openlineage_oai.adapters.llamastack import LlamaStackAdapter
    #     return LlamaStackAdapter(emitter, namespace)
    else:
        raise ValueError(f"Unknown tool: {tool}. Available: {AVAILABLE_TOOLS}")


__all__ = [
    "__version__",
    "init",
    "shutdown",
    "get_emitter",
    "get_adapter",
    "is_initialized",
    "AVAILABLE_TOOLS",
]
