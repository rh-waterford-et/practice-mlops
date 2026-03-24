"""
Base adapter interface for tool-specific OpenLineage integrations.

This module defines the abstract interface that all tool adapters must implement.
Each adapter is responsible for:
1. Intercepting tool operations (via hooks, plugins, or patching)
2. Converting tool-specific data to OpenLineage events
3. Managing tool-specific state (e.g., run accumulator)

Design Decision: Abstract Base Class
------------------------------------
We use ABC rather than Protocol because:
1. Clear contract with abstract methods
2. Can include shared implementation (build_job_name)
3. Better error messages when methods not implemented
4. Runtime checking with isinstance()
"""

from abc import ABC, abstractmethod
from typing import Optional

from openlineage_oai.core.emitter import OpenLineageEmitter
from openlineage_oai.utils.naming import build_job_name


class ToolAdapter(ABC):
    """
    Abstract base class for tool-specific OpenLineage adapters.

    Each adapter implements the hooks necessary to intercept tool operations
    and emit OpenLineage events through the shared emitter.

    Example implementation:
        class MLflowAdapter(ToolAdapter):
            def get_tool_name(self) -> str:
                return "mlflow"

            def install_hooks(self) -> None:
                # Register MLflow tracking store plugin
                ...

            def uninstall_hooks(self) -> None:
                # Cleanup (if possible)
                ...

    Attributes:
        emitter: Shared OpenLineageEmitter for event emission
        namespace: Default namespace for this adapter's jobs
    """

    def __init__(
        self,
        emitter: OpenLineageEmitter,
        namespace: str = "default",
    ):
        """
        Initialize the adapter.

        Args:
            emitter: OpenLineageEmitter instance for sending events
            namespace: Default namespace for jobs created by this adapter
        """
        self.emitter = emitter
        self.namespace = namespace
        self._installed = False

    @abstractmethod
    def get_tool_name(self) -> str:
        """
        Return the tool identifier.

        This is used for:
        - Job naming (e.g., "mlflow/experiment-123/run")
        - Logging and debugging
        - Dataset registry tool parameter

        Returns:
            Tool name like "mlflow", "ray", "kfp", "llamastack"
        """
        pass

    @abstractmethod
    def install_hooks(self) -> None:
        """
        Install the tool-specific hooks or plugins.

        This method is called during openlineage_oai.init() to set up
        the integration. What "install" means varies by tool:
        - MLflow: Register tracking store plugin
        - Ray: Patch data read/write functions
        - KFP: Register component decorators

        Should be idempotent (safe to call multiple times).

        Raises:
            ImportError: If tool is not installed
            RuntimeError: If hooks cannot be installed
        """
        pass

    @abstractmethod
    def uninstall_hooks(self) -> None:
        """
        Remove hooks and restore original tool behavior.

        Called during openlineage_oai.shutdown() to clean up.
        Some adapters may not support uninstall (e.g., entry-point plugins).
        In that case, this should be a no-op.
        """
        pass

    def build_job_name(
        self,
        name: str,
        context: Optional[str] = None,
    ) -> str:
        """
        Build a job name following the unified naming convention.

        Pattern: {tool}/{context}/{name}

        Args:
            name: Job/run name
            context: Optional context (e.g., experiment ID, pipeline name)

        Returns:
            Hierarchical job name like "mlflow/experiment-123/my-run"
        """
        return build_job_name(
            tool=self.get_tool_name(),
            name=name,
            context=context,
        )

    @property
    def is_installed(self) -> bool:
        """Check if hooks are currently installed."""
        return self._installed

    def __repr__(self) -> str:
        """String representation for debugging."""
        return f"{self.__class__.__name__}(tool={self.get_tool_name()}, namespace={self.namespace})"
