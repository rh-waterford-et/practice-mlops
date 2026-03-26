"""
Core OpenLineage event emitter.

This module handles the construction and HTTP transmission of OpenLineage events.
It's designed to be tool-agnostic - adapters use this to emit events.

Design Decisions:
-----------------
1. Non-blocking: Emission failures log warnings but don't raise exceptions.
   This ensures that lineage issues don't break ML training.

2. Stateless events: Each emit_* method is self-contained. State management
   (accumulating params, metrics, etc.) is handled by adapters.

3. Direct HTTP: We use requests directly rather than the OpenLineage SDK to:
   - Minimize dependencies
   - Have full control over error handling
   - Avoid SDK version compatibility issues
"""

import uuid
import warnings
from datetime import datetime, timezone
from typing import Any, Optional

import requests

from openlineage_oai.core.config import OpenLineageConfig

# OpenLineage schema URLs
RUN_EVENT_SCHEMA = "https://openlineage.io/spec/2-0-0/OpenLineage.json#/definitions/RunEvent"
DATASET_EVENT_SCHEMA = (
    "https://openlineage.io/spec/2-0-0/OpenLineage.json#/definitions/DatasetEvent"
)


class OpenLineageEmitter:
    """
    Core emitter for OpenLineage events.

    This class handles building and sending OpenLineage events to the backend.
    It's designed to be used by tool-specific adapters.

    Example:
        config = OpenLineageConfig(url="http://marquez:5000", namespace="ml")
        emitter = OpenLineageEmitter(config)

        emitter.emit_run_event(
            event_type="START",
            run_id="run-123",
            job_name="training-job",
        )
    """

    def __init__(self, config: Optional[OpenLineageConfig] = None):
        """
        Initialize the emitter.

        Args:
            config: Configuration object. If not provided, loads from environment.
        """
        self.config = config or OpenLineageConfig.from_env()

    @property
    def is_enabled(self) -> bool:
        """Check if emitter is enabled and properly configured."""
        return self.config.is_valid()

    # ========================================================================
    # RunEvent Methods
    # ========================================================================

    def emit_run_event(
        self,
        event_type: str,
        run_id: str,
        job_name: str,
        job_namespace: Optional[str] = None,
        inputs: Optional[list[dict[str, Any]]] = None,
        outputs: Optional[list[dict[str, Any]]] = None,
        run_facets: Optional[dict[str, Any]] = None,
        job_facets: Optional[dict[str, Any]] = None,
    ) -> bool:
        """
        Emit an OpenLineage RunEvent.

        Args:
            event_type: One of "START", "RUNNING", "COMPLETE", "FAIL", "ABORT"
            run_id: Unique identifier for this run
            job_name: Name of the job
            job_namespace: Job namespace (defaults to config.namespace)
            inputs: List of input datasets
            outputs: List of output datasets
            run_facets: Facets to attach to the run
            job_facets: Facets to attach to the job

        Returns:
            True if event was sent successfully, False otherwise
        """
        if not self.is_enabled:
            return False

        event = self._build_run_event(
            event_type=event_type,
            run_id=run_id,
            job_name=job_name,
            job_namespace=job_namespace or self.config.namespace,
            inputs=inputs or [],
            outputs=outputs or [],
            run_facets=run_facets or {},
            job_facets=job_facets or {},
        )

        return self._send_event(event)

    def emit_start(
        self,
        run_id: str,
        job_name: str,
        job_namespace: Optional[str] = None,
        inputs: Optional[list[dict[str, Any]]] = None,
        outputs: Optional[list[dict[str, Any]]] = None,
        run_facets: Optional[dict[str, Any]] = None,
        job_facets: Optional[dict[str, Any]] = None,
    ) -> bool:
        """
        Emit a START event for a new run.

        Args:
            run_id: Unique run identifier
            job_name: Name of the job
            job_namespace: Job namespace
            inputs: Initial input datasets (if known)
            outputs: Initial output datasets (if known)
            run_facets: Run facets
            job_facets: Job facets

        Returns:
            True if event was sent successfully
        """
        if not self.config.emit_on_start:
            return False

        return self.emit_run_event(
            event_type="START",
            run_id=run_id,
            job_name=job_name,
            job_namespace=job_namespace,
            inputs=inputs,
            outputs=outputs,
            run_facets=run_facets,
            job_facets=job_facets,
        )

    def emit_complete(
        self,
        run_id: str,
        job_name: str,
        job_namespace: Optional[str] = None,
        inputs: Optional[list[dict[str, Any]]] = None,
        outputs: Optional[list[dict[str, Any]]] = None,
        run_facets: Optional[dict[str, Any]] = None,
        job_facets: Optional[dict[str, Any]] = None,
    ) -> bool:
        """
        Emit a COMPLETE event for a successful run.

        Args:
            run_id: Unique run identifier
            job_name: Name of the job
            job_namespace: Job namespace
            inputs: Input datasets
            outputs: Output datasets
            run_facets: Run facets (params, metrics, etc.)
            job_facets: Job facets

        Returns:
            True if event was sent successfully
        """
        if not self.config.emit_on_end:
            return False

        return self.emit_run_event(
            event_type="COMPLETE",
            run_id=run_id,
            job_name=job_name,
            job_namespace=job_namespace,
            inputs=inputs,
            outputs=outputs,
            run_facets=run_facets,
            job_facets=job_facets,
        )

    def emit_fail(
        self,
        run_id: str,
        job_name: str,
        error_message: str,
        stack_trace: Optional[str] = None,
        job_namespace: Optional[str] = None,
        inputs: Optional[list[dict[str, Any]]] = None,
        outputs: Optional[list[dict[str, Any]]] = None,
        run_facets: Optional[dict[str, Any]] = None,
        job_facets: Optional[dict[str, Any]] = None,
    ) -> bool:
        """
        Emit a FAIL event for a failed run.

        Args:
            run_id: Unique run identifier
            job_name: Name of the job
            error_message: Error message
            stack_trace: Optional stack trace
            job_namespace: Job namespace
            inputs: Input datasets
            outputs: Output datasets (partial)
            run_facets: Additional run facets
            job_facets: Job facets

        Returns:
            True if event was sent successfully
        """
        if not self.config.emit_on_end:
            return False

        from openlineage_oai.core.facets import create_error_facet

        # Merge error facet with any provided run facets
        facets = run_facets.copy() if run_facets else {}
        facets["errorMessage"] = create_error_facet(
            message=error_message,
            stack_trace=stack_trace,
            producer=self.config.producer,
        )

        return self.emit_run_event(
            event_type="FAIL",
            run_id=run_id,
            job_name=job_name,
            job_namespace=job_namespace,
            inputs=inputs,
            outputs=outputs,
            run_facets=facets,
            job_facets=job_facets,
        )

    # ========================================================================
    # DatasetEvent Methods
    # ========================================================================

    def emit_dataset_event(
        self,
        event_type: str,
        dataset_name: str,
        dataset_namespace: str,
        facets: Optional[dict[str, Any]] = None,
    ) -> bool:
        """
        Emit an OpenLineage DatasetEvent.

        DatasetEvents are used to create or update datasets independently
        of job runs. This makes datasets visible in the lineage UI before
        any job references them.

        Args:
            event_type: "CREATE" or "ALTER"
            dataset_name: Name of the dataset
            dataset_namespace: Namespace of the dataset
            facets: Dataset facets (schema, documentation, etc.)

        Returns:
            True if event was sent successfully
        """
        if not self.is_enabled:
            return False

        event = self._build_dataset_event(
            event_type=event_type,
            name=dataset_name,
            namespace=dataset_namespace,
            facets=facets or {},
        )

        return self._send_event(event)

    def emit_create_dataset(
        self,
        name: str,
        namespace: str,
        schema_fields: Optional[list[dict[str, str]]] = None,
        description: Optional[str] = None,
        facets: Optional[dict[str, Any]] = None,
    ) -> bool:
        """
        Emit a CREATE DatasetEvent.

        Convenience method for creating a new dataset with common facets.

        Args:
            name: Dataset name
            namespace: Dataset namespace
            schema_fields: Column definitions [{"name": "col", "type": "int64"}, ...]
            description: Human-readable description
            facets: Additional facets

        Returns:
            True if event was sent successfully
        """
        from openlineage_oai.core.facets import (
            create_documentation_facet,
            create_schema_facet,
        )

        dataset_facets = facets.copy() if facets else {}

        if schema_fields:
            dataset_facets["schema"] = create_schema_facet(
                schema_fields, producer=self.config.producer
            )

        if description:
            dataset_facets["documentation"] = create_documentation_facet(
                description, producer=self.config.producer
            )

        return self.emit_dataset_event(
            event_type="CREATE",
            dataset_name=name,
            dataset_namespace=namespace,
            facets=dataset_facets,
        )

    # ========================================================================
    # Event Building
    # ========================================================================

    def _build_run_event(
        self,
        event_type: str,
        run_id: str,
        job_name: str,
        job_namespace: str,
        inputs: list[dict[str, Any]],
        outputs: list[dict[str, Any]],
        run_facets: dict[str, Any],
        job_facets: dict[str, Any],
    ) -> dict[str, Any]:
        """Build an OpenLineage RunEvent dictionary."""
        return {
            "eventType": event_type,
            "eventTime": datetime.now(timezone.utc).isoformat(),
            "producer": self.config.producer,
            "schemaURL": RUN_EVENT_SCHEMA,
            "job": {
                "namespace": job_namespace,
                "name": job_name,
                "facets": job_facets,
            },
            "run": {
                "runId": run_id,
                "facets": run_facets,
            },
            "inputs": inputs,
            "outputs": outputs,
        }

    def _build_dataset_event(
        self,
        event_type: str,
        name: str,
        namespace: str,
        facets: dict[str, Any],
    ) -> dict[str, Any]:
        """Build an OpenLineage DatasetEvent dictionary."""
        return {
            "eventType": event_type,
            "eventTime": datetime.now(timezone.utc).isoformat(),
            "producer": self.config.producer,
            "schemaURL": DATASET_EVENT_SCHEMA,
            "dataset": {
                "namespace": namespace,
                "name": name,
                "facets": facets,
            },
        }

    # ========================================================================
    # HTTP Transport
    # ========================================================================

    def _send_event(self, event: dict[str, Any]) -> bool:
        """
        Send an event to the OpenLineage backend.

        This method is fail-safe: it logs warnings but doesn't raise exceptions.

        Args:
            event: OpenLineage event dictionary

        Returns:
            True if sent successfully, False otherwise
        """
        try:
            headers = {"Content-Type": "application/json"}

            if self.config.api_key:
                headers["Authorization"] = f"Bearer {self.config.api_key}"

            response = requests.post(
                self.config.lineage_url,
                json=event,
                headers=headers,
                timeout=self.config.timeout_seconds,
            )

            if response.status_code in (200, 201):
                return True
            else:
                warnings.warn(
                    f"OpenLineage event rejected: HTTP {response.status_code} - {response.text}",
                    stacklevel=2,
                )
                return False

        except requests.exceptions.Timeout:
            warnings.warn(
                f"OpenLineage event timed out ({self.config.timeout_seconds}s)",
                stacklevel=2,
            )
            return False

        except requests.exceptions.ConnectionError:
            warnings.warn(
                f"OpenLineage backend not reachable at {self.config.lineage_url}",
                stacklevel=2,
            )
            return False

        except Exception as e:
            warnings.warn(
                f"Failed to send OpenLineage event: {e}",
                stacklevel=2,
            )
            return False


def generate_run_id() -> str:
    """
    Generate a unique run ID.

    Returns:
        UUID string suitable for OpenLineage run IDs
    """
    return str(uuid.uuid4())
