"""
OpenLineage Tracking Store Wrapper for MLflow.

This module implements the MLflow tracking store plugin that intercepts
all tracking operations and emits OpenLineage events while delegating
to the real tracking store.

Design Decisions:
-----------------
1. WRAPPER PATTERN: We wrap the real store rather than replace it.
   This ensures MLflow continues working normally.

2. DUAL-WRITE: Every operation is sent to BOTH the real store AND
   OpenLineage. If OpenLineage fails, MLflow still succeeds.

3. ACCUMULATOR PATTERN: We don't emit on every log_param/log_metric.
   Instead, we accumulate during the run and emit all at COMPLETE.

4. THREAD SAFETY: Run state is stored per run_id to support concurrent runs.

URI Format:
-----------
openlineage+<backend>://<connection-string>?openlineage_url=<url>

Examples:
- openlineage+postgresql://user:pass@localhost:5432/mlflow
- openlineage+http://mlflow-server:5000
- openlineage+file:///tmp/mlruns

The "openlineage+" prefix triggers this plugin via MLflow entry points.
"""

import os
import threading
import warnings
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

from openlineage_oai.adapters.mlflow.facets import (
    create_mlflow_dataset_facet,
    create_mlflow_run_facet,
    filter_system_tags,
)
from openlineage_oai.core.config import OpenLineageConfig
from openlineage_oai.core.emitter import OpenLineageEmitter
from openlineage_oai.utils.naming import build_mlflow_job_name
from openlineage_oai.utils.uri import parse_tracking_uri

if TYPE_CHECKING:
    pass


@dataclass
class RunState:
    """
    Accumulated state for a single MLflow run.

    This tracks params, metrics, inputs, and outputs during the run
    so we can emit them all in the COMPLETE event.
    """

    experiment_id: str
    experiment_name: str = ""
    run_name: str = ""
    job_name: str = ""
    params: dict[str, str] = field(default_factory=dict)
    metrics: dict[str, float] = field(default_factory=dict)
    tags: dict[str, str] = field(default_factory=dict)
    inputs: list[dict[str, Any]] = field(default_factory=list)
    outputs: list[dict[str, Any]] = field(default_factory=list)


class OpenLineageTrackingStore:
    """
    MLflow Tracking Store that emits OpenLineage events.

    This store wraps a real MLflow tracking store (PostgreSQL, REST, file, etc.)
    and intercepts operations to emit lineage while delegating actual storage.

    The store is loaded by MLflow when MLFLOW_TRACKING_URI starts with "openlineage+".

    Example:
        export MLFLOW_TRACKING_URI="openlineage+postgresql://localhost/mlflow"
        export OPENLINEAGE_URL="http://marquez:5000"

        import mlflow
        with mlflow.start_run():
            mlflow.log_param("lr", 0.01)  # Goes to PostgreSQL AND emits to Marquez
    """

    def __init__(self, store_uri: str, artifact_uri: str = None):
        """
        Initialize the OpenLineage tracking store.

        Args:
            store_uri: Full URI like "openlineage+postgresql://..."
            artifact_uri: Artifact storage URI (passed to delegate)
        """
        # Parse the URI to extract backend and OpenLineage config
        parsed = parse_tracking_uri(store_uri)

        self._store_uri = store_uri
        self._backend_uri = parsed.backend_uri
        self._artifact_uri = artifact_uri

        # Initialize the delegate (real) store
        self._delegate = self._create_delegate_store(parsed.backend_uri, artifact_uri)

        # Initialize OpenLineage emitter
        config = OpenLineageConfig(
            url=parsed.openlineage_url,
            namespace=parsed.openlineage_namespace or "mlflow",
        )
        self._emitter = OpenLineageEmitter(config)
        self._namespace = config.namespace

        # Thread-safe run state storage
        self._run_states: dict[str, RunState] = {}
        self._lock = threading.Lock()

    def _create_delegate_store(self, backend_uri: str, artifact_uri: str):
        """
        Create the delegate (real) tracking store.

        Directly instantiates the appropriate store based on URI scheme
        to avoid potential recursion through the registry.
        """
        from urllib.parse import urlparse

        parsed = urlparse(backend_uri)
        scheme = parsed.scheme.lower()

        if scheme in ("http", "https"):
            # REST store for tracking server
            from mlflow.store.tracking.rest_store import RestStore
            from mlflow.tracking._tracking_service.utils import get_default_host_creds

            def get_creds():
                return get_default_host_creds(backend_uri)

            return RestStore(get_creds)

        elif scheme in ("postgresql", "mysql", "sqlite", "mssql"):
            # SQL store
            from mlflow.store.tracking.sqlalchemy_store import SqlAlchemyStore

            return SqlAlchemyStore(backend_uri, artifact_uri)

        elif scheme == "file" or not scheme:
            # File-based store
            from mlflow.store.tracking.file_store import FileStore

            path = parsed.path if parsed.path else backend_uri
            return FileStore(path, artifact_uri)

        else:
            # Unknown scheme - try MLflow's factory as fallback
            from mlflow.tracking._tracking_service.utils import _get_store

            return _get_store(backend_uri, artifact_uri)

    # ========================================================================
    # Parent Run Facet
    # ========================================================================

    @staticmethod
    def _build_parent_run_facet() -> dict[str, Any]:
        """Build a ParentRunFacet from orchestrator env vars if available.

        Reads OPENLINEAGE_PARENT_RUN_ID and OPENLINEAGE_PARENT_JOB_NAME
        (or KFP_RUN_ID / KFP_PIPELINE_NAME as fallbacks) injected by the
        platform into pipeline pods.
        """
        run_id = (
            os.environ.get("OPENLINEAGE_PARENT_RUN_ID")
            or os.environ.get("KFP_RUN_ID", "")
        )
        job_name = (
            os.environ.get("OPENLINEAGE_PARENT_JOB_NAME")
            or os.environ.get("KFP_PIPELINE_NAME", "")
        )
        if not run_id:
            return {}
        return {
            "parent": {
                "_producer": "https://github.com/rh-waterford-et/openlineage-oai/mlflow-adapter",
                "_schemaURL": "https://openlineage.io/spec/facets/1-0-0/ParentRunFacet.json",
                "run": {"runId": run_id},
                "job": {
                    "namespace": os.environ.get("OPENLINEAGE_NAMESPACE", "default"),
                    "name": job_name or "unknown-pipeline",
                },
            }
        }

    # ========================================================================
    # Run Lifecycle Methods (emit OpenLineage events)
    # ========================================================================

    def create_run(
        self,
        experiment_id: str,
        user_id: str,
        start_time: int,
        tags: list,
        run_name: str,
    ):
        """
        Create a new run - emits START event.
        """
        # Delegate to real store first
        run = self._delegate.create_run(
            experiment_id=experiment_id,
            user_id=user_id,
            start_time=start_time,
            tags=tags,
            run_name=run_name,
        )

        run_id = run.info.run_id

        # Get experiment name
        try:
            experiment = self._delegate.get_experiment(experiment_id)
            experiment_name = experiment.name if experiment else ""
        except Exception:
            experiment_name = ""

        job_name = build_mlflow_job_name(
            experiment_id=experiment_id,
            run_name=run_name,
            run_id=run_id,
            experiment_name=experiment_name,
        )

        # Initialize run state
        with self._lock:
            self._run_states[run_id] = RunState(
                experiment_id=experiment_id,
                experiment_name=experiment_name,
                run_name=run_name or "",
                job_name=job_name,
            )

        # Emit START event
        self._emitter.emit_start(
            run_id=run_id,
            job_name=job_name,
            job_namespace=self._namespace,
            run_facets=self._build_parent_run_facet(),
        )

        return run

    def update_run_info(
        self,
        run_id: str,
        run_status,
        end_time: int,
        run_name: str,
    ):
        """
        Update run status - emits COMPLETE or FAIL event.
        """
        # Delegate first
        result = self._delegate.update_run_info(
            run_id=run_id,
            run_status=run_status,
            end_time=end_time,
            run_name=run_name,
        )

        # Get run state
        with self._lock:
            state = self._run_states.get(run_id)

        if state:
            # Import RunStatus here to avoid import issues
            try:
                from mlflow.entities import RunStatus

                is_finished = run_status == RunStatus.FINISHED
                is_failed = run_status == RunStatus.FAILED
            except ImportError:
                # Fallback: compare string representations
                is_finished = "FINISHED" in str(run_status)
                is_failed = "FAILED" in str(run_status)

            if is_finished:
                # Emit COMPLETE with accumulated data
                run_facets = {
                    "mlflow_run": create_mlflow_run_facet(
                        run_id=run_id,
                        experiment_id=state.experiment_id,
                        experiment_name=state.experiment_name,
                        run_name=state.run_name,
                        params=state.params,
                        metrics=state.metrics,
                        tags=filter_system_tags(state.tags),
                    ),
                }
                run_facets.update(self._build_parent_run_facet())

                self._emitter.emit_complete(
                    run_id=run_id,
                    job_name=state.job_name,
                    job_namespace=self._namespace,
                    inputs=state.inputs,
                    outputs=state.outputs,
                    run_facets=run_facets,
                )

            elif is_failed:
                # Emit FAIL event
                error_msg = state.tags.get("mlflow.note.content", "Run failed")
                self._emitter.emit_fail(
                    run_id=run_id,
                    job_name=state.job_name,
                    job_namespace=self._namespace,
                    error_message=error_msg,
                    inputs=state.inputs,
                    outputs=state.outputs,
                    run_facets=self._build_parent_run_facet(),
                )

            # Cleanup run state
            with self._lock:
                self._run_states.pop(run_id, None)

        return result

    # ========================================================================
    # Param/Metric/Tag Methods (accumulate, don't emit)
    # ========================================================================

    def log_param(self, run_id: str, param):
        """Log a parameter - accumulates for COMPLETE event."""
        # Delegate
        self._delegate.log_param(run_id, param)

        # Accumulate
        with self._lock:
            if run_id in self._run_states:
                self._run_states[run_id].params[param.key] = param.value

    def log_params(self, run_id: str, params: list):
        """Log multiple parameters."""
        # Delegate
        if hasattr(self._delegate, "log_params"):
            self._delegate.log_params(run_id, params)
        else:
            for param in params:
                self._delegate.log_param(run_id, param)

        # Accumulate
        with self._lock:
            if run_id in self._run_states:
                for param in params:
                    self._run_states[run_id].params[param.key] = param.value

    def log_metric(self, run_id: str, metric):
        """Log a metric - accumulates for COMPLETE event."""
        # Delegate
        self._delegate.log_metric(run_id, metric)

        # Accumulate (keep latest value)
        with self._lock:
            if run_id in self._run_states:
                self._run_states[run_id].metrics[metric.key] = metric.value

    def log_metrics(self, run_id: str, metrics: list):
        """Log multiple metrics."""
        # Delegate
        if hasattr(self._delegate, "log_metrics"):
            self._delegate.log_metrics(run_id, metrics)
        else:
            for metric in metrics:
                self._delegate.log_metric(run_id, metric)

        # Accumulate
        with self._lock:
            if run_id in self._run_states:
                for metric in metrics:
                    self._run_states[run_id].metrics[metric.key] = metric.value

    def set_tag(self, run_id: str, tag):
        """
        Set a tag - accumulates for COMPLETE event.

        Special handling for mlflow.log-model.history tag which contains
        model information that we track as output datasets.
        """
        # Delegate first
        self._delegate.set_tag(run_id, tag)

        # Accumulate tag
        with self._lock:
            if run_id in self._run_states:
                self._run_states[run_id].tags[tag.key] = tag.value

        # Check for model logging tag (legacy MLflow) - kept for compatibility
        if tag.key == "mlflow.log-model.history":
            self._handle_model_history_tag(run_id, tag.value)

    def set_tags(self, run_id: str, tags: list):
        """Set multiple tags."""
        # Delegate
        if hasattr(self._delegate, "set_tags"):
            self._delegate.set_tags(run_id, tags)
        else:
            for tag in tags:
                self._delegate.set_tag(run_id, tag)

        # Accumulate and check for model history
        with self._lock:
            if run_id in self._run_states:
                for tag in tags:
                    self._run_states[run_id].tags[tag.key] = tag.value

                    # Check for model history tag (legacy MLflow)
                    if tag.key == "mlflow.log-model.history":
                        self._handle_model_history_tag(run_id, tag.value)

    # ========================================================================
    # Model Output Tracking
    # ========================================================================

    def _handle_model_history_tag(self, run_id: str, tag_value: str):
        """
        Parse mlflow.log-model.history tag and register models as output datasets.

        MLflow stores model info in this tag as JSON array:
        [{"artifact_path": "model", "flavors": {"sklearn": {...}}, "run_id": "...", ...}]

        We emit a DatasetEvent for each model and track it as an output.
        """
        import json

        try:
            models = json.loads(tag_value)
            if not isinstance(models, list):
                return

            for model_info in models:
                artifact_path = model_info.get("artifact_path", "model")
                flavors = model_info.get("flavors", {})
                signature = model_info.get("signature", {})

                # Build model dataset name
                model_name = f"model/{artifact_path}"

                # Build facets
                model_facets = {
                    "mlflow_model": {
                        "_producer": "https://github.com/openlineage-oai/mlflow-adapter",
                        "_schemaURL": "https://openlineage.io/spec/facets/1-0-0/MLflowModelFacet.json",
                        "artifact_path": artifact_path,
                        "flavors": list(flavors.keys()),
                        "run_id": model_info.get("run_id", run_id),
                        "utc_time_created": model_info.get("utc_time_created", ""),
                    }
                }

                # Add schema from model signature if available
                if signature:
                    inputs_schema = signature.get("inputs")
                    outputs_schema = signature.get("outputs")
                    if inputs_schema or outputs_schema:
                        model_facets["mlflow_model"]["signature"] = {
                            "inputs": inputs_schema,
                            "outputs": outputs_schema,
                        }

                # Emit standalone DatasetEvent for the model
                self._emitter.emit_dataset_event(
                    event_type="CREATE",
                    dataset_name=model_name,
                    dataset_namespace=self._namespace,
                    facets=model_facets,
                )

                # Track as output for the run
                output_dataset = {
                    "namespace": self._namespace,
                    "name": model_name,
                    "facets": model_facets,
                }

                with self._lock:
                    if run_id in self._run_states:
                        self._run_states[run_id].outputs.append(output_dataset)

        except json.JSONDecodeError:
            warnings.warn(
                f"Failed to parse mlflow.log-model.history tag: {tag_value[:100]}", stacklevel=2
            )
        except Exception as e:
            warnings.warn(f"Failed to process model history tag: {e}", stacklevel=2)

    # ========================================================================
    # Dataset/Input Methods (track inputs)
    # ========================================================================

    def log_input(self, run_id: str, dataset=None, context: str = None, tags=None, model=None):
        """
        Log a single input dataset (singular form called by mlflow.log_input).

        Wraps into a list and delegates to log_inputs for consistent handling.
        """
        datasets = [dataset] if dataset else None
        models = [model] if model else None
        self.log_inputs(run_id, datasets=datasets, models=models)

    def log_inputs(self, run_id: str, datasets: list = None, models: list = None):
        """
        Log input datasets - tracks as OpenLineage inputs.

        This method:
        1. Extracts the source URI from each dataset
        2. Derives the OpenLineage (namespace, name) from the source URI
        3. Emits a standalone DatasetEvent to register the dataset in Marquez
        4. Accumulates the dataset for the run's inputs list

        Args:
            run_id: The MLflow run ID
            datasets: List of DatasetInput objects
            models: List of ModelInput objects (MLflow 2.x+)
        """
        # Delegate to real store first
        if hasattr(self._delegate, "log_inputs"):
            self._delegate.log_inputs(run_id, datasets=datasets, models=models)

        # Process each dataset
        if datasets:
            for dataset in datasets:
                try:
                    from openlineage_oai.adapters.mlflow.utils import (
                        extract_dataset_info,
                        parse_ol_identity,
                    )
                    from openlineage_oai.core.facets import create_schema_facet

                    info = extract_dataset_info(dataset.dataset)
                    source_uri = info.get("source", "")

                    if source_uri:
                        ol_namespace, ol_name = parse_ol_identity(source_uri)
                    else:
                        ol_namespace = self._namespace
                        ol_name = info.get("name", "unknown")

                    dataset_facets = {
                        "mlflow_dataset": create_mlflow_dataset_facet(
                            name=info.get("name", "unknown"),
                            source=source_uri,
                            source_type=info.get("source_type", ""),
                            digest=info.get("digest", ""),
                            context=getattr(dataset, "context", "training"),
                        ),
                    }

                    if info.get("schema"):
                        dataset_facets["schema"] = create_schema_facet(info["schema"])

                    self._emitter.emit_dataset_event(
                        event_type="CREATE",
                        dataset_name=ol_name,
                        dataset_namespace=ol_namespace,
                        facets=dataset_facets,
                    )

                    input_dataset = {
                        "namespace": ol_namespace,
                        "name": ol_name,
                        "facets": dataset_facets,
                    }

                    with self._lock:
                        if run_id in self._run_states:
                            self._run_states[run_id].inputs.append(input_dataset)

                except Exception as e:
                    warnings.warn(f"Failed to process input dataset: {e}", stacklevel=2)

        # Track models as outputs (MLflow 2.x+)
        with self._lock:
            if run_id in self._run_states and models:
                for model in models:
                    try:
                        # Extract model info
                        output_model = {
                            "namespace": self._namespace,
                            "name": f"model/{getattr(model, 'name', 'unknown')}",
                            "facets": {},
                        }
                        self._run_states[run_id].outputs.append(output_model)
                    except Exception as e:
                        warnings.warn(f"Failed to extract model info: {e}", stacklevel=2)

    # ========================================================================
    # Model Output Methods (newer MLflow API)
    # ========================================================================

    def log_outputs(self, run_id: str, models: list):
        """
        Log outputs (models) for a run - tracks as OpenLineage outputs.

        This is the newer MLflow API for logging models (MLflow 2.x+).
        """
        # Delegate first
        result = None
        if hasattr(self._delegate, "log_outputs"):
            result = self._delegate.log_outputs(run_id, models)

        # Track models as outputs
        if models:
            for output in models:
                try:
                    # Extract model info
                    model_name = f"model/{getattr(output, 'artifact_path', 'model')}"

                    model_facets = {
                        "mlflow_model": {
                            "_producer": "https://github.com/openlineage-oai/mlflow-adapter",
                            "_schemaURL": "https://openlineage.io/spec/facets/1-0-0/MLflowModelFacet.json",
                            "artifact_path": getattr(output, "artifact_path", ""),
                            "flavors": list(getattr(output, "flavors", {}).keys())
                            if hasattr(output, "flavors")
                            else [],
                            "run_id": run_id,
                        }
                    }

                    # Emit standalone DatasetEvent for the model
                    self._emitter.emit_dataset_event(
                        event_type="CREATE",
                        dataset_name=model_name,
                        dataset_namespace=self._namespace,
                        facets=model_facets,
                    )

                    # Track as output for the run
                    output_dataset = {
                        "namespace": self._namespace,
                        "name": model_name,
                        "facets": model_facets,
                    }

                    with self._lock:
                        if run_id in self._run_states:
                            self._run_states[run_id].outputs.append(output_dataset)

                except Exception as e:
                    warnings.warn(f"Failed to process model output: {e}", stacklevel=2)

        return result

    # ========================================================================
    # Delegation Methods (pure pass-through)
    # ========================================================================

    # These methods are delegated without any OpenLineage tracking.
    # They're required by the AbstractStore interface but don't affect lineage.

    def get_experiment(self, experiment_id: str):
        return self._delegate.get_experiment(experiment_id)

    def get_experiment_by_name(self, name: str):
        return self._delegate.get_experiment_by_name(name)

    def create_experiment(self, name: str, artifact_location: str = None, tags: list = None):
        return self._delegate.create_experiment(name, artifact_location, tags)

    def delete_experiment(self, experiment_id: str):
        return self._delegate.delete_experiment(experiment_id)

    def restore_experiment(self, experiment_id: str):
        return self._delegate.restore_experiment(experiment_id)

    def rename_experiment(self, experiment_id: str, new_name: str):
        return self._delegate.rename_experiment(experiment_id, new_name)

    def get_run(self, run_id: str):
        return self._delegate.get_run(run_id)

    def delete_run(self, run_id: str):
        return self._delegate.delete_run(run_id)

    def restore_run(self, run_id: str):
        return self._delegate.restore_run(run_id)

    def search_runs(self, *args, **kwargs):
        return self._delegate.search_runs(*args, **kwargs)

    def search_experiments(self, *args, **kwargs):
        return self._delegate.search_experiments(*args, **kwargs)

    def list_run_infos(self, *args, **kwargs):
        return self._delegate.list_run_infos(*args, **kwargs)

    def log_batch(self, run_id: str, metrics: list = None, params: list = None, tags: list = None):
        """Log batch of metrics, params, and tags."""
        # Delegate
        self._delegate.log_batch(run_id, metrics=metrics, params=params, tags=tags)

        # Accumulate
        with self._lock:
            if run_id in self._run_states:
                if params:
                    for param in params:
                        self._run_states[run_id].params[param.key] = param.value
                if metrics:
                    for metric in metrics:
                        self._run_states[run_id].metrics[metric.key] = metric.value
                if tags:
                    for tag in tags:
                        self._run_states[run_id].tags[tag.key] = tag.value

                        # Check for model history tag (legacy MLflow)
                        if tag.key == "mlflow.log-model.history":
                            self._handle_model_history_tag(run_id, tag.value)

    # Add more delegated methods as needed by MLflow version...

    def __getattr__(self, name: str):
        """
        Delegate any unimplemented methods to the real store.

        This ensures forward compatibility - new MLflow methods
        will be passed through even if we haven't implemented them.
        """
        return getattr(self._delegate, name)
