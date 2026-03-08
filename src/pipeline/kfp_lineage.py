"""
OpenLineage integration library for Kubeflow Pipelines v2.

Provides a context-manager and dataset-factory helpers for emitting OpenLineage
events from within KFP component function bodies.  Mirrors the design of
``src.feature_store.lineage`` but targets KFP pipeline steps rather than Feast
operations.

Fully no-op when ``OPENLINEAGE_URL`` is not set or when *openlineage-python* is
not installed, so it is safe to use in the base image even when no OpenLineage
backend is configured.

Configuration (environment variables)
--------------------------------------
OPENLINEAGE_URL        Marquez / any OL-compatible backend URL
                       (e.g. ``http://marquez.fkm-test.svc.cluster.local:5000``).
                       Leave empty to disable – the module becomes a no-op.
OPENLINEAGE_NAMESPACE  Logical namespace for KFP jobs (default: ``"kfp"``).

Typical usage inside a ``@dsl.component`` function body
--------------------------------------------------------
Because KFP component functions run inside ``FKM_IMAGE``, which has
``PYTHONPATH=/app``, they can import this library at runtime::

    from src.pipeline.kfp_lineage import kfp_lineage_run, parquet_input, parquet_output

    with kfp_lineage_run(
        "kfp.step2_data_validation",
        inputs=[parquet_input("extracted_features")],
        outputs=[parquet_output("validated_features")],
    ):
        df = pd.read_parquet(dataset.path)
        ...

For steps where output metadata (e.g. an MLflow ``run_id``) is only known after
execution, use :class:`KFPLineageRun` directly::

    with KFPLineageRun(
        "kfp.step4_model_training",
        inputs=[parquet_input("engineered_features")],
    ) as ol_run:
        result = train_and_log(df=df, ...)
        ol_run.add_output(mlflow_run_output(result["run_id"], experiment_name))
        return json.dumps(result)
"""

from __future__ import annotations

import contextlib
import logging
import os
from datetime import datetime, timezone
from typing import List, Optional
from uuid import uuid4

logger = logging.getLogger(__name__)

PRODUCER = "feast-kfp-mlflow/kfp"


# ── Private helpers ───────────────────────────────────────────────────────────

def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _kfp_namespace() -> str:
    return os.getenv("OPENLINEAGE_NAMESPACE", "kfp")


def _s3_namespace() -> str:
    """S3/MinIO endpoint used as the artifact-store namespace."""
    return os.getenv(
        "MLFLOW_S3_ENDPOINT_URL",
        os.getenv("MINIO_ENDPOINT", "http://localhost:9000"),
    )


def _mlflow_namespace() -> str:
    return os.getenv("MLFLOW_TRACKING_URI", "http://localhost:5000")


def _pg_namespace() -> str:
    try:
        from configs.settings import PG_DATABASE, PG_HOST, PG_PORT  # noqa: PLC0415
        return f"postgresql://{PG_HOST}:{PG_PORT}/{PG_DATABASE}"
    except Exception:
        host = os.getenv("PG_HOST", "localhost")
        port = os.getenv("PG_PORT", "5432")
        db   = os.getenv("PG_DATABASE", "warehouse")
        return f"postgresql://{host}:{port}/{db}"


def _get_client():
    from openlineage.client import OpenLineageClient  # noqa: PLC0415
    return OpenLineageClient.from_environment()


def _datasource_facet(name: str, uri: str):
    """Return a DataSourceDatasetFacet dict so Marquez can display source type/URL."""
    try:
        from openlineage.client.facet import DataSourceDatasetFacet  # noqa: PLC0415
        return {"dataSource": DataSourceDatasetFacet(name=name, uri=uri)}
    except Exception:
        return {}


# ── KFPLineageRun class ───────────────────────────────────────────────────────

class KFPLineageRun:
    """Context manager for KFP step lineage tracking with dynamic output support.

    Emits a ``START`` event on ``__enter__`` and a ``COMPLETE`` / ``FAIL`` event
    on ``__exit__``.  Completely transparent (no-op) when ``OPENLINEAGE_URL`` is
    not set.

    Unlike the simple :func:`kfp_lineage_run` helper, this class lets you
    attach output datasets that are only known *after* execution (e.g. an MLflow
    ``run_id``) by calling :meth:`add_output` inside the ``with`` block before
    returning::

        with KFPLineageRun(
            "kfp.step4_model_training",
            inputs=[parquet_input("engineered_features")],
        ) as ol_run:
            result = train_and_log(df=df, ...)
            ol_run.add_output(mlflow_run_output(result["run_id"], experiment_name))
            return json.dumps(result)
    """

    def __init__(
        self,
        job_name: str,
        inputs: Optional[List] = None,
        outputs: Optional[List] = None,
    ) -> None:
        self.job_name = job_name
        self.inputs: List = list(inputs or [])
        self.outputs: List = list(outputs or [])
        self._client = None
        self._run_obj = None
        self._job_obj = None

    # ------------------------------------------------------------------
    def add_input(self, dataset) -> None:
        """Append an input dataset (may be called inside the ``with`` block)."""
        self.inputs.append(dataset)

    def add_output(self, dataset) -> None:
        """Append an output dataset (may be called inside the ``with`` block)."""
        self.outputs.append(dataset)

    # ------------------------------------------------------------------
    def _emit(self, state) -> None:
        if self._client is None:
            return
        try:
            from openlineage.client.run import RunEvent  # noqa: PLC0415
            self._client.emit(RunEvent(
                eventType=state,
                eventTime=_now(),
                run=self._run_obj,
                job=self._job_obj,
                inputs=self.inputs,
                outputs=self.outputs,
                producer=PRODUCER,
            ))
        except Exception as exc:
            logger.warning("OpenLineage emit failed (%s): %s", state, exc)

    def __enter__(self) -> "KFPLineageRun":
        if not os.getenv("OPENLINEAGE_URL"):
            return self
        try:
            from openlineage.client.run import Job, Run, RunState  # noqa: PLC0415
            self._client = _get_client()
            self._run_obj = Run(runId=str(uuid4()))
            self._job_obj = Job(namespace=_kfp_namespace(), name=self.job_name)
            self._emit(RunState.START)
        except Exception as exc:
            logger.warning("OpenLineage setup failed for '%s': %s", self.job_name, exc)
            self._client = None
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> bool:
        if self._client is None:
            return False
        try:
            from openlineage.client.run import RunState  # noqa: PLC0415
            self._emit(RunState.COMPLETE if exc_type is None else RunState.FAIL)
        except Exception as exc:
            logger.warning("OpenLineage exit failed for '%s': %s", self.job_name, exc)
        return False


# ── Simple context manager ────────────────────────────────────────────────────

@contextlib.contextmanager
def kfp_lineage_run(
    job_name: str,
    inputs: Optional[List] = None,
    outputs: Optional[List] = None,
):
    """Context manager that wraps a KFP component step with OpenLineage events.

    Emits ``START`` on entry, ``COMPLETE`` on clean exit, ``FAIL`` on exception.
    Completely transparent (no-op) when ``OPENLINEAGE_URL`` is not set.

    All input and output datasets must be known *before* entering the block.
    For steps that produce dynamic outputs (e.g. an MLflow ``run_id`` returned
    by training), use :class:`KFPLineageRun` instead.

    Example::

        with kfp_lineage_run(
            "kfp.step2_data_validation",
            inputs=[parquet_input("extracted_features")],
            outputs=[parquet_output("validated_features")],
        ):
            df = pd.read_parquet(dataset.path)
            df = data_validation(df)
            df.to_parquet(output_path.path)
    """
    with KFPLineageRun(job_name, inputs=inputs, outputs=outputs):
        yield


# ── Dataset factory helpers ───────────────────────────────────────────────────
# Each factory returns an InputDataset or OutputDataset and attaches a
# DataSourceDatasetFacet so Marquez can display the connection type and URL.

def parquet_input(name: str):
    """InputDataset for an intermediate KFP parquet artifact in the artifact store."""
    from openlineage.client.run import InputDataset  # noqa: PLC0415
    ns = _s3_namespace()
    return InputDataset(namespace=ns, name=name, facets=_datasource_facet("s3", ns))


def parquet_output(name: str):
    """OutputDataset for an intermediate KFP parquet artifact in the artifact store."""
    from openlineage.client.run import OutputDataset  # noqa: PLC0415
    ns = _s3_namespace()
    return OutputDataset(namespace=ns, name=name, facets=_datasource_facet("s3", ns))


def pg_input(table_name: str):
    """InputDataset for a PostgreSQL table (e.g. offline Feast store)."""
    from openlineage.client.run import InputDataset  # noqa: PLC0415
    ns = _pg_namespace()
    return InputDataset(namespace=ns, name=table_name, facets=_datasource_facet("postgresql", ns))


def pg_output(table_name: str):
    """OutputDataset for a PostgreSQL table."""
    from openlineage.client.run import OutputDataset  # noqa: PLC0415
    ns = _pg_namespace()
    return OutputDataset(namespace=ns, name=table_name, facets=_datasource_facet("postgresql", ns))


def mlflow_run_input(run_id: str, experiment_name: str):
    """InputDataset for an MLflow run (e.g. consuming a training run's artifacts)."""
    from openlineage.client.run import InputDataset  # noqa: PLC0415
    ns = _mlflow_namespace()
    name = f"experiments/{experiment_name}/runs/{run_id}"
    return InputDataset(namespace=ns, name=name, facets=_datasource_facet("mlflow", ns))


def mlflow_run_output(run_id: str, experiment_name: str):
    """OutputDataset for an MLflow run (e.g. after logging metrics and model artifact)."""
    from openlineage.client.run import OutputDataset  # noqa: PLC0415
    ns = _mlflow_namespace()
    name = f"experiments/{experiment_name}/runs/{run_id}"
    return OutputDataset(namespace=ns, name=name, facets=_datasource_facet("mlflow", ns))


def mlflow_model_input(model_name: str, alias: str = "champion"):
    """InputDataset for a registered MLflow model referenced by alias."""
    from openlineage.client.run import InputDataset  # noqa: PLC0415
    ns = _mlflow_namespace()
    return InputDataset(
        namespace=ns,
        name=f"models:/{model_name}@{alias}",
        facets=_datasource_facet("mlflow", ns),
    )


def mlflow_model_output(model_name: str, version: Optional[str] = None):
    """OutputDataset for a newly registered MLflow model version."""
    from openlineage.client.run import OutputDataset  # noqa: PLC0415
    ns = _mlflow_namespace()
    name = f"models:/{model_name}" + (f"/{version}" if version else "")
    return OutputDataset(namespace=ns, name=name, facets=_datasource_facet("mlflow", ns))
