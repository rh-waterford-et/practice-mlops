"""
OpenLineage integration for Feast feature-store operations.

Emits START / COMPLETE / FAIL lineage events for:
  - feast.apply              – writes feature definitions to the SQL registry
  - feast.materialize        – offline PostgreSQL store → online Redis store
  - feast.get_historical_features – offline store → training dataset

Configuration (environment variables):
  OPENLINEAGE_URL        Marquez / any OpenLineage-compatible backend URL
                         Leave unset or empty to disable – module is fully no-op.
  OPENLINEAGE_NAMESPACE  Logical namespace for Feast jobs (default: "feast")

Datasets are annotated with DataSourceDatasetFacets so Marquez can render
the connection URL and source type alongside the lineage graph.
"""

import contextlib
import logging
import os
from datetime import datetime, timezone
from typing import Dict, List, Optional
from uuid import uuid4

logger = logging.getLogger(__name__)

PRODUCER = "feast-kfp-mlflow/feast"


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _job_namespace() -> str:
    return os.getenv("OPENLINEAGE_NAMESPACE", "feast")


def _pg_namespace() -> str:
    """Build the PostgreSQL namespace URI from runtime settings."""
    try:
        from configs.settings import PG_DATABASE, PG_HOST, PG_PORT  # noqa: PLC0415

        return f"postgresql://{PG_HOST}:{PG_PORT}/{PG_DATABASE}"
    except Exception:
        host = os.getenv("PG_HOST", "localhost")
        port = os.getenv("PG_PORT", "5432")
        db = os.getenv("PG_DATABASE", "warehouse")
        return f"postgresql://{host}:{port}/{db}"


def _redis_namespace() -> str:
    """Build the Redis namespace URI from runtime settings."""
    try:
        from configs.settings import REDIS_HOST, REDIS_PORT  # noqa: PLC0415

        return f"redis://{REDIS_HOST}:{REDIS_PORT}"
    except Exception:
        host = os.getenv("REDIS_HOST", "localhost")
        port = os.getenv("REDIS_PORT", "6379")
        return f"redis://{host}:{port}"


def _get_client():
    from openlineage.client import OpenLineageClient  # noqa: PLC0415

    return OpenLineageClient.from_environment()


def _datasource_facet(name: str, uri: str) -> Dict:
    """Return a DataSourceDatasetFacet dict so Marquez can display the source type/URL."""
    try:
        from openlineage.client.facet import DataSourceDatasetFacet  # noqa: PLC0415

        return {"dataSource": DataSourceDatasetFacet(name=name, uri=uri)}
    except Exception:
        return {}


@contextlib.contextmanager
def lineage_run(
    job_name: str,
    inputs: Optional[List] = None,
    outputs: Optional[List] = None,
):
    """Context manager that wraps a Feast operation with OpenLineage events.

    Emits START on entry, COMPLETE on clean exit, and FAIL on any exception.
    Completely transparent (no-op) when OPENLINEAGE_URL is not set or when
    the openlineage-python package is not installed.

    Example::

        with lineage_run(
            "feast.materialize",
            inputs=[pg_input("customer_features")],
            outputs=[redis_output("customer_features_view")],
        ):
            store.materialize(start_date=start, end_date=end)
    """
    if not os.getenv("OPENLINEAGE_URL"):
        yield
        return

    try:
        from openlineage.client.run import (  # noqa: PLC0415
            Job,
            Run,
            RunEvent,
            RunState,
        )
    except ImportError:
        logger.warning(
            "openlineage-python is not installed – Feast lineage emission skipped"
        )
        yield
        return

    client = _get_client()
    run_id = str(uuid4())
    job = Job(namespace=_job_namespace(), name=job_name)
    run = Run(runId=run_id)

    def _emit(state) -> None:
        try:
            event = RunEvent(
                eventType=state,
                eventTime=_now(),
                run=run,
                job=job,
                inputs=inputs or [],
                outputs=outputs or [],
                producer=PRODUCER,
            )
            client.emit(event)
        except Exception as exc:  # pylint: disable=broad-except
            logger.warning("OpenLineage emit failed (%s): %s", state, exc)

    _emit(RunState.START)
    try:
        yield
        _emit(RunState.COMPLETE)
    except Exception:
        _emit(RunState.FAIL)
        raise


# ── Dataset factory helpers ──────────────────────────────────────────────
# Each factory attaches a DataSourceDatasetFacet so Marquez renders the
# connection type and URL in the dataset detail panel.

def pg_input(table_name: str):
    """InputDataset for a PostgreSQL table in the offline store."""
    from openlineage.client.run import InputDataset  # noqa: PLC0415

    ns = _pg_namespace()
    return InputDataset(
        namespace=ns,
        name=table_name,
        facets=_datasource_facet("postgresql", ns),
    )


def pg_output(table_name: str):
    """OutputDataset for a PostgreSQL table (e.g. the Feast SQL registry)."""
    from openlineage.client.run import OutputDataset  # noqa: PLC0415

    ns = _pg_namespace()
    return OutputDataset(
        namespace=ns,
        name=table_name,
        facets=_datasource_facet("postgresql", ns),
    )


def redis_output(feature_view_name: str):
    """OutputDataset for a Redis-backed feature view in the online store."""
    from openlineage.client.run import OutputDataset  # noqa: PLC0415

    ns = _redis_namespace()
    return OutputDataset(
        namespace=ns,
        name=feature_view_name,
        facets=_datasource_facet("redis", ns),
    )


def logical_output(name: str, namespace: Optional[str] = None):
    """OutputDataset for a logical (in-memory) dataset such as a training DataFrame."""
    from openlineage.client.run import OutputDataset  # noqa: PLC0415

    return OutputDataset(namespace=namespace or _job_namespace(), name=name)
