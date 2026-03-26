"""
KFP lineage context manager for automatic OpenLineage emission.

This is the primary user-facing API for the KFP adapter.  It must be
imported **inside** a ``@dsl.component`` function body because KFP v2
serialises component source code and external decorators break
serialisation.

Usage inside a KFP component::

    from openlineage_oai.adapters.kfp import kfp_lineage

    with kfp_lineage(
        "my_step",
        inputs=[input_artifact],
        outputs=[output_artifact],
        url="http://marquez",
    ):
        df.to_parquet(output_artifact.path)
"""

import logging
import os
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Union
from urllib.parse import urlparse

from openlineage_oai.adapters.kfp.facets import (
    build_job_type_facet,
    build_parent_run_facet,
)
from openlineage_oai.core.config import OpenLineageConfig
from openlineage_oai.core.emitter import OpenLineageEmitter

logger = logging.getLogger(__name__)

DatasetSpec = Union[Any, Dict[str, str]]


def parse_kfp_artifact(artifact: Any) -> Dict[str, str]:
    """Derive an OpenLineage dataset dict from a KFP artifact object.

    Accepts:
      - A KFP artifact with a ``.uri`` attribute (``dsl.Input[dsl.Dataset]``,
        ``dsl.Output[dsl.Dataset]``, etc.)
      - A plain dict with ``namespace`` and ``name`` keys (pass-through)

    Returns:
        ``{"namespace": ..., "name": ...}`` suitable for an OL event.
    """
    if isinstance(artifact, dict):
        if "namespace" not in artifact or "name" not in artifact:
            raise ValueError(
                "Dict inputs must have 'namespace' and 'name' keys. "
                f"Got keys: {list(artifact.keys())}"
            )
        result = {"namespace": artifact["namespace"], "name": artifact["name"]}
        if "facets" in artifact:
            result["facets"] = artifact["facets"]
        return result

    uri = getattr(artifact, "uri", None)
    if not uri:
        name = getattr(artifact, "name", None) or getattr(artifact, "path", "unknown")
        return {"namespace": "kfp", "name": str(name)}

    parsed = urlparse(uri)
    namespace = f"{parsed.scheme}://{parsed.netloc}" if parsed.netloc else parsed.scheme
    name = parsed.path.lstrip("/")
    return {"namespace": namespace, "name": name}


class KFPRun:
    """Handle returned by the ``kfp_lineage`` context manager.

    Allows adding inputs/outputs discovered mid-execution, before the
    COMPLETE event is emitted on context-manager exit.
    """

    def __init__(self) -> None:
        self._extra_inputs: List[Dict[str, str]] = []
        self._extra_outputs: List[Dict[str, str]] = []

    def add_input(self, artifact: DatasetSpec) -> None:
        """Declare an additional input dataset mid-run."""
        self._extra_inputs.append(parse_kfp_artifact(artifact))

    def add_output(self, artifact: DatasetSpec) -> None:
        """Declare an additional output dataset mid-run."""
        self._extra_outputs.append(parse_kfp_artifact(artifact))


_PRODUCER = "https://github.com/rh-waterford-et/openlineage-oai/kfp-adapter"
_SCHEMA_URL = (
    "https://openlineage.io/spec/facets/1-0-0/SchemaDatasetFacet.json"
    "#/$defs/SchemaDatasetFacet"
)


def kfp_output_with_schema(artifact: Any, df: Any) -> Dict[str, str]:
    """Build an output dataset dict with schema extracted from a pandas DataFrame.

    Combines the KFP artifact's URI-derived identity with the DataFrame's
    column names and dtypes, so that the output dataset shows a schema facet
    in Marquez.

    Args:
        artifact: A KFP artifact (``dsl.Output[dsl.Dataset]``) with a ``.uri``.
        df: A pandas DataFrame whose columns define the schema.

    Returns:
        A dict suitable for ``run.add_output()``::

            {"namespace": "s3://bucket", "name": "path/to/file",
             "facets": {"schema": { ... }}}
    """
    ds = parse_kfp_artifact(artifact)
    ds["facets"] = {
        "schema": {
            "_producer": _PRODUCER,
            "_schemaURL": _SCHEMA_URL,
            "fields": [
                {"name": str(col), "type": str(df[col].dtype)}
                for col in df.columns
            ],
        }
    }
    return ds


class kfp_lineage:
    """Context manager that emits START / COMPLETE / FAIL events for a KFP step.

    Args:
        job_name: Human-readable name for this step (e.g. ``"ds_data_extraction"``).
        inputs:   KFP artifact objects or dicts describing input datasets.
        outputs:  KFP artifact objects or dicts describing output datasets.
        url:      Marquez / OpenLineage endpoint.  Falls back to ``OPENLINEAGE_URL``.
        namespace: Job namespace.  Falls back to ``OPENLINEAGE_NAMESPACE``.

    Example::

        with kfp_lineage("feature_eng", inputs=[ds_in], outputs=[ds_out], url="http://marquez") as run:
            df = pd.read_parquet(ds_in.path)
            ...
            df.to_parquet(ds_out.path)
            run.add_output({"namespace": "extra", "name": "side_table"})
    """

    def __init__(
        self,
        job_name: str,
        inputs: Optional[List[DatasetSpec]] = None,
        outputs: Optional[List[DatasetSpec]] = None,
        url: Optional[str] = None,
        namespace: Optional[str] = None,
    ) -> None:
        self._job_name = job_name
        self._raw_inputs = inputs or []
        self._raw_outputs = outputs or []

        ol_url = url or os.environ.get("OPENLINEAGE_URL", "")
        ol_namespace = namespace or os.environ.get("OPENLINEAGE_NAMESPACE", "default")

        if not ol_url:
            logger.warning(
                "OPENLINEAGE_URL not set and no url provided; KFP lineage events will not be emitted"
            )

        config = OpenLineageConfig(url=ol_url or None, namespace=ol_namespace)
        self._emitter = OpenLineageEmitter(config)
        self._namespace = ol_namespace
        self._run_id = str(uuid.uuid4())
        self._run = KFPRun()

    def __enter__(self) -> KFPRun:
        input_datasets = [parse_kfp_artifact(a) for a in self._raw_inputs]
        output_datasets = [parse_kfp_artifact(a) for a in self._raw_outputs]

        # Capture the pipeline-level parent before we overwrite the env vars.
        self._parent_facet = build_parent_run_facet()

        run_facets: Dict[str, Any] = {}
        if self._parent_facet:
            run_facets["parent"] = self._parent_facet

        job_facets: Dict[str, Any] = {
            "jobType": build_job_type_facet(),
        }

        self._emitter.emit_start(
            run_id=self._run_id,
            job_name=self._job_name,
            job_namespace=self._namespace,
            inputs=input_datasets,
            outputs=output_datasets,
            run_facets=run_facets,
            job_facets=job_facets,
        )

        # Expose this run as the parent for any nested OL emitter (MLflow,
        # Feast, etc.) that reads OPENLINEAGE_PARENT_* env vars.
        os.environ["OPENLINEAGE_PARENT_RUN_ID"] = self._run_id
        os.environ["OPENLINEAGE_PARENT_JOB_NAME"] = self._job_name

        return self._run

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        output_datasets = (
            [parse_kfp_artifact(a) for a in self._raw_outputs]
            + self._run._extra_outputs
        )
        input_datasets = (
            [parse_kfp_artifact(a) for a in self._raw_inputs]
            + self._run._extra_inputs
        )

        run_facets: Dict[str, Any] = {}
        if self._parent_facet:
            run_facets["parent"] = self._parent_facet

        job_facets: Dict[str, Any] = {
            "jobType": build_job_type_facet(),
        }

        if exc_type is not None:
            error_msg = str(exc_val) if exc_val else "Unknown error"
            self._emitter.emit_fail(
                run_id=self._run_id,
                job_name=self._job_name,
                job_namespace=self._namespace,
                error_message=error_msg,
                inputs=input_datasets,
                outputs=output_datasets,
                run_facets=run_facets,
                job_facets=job_facets,
            )
        else:
            self._emitter.emit_complete(
                run_id=self._run_id,
                job_name=self._job_name,
                job_namespace=self._namespace,
                inputs=input_datasets,
                outputs=output_datasets,
                run_facets=run_facets,
                job_facets=job_facets,
            )
