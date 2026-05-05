"""
Upload the compiled RAG ingestion pipeline to OpenShift AI (Data Science Pipelines).

Usage:
    python -m src.rag.upload_rag_pipeline

With ``oc`` in PATH, uses the same route + token discovery as ``upload_pipeline``.
Otherwise set ``DSP_ENDPOINT`` (full URL) and optionally ``DSP_TOKEN``.
"""
import os
import shutil
import sys
from pathlib import Path

import urllib3
from kfp.client import Client

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


def upload_pipeline() -> None:
    """Upload the RAG pipeline to OpenShift AI."""
    pipeline_yaml = Path(__file__).parent.parent.parent / "rag_ingestion_pipeline.yaml"

    if not pipeline_yaml.exists():
        print(f"Pipeline YAML not found: {pipeline_yaml}")
        print("Compile it first: python -m src.rag.rag_pipeline")
        sys.exit(1)

    if shutil.which("oc"):
        from src.pipeline.dsp_client import connect_dsp_client

        client = connect_dsp_client()
        print(f"Connecting via oc to Data Science Pipelines")
    else:
        dsp_endpoint = os.getenv(
            "DSP_ENDPOINT",
            "https://ds-pipeline-dspa-lineage.apps.your-cluster.com",
        )
        token = os.getenv("DSP_TOKEN")
        print(f"Connecting to Data Science Pipelines at: {dsp_endpoint}")
        if token:
            client = Client(host=dsp_endpoint, existing_token=token, ssl_ca_cert=False)
        else:
            print("No DSP_TOKEN; anonymous client (may fail on secured clusters)")
            client = Client(host=dsp_endpoint, ssl_ca_cert=False)

    pipeline_name = "RAG Document Ingestion"
    print(f"Uploading pipeline: {pipeline_name}")

    try:
        pipeline = client.upload_pipeline(
            pipeline_package_path=str(pipeline_yaml),
            pipeline_name=pipeline_name,
            description=(
                "RAG ingestion pipeline: load docs → chunk → embed → pgvector. "
                "Automatic OpenLineage tracking via MLflow and KFP artifacts."
            ),
        )
        print(f"Pipeline uploaded. ID: {pipeline.pipeline_id}  Name: {pipeline.display_name}")
    except Exception as e:
        print(f"Failed to upload pipeline: {e}")
        sys.exit(1)


if __name__ == "__main__":
    upload_pipeline()
