"""
Upload the compiled RAG ingestion pipeline to OpenShift AI (Data Science Pipelines).

Usage:
    python -m src.rag.upload_rag_pipeline
"""
import os
import sys
from pathlib import Path

import kfp
from kfp.client import Client


def upload_pipeline():
    """Upload the RAG pipeline to OpenShift AI."""
    # Path to compiled pipeline YAML
    pipeline_yaml = Path(__file__).parent.parent.parent / "rag_ingestion_pipeline.yaml"

    if not pipeline_yaml.exists():
        print(f"❌ Pipeline YAML not found: {pipeline_yaml}")
        print("   Compile it first: python -m src.rag.rag_pipeline")
        sys.exit(1)

    # Get DSP endpoint from environment or use default
    dsp_endpoint = os.getenv(
        "DSP_ENDPOINT",
        "https://ds-pipeline-dspa-lineage.apps.your-cluster.com",
    )

    # Get authentication token (for OpenShift AI)
    token = os.getenv("DSP_TOKEN")
    if not token:
        print("⚠️  No DSP_TOKEN found. Attempting anonymous upload...")
        print("   Set DSP_TOKEN env var for authenticated access")

    print(f"📡 Connecting to Data Science Pipelines at: {dsp_endpoint}")

    try:
        # Create KFP client
        if token:
            client = Client(
                host=dsp_endpoint,
                existing_token=token,
            )
        else:
            client = Client(host=dsp_endpoint)

        # Upload pipeline
        pipeline_name = "RAG Document Ingestion"
        print(f"📤 Uploading pipeline: {pipeline_name}")

        pipeline = client.upload_pipeline(
            pipeline_package_path=str(pipeline_yaml),
            pipeline_name=pipeline_name,
            description=(
                "RAG ingestion pipeline: load docs → chunk → embed → pgvector. "
                "Automatic OpenLineage tracking via MLflow and KFP artifacts."
            ),
        )

        print(f"✅ Pipeline uploaded successfully!")
        print(f"   Pipeline ID: {pipeline.pipeline_id}")
        print(f"   Pipeline Name: {pipeline.display_name}")
        print()
        print(f"🔗 View in UI: {dsp_endpoint}/#/pipelines")

    except Exception as e:
        print(f"❌ Failed to upload pipeline: {e}")
        print()
        print("Troubleshooting:")
        print("  1. Verify DSP_ENDPOINT is correct")
        print("  2. Check DSP_TOKEN is valid")
        print("  3. Ensure you have network access to the cluster")
        print("  4. Verify the Data Science Pipelines application is running:")
        print("     kubectl get dspa -n lineage")
        sys.exit(1)


if __name__ == "__main__":
    upload_pipeline()
