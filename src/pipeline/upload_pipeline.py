"""
Upload the compiled pipeline to the OpenShift AI Data Science Pipelines server.

Usage:
    python -m src.pipeline.upload_pipeline

Requires:
    - oc login (already authenticated)
    - DSPA deployed in lineage namespace
"""

import subprocess
import sys
import time

from kfp import compiler
from kfp.client import Client

NAMESPACE = "lineage"
DSPA_NAME = "dspa"
PIPELINE_YAML = "customer_churn_pipeline.yaml"
PIPELINE_NAME = "customer-churn-ml-pipeline"


def get_dsp_route() -> str:
    """Get the Data Science Pipeline API server route from OpenShift."""
    result = subprocess.run(
        [
            "oc", "get", "route",
            f"ds-pipeline-{DSPA_NAME}",
            "-n", NAMESPACE,
            "-o", "jsonpath={.spec.host}",
        ],
        capture_output=True, text=True,
    )
    if result.returncode != 0 or not result.stdout.strip():
        print("Could not find DSP route. Trying internal service...")
        return f"https://ds-pipeline-{DSPA_NAME}.{NAMESPACE}.svc:8888"
    return f"https://{result.stdout.strip()}"


def get_sa_token() -> str:
    """Get a bearer token for authenticating with the DSP API."""
    result = subprocess.run(
        ["oc", "whoami", "-t"],
        capture_output=True, text=True,
    )
    if result.returncode != 0:
        print("ERROR: Could not get token. Run 'oc login' first.")
        sys.exit(1)
    return result.stdout.strip()


def main():
    # 1. Compile the pipeline
    print(f"Compiling pipeline → {PIPELINE_YAML}")
    from src.pipeline.kfp_pipeline import customer_churn_pipeline
    compiler.Compiler().compile(
        pipeline_func=customer_churn_pipeline,
        package_path=PIPELINE_YAML,
    )
    print("Compilation successful")

    # 2. Connect to the DSP API server
    dsp_url = get_dsp_route()
    token = get_sa_token()
    print(f"Connecting to DSP at {dsp_url}")

    client = Client(
        host=dsp_url,
        existing_token=token,
        ssl_ca_cert=False,
    )
    # KFP client may need to skip TLS verification for self-signed certs
    import urllib3
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

    # 3. Upload or update the pipeline
    print(f"Uploading pipeline: {PIPELINE_NAME}")
    try:
        pipeline = client.upload_pipeline(
            pipeline_package_path=PIPELINE_YAML,
            pipeline_name=PIPELINE_NAME,
            description="End-to-end customer churn: Feast, Validate, Train, MLflow",
        )
        print(f"Pipeline created: id={pipeline.pipeline_id}")
    except Exception as e:
        if "already exist" in str(e).lower():
            print("Pipeline already exists, uploading as new version...")
            pipelines = client.list_pipelines(page_size=50)
            pid = None
            if pipelines.pipelines:
                for p in pipelines.pipelines:
                    if p.display_name == PIPELINE_NAME:
                        pid = p.pipeline_id
                        break
            if pid:
                version = client.upload_pipeline_version(
                    pipeline_package_path=PIPELINE_YAML,
                    pipeline_version_name=f"v{int(time.time())}",
                    pipeline_id=pid,
                )
                print(f"New version created: id={version.pipeline_version_id}")
            else:
                raise RuntimeError(f"Could not find pipeline '{PIPELINE_NAME}'")
        else:
            raise

    # 4. Create a run
    print("Creating pipeline run...")
    run = client.create_run_from_pipeline_package(
        pipeline_file=PIPELINE_YAML,
        arguments={},
        run_name=f"churn-run-{int(time.time())}",
        experiment_name="customer_churn_lineage",
    )
    print(f"Run started: {run.run_id}")
    print(f"\nView in OpenShift AI dashboard → Data Science Pipelines → Runs")


if __name__ == "__main__":
    main()
