"""
Upload the compiled pipeline to the OpenShift AI Data Science Pipelines server.

Usage:
    python -m src.pipeline.upload_pipeline

Requires:
    - oc login (already authenticated)
    - DSPA deployed (set OPENSHIFT_APP_NAMESPACE for non-default project, e.g. fkm)
"""

import time

from kfp import compiler

from src.pipeline.dsp_client import (
    DEFAULT_NAMESPACE,
    connect_dsp_client,
    get_dsp_route_host,
)

PIPELINE_YAML = "customer_churn_pipeline.yaml"
PIPELINE_NAME = "customer-churn-ml-pipeline"


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
    print(f"Connecting to DSP at {get_dsp_route_host(DEFAULT_NAMESPACE)} (namespace={DEFAULT_NAMESPACE})")
    client = connect_dsp_client(namespace=DEFAULT_NAMESPACE)

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
    print("\nView in OpenShift AI dashboard → Data Science Pipelines → Runs")


if __name__ == "__main__":
    main()
