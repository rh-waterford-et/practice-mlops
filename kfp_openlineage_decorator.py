"""
KFP OpenLineage Decorator - Automatically emit lineage events for KFP components
"""

import functools
import json
import os
from datetime import datetime, timezone
from urllib.request import Request, urlopen
from uuid import uuid4
from typing import Callable, Any


def emit_openlineage_event(event: dict, url: str) -> None:
    """Send OpenLineage event to Marquez"""
    event_type = event.get("eventType", "UNKNOWN")
    job_name = event.get("job", {}).get("name", "unknown")

    print(f"[OpenLineage] Emitting {event_type} event for job: {job_name}")

    req = Request(
        f"{url}/api/v1/lineage",
        data=json.dumps(event).encode("utf-8"),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urlopen(req, timeout=10) as resp:
            response_body = resp.read().decode('utf-8')
            print(f"[OpenLineage] {event_type} event sent: HTTP {resp.status}")
            if resp.status != 200 and resp.status != 201:
                print("[OpenLineage] ERROR - Marquez rejected event")
                print(f"[OpenLineage] Response: {response_body}")
                print(f"[OpenLineage] Event that was rejected:")
                print(json.dumps(event, indent=2))
    except Exception as e:
        error_msg = str(e)
        print(f"[OpenLineage] Failed to send {event_type} event: {error_msg}")

        # Try to extract error details from HTTP error
        if hasattr(e, 'read'):
            try:
                error_body = e.read().decode('utf-8')
                print(f"[OpenLineage] Error response body: {error_body}")
            except Exception:
                pass

        print("[OpenLineage] Event payload:")
        print(json.dumps(event, indent=2))
        # Don't raise - allow component to continue


def track_component_lineage(
    namespace: str = None,
    inputs: list = None,
    outputs: list = None,
):
    """
    Decorator to automatically emit OpenLineage events for KFP components.

    Usage:
        @dsl.component(base_image="...")
        @track_component_lineage(
            namespace="customer_churn_pipeline",
            inputs=[{"namespace": "s3://raw-data", "name": "customers.csv"}],
            outputs=[{"namespace": "postgres://postgres:5432/warehouse", "name": "customer_features"}]
        )
        def my_component(...):
            pass
    """
    def decorator(func: Callable) -> Callable:
        @functools.wraps(func)
        def wrapper(*args, **kwargs) -> Any:
            # Get configuration from environment or defaults
            ol_url = os.getenv("OPENLINEAGE_URL", "http://marquez.lineage.svc")
            ol_namespace = namespace or os.getenv("OPENLINEAGE_NAMESPACE", "kfp-pipeline")

            # Get KFP context if available
            pipeline_run_id = os.getenv("KFP_RUN_ID", str(uuid4()))
            pipeline_name = os.getenv("KFP_PIPELINE_NAME", "unknown_pipeline")

            run_id = str(uuid4())
            job_name = func.__name__

            print(f"[OpenLineage] Decorator wrapping function: {job_name}")
            print(f"[OpenLineage] Run ID: {run_id}")
            print(f"[OpenLineage] Namespace: {ol_namespace}")
            print(f"[OpenLineage] Marquez URL: {ol_url}")

            # START event
            start_time = datetime.now(timezone.utc).isoformat()
            start_event = {
                "eventType": "START",
                "eventTime": start_time,
                "run": {
                    "runId": run_id,
                    "facets": {
                        "parent": {
                            "_producer": "https://github.com/practice-mlops/kfp-openlineage",
                            "_schemaURL": "https://openlineage.io/spec/facets/1-0-0/ParentRunFacet.json",
                            "run": {"runId": pipeline_run_id},
                            "job": {
                                "namespace": "kfp://cluster",
                                "name": pipeline_name,
                            }
                        }
                    }
                },
                "job": {
                    "namespace": ol_namespace,
                    "name": job_name,
                    "facets": {
                        "jobType": {
                            "_producer": "https://github.com/practice-mlops/kfp-openlineage",
                            "_schemaURL": "https://openlineage.io/spec/facets/1-0-0/JobTypeJobFacet.json",
                            "jobType": "KFP_COMPONENT",
                            "integration": "KFP",
                            "processingType": "BATCH"
                        }
                    }
                },
                "producer": "https://github.com/practice-mlops/kfp-openlineage",
                "schemaURL": "https://openlineage.io/spec/2-0-2/OpenLineage.json#/$defs/RunEvent"
            }

            # Only add inputs if they exist
            if inputs:
                start_event["inputs"] = inputs

            emit_openlineage_event(start_event, ol_url)

            # Execute the actual component
            try:
                print(f"[OpenLineage] Executing function: {job_name}")
                result = func(*args, **kwargs)
                print(f"[OpenLineage] Function completed successfully: {job_name}")

                # COMPLETE event
                complete_time = datetime.now(timezone.utc).isoformat()
                print(f"[OpenLineage] Preparing COMPLETE event for: {job_name}")
                complete_event = {
                    "eventType": "COMPLETE",
                    "eventTime": complete_time,
                    "run": {
                        "runId": run_id,
                        "facets": {
                            "parent": {
                                "_producer": "https://github.com/practice-mlops/kfp-openlineage",
                                "_schemaURL": "https://openlineage.io/spec/facets/1-0-0/ParentRunFacet.json",
                                "run": {"runId": pipeline_run_id},
                                "job": {
                                    "namespace": "kfp://cluster",
                                    "name": pipeline_name,
                                }
                            }
                        }
                    },
                    "job": {
                        "namespace": ol_namespace,
                        "name": job_name,
                        "facets": {
                            "jobType": {
                                "_producer": "https://github.com/practice-mlops/kfp-openlineage",
                                "_schemaURL": "https://openlineage.io/spec/facets/1-0-0/JobTypeJobFacet.json",
                                "jobType": "KFP_COMPONENT",
                                "integration": "KFP",
                                "processingType": "BATCH"
                            }
                        }
                    },
                    "producer": "https://github.com/practice-mlops/kfp-openlineage",
                    "schemaURL": "https://openlineage.io/spec/2-0-2/OpenLineage.json#/$defs/RunEvent"
                }

                # Only add inputs/outputs if they exist
                if inputs:
                    complete_event["inputs"] = inputs
                if outputs:
                    complete_event["outputs"] = outputs

                emit_openlineage_event(complete_event, ol_url)
                print("[OpenLineage] COMPLETE event emitted, returning result")

                return result

            except Exception as e:
                # FAIL event
                print(f"[OpenLineage] Function failed with exception: {e}")
                print(f"[OpenLineage] Preparing FAIL event for: {job_name}")
                fail_time = datetime.now(timezone.utc).isoformat()
                fail_event = {
                    "eventType": "FAIL",
                    "eventTime": fail_time,
                    "run": {
                        "runId": run_id,
                        "facets": {
                            "errorMessage": {
                                "_producer": "https://github.com/practice-mlops/kfp-openlineage",
                                "_schemaURL": "https://openlineage.io/spec/facets/1-0-0/ErrorMessageRunFacet.json",
                                "message": str(e),
                                "programmingLanguage": "python"
                            },
                            "parent": {
                                "_producer": "https://github.com/practice-mlops/kfp-openlineage",
                                "_schemaURL": "https://openlineage.io/spec/facets/1-0-0/ParentRunFacet.json",
                                "run": {"runId": pipeline_run_id},
                                "job": {
                                    "namespace": "kfp://cluster",
                                    "name": pipeline_name,
                                }
                            }
                        }
                    },
                    "job": {
                        "namespace": ol_namespace,
                        "name": job_name,
                        "facets": {
                            "jobType": {
                                "_producer": "https://github.com/practice-mlops/kfp-openlineage",
                                "_schemaURL": "https://openlineage.io/spec/facets/1-0-0/JobTypeJobFacet.json",
                                "jobType": "KFP_COMPONENT",
                                "integration": "KFP",
                                "processingType": "BATCH"
                            }
                        }
                    },
                    "producer": "https://github.com/practice-mlops/kfp-openlineage",
                    "schemaURL": "https://openlineage.io/spec/2-0-2/OpenLineage.json#/$defs/RunEvent"
                }

                # Only add inputs if they exist
                if inputs:
                    fail_event["inputs"] = inputs

                emit_openlineage_event(fail_event, ol_url)
                raise

        return wrapper
    return decorator


# Example usage
if __name__ == "__main__":
    from kfp import dsl

    @dsl.component(base_image="python:3.11")
    @track_component_lineage(
        namespace="customer_churn_pipeline",
        inputs=[
            {"namespace": "s3://raw-data", "name": "customers.csv"}
        ],
        outputs=[
            {"namespace": "postgres://postgres:5432/warehouse", "name": "customer_features"}
        ]
    )
    def example_component(input_param: str) -> str:
        print("Doing work...")
        return "done"
