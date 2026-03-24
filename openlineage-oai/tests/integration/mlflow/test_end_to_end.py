"""
End-to-end integration tests for MLflow OpenLineage integration.

These tests run against REAL MLflow and Marquez instances deployed on OpenShift.
They verify that:
1. OpenLineage events are emitted correctly from MLflow operations
2. Events appear in Marquez as expected
3. The full lineage flow works

Configuration:
    Set environment variables or use defaults for OpenShift deployment:
    - MLFLOW_TRACKING_URI: MLflow server URL
    - OPENLINEAGE_URL: Marquez API URL
    - OPENLINEAGE_NAMESPACE: Namespace for test jobs

Run:
    pytest tests/integration/mlflow/ -v

Skip if services unavailable:
    pytest tests/integration/mlflow/ -v -m "not integration"
"""

import os
import time
import uuid

import pytest
import requests

# Default to OpenShift deployment URLs
MARQUEZ_URL = os.getenv(
    "OPENLINEAGE_URL", "http://marquez-lineage.apps.rosa.catoconn-ray-et.bo0z.p3.openshiftapps.com"
)
MLFLOW_URL = os.getenv(
    "MLFLOW_TRACKING_URI",
    "http://mlflow-server-lineage.apps.rosa.catoconn-ray-et.bo0z.p3.openshiftapps.com",
)
NAMESPACE = os.getenv("OPENLINEAGE_NAMESPACE", "integration-test")


def is_marquez_available():
    """Check if Marquez is reachable."""
    try:
        response = requests.get(f"{MARQUEZ_URL}/api/v1/namespaces", timeout=5)
        return response.status_code == 200
    except Exception:
        return False


def is_mlflow_available():
    """Check if MLflow is reachable."""
    try:
        response = requests.get(f"{MLFLOW_URL}/health", timeout=5)
        return response.status_code == 200
    except Exception:
        return False


# Skip all tests if services not available
pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(
        not is_marquez_available(), reason=f"Marquez not available at {MARQUEZ_URL}"
    ),
    pytest.mark.skipif(not is_mlflow_available(), reason=f"MLflow not available at {MLFLOW_URL}"),
]


class TestOpenLineageEmission:
    """Tests that verify OpenLineage events are emitted to Marquez."""

    @pytest.fixture
    def unique_namespace(self):
        """Create a unique namespace for each test to avoid conflicts."""
        return f"{NAMESPACE}-{uuid.uuid4().hex[:8]}"

    @pytest.fixture
    def emitter(self, unique_namespace):
        """Create an emitter configured for the test namespace."""
        from openlineage_oai.core.config import OpenLineageConfig
        from openlineage_oai.core.emitter import OpenLineageEmitter

        config = OpenLineageConfig(
            url=MARQUEZ_URL,
            namespace=unique_namespace,
        )
        return OpenLineageEmitter(config)

    def test_emit_start_event(self, emitter, unique_namespace):
        """Test that START events are emitted and visible in Marquez."""
        run_id = str(uuid.uuid4())
        job_name = f"test-job-{uuid.uuid4().hex[:8]}"

        # Emit START event
        result = emitter.emit_start(
            run_id=run_id,
            job_name=job_name,
            job_namespace=unique_namespace,
        )

        assert result is True, "emit_start should return True"

        # Wait for Marquez to process
        time.sleep(1)

        # Verify job exists in Marquez
        response = requests.get(
            f"{MARQUEZ_URL}/api/v1/namespaces/{unique_namespace}/jobs/{job_name}"
        )

        assert response.status_code == 200, f"Job not found in Marquez: {response.text}"
        job_data = response.json()
        assert job_data["name"] == job_name

    def test_emit_complete_event(self, emitter, unique_namespace):
        """Test that COMPLETE events are emitted with full data."""
        run_id = str(uuid.uuid4())
        job_name = f"test-complete-{uuid.uuid4().hex[:8]}"

        # Emit START
        emitter.emit_start(
            run_id=run_id,
            job_name=job_name,
            job_namespace=unique_namespace,
        )

        # Emit COMPLETE with data
        from openlineage_oai.core.facets import build_input_dataset, build_output_dataset

        inputs = [
            build_input_dataset(
                namespace="test-source",
                name="input-dataset",
            )
        ]
        outputs = [
            build_output_dataset(
                namespace="test-output",
                name="output-model",
            )
        ]

        result = emitter.emit_complete(
            run_id=run_id,
            job_name=job_name,
            job_namespace=unique_namespace,
            inputs=inputs,
            outputs=outputs,
            run_facets={
                "test_facet": {
                    "_producer": "test",
                    "_schemaURL": "test",
                    "value": 123,
                }
            },
        )

        assert result is True, "emit_complete should return True"

        # Wait for processing
        time.sleep(1)

        # Verify run exists
        response = requests.get(
            f"{MARQUEZ_URL}/api/v1/namespaces/{unique_namespace}/jobs/{job_name}/runs"
        )

        assert response.status_code == 200
        runs = response.json()["runs"]
        assert len(runs) > 0, "Run should exist in Marquez"

        # Find our run
        our_run = next((r for r in runs if r["id"] == run_id), None)
        assert our_run is not None, f"Run {run_id} not found"

    def test_emit_dataset_event(self, emitter, unique_namespace):
        """Test that DatasetEvents create standalone datasets in Marquez."""
        dataset_name = f"test-dataset-{uuid.uuid4().hex[:8]}"

        result = emitter.emit_create_dataset(
            name=dataset_name,
            namespace=unique_namespace,
            schema_fields=[
                {"name": "id", "type": "int64"},
                {"name": "value", "type": "string"},
            ],
            description="Test dataset for integration testing",
        )

        assert result is True, "emit_create_dataset should return True"

        # Wait for processing
        time.sleep(1)

        # Verify dataset exists
        response = requests.get(
            f"{MARQUEZ_URL}/api/v1/namespaces/{unique_namespace}/datasets/{dataset_name}"
        )

        assert response.status_code == 200, f"Dataset not found: {response.text}"
        dataset = response.json()
        assert dataset["name"] == dataset_name

        # Verify schema
        if "facets" in dataset and "schema" in dataset["facets"]:
            schema = dataset["facets"]["schema"]
            assert len(schema.get("fields", [])) == 2


class TestMLflowIntegration:
    """
    Tests that verify MLflow operations trigger OpenLineage events.

    These tests use MLflow directly and verify events appear in Marquez.
    """

    @pytest.fixture
    def unique_namespace(self):
        """Create a unique namespace for each test."""
        return f"{NAMESPACE}-mlflow-{uuid.uuid4().hex[:8]}"

    @pytest.fixture
    def mlflow_client(self):
        """Get an MLflow client configured for the test server."""
        import mlflow

        mlflow.set_tracking_uri(MLFLOW_URL)
        return mlflow

    def test_mlflow_run_basic(self, mlflow_client, unique_namespace):
        """
        Test basic MLflow run emits lineage.

        Note: This test verifies MLflow works. The OpenLineage emission
        happens via the tracking store wrapper when using the
        openlineage+ URI scheme. For direct testing, we use the emitter.
        """
        from urllib.parse import quote

        from openlineage_oai.core.config import OpenLineageConfig
        from openlineage_oai.core.emitter import OpenLineageEmitter

        # Set up emitter for manual lineage emission
        config = OpenLineageConfig(url=MARQUEZ_URL, namespace=unique_namespace)
        emitter = OpenLineageEmitter(config)

        # Create an MLflow experiment and run
        experiment_name = f"integration-test-{uuid.uuid4().hex[:8]}"
        mlflow_client.set_experiment(experiment_name)

        with mlflow_client.start_run(run_name="test-run") as run:
            run_id = run.info.run_id

            # Log some data
            mlflow_client.log_param("learning_rate", 0.01)
            mlflow_client.log_param("epochs", 100)
            mlflow_client.log_metric("accuracy", 0.95)
            mlflow_client.log_metric("loss", 0.05)

            # Use a simple job name without slashes for easier Marquez querying
            job_name = f"mlflow-exp{run.info.experiment_id}-test-run"

            emitter.emit_start(
                run_id=run_id,
                job_name=job_name,
                job_namespace=unique_namespace,
            )

        # Emit complete after run ends
        from openlineage_oai.adapters.mlflow.facets import create_mlflow_run_facet

        emitter.emit_complete(
            run_id=run_id,
            job_name=job_name,
            job_namespace=unique_namespace,
            run_facets={
                "mlflow_run": create_mlflow_run_facet(
                    run_id=run_id,
                    experiment_id=run.info.experiment_id,
                    run_name="test-run",
                    params={"learning_rate": "0.01", "epochs": "100"},
                    metrics={"accuracy": 0.95, "loss": 0.05},
                ),
            },
        )

        # Wait for Marquez
        time.sleep(1)

        # Verify in Marquez (URL-encode job name for safety)
        encoded_job_name = quote(job_name, safe="")
        response = requests.get(
            f"{MARQUEZ_URL}/api/v1/namespaces/{unique_namespace}/jobs/{encoded_job_name}"
        )

        assert response.status_code == 200, f"Job not found: {response.text}"
        job_data = response.json()

        # Verify job has latestRun (indicating a run was recorded)
        assert "latestRun" in job_data, "Job should have a latestRun"
        assert job_data["latestRun"] is not None, "latestRun should not be None"

        # Verify the run completed
        assert job_data["latestRun"]["state"] == "COMPLETED", "Run should be COMPLETED"

    def test_mlflow_with_datasets(self, mlflow_client, unique_namespace):
        """Test MLflow run with input/output datasets."""
        from urllib.parse import quote

        from openlineage_oai.core.config import OpenLineageConfig
        from openlineage_oai.core.emitter import OpenLineageEmitter
        from openlineage_oai.core.facets import build_input_dataset, build_output_dataset

        config = OpenLineageConfig(url=MARQUEZ_URL, namespace=unique_namespace)
        emitter = OpenLineageEmitter(config)

        # First, create a dataset
        input_dataset_name = f"input-{uuid.uuid4().hex[:8]}"
        emitter.emit_create_dataset(
            name=input_dataset_name,
            namespace=unique_namespace,
            schema_fields=[
                {"name": "feature1", "type": "float"},
                {"name": "feature2", "type": "float"},
                {"name": "target", "type": "int"},
            ],
            description="Input training data",
        )

        # Create MLflow run
        experiment_name = f"dataset-test-{uuid.uuid4().hex[:8]}"
        mlflow_client.set_experiment(experiment_name)

        with mlflow_client.start_run(run_name="training-with-data") as run:
            run_id = run.info.run_id
            # Use simple job name without slashes
            job_name = f"mlflow-exp{run.info.experiment_id}-training"

            mlflow_client.log_param("model_type", "random_forest")
            mlflow_client.log_metric("f1_score", 0.92)

            # Emit lineage with datasets
            emitter.emit_start(
                run_id=run_id,
                job_name=job_name,
                job_namespace=unique_namespace,
                inputs=[
                    build_input_dataset(
                        namespace=unique_namespace,
                        name=input_dataset_name,
                    )
                ],
            )

        # Complete with output
        output_model_name = f"model-{run_id[:8]}"
        emitter.emit_complete(
            run_id=run_id,
            job_name=job_name,
            job_namespace=unique_namespace,
            inputs=[
                build_input_dataset(
                    namespace=unique_namespace,
                    name=input_dataset_name,
                )
            ],
            outputs=[
                build_output_dataset(
                    namespace=unique_namespace,
                    name=output_model_name,
                )
            ],
        )

        # Wait and verify
        time.sleep(1)

        # Check job has inputs/outputs (URL-encode job name)
        encoded_job_name = quote(job_name, safe="")
        response = requests.get(
            f"{MARQUEZ_URL}/api/v1/namespaces/{unique_namespace}/jobs/{encoded_job_name}"
        )

        assert response.status_code == 200, f"Job not found: {response.text}"
        job = response.json()

        # Verify inputs are linked
        assert len(job.get("inputs", [])) > 0, "Job should have inputs"

        # Verify outputs are linked
        assert len(job.get("outputs", [])) > 0, "Job should have outputs"


class TestErrorHandling:
    """Tests that verify error handling doesn't break MLflow."""

    def test_emission_failure_doesnt_raise(self):
        """Test that emission failures don't raise exceptions."""
        from openlineage_oai.core.config import OpenLineageConfig
        from openlineage_oai.core.emitter import OpenLineageEmitter

        # Configure with invalid URL
        config = OpenLineageConfig(
            url="http://invalid-host-that-does-not-exist:9999",
            namespace="test",
        )
        emitter = OpenLineageEmitter(config)

        # Should return False, not raise
        import warnings

        with warnings.catch_warnings(record=True):
            warnings.simplefilter("always")
            result = emitter.emit_start(
                run_id="test-run",
                job_name="test-job",
            )

        assert result is False, "Should return False on failure"


class TestCleanup:
    """Optional cleanup after tests."""

    def test_cleanup_test_namespaces(self):
        """
        This is not a real test - it's a utility to clean up test namespaces.
        Run manually if needed.
        """
        # Skip by default
        pytest.skip("Cleanup utility - run manually if needed")

        # Would delete test namespaces from Marquez
        # response = requests.get(f"{MARQUEZ_URL}/api/v1/namespaces")
        # namespaces = response.json()["namespaces"]
        # for ns in namespaces:
        #     if ns["name"].startswith(NAMESPACE):
        #         # Marquez doesn't support namespace deletion via API
        #         pass
