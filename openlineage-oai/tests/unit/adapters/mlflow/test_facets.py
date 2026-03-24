"""Unit tests for MLflow-specific facets."""

from openlineage_oai.adapters.mlflow.facets import (
    MLFLOW_PRODUCER,
    create_mlflow_dataset_facet,
    create_mlflow_model_facet,
    create_mlflow_run_facet,
    filter_system_tags,
)


class TestMLflowRunFacet:
    """Tests for MLflow run facet."""

    def test_basic_run_facet(self):
        """Test basic run facet creation."""
        facet = create_mlflow_run_facet(
            run_id="run-123",
            experiment_id="exp-456",
        )

        assert facet["_producer"] == MLFLOW_PRODUCER
        assert facet["runId"] == "run-123"
        assert facet["experimentId"] == "exp-456"
        assert facet["params"] == {}
        assert facet["metrics"] == {}
        assert facet["tags"] == {}

    def test_run_facet_with_all_fields(self):
        """Test run facet with all fields populated."""
        facet = create_mlflow_run_facet(
            run_id="run-123",
            experiment_id="exp-456",
            experiment_name="My Experiment",
            run_name="training-v1",
            user_id="user@example.com",
            lifecycle_stage="active",
            params={"lr": "0.01", "epochs": "100"},
            metrics={"accuracy": 0.95, "loss": 0.05},
            tags={"env": "production"},
        )

        assert facet["experimentName"] == "My Experiment"
        assert facet["runName"] == "training-v1"
        assert facet["userId"] == "user@example.com"
        assert facet["lifecycleStage"] == "active"
        assert facet["params"]["lr"] == "0.01"
        assert facet["metrics"]["accuracy"] == 0.95
        assert facet["tags"]["env"] == "production"


class TestMLflowDatasetFacet:
    """Tests for MLflow dataset facet."""

    def test_basic_dataset_facet(self):
        """Test basic dataset facet creation."""
        facet = create_mlflow_dataset_facet(
            name="iris",
            source="s3://bucket/iris.parquet",
        )

        assert facet["name"] == "iris"
        assert facet["source"] == "s3://bucket/iris.parquet"
        assert facet["sourceType"] == "unknown"
        assert facet["context"] == "training"

    def test_dataset_facet_with_all_fields(self):
        """Test dataset facet with all fields."""
        facet = create_mlflow_dataset_facet(
            name="training_data",
            source="s3://bucket/data.parquet",
            source_type="parquet",
            digest="abc123",
            context="validation",
            profile={"num_rows": 1000, "num_features": 10},
        )

        assert facet["sourceType"] == "parquet"
        assert facet["digest"] == "abc123"
        assert facet["context"] == "validation"
        assert facet["profile"]["num_rows"] == 1000


class TestMLflowModelFacet:
    """Tests for MLflow model facet."""

    def test_basic_model_facet(self):
        """Test basic model facet creation."""
        facet = create_mlflow_model_facet(
            artifact_path="model",
            run_id="run-123",
        )

        assert facet["artifactPath"] == "model"
        assert facet["runId"] == "run-123"
        assert facet["flavors"] == []

    def test_model_facet_with_registry(self):
        """Test model facet with registry information."""
        facet = create_mlflow_model_facet(
            artifact_path="model",
            run_id="run-123",
            flavors=["sklearn", "python_function"],
            model_uuid="model-uuid-456",
            registered_model_name="iris-classifier",
            registered_model_version="1",
        )

        assert facet["flavors"] == ["sklearn", "python_function"]
        assert facet["modelUuid"] == "model-uuid-456"
        assert facet["registeredModelName"] == "iris-classifier"
        assert facet["registeredModelVersion"] == "1"


class TestFilterSystemTags:
    """Tests for system tag filtering."""

    def test_filters_mlflow_tags(self):
        """Test that mlflow.* tags are filtered out."""
        tags = {
            "mlflow.user": "admin",
            "mlflow.source.name": "train.py",
            "mlflow.source.type": "LOCAL",
            "env": "production",
            "version": "1.0",
        }

        filtered = filter_system_tags(tags)

        assert "mlflow.user" not in filtered
        assert "mlflow.source.name" not in filtered
        assert "env" in filtered
        assert "version" in filtered
        assert len(filtered) == 2

    def test_empty_tags(self):
        """Test filtering empty tags."""
        assert filter_system_tags({}) == {}
        assert filter_system_tags(None) == {}

    def test_no_mlflow_tags(self):
        """Test when there are no mlflow.* tags."""
        tags = {"env": "prod", "team": "ml"}

        filtered = filter_system_tags(tags)

        assert filtered == tags
