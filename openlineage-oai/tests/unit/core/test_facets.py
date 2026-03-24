"""Unit tests for facets module."""

from openlineage_oai.core.facets import (
    DEFAULT_PRODUCER,
    SCHEMA_BASE,
    build_input_dataset,
    build_output_dataset,
    create_documentation_facet,
    create_error_facet,
    create_job_documentation_facet,
    create_job_type_facet,
    create_parent_run_facet,
    create_schema_facet,
    create_source_code_location_facet,
)


class TestSchemaFacet:
    """Tests for schema facet creation."""

    def test_basic_schema(self):
        """Test creating a basic schema facet."""
        fields = [
            {"name": "id", "type": "int64"},
            {"name": "name", "type": "string"},
        ]

        facet = create_schema_facet(fields)

        assert facet["_producer"] == DEFAULT_PRODUCER
        assert facet["_schemaURL"] == f"{SCHEMA_BASE}/SchemaDatasetFacet.json"
        assert facet["fields"] == fields

    def test_schema_with_descriptions(self):
        """Test schema facet with field descriptions."""
        fields = [
            {"name": "id", "type": "int64", "description": "Primary key"},
            {"name": "name", "type": "string", "description": "User name"},
        ]

        facet = create_schema_facet(fields)

        assert facet["fields"][0]["description"] == "Primary key"

    def test_empty_schema(self):
        """Test schema facet with no fields."""
        facet = create_schema_facet([])

        assert facet["fields"] == []

    def test_custom_producer(self):
        """Test schema facet with custom producer."""
        facet = create_schema_facet([], producer="custom-producer")

        assert facet["_producer"] == "custom-producer"


class TestDocumentationFacets:
    """Tests for documentation facets."""

    def test_dataset_documentation(self):
        """Test dataset documentation facet."""
        facet = create_documentation_facet("This is a test dataset")

        assert facet["_producer"] == DEFAULT_PRODUCER
        assert facet["_schemaURL"] == f"{SCHEMA_BASE}/DocumentationDatasetFacet.json"
        assert facet["description"] == "This is a test dataset"

    def test_job_documentation(self):
        """Test job documentation facet."""
        facet = create_job_documentation_facet("This job trains a model")

        assert facet["_producer"] == DEFAULT_PRODUCER
        assert facet["_schemaURL"] == f"{SCHEMA_BASE}/DocumentationJobFacet.json"
        assert facet["description"] == "This job trains a model"


class TestJobTypeFacet:
    """Tests for job type facet."""

    def test_ml_training_job_type(self):
        """Test ML training job type."""
        facet = create_job_type_facet(
            job_type="ML_TRAINING",
            integration="MLFLOW",
            processing_type="BATCH",
        )

        assert facet["jobType"] == "ML_TRAINING"
        assert facet["integration"] == "MLFLOW"
        assert facet["processingType"] == "BATCH"

    def test_default_values(self):
        """Test default values for job type facet."""
        facet = create_job_type_facet(job_type="ETL")

        assert facet["jobType"] == "ETL"
        assert facet["integration"] == "OPENLINEAGE_ML"
        assert facet["processingType"] == "BATCH"


class TestSourceCodeLocationFacet:
    """Tests for source code location facet."""

    def test_basic_source_location(self):
        """Test basic source code location."""
        facet = create_source_code_location_facet(
            url="https://github.com/org/repo",
            path="src/train.py",
        )

        assert facet["url"] == "https://github.com/org/repo"
        assert facet["path"] == "src/train.py"
        assert facet["type"] == "git"

    def test_with_version(self):
        """Test source code location with version."""
        facet = create_source_code_location_facet(
            url="https://github.com/org/repo",
            path="src/train.py",
            version="v1.0.0",
        )

        assert facet["version"] == "v1.0.0"

    def test_minimal(self):
        """Test minimal source code location (URL only)."""
        facet = create_source_code_location_facet(
            url="https://github.com/org/repo",
        )

        assert facet["url"] == "https://github.com/org/repo"
        assert "path" not in facet
        assert "version" not in facet


class TestErrorFacet:
    """Tests for error facet."""

    def test_basic_error(self):
        """Test basic error facet."""
        facet = create_error_facet("Something went wrong")

        assert facet["message"] == "Something went wrong"
        assert facet["programmingLanguage"] == "python"
        assert "stackTrace" not in facet

    def test_error_with_stack_trace(self):
        """Test error facet with stack trace."""
        facet = create_error_facet(
            message="ValueError: invalid input",
            stack_trace="Traceback (most recent call last):\n  ...",
        )

        assert facet["message"] == "ValueError: invalid input"
        assert "Traceback" in facet["stackTrace"]


class TestParentRunFacet:
    """Tests for parent run facet."""

    def test_parent_run(self):
        """Test parent run facet."""
        facet = create_parent_run_facet(
            run_id="parent-run-123",
            job_name="parent-job",
            job_namespace="parent-ns",
        )

        assert facet["run"]["runId"] == "parent-run-123"
        assert facet["job"]["name"] == "parent-job"
        assert facet["job"]["namespace"] == "parent-ns"


class TestDatasetBuilders:
    """Tests for input/output dataset builders."""

    def test_input_dataset_basic(self):
        """Test basic input dataset structure."""
        dataset = build_input_dataset(
            namespace="s3",
            name="bucket/data.parquet",
        )

        assert dataset["namespace"] == "s3"
        assert dataset["name"] == "bucket/data.parquet"
        assert dataset["facets"] == {}

    def test_input_dataset_with_facets(self):
        """Test input dataset with facets."""
        schema = create_schema_facet([{"name": "id", "type": "int64"}])
        dataset = build_input_dataset(
            namespace="s3",
            name="bucket/data.parquet",
            facets={"schema": schema},
        )

        assert "schema" in dataset["facets"]
        assert dataset["facets"]["schema"]["fields"][0]["name"] == "id"

    def test_output_dataset_basic(self):
        """Test basic output dataset structure."""
        dataset = build_output_dataset(
            namespace="mlflow",
            name="runs/123/artifacts/model",
        )

        assert dataset["namespace"] == "mlflow"
        assert dataset["name"] == "runs/123/artifacts/model"

    def test_output_dataset_with_facets(self):
        """Test output dataset with facets."""
        doc = create_documentation_facet("Trained model")
        dataset = build_output_dataset(
            namespace="mlflow",
            name="model",
            facets={"documentation": doc},
        )

        assert dataset["facets"]["documentation"]["description"] == "Trained model"
