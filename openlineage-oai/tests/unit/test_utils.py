"""Unit tests for utility modules."""

from openlineage_oai.utils.naming import (
    build_job_name,
    build_mlflow_job_name,
    extract_namespace_from_uri,
    sanitize_name,
)
from openlineage_oai.utils.uri import (
    is_openlineage_uri,
    normalize_dataset_uri,
    parse_tracking_uri,
)


class TestBuildJobName:
    """Tests for job name building."""

    def test_basic_job_name(self):
        """Test basic job name without context."""
        name = build_job_name(tool="mlflow", name="my-run")
        assert name == "mlflow/my-run"

    def test_job_name_with_context(self):
        """Test job name with context."""
        name = build_job_name(tool="mlflow", name="my-run", context="experiment-123")
        assert name == "mlflow/experiment-123/my-run"

    def test_job_name_sanitization(self):
        """Test that job names are sanitized."""
        name = build_job_name(tool="MLflow", name="My Run_v2", context="Experiment 123")
        assert name == "mlflow/experiment-123/my-run-v2"


class TestBuildMLflowJobName:
    """Tests for MLflow-specific job name building."""

    def test_with_experiment_name(self):
        """Test MLflow job name uses experiment name."""
        name = build_mlflow_job_name(
            experiment_id="1", experiment_name="customer_churn_lineage",
        )
        assert name == "mlflow-customer-churn-lineage"

    def test_falls_back_to_experiment_id(self):
        """Test MLflow job name falls back to experiment ID."""
        name = build_mlflow_job_name(experiment_id="123")
        assert name == "mlflow-experiment-123"

    def test_experiment_name_takes_priority(self):
        """Test experiment name is preferred over run name / run ID."""
        name = build_mlflow_job_name(
            experiment_id="123",
            run_name="named-run",
            run_id="abc-def-456",
            experiment_name="my_experiment",
        )
        assert name == "mlflow-my-experiment"


class TestSanitizeName:
    """Tests for name sanitization."""

    def test_lowercase(self):
        """Test conversion to lowercase."""
        assert sanitize_name("MyName") == "myname"

    def test_spaces_to_hyphens(self):
        """Test spaces are replaced with hyphens."""
        assert sanitize_name("my name") == "my-name"

    def test_underscores_to_hyphens(self):
        """Test underscores are replaced with hyphens."""
        assert sanitize_name("my_name") == "my-name"

    def test_special_chars_removed(self):
        """Test special characters are removed."""
        assert sanitize_name("name@#$%^123") == "name123"

    def test_multiple_hyphens_collapsed(self):
        """Test multiple hyphens are collapsed."""
        assert sanitize_name("my---name") == "my-name"

    def test_leading_trailing_hyphens_stripped(self):
        """Test leading/trailing hyphens are stripped."""
        assert sanitize_name("-my-name-") == "my-name"

    def test_empty_becomes_unnamed(self):
        """Test empty string becomes 'unnamed'."""
        assert sanitize_name("") == "unnamed"
        assert sanitize_name("@#$%") == "unnamed"


class TestExtractNamespaceFromUri:
    """Tests for namespace extraction from URIs."""

    def test_s3_uri(self):
        """Test S3 URI namespace extraction."""
        assert extract_namespace_from_uri("s3://bucket/path") == "s3"

    def test_postgresql_uri(self):
        """Test PostgreSQL URI namespace extraction."""
        assert extract_namespace_from_uri("postgresql://host:5432/db") == "postgresql"

    def test_file_uri(self):
        """Test file URI namespace extraction."""
        assert extract_namespace_from_uri("file:///path/to/data") == "file"

    def test_http_uri(self):
        """Test HTTP URI namespace extraction."""
        assert extract_namespace_from_uri("http://server:5000") == "http"

    def test_invalid_uri(self):
        """Test invalid URI returns 'unknown'."""
        assert extract_namespace_from_uri("not-a-uri") == "unknown"


class TestParseTrackingUri:
    """Tests for tracking URI parsing."""

    def test_openlineage_postgresql(self):
        """Test parsing OpenLineage PostgreSQL URI."""
        result = parse_tracking_uri("openlineage+postgresql://user:pass@localhost:5432/mlflow")

        assert result.backend_uri == "postgresql://user:pass@localhost:5432/mlflow"
        assert result.backend_scheme == "postgresql"

    def test_openlineage_http(self):
        """Test parsing OpenLineage HTTP URI."""
        result = parse_tracking_uri("openlineage+http://mlflow-server:5000")

        assert result.backend_uri == "http://mlflow-server:5000"
        assert result.backend_scheme == "http"

    def test_openlineage_file(self):
        """Test parsing OpenLineage file URI."""
        result = parse_tracking_uri("openlineage+file:///tmp/mlruns")

        assert result.backend_uri == "file:///tmp/mlruns"
        assert result.backend_scheme == "file"

    def test_with_query_params(self, monkeypatch):
        """Test parsing URI with OpenLineage query params."""
        monkeypatch.delenv("OPENLINEAGE_URL", raising=False)
        monkeypatch.delenv("OPENLINEAGE_NAMESPACE", raising=False)

        result = parse_tracking_uri(
            "openlineage+postgresql://localhost/db?openlineage_url=http://marquez:5000&openlineage_namespace=test-ns"
        )

        assert result.backend_uri == "postgresql://localhost/db"
        assert result.openlineage_url == "http://marquez:5000"
        assert result.openlineage_namespace == "test-ns"

    def test_env_fallback(self, monkeypatch):
        """Test fallback to environment variables."""
        monkeypatch.setenv("OPENLINEAGE_URL", "http://env-marquez:5000")
        monkeypatch.setenv("OPENLINEAGE_NAMESPACE", "env-ns")

        result = parse_tracking_uri("openlineage+postgresql://localhost/db")

        assert result.openlineage_url == "http://env-marquez:5000"
        assert result.openlineage_namespace == "env-ns"

    def test_non_openlineage_uri(self):
        """Test parsing regular (non-OpenLineage) URI."""
        result = parse_tracking_uri("postgresql://localhost/db")

        assert result.backend_uri == "postgresql://localhost/db"
        assert result.backend_scheme == "postgresql"


class TestNormalizeDatasetUri:
    """Tests for dataset URI normalization."""

    def test_s3_uri(self):
        """Test normalizing S3 URI."""
        namespace, name = normalize_dataset_uri("s3://bucket/path/data.parquet")

        assert namespace == "s3"
        assert name == "bucket/path/data.parquet"

    def test_file_uri(self):
        """Test normalizing file URI."""
        namespace, name = normalize_dataset_uri("file:///home/user/data.csv")

        assert namespace == "file"
        assert name == "home/user/data.csv"

    def test_local_path(self):
        """Test normalizing local path without scheme."""
        namespace, name = normalize_dataset_uri("/home/user/data.csv")

        assert namespace == "file"
        assert name == "home/user/data.csv"

    def test_postgresql_uri(self):
        """Test normalizing PostgreSQL URI."""
        namespace, name = normalize_dataset_uri("postgresql://host:5432/db/schema.table")

        assert namespace == "postgresql"
        assert name == "host:5432/db/schema.table"


class TestIsOpenlineageUri:
    """Tests for OpenLineage URI detection."""

    def test_openlineage_uri(self):
        """Test detection of OpenLineage URI."""
        assert is_openlineage_uri("openlineage+postgresql://localhost") is True

    def test_regular_uri(self):
        """Test regular URI is not detected."""
        assert is_openlineage_uri("postgresql://localhost") is False
        assert is_openlineage_uri("http://server:5000") is False
