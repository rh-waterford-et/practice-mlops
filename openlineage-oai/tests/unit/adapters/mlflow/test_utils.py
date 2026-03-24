"""Unit tests for MLflow utility functions."""

from unittest.mock import MagicMock

from openlineage_oai.adapters.mlflow.utils import (
    build_model_name,
    build_model_namespace,
    extract_dataset_info,
    extract_model_info,
)


class TestExtractDatasetInfo:
    """Tests for extract_dataset_info function."""

    def test_minimal_dataset(self):
        """Test with minimal dataset attributes."""
        dataset = MagicMock()
        dataset.name = "test-dataset"
        dataset.digest = "abc123"
        del dataset.source
        del dataset.schema
        del dataset.profile

        result = extract_dataset_info(dataset)

        assert result["name"] == "test-dataset"
        assert result["digest"] == "abc123"
        assert result["source_type"] == "unknown"
        assert result["source"] == ""
        assert result["schema"] is None

    def test_with_dict_source(self):
        """Test dataset with source that has to_dict method."""
        dataset = MagicMock()
        dataset.name = "pandas-dataset"
        dataset.digest = "xyz789"

        source = MagicMock()
        source.to_dict.return_value = {
            "type": "pandas",
            "uri": "memory://pandas/iris",
        }
        dataset.source = source
        del dataset.schema
        del dataset.profile

        result = extract_dataset_info(dataset)

        assert result["source_type"] == "pandas"
        assert result["source"] == "memory://pandas/iris"

    def test_with_string_source(self):
        """Test dataset with simple string source."""
        dataset = MagicMock()
        dataset.name = "file-dataset"
        dataset.digest = ""

        dataset.source = "/path/to/data.csv"
        del dataset.schema
        del dataset.profile

        result = extract_dataset_info(dataset)

        assert result["source"] == "/path/to/data.csv"
        assert result["source_type"] == "unknown"

    def test_with_schema_dict(self):
        """Test dataset with schema that converts to dict list."""
        dataset = MagicMock()
        dataset.name = "typed-dataset"
        dataset.digest = ""
        del dataset.source
        del dataset.profile

        schema = MagicMock()
        schema.to_dict.return_value = [
            {"name": "col1", "type": "string"},
            {"name": "col2", "type": "integer"},
        ]
        dataset.schema = schema

        result = extract_dataset_info(dataset)

        assert result["schema"] is not None
        assert len(result["schema"]) == 2
        assert result["schema"][0]["name"] == "col1"
        assert result["schema"][0]["type"] == "string"

    def test_with_schema_inputs(self):
        """Test dataset with ModelSignature-style schema."""
        dataset = MagicMock()
        dataset.name = "model-dataset"
        dataset.digest = ""
        del dataset.source
        del dataset.profile

        col1 = MagicMock()
        col1.name = "feature1"
        col1.type = "double"

        col2 = MagicMock()
        col2.name = "feature2"
        col2.type = "double"

        schema = MagicMock()
        schema.inputs = [col1, col2]
        del schema.to_dict
        dataset.schema = schema

        result = extract_dataset_info(dataset)

        assert result["schema"] is not None
        assert len(result["schema"]) == 2
        assert result["schema"][0]["name"] == "feature1"

    def test_with_profile(self):
        """Test dataset with profile information."""
        dataset = MagicMock()
        dataset.name = "profiled-dataset"
        dataset.digest = ""
        del dataset.source
        del dataset.schema

        dataset.profile = {"num_rows": 1000, "num_cols": 5}

        result = extract_dataset_info(dataset)

        assert "profile" in result
        assert result["profile"]["num_rows"] == 1000

    def test_missing_name_attribute(self):
        """Test dataset without name attribute."""
        dataset = MagicMock(spec=[])  # Empty spec, no attributes
        dataset.name = None
        del dataset.digest

        result = extract_dataset_info(dataset)

        # getattr with default should return "unknown" for missing name
        assert result["name"] is None or result["name"] == "unknown"


class TestExtractModelInfo:
    """Tests for extract_model_info function."""

    def test_minimal_model(self):
        """Test with just artifact path and run_id."""
        result = extract_model_info(
            artifact_path="model",
            run_id="run-123",
            model_info=None,
        )

        assert result["artifact_path"] == "model"
        assert result["run_id"] == "run-123"
        assert result["flavors"] == []
        assert result["model_uuid"] == ""

    def test_with_flavors(self):
        """Test model with flavors."""
        model_info = MagicMock()
        model_info.flavors = {"sklearn": {}, "python_function": {}}
        model_info.model_uuid = None
        model_info.signature = None

        result = extract_model_info(
            artifact_path="sklearn_model",
            run_id="run-456",
            model_info=model_info,
        )

        assert "sklearn" in result["flavors"]
        assert "python_function" in result["flavors"]

    def test_with_model_uuid(self):
        """Test model with UUID."""
        model_info = MagicMock()
        model_info.flavors = {}
        model_info.model_uuid = "uuid-12345"
        model_info.signature = None

        result = extract_model_info(
            artifact_path="model",
            run_id="run-789",
            model_info=model_info,
        )

        assert result["model_uuid"] == "uuid-12345"

    def test_with_signature(self):
        """Test model with signature."""
        model_info = MagicMock()
        model_info.flavors = {}
        model_info.model_uuid = ""

        inputs = MagicMock()
        inputs.to_json.return_value = '[{"name": "x", "type": "double"}]'

        outputs = MagicMock()
        outputs.to_json.return_value = '[{"name": "y", "type": "long"}]'

        signature = MagicMock()
        signature.inputs = inputs
        signature.outputs = outputs
        model_info.signature = signature

        result = extract_model_info(
            artifact_path="model",
            run_id="run-000",
            model_info=model_info,
        )

        assert "double" in result["signature_inputs"]
        assert "long" in result["signature_outputs"]

    def test_signature_json_error(self):
        """Test handling of signature JSON conversion error."""
        model_info = MagicMock()
        model_info.flavors = {}
        model_info.model_uuid = ""

        inputs = MagicMock()
        inputs.to_json.side_effect = Exception("JSON error")

        signature = MagicMock()
        signature.inputs = inputs
        signature.outputs = None
        model_info.signature = signature

        # Should not raise, just return empty signature_inputs
        result = extract_model_info(
            artifact_path="model",
            run_id="run-error",
            model_info=model_info,
        )

        assert result["signature_inputs"] == ""


class TestParseSchemaJson:
    """Tests for _parse_schema_json function."""

    def test_mlflow_colspec_format(self):
        """Test parsing mlflow_colspec JSON format."""
        from openlineage_oai.adapters.mlflow.utils import _parse_schema_json

        schema_json = '{"mlflow_colspec": [{"type": "double", "name": "x", "required": true}, {"type": "long", "name": "y", "required": false}]}'
        result = _parse_schema_json(schema_json)

        assert result is not None
        assert len(result) == 2
        assert result[0]["name"] == "x"
        assert result[0]["type"] == "double"
        assert result[1]["name"] == "y"
        assert result[1]["type"] == "long"

    def test_list_format(self):
        """Test parsing direct list JSON format."""
        from openlineage_oai.adapters.mlflow.utils import _parse_schema_json

        schema_json = '[{"name": "a", "type": "string"}, {"name": "b", "type": "int"}]'
        result = _parse_schema_json(schema_json)

        assert result is not None
        assert len(result) == 2
        assert result[0]["name"] == "a"

    def test_invalid_json(self):
        """Test handling of invalid JSON."""
        from openlineage_oai.adapters.mlflow.utils import _parse_schema_json

        result = _parse_schema_json("not valid json")
        assert result is None

    def test_empty_string(self):
        """Test handling of empty string."""
        from openlineage_oai.adapters.mlflow.utils import _parse_schema_json

        result = _parse_schema_json("")
        assert result is None


class TestBuildModelNamespace:
    """Tests for build_model_namespace function."""

    def test_postgresql_uri(self):
        """Test with PostgreSQL tracking URI."""
        result = build_model_namespace("postgresql://localhost:5432/mlflow")

        assert result == "mlflow://postgresql/localhost:5432/mlflow"

    def test_http_uri(self):
        """Test with HTTP tracking URI."""
        result = build_model_namespace("http://mlflow-server:5000")

        assert result == "mlflow://http/mlflow-server:5000"

    def test_file_uri(self):
        """Test with file tracking URI."""
        result = build_model_namespace("file:///tmp/mlruns")

        assert result == "mlflow://file//tmp/mlruns"


class TestBuildModelName:
    """Tests for build_model_name function."""

    def test_basic_model_name(self):
        """Test basic model name building."""
        result = build_model_name(run_id="abc-123", artifact_path="model")

        assert result == "runs/abc-123/artifacts/model"

    def test_nested_artifact_path(self):
        """Test with nested artifact path."""
        result = build_model_name(run_id="def-456", artifact_path="models/sklearn")

        assert result == "runs/def-456/artifacts/models/sklearn"
