"""Unit tests for the emitter module."""

import json

import pytest
import responses

from openlineage_oai.core.config import OpenLineageConfig
from openlineage_oai.core.emitter import OpenLineageEmitter, generate_run_id


class TestEmitterBasics:
    """Tests for basic emitter functionality."""

    def test_is_enabled_with_valid_config(self):
        """Test is_enabled returns True with valid config."""
        config = OpenLineageConfig(url="http://marquez:5000", enabled=True)
        emitter = OpenLineageEmitter(config)

        assert emitter.is_enabled is True

    def test_is_enabled_without_url(self):
        """Test is_enabled returns False without URL."""
        config = OpenLineageConfig(enabled=True)
        emitter = OpenLineageEmitter(config)

        assert emitter.is_enabled is False

    def test_is_enabled_when_disabled(self):
        """Test is_enabled returns False when disabled."""
        config = OpenLineageConfig(url="http://marquez:5000", enabled=False)
        emitter = OpenLineageEmitter(config)

        assert emitter.is_enabled is False

    def test_from_env_config(self, monkeypatch):
        """Test emitter loads config from environment."""
        monkeypatch.setenv("OPENLINEAGE_URL", "http://env-marquez:5000")
        monkeypatch.setenv("OPENLINEAGE_NAMESPACE", "env-ns")

        emitter = OpenLineageEmitter()

        assert emitter.config.url == "http://env-marquez:5000"
        assert emitter.config.namespace == "env-ns"


class TestRunEvents:
    """Tests for RunEvent emission."""

    @responses.activate
    def test_emit_start_success(self):
        """Test successful START event emission."""
        responses.add(
            responses.POST,
            "http://marquez:5000/api/v1/lineage",
            json={"status": "ok"},
            status=200,
        )

        config = OpenLineageConfig(url="http://marquez:5000", namespace="test-ns")
        emitter = OpenLineageEmitter(config)

        result = emitter.emit_start(
            run_id="run-123",
            job_name="test-job",
        )

        assert result is True
        assert len(responses.calls) == 1

        # Verify event structure
        sent_event = json.loads(responses.calls[0].request.body)
        assert sent_event["eventType"] == "START"
        assert sent_event["run"]["runId"] == "run-123"
        assert sent_event["job"]["name"] == "test-job"
        assert sent_event["job"]["namespace"] == "test-ns"

    @responses.activate
    def test_emit_complete_with_data(self):
        """Test COMPLETE event with inputs, outputs, and facets."""
        responses.add(
            responses.POST,
            "http://marquez:5000/api/v1/lineage",
            json={"status": "ok"},
            status=200,
        )

        config = OpenLineageConfig(url="http://marquez:5000", namespace="test-ns")
        emitter = OpenLineageEmitter(config)

        inputs = [{"namespace": "s3", "name": "input-data", "facets": {}}]
        outputs = [{"namespace": "mlflow", "name": "model", "facets": {}}]
        run_facets = {"custom": {"_producer": "test", "_schemaURL": "test", "value": 123}}
        job_facets = {
            "documentation": {"_producer": "test", "_schemaURL": "test", "description": "Test job"}
        }

        result = emitter.emit_complete(
            run_id="run-123",
            job_name="test-job",
            inputs=inputs,
            outputs=outputs,
            run_facets=run_facets,
            job_facets=job_facets,
        )

        assert result is True

        sent_event = json.loads(responses.calls[0].request.body)
        assert sent_event["eventType"] == "COMPLETE"
        assert sent_event["inputs"] == inputs
        assert sent_event["outputs"] == outputs
        assert sent_event["run"]["facets"]["custom"]["value"] == 123
        assert sent_event["job"]["facets"]["documentation"]["description"] == "Test job"

    @responses.activate
    def test_emit_fail_includes_error_facet(self):
        """Test FAIL event includes error message facet."""
        responses.add(
            responses.POST,
            "http://marquez:5000/api/v1/lineage",
            json={"status": "ok"},
            status=200,
        )

        config = OpenLineageConfig(url="http://marquez:5000", namespace="test-ns")
        emitter = OpenLineageEmitter(config)

        result = emitter.emit_fail(
            run_id="run-123",
            job_name="test-job",
            error_message="Something went wrong",
            stack_trace="Traceback...",
        )

        assert result is True

        sent_event = json.loads(responses.calls[0].request.body)
        assert sent_event["eventType"] == "FAIL"
        assert "errorMessage" in sent_event["run"]["facets"]
        assert sent_event["run"]["facets"]["errorMessage"]["message"] == "Something went wrong"
        assert "Traceback" in sent_event["run"]["facets"]["errorMessage"]["stackTrace"]

    def test_emit_start_disabled_returns_false(self):
        """Test emit_start returns False when emit_on_start is False."""
        config = OpenLineageConfig(
            url="http://marquez:5000",
            emit_on_start=False,
        )
        emitter = OpenLineageEmitter(config)

        result = emitter.emit_start(run_id="run-123", job_name="test-job")

        assert result is False

    def test_emit_complete_disabled_returns_false(self):
        """Test emit_complete returns False when emit_on_end is False."""
        config = OpenLineageConfig(
            url="http://marquez:5000",
            emit_on_end=False,
        )
        emitter = OpenLineageEmitter(config)

        result = emitter.emit_complete(run_id="run-123", job_name="test-job")

        assert result is False


class TestDatasetEvents:
    """Tests for DatasetEvent emission."""

    @responses.activate
    def test_emit_create_dataset(self):
        """Test CREATE DatasetEvent emission."""
        responses.add(
            responses.POST,
            "http://marquez:5000/api/v1/lineage",
            json={"status": "ok"},
            status=200,
        )

        config = OpenLineageConfig(url="http://marquez:5000", namespace="test-ns")
        emitter = OpenLineageEmitter(config)

        result = emitter.emit_create_dataset(
            name="test-dataset",
            namespace="s3",
            schema_fields=[
                {"name": "id", "type": "int64"},
                {"name": "name", "type": "string"},
            ],
            description="Test dataset for unit tests",
        )

        assert result is True

        sent_event = json.loads(responses.calls[0].request.body)
        assert sent_event["eventType"] == "CREATE"
        assert sent_event["dataset"]["name"] == "test-dataset"
        assert sent_event["dataset"]["namespace"] == "s3"
        assert "schema" in sent_event["dataset"]["facets"]
        assert "documentation" in sent_event["dataset"]["facets"]


class TestErrorHandling:
    """Tests for error handling in emission."""

    @responses.activate
    def test_http_error_returns_false(self):
        """Test HTTP errors return False without raising."""
        responses.add(
            responses.POST,
            "http://marquez:5000/api/v1/lineage",
            json={"error": "bad request"},
            status=400,
        )

        config = OpenLineageConfig(url="http://marquez:5000")
        emitter = OpenLineageEmitter(config)

        with pytest.warns(UserWarning, match="HTTP 400"):
            result = emitter.emit_start(run_id="run-123", job_name="test-job")

        assert result is False

    @responses.activate
    def test_timeout_returns_false(self):
        """Test timeout returns False without raising."""
        responses.add(
            responses.POST,
            "http://marquez:5000/api/v1/lineage",
            body=responses.ConnectionError("timeout"),
        )

        config = OpenLineageConfig(url="http://marquez:5000", timeout_seconds=1)
        emitter = OpenLineageEmitter(config)

        with pytest.warns(UserWarning, match="not reachable"):
            result = emitter.emit_start(run_id="run-123", job_name="test-job")

        assert result is False

    @responses.activate
    def test_connection_error_returns_false(self):
        """Test connection error returns False without raising."""
        responses.add(
            responses.POST,
            "http://marquez:5000/api/v1/lineage",
            body=responses.ConnectionError("Connection refused"),
        )

        config = OpenLineageConfig(url="http://marquez:5000")
        emitter = OpenLineageEmitter(config)

        with pytest.warns(UserWarning, match="not reachable"):
            result = emitter.emit_start(run_id="run-123", job_name="test-job")

        assert result is False


class TestRunIdGeneration:
    """Tests for run ID generation."""

    def test_generate_run_id_is_uuid(self):
        """Test generated run ID is a valid UUID."""
        import uuid

        run_id = generate_run_id()

        # Should not raise
        uuid.UUID(run_id)

    def test_generate_run_id_is_unique(self):
        """Test generated run IDs are unique."""
        ids = [generate_run_id() for _ in range(100)]

        assert len(set(ids)) == 100


class TestEventStructure:
    """Tests for correct event structure."""

    @responses.activate
    def test_run_event_has_required_fields(self):
        """Test RunEvent has all required OpenLineage fields."""
        responses.add(
            responses.POST,
            "http://marquez:5000/api/v1/lineage",
            json={"status": "ok"},
            status=200,
        )

        config = OpenLineageConfig(url="http://marquez:5000", namespace="test-ns")
        emitter = OpenLineageEmitter(config)
        emitter.emit_start(run_id="run-123", job_name="test-job")

        sent_event = json.loads(responses.calls[0].request.body)

        # Required fields per OpenLineage spec
        assert "eventType" in sent_event
        assert "eventTime" in sent_event
        assert "producer" in sent_event
        assert "schemaURL" in sent_event
        assert "job" in sent_event
        assert "run" in sent_event

        assert "namespace" in sent_event["job"]
        assert "name" in sent_event["job"]
        assert "runId" in sent_event["run"]

    @responses.activate
    def test_dataset_event_has_required_fields(self):
        """Test DatasetEvent has all required OpenLineage fields."""
        responses.add(
            responses.POST,
            "http://marquez:5000/api/v1/lineage",
            json={"status": "ok"},
            status=200,
        )

        config = OpenLineageConfig(url="http://marquez:5000")
        emitter = OpenLineageEmitter(config)
        emitter.emit_create_dataset(name="test", namespace="s3")

        sent_event = json.loads(responses.calls[0].request.body)

        # Required fields per OpenLineage spec
        assert "eventType" in sent_event
        assert "eventTime" in sent_event
        assert "producer" in sent_event
        assert "schemaURL" in sent_event
        assert "dataset" in sent_event

        assert "namespace" in sent_event["dataset"]
        assert "name" in sent_event["dataset"]
