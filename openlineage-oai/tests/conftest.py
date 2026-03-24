"""
Pytest configuration and shared fixtures.

This module provides fixtures used across unit and integration tests.
"""

import pytest

# ============================================================================
# Configuration Fixtures
# ============================================================================


@pytest.fixture
def sample_config():
    """Sample OpenLineage configuration for testing."""
    return {
        "url": "http://localhost:5000",
        "namespace": "test-namespace",
        "api_key": None,
        "timeout_seconds": 5,
        "emit_on_start": True,
        "emit_on_end": True,
    }


@pytest.fixture
def env_config(monkeypatch):
    """Set up environment variables for configuration testing."""
    monkeypatch.setenv("OPENLINEAGE_URL", "http://marquez:5000")
    monkeypatch.setenv("OPENLINEAGE_NAMESPACE", "env-namespace")
    monkeypatch.setenv("OPENLINEAGE_TIMEOUT", "10")
    return {
        "url": "http://marquez:5000",
        "namespace": "env-namespace",
        "timeout_seconds": 10,
    }


# ============================================================================
# Mock Server Fixtures
# ============================================================================


@pytest.fixture
def mock_marquez(responses):
    """
    Mock Marquez server that accepts OpenLineage events.

    Usage:
        def test_emit(mock_marquez):
            mock_marquez.add_event_endpoint()
            # ... test code that emits events ...
            assert len(mock_marquez.events) == 1
    """

    class MockMarquez:
        def __init__(self, responses):
            self.responses = responses
            self.events = []

        def add_event_endpoint(self, url="http://localhost:5000/api/v1/lineage"):
            def callback(request):
                import json

                self.events.append(json.loads(request.body))
                return (200, {}, '{"status": "ok"}')

            self.responses.add_callback(
                self.responses.POST,
                url,
                callback=callback,
                content_type="application/json",
            )

    import responses as responses_lib

    with responses_lib.RequestsMock() as rsps:
        yield MockMarquez(rsps)


# ============================================================================
# MLflow Fixtures (for integration tests)
# ============================================================================


@pytest.fixture
def temp_mlflow_dir(tmp_path):
    """Temporary directory for MLflow file-based tracking."""
    mlruns = tmp_path / "mlruns"
    mlruns.mkdir()
    return str(mlruns)


# ============================================================================
# Sample Data Fixtures
# ============================================================================


@pytest.fixture
def sample_run_event():
    """Sample OpenLineage RunEvent structure."""
    return {
        "eventType": "START",
        "eventTime": "2026-02-05T12:00:00Z",
        "producer": "openlineage-oai",
        "schemaURL": "https://openlineage.io/spec/2-0-0/OpenLineage.json#/definitions/RunEvent",
        "job": {
            "namespace": "test-namespace",
            "name": "test-job",
            "facets": {},
        },
        "run": {
            "runId": "test-run-id-123",
            "facets": {},
        },
        "inputs": [],
        "outputs": [],
    }


@pytest.fixture
def sample_dataset_event():
    """Sample OpenLineage DatasetEvent structure."""
    return {
        "eventType": "CREATE",
        "eventTime": "2026-02-05T12:00:00Z",
        "producer": "openlineage-oai",
        "schemaURL": "https://openlineage.io/spec/2-0-0/OpenLineage.json#/definitions/DatasetEvent",
        "dataset": {
            "namespace": "test-namespace",
            "name": "test-dataset",
            "facets": {},
        },
    }
