"""Unit tests for the registry module."""

import pytest
import responses

from openlineage_oai.core.registry import (
    RegistryClient,
    ResolvedDataset,
    resolve_with_fallback,
)


class TestResolvedDataset:
    """Tests for ResolvedDataset dataclass."""

    def test_default_values(self):
        """Test default values are set correctly."""
        result = ResolvedDataset(namespace="ns", name="name")

        assert result.namespace == "ns"
        assert result.name == "name"
        assert result.registry_id is None
        assert result.resolved is False

    def test_with_registry_id(self):
        """Test with registry ID set."""
        result = ResolvedDataset(
            namespace="s3",
            name="bucket/data",
            registry_id="dataset-123",
            resolved=True,
        )

        assert result.registry_id == "dataset-123"
        assert result.resolved is True


class TestRegistryClient:
    """Tests for RegistryClient."""

    @responses.activate
    def test_resolve_success(self):
        """Test successful dataset resolution."""
        responses.add(
            responses.GET,
            "http://registry:8080/api/v1/datasets/resolve",
            json={
                "id": "dataset-123",
                "canonical_namespace": "s3",
                "canonical_name": "bucket/iris.parquet",
            },
            status=200,
        )

        client = RegistryClient(url="http://registry:8080")
        result = client.resolve(namespace="pandas", name="iris", tool="mlflow")

        assert result.resolved is True
        assert result.namespace == "s3"
        assert result.name == "bucket/iris.parquet"
        assert result.registry_id == "dataset-123"

    @responses.activate
    def test_resolve_not_found(self):
        """Test resolution when dataset not in registry."""
        responses.add(
            responses.GET,
            "http://registry:8080/api/v1/datasets/resolve",
            json={"error": "not found"},
            status=404,
        )

        client = RegistryClient(url="http://registry:8080")
        result = client.resolve(namespace="pandas", name="iris")

        # Should fall back to original
        assert result.resolved is False
        assert result.namespace == "pandas"
        assert result.name == "iris"
        assert result.registry_id is None

    @responses.activate
    def test_resolve_timeout(self):
        """Test resolution handles timeout gracefully."""
        responses.add(
            responses.GET,
            "http://registry:8080/api/v1/datasets/resolve",
            body=responses.ConnectionError("timeout"),
        )

        client = RegistryClient(url="http://registry:8080", timeout=1)

        with pytest.warns(UserWarning, match="not reachable"):
            result = client.resolve(namespace="pandas", name="iris")

        # Should fall back to original
        assert result.resolved is False
        assert result.namespace == "pandas"
        assert result.name == "iris"

    @responses.activate
    def test_resolve_connection_error(self):
        """Test resolution handles connection error gracefully."""
        responses.add(
            responses.GET,
            "http://registry:8080/api/v1/datasets/resolve",
            body=responses.ConnectionError("refused"),
        )

        client = RegistryClient(url="http://registry:8080")

        with pytest.warns(UserWarning, match="not reachable"):
            result = client.resolve(namespace="pandas", name="iris")

        assert result.resolved is False
        assert result.namespace == "pandas"

    def test_resolve_disabled(self):
        """Test resolution when client is disabled."""
        client = RegistryClient(url="http://registry:8080", enabled=False)
        result = client.resolve(namespace="pandas", name="iris")

        # Should return original without any network call
        assert result.resolved is False
        assert result.namespace == "pandas"
        assert result.name == "iris"

    @responses.activate
    def test_register_success(self):
        """Test successful dataset registration."""
        responses.add(
            responses.POST,
            "http://registry:8080/api/v1/datasets",
            json={"id": "dataset-456"},
            status=201,
        )

        client = RegistryClient(url="http://registry:8080")
        result = client.register(
            namespace="pandas",
            name="iris",
            canonical_namespace="s3",
            canonical_name="bucket/iris.parquet",
            tool="mlflow",
        )

        assert result == "dataset-456"

    @responses.activate
    def test_register_failure(self):
        """Test registration handles failure gracefully."""
        responses.add(
            responses.POST,
            "http://registry:8080/api/v1/datasets",
            json={"error": "bad request"},
            status=400,
        )

        client = RegistryClient(url="http://registry:8080")

        with pytest.warns(UserWarning, match="Failed to register"):
            result = client.register(namespace="pandas", name="iris")

        assert result is None

    def test_register_disabled(self):
        """Test registration when client is disabled."""
        client = RegistryClient(url="http://registry:8080", enabled=False)
        result = client.register(namespace="pandas", name="iris")

        assert result is None


class TestResolveWithFallback:
    """Tests for resolve_with_fallback helper."""

    def test_with_none_registry(self):
        """Test fallback when registry is None."""
        result = resolve_with_fallback(
            registry=None,
            namespace="pandas",
            name="iris",
        )

        assert result.namespace == "pandas"
        assert result.name == "iris"
        assert result.resolved is False

    @responses.activate
    def test_with_working_registry(self):
        """Test with working registry."""
        responses.add(
            responses.GET,
            "http://registry:8080/api/v1/datasets/resolve",
            json={
                "id": "dataset-123",
                "canonical_namespace": "s3",
                "canonical_name": "bucket/data",
            },
            status=200,
        )

        client = RegistryClient(url="http://registry:8080")
        result = resolve_with_fallback(
            registry=client,
            namespace="pandas",
            name="iris",
            tool="mlflow",
        )

        assert result.resolved is True
        assert result.namespace == "s3"
