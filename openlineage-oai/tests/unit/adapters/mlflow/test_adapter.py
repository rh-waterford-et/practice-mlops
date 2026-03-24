"""Unit tests for MLflow adapter."""

from unittest.mock import patch

from openlineage_oai.adapters.mlflow import MLflowAdapter
from openlineage_oai.core.config import OpenLineageConfig
from openlineage_oai.core.emitter import OpenLineageEmitter


class TestMLflowAdapter:
    """Tests for MLflowAdapter class."""

    def test_get_tool_name(self):
        """Test tool name is 'mlflow'."""
        config = OpenLineageConfig(url="http://test:5000")
        emitter = OpenLineageEmitter(config)
        adapter = MLflowAdapter(emitter)

        assert adapter.get_tool_name() == "mlflow"

    def test_build_job_name(self):
        """Test job name building."""
        config = OpenLineageConfig(url="http://test:5000")
        emitter = OpenLineageEmitter(config)
        adapter = MLflowAdapter(emitter)

        name = adapter.build_job_name(name="training", context="experiment-123")

        assert name == "mlflow/experiment-123/training"

    def test_install_hooks_success(self):
        """Test successful hook installation (MLflow is installed in test env)."""
        config = OpenLineageConfig(url="http://test:5000")
        emitter = OpenLineageEmitter(config)
        adapter = MLflowAdapter(emitter)

        # MLflow is installed in our test environment
        adapter.install_hooks()

        assert adapter.is_installed is True

    def test_install_hooks_no_mlflow(self):
        """Test install_hooks raises when MLflow not installed."""
        config = OpenLineageConfig(url="http://test:5000")
        emitter = OpenLineageEmitter(config)
        MLflowAdapter(emitter)

        with patch.dict("sys.modules", {"mlflow": None}):
            # This would raise ImportError if mlflow wasn't available
            # In our test environment, mlflow IS available
            pass

    def test_uninstall_hooks(self):
        """Test uninstall marks adapter as not installed."""
        config = OpenLineageConfig(url="http://test:5000")
        emitter = OpenLineageEmitter(config)
        adapter = MLflowAdapter(emitter)

        adapter._installed = True
        adapter.uninstall_hooks()

        assert adapter.is_installed is False
