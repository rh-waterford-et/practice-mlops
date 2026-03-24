"""Unit tests for the base adapter interface."""

import pytest

from openlineage_oai.adapters.base import ToolAdapter
from openlineage_oai.core.config import OpenLineageConfig
from openlineage_oai.core.emitter import OpenLineageEmitter


class ConcreteAdapter(ToolAdapter):
    """Concrete implementation for testing."""

    def get_tool_name(self) -> str:
        return "test-tool"

    def install_hooks(self) -> None:
        self._installed = True

    def uninstall_hooks(self) -> None:
        self._installed = False


class TestToolAdapterInterface:
    """Tests for ToolAdapter abstract interface."""

    def test_cannot_instantiate_abstract_class(self):
        """Test that ToolAdapter cannot be instantiated directly."""
        config = OpenLineageConfig(url="http://test:5000")
        emitter = OpenLineageEmitter(config)

        with pytest.raises(TypeError, match="abstract"):
            ToolAdapter(emitter)

    def test_concrete_adapter_can_be_instantiated(self):
        """Test that concrete implementation can be instantiated."""
        config = OpenLineageConfig(url="http://test:5000")
        emitter = OpenLineageEmitter(config)

        adapter = ConcreteAdapter(emitter, namespace="test-ns")

        assert adapter.emitter is emitter
        assert adapter.namespace == "test-ns"

    def test_get_tool_name(self):
        """Test get_tool_name returns correct value."""
        config = OpenLineageConfig(url="http://test:5000")
        emitter = OpenLineageEmitter(config)
        adapter = ConcreteAdapter(emitter)

        assert adapter.get_tool_name() == "test-tool"

    def test_install_uninstall_hooks(self):
        """Test install and uninstall hooks."""
        config = OpenLineageConfig(url="http://test:5000")
        emitter = OpenLineageEmitter(config)
        adapter = ConcreteAdapter(emitter)

        assert adapter.is_installed is False

        adapter.install_hooks()
        assert adapter.is_installed is True

        adapter.uninstall_hooks()
        assert adapter.is_installed is False

    def test_build_job_name_without_context(self):
        """Test job name building without context."""
        config = OpenLineageConfig(url="http://test:5000")
        emitter = OpenLineageEmitter(config)
        adapter = ConcreteAdapter(emitter)

        name = adapter.build_job_name(name="my-job")

        assert name == "test-tool/my-job"

    def test_build_job_name_with_context(self):
        """Test job name building with context."""
        config = OpenLineageConfig(url="http://test:5000")
        emitter = OpenLineageEmitter(config)
        adapter = ConcreteAdapter(emitter)

        name = adapter.build_job_name(name="my-job", context="experiment-123")

        assert name == "test-tool/experiment-123/my-job"

    def test_repr(self):
        """Test string representation."""
        config = OpenLineageConfig(url="http://test:5000")
        emitter = OpenLineageEmitter(config)
        adapter = ConcreteAdapter(emitter, namespace="my-ns")

        repr_str = repr(adapter)

        assert "ConcreteAdapter" in repr_str
        assert "test-tool" in repr_str
        assert "my-ns" in repr_str

    def test_default_namespace(self):
        """Test default namespace is 'default'."""
        config = OpenLineageConfig(url="http://test:5000")
        emitter = OpenLineageEmitter(config)
        adapter = ConcreteAdapter(emitter)

        assert adapter.namespace == "default"
