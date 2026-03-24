"""Unit tests for package initialization."""

import pytest

import openlineage_oai


class TestInit:
    """Tests for openlineage_oai.init()."""

    def teardown_method(self):
        """Clean up after each test."""
        openlineage_oai._emitter = None
        openlineage_oai._adapters = {}
        openlineage_oai._initialized = False

    def test_init_with_explicit_url(self, capsys):
        """Test initialization with explicit URL."""
        openlineage_oai.init(
            url="http://marquez:5000",
            namespace="test-ns",
            tools=["mlflow"],
        )

        assert openlineage_oai.is_initialized() is True
        assert openlineage_oai.get_emitter() is not None
        assert openlineage_oai.get_emitter().config.url == "http://marquez:5000"
        assert openlineage_oai.get_emitter().config.namespace == "test-ns"

        captured = capsys.readouterr()
        assert "✓ OpenLineage enabled for mlflow" in captured.out

    def test_init_with_env_vars(self, monkeypatch, capsys):
        """Test initialization from environment variables."""
        monkeypatch.setenv("OPENLINEAGE_URL", "http://env-marquez:5000")
        monkeypatch.setenv("OPENLINEAGE_NAMESPACE", "env-ns")

        openlineage_oai.init(tools=["mlflow"])

        assert openlineage_oai.get_emitter().config.url == "http://env-marquez:5000"

    def test_init_silent_mode(self, capsys):
        """Test silent initialization."""
        openlineage_oai.init(
            url="http://marquez:5000",
            tools=["mlflow"],
            silent=True,
        )

        captured = capsys.readouterr()
        assert captured.out == ""

    def test_init_warns_without_url(self, monkeypatch):
        """Test warning when no URL configured."""
        monkeypatch.delenv("OPENLINEAGE_URL", raising=False)

        with pytest.warns(UserWarning, match="URL not configured"):
            openlineage_oai.init(tools=["mlflow"], silent=True)

    def test_init_unknown_tool(self, capsys):
        """Test initialization with unknown tool."""
        openlineage_oai.init(
            url="http://marquez:5000",
            tools=["unknown-tool"],
        )

        captured = capsys.readouterr()
        assert "Failed to initialize unknown-tool" in captured.out

    def test_get_adapter(self):
        """Test getting a specific adapter."""
        openlineage_oai.init(
            url="http://marquez:5000",
            tools=["mlflow"],
            silent=True,
        )

        adapter = openlineage_oai.get_adapter("mlflow")
        assert adapter is not None
        assert adapter.get_tool_name() == "mlflow"

        # Non-existent adapter returns None
        assert openlineage_oai.get_adapter("ray") is None


class TestShutdown:
    """Tests for openlineage_oai.shutdown()."""

    def teardown_method(self):
        """Clean up after each test."""
        openlineage_oai._emitter = None
        openlineage_oai._adapters = {}
        openlineage_oai._initialized = False

    def test_shutdown(self, capsys):
        """Test shutdown cleans up adapters."""
        openlineage_oai.init(
            url="http://marquez:5000",
            tools=["mlflow"],
            silent=True,
        )

        assert openlineage_oai.is_initialized() is True

        openlineage_oai.shutdown()

        assert openlineage_oai.is_initialized() is False
        assert openlineage_oai._adapters == {}

        captured = capsys.readouterr()
        assert "✓ OpenLineage disabled for mlflow" in captured.out


class TestIsInitialized:
    """Tests for openlineage_oai.is_initialized()."""

    def teardown_method(self):
        """Clean up after each test."""
        openlineage_oai._emitter = None
        openlineage_oai._adapters = {}
        openlineage_oai._initialized = False

    def test_not_initialized_by_default(self):
        """Test is_initialized returns False by default."""
        assert openlineage_oai.is_initialized() is False

    def test_initialized_after_init(self):
        """Test is_initialized returns True after init."""
        openlineage_oai.init(
            url="http://marquez:5000",
            tools=["mlflow"],
            silent=True,
        )

        assert openlineage_oai.is_initialized() is True


class TestAvailableTools:
    """Tests for AVAILABLE_TOOLS constant."""

    def test_mlflow_available(self):
        """Test MLflow is in available tools."""
        assert "mlflow" in openlineage_oai.AVAILABLE_TOOLS
