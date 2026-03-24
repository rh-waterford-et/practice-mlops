"""Unit tests for configuration module."""

import pytest

from openlineage_oai.core.config import OpenLineageConfig


class TestOpenLineageConfig:
    """Tests for OpenLineageConfig class."""

    def test_default_values(self):
        """Test that defaults are applied correctly."""
        config = OpenLineageConfig()

        assert config.url is None
        assert config.namespace == "default"
        assert config.api_key is None
        assert config.timeout_seconds == 5
        assert config.registry_url is None
        assert config.emit_on_start is True
        assert config.emit_on_end is True
        assert config.enabled is True

    def test_explicit_values(self):
        """Test explicit value assignment."""
        config = OpenLineageConfig(
            url="http://marquez:5000",
            namespace="test-ns",
            api_key="secret",
            timeout_seconds=10,
            registry_url="http://registry:8080",
            emit_on_start=False,
            emit_on_end=True,
            enabled=True,
        )

        assert config.url == "http://marquez:5000"
        assert config.namespace == "test-ns"
        assert config.api_key == "secret"
        assert config.timeout_seconds == 10
        assert config.registry_url == "http://registry:8080"
        assert config.emit_on_start is False

    def test_invalid_timeout_raises(self):
        """Test that invalid timeout raises ValueError."""
        with pytest.raises(ValueError, match="timeout_seconds must be positive"):
            OpenLineageConfig(timeout_seconds=0)

        with pytest.raises(ValueError, match="timeout_seconds must be positive"):
            OpenLineageConfig(timeout_seconds=-1)

    def test_from_env(self, monkeypatch):
        """Test configuration from environment variables."""
        monkeypatch.setenv("OPENLINEAGE_URL", "http://env-marquez:5000")
        monkeypatch.setenv("OPENLINEAGE_NAMESPACE", "env-namespace")
        monkeypatch.setenv("OPENLINEAGE_API_KEY", "env-key")
        monkeypatch.setenv("OPENLINEAGE_TIMEOUT", "15")
        monkeypatch.setenv("OPENLINEAGE_REGISTRY_URL", "http://registry:8080")
        monkeypatch.setenv("OPENLINEAGE_EMIT_ON_START", "false")
        monkeypatch.setenv("OPENLINEAGE_EMIT_ON_END", "true")
        monkeypatch.setenv("OPENLINEAGE_ENABLED", "true")

        config = OpenLineageConfig.from_env()

        assert config.url == "http://env-marquez:5000"
        assert config.namespace == "env-namespace"
        assert config.api_key == "env-key"
        assert config.timeout_seconds == 15
        assert config.registry_url == "http://registry:8080"
        assert config.emit_on_start is False
        assert config.emit_on_end is True
        assert config.enabled is True

    def test_from_env_defaults(self, monkeypatch):
        """Test that from_env uses defaults for missing variables."""
        # Clear all relevant env vars
        for var in [
            "OPENLINEAGE_URL",
            "OPENLINEAGE_NAMESPACE",
            "OPENLINEAGE_API_KEY",
            "OPENLINEAGE_TIMEOUT",
            "OPENLINEAGE_REGISTRY_URL",
        ]:
            monkeypatch.delenv(var, raising=False)

        config = OpenLineageConfig.from_env()

        assert config.url is None
        assert config.namespace == "default"
        assert config.timeout_seconds == 5

    def test_from_env_invalid_timeout_uses_default(self, monkeypatch):
        """Test that invalid timeout in env falls back to default."""
        monkeypatch.setenv("OPENLINEAGE_TIMEOUT", "not-a-number")

        config = OpenLineageConfig.from_env()

        assert config.timeout_seconds == 5  # Default

    def test_lineage_url_basic(self):
        """Test lineage_url property with basic URL."""
        config = OpenLineageConfig(url="http://marquez:5000")
        assert config.lineage_url == "http://marquez:5000/api/v1/lineage"

    def test_lineage_url_with_trailing_slash(self):
        """Test lineage_url handles trailing slash."""
        config = OpenLineageConfig(url="http://marquez:5000/")
        assert config.lineage_url == "http://marquez:5000/api/v1/lineage"

    def test_lineage_url_already_has_lineage(self):
        """Test lineage_url when URL already ends with /lineage."""
        config = OpenLineageConfig(url="http://marquez:5000/api/v1/lineage")
        assert config.lineage_url == "http://marquez:5000/api/v1/lineage"

    def test_lineage_url_has_api_v1(self):
        """Test lineage_url when URL ends with /api/v1."""
        config = OpenLineageConfig(url="http://marquez:5000/api/v1")
        assert config.lineage_url == "http://marquez:5000/api/v1/lineage"

    def test_lineage_url_empty_when_no_url(self):
        """Test lineage_url returns empty string when no URL configured."""
        config = OpenLineageConfig()
        assert config.lineage_url == ""

    def test_is_valid_with_url_and_enabled(self):
        """Test is_valid returns True when configured properly."""
        config = OpenLineageConfig(url="http://marquez:5000", enabled=True)
        assert config.is_valid() is True

    def test_is_valid_without_url(self):
        """Test is_valid returns False without URL."""
        config = OpenLineageConfig(enabled=True)
        assert config.is_valid() is False

    def test_is_valid_when_disabled(self):
        """Test is_valid returns False when disabled."""
        config = OpenLineageConfig(url="http://marquez:5000", enabled=False)
        assert config.is_valid() is False

    def test_merge_with_overrides(self):
        """Test merge_with creates new config with overrides."""
        original = OpenLineageConfig(
            url="http://original:5000",
            namespace="original-ns",
            timeout_seconds=5,
        )

        merged = original.merge_with(
            namespace="new-ns",
            timeout_seconds=10,
        )

        # Original unchanged
        assert original.namespace == "original-ns"
        assert original.timeout_seconds == 5

        # Merged has overrides
        assert merged.url == "http://original:5000"  # Kept from original
        assert merged.namespace == "new-ns"  # Overridden
        assert merged.timeout_seconds == 10  # Overridden

    def test_merge_with_none_values_ignored(self):
        """Test that None values in merge_with are ignored."""
        original = OpenLineageConfig(
            url="http://original:5000",
            namespace="original-ns",
        )

        merged = original.merge_with(
            namespace=None,  # Should be ignored
            timeout_seconds=10,
        )

        assert merged.namespace == "original-ns"  # Kept original
        assert merged.timeout_seconds == 10  # Applied override


class TestBoolParsing:
    """Tests for boolean parsing in environment variables."""

    @pytest.mark.parametrize(
        "value,expected",
        [
            ("true", True),
            ("True", True),
            ("TRUE", True),
            ("1", True),
            ("yes", True),
            ("on", True),
            ("false", False),
            ("False", False),
            ("0", False),
            ("no", False),
            ("off", False),
            ("anything-else", False),
        ],
    )
    def test_emit_on_start_parsing(self, monkeypatch, value, expected):
        """Test various boolean string representations."""
        monkeypatch.setenv("OPENLINEAGE_EMIT_ON_START", value)
        monkeypatch.delenv("OPENLINEAGE_URL", raising=False)

        config = OpenLineageConfig.from_env()

        assert config.emit_on_start is expected
