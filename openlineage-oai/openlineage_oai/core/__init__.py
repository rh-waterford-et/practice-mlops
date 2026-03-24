"""
Core OpenLineage components.

This module contains the foundational components used by all adapters:
- config: Configuration management
- emitter: OpenLineage event emission
- facets: Standard and custom facets
- registry: Dataset registry client
"""

from openlineage_oai.core.config import OpenLineageConfig
from openlineage_oai.core.emitter import OpenLineageEmitter, generate_run_id
from openlineage_oai.core.registry import RegistryClient, ResolvedDataset, resolve_with_fallback

__all__ = [
    "OpenLineageConfig",
    "OpenLineageEmitter",
    "generate_run_id",
    "RegistryClient",
    "ResolvedDataset",
    "resolve_with_fallback",
]
