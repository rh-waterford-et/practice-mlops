"""Core OpenLineage config and emitter (used by KFP + MLflow adapters)."""

from openlineage_oai.core.config import OpenLineageConfig
from openlineage_oai.core.emitter import OpenLineageEmitter, generate_run_id

__all__ = [
    "OpenLineageConfig",
    "OpenLineageEmitter",
    "generate_run_id",
]
