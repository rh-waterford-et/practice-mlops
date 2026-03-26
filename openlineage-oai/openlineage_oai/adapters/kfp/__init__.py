"""
KFP adapter for OpenLineage.

Provides automatic OpenLineage event emission for Kubeflow Pipelines v2
components via a context manager that is imported inside the component body.

Usage inside a @dsl.component::

    from openlineage_oai.adapters.kfp import kfp_lineage

    with kfp_lineage("my_step", inputs=[ds_in], outputs=[ds_out], url="http://marquez"):
        # ... component work ...
"""

from openlineage_oai.adapters.kfp.adapter import KFPAdapter
from openlineage_oai.adapters.kfp.lineage import kfp_lineage, kfp_output_with_schema

__all__ = ["KFPAdapter", "kfp_lineage", "kfp_output_with_schema"]
