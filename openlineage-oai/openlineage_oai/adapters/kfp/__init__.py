"""
KFP adapter — context manager used inside ``@dsl.component`` bodies.

``KFPAdapter`` (optional ``openlineage_oai.init(tools=[...])`` hook) was removed;
this package keeps the lineage helpers used by the churn KFP pipeline.
"""

from openlineage_oai.adapters.kfp.lineage import kfp_lineage, kfp_output_with_schema

__all__ = ["kfp_lineage", "kfp_output_with_schema"]
