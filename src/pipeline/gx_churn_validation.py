"""
Great Expectations + OpenLineage validation for the churn KFP path.

Used by ``kfp_pipeline.ds_data_validation``; local ``run_pipeline`` uses the
lighter ``components.data_validation`` instead.
"""

from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from typing import Any
from urllib.request import Request, urlopen
from uuid import uuid4

import numpy as np
import pandas as pd
import great_expectations as gx


def validate_churn_dataset_with_gx(
    df: pd.DataFrame,
    openlineage_url: str,
    openlineage_namespace: str,
) -> pd.DataFrame:
    """
    Run GX expectations, emit OpenLineage START/COMPLETE, then fill nulls /
    flag constant columns (same remediation as the historical KFP component).
    """
    ol_ns = openlineage_namespace or os.environ.get("OPENLINEAGE_NAMESPACE", "default")

    print(f"Loaded {len(df)} rows for validation")

    context = gx.get_context()

    data_source = context.data_sources.add_pandas(name="pipeline_data")
    data_asset = data_source.add_dataframe_asset(name="customer_features")
    batch_def = data_asset.add_batch_definition_whole_dataframe(
        "validation_batch",
    )

    suite = gx.ExpectationSuite(name="customer_data_quality")

    critical_cols = [
        "entity_id", "event_timestamp", "tenure_months",
        "monthly_charges", "total_charges", "num_support_tickets", "churn",
    ]
    for col in critical_cols:
        suite.add_expectation(
            gx.expectations.ExpectColumnToExist(column=col),
        )
        suite.add_expectation(
            gx.expectations.ExpectColumnValuesToNotBeNull(
                column=col, mostly=0.95,
            ),
        )

    numeric_cols = [
        "tenure_months", "monthly_charges", "total_charges",
        "num_support_tickets",
    ]
    for col in numeric_cols:
        suite.add_expectation(
            gx.expectations.ExpectColumnValuesToBeBetween(
                column=col, min_value=0, mostly=0.99,
            ),
        )

    suite.add_expectation(
        gx.expectations.ExpectColumnDistinctValuesToBeInSet(
            column="churn", value_set=[0, 1],
        ),
    )

    suite = context.suites.add(suite)

    val_def = gx.ValidationDefinition(
        data=batch_def,
        suite=suite,
        name="customer_validation",
    )
    val_def = context.validation_definitions.add(val_def)

    checkpoint = gx.Checkpoint(
        name="customer_checkpoint",
        validation_definitions=[val_def],
        actions=[],
        result_format={"result_format": "SUMMARY"},
    )
    context.checkpoints.add(checkpoint)

    result = checkpoint.run(batch_parameters={"dataframe": df})

    evaluated = 0
    successful = 0
    unsuccessful = 0
    if result.success:
        print("All expectations passed.")
    else:
        print("WARNING: Some expectations failed. Proceeding with remediation.")

    all_vr_results: list[Any] = []
    for _vr_key, vr in result.run_results.items():
        vr_obj = vr if hasattr(vr, "statistics") else vr.get("validation_result", vr)
        stats = vr_obj.statistics
        evaluated += stats["evaluated_expectations"]
        successful += stats["successful_expectations"]
        unsuccessful += stats["unsuccessful_expectations"]
        print(
            f"  Evaluated: {stats['evaluated_expectations']}, "
            f"Successful: {stats['successful_expectations']}, "
            f"Unsuccessful: {stats['unsuccessful_expectations']}"
        )
        all_vr_results.append(vr_obj)

    run_id = str(uuid4())
    now = datetime.now(timezone.utc).isoformat()

    assertions: list[dict[str, Any]] = []
    for vr_obj in all_vr_results:
        for exp_result in vr_obj.results:
            exp = exp_result.expectation_config
            assertions.append({
                "assertion": exp.type,
                "success": exp_result.success,
                "column": getattr(exp, "column", "") or exp.kwargs.get("column", ""),
            })

    dq_facet = {
        "dataQualityAssertions": {
            "_producer": "https://greatexpectations.io",
            "_schemaURL": "https://openlineage.io/spec/facets/1-0-0/DataQualityAssertionsDatasetFacet.json",
            "assertions": assertions,
        }
    }

    for event_type in ("START", "COMPLETE"):
        event = {
            "eventType": event_type,
            "eventTime": now,
            "run": {"runId": run_id, "facets": {}},
            "job": {
                "namespace": ol_ns,
                "name": "validate_customer_data",
                "facets": {},
            },
            "inputs": [
                {
                    "namespace": ol_ns,
                    "name": "customer_features_view",
                    "facets": dq_facet if event_type == "COMPLETE" else {},
                }
            ],
            "outputs": [
                {
                    "namespace": ol_ns,
                    "name": "customer_features_validated",
                    "facets": {},
                }
            ],
            "producer": "https://greatexpectations.io",
            "schemaURL": "https://openlineage.io/spec/2-0-2/OpenLineage.json#/$defs/RunEvent",
        }
        body = json.dumps(event).encode("utf-8")
        req = Request(
            f"{openlineage_url}/api/v1/lineage",
            data=body,
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        try:
            with urlopen(req) as resp:
                print(f"OL event {event_type}: HTTP {resp.status}")
        except Exception as e:
            print(f"OL event {event_type} failed: {e}")

    for col in critical_cols:
        nulls = df[col].isna().sum()
        if nulls > 0:
            print(f"  Filling {nulls} nulls in {col}")
            df[col] = df[col].fillna(0)

    const_cols = df.select_dtypes(include=[np.number]).columns
    for col in const_cols:
        if df[col].nunique() <= 1:
            print(f"  WARNING: {col} is constant")

    print(
        f"DS: validation complete - {len(df)} rows, "
        f"{evaluated} checks ({successful} passed, {unsuccessful} failed)"
    )
    return df
