"""
STAGE 3 – Kubeflow Pipelines v2 DSL with OpenLineage tracking.

This module is a *transparent instrumentation layer* over ``kfp_pipeline.py``.
The original pipeline file is **never modified**; this module re-defines each
component with an identical signature and logic while wrapping every step in
OpenLineage ``KFPLineageRun`` / ``kfp_lineage_run`` context managers provided
by :mod:`src.pipeline.kfp_lineage`.

How it works
------------
Each component function below imports :mod:`src.pipeline.kfp_lineage` at
*runtime* (i.e. inside the running KFP pod).  Because ``FKM_IMAGE`` sets
``PYTHONPATH=/app``, the import resolves cleanly without any path manipulation.

Steps 2, 3 delegate their business logic to :mod:`src.pipeline.components`
(the plain-Python versions) to avoid code duplication.  Steps 1, 4, 6 keep
their own self-contained logic because they require KFP-specific I/O handling
or environment setup not present in the components module.

OpenLineage events are emitted only when ``OPENLINEAGE_URL`` is set in the pod
environment; otherwise every ``kfp_lineage_run`` / ``KFPLineageRun`` call is a
complete no-op, so this pipeline is safe to run without a Marquez backend.

Compile::

    python -m src.pipeline.instrumented_pipeline

The resulting ``customer_churn_pipeline_with_lineage.yaml`` can be uploaded to
OpenShift AI Pipelines instead of (or alongside) the original YAML.
"""

from kfp import dsl, compiler

FKM_IMAGE = (
    "image-registry.openshift-image-registry.svc:5000/fkm-test/fkm-app:latest"
)


# ═══════════════════════════════════════════════════════════════════════
# STEP 1 — Data Extraction via Feast  (+ OpenLineage)
# ═══════════════════════════════════════════════════════════════════════
@dsl.component(base_image=FKM_IMAGE)
def step1_data_extraction(
    pg_url: str,
    feast_repo_path: str,
    table_name: str,
    pg_host: str,
    redis_host: str,
    output_path: dsl.Output[dsl.Dataset],
) -> None:
    """Retrieve historical features from Feast and save as parquet.

    Emits OpenLineage START / COMPLETE / FAIL events via kfp_lineage.
    Inputs:  PostgreSQL ``table_name`` table (offline Feast store).
    Outputs: ``extracted_features`` parquet artifact.
    """
    import os
    import pandas as pd
    from sqlalchemy import create_engine, text
    from feast import FeatureStore
    from src.pipeline.kfp_lineage import (  # noqa: PLC0415
        kfp_lineage_run,
        parquet_output,
        pg_input,
    )

    # Patch feature_store.yaml so Feast points at in-cluster services
    fs_yaml = os.path.join(feast_repo_path, "feature_store.yaml")
    with open(fs_yaml, "w") as f:
        f.write(f"""\
project: customer_churn
provider: local
registry:
  registry_type: sql
  path: postgresql://feast:feast@{pg_host}:5432/warehouse
  cache_ttl_seconds: 60
offline_store:
  type: postgres
  host: {pg_host}
  port: 5432
  database: warehouse
  db_schema: public
  user: feast
  password: feast
online_store:
  type: redis
  connection_string: {redis_host}:6379
entity_key_serialization_version: 2
""")
    print(f"Patched {fs_yaml} -> pg={pg_host}, redis={redis_host}")

    with kfp_lineage_run(
        "kfp.step1_data_extraction",
        inputs=[pg_input(table_name)],
        outputs=[parquet_output("extracted_features")],
    ):
        engine = create_engine(pg_url)
        with engine.connect() as conn:
            entity_df = pd.read_sql(
                text(f"SELECT entity_id, event_timestamp, churn FROM {table_name}"),
                conn,
            )
        engine.dispose()
        entity_df["event_timestamp"] = pd.to_datetime(
            entity_df["event_timestamp"], utc=True,
        )

        store = FeatureStore(repo_path=feast_repo_path)
        features = [
            "customer_features_view:tenure_months",
            "customer_features_view:monthly_charges",
            "customer_features_view:total_charges",
            "customer_features_view:num_support_tickets",
            "customer_features_view:contract_type",
            "customer_features_view:internet_service",
            "customer_features_view:payment_method",
        ]
        features_df = store.get_historical_features(
            entity_df=entity_df[["entity_id", "event_timestamp"]],
            features=features,
        ).to_df()

        for df_ in (features_df, entity_df):
            df_["event_timestamp"] = pd.to_datetime(
                df_["event_timestamp"], utc=True,
            )

        result = features_df.merge(
            entity_df[["entity_id", "event_timestamp", "churn"]],
            on=["entity_id", "event_timestamp"],
            how="left",
        )
        print(f"Step 1 complete - shape {result.shape}")
        result.to_parquet(output_path.path)


# ═══════════════════════════════════════════════════════════════════════
# STEP 2 — Data Validation  (+ OpenLineage)
# ═══════════════════════════════════════════════════════════════════════
@dsl.component(base_image=FKM_IMAGE)
def step2_data_validation(
    dataset: dsl.Input[dsl.Dataset],
    output_path: dsl.Output[dsl.Dataset],
) -> None:
    """Validate schema, nulls, and distributions.

    Emits OpenLineage START / COMPLETE / FAIL events via kfp_lineage.
    Inputs:  ``extracted_features`` parquet artifact.
    Outputs: ``validated_features`` parquet artifact.
    """
    import pandas as pd
    from src.pipeline.kfp_lineage import (  # noqa: PLC0415
        kfp_lineage_run,
        parquet_input,
        parquet_output,
    )
    from src.pipeline.components import data_validation  # noqa: PLC0415

    with kfp_lineage_run(
        "kfp.step2_data_validation",
        inputs=[parquet_input("extracted_features")],
        outputs=[parquet_output("validated_features")],
    ):
        df = pd.read_parquet(dataset.path)
        df = data_validation(df)
        print(f"Step 2 complete – {len(df)} rows validated")
        df.to_parquet(output_path.path)


# ═══════════════════════════════════════════════════════════════════════
# STEP 3 — Feature Engineering  (+ OpenLineage)
# ═══════════════════════════════════════════════════════════════════════
@dsl.component(base_image=FKM_IMAGE)
def step3_feature_engineering(
    dataset: dsl.Input[dsl.Dataset],
    output_path: dsl.Output[dsl.Dataset],
) -> None:
    """Add derived features (charges_per_month, ticket_rate).

    Emits OpenLineage START / COMPLETE / FAIL events via kfp_lineage.
    Inputs:  ``validated_features`` parquet artifact.
    Outputs: ``engineered_features`` parquet artifact.
    """
    import pandas as pd
    from src.pipeline.kfp_lineage import (  # noqa: PLC0415
        kfp_lineage_run,
        parquet_input,
        parquet_output,
    )
    from src.pipeline.components import feature_engineering  # noqa: PLC0415

    with kfp_lineage_run(
        "kfp.step3_feature_engineering",
        inputs=[parquet_input("validated_features")],
        outputs=[parquet_output("engineered_features")],
    ):
        df = pd.read_parquet(dataset.path)
        df = feature_engineering(df)
        print(f"Step 3 complete – shape {df.shape}")
        df.to_parquet(output_path.path)


# ═══════════════════════════════════════════════════════════════════════
# STEP 4 — Model Training + MLflow  (+ OpenLineage)
# ═══════════════════════════════════════════════════════════════════════
@dsl.component(base_image=FKM_IMAGE)
def step4_model_training(
    dataset: dsl.Input[dsl.Dataset],
    tracking_uri: str,
    experiment_name: str,
    s3_endpoint: str,
    aws_key: str,
    aws_secret: str,
) -> str:
    """Train XGBoost, log to MLflow. Returns JSON with run_id, model_uri, metrics.

    Emits OpenLineage START / COMPLETE / FAIL events via kfp_lineage.
    The MLflow run_id is attached as a dynamic output dataset after training
    completes (using KFPLineageRun.add_output).
    Inputs:  ``engineered_features`` parquet artifact.
    Outputs: MLflow run (experiment/runs/<run_id>).
    """
    import json
    import os
    import pandas as pd
    from src.pipeline.kfp_lineage import (  # noqa: PLC0415
        KFPLineageRun,
        mlflow_run_output,
        parquet_input,
    )
    from src.training.trainer import train_and_log  # noqa: PLC0415

    os.environ["MLFLOW_S3_ENDPOINT_URL"] = s3_endpoint
    os.environ["AWS_ACCESS_KEY_ID"] = aws_key
    os.environ["AWS_SECRET_ACCESS_KEY"] = aws_secret

    df = pd.read_parquet(dataset.path)

    with KFPLineageRun(
        "kfp.step4_model_training",
        inputs=[parquet_input("engineered_features")],
    ) as ol_run:
        result = train_and_log(
            df=df,
            tracking_uri=tracking_uri,
            experiment_name=experiment_name,
        )
        # Attach the MLflow run as a dynamic output dataset now that run_id is known
        ol_run.add_output(mlflow_run_output(result["run_id"], experiment_name))

        print(f"Step 4 complete – metrics: {result['metrics']}")
        return json.dumps({
            "run_id": result["run_id"],
            "model_uri": result["model_uri"],
            "metrics": result["metrics"],
        })


# ═══════════════════════════════════════════════════════════════════════
# STEP 5 — Evaluation  (+ OpenLineage)
# ═══════════════════════════════════════════════════════════════════════
@dsl.component(base_image=FKM_IMAGE)
def step5_evaluation(train_result_json: str) -> str:
    """Display and return evaluation metrics.

    Emits OpenLineage START / COMPLETE / FAIL events via kfp_lineage.
    """
    import json
    from src.pipeline.kfp_lineage import kfp_lineage_run  # noqa: PLC0415

    with kfp_lineage_run("kfp.step5_evaluation"):
        result = json.loads(train_result_json)
        m = result["metrics"]
        print(
            f"Step 5  ROC-AUC={m['roc_auc']:.4f}  F1={m['f1']:.4f}  "
            f"Precision={m['precision']:.4f}  Recall={m['recall']:.4f}"
        )
        return json.dumps(m)


# ═══════════════════════════════════════════════════════════════════════
# STEP 6 — Model Registration  (+ OpenLineage)
# ═══════════════════════════════════════════════════════════════════════
@dsl.component(base_image=FKM_IMAGE)
def step6_model_registration(
    train_result_json: str,
    metrics_json: str,
    model_name: str,
    tracking_uri: str,
    s3_endpoint: str,
    aws_key: str,
    aws_secret: str,
    roc_auc_threshold: float,
) -> str:
    """Register model in MLflow Registry if metrics pass the threshold.

    Emits OpenLineage START / COMPLETE / FAIL events via kfp_lineage.
    The registered model version is attached as a dynamic output dataset
    (using KFPLineageRun.add_output) after the version number is known.
    Inputs:  MLflow run (from step 4).
    Outputs: MLflow registered model (models:/<model_name>/<version>).
    """
    import json
    import os
    from src.pipeline.kfp_lineage import (  # noqa: PLC0415
        KFPLineageRun,
        mlflow_model_output,
        mlflow_run_input,
    )
    from src.training.registry import promote_to_alias, register_model  # noqa: PLC0415

    os.environ["MLFLOW_S3_ENDPOINT_URL"] = s3_endpoint
    os.environ["AWS_ACCESS_KEY_ID"] = aws_key
    os.environ["AWS_SECRET_ACCESS_KEY"] = aws_secret

    result = json.loads(train_result_json)
    metrics = json.loads(metrics_json)

    with KFPLineageRun(
        "kfp.step6_model_registration",
        inputs=[mlflow_run_input(result["run_id"], "customer_churn")],
    ) as ol_run:
        if metrics["roc_auc"] < roc_auc_threshold:
            print(
                f"Step 6  ROC-AUC {metrics['roc_auc']:.4f} < {roc_auc_threshold}"
                " – skipping registration"
            )
            return json.dumps({"registered": False, "reason": "below_threshold"})

        version = register_model(
            model_uri=result["model_uri"],
            model_name=model_name,
            tracking_uri=tracking_uri,
        )
        promote_to_alias(
            model_name=model_name,
            version=version,
            alias="champion",
            tracking_uri=tracking_uri,
        )
        ol_run.add_output(mlflow_model_output(model_name, str(version)))

        print(f"Step 6  Registered {model_name} v{version} as alias 'champion'")
        return json.dumps({
            "registered": True,
            "model_name": model_name,
            "version": int(version),
            "alias": "champion",
        })


# ═══════════════════════════════════════════════════════════════════════
# Pipeline definition
# ═══════════════════════════════════════════════════════════════════════
@dsl.pipeline(
    name="Customer Churn ML Pipeline (with OpenLineage)",
    description=(
        "End-to-end: Feast extraction, Validation, Feature engineering, "
        "XGBoost training, Evaluation, MLflow registration. "
        "Each step emits OpenLineage lineage events when OPENLINEAGE_URL is set."
    ),
)
def customer_churn_pipeline_with_lineage(
    pg_url: str = "postgresql://feast:feast@postgres.fkm-test.svc.cluster.local:5432/warehouse",
    feast_repo_path: str = "/app/src/feature_store",
    table_name: str = "customer_features",
    pg_host: str = "postgres.fkm-test.svc.cluster.local",
    redis_host: str = "redis.fkm-test.svc.cluster.local",
    tracking_uri: str = "http://mlflow.fkm-test.svc.cluster.local:5000",
    experiment_name: str = "customer_churn",
    model_name: str = "customer_churn_model",
    s3_endpoint: str = "http://minio.fkm-test.svc.cluster.local:9000",
    aws_key: str = "minioadmin",
    aws_secret: str = "minioadmin",
    roc_auc_threshold: float = 0.70,
):
    # STEP 1 – Extract features from Feast
    extract_task = step1_data_extraction(
        pg_url=pg_url,
        feast_repo_path=feast_repo_path,
        table_name=table_name,
        pg_host=pg_host,
        redis_host=redis_host,
    )

    # STEP 2 – Validate data
    validate_task = step2_data_validation(
        dataset=extract_task.outputs["output_path"],
    )

    # STEP 3 – Feature engineering
    engineer_task = step3_feature_engineering(
        dataset=validate_task.outputs["output_path"],
    )

    # STEP 4 – Train model + log to MLflow
    train_task = step4_model_training(
        dataset=engineer_task.outputs["output_path"],
        tracking_uri=tracking_uri,
        experiment_name=experiment_name,
        s3_endpoint=s3_endpoint,
        aws_key=aws_key,
        aws_secret=aws_secret,
    )

    # STEP 5 – Evaluate
    eval_task = step5_evaluation(
        train_result_json=train_task.output,
    )

    # STEP 6 – Register model
    step6_model_registration(
        train_result_json=train_task.output,
        metrics_json=eval_task.output,
        model_name=model_name,
        tracking_uri=tracking_uri,
        s3_endpoint=s3_endpoint,
        aws_key=aws_key,
        aws_secret=aws_secret,
        roc_auc_threshold=roc_auc_threshold,
    )


# ═══════════════════════════════════════════════════════════════════════
# Compile to YAML
# ═══════════════════════════════════════════════════════════════════════
if __name__ == "__main__":
    compiler.Compiler().compile(
        pipeline_func=customer_churn_pipeline_with_lineage,
        package_path="customer_churn_pipeline_with_lineage.yaml",
    )
    print("Pipeline compiled → customer_churn_pipeline_with_lineage.yaml")
