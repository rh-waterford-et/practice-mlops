"""
STAGE 3  –  Kubeflow Pipelines v2 DSL definition for OpenShift AI.

Each component uses the pre-built fkm-app image (which already contains
all Python dependencies, source code, and the Feast feature_store.yaml).

Compile:
    python -m src.pipeline.kfp_pipeline

Upload to OpenShift AI:
    python -m src.pipeline.upload_pipeline
"""

from kfp import dsl, compiler

# The fkm-app image built via OpenShift BuildConfig contains all deps.
# Override at pipeline-creation time via the `image` parameter.
FKM_IMAGE = (
    "image-registry.openshift-image-registry.svc:5000/lineage/fkm-app:latest"
)


FEAST_STORE_YAML = """\
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
openlineage:
  enabled: true
  transport_type: http
  transport_url: http://marquez
  namespace: churn-demo
  emit_on_apply: true
  emit_on_materialize: true
"""


def _patch_feast_yaml(feast_repo_path: str, pg_host: str, redis_host: str) -> None:
    """Write feature_store.yaml pointing at in-cluster services."""
    import os
    fs_yaml = os.path.join(feast_repo_path, "feature_store.yaml")
    with open(fs_yaml, "w") as f:
        f.write(FEAST_STORE_YAML.format(pg_host=pg_host, redis_host=redis_host))
    print(f"Patched {fs_yaml} -> pg={pg_host}, redis={redis_host}")


# ═══════════════════════════════════════════════════════════════════════
# STEP 0a — Feast Apply (registers entities/views, emits OpenLineage)
# ═══════════════════════════════════════════════════════════════════════
@dsl.component(base_image=FKM_IMAGE)
def step0a_feast_apply(
    feast_repo_path: str,
    pg_host: str,
    redis_host: str,
) -> str:
    """Run feast apply to register feature definitions. Emits OpenLineage events."""
    import os
    import subprocess

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
openlineage:
  enabled: true
  transport_type: http
  transport_url: http://marquez
  namespace: churn-demo
  emit_on_apply: true
  emit_on_materialize: true
""")
    print(f"Patched {fs_yaml}")

    env = os.environ.copy()
    env["REDIS_PORT"] = "6379"
    result = subprocess.run(
        ["feast", "apply"],
        cwd=feast_repo_path,
        capture_output=True, text=True,
        env=env,
    )
    print(result.stdout)
    if result.returncode != 0:
        print(f"STDERR: {result.stderr}")
        raise RuntimeError(f"feast apply failed: {result.stderr[:500]}")
    print("Step 0a complete - feast apply succeeded")
    return "applied"


# ═══════════════════════════════════════════════════════════════════════
# STEP 0b — Feast Materialize (offline -> online store, emits OpenLineage)
# ═══════════════════════════════════════════════════════════════════════
@dsl.component(base_image=FKM_IMAGE)
def step0b_feast_materialize(
    feast_repo_path: str,
    pg_host: str,
    redis_host: str,
    apply_done: str,
) -> str:
    """Materialize features from offline to online store. Emits OpenLineage events."""
    import os
    from datetime import datetime, timedelta
    from feast import FeatureStore

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
openlineage:
  enabled: true
  transport_type: http
  transport_url: http://marquez
  namespace: churn-demo
  emit_on_apply: true
  emit_on_materialize: true
""")

    store = FeatureStore(repo_path=feast_repo_path)
    end = datetime.utcnow()
    start = end - timedelta(days=1000)
    print(f"Materializing features {start.isoformat()} -> {end.isoformat()}")
    store.materialize(start_date=start, end_date=end)
    print("Step 0b complete - feast materialize succeeded")
    return "materialized"


# ═══════════════════════════════════════════════════════════════════════
# STEP 1 — Data Extraction via Feast
# ═══════════════════════════════════════════════════════════════════════
@dsl.component(base_image=FKM_IMAGE)
def step1_data_extraction(
    pg_url: str,
    feast_repo_path: str,
    table_name: str,
    pg_host: str,
    redis_host: str,
    materialize_done: str,
    output_path: dsl.Output[dsl.Dataset],
) -> None:
    """Retrieve historical features from Feast and save as parquet."""
    import os
    import pandas as pd
    from sqlalchemy import create_engine, text
    from feast import FeatureStore

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
openlineage:
  enabled: true
  transport_type: http
  transport_url: http://marquez
  namespace: churn-demo
  emit_on_apply: true
  emit_on_materialize: true
""")
    print(f"Patched {fs_yaml} -> pg={pg_host}, redis={redis_host}")

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
# STEP 2 — Data Validation
# ═══════════════════════════════════════════════════════════════════════
@dsl.component(base_image=FKM_IMAGE)
def step2_data_validation(
    dataset: dsl.Input[dsl.Dataset],
    output_path: dsl.Output[dsl.Dataset],
) -> None:
    """Validate schema, nulls, distributions."""
    import numpy as np
    import pandas as pd

    df = pd.read_parquet(dataset.path)

    critical_cols = [
        "entity_id", "event_timestamp", "tenure_months",
        "monthly_charges", "total_charges", "num_support_tickets", "churn",
    ]
    for col in critical_cols:
        nulls = df[col].isna().sum()
        if nulls > 0:
            print(f"  Filling {nulls} nulls in {col}")
            df[col] = df[col].fillna(0)

    numeric_cols = df.select_dtypes(include=[np.number]).columns
    for col in numeric_cols:
        if df[col].nunique() <= 1:
            print(f"  WARNING: {col} is constant")

    print(f"Step 2 complete – {len(df)} rows validated")
    df.to_parquet(output_path.path)


# ═══════════════════════════════════════════════════════════════════════
# STEP 3 — Feature Engineering
# ═══════════════════════════════════════════════════════════════════════
@dsl.component(base_image=FKM_IMAGE)
def step3_feature_engineering(
    dataset: dsl.Input[dsl.Dataset],
    output_path: dsl.Output[dsl.Dataset],
) -> None:
    """Add derived features."""
    import numpy as np
    import pandas as pd

    df = pd.read_parquet(dataset.path)
    tenure_safe = df["tenure_months"].replace(0, 1)
    df["charges_per_month"] = df["total_charges"] / tenure_safe
    df["ticket_rate"] = df["num_support_tickets"] / tenure_safe

    for col in ["charges_per_month", "ticket_rate"]:
        df[col] = df[col].replace([np.inf, -np.inf], 0.0)

    print(f"Step 3 complete – shape {df.shape}")
    df.to_parquet(output_path.path)


# ═══════════════════════════════════════════════════════════════════════
# STEP 4 — Model Training + MLflow
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
    """Train XGBoost, log to MLflow. Returns JSON with run_id, model_uri, metrics."""
    import json
    import os

    import mlflow
    import mlflow.sklearn
    import numpy as np
    import pandas as pd
    import xgboost as xgb
    from sklearn.metrics import (
        f1_score, precision_score, recall_score, roc_auc_score,
    )
    from sklearn.model_selection import train_test_split
    from sklearn.preprocessing import LabelEncoder

    os.environ["MLFLOW_S3_ENDPOINT_URL"] = s3_endpoint
    os.environ["AWS_ACCESS_KEY_ID"] = aws_key
    os.environ["AWS_SECRET_ACCESS_KEY"] = aws_secret
    os.environ["OPENLINEAGE_URL"] = "http://marquez"
    os.environ["OPENLINEAGE_NAMESPACE"] = "churn-demo/customer_churn"

    df = pd.read_parquet(dataset.path)

    cat_cols = ["contract_type", "internet_service", "payment_method"]
    for col in cat_cols:
        df[col] = LabelEncoder().fit_transform(df[col].astype(str))

    feature_cols = [
        "tenure_months", "monthly_charges", "total_charges",
        "num_support_tickets", "contract_type", "internet_service",
        "payment_method",
    ]
    X = df[feature_cols].values.astype(np.float32)
    y = df["churn"].values.astype(np.int32)

    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.2, random_state=42, stratify=y,
    )

    params = {
        "max_depth": 6,
        "learning_rate": 0.1,
        "n_estimators": 200,
        "objective": "binary:logistic",
        "eval_metric": "logloss",
        "seed": 42,
    }

    mlflow.set_tracking_uri(tracking_uri)
    artifact_root = os.getenv("MLFLOW_S3_ARTIFACT_ROOT", "s3://mlflow/artifacts")
    if mlflow.get_experiment_by_name(experiment_name) is None:
        mlflow.create_experiment(experiment_name, artifact_location=artifact_root)
    mlflow.set_experiment(experiment_name)

    with mlflow.start_run() as run:
        train_dataset = mlflow.data.from_pandas(
            df, name="customer_features_view",
        )
        mlflow.log_input(train_dataset, context="training")

        mlflow.log_params(params)

        model = xgb.XGBClassifier(**params)
        model.fit(X_train, y_train, eval_set=[(X_test, y_test)], verbose=False)

        y_pred = model.predict(X_test)
        y_prob = model.predict_proba(X_test)[:, 1]

        metrics = {
            "roc_auc": float(roc_auc_score(y_test, y_prob)),
            "f1": float(f1_score(y_test, y_pred)),
            "precision": float(precision_score(y_test, y_pred)),
            "recall": float(recall_score(y_test, y_pred)),
        }
        mlflow.log_metrics(metrics)

        info = mlflow.sklearn.log_model(model, artifact_path="model")

        print(f"Step 4 complete – metrics: {metrics}")
        return json.dumps({
            "run_id": run.info.run_id,
            "model_uri": info.model_uri,
            "metrics": metrics,
        })


# ═══════════════════════════════════════════════════════════════════════
# STEP 5 — Evaluation
# ═══════════════════════════════════════════════════════════════════════
@dsl.component(base_image=FKM_IMAGE)
def step5_evaluation(train_result_json: str) -> str:
    """Display and return evaluation metrics."""
    import json

    result = json.loads(train_result_json)
    m = result["metrics"]
    print(
        f"Step 5  ROC-AUC={m['roc_auc']:.4f}  F1={m['f1']:.4f}  "
        f"Precision={m['precision']:.4f}  Recall={m['recall']:.4f}"
    )
    return json.dumps(m)


# ═══════════════════════════════════════════════════════════════════════
# STEP 6 — Model Registration
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
    """Register model in MLflow if metrics beat the threshold."""
    import json
    import os

    import mlflow
    from mlflow.tracking import MlflowClient

    os.environ["MLFLOW_S3_ENDPOINT_URL"] = s3_endpoint
    os.environ["AWS_ACCESS_KEY_ID"] = aws_key
    os.environ["AWS_SECRET_ACCESS_KEY"] = aws_secret

    # The model registry doesn't have an openlineage+ scheme plugin,
    # so point it at the plain MLflow server URL directly.
    plain_uri = tracking_uri.replace("openlineage+", "")
    os.environ["MLFLOW_REGISTRY_URI"] = plain_uri

    result = json.loads(train_result_json)
    metrics = json.loads(metrics_json)

    if metrics["roc_auc"] < roc_auc_threshold:
        print(f"Step 6  ROC-AUC {metrics['roc_auc']:.4f} < {roc_auc_threshold} – skipping")
        return json.dumps({"registered": False, "reason": "below_threshold"})

    mlflow.set_tracking_uri(tracking_uri)
    try:
        mv = mlflow.register_model(model_uri=result["model_uri"], name=model_name)
        client = MlflowClient()
        client.set_registered_model_alias(model_name, "champion", str(mv.version))
        print(f"Step 6  Registered {model_name} v{mv.version} as alias 'champion'")
        return json.dumps({
            "registered": True,
            "model_name": model_name,
            "version": int(mv.version),
            "alias": "champion",
        })
    except Exception as e:
        print(f"Step 6  Model registration failed (non-fatal): {e}")
        return json.dumps({"registered": False, "reason": str(e)[:200]})


# ═══════════════════════════════════════════════════════════════════════
# Pipeline definition
# ═══════════════════════════════════════════════════════════════════════
@dsl.pipeline(
    name="Customer Churn ML Pipeline",
    description=(
        "End-to-end: Feast apply, Materialize, Extraction, Validation, "
        "Feature engineering, XGBoost training, Evaluation, MLflow registration"
    ),
)
def customer_churn_pipeline(
    pg_url: str = "postgresql://feast:feast@postgres:5432/warehouse",
    feast_repo_path: str = "/app/src/feature_store",
    table_name: str = "customer_features",
    pg_host: str = "postgres",
    redis_host: str = "redis",
    tracking_uri: str = "openlineage+http://mlflow-server:5000",
    experiment_name: str = "customer_churn_lineage",
    model_name: str = "customer_churn_model",
    s3_endpoint: str = "http://mlflow-minio:9000",
    aws_key: str = "minioadmin",
    aws_secret: str = "minioadmin123",
    roc_auc_threshold: float = 0.70,
):
    # STEP 0a – Register feature definitions (emits OpenLineage)
    apply_task = step0a_feast_apply(
        feast_repo_path=feast_repo_path,
        pg_host=pg_host,
        redis_host=redis_host,
    )

    # STEP 0b – Materialize features offline->online (emits OpenLineage)
    materialize_task = step0b_feast_materialize(
        feast_repo_path=feast_repo_path,
        pg_host=pg_host,
        redis_host=redis_host,
        apply_done=apply_task.output,
    )

    # STEP 1 – Extract historical features from Feast
    extract_task = step1_data_extraction(
        pg_url=pg_url,
        feast_repo_path=feast_repo_path,
        table_name=table_name,
        pg_host=pg_host,
        redis_host=redis_host,
        materialize_done=materialize_task.output,
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
        pipeline_func=customer_churn_pipeline,
        package_path="customer_churn_pipeline.yaml",
    )
    print("Pipeline compiled -> customer_churn_pipeline.yaml")
