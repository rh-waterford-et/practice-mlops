"""
Reverse-engineered Customer Churn ML Pipeline from customer_churn_pipeline.yaml
This is a complete end-to-end ML pipeline with lineage tracking.

Pipeline Flow:
1. platform_spark_etl - Spark ETL from MinIO to PostgreSQL
2. platform_feast_apply - Register feature definitions
3. platform_feast_materialize - Materialize features to online store
4. ds_data_extraction - Extract features from Feast
5. ds_data_validation - Validate data quality with Great Expectations
6. ds_feature_engineering - Create derived features
7. ds_model_training - Train XGBoost model with MLflow
8. ds_evaluation - Evaluate model metrics
9. ds_model_registration - Register model in MLflow if meets threshold
"""

from kfp import dsl
from kfp import compiler


# ============================================================================
# PLATFORM COMPONENTS (Infrastructure/Data Engineering)
# ============================================================================

@dsl.component(
    base_image='image-registry.openshift-image-registry.svc:5000/lineage/spark-etl:latest'
)
def platform_spark_etl(
    minio_endpoint: str,
    pg_host: str,
    pg_user: str,
    pg_password: str,
    pg_database: str,
    warehouse_table: str,
    openlineage_url: str,
    openlineage_namespace: str,
    aws_access_key: str,
    aws_secret_key: str,
) -> str:
    """PySpark ETL that reads CSV from MinIO, transforms, writes to PostgreSQL.
    The OpenLineage Spark listener emits lineage events automatically."""
    import os
    import subprocess

    os.environ["MINIO_ENDPOINT"] = minio_endpoint
    os.environ["PG_HOST"] = pg_host
    os.environ["PG_USER"] = pg_user
    os.environ["PG_PASSWORD"] = pg_password
    os.environ["PG_DATABASE"] = pg_database
    os.environ["WAREHOUSE_TABLE"] = warehouse_table
    os.environ["OPENLINEAGE_URL"] = openlineage_url
    os.environ["OPENLINEAGE_NAMESPACE"] = openlineage_namespace
    os.environ["AWS_ACCESS_KEY_ID"] = aws_access_key
    os.environ["AWS_SECRET_ACCESS_KEY"] = aws_secret_key

    result = subprocess.run(
        ["python3", "/opt/spark/spark_etl_native_lineage.py"],
        capture_output=True, text=True,
    )
    print(result.stdout)
    if result.returncode != 0:
        print(f"STDERR: {result.stderr}")
        raise RuntimeError(f"Spark ETL failed: {result.stderr[:500]}")
    print("Platform: spark ETL succeeded")
    return "etl_done"


@dsl.component(
    base_image='image-registry.openshift-image-registry.svc:5000/lineage/fkm-app:latest'
)
def platform_feast_apply(
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
    print("Platform: feast apply succeeded")
    return "applied"


@dsl.component(
    base_image='image-registry.openshift-image-registry.svc:5000/lineage/fkm-app:latest'
)
def platform_feast_materialize(
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
    print("Platform: feast materialize succeeded")
    return "materialized"


# ============================================================================
# DATA SCIENCE COMPONENTS
# ============================================================================

@dsl.component(
    base_image='image-registry.openshift-image-registry.svc:5000/lineage/fkm-app:latest'
)
def ds_data_extraction(
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
    print(f"DS: data extraction complete - shape {result.shape}")
    result.to_parquet(output_path.path)


@dsl.component(
    base_image='image-registry.openshift-image-registry.svc:5000/lineage/fkm-app:latest'
)
def ds_data_validation(
    dataset: dsl.Input[dsl.Dataset],
    openlineage_url: str,
    openlineage_namespace: str,
    output_path: dsl.Output[dsl.Dataset],
) -> None:
    """Validate data quality with Great Expectations, emitting OpenLineage events."""
    import json
    import os
    from datetime import datetime, timezone
    from urllib.request import Request, urlopen
    from uuid import uuid4

    import numpy as np
    import pandas as pd
    import great_expectations as gx

    df = pd.read_parquet(dataset.path)
    print(f"Loaded {len(df)} rows for validation")

    # -- Great Expectations validation ---------------------------------
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

    validator = context.get_validator(
        batch_definition=batch_def,
        batch_parameters={"dataframe": df},
        expectation_suite=suite,
    )
    validation_result = validator.validate()

    success = validation_result.success
    print(f"Validation {'PASSED' if success else 'FAILED'}")
    if not success:
        failed = [
            r.expectation_config.to_dict()
            for r in validation_result.results
            if not r.success
        ]
        print(f"Failed expectations: {failed[:3]}")
        raise RuntimeError("Data validation failed")

    # -- OpenLineage event emission ------------------------------------
    run_id = str(uuid4())
    job_name = "ds_data_validation"
    now = datetime.now(timezone.utc).isoformat()

    event = {
        "eventType": "COMPLETE",
        "eventTime": now,
        "run": {"runId": run_id},
        "job": {
            "namespace": openlineage_namespace,
            "name": job_name,
        },
        "inputs": [
            {
                "namespace": openlineage_namespace,
                "name": "customer_features_extracted",
                "facets": {
                    "dataQuality": {
                        "_producer": "great-expectations",
                        "_schemaURL": "https://openlineage.io/spec/facets/1-0-0/DataQualityMetricsInputDatasetFacet.json",
                        "rowCount": len(df),
                        "columnMetrics": {
                            col: {"nullCount": int(df[col].isnull().sum())}
                            for col in critical_cols
                        },
                    }
                },
            }
        ],
        "outputs": [
            {
                "namespace": openlineage_namespace,
                "name": "customer_features_validated",
                "facets": {},
            }
        ],
    }

    req = Request(
        f"{openlineage_url}/api/v1/lineage",
        data=json.dumps(event).encode("utf-8"),
        headers={"Content-Type": "application/json"},
    )
    with urlopen(req) as resp:
        print(f"OpenLineage event sent: {resp.status}")

    print("DS: data validation complete")
    df.to_parquet(output_path.path)


@dsl.component(
    base_image='image-registry.openshift-image-registry.svc:5000/lineage/fkm-app:latest'
)
def ds_feature_engineering(
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

    print(f"DS: feature engineering complete - shape {df.shape}")
    df.to_parquet(output_path.path)


@dsl.component(
    base_image='image-registry.openshift-image-registry.svc:5000/lineage/fkm-app:latest'
)
def ds_model_training(
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

        print(f"DS: model training complete - metrics: {metrics}")
        return json.dumps({
            "run_id": run.info.run_id,
            "model_uri": info.model_uri,
            "metrics": metrics,
        })


@dsl.component(
    base_image='image-registry.openshift-image-registry.svc:5000/lineage/fkm-app:latest'
)
def ds_evaluation(train_result_json: str) -> str:
    """Display and return evaluation metrics."""
    import json

    result = json.loads(train_result_json)
    m = result["metrics"]
    print(
        f"DS: evaluation  ROC-AUC={m['roc_auc']:.4f}  F1={m['f1']:.4f}  "
        f"Precision={m['precision']:.4f}  Recall={m['recall']:.4f}"
    )
    return json.dumps(m)


@dsl.component(
    base_image='image-registry.openshift-image-registry.svc:5000/lineage/fkm-app:latest'
)
def ds_model_registration(
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
        print(f"DS: model registration  ROC-AUC {metrics['roc_auc']:.4f} < {roc_auc_threshold} - skipping")
        return json.dumps({"registered": False, "reason": "below_threshold"})

    mlflow.set_tracking_uri(tracking_uri)
    try:
        mv = mlflow.register_model(model_uri=result["model_uri"], name=model_name)
        client = MlflowClient()
        client.set_registered_model_alias(model_name, "champion", str(mv.version))
        print(f"DS: registered {model_name} v{mv.version} as alias 'champion'")
        return json.dumps({
            "registered": True,
            "model_name": model_name,
            "version": int(mv.version),
            "alias": "champion",
        })
    except Exception as e:
        print(f"DS: model registration failed (non-fatal): {e}")
        return json.dumps({"registered": False, "reason": str(e)[:200]})


# ============================================================================
# PIPELINE DEFINITION
# ============================================================================

@dsl.pipeline(
    name="customer-churn-ml-pipeline",
    description=(
        "End-to-end churn prediction pipeline. "
        "PLATFORM steps: Spark ETL, Feast apply & materialize (managed by infra). "
        "DS steps: data extraction, validation, feature engineering, XGBoost training, "
        "evaluation, MLflow registration (owned by data scientists)."
    ),
)
def customer_churn_pipeline(
    # S3/MinIO credentials
    aws_key: str = "minioadmin",
    aws_secret: str = "minioadmin123",

    # Database & storage endpoints
    pg_host: str = "postgres",
    pg_url: str = "postgresql://feast:feast@postgres:5432/warehouse",
    redis_host: str = "redis",
    s3_endpoint: str = "http://mlflow-minio:9000",

    # Feast configuration
    feast_repo_path: str = "/app/src/feature_store",
    table_name: str = "customer_features",

    # MLflow configuration
    tracking_uri: str = "openlineage+http://mlflow-server:5000",
    experiment_name: str = "customer_churn_lineage",
    model_name: str = "customer_churn_model",
    roc_auc_threshold: float = 0.7,
):
    # ========================================================================
    # PLATFORM TASKS (Data Engineering/Infrastructure)
    # ========================================================================

    # 1. Spark ETL: Read from MinIO, transform, write to PostgreSQL
    spark_etl_task = platform_spark_etl(
        minio_endpoint="mlflow-minio:9000",
        pg_host=pg_host,
        pg_user="feast",
        pg_password="feast",
        pg_database="warehouse",
        warehouse_table=table_name,
        openlineage_url="http://marquez.lineage.svc",
        openlineage_namespace="churn-demo/customer_churn",
        aws_access_key=aws_key,
        aws_secret_key=aws_secret,
    )

    spark_etl_task.set_caching_options(False)
    
    # 2. Feast Apply: Register feature definitions (depends on Spark ETL)
    feast_apply_task = platform_feast_apply(
        feast_repo_path=feast_repo_path,
        pg_host=pg_host,
        redis_host=redis_host,
    )
    feast_apply_task.after(spark_etl_task)

    # 3. Feast Materialize: Load features to online store (depends on Feast Apply)
    feast_materialize_task = platform_feast_materialize(
        feast_repo_path=feast_repo_path,
        pg_host=pg_host,
        redis_host=redis_host,
        apply_done=feast_apply_task.output,
    )

    # ========================================================================
    # DATA SCIENCE TASKS
    # ========================================================================

    # 4. Data Extraction: Get historical features from Feast
    extraction_task = ds_data_extraction(
        pg_url=pg_url,
        feast_repo_path=feast_repo_path,
        table_name=table_name,
        pg_host=pg_host,
        redis_host=redis_host,
        materialize_done=feast_materialize_task.output,
    )

    # 5. Data Validation: Great Expectations validation
    validation_task = ds_data_validation(
        dataset=extraction_task.outputs["output_path"],
        openlineage_url="http://marquez.lineage.svc",
        openlineage_namespace="churn-demo/customer_churn",
    )

    # 6. Feature Engineering: Create derived features
    feature_eng_task = ds_feature_engineering(
        dataset=validation_task.outputs["output_path"],
    )

    # 7. Model Training: Train XGBoost with MLflow tracking
    training_task = ds_model_training(
        dataset=feature_eng_task.outputs["output_path"],
        tracking_uri=tracking_uri,
        experiment_name=experiment_name,
        s3_endpoint=s3_endpoint,
        aws_key=aws_key,
        aws_secret=aws_secret,
    )

    # 8. Evaluation: Display metrics
    evaluation_task = ds_evaluation(
        train_result_json=training_task.output,
    )

    # 9. Model Registration: Register if metrics meet threshold
    registration_task = ds_model_registration(
        train_result_json=training_task.output,
        metrics_json=evaluation_task.output,
        model_name=model_name,
        tracking_uri=tracking_uri,
        s3_endpoint=s3_endpoint,
        aws_key=aws_key,
        aws_secret=aws_secret,
        roc_auc_threshold=roc_auc_threshold,
    )


# ============================================================================
# COMPILATION
# ============================================================================

if __name__ == "__main__":
    compiler.Compiler().compile(
        pipeline_func=customer_churn_pipeline,
        package_path="customer_churn_pipeline_reconstructed.yaml",
    )
    print("Pipeline compiled successfully to: customer_churn_pipeline_reconstructed.yaml")
