"""
STAGE 3  -  Kubeflow Pipelines v2 DSL definition for OpenShift AI.

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
SPARK_IMAGE = (
    "image-registry.openshift-image-registry.svc:5000/lineage/spark-etl:latest"
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


# =======================================================================
# PLATFORM - Spark ETL (CSV -> transform -> PostgreSQL, emits OpenLineage)
# =======================================================================
@dsl.component(base_image=SPARK_IMAGE)
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
        ["python3", "/opt/spark/spark_etl.py"],
        capture_output=True, text=True,
    )
    print(result.stdout)
    if result.returncode != 0:
        print(f"STDERR: {result.stderr}")
        raise RuntimeError(f"Spark ETL failed: {result.stderr[:500]}")
    print("Platform: spark ETL succeeded")
    return "etl_done"


# =======================================================================
# PLATFORM - Feast Apply (registers entities/views, emits OpenLineage)
# =======================================================================
@dsl.component(base_image=FKM_IMAGE)
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


# =======================================================================
# PLATFORM - Feast Materialize (offline -> online store, emits OpenLineage)
# =======================================================================
@dsl.component(base_image=FKM_IMAGE)
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


# =======================================================================
# DS - Data Extraction via Feast
# =======================================================================
@dsl.component(base_image=FKM_IMAGE)
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


# =======================================================================
# DS - Data Validation (Great Expectations + OpenLineage)
# =======================================================================
@dsl.component(base_image=FKM_IMAGE)
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

    all_vr_results = []
    for vr_key, vr in result.run_results.items():
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

    # -- Emit OpenLineage event with data quality facets ---------------
    run_id = str(uuid4())
    now = datetime.now(timezone.utc).isoformat()

    assertions = []
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
                "namespace": openlineage_namespace,
                "name": "validate_customer_data",
                "facets": {},
            },
            "inputs": [
                {
                    "namespace": openlineage_namespace,
                    "name": "customer_features_view",
                    "facets": dq_facet if event_type == "COMPLETE" else {},
                }
            ],
            "outputs": [
                {
                    "namespace": openlineage_namespace,
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

    # -- Remediation: fill nulls so downstream steps don't fail --------
    for col in critical_cols:
        nulls = df[col].isna().sum()
        if nulls > 0:
            print(f"  Filling {nulls} nulls in {col}")
            df[col] = df[col].fillna(0)

    const_cols = df.select_dtypes(include=[np.number]).columns
    for col in const_cols:
        if df[col].nunique() <= 1:
            print(f"  WARNING: {col} is constant")

    print(f"DS: validation complete - {len(df)} rows, "
          f"{evaluated} checks ({successful} passed, {unsuccessful} failed)")
    df.to_parquet(output_path.path)


# =======================================================================
# DS - Feature Engineering
# =======================================================================
@dsl.component(base_image=FKM_IMAGE)
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


# =======================================================================
# DS - Model Training + MLflow (emits OpenLineage)
# =======================================================================
@dsl.component(base_image=FKM_IMAGE)
def ds_model_training(
    dataset: dsl.Input[dsl.Dataset],
    tracking_uri: str,
    experiment_name: str,
    s3_endpoint: str,
    aws_key: str,
    aws_secret: str,
    pg_host: str = "postgres",
    pg_database: str = "warehouse",
) -> str:
    """Train XGBoost, log to MLflow. Returns JSON with run_id, model_uri, metrics."""
    import json
    import os

    import mlflow
    import mlflow.data
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
        dataset_source = f"postgresql://{pg_host}:5432/{pg_database}.public.customer_features"
        train_dataset = mlflow.data.from_pandas(
            df, source=dataset_source, name="customer_features_view",
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


# =======================================================================
# DS - Evaluation
# =======================================================================
@dsl.component(base_image=FKM_IMAGE)
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


# =======================================================================
# DS - Model Registration
# =======================================================================
@dsl.component(base_image=FKM_IMAGE)
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


# =======================================================================
# Pipeline definition
# =======================================================================
@dsl.pipeline(
    name="Customer Churn ML Pipeline",
    description=(
        "End-to-end churn prediction pipeline. "
        "PLATFORM steps: Spark ETL, Feast apply & materialize (managed by infra). "
        "DS steps: data extraction, validation, feature engineering, "
        "XGBoost training, evaluation, MLflow registration (owned by data scientists)."
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
    # -- PLATFORM STEPS (managed by infrastructure) ----------------------

    etl_task = platform_spark_etl(
        minio_endpoint="mlflow-minio:9000",
        pg_host=pg_host,
        pg_user="feast",
        pg_password="feast",
        pg_database="warehouse",
        warehouse_table=table_name,
        openlineage_url="http://marquez",
        openlineage_namespace="churn-demo/customer_churn",
        aws_access_key=aws_key,
        aws_secret_key=aws_secret,
    )
    etl_task.set_caching_options(False)

    apply_task = platform_feast_apply(
        feast_repo_path=feast_repo_path,
        pg_host=pg_host,
        redis_host=redis_host,
    )
    apply_task.after(etl_task)
    apply_task.set_caching_options(False)

    materialize_task = platform_feast_materialize(
        feast_repo_path=feast_repo_path,
        pg_host=pg_host,
        redis_host=redis_host,
        apply_done=apply_task.output,
    )
    materialize_task.set_caching_options(False)

    # -- DS STEPS (owned by data scientists) ---------------------------

    extract_task = ds_data_extraction(
        pg_url=pg_url,
        feast_repo_path=feast_repo_path,
        table_name=table_name,
        pg_host=pg_host,
        redis_host=redis_host,
        materialize_done=materialize_task.output,
    )
    extract_task.set_caching_options(False)

    validate_task = ds_data_validation(
        dataset=extract_task.outputs["output_path"],
        openlineage_url="http://marquez",
        openlineage_namespace="churn-demo/customer_churn",
    )
    validate_task.set_caching_options(False)

    engineer_task = ds_feature_engineering(
        dataset=validate_task.outputs["output_path"],
    )
    engineer_task.set_caching_options(False)

    train_task = ds_model_training(
        dataset=engineer_task.outputs["output_path"],
        tracking_uri=tracking_uri,
        experiment_name=experiment_name,
        s3_endpoint=s3_endpoint,
        aws_key=aws_key,
        aws_secret=aws_secret,
        pg_host=pg_host,
        pg_database="warehouse",
    )
    train_task.set_caching_options(False)

    eval_task = ds_evaluation(
        train_result_json=train_task.output,
    )
    eval_task.set_caching_options(False)

    reg_task = ds_model_registration(
        train_result_json=train_task.output,
        metrics_json=eval_task.output,
        model_name=model_name,
        tracking_uri=tracking_uri,
        s3_endpoint=s3_endpoint,
        aws_key=aws_key,
        aws_secret=aws_secret,
        roc_auc_threshold=roc_auc_threshold,
    )
    reg_task.set_caching_options(False)


# =======================================================================
# Compile to YAML
# =======================================================================
if __name__ == "__main__":
    compiler.Compiler().compile(
        pipeline_func=customer_churn_pipeline,
        package_path="customer_churn_pipeline.yaml",
    )
    print("Pipeline compiled -> customer_churn_pipeline.yaml")
