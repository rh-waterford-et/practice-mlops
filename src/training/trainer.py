"""
STAGE 4  –  Model training with XGBoost + MLflow tracking.

Responsibilities
----------------
- Encode categoricals and prepare feature matrix
- Train an XGBoost classifier
- Log params, metrics, and the model artifact to MLflow
- Return trained model + evaluation metrics
"""

import logging
import os

import mlflow
import mlflow.sklearn
import numpy as np
import pandas as pd
import xgboost as xgb
from sklearn.metrics import (
    f1_score,
    precision_score,
    recall_score,
    roc_auc_score,
)
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import LabelEncoder

logger = logging.getLogger(__name__)

CATEGORICAL_COLS = ["contract_type", "internet_service", "payment_method"]
FEATURE_COLS = [
    "tenure_months",
    "monthly_charges",
    "total_charges",
    "num_support_tickets",
    "contract_type",
    "internet_service",
    "payment_method",
]
TARGET = "churn"


def _encode_categoricals(
    df: pd.DataFrame,
) -> tuple[pd.DataFrame, dict[str, LabelEncoder]]:
    """Label-encode categorical columns; return encoded df + encoder map."""
    df = df.copy()
    encoders: dict[str, LabelEncoder] = {}
    for col in CATEGORICAL_COLS:
        le = LabelEncoder()
        df[col] = le.fit_transform(df[col].astype(str))
        encoders[col] = le
    return df, encoders


def prepare_data(
    df: pd.DataFrame, test_size: float = 0.2, seed: int = 42
) -> tuple:
    """Encode features, split into train/test, return matrices."""
    df, encoders = _encode_categoricals(df)

    X = df[FEATURE_COLS].values.astype(np.float32)
    y = df[TARGET].values.astype(np.int32)

    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=test_size, random_state=seed, stratify=y,
    )
    return X_train, X_test, y_train, y_test, encoders


def train_and_log(
    df: pd.DataFrame,
    tracking_uri: str,
    experiment_name: str,
    params: dict | None = None,
) -> dict:
    """
    Full training loop with MLflow tracking.

    Returns dict with run_id, metrics, and model_uri.
    """
    os.environ["MLFLOW_S3_ENDPOINT_URL"] = os.getenv(
        "MLFLOW_S3_ENDPOINT_URL", "http://localhost:9000"
    )
    os.environ["AWS_ACCESS_KEY_ID"] = os.getenv("AWS_ACCESS_KEY_ID", "minioadmin")
    os.environ["AWS_SECRET_ACCESS_KEY"] = os.getenv("AWS_SECRET_ACCESS_KEY", "minioadmin123")

    mlflow.set_tracking_uri(tracking_uri)
    artifact_root = os.getenv("MLFLOW_S3_ARTIFACT_ROOT", "s3://mlflow/artifacts")
    if mlflow.get_experiment_by_name(experiment_name) is None:
        mlflow.create_experiment(experiment_name, artifact_location=artifact_root)
    mlflow.set_experiment(experiment_name)

    X_train, X_test, y_train, y_test, encoders = prepare_data(df)

    # Balance classes: weight positives by neg/pos ratio so rare churners
    # contribute equally to the loss as the majority non-churn class.
    neg = int((y_train == 0).sum())
    pos = int((y_train == 1).sum())
    spw = round(neg / pos, 2) if pos > 0 else 1.0
    logger.info("Class counts – neg: %d  pos: %d  scale_pos_weight: %.2f", neg, pos, spw)

    default_params = {
        "max_depth": 6,
        "learning_rate": 0.1,
        "n_estimators": 200,
        "objective": "binary:logistic",
        "eval_metric": "logloss",
        "scale_pos_weight": spw,
        "seed": 42,
    }
    if params:
        default_params.update(params)

    with mlflow.start_run() as run:
        train_dataset = mlflow.data.from_pandas(
            df, name="customer_features_view",
        )
        mlflow.log_input(train_dataset, context="training")

        mlflow.log_params(default_params)

        model = xgb.XGBClassifier(**default_params)
        model.fit(
            X_train,
            y_train,
            eval_set=[(X_test, y_test)],
            verbose=False,
        )

        y_pred = model.predict(X_test)
        y_prob = model.predict_proba(X_test)[:, 1]

        metrics = {
            "roc_auc": roc_auc_score(y_test, y_prob),
            "f1": f1_score(y_test, y_pred),
            "precision": precision_score(y_test, y_pred),
            "recall": recall_score(y_test, y_pred),
        }
        mlflow.log_metrics(metrics)
        logger.info("Metrics: %s", metrics)

        model_info = mlflow.sklearn.log_model(
            model, artifact_path="model", registered_model_name=None,
        )

        return {
            "run_id": run.info.run_id,
            "metrics": metrics,
            "model_uri": model_info.model_uri,
        }
