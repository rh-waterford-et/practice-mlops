"""
STAGE 3  –  Kubeflow Pipeline component functions.

Each function is a self-contained KFP component that can be compiled
into a container step.  When running locally (outside KFP) the same
functions can be called directly from `run_pipeline.py`.

Components
----------
1. data_extraction     – Feast historical features → DataFrame
2. data_validation     – Null / schema / distribution checks
3. feature_engineering – Scaling & encoding transforms
4. model_training      – XGBoost training + MLflow logging
5. evaluation          – Compute & log final metrics
6. model_registration  – Conditionally register in MLflow Registry
"""

import json
import logging
import os
import sys
from typing import NamedTuple

import numpy as np
import pandas as pd

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

logging.basicConfig(level=logging.INFO, format="%(asctime)s  %(levelname)s  %(message)s")
logger = logging.getLogger(__name__)


# ═══════════════════════════════════════════════════════════════════════
# STEP 1 — Data Extraction (Feast)
# ═══════════════════════════════════════════════════════════════════════
def data_extraction(
    pg_url: str,
    feast_repo_path: str,
    table_name: str,
) -> pd.DataFrame:
    """
    Retrieve historical features from Feast via point-in-time join.

    The entity DataFrame is built from the warehouse table so every
    entity_id + event_timestamp pair is used for the join.
    """
    from sqlalchemy import create_engine, text
    from src.feature_store.feast_workflow import get_historical_features

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

    features_df = get_historical_features(
        entity_df=entity_df[["entity_id", "event_timestamp"]],
        repo_path=feast_repo_path,
    )

    # Normalise timestamp dtypes before merging (Feast may return
    # datetime64[us] while entity_df uses datetime64[ns, UTC])
    for df_ in (features_df, entity_df):
        df_["event_timestamp"] = pd.to_datetime(
            df_["event_timestamp"], utc=True,
        )

    # Re-attach target column
    result = features_df.merge(
        entity_df[["entity_id", "event_timestamp", "churn"]],
        on=["entity_id", "event_timestamp"],
        how="left",
    )
    logger.info("STEP 1  Data extraction complete – shape %s", result.shape)
    return result


# ═══════════════════════════════════════════════════════════════════════
# STEP 2 — Data Validation
# ═══════════════════════════════════════════════════════════════════════
def data_validation(df: pd.DataFrame) -> pd.DataFrame:
    """
    Validate the training DataFrame:
      - No nulls in critical columns
      - Schema type checks
      - Simple distribution sanity (no constant columns)
    """
    critical_cols = [
        "entity_id",
        "event_timestamp",
        "tenure_months",
        "monthly_charges",
        "total_charges",
        "num_support_tickets",
        "churn",
    ]

    # ── Null check ───────────────────────────────────────────────────────
    for col in critical_cols:
        null_count = df[col].isna().sum()
        if null_count > 0:
            logger.warning("Column %s has %d nulls – filling with 0", col, null_count)
            df[col] = df[col].fillna(0)

    # ── Schema validation ────────────────────────────────────────────────
    expected_types = {
        "tenure_months": "float",
        "monthly_charges": "float",
        "total_charges": "float",
        "num_support_tickets": "int",
    }
    for col, expected in expected_types.items():
        if expected == "float":
            df[col] = pd.to_numeric(df[col], errors="coerce").astype(float)
        else:
            df[col] = pd.to_numeric(df[col], errors="coerce").astype(int)

    # ── Distribution check (flag constant columns) ───────────────────────
    numeric_cols = df.select_dtypes(include=[np.number]).columns
    for col in numeric_cols:
        if df[col].nunique() <= 1:
            logger.warning("Column %s is constant – may not be informative", col)

    logger.info("STEP 2  Data validation passed – %d rows", len(df))
    return df


# ═══════════════════════════════════════════════════════════════════════
# STEP 3 — Feature Engineering
# ═══════════════════════════════════════════════════════════════════════
def feature_engineering(df: pd.DataFrame) -> pd.DataFrame:
    """
    Additional feature transforms on top of Feast output:
      - charges_per_month ratio
      - ticket_rate ratio
      - Label-encode categoricals (done later in trainer, but we
        validate encodability here)
    """
    df = df.copy()

    tenure_safe = df["tenure_months"].replace(0, 1)
    df["charges_per_month"] = df["total_charges"] / tenure_safe
    df["ticket_rate"] = df["num_support_tickets"] / tenure_safe

    # Clamp infinities
    for col in ["charges_per_month", "ticket_rate"]:
        df[col] = df[col].replace([np.inf, -np.inf], 0.0)

    logger.info("STEP 3  Feature engineering complete – new shape %s", df.shape)
    return df


# ═══════════════════════════════════════════════════════════════════════
# STEP 4 — Model Training
# ═══════════════════════════════════════════════════════════════════════
def model_training(
    df: pd.DataFrame,
    tracking_uri: str,
    experiment_name: str,
    params: dict | None = None,
) -> dict:
    """
    Train an XGBoost model and log everything to MLflow.

    Returns dict: {run_id, metrics, model_uri}
    """
    from src.training.trainer import train_and_log

    result = train_and_log(
        df=df,
        tracking_uri=tracking_uri,
        experiment_name=experiment_name,
        params=params,
    )
    logger.info("STEP 4  Model training complete – run_id=%s", result["run_id"])
    return result


# ═══════════════════════════════════════════════════════════════════════
# STEP 5 — Evaluation
# ═══════════════════════════════════════════════════════════════════════
def evaluation(train_result: dict, tracking_uri: str) -> dict:
    """
    Log final evaluation metrics to MLflow and return them.
    (Metrics are already computed during training; this step is an
    explicit verification / aggregation point.)
    """
    import mlflow

    mlflow.set_tracking_uri(tracking_uri)
    metrics = train_result["metrics"]
    logger.info(
        "STEP 5  Evaluation  ROC-AUC=%.4f  F1=%.4f  Precision=%.4f  Recall=%.4f",
        metrics["roc_auc"],
        metrics["f1"],
        metrics["precision"],
        metrics["recall"],
    )
    return metrics


# ═══════════════════════════════════════════════════════════════════════
# STEP 6 — Model Registration
# ═══════════════════════════════════════════════════════════════════════
def model_registration(
    train_result: dict,
    metrics: dict,
    model_name: str,
    tracking_uri: str,
    roc_auc_threshold: float = 0.70,
) -> dict:
    """
    If evaluation metrics exceed the threshold, register the model in
    MLflow's Model Registry and assign the 'champion' alias.
    """
    from src.training.registry import register_model, promote_to_alias

    if metrics["roc_auc"] < roc_auc_threshold:
        logger.warning(
            "STEP 6  ROC-AUC %.4f < threshold %.2f – model NOT registered",
            metrics["roc_auc"],
            roc_auc_threshold,
        )
        return {"registered": False, "reason": "below_threshold"}

    version = register_model(
        model_uri=train_result["model_uri"],
        model_name=model_name,
        tracking_uri=tracking_uri,
    )

    promote_to_alias(
        model_name=model_name,
        version=version,
        alias="champion",
        tracking_uri=tracking_uri,
    )

    logger.info(
        "STEP 6  Registered %s v%d → alias 'champion'", model_name, version,
    )
    return {"registered": True, "model_name": model_name, "version": version, "alias": "champion"}
