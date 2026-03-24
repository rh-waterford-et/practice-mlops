"""
STAGE 3  –  Orchestrate the full Kubeflow-style pipeline locally.

This script can be invoked standalone to run all six steps in sequence
on a local machine (no KFP cluster required).  The same component
functions are also wired into the KFP DSL in `kfp_pipeline.py`.
"""

import logging
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

from configs.settings import (
    FEAST_REPO_PATH,
    MLFLOW_EXPERIMENT_NAME,
    MLFLOW_TRACKING_URI,
    MODEL_NAME,
    MODEL_ROC_AUC_THRESHOLD,
    PG_URL,
    WAREHOUSE_TABLE,
)
from src.pipeline.components import (
    data_extraction,
    data_validation,
    evaluation,
    feature_engineering,
    model_registration,
    model_training,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s  %(levelname)s  %(message)s")
logger = logging.getLogger(__name__)


def run() -> dict:
    logger.info("═══ STAGE 3 – PIPELINE START ═══")

    # STEP 1 — Data Extraction via Feast
    df = data_extraction(
        pg_url=PG_URL,
        feast_repo_path=FEAST_REPO_PATH,
        table_name=WAREHOUSE_TABLE,
    )

    # STEP 2 — Data Validation
    df = data_validation(df)

    # STEP 3 — Feature Engineering
    df = feature_engineering(df)

    # STEP 4 — Model Training (+ MLflow logging)
    train_result = model_training(
        df=df,
        tracking_uri=MLFLOW_TRACKING_URI,
        experiment_name=MLFLOW_EXPERIMENT_NAME,
    )

    # STEP 5 — Evaluation
    metrics = evaluation(train_result, tracking_uri=MLFLOW_TRACKING_URI)

    # STEP 6 — Conditional Model Registration
    try:
        reg_result = model_registration(
            train_result=train_result,
            metrics=metrics,
            model_name=MODEL_NAME,
            tracking_uri=MLFLOW_TRACKING_URI,
            roc_auc_threshold=MODEL_ROC_AUC_THRESHOLD,
        )
    except Exception:
        logger.exception("STEP 6  Model registration failed (non-fatal)")
        reg_result = {"registered": False, "reason": "error"}

    logger.info("═══ STAGE 3 – PIPELINE COMPLETE ═══")
    return {"metrics": metrics, "registration": reg_result}


if __name__ == "__main__":
    result = run()
    print(result)
