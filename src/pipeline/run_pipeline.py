"""
STAGE 3  –  Orchestrate the full Kubeflow-style pipeline locally.

This script runs all six steps locally (no KFP). The same
``src.pipeline.components`` functions are invoked from thin ``kfp_pipeline``
``@dsl.component`` bodies on the cluster (plus GX validation in
``gx_churn_validation`` for the KFP path only).
"""

import logging
import sys
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parents[2]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

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
