"""
STAGE 2  –  Feast workflow helpers.

2.2  apply          – register entities, sources, feature views
2.3  materialize    – offline store → online store (Redis)
2.4  get_historical – point-in-time join for training data
"""

import logging
import subprocess
import sys
import os
from datetime import datetime, timedelta

import pandas as pd
from feast import FeatureStore

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))
from configs.settings import FEAST_REPO_PATH
from src.feature_store.lineage import (
    lineage_run,
    logical_output,
    pg_input,
    pg_output,
    redis_output,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s  %(levelname)s  %(message)s")
logger = logging.getLogger(__name__)


def get_store(repo_path: str | None = None) -> FeatureStore:
    return FeatureStore(repo_path=repo_path or FEAST_REPO_PATH)


# ── 2.2  feast apply ────────────────────────────────────────────────────
def apply(repo_path: str | None = None) -> None:
    """Run `feast apply` to register metadata in the Feast registry."""
    path = repo_path or FEAST_REPO_PATH
    logger.info("Running feast apply on %s", path)
    with lineage_run(
        "feast.apply",
        outputs=[pg_output("feast_registry")],
    ):
        subprocess.check_call(["feast", "apply"], cwd=path)
    logger.info("feast apply completed")


# ── 2.3  Materialization ────────────────────────────────────────────────
def materialize(
    repo_path: str | None = None,
    start: datetime | None = None,
    end: datetime | None = None,
) -> None:
    """
    Materialize features from the offline store (PostgreSQL) into the
    online store (Redis) for the given time window.
    """
    store = get_store(repo_path)
    end = end or datetime.utcnow()
    start = start or (end - timedelta(days=1000))
    logger.info("Materializing features  %s → %s", start.isoformat(), end.isoformat())
    with lineage_run(
        "feast.materialize",
        inputs=[pg_input("customer_features")],
        outputs=[redis_output("customer_features_view")],
    ):
        store.materialize(start_date=start, end_date=end)
    logger.info("Materialization complete – online store updated")


# ── 2.4  Training dataset retrieval ─────────────────────────────────────
def get_historical_features(
    entity_df: pd.DataFrame,
    features: list[str] | None = None,
    repo_path: str | None = None,
) -> pd.DataFrame:
    """
    Point-in-time join via Feast to avoid data leakage.

    Parameters
    ----------
    entity_df : must contain `entity_id` and `event_timestamp` columns.
    features  : list of "feature_view:feature" strings.

    Returns
    -------
    Training DataFrame with joined features.
    """
    store = get_store(repo_path)

    if features is None:
        features = [
            "customer_features_view:tenure_months",
            "customer_features_view:monthly_charges",
            "customer_features_view:total_charges",
            "customer_features_view:num_support_tickets",
            "customer_features_view:contract_type",
            "customer_features_view:internet_service",
            "customer_features_view:payment_method",
        ]

    logger.info("Fetching historical features for %d entities", len(entity_df))
    with lineage_run(
        "feast.get_historical_features",
        inputs=[pg_input("customer_features")],
        outputs=[logical_output("customer_features_training_dataset")],
    ):
        training_df = store.get_historical_features(
            entity_df=entity_df,
            features=features,
        ).to_df()

    logger.info("Historical features retrieved – shape %s", training_df.shape)
    return training_df


# ── CLI entry-point ─────────────────────────────────────────────────────
if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("action", choices=["apply", "materialize", "historical"])
    args = parser.parse_args()

    if args.action == "apply":
        apply()
    elif args.action == "materialize":
        materialize()
    elif args.action == "historical":
        from sqlalchemy import create_engine, text
        from configs.settings import PG_URL, WAREHOUSE_TABLE

        engine = create_engine(PG_URL)
        with engine.connect() as conn:
            entity_df = pd.read_sql(
                text(f"SELECT entity_id, event_timestamp FROM {WAREHOUSE_TABLE}"),
                conn,
            )
        entity_df["event_timestamp"] = pd.to_datetime(entity_df["event_timestamp"], utc=True)
        df = get_historical_features(entity_df)
        print(df.head())
