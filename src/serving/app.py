"""
STAGE 5  –  FastAPI online inference service.

Request flow
------------
1. Receive prediction request (entity_id list)
2. Fetch latest features from Feast online store (Redis)
3. Load model from MLflow Registry (Production stage)
4. Run prediction
5. Return results
"""

import logging
import os
from contextlib import asynccontextmanager
from typing import Any

import mlflow
import mlflow.sklearn
import numpy as np
import pandas as pd
from fastapi import FastAPI, HTTPException
from feast import FeatureStore
from pydantic import BaseModel, Field

logger = logging.getLogger(__name__)

# ── Configuration ────────────────────────────────────────────────────────
FEAST_REPO_PATH = os.getenv("FEAST_REPO_PATH", "src/feature_store")
MLFLOW_TRACKING_URI = os.getenv("MLFLOW_TRACKING_URI", "http://localhost:5000")
MODEL_NAME = os.getenv("MODEL_NAME", "customer_churn_model")

FEATURE_SERVICE_FEATURES = [
    "customer_features_view:tenure_months",
    "customer_features_view:monthly_charges",
    "customer_features_view:total_charges",
    "customer_features_view:num_support_tickets",
    "customer_features_view:contract_type",
    "customer_features_view:internet_service",
    "customer_features_view:payment_method",
]

NUMERIC_FEATURES = [
    "tenure_months",
    "monthly_charges",
    "total_charges",
    "num_support_tickets",
]

CATEGORICAL_FEATURES = ["contract_type", "internet_service", "payment_method"]

ALL_FEATURES = NUMERIC_FEATURES + CATEGORICAL_FEATURES

# ── Globals (populated on startup) ──────────────────────────────────────
_model = None
_store: FeatureStore | None = None


def _load_model():
    """Load the latest Production model from MLflow Registry."""
    global _model
    mlflow.set_tracking_uri(MLFLOW_TRACKING_URI)
    model_uri = f"models:/{MODEL_NAME}@champion"
    logger.info("Loading model from %s", model_uri)
    _model = mlflow.sklearn.load_model(model_uri)
    logger.info("Model loaded successfully")


def _init_feast():
    global _store
    _store = FeatureStore(repo_path=FEAST_REPO_PATH)
    logger.info("Feast store initialised (%s)", FEAST_REPO_PATH)


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Startup: load model + Feast store; Shutdown: cleanup."""
    logging.basicConfig(level=logging.INFO)
    _init_feast()
    try:
        _load_model()
    except Exception as exc:
        logger.warning("Could not load model on startup: %s (will retry on first request)", exc)
    yield


app = FastAPI(
    title="Customer Churn Prediction API",
    version="1.0.0",
    lifespan=lifespan,
)


# ── Request / Response schemas ──────────────────────────────────────────
class PredictionRequest(BaseModel):
    entity_ids: list[int] = Field(..., min_length=1, description="List of customer entity IDs")


class EntityPrediction(BaseModel):
    entity_id: int
    churn_probability: float
    churn_prediction: int
    features: dict[str, Any]


class PredictionResponse(BaseModel):
    predictions: list[EntityPrediction]


class HealthResponse(BaseModel):
    status: str
    model_loaded: bool
    feast_connected: bool


# ── Helpers ──────────────────────────────────────────────────────────────
def _encode_features(feature_dict: dict) -> np.ndarray:
    """
    Encode a single entity's feature dict into the model's expected
    input vector.  Categorical columns are label-encoded with a
    deterministic mapping.
    """
    cat_mappings = {
        "contract_type": {"Month-to-month": 0, "One-year": 1, "Two-year": 2},
        "internet_service": {"DSL": 0, "Fiber optic": 1, "No": 2},
        "payment_method": {
            "Bank transfer": 0,
            "Credit card": 1,
            "Electronic check": 2,
            "Mailed check": 3,
        },
    }
    row = []
    for feat in NUMERIC_FEATURES:
        row.append(float(feature_dict.get(feat, 0.0) or 0.0))
    for feat in CATEGORICAL_FEATURES:
        val = str(feature_dict.get(feat, ""))
        row.append(float(cat_mappings.get(feat, {}).get(val, 0)))
    return np.array(row, dtype=np.float32)


# ── Endpoints ────────────────────────────────────────────────────────────
@app.get("/health", response_model=HealthResponse)
async def health():
    return HealthResponse(
        status="ok",
        model_loaded=_model is not None,
        feast_connected=_store is not None,
    )


@app.post("/predict", response_model=PredictionResponse)
async def predict(request: PredictionRequest):
    """
    Online inference endpoint.

    1. Fetch features from Redis (Feast online store)
    2. Encode features
    3. Run XGBoost prediction
    4. Return results
    """
    global _model

    if _model is None:
        try:
            _load_model()
        except Exception as exc:
            raise HTTPException(
                status_code=503,
                detail=f"Model not available: {exc}",
            )

    if _store is None:
        raise HTTPException(status_code=503, detail="Feast store not initialised")

    # ── Fetch features from online store ─────────────────────────────
    entity_rows = [{"entity_id": eid} for eid in request.entity_ids]
    online_features = _store.get_online_features(
        features=FEATURE_SERVICE_FEATURES,
        entity_rows=entity_rows,
    ).to_dict()

    predictions: list[EntityPrediction] = []
    n = len(request.entity_ids)

    for i in range(n):
        feature_dict = {
            feat: online_features[feat][i] for feat in ALL_FEATURES
        }
        feature_dict["entity_id"] = online_features["entity_id"][i]

        x = _encode_features(feature_dict).reshape(1, -1)
        prob = float(_model.predict_proba(x)[0, 1])
        pred = int(prob >= 0.5)

        predictions.append(
            EntityPrediction(
                entity_id=feature_dict["entity_id"],
                churn_probability=round(prob, 4),
                churn_prediction=pred,
                features=feature_dict,
            )
        )

    return PredictionResponse(predictions=predictions)


@app.post("/reload-model")
async def reload_model():
    """Hot-reload the model from MLflow without restarting the service."""
    try:
        _load_model()
        return {"status": "reloaded"}
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))
