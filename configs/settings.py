"""
Centralised configuration – all service endpoints and credentials in one place.
Values fall back to environment variables so the same code works inside
Docker Compose and on a local dev machine.
"""

import os

# ── MinIO / S3 ──────────────────────────────────────────────────────────
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "localhost:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")
MINIO_BUCKET = os.getenv("MINIO_BUCKET", "raw-data")
MINIO_SECURE = os.getenv("MINIO_SECURE", "false").lower() == "true"

# ── PostgreSQL ──────────────────────────────────────────────────────────
PG_HOST = os.getenv("PG_HOST", "localhost")
PG_PORT = int(os.getenv("PG_PORT", "5432"))
PG_USER = os.getenv("PG_USER", "feast")
PG_PASSWORD = os.getenv("PG_PASSWORD", "feast")
PG_DATABASE = os.getenv("PG_DATABASE", "warehouse")
PG_URL = (
    f"postgresql://{PG_USER}:{PG_PASSWORD}@{PG_HOST}:{PG_PORT}/{PG_DATABASE}"
)

# ── Redis ───────────────────────────────────────────────────────────────
REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))

# ── MLflow ──────────────────────────────────────────────────────────────
MLFLOW_TRACKING_URI = os.getenv("MLFLOW_TRACKING_URI", "http://localhost:5000")
MLFLOW_S3_ENDPOINT_URL = os.getenv("MLFLOW_S3_ENDPOINT_URL", "http://localhost:9000")
MLFLOW_EXPERIMENT_NAME = os.getenv("MLFLOW_EXPERIMENT_NAME", "customer_churn")
MODEL_NAME = os.getenv("MODEL_NAME", "customer_churn_model")

# ── Feast ───────────────────────────────────────────────────────────────
FEAST_REPO_PATH = os.getenv("FEAST_REPO_PATH", "src/feature_store")

# ── OpenLineage ──────────────────────────────────────────────────────────
# Leave OPENLINEAGE_URL unset (or empty) to disable lineage emission entirely.
OPENLINEAGE_URL = os.getenv("OPENLINEAGE_URL", "")
OPENLINEAGE_NAMESPACE = os.getenv("OPENLINEAGE_NAMESPACE", "feast")

# ── Data ────────────────────────────────────────────────────────────────
RAW_CSV_OBJECT = os.getenv("RAW_CSV_OBJECT", "customers.csv")
WAREHOUSE_TABLE = "customer_features"
TARGET_COLUMN = "churn"
ENTITY_COLUMN = "entity_id"
TIMESTAMP_COLUMN = "event_timestamp"

# ── Thresholds ──────────────────────────────────────────────────────────
MODEL_ROC_AUC_THRESHOLD = float(os.getenv("MODEL_ROC_AUC_THRESHOLD", "0.75"))
