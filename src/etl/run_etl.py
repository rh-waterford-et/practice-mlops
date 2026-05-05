"""
STAGE 1  –  End-to-end ETL orchestrator (pandas / MinIO client → PostgreSQL).

Used by ``scripts/run_all.sh`` and local jobs. For OpenShift / KFP, the
platform step runs ``spark_etl.py`` (PySpark + OpenLineage listener) instead;
transform logic is intentionally parallel, not shared, between the two stacks.
"""

import logging
import sys
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parents[2]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from configs.settings import (
    MINIO_ACCESS_KEY,
    MINIO_BUCKET,
    MINIO_ENDPOINT,
    MINIO_SECRET_KEY,
    MINIO_SECURE,
    PG_URL,
    RAW_CSV_OBJECT,
    WAREHOUSE_TABLE,
)
from src.etl.extract import extract_from_minio
from src.etl.transform import transform
from src.etl.load import load_to_postgres

logging.basicConfig(level=logging.INFO, format="%(asctime)s  %(levelname)s  %(message)s")
logger = logging.getLogger(__name__)


def run() -> None:
    logger.info("═══ STAGE 1 – ETL START ═══")

    # ── Extract ──────────────────────────────────────────────────────────
    raw_df = extract_from_minio(
        endpoint=MINIO_ENDPOINT,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        bucket=MINIO_BUCKET,
        object_name=RAW_CSV_OBJECT,
        secure=MINIO_SECURE,
    )

    # ── Transform ────────────────────────────────────────────────────────
    clean_df = transform(raw_df)

    # ── Load ─────────────────────────────────────────────────────────────
    load_to_postgres(clean_df, PG_URL, WAREHOUSE_TABLE)

    logger.info("═══ STAGE 1 – ETL COMPLETE ═══")


if __name__ == "__main__":
    run()
