"""
Spark ETL  –  MinIO (CSV) -> Transform -> PostgreSQL

Uses PySpark with the OpenLineage Spark listener for automatic lineage,
plus a bridge event to connect Spark's JDBC-derived dataset names to the
Feast namespace so they form one continuous chain in Marquez.
"""

import json
import os
import sys
from datetime import datetime, timezone
from urllib.request import Request, urlopen
from uuid import uuid4

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

NUMERIC_COLS = [
    "tenure_months",
    "monthly_charges",
    "total_charges",
    "num_support_tickets",
]

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "mlflow-minio:9000")
MINIO_BUCKET = os.getenv("MINIO_BUCKET", "raw-data")
RAW_CSV_OBJECT = os.getenv("RAW_CSV_OBJECT", "customers.csv")
AWS_ACCESS_KEY = os.getenv("AWS_ACCESS_KEY_ID", "minioadmin")
AWS_SECRET_KEY = os.getenv("AWS_SECRET_ACCESS_KEY", "minioadmin123")

PG_HOST = os.getenv("PG_HOST", "postgres")
PG_PORT = os.getenv("PG_PORT", "5432")
PG_USER = os.getenv("PG_USER", "feast")
PG_PASSWORD = os.getenv("PG_PASSWORD", "feast")
PG_DATABASE = os.getenv("PG_DATABASE", "warehouse")
WAREHOUSE_TABLE = os.getenv("WAREHOUSE_TABLE", "customer_features")

OPENLINEAGE_URL = os.getenv("OPENLINEAGE_URL", "http://marquez")
OPENLINEAGE_NAMESPACE = os.getenv("OPENLINEAGE_NAMESPACE", "churn-demo/customer_churn")


def create_spark_session() -> SparkSession:
    pg_jdbc_url = f"jdbc:postgresql://{PG_HOST}:{PG_PORT}/{PG_DATABASE}"

    return (
        SparkSession.builder
        .master("local[*]")
        .appName("churn_etl")
        .config("spark.jars", "/opt/spark/jars/postgresql.jar")
        .config("spark.hadoop.fs.s3a.endpoint", f"http://{MINIO_ENDPOINT}")
        .config("spark.hadoop.fs.s3a.access.key", AWS_ACCESS_KEY)
        .config("spark.hadoop.fs.s3a.secret.key", AWS_SECRET_KEY)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config("spark.driver.memory", "1g")
        .getOrCreate()
    )


def extract(spark: SparkSession) -> "DataFrame":
    path = f"s3a://{MINIO_BUCKET}/{RAW_CSV_OBJECT}"
    print(f"Reading {path}")
    return spark.read.option("header", "true").option("inferSchema", "true").csv(path)


def transform(df: "DataFrame") -> "DataFrame":
    before = df.count()

    # Deduplicate on entity_id, keeping first occurrence
    window = Window.partitionBy("entity_id").orderBy("event_timestamp")
    df = df.withColumn("_rn", F.row_number().over(window)).filter(F.col("_rn") == 1).drop("_rn")
    after = df.count()
    print(f"Dropped {before - after} duplicate rows")

    # Parse timestamp to proper type (already inferred, ensure UTC)
    df = df.withColumn("event_timestamp", F.to_utc_timestamp(F.col("event_timestamp"), "UTC"))

    # Cast numeric columns and fill nulls with median
    for col_name in NUMERIC_COLS:
        df = df.withColumn(col_name, F.col(col_name).cast(DoubleType()))

    for col_name in NUMERIC_COLS:
        median_val = df.approxQuantile(col_name, [0.5], 0.01)[0]
        null_count = df.filter(F.col(col_name).isNull()).count()
        if null_count > 0:
            df = df.fillna({col_name: median_val})
            print(f"Filled {col_name} nulls ({null_count}) with median {median_val:.2f}")

    df = df.withColumn("churn", F.col("churn").cast(IntegerType()))

    # Min-max normalise numeric columns
    for col_name in NUMERIC_COLS:
        col_min = df.agg(F.min(col_name)).collect()[0][0]
        col_max = df.agg(F.max(col_name)).collect()[0][0]
        if col_max - col_min > 0:
            df = df.withColumn(
                col_name,
                (F.col(col_name) - F.lit(col_min)) / F.lit(col_max - col_min),
            )
        else:
            df = df.withColumn(col_name, F.lit(0.0))
        print(f"Normalised {col_name} (min={col_min:.2f}, max={col_max:.2f})")

    print(f"Transform complete – {df.count()} rows, {len(df.columns)} cols")
    return df


def load(df: "DataFrame") -> None:
    jdbc_url = f"jdbc:postgresql://{PG_HOST}:{PG_PORT}/{PG_DATABASE}"
    props = {
        "user": PG_USER,
        "password": PG_PASSWORD,
        "driver": "org.postgresql.Driver",
    }
    print(f"Writing to {jdbc_url} table={WAREHOUSE_TABLE}")
    df.write.jdbc(jdbc_url, WAREHOUSE_TABLE, mode="overwrite", properties=props)
    print("Load complete")


def emit_bridge_event():
    """Emit an OpenLineage event that connects the Spark output dataset
    (auto-named by the JDBC URL) to the Feast namespace so the lineage
    chain is continuous in Marquez: CSV -> spark_etl -> customer_features_source."""
    run_id = str(uuid4())
    now = datetime.now(timezone.utc).isoformat()

    for event_type in ("START", "COMPLETE"):
        event = {
            "eventType": event_type,
            "eventTime": now,
            "run": {"runId": run_id, "facets": {}},
            "job": {
                "namespace": OPENLINEAGE_NAMESPACE,
                "name": "spark_etl",
                "facets": {},
            },
            "inputs": [
                {
                    "namespace": f"s3://{MINIO_BUCKET}",
                    "name": RAW_CSV_OBJECT,
                    "facets": {},
                }
            ],
            "outputs": [
                {
                    "namespace": OPENLINEAGE_NAMESPACE,
                    "name": "customer_features_source",
                    "facets": {},
                }
            ],
            "producer": "https://github.com/practice-mlops/spark-etl",
            "schemaURL": "https://openlineage.io/spec/2-0-2/OpenLineage.json#/$defs/RunEvent",
        }
        body = json.dumps(event).encode("utf-8")
        req = Request(
            f"{OPENLINEAGE_URL}/api/v1/lineage",
            data=body,
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        try:
            with urlopen(req) as resp:
                print(f"Bridge event {event_type}: HTTP {resp.status}")
        except Exception as e:
            print(f"Bridge event {event_type} failed: {e}")


def main():
    print("=== SPARK ETL START ===")
    spark = create_spark_session()
    try:
        raw = extract(spark)
        clean = transform(raw)
        load(clean)
        print("=== SPARK ETL COMPLETE ===")
    finally:
        spark.stop()

    emit_bridge_event()
    print("=== BRIDGE LINEAGE EMITTED ===")


if __name__ == "__main__":
    main()
