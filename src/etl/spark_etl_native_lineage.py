"""
Spark ETL  –  MinIO (CSV) -> Transform -> PostgreSQL

Uses PySpark with the **native** OpenLineage Spark listener for automatic lineage.
All lineage events are automatically captured by the Spark listener - no manual
event emission needed!

Requirements:
  pip install openlineage-spark

The openlineage-spark JAR must be available in Spark's classpath.
"""

import os
from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, IntegerType

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

OPENLINEAGE_URL = os.getenv("OPENLINEAGE_URL", "http://marquez.lineage.svc")
OPENLINEAGE_NAMESPACE = os.getenv("OPENLINEAGE_NAMESPACE", "spark-etl")


def create_spark_session() -> SparkSession:
    """
    Creates a Spark session with native OpenLineage integration.

    The OpenLineage listener automatically captures:
    - All data reads (CSV, Parquet, JDBC, etc.)
    - All data writes (JDBC, Parquet, etc.)
    - Schema information
    - Job metadata
    - Execution statistics
    """
    pg_jdbc_url = f"jdbc:postgresql://{PG_HOST}:{PG_PORT}/{PG_DATABASE}"

    # Debug: Print OpenLineage configuration
    print(f"[DEBUG] OpenLineage URL: {OPENLINEAGE_URL}")
    print(f"[DEBUG] OpenLineage Namespace: {OPENLINEAGE_NAMESPACE}")

    # Check if JARs exist
    import os
    jars = ["/opt/spark/jars/openlineage-spark.jar", "/opt/spark/jars/postgresql.jar"]
    for jar in jars:
        exists = os.path.exists(jar)
        print(f"[DEBUG] JAR {jar}: {'EXISTS' if exists else 'MISSING'}")

    return (
        SparkSession.builder
        .master("local[*]")
        .appName("churn_etl")

        # Standard Spark configs
        .config("spark.jars", "/opt/spark/jars/postgresql.jar,/opt/spark/jars/openlineage-spark.jar")
        .config("spark.driver.extraClassPath", "/opt/spark/jars/openlineage-spark.jar:/opt/spark/jars/postgresql.jar")
        .config("spark.hadoop.fs.s3a.endpoint", f"http://{MINIO_ENDPOINT}")
        .config("spark.hadoop.fs.s3a.access.key", AWS_ACCESS_KEY)
        .config("spark.hadoop.fs.s3a.secret.key", AWS_SECRET_KEY)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config("spark.driver.memory", "1g")

        # Enable INFO logging to see OpenLineage output
        .config("spark.log.level", "INFO")

        # ===================================================================
        # OpenLineage Native Integration
        # ===================================================================
        # Enable the OpenLineage Spark listener
        .config("spark.extraListeners", "io.openlineage.spark.agent.OpenLineageSparkListener")

        # Configure OpenLineage HTTP transport
        .config("spark.openlineage.transport.type", "http")
        .config("spark.openlineage.transport.url", OPENLINEAGE_URL)

        # Set the namespace for lineage events
        .config("spark.openlineage.namespace", OPENLINEAGE_NAMESPACE)

        # Transform s3a:// scheme to s3:// in dataset URIs (OpenLineage 1.18+)
        .config("spark.openlineage.transport.urlParams.replaceDatasetNamespacePattern", "s3a://->s3://")

        # Optional: Capture full column-level lineage (slower but more detailed)
        # .config("spark.openlineage.facets.columnLineage.enabled", "true")

        # Optional: Disable certain facets if needed
        # .config("spark.openlineage.facets.spark_version.disabled", "true")

        # Optional: Custom facets or tags
        # .config("spark.openlineage.facets.custom.pipeline", "customer_churn")
        # .config("spark.openlineage.facets.custom.env", "production")

        .getOrCreate()
    )


def extract(spark: SparkSession) -> "DataFrame":
    """
    Read CSV from MinIO/S3.

    OpenLineage automatically captures:
    - Input dataset: s3a://raw-data/customers.csv
    - Schema of the CSV file
    - Number of partitions read
    """
    path = f"s3a://{MINIO_BUCKET}/{RAW_CSV_OBJECT}"
    print(f"Reading {path}")
    return spark.read.option("header", "true").option("inferSchema", "true").csv(path)


def transform(df: "DataFrame") -> "DataFrame":
    """
    Transform the data: deduplicate, parse timestamps, normalize.

    OpenLineage automatically captures:
    - Column transformations
    - Schema changes
    - Data quality metrics (if enabled)
    """
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
    """
    Write to PostgreSQL.

    OpenLineage automatically captures:
    - Output dataset: jdbc:postgresql://postgres:5432/warehouse.customer_features
    - Output schema
    - Number of rows written
    - Write mode (overwrite)
    """
    jdbc_url = f"jdbc:postgresql://{PG_HOST}:{PG_PORT}/{PG_DATABASE}"
    props = {
        "user": PG_USER,
        "password": PG_PASSWORD,
        "driver": "org.postgresql.Driver",
    }
    print(f"Writing to {jdbc_url} table={WAREHOUSE_TABLE}")
    df.write.jdbc(jdbc_url, WAREHOUSE_TABLE, mode="overwrite", properties=props)
    print("Load complete")

def main():
    """
    Main ETL pipeline.

    With native OpenLineage integration, lineage events are automatically emitted:

    1. JOB START event when SparkSession starts
    2. INPUT dataset event when CSV is read
    3. OUTPUT dataset event when JDBC write happens
    4. JOB COMPLETE event when SparkSession stops

    As a fallback, we also emit manual bridge events to ensure lineage is captured.
    """
    print("=== SPARK ETL START (Native OpenLineage) ===")
    spark = create_spark_session()

    # Debug: Verify OpenLineage configuration was applied
    ol_url = spark.sparkContext.getConf().get("spark.openlineage.transport.url", "NOT_SET")
    ol_ns = spark.sparkContext.getConf().get("spark.openlineage.namespace", "NOT_SET")
    ol_listener = spark.sparkContext.getConf().get("spark.extraListeners", "NOT_SET")
    print(f"[DEBUG] Spark Config - OpenLineage URL: {ol_url}")
    print(f"[DEBUG] Spark Config - OpenLineage Namespace: {ol_ns}")
    print(f"[DEBUG] Spark Config - Listeners: {ol_listener}")

    try:
        raw = extract(spark)
        clean = transform(raw)
        load(clean)
        print("=== SPARK ETL COMPLETE ===")
    finally:
        # OpenLineage automatically emits COMPLETE event on spark.stop()
        spark.stop()
        print("=== Native OpenLineage listener completed ===")

    # Fallback: Emit manual bridge events to ensure lineage is tracked
    # Temporarily disabled to test native OpenLineage listener
    # print("\n=== Emitting manual bridge events (fallback) ===")
    # emit_manual_bridge_event()
    # print("=== Manual bridge events sent ===")


if __name__ == "__main__":
    main()
