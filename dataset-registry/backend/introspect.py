"""Auto-detect dataset schema from the physical source.

Supports:
  - S3/MinIO CSV files  (s3://bucket/path.csv)
  - PostgreSQL tables   (postgres://user:pass@host:port/db.schema.table)
"""

import csv
import io
import os
import re
from datetime import datetime
from urllib.parse import urlparse

S3_ENDPOINT = os.getenv("S3_ENDPOINT", "http://mlflow-minio:9000")
S3_ACCESS_KEY = os.getenv("S3_ACCESS_KEY", "minioadmin")
S3_SECRET_KEY = os.getenv("S3_SECRET_KEY", "minioadmin123")

SAMPLE_ROWS = 100


def introspect_schema(source: str) -> list[dict] | None:
    """Attempt to introspect column names and types from *source*.

    Returns a list of ``{"name": ..., "type": ...}`` dicts, or ``None``
    if the source type is unsupported or introspection fails.
    """
    cleaned = source.strip()
    try:
        if cleaned.startswith("s3://") or cleaned.startswith("s3a://"):
            return _introspect_s3_csv(cleaned)
        if cleaned.startswith("postgres://") or cleaned.startswith("postgresql://"):
            return _introspect_postgres(cleaned)
        if cleaned.startswith("jdbc:postgresql://"):
            return _introspect_postgres(cleaned.replace("jdbc:postgresql://", "postgresql://", 1))
    except Exception as exc:
        print(f"Schema introspection failed for {source}: {exc}")
    return None


def _introspect_s3_csv(uri: str) -> list[dict] | None:
    import boto3
    from botocore.config import Config as BotoConfig

    scheme_end = uri.index("://") + 3
    rest = uri[scheme_end:]
    slash = rest.index("/")
    bucket = rest[:slash]
    key = rest[slash + 1:]

    if not key.lower().endswith(".csv"):
        print(f"S3 introspection skipped: {key} is not a CSV file")
        return None

    client = boto3.client(
        "s3",
        endpoint_url=S3_ENDPOINT,
        aws_access_key_id=S3_ACCESS_KEY,
        aws_secret_access_key=S3_SECRET_KEY,
        config=BotoConfig(signature_version="s3v4"),
        region_name="us-east-1",
    )

    obj = client.get_object(Bucket=bucket, Key=key)
    body = obj["Body"].read().decode("utf-8")

    reader = csv.DictReader(io.StringIO(body))
    columns = reader.fieldnames
    if not columns:
        return None

    rows = []
    for i, row in enumerate(reader):
        rows.append(row)
        if i >= SAMPLE_ROWS:
            break

    return [{"name": col, "type": _infer_type(col, rows)} for col in columns]


def _infer_type(column: str, rows: list[dict]) -> str:
    """Infer a simple type string from sample values."""
    values = [r.get(column, "") for r in rows if r.get(column, "").strip()]
    if not values:
        return "STRING"

    if all(_is_int(v) for v in values):
        return "INTEGER"
    if all(_is_float(v) for v in values):
        return "FLOAT"
    if all(_is_timestamp(v) for v in values):
        return "TIMESTAMP"
    return "STRING"


def _is_int(v: str) -> bool:
    try:
        int(v)
        return True
    except ValueError:
        return False


def _is_float(v: str) -> bool:
    try:
        float(v)
        return True
    except ValueError:
        return False


_TS_PATTERNS = [
    re.compile(r"^\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}"),
    re.compile(r"^\d{2}/\d{2}/\d{4}"),
]


def _is_timestamp(v: str) -> bool:
    return any(p.match(v) for p in _TS_PATTERNS)


def _introspect_postgres(uri: str) -> list[dict] | None:
    import psycopg2

    parsed = urlparse(uri)
    host = parsed.hostname or "localhost"
    port = parsed.port or 5432
    user = parsed.username or "postgres"
    password = parsed.password or ""
    path = parsed.path.lstrip("/")

    parts = path.split(".")
    if len(parts) == 3:
        database, schema, table = parts
    elif len(parts) == 2:
        database, table = parts
        schema = "public"
    elif len(parts) == 1:
        database = parts[0]
        table = None
        schema = "public"
    else:
        print(f"Cannot parse Postgres path: {path}")
        return None

    if not table:
        print(f"No table specified in Postgres URI: {uri}")
        return None

    dsn = f"host={host} port={port} dbname={database} user={user} password={password}"
    conn = psycopg2.connect(dsn)
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT column_name, data_type FROM information_schema.columns "
                "WHERE table_schema = %s AND table_name = %s "
                "ORDER BY ordinal_position",
                (schema, table),
            )
            rows = cur.fetchall()
    finally:
        conn.close()

    if not rows:
        print(f"No columns found for {schema}.{table} in {database}")
        return None

    pg_type_map = {
        "integer": "INTEGER",
        "bigint": "BIGINT",
        "smallint": "SMALLINT",
        "numeric": "NUMERIC",
        "real": "FLOAT",
        "double precision": "DOUBLE",
        "boolean": "BOOLEAN",
        "character varying": "VARCHAR",
        "character": "CHAR",
        "text": "STRING",
        "timestamp with time zone": "TIMESTAMPTZ",
        "timestamp without time zone": "TIMESTAMP",
        "date": "DATE",
        "jsonb": "JSONB",
        "json": "JSON",
        "uuid": "UUID",
    }

    return [
        {"name": col, "type": pg_type_map.get(dtype, dtype.upper())}
        for col, dtype in rows
    ]
