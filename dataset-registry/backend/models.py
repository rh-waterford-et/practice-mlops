from datetime import datetime
from typing import Optional
from urllib.parse import urlparse
from uuid import UUID

from pydantic import BaseModel, Field, model_validator


class SchemaField(BaseModel):
    name: str
    type: str


def parse_ol_identity(source: str) -> tuple[str, str]:
    """Derive the OpenLineage (namespace, name) from a physical source URI.

    Examples:
        postgres://host:5432/warehouse.public.customer_features
          -> ("postgres://host:5432", "warehouse.public.customer_features")
        s3://raw-data/customers.csv
          -> ("s3://raw-data", "customers.csv")
        jdbc:postgresql://host:5432/db
          -> ("postgres://host:5432", "db")
    """
    cleaned = source.strip()

    if cleaned.startswith("jdbc:postgresql://"):
        cleaned = cleaned.replace("jdbc:postgresql://", "postgres://", 1)
    elif cleaned.startswith("jdbc:"):
        cleaned = cleaned[5:]

    parsed = urlparse(cleaned)
    scheme = parsed.scheme or "unknown"
    host = parsed.hostname or "localhost"
    port = parsed.port

    namespace = f"{scheme}://{host}"
    if port:
        namespace += f":{port}"

    path = parsed.path.lstrip("/")
    name = path if path else parsed.netloc

    return namespace, name


class DatasetCreate(BaseModel):
    name: str = Field(..., min_length=1, examples=["Customer Features"])
    source: str = Field(
        ..., min_length=1,
        examples=["postgres://postgres:5432/warehouse.public.customer_features"],
    )
    description: Optional[str] = None
    schema_fields: Optional[list[SchemaField]] = None
    tags: Optional[list[str]] = None


class DatasetUpdate(BaseModel):
    name: Optional[str] = None
    description: Optional[str] = None
    schema_fields: Optional[list[SchemaField]] = None
    tags: Optional[list[str]] = None


class Dataset(BaseModel):
    id: UUID
    name: str
    source: str
    ol_namespace: str
    ol_name: str
    description: Optional[str] = None
    schema_fields: Optional[list[SchemaField]] = None
    tags: Optional[list[str]] = None
    created_at: datetime
    updated_at: datetime


class DatasetList(BaseModel):
    datasets: list[Dataset]
    total: int
