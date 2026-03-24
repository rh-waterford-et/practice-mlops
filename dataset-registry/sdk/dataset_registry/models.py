from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional
from uuid import UUID


@dataclass
class SchemaField:
    name: str
    type: str


@dataclass
class Dataset:
    id: UUID
    name: str
    source: str
    ol_namespace: str
    ol_name: str
    description: Optional[str] = None
    schema_fields: Optional[list[SchemaField]] = None
    tags: Optional[list[str]] = None
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None

    @classmethod
    def from_dict(cls, data: dict) -> "Dataset":
        sf = None
        if data.get("schema_fields"):
            sf = [SchemaField(**f) for f in data["schema_fields"]]
        return cls(
            id=UUID(data["id"]) if isinstance(data["id"], str) else data["id"],
            name=data["name"],
            source=data["source"],
            ol_namespace=data["ol_namespace"],
            ol_name=data["ol_name"],
            description=data.get("description"),
            schema_fields=sf,
            tags=data.get("tags"),
            created_at=data.get("created_at"),
            updated_at=data.get("updated_at"),
        )

    @property
    def openlineage_node_id(self) -> str:
        return f"dataset:{self.ol_namespace}:{self.ol_name}"
