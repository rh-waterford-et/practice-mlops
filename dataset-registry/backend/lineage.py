"""Emit OpenLineage DatasetEvents to Marquez when datasets are registered.

Event structure follows openlineage-oai conventions.
"""

import json
import os
import urllib.request
from datetime import datetime, timezone

MARQUEZ_URL = os.getenv("MARQUEZ_URL", "http://marquez:80")

SCHEMA_BASE = "https://openlineage.io/spec/facets/1-0-0"
DATASET_EVENT_SCHEMA = (
    "https://openlineage.io/spec/2-0-0/OpenLineage.json#/definitions/DatasetEvent"
)
PRODUCER = "https://github.com/dataset-registry"


def emit_dataset_registered(ol_namespace: str, ol_name: str,
                            schema_fields: list[dict] | None = None,
                            description: str | None = None) -> bool:
    facets: dict = {}

    if schema_fields:
        facets["schema"] = {
            "_producer": PRODUCER,
            "_schemaURL": f"{SCHEMA_BASE}/SchemaDatasetFacet.json",
            "fields": schema_fields,
        }

    if description:
        facets["documentation"] = {
            "_producer": PRODUCER,
            "_schemaURL": f"{SCHEMA_BASE}/DocumentationDatasetFacet.json",
            "description": description,
        }

    event = {
        "eventType": "CREATE",
        "eventTime": datetime.now(timezone.utc).isoformat(),
        "producer": PRODUCER,
        "schemaURL": DATASET_EVENT_SCHEMA,
        "dataset": {
            "namespace": ol_namespace,
            "name": ol_name,
            "facets": facets,
        },
    }

    url = f"{MARQUEZ_URL}/api/v1/lineage"
    body = json.dumps(event).encode()
    req = urllib.request.Request(url, data=body, headers={"Content-Type": "application/json"})

    try:
        with urllib.request.urlopen(req, timeout=5) as resp:
            print(f"DatasetEvent CREATE sent: {resp.status} - {ol_namespace}/{ol_name}")
            return resp.status in (200, 201)
    except Exception as e:
        print(f"Failed to emit DatasetEvent: {e}")
        return False
