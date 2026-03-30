#!/usr/bin/env python3
"""
OpenLineage Architecture Registration InitContainer

Reads pod annotations to discover inputs/outputs and registers
the workload's lineage architecture with the central collector.

Annotations use standard OpenLineage URI format:
  ai.platform/input-0: "milvus://prod:19530/ml_docs"
  ai.platform/output-0: "s3://mlflow-minio:9000/models/llama3-v1"

This script is injected by the Mutating Admission Webhook into
any pod with: ai.platform/lineage-enabled: "true"
"""

import os
import sys
from datetime import datetime, timezone
from uuid import uuid4, uuid5, NAMESPACE_DNS

from openlineage.client import OpenLineageClient
from openlineage.client.run import RunEvent, RunState, Run, Job, Dataset
from openlineage.client.facet import BaseFacet
from typing import List, Dict
from attr import define, field


@define
class Tag:
    """Tag class for key/value/source structure"""
    key: str
    value: str
    source: str


@define
class TagsRunFacet(BaseFacet):
    """Custom Tags Run Facet for pod metadata"""
    tags: List[Tag] = field(factory=list)

    _producer: str = field(default="https://github.com/OpenLineage/OpenLineage/tree/1.0.0/integration/spark")
    _schemaURL: str = field(default="https://openlineage.io/spec/facets/1-0-0/TagsRunFacet.json")


def parse_annotations():
    """Parse annotations from Downward API volume mount."""
    annotations_file = "/etc/podinfo/annotations/annotations"

    if not os.path.exists(annotations_file):
        print(f"ERROR: Annotations file not found: {annotations_file}")
        print("Ensure Downward API volume is mounted with pod annotations")
        sys.exit(1)

    inputs = []
    outputs = []

    # Read annotations file (format: key="value" one per line)
    with open(annotations_file, 'r') as f:
        for line in f:
            line = line.strip()
            if not line or '=' not in line:
                continue

            # Parse key="value" format
            key, value = line.split('=', 1)
            # Remove quotes from value
            value = value.strip('"')

            # Parse ai.platform/* annotations
            # Format: ai.platform/input-0, ai.platform/input-1, etc.
            if key.startswith("ai.platform/input-"):
                dataset = parse_dataset_uri(value)
                if dataset:
                    inputs.append(dataset)

            elif key.startswith("ai.platform/output-"):
                dataset = parse_dataset_uri(value)
                if dataset:
                    outputs.append(dataset)

    return inputs, outputs


def parse_dataset_uri(uri):
    """
    Parse OpenLineage dataset URI into namespace and name.

    OpenLineage standard:
      - namespace = scheme://authority (where data lives)
      - name = path (what data it is)

    Examples:
      - "milvus://milvus:19530/ml_docs"
        -> namespace="milvus://milvus:19530", name="ml_docs"

      - "s3://mlflow-minio:9000/data/sample_docs/"
        -> namespace="s3://mlflow-minio:9000", name="data/sample_docs/"

      - "postgresql://postgres:5432/warehouse.customers"
        -> namespace="postgresql://postgres:5432", name="warehouse.customers"

      - "feast://prod/user_behavior"
        -> namespace="feast://prod", name="user_behavior"
    """
    if not uri or "://" not in uri:
        print(f"WARNING: Invalid URI format (missing scheme): {uri}")
        return None

    # Split on first '://' to get scheme
    scheme, rest = uri.split("://", 1)
    
    if scheme=="feast":
        scheme = ""
    else:
        scheme+="://"

    # Split the rest to get authority and path
    # Authority ends at first '/' or end of string
    if "/" in rest:
        authority, path = rest.split("/", 1)
        namespace = f"{scheme}{authority}"
        name = path
    else:
        # No path, entire rest is authority
        namespace = f"{scheme}{rest}"
        name = ""

    return Dataset(namespace=namespace, name=name, facets={})


def register_lineage():
    """Register the workload's lineage architecture."""

    # 1. Discover identity from environment
    namespace = os.getenv("OPENLINEAGE_NAMESPACE", os.getenv("POD_NAMESPACE", "default"))
    job_name = os.getenv("OWNER_NAME", os.getenv("POD_NAME", "unknown"))
    openlineage_url = os.getenv("OPENLINEAGE_URL", "http://marquez")

    print("=" * 60)
    print("OpenLineage Architecture Registration")
    print("=" * 60)
    print(f"Job Namespace: {namespace}")
    print(f"Job Name: {job_name}")
    print(f"OpenLineage URL: {openlineage_url}")

    # 2. Parse annotations to discover inputs/outputs
    inputs, outputs = parse_annotations()

    print(f"\nDiscovered {len(inputs)} input(s):")
    for inp in inputs:
        print(f"  - {inp.namespace}/{inp.name}")

    print(f"\nDiscovered {len(outputs)} output(s):")
    for out in outputs:
        print(f"  - {out.namespace}/{out.name}")

    # 3. Emit START event (architecture registration)
    client = OpenLineageClient(url=openlineage_url)

    # Generate deterministic UUID from pod name (or random if not available)
    pod_name = os.getenv("POD_NAME")
    if pod_name:
        # Create UUID v5 from pod name (deterministic)
        run_id = str(uuid5(NAMESPACE_DNS, pod_name))
    else:
        # Fallback to random UUID
        run_id = str(uuid4())

    # Add pod name as a run facet for traceability
    run_facets = {}

    if pod_name:
        # Create tags using the custom Tag and TagsRunFacet classes
        tags_list = [
            Tag(key="pod_name", value=pod_name, source="KUBERNETES"),
            Tag(key="pod_namespace", value=os.getenv("POD_NAMESPACE", namespace), source="KUBERNETES")
        ]

        run_facets["nominalTime"] = {
            "_producer": "lineage-init-container/webhook",
            "_schemaURL": "https://openlineage.io/spec/facets/1-0-0/NominalTimeRunFacet.json",
            "nominalStartTime": datetime.now(timezone.utc).isoformat()
        }
        run_facets["tags"] = TagsRunFacet(tags=tags_list)

        print(f"\nRun Tags:")
        print(f"  pod_name: {pod_name}")
        print(f"  pod_namespace: {os.getenv('POD_NAMESPACE', namespace)}")

    start_event = RunEvent(
        eventType=RunState.START,
        eventTime=datetime.now(timezone.utc).isoformat(),
        run=Run(runId=run_id, facets=run_facets),
        job=Job(namespace=namespace, name=job_name, facets={}),
        inputs=inputs,
        outputs=outputs,
        producer="lineage-init-container/webhook",
    )

    try:
        client.emit(start_event)
        print(f"\n✓ START event emitted (run: {run_id})")
    except Exception as e:
        print(f"\n✗ Failed to emit START event: {e}")
        sys.exit(1)

    # 4. Immediately emit COMPLETE event (structural registration)
    complete_event = RunEvent(
        eventType=RunState.COMPLETE,
        eventTime=datetime.now(timezone.utc).isoformat(),
        run=Run(runId=run_id, facets=run_facets),
        job=Job(namespace=namespace, name=job_name, facets={}),
        inputs=inputs,
        outputs=outputs,
        producer="lineage-init-container/webhook",
    )

    try:
        client.emit(complete_event)
        print(f"✓ COMPLETE event emitted")
    except Exception as e:
        print(f"✗ Failed to emit COMPLETE event: {e}")
        sys.exit(1)

    print("\n" + "=" * 60)
    print("Architecture registered successfully")
    print("=" * 60)


if __name__ == "__main__":
    try:
        register_lineage()
    except Exception as e:
        print(f"\nFATAL ERROR: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
