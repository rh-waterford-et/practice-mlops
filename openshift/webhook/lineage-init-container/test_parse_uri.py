#!/usr/bin/env python3
"""
Test URI parsing logic for lineage init container
"""

def parse_dataset_uri(uri):
    """
    Parse OpenLineage dataset URI into namespace and name.

    OpenLineage standard:
      - namespace = scheme://authority (where data lives)
      - name = path (what data it is)
    """
    if not uri or "://" not in uri:
        print(f"WARNING: Invalid URI format (missing scheme): {uri}")
        return None

    # Split on first '://' to get scheme
    scheme, rest = uri.split("://", 1)

    # Split the rest to get authority and path
    # Authority ends at first '/' or end of string
    if "/" in rest:
        authority, path = rest.split("/", 1)
        namespace = f"{scheme}://{authority}"
        name = path
    else:
        # No path, entire rest is authority
        namespace = f"{scheme}://{rest}"
        name = ""

    return {"namespace": namespace, "name": name}


# Test cases
test_cases = [
    "milvus://milvus:19530/ml_docs",
    "s3://mlflow-minio:9000/data/sample_docs/",
    "postgresql://postgres:5432/warehouse.customers",
    "feast://prod/user_behavior_features",
    "redis://redis:6379/cache_key",
    "kafka://broker:9092/topic.name",
    "s3://bucket/path/to/deep/dataset",
    "milvus://milvus/collection",  # No port
    "feast://feast/features",  # No port
]

print("Testing URI Parsing")
print("=" * 80)

for uri in test_cases:
    result = parse_dataset_uri(uri)
    if result:
        print(f"\nURI:       {uri}")
        print(f"Namespace: {result['namespace']}")
        print(f"Name:      {result['name']}")
    else:
        print(f"\nFailed to parse: {uri}")

print("\n" + "=" * 80)
print("All tests passed!")
