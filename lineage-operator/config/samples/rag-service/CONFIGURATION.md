# RAG Service Configuration Guide

## Required Configuration Files

### 1. Feast Configuration: `feature_store.yaml`

Located at: `feast_config/feature_store.yaml`

```yaml
project: customer_churn
provider: local

registry:
  registry_type: sql
  path: postgresql://feast:feast@postgres:5432/warehouse
  cache_ttl_seconds: 60

online_store:
  type: redis
  connection_string: redis:6379

entity_key_serialization_version: 2
```

**Deployment**: This file is mounted via ConfigMap at `/app/feast_repo/feature_store.yaml`

```bash
# Create ConfigMap
oc apply -f ../feast-config-configmap.yaml

# ConfigMap is automatically mounted by the deployment
```

### 2. Environment Variables

Set in deployment manifest (`rag-inference-service.yaml`):

```yaml
env:
  # Milvus Configuration
  - name: MILVUS_HOST
    value: "milvus"
  - name: MILVUS_PORT
    value: "19530"
  - name: MILVUS_COLLECTION
    value: "ml_docs"

  # Feast Configuration
  - name: FEAST_REPO_PATH
    value: "/app/feast_repo"  # Points to mounted ConfigMap

  # Model Configuration
  - name: MODEL_NAME
    value: "llama3-70b-rag-v1"
  - name: TOP_K_DOCUMENTS
    value: "3"
```

## Configuration Precedence

1. **ConfigMap Mount** (Recommended for production)
   - Mount `feast-config` ConfigMap to `/app/feast_repo`
   - Allows updating Feast config without rebuilding image

2. **Baked-in Default** (For testing)
   - `feast_config/feature_store.yaml` copied to image at build time
   - Used if no ConfigMap is mounted

## External Service Dependencies

The service requires these external systems to be running:

### PostgreSQL (Feast Registry)
```yaml
Host: postgres
Port: 5432
Database: warehouse
User: feast
Password: feast
```

### Redis (Feast Online Store)
```yaml
Host: redis
Port: 6379
```

### Milvus (Vector Database)
```yaml
Host: milvus
Port: 19530
Collection: ml_docs
```

## Verifying Configuration

Once deployed, check configuration via the `/config` endpoint:

```bash
ROUTE=$(oc get route rag-inference -n lineage -o jsonpath='{.spec.host}')
curl https://$ROUTE/config | jq
```

Expected response:
```json
{
  "milvus": {
    "host": "milvus",
    "port": 19530,
    "collection": "ml_docs",
    "connected": true,
    "documents": 42
  },
  "feast": {
    "repo_path": "/app/feast_repo",
    "config": {
      "project": "customer_churn",
      "provider": "local",
      "registry": {
        "registry_type": "sql",
        "path": "postgresql://feast:feast@postgres:5432/warehouse",
        "cache_ttl_seconds": 60
      },
      "online_store": {
        "type": "redis",
        "connection_string": "redis:6379"
      },
      "entity_key_serialization_version": 2
    },
    "initialized": true
  },
  "model": {
    "name": "llama3-70b-rag-v1",
    "top_k_documents": 3
  }
}
```

## Updating Configuration

### To update Feast config:

```bash
# Edit the ConfigMap
oc edit configmap feast-config -n lineage

# Restart pods to pick up changes
oc rollout restart deployment/rag-inference-service -n lineage
```

### To update environment variables:

```bash
# Edit the deployment
oc edit deployment rag-inference-service -n lineage

# Pods automatically restart with new values
```

## Troubleshooting

### Feast initialization fails

Check if feature_store.yaml is mounted:
```bash
oc exec -n lineage deployment/rag-inference-service -- cat /app/feast_repo/feature_store.yaml
```

### Can't connect to PostgreSQL

Check if postgres service is accessible:
```bash
oc exec -n lineage deployment/rag-inference-service -- \
  curl -v telnet://postgres:5432
```

### Can't connect to Redis

Check if redis service is accessible:
```bash
oc exec -n lineage deployment/rag-inference-service -- \
  curl -v telnet://redis:6379
```

### Can't connect to Milvus

Check if milvus service is accessible:
```bash
oc exec -n lineage deployment/rag-inference-service -- \
  curl -v telnet://milvus:19530
```
