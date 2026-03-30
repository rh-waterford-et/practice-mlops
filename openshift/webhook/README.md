# Intent-Based OpenLineage via Mutating Admission Webhook

## Overview

This is the **Platform Engineering** approach to data lineage. Developers declare their intent via annotations, and the OpenShift control plane automatically registers the lineage architecture.

### Key Benefits

1. **Separation of Concerns**: Developers focus on application logic, platform handles lineage
2. **No SDK Dependencies**: Application code doesn't need OpenLineage SDK
3. **Centralized Management**: Single initContainer used across all teams
4. **GitOps-Friendly**: Lineage is declared in YAML manifests
5. **Standards-Based**: Uses `OPENLINEAGE_NAMESPACE` environment variable

## Architecture

```
┌─────────────────────────────────────────────────────────┐
│  Developer declares intent via annotations              │
│                                                          │
│  metadata:                                               │
│    annotations:                                          │
│      ai.platform/lineage-enabled: "true"                │
│      ai.platform/input-vectorstore: "milvus.prod/docs"  │
│      ai.platform/output-model: "llama3-v1"              │
└─────────────────────────────────────────────────────────┘
                          ↓
┌─────────────────────────────────────────────────────────┐
│  Mutating Admission Webhook (OpenShift control plane)   │
│                                                          │
│  1. Intercepts Pod creation                             │
│  2. Injects lineage-registration initContainer          │
│  3. Adds Downward API volume for annotations            │
│  4. Injects OPENLINEAGE_NAMESPACE from pod namespace    │
└─────────────────────────────────────────────────────────┘
                          ↓
┌─────────────────────────────────────────────────────────┐
│  InitContainer runs before app starts                   │
│                                                          │
│  1. Reads annotations from /etc/podinfo/annotations     │
│  2. Parses ai.platform/* annotations                    │
│  3. Emits START & COMPLETE events to Marquez            │
│  4. Registers architectural lineage                     │
└─────────────────────────────────────────────────────────┘
                          ↓
┌─────────────────────────────────────────────────────────┐
│  Application container starts                           │
│  - NO OpenLineage code required                         │
│  - Focus purely on business logic                       │
└─────────────────────────────────────────────────────────┘
```

## Components

### 1. Lineage InitContainer

**Image**: `lineage-init-container:latest`

- Reads pod annotations via Downward API
- Parses `ai.platform/input-*` and `ai.platform/output-*` annotations
- Emits OpenLineage START/COMPLETE events
- Uses `OPENLINEAGE_NAMESPACE` from environment

### 2. Webhook Server

**Service**: `lineage-webhook.lineage.svc`

- Mutating admission webhook
- Injects initContainer when pod has label `lineage-enabled: "true"`
- Adds Downward API volume for annotations
- Injects environment variables (OPENLINEAGE_NAMESPACE, OPENLINEAGE_URL, etc.)

### 3. Label & Annotation Format

Use a **label** to enable the webhook, and **annotations** to declare datasets:

```yaml
metadata:
  labels:
    # Enable the webhook (MUST be a label, not annotation)
    lineage-enabled: "true"
  annotations:
    # Declare inputs using OpenLineage URI format: scheme://authority/path
    ai.platform/input-0: "milvus://milvus:19530/ml_docs"
    ai.platform/input-1: "feast://feast/user_behavior_features"
    ai.platform/input-2: "s3://mlflow-minio:9000/data/sample_docs/"

    # Declare outputs
    ai.platform/output-0: "s3://mlflow-minio:9000/models/llama3-70b-v1"
    ai.platform/output-1: "redis://redis:6379/inference_cache"
```

**OpenLineage Standard**:

Each dataset URI follows the OpenLineage specification:
- **Namespace** = `scheme://authority` (where the data lives)
- **Name** = `path` (what the data is)

**Examples**:

| URI | Namespace | Name |
|-----|-----------|------|
| `milvus://milvus:19530/ml_docs` | `milvus://milvus:19530` | `ml_docs` |
| `s3://bucket:9000/data/docs/` | `s3://bucket:9000` | `data/docs/` |
| `postgresql://postgres:5432/warehouse.customers` | `postgresql://postgres:5432` | `warehouse.customers` |
| `feast://prod/features` | `feast://prod` | `features` |

**Supported Schemes**:
- `s3://` - Object storage (MinIO, AWS S3)
- `milvus://` - Vector database
- `postgresql://`, `mysql://` - Relational databases
- `feast://` - Feature store
- `redis://` - Cache
- `kafka://` - Streaming
- Custom schemes for your systems

## Developer Experience

**Without Webhook** (inline SDK approach):
```python
from openlineage.client import OpenLineageClient
from openlineage.client.run import Dataset, RunEvent, RunState

client = OpenLineageClient(url="http://marquez")
client.emit(RunEvent(
    eventType=RunState.COMPLETE,
    job=Job(namespace="lineage", name="rag-inference"),
    inputs=[
        Dataset(namespace="milvus://milvus:19530", name="ml_docs"),
        Dataset(namespace="feast://feast", name="user_context_features"),
    ],
    outputs=[
        Dataset(namespace="s3://mlflow-minio:9000", name="models/llama3-v1"),
    ],
    ...
))
```

**With Webhook** (declarative label + annotations):
```yaml
metadata:
  labels:
    lineage-enabled: "true"
  annotations:
    ai.platform/input-0: "milvus://milvus:19530/ml_docs"
    ai.platform/input-1: "feast://feast/user_context_features"
    ai.platform/output-0: "s3://mlflow-minio:9000/models/llama3-v1"
```

**Result**: Same OpenLineage standard URIs, but declared in YAML instead of Python. Zero SDK dependencies!

## Setup

### Step 1: Build InitContainer Image

```bash
cd openshift/webhook/lineage-init-container
oc apply -f ../lineage-init-container-buildconfig.yaml
oc start-build lineage-init-container --from-dir=. --follow
```

### Step 2: Build Webhook Server Image

```bash
cd openshift/webhook/webhook-server
oc apply -f ../webhook-deployment.yaml
oc start-build lineage-webhook --from-dir=. --follow
```

### Step 3: Generate TLS Certificates

```bash
cd openshift/webhook
chmod +x generate-webhook-certs.sh
./generate-webhook-certs.sh lineage
```

**Save the CA Bundle output** - you'll need it in the next step.

### Step 4: Deploy Webhook Server

```bash
# The secret was created by the script in step 3
oc apply -f webhook-deployment.yaml

# Wait for webhook to be ready
oc wait --for=condition=available deployment/lineage-webhook -n lineage --timeout=60s
```

### Step 5: Create MutatingWebhookConfiguration

```bash
# Edit the template and replace CA_BUNDLE_PLACEHOLDER with the value from step 3
cp mutating-webhook-config.yaml.template mutating-webhook-config.yaml
# Edit mutating-webhook-config.yaml and paste the CA bundle

# Apply the configuration
oc apply -f mutating-webhook-config.yaml
```

### Step 6: Test with Example Deployment

```bash
oc apply -f examples/rag-inference-service.yaml

# Check that the initContainer was injected
oc get pod -n lineage -l app=rag-inference -o jsonpath='{.items[0].spec.initContainers[*].name}'
# Should show: lineage-registration

# Check the initContainer logs
oc logs -n lineage -l app=rag-inference -c lineage-registration
```

## Usage for Developers

### Example: RAG Inference Service

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: rag-service
  namespace: lineage
spec:
  template:
    metadata:
      labels:
        lineage-enabled: "true"
      annotations:
        ai.platform/input-0: "milvus://milvus:19530/ml_docs"
        ai.platform/input-1: "feast://feast/user_context_features"
        ai.platform/output-0: "s3://mlflow-minio:9000/models/llama3-70b-rag"
    spec:
      containers:
        - name: app
          image: my-rag-app:latest
          # No OpenLineage SDK needed!
```

### Example: Feature Store Materialization Job

```yaml
apiVersion: batch/v1
kind: Job
metadata:
  name: feast-materialize
  namespace: lineage
spec:
  template:
    metadata:
      labels:
        lineage-enabled: "true"
      annotations:
        ai.platform/input-0: "postgresql://postgres:5432/warehouse.feature_views"
        ai.platform/output-0: "redis://redis:6379/online_store"
    spec:
      containers:
        - name: materialize
          image: feast-runner:latest
```

## Environment Variables Injected

The webhook automatically injects these environment variables into the initContainer:

- `OPENLINEAGE_NAMESPACE`: Set to the pod's Kubernetes namespace
- `OPENLINEAGE_URL`: Set to `http://marquez` (configurable in webhook deployment)
- `POD_NAMESPACE`: Pod namespace (via Downward API)
- `POD_NAME`: Pod name (via Downward API)
- `OWNER_NAME`: Name of the owning resource (Deployment, StatefulSet, etc.)

## Compatibility

### Works With:
- ✅ Deployments
- ✅ StatefulSets
- ✅ Jobs
- ✅ InferenceServices (KServe)
- ✅ DaemonSets
- ✅ Any workload that creates Pods

### Not Applicable To:
- ❌ KFP pipeline components (use inline OpenLineage emitting)
- ❌ Spark jobs with built-in listener (already has OpenLineage integration)

## When to Use Each Approach

| Use Case | Approach | Reason |
|----------|----------|--------|
| Long-running services | Webhook | Architectural lineage, no code changes |
| Inference endpoints | Webhook | Declare dependencies via annotations |
| KFP pipeline steps | Inline emission | Per-step data transformation tracking |
| Spark jobs | Built-in listener | Spark already has OpenLineage support |
| Batch jobs | Webhook | Simple intent declaration |

## Verification

Check that lineage events are being emitted:

```bash
# View initContainer logs
oc logs -n lineage deployment/rag-inference-service -c lineage-registration

# Check Marquez UI
oc get route marquez-web -n lineage -o jsonpath='{.spec.host}'
# Browse to: https://<marquez-url> and search for your job name
```

## Troubleshooting

### Webhook not injecting initContainer

```bash
# Check webhook is running
oc get pods -n lineage -l app=lineage-webhook

# Check webhook logs
oc logs -n lineage deployment/lineage-webhook

# Verify MutatingWebhookConfiguration
oc get mutatingwebhookconfiguration lineage-webhook

# Check if annotation is correct
oc get pod <pod-name> -n lineage -o yaml | grep -A5 annotations
```

### InitContainer failing

```bash
# Check initContainer logs
oc logs <pod-name> -n lineage -c lineage-registration

# Verify Downward API volume is mounted
oc get pod <pod-name> -n lineage -o yaml | grep -A10 "name: podinfo"

# Check if Marquez is accessible
oc exec <pod-name> -n lineage -c lineage-registration -- curl http://marquez/api/v1/namespaces
```

### Certificate issues

```bash
# Regenerate certificates
cd openshift/webhook
./generate-webhook-certs.sh lineage

# Update the MutatingWebhookConfiguration with new CA bundle
```

## Further Reading

- [OpenLineage Specification](https://github.com/OpenLineage/OpenLineage/blob/main/spec/OpenLineage.md)
- [Kubernetes Admission Webhooks](https://kubernetes.io/docs/reference/access-authn-authz/extensible-admission-controllers/)
- [Downward API](https://kubernetes.io/docs/concepts/workloads/pods/downward-api/)
