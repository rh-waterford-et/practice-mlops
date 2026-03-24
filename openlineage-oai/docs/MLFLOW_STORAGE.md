# MLflow Storage Architecture

This document explains how MLflow stores data and why artifact storage configuration is important when using the OpenLineage plugin.

## MLflow Has Two Storage Systems

```
MLflow Server
┌─────────────────────────────────────────────────────────┐
│                                                         │
│  1. TRACKING DATABASE (PostgreSQL, MySQL, SQLite)       │
│     └── Stores: run metadata, params, metrics, tags     │
│         (Small data - text and numbers)                 │
│                                                         │
│  2. ARTIFACT STORAGE (File System)                      │
│     └── Stores: models, plots, data files, logs         │
│         (Large data - actual files, can be GBs)         │
│                                                         │
└─────────────────────────────────────────────────────────┘
```

### Tracking Database

Stores **metadata** about your ML runs:
- Run IDs, experiment IDs, timestamps
- Parameters (`mlflow.log_param("lr", 0.01)`)
- Metrics (`mlflow.log_metric("accuracy", 0.95)`)
- Tags (`mlflow.set_tag("model_type", "sklearn")`)

This is small, structured data that fits well in a relational database.

### Artifact Storage

Stores **files** you create during training:
- Trained models (`.pkl`, `.pt`, `.h5`, `.onnx`)
- Plots and visualizations (`.png`, `.html`)
- Data samples (`.csv`, `.parquet`)
- Logs, configs, any other files

This can be large (GBs for big models) and needs file storage.

---

## What is an "Artifact"?

An artifact is any **file** you save during an MLflow run:

```python
# Log individual files
mlflow.log_artifact("confusion_matrix.png")
mlflow.log_artifact("config.yaml")

# Log entire directories
mlflow.log_artifacts("./output_plots/")

# Log models (creates a directory with model files)
mlflow.sklearn.log_model(model, "model")
mlflow.pytorch.log_model(model, "pytorch_model")
```

---

## Where Do Artifacts Go?

### Option 1: Local File System

```
mlruns/
└── 0/                          # Experiment ID
    └── abc123/                 # Run ID
        └── artifacts/
            ├── model/
            │   ├── model.pkl
            │   └── MLmodel
            └── plots/
                └── accuracy.png
```

Simple but doesn't scale for teams.

### Option 2: Cloud Storage (S3, GCS, Azure Blob)

```
s3://my-bucket/mlflow-artifacts/
└── 0/
    └── abc123/
        └── artifacts/
            └── model/
```

Scalable, durable, accessible by team.

### Option 3: MLflow Artifact Proxy (`mlflow-artifacts://`)

```
Your Script ───► MLflow Server ───► Server's Storage
                 (proxy upload)
```

The client uploads to the MLflow server, which then stores the files. The client doesn't need direct access to storage.

---

## The `mlflow-artifacts://` Proxy Explained

When using a **remote MLflow tracking server**, you have a choice:

### Direct Upload (Client → Storage)

```
┌──────────┐                           ┌─────────────┐
│  Client  │ ─────── Upload ─────────► │  S3 Bucket  │
│  Script  │                           │             │
└──────────┘                           └─────────────┘
     │
     │ Metadata only
     ▼
┌─────────────┐
│   MLflow    │
│   Server    │
└─────────────┘
```

**Requires:** Client has S3 credentials (AWS_ACCESS_KEY_ID, etc.)

### Proxy Upload (Client → Server → Storage)

```
┌──────────┐         ┌─────────────┐         ┌─────────────┐
│  Client  │ ──────► │   MLflow    │ ──────► │  S3 Bucket  │
│  Script  │  HTTP   │   Server    │         │             │
└──────────┘         └─────────────┘         └─────────────┘
                           │
                     Server has
                     S3 credentials
```

**Requires:** Only the server needs credentials. Client just talks HTTP to the server.

This proxy mode uses the `mlflow-artifacts://` URI scheme.

---

## How the Proxy Works

```
1. Client: mlflow.sklearn.log_model(model, "model")

2. MLflow checks: "Where should artifacts go?"
   └── Artifact URI = mlflow-artifacts://<tracking-server>/api/2.0/mlflow-artifacts/artifacts

3. MLflow: "I need to proxy through the tracking server"
   └── POST https://<tracking-server>/api/2.0/mlflow-artifacts/artifacts/model/model.pkl
   
4. Server receives the file and stores it in configured backend (S3, local, etc.)
```

---

## The Problem with Our Plugin

Our OpenLineage plugin uses a URI prefix: `openlineage+http://mlflow-server`

When MLflow tries to use the artifact proxy, it validates the tracking URI:

```python
# MLflow's internal validation (simplified)
def validate_tracking_uri_for_proxy(tracking_uri):
    if not tracking_uri.startswith(("http://", "https://")):
        raise MlflowException(
            "Tracking URI must be HTTP/HTTPS for artifact proxy"
        )

# Our URI: "openlineage+http://server"
# MLflow sees: "openlineage+http://..." 
# MLflow says: "That doesn't start with http:// !" → FAILS
```

MLflow doesn't know to strip our `openlineage+` prefix before validation.

---

## Real-World Impact

### Scenario: User with "Vanilla" MLflow Server

```python
# User just sets up a basic MLflow server with PostgreSQL
# No S3 configured, using default proxy mode

os.environ["MLFLOW_TRACKING_URI"] = "openlineage+http://mlflow-server"
mlflow.sklearn.log_model(model, "model")  # ← FAILS
```

**Error:** `"Invalid tracking URI for artifact proxy"`

The model files can't be saved because MLflow's proxy validation doesn't understand our prefix.

### Scenario: Production MLflow Server

```python
# MLflow server has artifact storage configured (S3, or local path)
# Server sets default_artifact_root

os.environ["MLFLOW_TRACKING_URI"] = "openlineage+http://mlflow-server"
mlflow.sklearn.log_model(model, "model")  # ← WORKS
```

Works because artifacts go directly to configured storage, bypassing the proxy validation.

### Who Is Affected?

| User Type | Typical Setup | Impact |
|-----------|--------------|--------|
| **Enterprise** | S3/GCS artifact storage, IAM roles | ✅ No impact |
| **Hobbyist/Student** | `mlflow server` with defaults | ❌ Needs extra config |
| **Tutorial follower** | Docker compose, minimal setup | ❌ Needs extra config |

**Bottom line:** Production deployments are fine. Quick-start/demo scenarios need explicit artifact configuration.

---

## Solutions

### Solution 1: Configure Artifact Storage Explicitly

Don't rely on the proxy. Configure artifact storage directly:

```python
# At experiment creation
mlflow.create_experiment(
    "my-experiment",
    artifact_location="s3://my-bucket/mlflow-artifacts"
)
```

Or server-side:
```yaml
# MLflow server config
default_artifact_root: s3://my-bucket/mlflow-artifacts
```

### Solution 2: Use Local Storage (Development Only)

```python
mlflow.create_experiment(
    "my-experiment", 
    artifact_location="/tmp/mlflow-artifacts"
)
```

### Solution 3: Fix MLflow (Upstream)

File an issue/PR with MLflow to handle plugin URI prefixes in artifact proxy validation.

---

## Recommended Production Setup

```
┌─────────────────────────────────────────────────────────────────┐
│                     Production Architecture                      │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│   Client                                                         │
│   ┌──────────────────────────────────────────────────────────┐  │
│   │  MLFLOW_TRACKING_URI=openlineage+http://mlflow-server    │  │
│   │  OPENLINEAGE_URL=http://marquez                          │  │
│   │  OPENLINEAGE_NAMESPACE=ml-platform                       │  │
│   │                                                          │  │
│   │  # Artifact storage configured at server level           │  │
│   │  # Client doesn't need S3 credentials                    │  │
│   └──────────────────────────────────────────────────────────┘  │
│                              │                                   │
│              ┌───────────────┴───────────────┐                  │
│              ▼                               ▼                  │
│   ┌─────────────────┐             ┌─────────────────┐          │
│   │  MLflow Server  │             │     Marquez     │          │
│   │                 │             │  (OpenLineage)  │          │
│   │  • Tracking DB  │             │                 │          │
│   │  • S3 artifacts │             │  • Lineage DB   │          │
│   │    (server has  │             │                 │          │
│   │     IAM role)   │             │                 │          │
│   └─────────────────┘             └─────────────────┘          │
│              │                                                  │
│              ▼                                                  │
│   ┌─────────────────┐                                          │
│   │   S3 Bucket     │                                          │
│   │   (artifacts)   │                                          │
│   └─────────────────┘                                          │
│                                                                  │
└─────────────────────────────────────────────────────────────────┘
```

**Key point:** Configure artifact storage at the MLflow server level so clients don't need storage credentials. The OpenLineage plugin handles lineage separately.

---

## Summary

| Component | What It Stores | Where |
|-----------|---------------|-------|
| Tracking Database | Params, metrics, tags, run metadata | PostgreSQL, MySQL |
| Artifact Storage | Models, plots, files | S3, GCS, Azure, local |
| `mlflow-artifacts://` | Proxy for artifact uploads | Through MLflow server |

| With OpenLineage Plugin | Status |
|------------------------|--------|
| Tracking operations | ✅ Works perfectly |
| Direct artifact storage (S3) | ✅ Works perfectly |
| Artifact proxy (`mlflow-artifacts://`) | ❌ Requires explicit artifact config |

The limitation is a gap in MLflow's plugin URI handling, not a fundamental issue with our approach.
