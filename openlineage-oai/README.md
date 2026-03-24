# OpenLineage OAI

**Unified OpenLineage client for OpenShift AI.**

Automatically capture data lineage from your ML pipelines with zero code changes.

## Supported Tools

| Tool | Status | Mechanism |
|------|--------|-----------|
| **MLflow** | ✅ Supported | Tracking store plugin |
| **LlamaStack** | 📝 Designed | Provider extension |
| **Ray** | 🔜 Planned | TBD |
| **Kubeflow Pipelines** | 🔜 Planned | TBD |

## Installation

```bash
# Core package
pip install openlineage-oai

# With MLflow support
pip install openlineage-oai[mlflow]

# All adapters
pip install openlineage-oai[all]
```

## Quick Start

### Option 1: Environment Variables (Zero Code Changes)

```bash
# Configure OpenLineage backend
export OPENLINEAGE_URL="http://marquez:5000"
export OPENLINEAGE_NAMESPACE="ml-platform"

# Use the OpenLineage tracking store wrapper
export MLFLOW_TRACKING_URI="openlineage+postgresql://user:pass@localhost/mlflow"
```

Your existing MLflow code works unchanged:

```python
import mlflow

with mlflow.start_run():
    mlflow.log_param("lr", 0.01)
    mlflow.log_metric("accuracy", 0.95)
# OpenLineage events emitted automatically!
```

### Option 2: Programmatic Initialization

```python
import openlineage_oai

# Initialize OpenLineage for MLflow
openlineage_oai.init(
    url="http://marquez:5000",
    namespace="ml-platform",
    tools=["mlflow"],
)

# Use MLflow normally
import mlflow
with mlflow.start_run():
    mlflow.log_param("lr", 0.01)
# Lineage captured automatically
```

## Configuration

### Environment Variables

| Variable | Required | Default | Description |
|----------|----------|---------|-------------|
| `OPENLINEAGE_URL` | Yes | - | Marquez/OpenLineage backend URL |
| `OPENLINEAGE_NAMESPACE` | No | `default` | Default namespace for jobs |
| `OPENLINEAGE_API_KEY` | No | - | API key for authenticated backends |
| `OPENLINEAGE_TIMEOUT` | No | `5` | HTTP timeout in seconds |
| `OPENLINEAGE_REGISTRY_URL` | No | - | Dataset registry URL |

### MLflow URI Format

```
openlineage+<backend>://<connection-string>
```

Examples:
```bash
# PostgreSQL backend
MLFLOW_TRACKING_URI="openlineage+postgresql://user:pass@localhost:5432/mlflow"

# REST backend
MLFLOW_TRACKING_URI="openlineage+http://mlflow-server:5000"

# File backend (for testing)
MLFLOW_TRACKING_URI="openlineage+file:///tmp/mlruns"
```

## How It Works

### MLflow Integration

OpenLineage OAI uses MLflow's native **tracking store plugin** system:

1. When `MLFLOW_TRACKING_URI` starts with `openlineage+`, MLflow loads our `OpenLineageTrackingStore`
2. Our store wraps the real tracking store (PostgreSQL, REST, etc.)
3. All MLflow operations go through our store, which:
   - **Delegates** to the real store → MLflow works normally
   - **Emits** OpenLineage events → Lineage is captured

```
mlflow.start_run()  →  OpenLineageTrackingStore.create_run()
                              │
               ┌──────────────┴──────────────┐
               ▼                              ▼
       PostgreSQL.create_run()         Marquez: emit START
       (MLflow works normally)         (Lineage captured)
```

**Key benefit:** Your existing MLflow code requires **no changes**.

### LlamaStack Integration

OpenLineage OAI uses LlamaStack's **provider extension** system:

1. Configure our provider in `stack_config.yaml` with `module: openlineage_oai.adapters.llamastack`
2. Our provider wraps the real DatasetIO/Inference/PostTraining providers
3. All LlamaStack operations pass through our wrapper, which:
   - **Delegates** to the real provider → LlamaStack works normally
   - **Emits** OpenLineage events → Lineage is captured

## Events Captured

| MLflow Operation | OpenLineage Event |
|-----------------|-------------------|
| `mlflow.start_run()` | `START` RunEvent |
| `mlflow.log_param()` | Accumulated, included in `COMPLETE` |
| `mlflow.log_metric()` | Accumulated, included in `COMPLETE` |
| `mlflow.log_input()` | Input dataset tracked (standalone + job linkage) |
| `mlflow.log_model()` | Output model tracked (standalone + job linkage) |
| `mlflow.end_run()` | `COMPLETE` RunEvent |
| Run failure | `FAIL` RunEvent |

## Complete Example

```python
import mlflow
import pandas as pd
from sklearn.ensemble import RandomForestClassifier

# Configure (or use environment variables)
mlflow.set_tracking_uri("openlineage+http://mlflow-server:5000")
mlflow.set_experiment("my-experiment")

with mlflow.start_run(run_name="training-run"):
    # Log input dataset - creates standalone dataset in Marquez
    train_data = mlflow.data.from_pandas(df, name="training_data")
    mlflow.log_input(train_data, context="training")
    
    # Log parameters
    mlflow.log_param("n_estimators", 100)
    mlflow.log_param("max_depth", 5)
    
    # Train model
    model = RandomForestClassifier(n_estimators=100, max_depth=5)
    model.fit(X_train, y_train)
    
    # Log metrics
    mlflow.log_metric("accuracy", model.score(X_test, y_test))
    
    # Log model - creates standalone dataset in Marquez
    mlflow.sklearn.log_model(model, "model")

# Result in Marquez:
#   - Dataset: training_data (input)
#   - Dataset: model/model (output)
#   - Job: mlflow/experiment-1/training-run
#   - Lineage: training_data → Job → model/model
```

## Important: Artifact Storage Configuration

**Known Limitation:** The `openlineage+` URI prefix conflicts with MLflow's default `mlflow-artifacts://` artifact proxy.

**Solution:** Configure artifact storage explicitly when using a remote MLflow server:

```python
# Option 1: Use S3/GCS/Azure (recommended for production)
mlflow.create_experiment("my-exp", artifact_location="s3://my-bucket/artifacts")

# Option 2: Use local/NFS storage
mlflow.create_experiment("my-exp", artifact_location="/shared/mlflow/artifacts")
```

This is actually a **production best practice** - the default `mlflow-artifacts://` proxy doesn't scale well for large models.

See [docs/IMPLEMENTATION.md](docs/IMPLEMENTATION.md) for full technical details.

## Development

```bash
# Clone the repository
git clone https://github.com/your-org/openlineage-oai.git
cd openlineage-oai

# Install development dependencies
pip install -e ".[dev]"

# Run tests
pytest

# Run with coverage
pytest --cov=openlineage_oai --cov-report=html
```

## Architecture

```
openlineage-oai/
├── openlineage_oai/
│   ├── core/           # Shared components
│   │   ├── config.py   # Configuration
│   │   ├── emitter.py  # Event emission
│   │   ├── facets.py   # OpenLineage facets
│   │   └── registry.py # Dataset registry client
│   │
│   ├── adapters/       # Tool-specific adapters
│   │   ├── base.py     # Adapter interface
│   │   ├── mlflow/     # MLflow tracking store wrapper
│   │   └── llamastack/ # LlamaStack provider extension (planned)
│   │
│   └── utils/          # Utilities
│       ├── naming.py   # Job naming
│       └── uri.py      # URI parsing
│
└── tests/
    ├── unit/           # Fast, isolated tests
    └── integration/    # End-to-end tests
```

## License

Apache 2.0

## Documentation

- [Implementation Guide](docs/IMPLEMENTATION.md) - Detailed architecture and design decisions
- [MLflow Storage Explained](docs/MLFLOW_STORAGE.md) - Understanding MLflow's tracking and artifact systems
- [Example Script](examples/iris_training.py) - Complete working example

## Related

- [OpenLineage Spec](https://openlineage.io/spec/)
- [Marquez](https://marquezproject.ai/)
- [MLflow Tracking Store Plugins](https://mlflow.org/docs/latest/plugins.html#tracking-store-plugins)
- [LlamaStack](https://github.com/llamastack/llama-stack)