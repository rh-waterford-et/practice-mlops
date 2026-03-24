# OpenLineage-OAI Implementation Guide

This document details the implementation of the `openlineage-oai` library, including architectural decisions, design justifications, and known limitations.

## Overview

`openlineage-oai` is a unified OpenLineage client for ML tools. The first adapter supports MLflow, with future adapters planned for Ray, Kubeflow Pipelines, and LlamaStack.

## Architecture

### Core Design: Tracking Store Wrapper

The MLflow adapter uses MLflow's native **plugin system** - specifically the tracking store plugin mechanism.

```
┌─────────────────────────────────────────────────────────────────┐
│                     User's Training Script                       │
│                                                                  │
│   mlflow.set_tracking_uri("openlineage+http://mlflow-server")   │
│   mlflow.log_param("lr", 0.01)                                  │
│   mlflow.log_model(model, "model")                              │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│                  OpenLineageTrackingStore                        │
│                                                                  │
│   • Intercepts all MLflow tracking operations                    │
│   • Strips "openlineage+" prefix                                 │
│   • Delegates to real MLflow store (PostgreSQL, REST, etc.)     │
│   • Emits OpenLineage events to Marquez                         │
└─────────────────────────────────────────────────────────────────┘
                    │                       │
                    ▼                       ▼
           ┌──────────────┐        ┌──────────────┐
           │ MLflow Store │        │   Marquez    │
           │ (PostgreSQL) │        │ (OpenLineage)│
           └──────────────┘        └──────────────┘
```

### Why This Approach?

**Considered Alternatives:**

| Approach | Pros | Cons | Decision |
|----------|------|------|----------|
| Tracking Store Plugin | Zero code changes, MLflow-native | Artifact URI limitation | ✅ Chosen |
| Function Patching | Works with default artifacts | Fragile, version-dependent | ❌ Rejected |
| Server Middleware | Client unchanged | Requires custom server | ❌ Rejected |
| RunContextProvider | Simple | Can't intercept operations | ❌ Rejected |

**Key Justifications:**

1. **MLflow-Native**: The tracking store plugin is MLflow's designed extension point
2. **Zero Code Changes**: Users only change the tracking URI
3. **Complete Coverage**: Intercepts all tracking operations (params, metrics, tags, inputs, outputs)
4. **Fail-Safe**: If OpenLineage emission fails, MLflow still works

## Plugin Registration

The plugin is registered via Python entry points in `pyproject.toml`:

```toml
[project.entry-points."mlflow.tracking_store"]
"openlineage+http" = "openlineage_oai.adapters.mlflow.tracking_store:OpenLineageTrackingStore"
"openlineage+https" = "openlineage_oai.adapters.mlflow.tracking_store:OpenLineageTrackingStore"
"openlineage+postgresql" = "openlineage_oai.adapters.mlflow.tracking_store:OpenLineageTrackingStore"
# ... etc for other schemes
```

When MLflow sees a URI like `openlineage+http://server`, it:
1. Looks up entry points for the scheme `openlineage+http`
2. Finds our `OpenLineageTrackingStore` class
3. Instantiates it with the full URI

## MLflow Operation → OpenLineage Event Mapping

| MLflow Operation | Store Method | OpenLineage Action |
|-----------------|--------------|-------------------|
| `mlflow.start_run()` | `create_run()` | Emit START RunEvent |
| `mlflow.log_param()` | `log_param()` | Accumulate (no event) |
| `mlflow.log_metric()` | `log_metric()` | Accumulate (no event) |
| `mlflow.log_input()` | `log_inputs()` | Emit DatasetEvent + accumulate |
| `mlflow.log_model()` | `log_outputs()` | Emit DatasetEvent + accumulate |
| `mlflow.end_run()` | `update_run_info(FINISHED)` | Emit COMPLETE RunEvent with all data |
| Run fails | `update_run_info(FAILED)` | Emit FAIL RunEvent |

### Accumulator Pattern

We don't emit an OpenLineage event for every MLflow call. Instead:

1. **START** - Emit immediately when run begins
2. **Accumulate** - Gather params, metrics, inputs, outputs during the run
3. **COMPLETE/FAIL** - Emit once at the end with everything

This reduces network traffic and provides a complete picture in each event.

## Standalone Dataset Registration

When `mlflow.log_input()` is called, we:

1. **Emit a DatasetEvent** - Creates the dataset as a first-class entity in Marquez
2. **Accumulate for run** - Include in the COMPLETE event's inputs list

This ensures datasets exist independently in the lineage graph, not just as references from jobs.

```python
# In log_inputs():
# Step 1: Register dataset standalone
self._emitter.emit_dataset_event(
    event_type="CREATE",
    dataset_name=dataset_name,
    dataset_namespace=self._namespace,
    facets=dataset_facets,
)

# Step 2: Track for run completion
self._run_states[run_id].inputs.append(input_dataset)
```

## Artifact Storage Configuration

MLflow has **separate URIs** for tracking (metadata) and artifacts (files):

- **Tracking URI**: Where params, metrics, tags go → Our plugin intercepts this
- **Artifact URI**: Where model files go → Configure separately, no plugin needed

```bash
# Tracking - uses our plugin
export MLFLOW_TRACKING_URI="openlineage+http://mlflow-server:5000"

# Artifacts - configured separately (S3, PVC, local, etc.)
# Set on MLflow server or per-experiment
```

**Why this matters:** We capture model metadata through the tracking store (via `mlflow.log-model.history` tag), not the artifact system. Model files are stored wherever you configure artifacts - we don't need to intercept that.

### MLflow Version Compatibility

The plugin uses:
- `log_outputs()` method (MLflow 2.x+) for model tracking
- `mlflow.log-model.history` tag as fallback for older versions

We recommend MLflow 2.0+ for full compatibility.

## Configuration

### Required Environment Variables

```bash
# OpenLineage backend (Marquez)
export OPENLINEAGE_URL="http://marquez:5000"
export OPENLINEAGE_NAMESPACE="my-namespace"

# MLflow tracking URI with plugin prefix
export MLFLOW_TRACKING_URI="openlineage+http://mlflow-server:5000"
```

### Optional Configuration

```bash
# API key if Marquez requires authentication
export OPENLINEAGE_API_KEY="your-key"

# Timeout for OpenLineage HTTP requests (default: 10s)
export OPENLINEAGE_TIMEOUT_SECONDS="30"
```

## File Structure

```
openlineage-oai/
├── openlineage_oai/
│   ├── __init__.py              # Package entry point (init/shutdown)
│   ├── core/
│   │   ├── config.py            # Configuration management
│   │   ├── emitter.py           # OpenLineage event emission
│   │   ├── facets.py            # Standard OpenLineage facets
│   │   └── registry.py          # Dataset registry client (optional)
│   ├── adapters/
│   │   ├── base.py              # Abstract adapter interface
│   │   └── mlflow/
│   │       ├── __init__.py      # MLflowAdapter class
│   │       ├── tracking_store.py # Tracking store wrapper (main logic)
│   │       ├── facets.py        # MLflow-specific facets
│   │       └── utils.py         # MLflow utilities
│   └── utils/
│       ├── naming.py            # Job naming conventions
│       └── uri.py               # URI parsing utilities
├── tests/
│   ├── unit/                    # Unit tests
│   └── integration/             # Integration tests
├── examples/
│   └── iris_training.py         # Demo script
└── docs/
    └── IMPLEMENTATION.md        # This file
```

## Testing

### Unit Tests

```bash
pytest tests/unit/ -v
```

### Integration Tests

Requires running MLflow and Marquez:

```bash
export OPENLINEAGE_URL="http://marquez:5000"
export MLFLOW_TRACKING_URI="http://mlflow-server:5000"
pytest tests/integration/ -v
```

## Future Work

1. **Ray Adapter** - Integrate with Ray's distributed execution
2. **KFP Adapter** - Integrate with Kubeflow Pipelines
3. **LlamaStack Adapter** - Integrate with LlamaStack
4. **Dataset Registry** - Central registry for cross-tool dataset identity
5. **Schema Extraction** - Automatic schema detection from DataFrames

## References

- [MLflow Tracking Store Plugins](https://mlflow.org/docs/latest/plugins.html#tracking-store-plugins)
- [OpenLineage Spec](https://openlineage.io/spec/)
- [Marquez API](https://marquezproject.github.io/marquez/openapi.html)
