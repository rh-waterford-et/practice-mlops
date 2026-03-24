# `mlflow/facets.py` - MLflow-Specific OpenLineage Facets

**Location:** `openlineage_oai/adapters/mlflow/facets.py`

**Purpose:** Provides builders for MLflow-specific OpenLineage facets. These extend standard OpenLineage facets to capture MLflow-specific metadata like run IDs, experiment IDs, model flavors, etc.

---

## Custom Facets

| Facet | Purpose |
|-------|---------|
| `MLflowRunFacet` | MLflow run metadata (experiment, params, metrics, tags) |
| `MLflowDatasetFacet` | MLflow dataset input metadata (source, digest, context) |
| `MLflowModelFacet` | MLflow model output metadata (flavors, signature, registry) |

---

## Constants

### `MLFLOW_PRODUCER`

```python
MLFLOW_PRODUCER = "https://github.com/openlineage-oai/mlflow-adapter"
```

**Description:** Producer URL for MLflow-specific facets. Used in the `_producer` field of all facets.

---

## Functions

### `create_mlflow_run_facet()`

```python
def create_mlflow_run_facet(
    run_id: str,
    experiment_id: str,
    experiment_name: str = "",
    run_name: str = "",
    user_id: str = "",
    lifecycle_stage: str = "active",
    params: Optional[dict[str, str]] = None,
    metrics: Optional[dict[str, float]] = None,
    tags: Optional[dict[str, str]] = None,
) -> dict[str, Any]
```

**Description:** Creates an MLflowRunFacet for run metadata. Captures MLflow-specific run information beyond standard OpenLineage facets.

**Args:**
| Arg | Type | Description |
|-----|------|-------------|
| `run_id` | `str` | MLflow run ID |
| `experiment_id` | `str` | MLflow experiment ID |
| `experiment_name` | `str` | Human-readable experiment name |
| `run_name` | `str` | Human-readable run name |
| `user_id` | `str` | User who created the run |
| `lifecycle_stage` | `str` | Run lifecycle stage (`"active"`, `"deleted"`) |
| `params` | `dict[str, str]` | Run parameters dictionary |
| `metrics` | `dict[str, float]` | Run metrics dictionary (final values) |
| `tags` | `dict[str, str]` | Run tags dictionary (filtered) |

**Returns:** MLflowRunFacet dictionary

**Output Structure:**
```json
{
  "_producer": "https://github.com/openlineage-oai/mlflow-adapter",
  "_schemaURL": "https://openlineage.io/spec/facets/1-0-0/MLflowRunFacet.json",
  "runId": "abc123",
  "experimentId": "1",
  "experimentName": "my-experiment",
  "runName": "training-run",
  "userId": "user@example.com",
  "lifecycleStage": "active",
  "params": {"lr": "0.01", "epochs": "100"},
  "metrics": {"accuracy": 0.95, "loss": 0.05},
  "tags": {"team": "ml-ops"}
}
```

**Usage:**
```python
facet = create_mlflow_run_facet(
    run_id="abc123",
    experiment_id="1",
    experiment_name="my-experiment",
    params={"lr": "0.01"},
    metrics={"accuracy": 0.95},
)
```

---

### `create_mlflow_dataset_facet()`

```python
def create_mlflow_dataset_facet(
    name: str,
    source: str,
    source_type: str = "unknown",
    digest: str = "",
    context: str = "training",
    profile: Optional[dict[str, Any]] = None,
) -> dict[str, Any]
```

**Description:** Creates an MLflowDatasetFacet for input dataset metadata.

**Args:**
| Arg | Type | Description |
|-----|------|-------------|
| `name` | `str` | Dataset name as registered in MLflow |
| `source` | `str` | Dataset source URI |
| `source_type` | `str` | Type of source (`"pandas"`, `"spark"`, `"huggingface"`, etc.) |
| `digest` | `str` | Content hash/digest |
| `context` | `str` | Usage context (`"training"`, `"validation"`, `"test"`) |
| `profile` | `dict` | Dataset profile (num_rows, num_features, etc.) |

**Returns:** MLflowDatasetFacet dictionary

**Output Structure:**
```json
{
  "_producer": "https://github.com/openlineage-oai/mlflow-adapter",
  "_schemaURL": "https://openlineage.io/spec/facets/1-0-0/MLflowDatasetFacet.json",
  "name": "iris_data",
  "source": "sklearn.datasets.load_iris",
  "sourceType": "pandas",
  "digest": "abc123hash",
  "context": "training",
  "profile": {"num_rows": 150, "num_features": 4}
}
```

**Usage:**
```python
facet = create_mlflow_dataset_facet(
    name="iris_data",
    source="sklearn.datasets.load_iris",
    source_type="pandas",
    context="training",
)
```

---

### `create_mlflow_model_facet()`

```python
def create_mlflow_model_facet(
    artifact_path: str,
    run_id: str,
    flavors: Optional[list[str]] = None,
    model_uuid: str = "",
    signature_inputs: str = "",
    signature_outputs: str = "",
    registered_model_name: Optional[str] = None,
    registered_model_version: Optional[str] = None,
) -> dict[str, Any]
```

**Description:** Creates an MLflowModelFacet for model output metadata.

**Args:**
| Arg | Type | Description |
|-----|------|-------------|
| `artifact_path` | `str` | Path where model was logged |
| `run_id` | `str` | MLflow run ID |
| `flavors` | `list[str]` | Model flavors (`["sklearn", "python_function"]`) |
| `model_uuid` | `str` | Unique model identifier |
| `signature_inputs` | `str` | JSON string of input signature |
| `signature_outputs` | `str` | JSON string of output signature |
| `registered_model_name` | `str` | Name in model registry (if registered) |
| `registered_model_version` | `str` | Version in model registry (if registered) |

**Returns:** MLflowModelFacet dictionary

**Output Structure:**
```json
{
  "_producer": "https://github.com/openlineage-oai/mlflow-adapter",
  "_schemaURL": "https://openlineage.io/spec/facets/1-0-0/MLflowModelFacet.json",
  "artifactPath": "model",
  "runId": "abc123",
  "flavors": ["sklearn", "python_function"],
  "modelUuid": "uuid-456",
  "signatureInputs": "[{\"name\": \"x\", \"type\": \"double\"}]",
  "signatureOutputs": "[{\"name\": \"y\", \"type\": \"long\"}]",
  "registeredModelName": "iris-classifier",
  "registeredModelVersion": "1"
}
```

**Usage:**
```python
facet = create_mlflow_model_facet(
    artifact_path="model",
    run_id="abc123",
    flavors=["sklearn"],
    registered_model_name="iris-classifier",
    registered_model_version="1",
)
```

---

### `filter_system_tags()`

```python
def filter_system_tags(tags: dict[str, str]) -> dict[str, str]
```

**Description:** Filters out MLflow system tags (those starting with `mlflow.`) from user tags. System tags are not useful for lineage tracking.

**Args:**
- `tags`: All tags from MLflow run

**Returns:** Filtered dictionary containing only user-defined tags

**System Tags Filtered:**
- `mlflow.source.name`
- `mlflow.source.type`
- `mlflow.user`
- `mlflow.runName`
- `mlflow.log-model.history`
- Any tag starting with `mlflow.`

**Usage:**
```python
all_tags = {
    "mlflow.source.name": "script.py",
    "mlflow.user": "user@example.com",
    "team": "ml-ops",
    "version": "1.0",
}

user_tags = filter_system_tags(all_tags)
# {"team": "ml-ops", "version": "1.0"}
```

---

## Facet Schema URLs

All facets include a `_schemaURL` field pointing to the OpenLineage spec:

| Facet | Schema URL |
|-------|------------|
| MLflowRunFacet | `https://openlineage.io/spec/facets/1-0-0/MLflowRunFacet.json` |
| MLflowDatasetFacet | `https://openlineage.io/spec/facets/1-0-0/MLflowDatasetFacet.json` |
| MLflowModelFacet | `https://openlineage.io/spec/facets/1-0-0/MLflowModelFacet.json` |

**Note:** These are custom facet schemas specific to the MLflow adapter. They extend the standard OpenLineage facet model.

---

## How Facets Are Used

### In Run Events

```python
# In tracking_store.py update_run_info()
run_facets = {
    "mlflow_run": create_mlflow_run_facet(
        run_id=run_id,
        experiment_id=state.experiment_id,
        params=state.params,
        metrics=state.metrics,
        tags=filter_system_tags(state.tags),
    ),
}

self._emitter.emit_complete(
    run_id=run_id,
    job_name=state.job_name,
    run_facets=run_facets,  # ← Attached to the run event
)
```

### In Dataset Events

```python
# In tracking_store.py log_inputs()
dataset_facets = {
    "mlflow_dataset": create_mlflow_dataset_facet(
        name=dataset_name,
        source=info.get("source", ""),
        context="training",
    ),
}

self._emitter.emit_dataset_event(
    dataset_name=dataset_name,
    facets=dataset_facets,  # ← Attached to the dataset
)
```
