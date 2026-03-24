# `mlflow/tracking_store.py` - Core Tracking Store Wrapper

**Location:** `openlineage_oai/adapters/mlflow/tracking_store.py`

**Purpose:** The core of the MLflow integration. Implements an MLflow tracking store that wraps a real store (PostgreSQL, REST, file, etc.) and emits OpenLineage events for run lifecycle, parameters, metrics, inputs, and outputs.

---

## Design Patterns

| Pattern | Description |
|---------|-------------|
| **Wrapper** | Wraps the real store, ensuring MLflow works normally |
| **Dual-Write** | Every operation goes to BOTH real store AND OpenLineage |
| **Accumulator** | Params/metrics accumulate during run, emit all at COMPLETE |
| **Thread-Safe** | Run state stored per run_id with locks |

---

## URI Format

```
openlineage+<backend>://<connection-string>?openlineage_url=<url>
```

**Examples:**
- `openlineage+postgresql://user:pass@localhost:5432/mlflow`
- `openlineage+http://mlflow-server:5000`
- `openlineage+file:///tmp/mlruns`

The `"openlineage+"` prefix triggers this plugin via MLflow entry points.

---

## Classes

### `RunState`

```python
@dataclass
class RunState:
    experiment_id: str
    experiment_name: str = ""
    run_name: str = ""
    job_name: str = ""
    params: dict[str, str] = field(default_factory=dict)
    metrics: dict[str, float] = field(default_factory=dict)
    tags: dict[str, str] = field(default_factory=dict)
    inputs: list[dict[str, Any]] = field(default_factory=list)
    outputs: list[dict[str, Any]] = field(default_factory=list)
```

**Description:** Dataclass that accumulates state for a single MLflow run. Used to collect params, metrics, inputs, and outputs during the run lifetime, then emit them all in the COMPLETE event.

**Fields:**
| Field | Type | Description |
|-------|------|-------------|
| `experiment_id` | `str` | MLflow experiment ID |
| `experiment_name` | `str` | Human-readable experiment name |
| `run_name` | `str` | Human-readable run name |
| `job_name` | `str` | OpenLineage job name |
| `params` | `dict[str, str]` | Accumulated parameters |
| `metrics` | `dict[str, float]` | Accumulated metrics (latest values) |
| `tags` | `dict[str, str]` | Accumulated tags |
| `inputs` | `list[dict]` | Input datasets |
| `outputs` | `list[dict]` | Output datasets (models) |

---

### `OpenLineageTrackingStore`

```python
class OpenLineageTrackingStore
```

**Description:** MLflow Tracking Store that emits OpenLineage events. Wraps a real MLflow tracking store (PostgreSQL, REST, file, etc.) and intercepts operations to emit lineage while delegating actual storage.

The store is loaded by MLflow when `MLFLOW_TRACKING_URI` starts with `"openlineage+"`.

**Example:**
```bash
export MLFLOW_TRACKING_URI="openlineage+postgresql://localhost/mlflow"
export OPENLINEAGE_URL="http://marquez:5000"
```

```python
import mlflow
with mlflow.start_run():
    mlflow.log_param("lr", 0.01)  # Goes to PostgreSQL AND emits to Marquez
```

---

## Functions

### `__init__()`

```python
def __init__(self, store_uri: str, artifact_uri: str = None)
```

**Description:** Initializes the tracking store wrapper. Parses the URI to extract the backend type and OpenLineage configuration, creates the delegate store, and initializes the OpenLineage emitter.

**Args:**
- `store_uri`: Full URI like `"openlineage+postgresql://user:pass@localhost/mlflow"`
- `artifact_uri`: Optional artifact storage URI

**Usage:**
```python
# Automatically called by MLflow when URI starts with "openlineage+"
store = OpenLineageTrackingStore(
    "openlineage+http://mlflow-server:5000",
    artifact_uri="s3://bucket/artifacts"
)
```

---

### `_create_delegate_store()`

```python
def _create_delegate_store(self, backend_uri: str, artifact_uri: str)
```

**Description:** Creates the delegate (real) tracking store based on the URI scheme. Directly instantiates the appropriate store class to avoid recursion through MLflow's registry.

**Args:**
- `backend_uri`: Backend URI without the `openlineage+` prefix
- `artifact_uri`: Artifact storage URI

**Supported Schemes:**
| Scheme | Store Class |
|--------|-------------|
| `http`, `https` | `RestStore` |
| `postgresql`, `mysql`, `sqlite`, `mssql` | `SqlAlchemyStore` |
| `file` or empty | `FileStore` |

**Returns:** MLflow store instance

---

## Run Lifecycle Methods

These methods emit OpenLineage events.

### `create_run()`

```python
def create_run(
    self,
    experiment_id: str,
    user_id: str,
    start_time: int,
    tags: list,
    run_name: str,
)
```

**Description:** Creates a new run and emits an OpenLineage START event. Initializes the `RunState` for accumulating data during the run.

**Args:**
- `experiment_id`: MLflow experiment ID
- `user_id`: User who created the run
- `start_time`: Run start timestamp
- `tags`: Initial run tags
- `run_name`: Human-readable run name

**Returns:** MLflow `Run` object

**OpenLineage Event:** `START`

---

### `update_run_info()`

```python
def update_run_info(
    self,
    run_id: str,
    run_status,
    end_time: int,
    run_name: str,
)
```

**Description:** Updates run status. When status is `FINISHED`, emits a COMPLETE event with all accumulated params, metrics, inputs, and outputs. When status is `FAILED`, emits a FAIL event.

**Args:**
- `run_id`: MLflow run ID
- `run_status`: New status (FINISHED, FAILED, etc.)
- `end_time`: Run end timestamp
- `run_name`: Run name

**Returns:** Result from delegate store

**OpenLineage Events:**
| Status | Event |
|--------|-------|
| `FINISHED` | `COMPLETE` with accumulated data |
| `FAILED` | `FAIL` with error message |

---

## Param/Metric/Tag Methods

These methods accumulate data without emitting events.

### `log_param()`

```python
def log_param(self, run_id: str, param)
```

**Description:** Logs a single parameter. Delegates to the real store and accumulates in `RunState` for inclusion in the COMPLETE event.

**Args:**
- `run_id`: MLflow run ID
- `param`: MLflow Param object with `key` and `value`

---

### `log_params()`

```python
def log_params(self, run_id: str, params: list)
```

**Description:** Logs multiple parameters. Delegates and accumulates each one.

**Args:**
- `run_id`: MLflow run ID
- `params`: List of MLflow Param objects

---

### `log_metric()`

```python
def log_metric(self, run_id: str, metric)
```

**Description:** Logs a single metric. Delegates to the real store and accumulates the latest value in `RunState`.

**Args:**
- `run_id`: MLflow run ID
- `metric`: MLflow Metric object with `key` and `value`

---

### `log_metrics()`

```python
def log_metrics(self, run_id: str, metrics: list)
```

**Description:** Logs multiple metrics. Delegates and accumulates each one.

**Args:**
- `run_id`: MLflow run ID
- `metrics`: List of MLflow Metric objects

---

### `set_tag()`

```python
def set_tag(self, run_id: str, tag)
```

**Description:** Sets a run tag. Delegates to the real store and accumulates in `RunState`. Special handling for `mlflow.log-model.history` tag which contains model information.

**Args:**
- `run_id`: MLflow run ID
- `tag`: MLflow RunTag object with `key` and `value`

---

### `set_tags()`

```python
def set_tags(self, run_id: str, tags: list)
```

**Description:** Sets multiple tags. Delegates and accumulates each one with model history handling.

**Args:**
- `run_id`: MLflow run ID
- `tags`: List of MLflow RunTag objects

---

### `log_batch()`

```python
def log_batch(self, run_id: str, metrics: list = None, params: list = None, tags: list = None)
```

**Description:** Logs a batch of metrics, params, and tags. Delegates to the real store and accumulates all values.

**Args:**
- `run_id`: MLflow run ID
- `metrics`: List of Metric objects
- `params`: List of Param objects
- `tags`: List of RunTag objects

---

## Model Output Tracking

### `_handle_model_history_tag()`

```python
def _handle_model_history_tag(self, run_id: str, tag_value: str)
```

**Description:** Parses the `mlflow.log-model.history` tag (JSON array) and registers each model as an output dataset. Emits a standalone DatasetEvent and tracks as run output.

**Args:**
- `run_id`: MLflow run ID
- `tag_value`: JSON string containing model history

**OpenLineage Event:** `DatasetEvent(CREATE)` for each model

**JSON Format:**
```json
[{
  "artifact_path": "model",
  "flavors": {"sklearn": {...}},
  "run_id": "...",
  "utc_time_created": "..."
}]
```

---

## Dataset/Input Methods

### `log_inputs()`

```python
def log_inputs(self, run_id: str, datasets: list = None, models: list = None)
```

**Description:** Logs input datasets. For each dataset:
1. Emits a standalone `DatasetEvent` to create it in Marquez
2. Accumulates as input for the run's COMPLETE event

**Args:**
- `run_id`: MLflow run ID
- `datasets`: List of `DatasetInput` objects
- `models`: List of `ModelInput` objects (MLflow 2.x+)

**OpenLineage Event:** `DatasetEvent(CREATE)` for each input dataset

---

### `log_outputs()`

```python
def log_outputs(self, run_id: str, models: list)
```

**Description:** Logs output models (newer MLflow 2.x+ API). Emits a standalone DatasetEvent for each model and tracks as run output.

**Args:**
- `run_id`: MLflow run ID
- `models`: List of model output objects

**OpenLineage Event:** `DatasetEvent(CREATE)` for each model

---

## Delegation Methods

These methods delegate directly to the real store without any OpenLineage tracking.

| Method | Description |
|--------|-------------|
| `get_experiment(experiment_id)` | Get experiment by ID |
| `get_experiment_by_name(name)` | Get experiment by name |
| `create_experiment(name, artifact_location, tags)` | Create new experiment |
| `delete_experiment(experiment_id)` | Delete experiment |
| `restore_experiment(experiment_id)` | Restore deleted experiment |
| `rename_experiment(experiment_id, new_name)` | Rename experiment |
| `get_run(run_id)` | Get run by ID |
| `delete_run(run_id)` | Delete run |
| `restore_run(run_id)` | Restore deleted run |
| `search_runs(*args, **kwargs)` | Search runs |
| `search_experiments(*args, **kwargs)` | Search experiments |
| `list_run_infos(*args, **kwargs)` | List run infos |

---

### `__getattr__()`

```python
def __getattr__(self, name: str)
```

**Description:** Delegates any unimplemented methods to the real store. Ensures forward compatibility with new MLflow methods.

**Args:**
- `name`: Method name

**Returns:** Method from delegate store

---

## Event Flow Diagram

```
mlflow.start_run()
       │
       ▼
┌──────────────────────────────────────┐
│  OpenLineageTrackingStore.create_run │
│                                      │
│  1. Delegate to real store           │
│  2. Initialize RunState              │
│  3. Emit START event                 │
└──────────────────────────────────────┘
       │
       ▼
mlflow.log_param("lr", 0.01)
       │
       ▼
┌──────────────────────────────────────┐
│  OpenLineageTrackingStore.log_param  │
│                                      │
│  1. Delegate to real store           │
│  2. Accumulate in RunState           │
│  (no event emitted)                  │
└──────────────────────────────────────┘
       │
       ▼
mlflow.end_run()
       │
       ▼
┌──────────────────────────────────────────┐
│  OpenLineageTrackingStore.update_run_info│
│                                          │
│  1. Delegate to real store               │
│  2. Emit COMPLETE with all accumulated:  │
│     - params                             │
│     - metrics                            │
│     - inputs                             │
│     - outputs                            │
│  3. Cleanup RunState                     │
└──────────────────────────────────────────┘
```
