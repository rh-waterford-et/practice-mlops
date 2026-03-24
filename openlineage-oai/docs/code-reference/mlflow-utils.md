# `mlflow/utils.py` - MLflow Utility Functions

**Location:** `openlineage_oai/adapters/mlflow/utils.py`

**Purpose:** Provides utility functions for extracting information from MLflow data structures and converting them to OpenLineage format.

---

## Functions

### `extract_dataset_info()`

```python
def extract_dataset_info(dataset: Any) -> dict[str, Any]
```

**Description:** Extracts OpenLineage-relevant information from an MLflow Dataset object. Normalizes various MLflow dataset formats into a consistent structure.

**Args:**
- `dataset`: MLflow Dataset object

**Returns:** Dictionary with:
| Key | Type | Description |
|-----|------|-------------|
| `name` | `str` | Dataset name |
| `source_type` | `str` | Type of source (`"pandas"`, `"spark"`, etc.) |
| `source` | `str` | Source URI or description |
| `digest` | `str` | Content hash |
| `schema` | `list[dict]` | List of field definitions (if available) |
| `profile` | `dict` | Dataset profile (if available) |

**Usage:**
```python
import mlflow
import pandas as pd

df = pd.DataFrame({"x": [1, 2], "y": [3, 4]})
dataset = mlflow.data.from_pandas(df, name="my_data")

info = extract_dataset_info(dataset)
# {
#   "name": "my_data",
#   "source_type": "pandas",
#   "source": "",
#   "digest": "abc123",
#   "schema": [{"name": "x", "type": "long"}, {"name": "y", "type": "long"}],
#   "profile": {}
# }
```

---

### `_parse_schema_json()`

```python
def _parse_schema_json(schema_str: str) -> Optional[list[dict[str, str]]]
```

**Description:** Parses schema from JSON string format (used by `mlflow.entities.Dataset`). Handles both `mlflow_colspec` format and direct list format.

**Args:**
- `schema_str`: JSON string representation of schema

**Returns:** List of field definitions `[{"name": ..., "type": ...}, ...]` or `None`

**Supported JSON Formats:**

**Format 1: mlflow_colspec**
```json
{
  "mlflow_colspec": [
    {"type": "double", "name": "x", "required": true},
    {"type": "long", "name": "y", "required": true}
  ]
}
```

**Format 2: Direct list**
```json
[
  {"type": "double", "name": "x"},
  {"type": "long", "name": "y"}
]
```

**Usage:**
```python
schema_json = '{"mlflow_colspec": [{"name": "x", "type": "double"}]}'
fields = _parse_schema_json(schema_json)
# [{"name": "x", "type": "double"}]
```

---

### `_convert_mlflow_schema()`

```python
def _convert_mlflow_schema(schema: Any) -> Optional[list[dict[str, str]]]
```

**Description:** Converts MLflow Schema object to OpenLineage field list. Handles both ColSpec schema and ModelSignature schema.

**Args:**
- `schema`: MLflow Schema object

**Returns:** List of field definitions or `None`

**Supported Schema Types:**
| Type | Method Used |
|------|-------------|
| Schema with `to_dict()` | Parses dictionary output |
| ModelSignature with `inputs` | Iterates over input columns |

**Usage:**
```python
from mlflow.types.schema import Schema, ColSpec

schema = Schema([
    ColSpec("double", "feature_1"),
    ColSpec("double", "feature_2"),
])

fields = _convert_mlflow_schema(schema)
# [{"name": "feature_1", "type": "double"}, {"name": "feature_2", "type": "double"}]
```

---

### `extract_model_info()`

```python
def extract_model_info(
    artifact_path: str,
    run_id: str,
    model_info: Any = None,
) -> dict[str, Any]
```

**Description:** Extracts OpenLineage-relevant information from an MLflow model.

**Args:**
| Arg | Type | Description |
|-----|------|-------------|
| `artifact_path` | `str` | Path where model was logged |
| `run_id` | `str` | MLflow run ID |
| `model_info` | `Any` | Optional MLflow ModelInfo object |

**Returns:** Dictionary with:
| Key | Type | Description |
|-----|------|-------------|
| `artifact_path` | `str` | Path where model was logged |
| `run_id` | `str` | MLflow run ID |
| `flavors` | `list[str]` | List of model flavors |
| `model_uuid` | `str` | Unique identifier |
| `signature_inputs` | `str` | JSON string of input signature |
| `signature_outputs` | `str` | JSON string of output signature |

**Usage:**
```python
# After logging a model
model_info = mlflow.sklearn.log_model(model, "model")

info = extract_model_info(
    artifact_path="model",
    run_id="abc123",
    model_info=model_info,
)
# {
#   "artifact_path": "model",
#   "run_id": "abc123",
#   "flavors": ["sklearn", "python_function"],
#   "model_uuid": "uuid-456",
#   "signature_inputs": "[{\"name\": \"x\", \"type\": \"double\"}]",
#   "signature_outputs": "[{\"name\": \"y\", \"type\": \"long\"}]"
# }
```

---

### `build_model_namespace()`

```python
def build_model_namespace(tracking_uri: str) -> str
```

**Description:** Builds the namespace for model outputs based on the tracking URI.

**Args:**
- `tracking_uri`: MLflow tracking URI

**Returns:** Namespace string like `"mlflow://localhost:5000"`

**Transformation:**
| Input | Output |
|-------|--------|
| `http://mlflow-server:5000` | `mlflow://http/mlflow-server:5000` |
| `postgresql://localhost/mlflow` | `mlflow://postgresql/localhost/mlflow` |
| `file:///tmp/mlruns` | `mlflow://file//tmp/mlruns` |

**Usage:**
```python
ns = build_model_namespace("http://mlflow-server:5000")
# "mlflow://http/mlflow-server:5000"
```

---

### `build_model_name()`

```python
def build_model_name(run_id: str, artifact_path: str) -> str
```

**Description:** Builds the name for a model output dataset.

**Args:**
- `run_id`: MLflow run ID
- `artifact_path`: Artifact path where model was logged

**Returns:** Name string like `"runs/abc-123/artifacts/model"`

**Usage:**
```python
name = build_model_name("abc123", "model")
# "runs/abc123/artifacts/model"

name = build_model_name("abc123", "models/classifier")
# "runs/abc123/artifacts/models/classifier"
```

---

## Schema Extraction Flow

```
mlflow.log_input(dataset)
         │
         ▼
┌─────────────────────────────────────┐
│  extract_dataset_info(dataset)      │
│                                     │
│  1. Get name, digest from attrs     │
│  2. Get source from dataset.source  │
│  3. Get schema:                     │
│     └─► _convert_mlflow_schema()    │
│         or                          │
│         _parse_schema_json()        │
└─────────────────────────────────────┘
         │
         ▼
┌─────────────────────────────────────┐
│  Returns:                           │
│  {                                  │
│    "name": "my_data",               │
│    "source_type": "pandas",         │
│    "schema": [                      │
│      {"name": "x", "type": "double"}│
│    ]                                │
│  }                                  │
└─────────────────────────────────────┘
```

---

## Why Schema Parsing Is Complex

MLflow represents schemas differently depending on the context:

| Context | Schema Format |
|---------|---------------|
| `mlflow.data.from_pandas()` | `Schema` object with `to_dict()` |
| `mlflow.entities.Dataset` | JSON string with `mlflow_colspec` |
| Model signature | `ModelSignature` with `inputs`/`outputs` |

The utility functions handle all these formats to provide consistent schema extraction.
