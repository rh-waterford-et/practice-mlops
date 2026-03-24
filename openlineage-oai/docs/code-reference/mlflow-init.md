# `mlflow/__init__.py` - MLflow Adapter Entry Point

**Location:** `openlineage_oai/adapters/mlflow/__init__.py`

**Purpose:** Entry point for the MLflow adapter. Provides the `MLflowAdapter` class that implements the `ToolAdapter` interface and exports the `OpenLineageTrackingStore` for MLflow's entry point discovery.

---

## How It Works

1. When `MLFLOW_TRACKING_URI` starts with `"openlineage+"`, MLflow loads our `OpenLineageTrackingStore` via Python entry points
2. Our store wraps the real tracking store (PostgreSQL, REST, etc.)
3. All MLflow operations go through our store, which:
   - **Delegates** to the real store → MLflow works normally
   - **Emits** OpenLineage events → Lineage is captured

---

## Classes

### `MLflowAdapter`

```python
class MLflowAdapter(ToolAdapter)
```

**Description:** The main adapter class that integrates OpenLineage with MLflow via the tracking store plugin system.

**Inherits from:** `ToolAdapter`

**Note:** The tracking store is primarily activated via the URI scheme (`openlineage+...`) and Python entry points, not by calling `install_hooks()`. The `install_hooks()` method is provided for completeness and potential future runtime registration.

---

## Functions

### `get_tool_name()`

```python
def get_tool_name(self) -> str
```

**Description:** Returns the identifier for this adapter.

**Returns:** `"mlflow"`

**Usage:**
```python
adapter = MLflowAdapter(emitter, namespace)
print(adapter.get_tool_name())  # "mlflow"
```

---

### `install_hooks()`

```python
def install_hooks(self) -> None
```

**Description:** Installs the MLflow OpenLineage integration. Verifies that MLflow is available.

**Note:** The primary installation mechanism is via entry points registered in `pyproject.toml`. When users set:

```bash
MLFLOW_TRACKING_URI=openlineage+postgresql://...
```

MLflow automatically discovers and loads our tracking store.

This method is provided for:
1. Completeness of the `ToolAdapter` interface
2. Potential runtime registration in future MLflow versions
3. Explicit verification that MLflow is available

**Raises:** `ImportError` if MLflow is not installed

**Usage:**
```python
adapter = MLflowAdapter(emitter, namespace)
adapter.install_hooks()  # Verifies MLflow is available
```

---

### `uninstall_hooks()`

```python
def uninstall_hooks(self) -> None
```

**Description:** Marks the adapter as uninstalled.

**Note:** Entry point-based plugins cannot be dynamically unregistered from MLflow. This method marks the adapter as uninstalled but the tracking store will remain registered with MLflow.

To fully disable, users should change `MLFLOW_TRACKING_URI` to not use the `"openlineage+"` prefix.

**Usage:**
```python
adapter.uninstall_hooks()
```

---

## Exports

The module exports:

| Name | Type | Description |
|------|------|-------------|
| `MLflowAdapter` | Class | Main adapter class |
| `OpenLineageTrackingStore` | Class | Tracking store for entry point discovery |

---

## Entry Points

The adapter is registered in `pyproject.toml`:

```toml
[project.entry-points."mlflow.tracking_store"]
openlineage = "openlineage_oai.adapters.mlflow.tracking_store:OpenLineageTrackingStore"
"openlineage+http" = "openlineage_oai.adapters.mlflow.tracking_store:OpenLineageTrackingStore"
"openlineage+https" = "openlineage_oai.adapters.mlflow.tracking_store:OpenLineageTrackingStore"
"openlineage+postgresql" = "openlineage_oai.adapters.mlflow.tracking_store:OpenLineageTrackingStore"
# ... more schemes
```

This allows MLflow to automatically discover and load our tracking store when the URI starts with `openlineage+`.

---

## Usage Examples

### Option 1: Programmatic Initialization

```python
import openlineage_oai

openlineage_oai.init(
    url="http://marquez:5000",
    namespace="ml-platform",
    tools=["mlflow"],
)

# Now use MLflow normally
import mlflow
with mlflow.start_run():
    mlflow.log_param("lr", 0.01)
# OpenLineage events emitted automatically
```

### Option 2: Environment Variables (Zero Code Changes)

```bash
export MLFLOW_TRACKING_URI="openlineage+postgresql://user:pass@localhost/mlflow"
export OPENLINEAGE_URL="http://marquez:5000"
export OPENLINEAGE_NAMESPACE="ml-platform"
```

```python
# No openlineage_oai import needed!
import mlflow

with mlflow.start_run():
    mlflow.log_param("lr", 0.01)
# OpenLineage events emitted automatically via entry points
```
