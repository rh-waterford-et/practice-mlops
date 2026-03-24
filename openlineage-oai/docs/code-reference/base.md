# `base.py` - Abstract Adapter Interface

**Location:** `openlineage_oai/adapters/base.py`

**Purpose:** Defines the abstract interface that all tool adapters must implement. Each adapter is responsible for:
1. Intercepting tool operations (via hooks, plugins, or patching)
2. Converting tool-specific data to OpenLineage events
3. Managing tool-specific state (e.g., run accumulator)

---

## Design Decision: Abstract Base Class

We use `ABC` rather than `Protocol` because:
1. Clear contract with abstract methods
2. Can include shared implementation (`build_job_name`)
3. Better error messages when methods not implemented
4. Runtime checking with `isinstance()`

---

## Classes

### `ToolAdapter`

```python
class ToolAdapter(ABC)
```

**Description:** Abstract base class for tool-specific OpenLineage adapters. Each adapter implements the hooks necessary to intercept tool operations and emit OpenLineage events through the shared emitter.

**Attributes:**
| Attribute | Type | Description |
|-----------|------|-------------|
| `emitter` | `OpenLineageEmitter` | Shared emitter for event emission |
| `namespace` | `str` | Default namespace for this adapter's jobs |
| `_installed` | `bool` | Internal flag tracking hook installation status |

**Example Implementation:**
```python
class MLflowAdapter(ToolAdapter):
    def get_tool_name(self) -> str:
        return "mlflow"

    def install_hooks(self) -> None:
        # Register MLflow tracking store plugin
        ...

    def uninstall_hooks(self) -> None:
        # Cleanup (if possible)
        ...
```

---

## Functions

### `__init__()`

```python
def __init__(
    self,
    emitter: OpenLineageEmitter,
    namespace: str = "default",
)
```

**Description:** Initializes the adapter with a shared emitter and namespace.

**Args:**
- `emitter`: `OpenLineageEmitter` instance for sending events
- `namespace`: Default namespace for jobs created by this adapter (default: `"default"`)

**Usage:**
```python
from openlineage_oai.core.emitter import OpenLineageEmitter
from openlineage_oai.core.config import OpenLineageConfig

config = OpenLineageConfig(url="http://marquez:5000", namespace="ml-platform")
emitter = OpenLineageEmitter(config)

adapter = MLflowAdapter(emitter, namespace="ml-platform")
```

---

### `get_tool_name()` (abstract)

```python
@abstractmethod
def get_tool_name(self) -> str
```

**Description:** Returns the tool identifier. This is used for:
- Job naming (e.g., `"mlflow/experiment-123/run"`)
- Logging and debugging
- Dataset registry tool parameter

**Returns:** Tool name like `"mlflow"`, `"ray"`, `"kfp"`, `"llamastack"`

**Must be implemented by subclasses.**

---

### `install_hooks()` (abstract)

```python
@abstractmethod
def install_hooks(self) -> None
```

**Description:** Installs the tool-specific hooks or plugins. Called during `openlineage_oai.init()` to set up the integration.

What "install" means varies by tool:
| Tool | Installation Method |
|------|---------------------|
| MLflow | Register tracking store plugin |
| Ray | Patch data read/write functions |
| KFP | Register component decorators |
| LlamaStack | Configure provider wrapper |

**Should be idempotent** (safe to call multiple times).

**Raises:**
- `ImportError`: If tool is not installed
- `RuntimeError`: If hooks cannot be installed

**Must be implemented by subclasses.**

---

### `uninstall_hooks()` (abstract)

```python
@abstractmethod
def uninstall_hooks(self) -> None
```

**Description:** Removes hooks and restores original tool behavior. Called during `openlineage_oai.shutdown()` to clean up.

Some adapters may not support uninstall (e.g., entry-point plugins). In that case, this should be a no-op.

**Must be implemented by subclasses.**

---

### `build_job_name()`

```python
def build_job_name(
    self,
    name: str,
    context: Optional[str] = None,
) -> str
```

**Description:** Builds a job name following the unified naming convention.

**Pattern:** `{tool}/{context}/{name}`

**Args:**
- `name`: Job/run name
- `context`: Optional context (e.g., experiment ID, pipeline name)

**Returns:** Hierarchical job name like `"mlflow/experiment-123/my-run"`

**Usage:**
```python
adapter = MLflowAdapter(emitter, namespace="ml-platform")

# Without context
job_name = adapter.build_job_name("training-run")
# "mlflow/training-run"

# With context
job_name = adapter.build_job_name("training-run", context="experiment-123")
# "mlflow/experiment-123/training-run"
```

---

### `is_installed` (property)

```python
@property
def is_installed(self) -> bool
```

**Description:** Checks if hooks are currently installed.

**Returns:** `True` if hooks are installed, `False` otherwise

**Usage:**
```python
adapter = MLflowAdapter(emitter, namespace="ml-platform")
print(adapter.is_installed)  # False

adapter.install_hooks()
print(adapter.is_installed)  # True
```

---

### `__repr__()`

```python
def __repr__(self) -> str
```

**Description:** Returns a string representation for debugging.

**Returns:** String like `"MLflowAdapter(tool=mlflow, namespace=ml-platform)"`

---

## Creating a New Adapter

To create an adapter for a new tool:

```python
from openlineage_oai.adapters.base import ToolAdapter

class MyToolAdapter(ToolAdapter):
    """Adapter for MyTool."""
    
    def get_tool_name(self) -> str:
        return "mytool"
    
    def install_hooks(self) -> None:
        try:
            import mytool
        except ImportError as e:
            raise ImportError("MyTool is not installed") from e
        
        # Install hooks/patches
        mytool.register_callback(self._on_operation)
        self._installed = True
    
    def uninstall_hooks(self) -> None:
        import mytool
        mytool.unregister_callback(self._on_operation)
        self._installed = False
    
    def _on_operation(self, event):
        """Callback for MyTool operations."""
        self.emitter.emit_run_event(
            event_type="COMPLETE",
            run_id=event.id,
            job_name=self.build_job_name(event.name),
            job_namespace=self.namespace,
        )
```

Then register in `openlineage_oai/__init__.py`:

```python
AVAILABLE_TOOLS = ["mlflow", "mytool"]

def _load_adapter(tool, emitter, namespace):
    if tool == "mytool":
        from openlineage_oai.adapters.mytool import MyToolAdapter
        return MyToolAdapter(emitter, namespace)
    # ...
```
