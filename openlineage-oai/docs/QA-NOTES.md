# Q&A Notes

A running log of questions and answers about the `openlineage-oai` plugin.

---

## Plugin Architecture

### Q: What happens if a user calls an MLflow function we haven't implemented?

**A:** It works fine. We have a `__getattr__` fallback that delegates any unimplemented method to the real store:

```python
def __getattr__(self, name: str):
    """Delegate any unimplemented methods to the real store."""
    return getattr(self._delegate, name)
```

**Result:**
- MLflow always works - nothing breaks
- Forward compatible - new MLflow versions with new methods work automatically
- We only intercept what matters for lineage (run lifecycle, data operations)

**Example:** `mlflow.create_experiment()` passes through to the real store without emitting an OpenLineage event.

---

### Q: Could the plugin be a sidecar container to the MLflow deployment?

**A:** No - because of where the interception happens. Our plugin works **client-side**, inside the user's Python process. A sidecar runs **next to** a container, not inside the Python process.

```
Client-Side (Our Approach):
┌────────────────────────────────────┐
│  User's Python Process             │
│  ┌──────────────────────────────┐  │
│  │ mlflow.start_run()           │  │
│  │      ↓                       │  │
│  │ OpenLineageTrackingStore     │  │  ← Plugin intercepts HERE
│  │      ↓                       │  │
│  │ Delegate to real store       │  │
│  └──────────────────────────────┘  │
└────────────────────────────────────┘

Sidecar (Wouldn't Work):
┌──────────────────┐  ┌──────────────────┐
│ User Container   │  │ Sidecar          │
│                  │  │                  │
│ mlflow.start_    │  │ Can't intercept  │
│ run() ────────── │──│─► Python calls   │
└──────────────────┘  └──────────────────┘
```

A sidecar could only intercept **network traffic** (HTTP), not Python function calls. This would lose access to rich metadata like dataset schemas and model signatures.

**Alternative:** A proxy sidecar that intercepts HTTP to MLflow is possible but has downsides:
- Only sees HTTP bodies, not Python objects
- Can't access schemas, signatures, etc.
- More complex deployment

---

### Q: Where do log_param/log_metric actually save to OpenLineage? It just shows delegation to the original store.

**A:** We do TWO things: delegate AND accumulate. The OpenLineage emission happens later in the COMPLETE event.

**Step 1: Accumulate (during run)**
```python
def log_param(self, run_id: str, param):
    # 1. Delegate to real store (PostgreSQL/REST)
    self._delegate.log_param(run_id, param)

    # 2. Accumulate in memory for later
    with self._lock:
        if run_id in self._run_states:
            self._run_states[run_id].params[param.key] = param.value  # ← Saved here
```

**Step 2: Emit (when run ends)**
```python
def update_run_info(self, run_id, run_status, ...):
    if is_finished:
        # NOW we emit everything accumulated
        self._emitter.emit_complete(
            run_facets={"mlflow_run": {
                params=state.params,    # ← From accumulator
                metrics=state.metrics,  # ← From accumulator
            }},
            inputs=state.inputs,
            outputs=state.outputs,
        )
```

**Why this design?** Instead of emitting an event for every `log_param` call (could be hundreds), we accumulate during the run and emit once at the end. This reduces Marquez API calls from potentially hundreds to just one COMPLETE event.

---

### Q: At what point is the OpenLineageTrackingStore initialized?

**A:** It's initialized **lazily** by MLflow when you first use any tracking operation.

```
1. User sets tracking URI:
   mlflow.set_tracking_uri("openlineage+http://mlflow-server:5000")
   └─► Just stores the URI string - nothing initialized yet

2. User calls any MLflow tracking function:
   mlflow.get_experiment_by_name("my-exp")
   │
   ▼
3. MLflow needs a store - looks at URI scheme:
   URI: "openlineage+http://mlflow-server:5000"
   Scheme: "openlineage+http"
   │
   ▼
4. MLflow checks entry point registry:
   "Is there a tracking_store plugin for 'openlineage+http'?"
   │
   ▼
5. Finds our entry point (from pyproject.toml):
   "openlineage+http" = "openlineage_oai...tracking_store:OpenLineageTrackingStore"
   │
   ▼
6. MLflow imports and instantiates our class:
   store = OpenLineageTrackingStore(store_uri="openlineage+http://...", ...)
   │
   ▼
7. Our __init__ runs:
   - Parse URI, strip "openlineage+" prefix
   - Create delegate store (RestStore for http)
   - Create OpenLineage emitter
   │
   ▼
8. MLflow caches the store for future calls
```

**Key point:** `set_tracking_uri()` doesn't trigger initialization - the first actual tracking operation does.

---
