# Sequence Diagram: Iris Training Example

This document shows the complete flow through all components when running the iris training example.

---

## High-Level Flow

```
┌──────────────────┐     ┌─────────────────────────┐     ┌─────────────────┐     ┌─────────────────┐
│  User Script     │     │  OpenLineageTracking    │     │  OpenLineage    │     │    Marquez      │
│  (iris_training) │     │  Store                  │     │  Emitter        │     │    Server       │
└────────┬─────────┘     └───────────┬─────────────┘     └────────┬────────┘     └────────┬────────┘
         │                           │                            │                       │
         │ mlflow.set_tracking_uri() │                            │                       │
         │ ("openlineage+http://..") │                            │                       │
         │──────────────────────────>│                            │                       │
         │                           │ Parse URI, create delegate │                       │
         │                           │ store, init emitter        │                       │
         │                           │───────────────────────────>│                       │
         │                           │                            │                       │
         │ mlflow.start_run()        │                            │                       │
         │──────────────────────────>│ create_run()               │                       │
         │                           │───────────────────────────>│ emit_start()         │
         │                           │                            │──────────────────────>│ POST /lineage
         │                           │                            │                       │ RunEvent(START)
         │                           │                            │                       │
         │ mlflow.log_input()        │                            │                       │
         │──────────────────────────>│ log_inputs()               │                       │
         │                           │───────────────────────────>│ emit_dataset_event() │
         │                           │                            │──────────────────────>│ POST /lineage
         │                           │                            │                       │ DatasetEvent
         │                           │                            │                       │
         │ mlflow.log_param()        │                            │                       │
         │──────────────────────────>│ log_param()                │                       │
         │                           │ (accumulate, no emit)      │                       │
         │                           │                            │                       │
         │ mlflow.log_metric()       │                            │                       │
         │──────────────────────────>│ log_metric()               │                       │
         │                           │ (accumulate, no emit)      │                       │
         │                           │                            │                       │
         │ mlflow.end_run()          │                            │                       │
         │──────────────────────────>│ update_run_info(FINISHED)  │                       │
         │                           │───────────────────────────>│ emit_complete()      │
         │                           │                            │──────────────────────>│ POST /lineage
         │                           │                            │                       │ RunEvent(COMPLETE)
         │                           │                            │                       │ + inputs/outputs
         │                           │                            │                       │ + params/metrics
```

---

## Detailed Step-by-Step Flow

### Step 1: Set Tracking URI

```python
# iris_training.py
mlflow.set_tracking_uri("openlineage+http://mlflow-server:5000")
```

```
┌────────────────┐          ┌─────────────────────────────┐
│    MLflow      │          │  Entry Point Registry       │
│                │          │  (pyproject.toml)           │
└───────┬────────┘          └──────────────┬──────────────┘
        │                                  │
        │ URI starts with "openlineage+"   │
        │ Look up tracking_store plugin    │
        │─────────────────────────────────>│
        │                                  │
        │                   Found: openlineage_oai.adapters
        │                          .mlflow.tracking_store
        │<─────────────────────────────────│
        │                                  │
        ▼                                  
┌────────────────────────────────────────────────────────────────┐
│  OpenLineageTrackingStore.__init__()                           │
│                                                                │
│  1. parse_tracking_uri("openlineage+http://mlflow-server")     │
│     → backend_uri = "http://mlflow-server:5000"                │
│     → openlineage_url = os.environ["OPENLINEAGE_URL"]          │
│                                                                │
│  2. _create_delegate_store("http://mlflow-server:5000")        │
│     → RestStore(get_credentials)                               │
│                                                                │
│  3. OpenLineageEmitter(config)                                 │
│     → url = "http://marquez:5000"                              │
│     → namespace = "plugin-test-4"                              │
└────────────────────────────────────────────────────────────────┘
```

---

### Step 2: Start Run

```python
# iris_training.py
with mlflow.start_run(run_name="iris-rf-demo"):
```

```
┌────────────┐     ┌───────────────────────┐     ┌─────────────┐     ┌──────────┐     ┌─────────┐
│   User     │     │ OpenLineageTracking   │     │  RestStore  │     │ Emitter  │     │ Marquez │
│   Script   │     │ Store                 │     │ (delegate)  │     │          │     │         │
└─────┬──────┘     └───────────┬───────────┘     └──────┬──────┘     └────┬─────┘     └────┬────┘
      │                        │                        │                 │                │
      │ start_run()            │                        │                 │                │
      │───────────────────────>│                        │                 │                │
      │                        │                        │                 │                │
      │                        │ create_run()           │                 │                │
      │                        │───────────────────────>│                 │                │
      │                        │                        │                 │                │
      │                        │   Run(run_id="abc123") │                 │                │
      │                        │<───────────────────────│                 │                │
      │                        │                        │                 │                │
      │                        │ Initialize RunState    │                 │                │
      │                        │ ┌─────────────────┐    │                 │                │
      │                        │ │ RunState:       │    │                 │                │
      │                        │ │  run_id=abc123  │    │                 │                │
      │                        │ │  params={}      │    │                 │                │
      │                        │ │  metrics={}     │    │                 │                │
      │                        │ │  inputs=[]      │    │                 │                │
      │                        │ └─────────────────┘    │                 │                │
      │                        │                        │                 │                │
      │                        │ emit_start()           │                 │                │
      │                        │───────────────────────────────────────>│                │
      │                        │                        │                 │                │
      │                        │                        │                 │ POST /lineage  │
      │                        │                        │                 │───────────────>│
      │                        │                        │                 │                │
      │                        │                        │                 │  RunEvent:     │
      │                        │                        │                 │  type=START    │
      │                        │                        │                 │  run_id=abc123 │
      │                        │                        │                 │  job=mlflow/   │
      │                        │                        │                 │   experiment-1/│
      │                        │                        │                 │   iris-rf-demo │
      │                        │                        │                 │                │
      │   Run object           │                        │                 │      200 OK    │
      │<───────────────────────│                        │                 │<───────────────│
```

---

### Step 3: Log Input Dataset

```python
# iris_training.py
train_dataset = mlflow.data.from_pandas(df, name="iris_training_data")
mlflow.log_input(train_dataset, context="training")
```

```
┌────────────┐     ┌───────────────────────┐     ┌─────────────┐     ┌──────────┐     ┌─────────┐
│   User     │     │ OpenLineageTracking   │     │  RestStore  │     │ Emitter  │     │ Marquez │
│   Script   │     │ Store                 │     │ (delegate)  │     │          │     │         │
└─────┬──────┘     └───────────┬───────────┘     └──────┬──────┘     └────┬─────┘     └────┬────┘
      │                        │                        │                 │                │
      │ log_input(dataset)     │                        │                 │                │
      │───────────────────────>│                        │                 │                │
      │                        │                        │                 │                │
      │                        │ log_inputs()           │                 │                │
      │                        │───────────────────────>│                 │                │
      │                        │                        │                 │                │
      │                        │ extract_dataset_info() │                 │                │
      │                        │ ┌─────────────────────┐│                 │                │
      │                        │ │ utils.py:           ││                 │                │
      │                        │ │  name="iris_data"   ││                 │                │
      │                        │ │  schema=[...]       ││                 │                │
      │                        │ │  source="sklearn"   ││                 │                │
      │                        │ └─────────────────────┘│                 │                │
      │                        │                        │                 │                │
      │                        │ create_mlflow_dataset_ │                 │                │
      │                        │ facet()                │                 │                │
      │                        │ ┌─────────────────────┐│                 │                │
      │                        │ │ facets.py:          ││                 │                │
      │                        │ │  MLflowDatasetFacet ││                 │                │
      │                        │ └─────────────────────┘│                 │                │
      │                        │                        │                 │                │
      │                        │ emit_dataset_event()   │                 │                │
      │                        │───────────────────────────────────────>│                │
      │                        │                        │                 │ POST /lineage  │
      │                        │                        │                 │───────────────>│
      │                        │                        │                 │                │
      │                        │                        │                 │ DatasetEvent:  │
      │                        │                        │                 │ name=iris_data │
      │                        │                        │                 │ schema=[...]   │
      │                        │                        │                 │                │
      │                        │                        │                 │                │
      │                        │ Accumulate in RunState │                 │                │
      │                        │ ┌─────────────────────┐│                 │                │
      │                        │ │ RunState:           ││                 │                │
      │                        │ │  inputs=[{          ││                 │                │
      │                        │ │    name="iris_data" ││                 │                │
      │                        │ │  }]                 ││                 │                │
      │                        │ └─────────────────────┘│                 │                │
      │<───────────────────────│                        │                 │<───────────────│
```

---

### Step 4: Log Parameters (Accumulated, No Event)

```python
# iris_training.py
mlflow.log_param("n_estimators", 100)
mlflow.log_param("max_depth", 5)
```

```
┌────────────┐     ┌───────────────────────┐     ┌─────────────┐
│   User     │     │ OpenLineageTracking   │     │  RestStore  │
│   Script   │     │ Store                 │     │ (delegate)  │
└─────┬──────┘     └───────────┬───────────┘     └──────┬──────┘
      │                        │                        │
      │ log_param("n_est",100) │                        │
      │───────────────────────>│                        │
      │                        │                        │
      │                        │ log_param()            │
      │                        │───────────────────────>│ (stored in PostgreSQL)
      │                        │                        │
      │                        │ Accumulate (NO EMIT)   │
      │                        │ ┌─────────────────────┐│
      │                        │ │ RunState:           ││
      │                        │ │  params={           ││
      │                        │ │   "n_estimators":   ││
      │                        │ │     "100"           ││
      │                        │ │  }                  ││
      │                        │ └─────────────────────┘│
      │<───────────────────────│                        │
      │                        │                        │
      │ log_param("max_d", 5)  │                        │
      │───────────────────────>│                        │
      │                        │                        │
      │                        │ log_param()            │
      │                        │───────────────────────>│ (stored in PostgreSQL)
      │                        │                        │
      │                        │ Accumulate (NO EMIT)   │
      │                        │ ┌─────────────────────┐│
      │                        │ │ RunState:           ││
      │                        │ │  params={           ││
      │                        │ │   "n_estimators":   ││
      │                        │ │     "100",          ││
      │                        │ │   "max_depth": "5"  ││
      │                        │ │  }                  ││
      │                        │ └─────────────────────┘│
      │<───────────────────────│                        │
```

**Why no event?** Params and metrics are accumulated during the run and only emitted in the final COMPLETE event. This reduces API calls to Marquez.

---

### Step 5: Log Metrics (Accumulated, No Event)

```python
# iris_training.py
mlflow.log_metric("accuracy", 0.95)
mlflow.log_metric("f1_score", 0.94)
```

```
┌────────────┐     ┌───────────────────────┐     ┌─────────────┐
│   User     │     │ OpenLineageTracking   │     │  RestStore  │
│   Script   │     │ Store                 │     │ (delegate)  │
└─────┬──────┘     └───────────┬───────────┘     └──────┬──────┘
      │                        │                        │
      │ log_metric("acc",0.95) │                        │
      │───────────────────────>│                        │
      │                        │                        │
      │                        │ log_metric()           │
      │                        │───────────────────────>│ (stored in PostgreSQL)
      │                        │                        │
      │                        │ Accumulate (NO EMIT)   │
      │                        │ ┌─────────────────────┐│
      │                        │ │ RunState:           ││
      │                        │ │  metrics={          ││
      │                        │ │   "accuracy": 0.95  ││
      │                        │ │  }                  ││
      │                        │ └─────────────────────┘│
      │<───────────────────────│                        │
```

---

### Step 6: Log Model (Output Dataset)

```python
# iris_training.py
mlflow.sklearn.log_model(model, "model")
```

```
┌────────────┐     ┌───────────────────────┐     ┌────────────────────┐     ┌──────────┐     ┌─────────┐
│   User     │     │ OpenLineageTracking   │     │ OpenLineageArtifact│     │ Emitter  │     │ Marquez │
│   Script   │     │ Store                 │     │ Repository         │     │          │     │         │
└─────┬──────┘     └───────────┬───────────┘     └─────────┬──────────┘     └────┬─────┘     └────┬────┘
      │                        │                           │                     │                │
      │ log_model(model)       │                           │                     │                │
      │───────────────────────>│                           │                     │                │
      │                        │                           │                     │                │
      │                        │ MLflow saves model files  │                     │                │
      │                        │──────────────────────────>│                     │                │
      │                        │                           │                     │                │
      │                        │                           │ Strip "openlineage+"│                │
      │                        │                           │ Delegate to real    │                │
      │                        │                           │ artifact repo       │                │
      │                        │                           │                     │                │
      │                        │ set_tag("mlflow.log-     │                     │                │
      │                        │   model.history", json)  │                     │                │
      │                        │                           │                     │                │
      │                        │ _handle_model_history_tag │                     │                │
      │                        │ ┌───────────────────────┐ │                     │                │
      │                        │ │ Parse JSON:           │ │                     │                │
      │                        │ │  artifact_path=model  │ │                     │                │
      │                        │ │  flavors=[sklearn]    │ │                     │                │
      │                        │ └───────────────────────┘ │                     │                │
      │                        │                           │                     │                │
      │                        │ emit_dataset_event()      │                     │                │
      │                        │──────────────────────────────────────────────>│                │
      │                        │                           │                     │ POST /lineage  │
      │                        │                           │                     │───────────────>│
      │                        │                           │                     │ DatasetEvent:  │
      │                        │                           │                     │ name=model/    │
      │                        │                           │                     │   model        │
      │                        │                           │                     │                │
      │                        │ Accumulate in RunState    │                     │                │
      │                        │ ┌───────────────────────┐ │                     │                │
      │                        │ │ RunState:             │ │                     │                │
      │                        │ │  outputs=[{           │ │                     │                │
      │                        │ │    name="model/model" │ │                     │                │
      │                        │ │  }]                   │ │                     │                │
      │                        │ └───────────────────────┘ │                     │                │
      │<───────────────────────│                           │                     │<───────────────│
```

---

### Step 7: End Run (COMPLETE Event with All Data)

```python
# iris_training.py
# (implicit when exiting `with mlflow.start_run():` block)
```

```
┌────────────┐     ┌───────────────────────┐     ┌─────────────┐     ┌──────────┐     ┌─────────┐
│   User     │     │ OpenLineageTracking   │     │  RestStore  │     │ Emitter  │     │ Marquez │
│   Script   │     │ Store                 │     │ (delegate)  │     │          │     │         │
└─────┬──────┘     └───────────┬───────────┘     └──────┬──────┘     └────┬─────┘     └────┬────┘
      │                        │                        │                 │                │
      │ end_run()              │                        │                 │                │
      │───────────────────────>│                        │                 │                │
      │                        │                        │                 │                │
      │                        │ update_run_info(       │                 │                │
      │                        │   status=FINISHED)     │                 │                │
      │                        │───────────────────────>│                 │                │
      │                        │                        │                 │                │
      │                        │ Get accumulated state  │                 │                │
      │                        │ ┌─────────────────────┐│                 │                │
      │                        │ │ RunState:           ││                 │                │
      │                        │ │  params={           ││                 │                │
      │                        │ │   "n_estimators":   ││                 │                │
      │                        │ │     "100",          ││                 │                │
      │                        │ │   "max_depth": "5"  ││                 │                │
      │                        │ │  }                  ││                 │                │
      │                        │ │  metrics={          ││                 │                │
      │                        │ │   "accuracy": 0.95, ││                 │                │
      │                        │ │   "f1_score": 0.94  ││                 │                │
      │                        │ │  }                  ││                 │                │
      │                        │ │  inputs=[iris_data] ││                 │                │
      │                        │ │  outputs=[model]    ││                 │                │
      │                        │ └─────────────────────┘│                 │                │
      │                        │                        │                 │                │
      │                        │ create_mlflow_run_facet│                 │                │
      │                        │ ┌─────────────────────┐│                 │                │
      │                        │ │ facets.py:          ││                 │                │
      │                        │ │  MLflowRunFacet     ││                 │                │
      │                        │ │  with params/metrics││                 │                │
      │                        │ └─────────────────────┘│                 │                │
      │                        │                        │                 │                │
      │                        │ emit_complete()        │                 │                │
      │                        │───────────────────────────────────────>│                │
      │                        │                        │                 │ POST /lineage  │
      │                        │                        │                 │───────────────>│
      │                        │                        │                 │                │
      │                        │                        │                 │ RunEvent:      │
      │                        │                        │                 │  type=COMPLETE │
      │                        │                        │                 │  inputs=[      │
      │                        │                        │                 │   iris_data]   │
      │                        │                        │                 │  outputs=[     │
      │                        │                        │                 │   model/model] │
      │                        │                        │                 │  facets={      │
      │                        │                        │                 │   mlflow_run:  │
      │                        │                        │                 │    params={..} │
      │                        │                        │                 │    metrics={..}│
      │                        │                        │                 │  }             │
      │                        │                        │                 │                │
      │                        │ Cleanup RunState       │                 │      200 OK    │
      │                        │ (delete from memory)   │                 │<───────────────│
      │<───────────────────────│                        │                 │                │
```

---

## Final State in Marquez

After the run completes, Marquez contains:

```
┌─────────────────────────────────────────────────────────────────┐
│                          Marquez                                 │
│                                                                 │
│  Namespace: plugin-test-4                                       │
│                                                                 │
│  ┌─────────────────────────────────────────────────────────┐   │
│  │  Dataset: iris_training_data                             │   │
│  │  Schema: [sepal_length, sepal_width, petal_length, ...]  │   │
│  │  Facets: MLflowDatasetFacet                              │   │
│  └────────────────────────┬────────────────────────────────┘   │
│                           │                                     │
│                           │ INPUT                               │
│                           ▼                                     │
│  ┌─────────────────────────────────────────────────────────┐   │
│  │  Job: mlflow/experiment-15/iris-rf-demo                  │   │
│  │                                                          │   │
│  │  Latest Run: abc123 (COMPLETE)                           │   │
│  │  Facets:                                                 │   │
│  │    mlflow_run:                                           │   │
│  │      params: {n_estimators: 100, max_depth: 5}           │   │
│  │      metrics: {accuracy: 0.95, f1_score: 0.94}           │   │
│  └────────────────────────┬────────────────────────────────┘   │
│                           │                                     │
│                           │ OUTPUT                              │
│                           ▼                                     │
│  ┌─────────────────────────────────────────────────────────┐   │
│  │  Dataset: model/model                                    │   │
│  │  Facets: MLflowModelFacet                                │   │
│  │    flavors: [sklearn]                                    │   │
│  └─────────────────────────────────────────────────────────┘   │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

---

## Summary: Events Emitted

| Step | MLflow Call | OpenLineage Event | Data Included |
|------|-------------|-------------------|---------------|
| 2 | `mlflow.start_run()` | `RunEvent(START)` | job name, run ID |
| 3 | `mlflow.log_input()` | `DatasetEvent(CREATE)` | dataset name, schema, facets |
| 4 | `mlflow.log_param()` | *(accumulated)* | — |
| 5 | `mlflow.log_metric()` | *(accumulated)* | — |
| 6 | `mlflow.log_model()` | `DatasetEvent(CREATE)` | model name, flavors |
| 7 | `mlflow.end_run()` | `RunEvent(COMPLETE)` | inputs, outputs, params, metrics, facets |

**Total API calls to Marquez: 4**
- 1 × START
- 1 × DatasetEvent (input)
- 1 × DatasetEvent (output model)
- 1 × COMPLETE (with all accumulated data)
