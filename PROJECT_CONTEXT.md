# OpenLineage Integration for OpenShift AI — Project Context

This document captures the full technical context of the OpenLineage lineage integration work
for an ML pipeline running on OpenShift AI. It is intended to give any developer or LLM complete
situational awareness of the architecture, the problems encountered, and how they were solved.

---

## 1. Goal

Build production-grade, end-to-end data lineage for an ML pipeline running on OpenShift AI,
visible through a Marquez UI. The pipeline covers everything from raw CSV ingestion through to
a registered MLflow model. Every tool in the chain should emit OpenLineage events that connect
into a single, unbroken lineage graph.

The lineage graph should look like this:

```
customers.csv → Spark ETL → warehouse.customer_features (Postgres)
                                  ↓
                            Feast apply/materialize → customer_features_view → online_store
                                  ↓
                            ds_data_extraction (SDK) → KFP artifact (S3)
                                  ↓
                            ds_feature_engineering (SDK) → KFP artifact (S3)
                                  ↓
                            MLflow model training → model/model
```

---

## 2. Architecture Overview

### 2.1 Infrastructure (OpenShift)

All services run in the `lineage` namespace on an OpenShift (ROSA) cluster:

| Service | Purpose |
|---|---|
| **Marquez** | OpenLineage backend + lineage graph UI |
| **PostgreSQL** (`postgres`) | Stores the feature warehouse (`warehouse` DB), Feast registry |
| **Redis** | Feast online store |
| **MinIO** (`mlflow-minio`) | S3-compatible object store for raw data + MLflow artifacts + KFP artifacts |
| **MLflow** (`mlflow-server`) | Experiment tracking and model registry |
| **DSPA** (Data Science Pipelines Application) | OpenShift AI's native KFP v2 runner |

`OPENLINEAGE_NAMESPACE` is injected into every pod by the Argo workflow controller via
`fieldRef: metadata.namespace`, so all tools default to the `lineage` namespace.

### 2.2 Pipeline Steps

The pipeline is defined in `src/pipeline/kfp_pipeline.py` as a KFP v2 DSL pipeline. Steps are
split into **Platform** (infra-managed) and **DS** (data-scientist-owned):

| Step | Image | Lineage Source |
|---|---|---|
| `platform_spark_etl` | `spark-etl:latest` | **Native** — Spark OpenLineage listener (`openlineage-spark_2.12-1.45.0.jar`) |
| `platform_feast_apply` | `fkm-app:latest` | **Native** — Feast OpenLineage emitter (local fork) |
| `platform_feast_materialize` | `fkm-app:latest` | **Native** — Feast OpenLineage emitter (local fork) |
| `ds_data_extraction` | `fkm-app:latest` | **Manual** — `openlineage-sdk` (`OLClient.emit_job`) |
| `ds_feature_engineering` | `fkm-app:latest` | **Manual** — `openlineage-sdk` (`OLClient.emit_job`) |
| `ds_model_training` | `fkm-app:latest` | **Native** — MLflow via `openlineage-oai` adapter |
| `ds_evaluation` | `fkm-app:latest` | None (pure computation, no data I/O) |
| `ds_model_registration` | `fkm-app:latest` | None (MLflow registry call only) |

### 2.3 Docker Images

Two images are built via OpenShift BuildConfigs:

**`fkm-app`** (`Dockerfile`): UBI9 Python 3.11 image containing:
- All Python deps from `requirements.txt`
- Local Feast wheel (fork with mapper fix) from `wheels/`
- The `openlineage-sdk` package (installed from `openlineage-sdk/`)
- Pipeline source code (`src/`), Feast feature store config, sample data

**`spark-etl`** (`Dockerfile.spark`): UBI9 Python 3.11 + Java 17 image containing:
- PySpark 3.5.4
- OpenLineage Spark listener JAR v1.45.0
- PostgreSQL JDBC driver, Hadoop AWS JARs
- `src/etl/spark_etl.py`

### 2.4 Key Namespaces in Marquez

| Namespace | Used by |
|---|---|
| `lineage` | Feast jobs, SDK jobs, MLflow jobs |
| `postgres://postgres:5432` | Spark-written and Feast-read Postgres datasets |
| `s3://raw-data` | Spark-read raw CSV |
| `s3://pipeline-artifacts` | KFP intermediate artifacts (parquet files written by DS steps) |

---

## 3. Tool-by-Tool Integration Details

### 3.1 Spark ETL

**File**: `src/etl/spark_etl.py`

The Spark job reads `s3a://raw-data/customers.csv` from MinIO, transforms the data
(dedup, null fill, normalise), and writes to `jdbc:postgresql://postgres:5432/warehouse`
table `customer_features`.

Lineage is emitted automatically by the native OpenLineage Spark listener, configured via
Spark session config:

```python
.config("spark.extraListeners", "io.openlineage.spark.agent.OpenLineageSparkListener")
.config("spark.openlineage.transport.type", "http")
.config("spark.openlineage.transport.url", OPENLINEAGE_URL)
.config("spark.openlineage.namespace", OPENLINEAGE_NAMESPACE)
.config("spark.openlineage.transport.urlParams.replaceDatasetNamespacePattern", "s3a://->s3://")
```

The `replaceDatasetNamespacePattern` config normalises `s3a://` URIs to `s3://` so that
dataset namespaces match what other tools use.

**Key output dataset identity**:
- Namespace: `postgres://postgres:5432`
- Name: `warehouse.customer_features`

The Spark listener derives this automatically from the JDBC URL. This identity is what
every downstream tool must match to keep the graph connected.

**Past issue — "bridge lineage" hack**: A previously deployed `spark-etl` image contained
a manual OpenLineage emitter that overrode the native listener's output, emitting
`lineage:customer_features_source` instead of the correct JDBC-derived identity. This was
fixed by rebuilding the image from the clean local code.

### 3.2 Feast

**Files modified** (local fork, built into a wheel at `wheels/`):
- `feast/sdk/python/feast/feature_store.py` — passes `self.config` (RepoConfig) to the emitter
- `feast/sdk/python/feast/openlineage/emitter.py` — accepts `repo_config` parameter
- `feast/sdk/python/feast/openlineage/mappers.py` — new `_resolve_data_source_identity()` function

**The core problem**: Feast's OpenLineage mapper had no access to the offline store connection
details. When mapping a data source to an OpenLineage dataset, it could only see the
`DataSource` object (which has a table name but no host/port/database). It fell back to
`{namespace}/{source_name}` (e.g. `lineage/customer_features_source`), which didn't match
Spark's URI-based identity.

**The fix**: Modified the mapper pipeline so that `RepoConfig` flows from
`FeatureStore → FeastOpenLineageEmitter → mappers`. The new `_resolve_data_source_identity()`
function checks if the data source is a `PostgreSQLSource` and, when `repo_config` is available,
constructs the identity from the offline store config:

```python
# Produces: namespace = "postgres://postgres:5432", name = "warehouse.customer_features"
ns = f"postgres://{host}:{port}"
name = f"{database}.{table}"
```

This exactly matches what Spark emits, so Marquez sees them as the same dataset and the graph
connects.

**Feast project name**: Set to `{OPENLINEAGE_NAMESPACE}` (i.e. `lineage`) so that Feast jobs
appear in the correct Marquez namespace. The `feature_store.yaml` is written dynamically in each
KFP component with the correct host/port values for the cluster.

**Feast OpenLineage config** (in the dynamically written `feature_store.yaml`):
```yaml
openlineage:
  enabled: true
  transport_type: http
  transport_url: http://marquez
  emit_on_apply: true
  emit_on_materialize: true
```

### 3.3 MLflow (via openlineage-oai adapter)

**Adapter**: `openlineage-oai` — a tracking store wrapper that intercepts MLflow calls and
emits OpenLineage events.

Activated by using the `openlineage+http://` scheme in the tracking URI:

```python
tracking_uri = "openlineage+http://mlflow-server:5000"
```

The model training step logs an input dataset using `mlflow.log_input()` with a
`URIDatasetSource` pointing to the upstream KFP artifact URI:

```python
train_dataset = mlflow.data.from_pandas(df, source=URIDatasetSource(dataset.uri), name="engineered_features")
mlflow.log_input(train_dataset, context="training")
```

This `dataset.uri` is the S3 path of the feature-engineered parquet file output by the previous
KFP step. Because the SDK emits an event declaring that same S3 path as an output, Marquez
connects the SDK's feature engineering job to MLflow's training job.

### 3.4 OpenLineage SDK (Manual Emission)

**Directory**: `openlineage-sdk/`

A lightweight Python SDK for manually emitting OpenLineage events, created for pipeline steps
that don't have native integration. This is what bridges the gap between Feast and MLflow in
the current pipeline.

**Why it exists**: KFP v2 does not have native OpenLineage integration. The intermediate DS
steps (`ds_data_extraction`, `ds_feature_engineering`) read from Feast and write parquet files
to S3 via KFP artifacts, but nothing was emitting lineage for these steps. This left a gap in
the graph between Feast's output and MLflow's input.

**How it's used in the pipeline** (`kfp_pipeline.py`):

In `ds_data_extraction`:
```python
parsed = urlparse(output_path.uri)
OLClient(url="http://marquez").emit_job(
    "ds_data_extraction",
    inputs=[Dataset(source=ol_namespace, name="customer_features_view")],
    outputs=[Dataset(source=f"{parsed.scheme}://{parsed.netloc}", name=parsed.path.lstrip("/"))],
)
```

In `ds_feature_engineering`:
```python
parsed_in = urlparse(dataset.uri)
parsed_out = urlparse(output_path.uri)
OLClient(url="http://marquez").emit_job(
    "ds_feature_engineering",
    inputs=[Dataset(source=f"{parsed_in.scheme}://{parsed_in.netloc}", name=parsed_in.path.lstrip("/"))],
    outputs=[Dataset(source=f"{parsed_out.scheme}://{parsed_out.netloc}", name=parsed_out.path.lstrip("/"))],
)
```

The KFP artifact URIs are parsed to extract the S3 bucket as the namespace and the path as the
dataset name. This creates dataset nodes in Marquez that match both the upstream KFP output and
the downstream MLflow input.

**Important**: The URL must be passed explicitly (`url="http://marquez"`) because `OPENLINEAGE_URL`
is not set in the KFP pod environment for these steps. Without this, the SDK silently skips
emission.

**Public API**:

| Method | Purpose |
|---|---|
| `emit_job(job_name, inputs, outputs)` | Fire-and-forget single COMPLETE event |
| `track(job_name, inputs, outputs)` | Decorator/context manager with START/COMPLETE/FAIL lifecycle |
| `dataset(name)` | Resolve a dataset identity by name from the Dataset Registry |

**Dataset Registry integration**: `OLClient.dataset("Customer Features")` calls the Dataset
Registry API (`/api/v1/datasets/lookup?name=Customer Features`), which returns the canonical
`ol_namespace` and `ol_name`. The SDK wraps these into a `Dataset(source=..., name=...)` object.
This allows users to reference datasets by human-readable name without needing to know the
underlying URI format.

### 3.5 KFP v2 (Kubeflow Pipelines)

KFP v2 **does not have native OpenLineage integration**. It serves as the orchestrator — it
runs the steps and passes artifacts between them, but it does not emit lineage events.

The `dsl.importer` component was considered as a mechanism: it allows declaring external
artifacts (by URI) as pipeline inputs, which would register them in ML Metadata. However, this
still requires user-added code in the pipeline definition and doesn't emit OpenLineage events
on its own. It would only be useful if a KFP-to-OpenLineage bridge existed.

For now, the SDK fills this gap. A proper KFP OpenLineage integration would need to:
1. Intercept artifact creation/consumption at the executor level
2. Emit OpenLineage events with the correct dataset identities (parsed from artifact URIs)
3. Handle the mapping of KFP artifact types to OpenLineage dataset facets

---

## 4. The Dataset Identity Problem

The single hardest problem in this project is **dataset identity correlation**. For the lineage
graph to be connected, every tool must refer to the same dataset using the exact same
namespace + name pair.

### What each tool naturally emits

| Tool | Namespace | Name | Format |
|---|---|---|---|
| Spark (JDBC write) | `postgres://postgres:5432` | `warehouse.customer_features` | Derived from JDBC URL |
| Feast (before fix) | `lineage` | `customer_features_source` | `{project}/{source_name}` |
| Feast (after fix) | `postgres://postgres:5432` | `warehouse.customer_features` | Derived from RepoConfig offline store |
| SDK | Whatever you pass | Whatever you pass | User-controlled |
| MLflow | `lineage` | `model/model` | Derived from experiment/artifact |

The Feast fix was the critical change — without it, Spark and Feast referred to the same
physical Postgres table using completely different identities, breaking the graph.

### How the chain connects

```
Spark outputs:  postgres://postgres:5432 / warehouse.customer_features
                                    ↕  (same identity)
Feast reads:    postgres://postgres:5432 / warehouse.customer_features
Feast outputs:  lineage / customer_features_view
                                    ↕  (same identity)
SDK reads:      lineage / customer_features_view
SDK outputs:    s3://pipeline-artifacts / <kfp-artifact-path>
                                    ↕  (same identity)
SDK reads:      s3://pipeline-artifacts / <kfp-artifact-path>
SDK outputs:    s3://pipeline-artifacts / <kfp-artifact-path-2>
                                    ↕  (same identity)
MLflow reads:   s3://pipeline-artifacts / <kfp-artifact-path-2>
MLflow outputs: lineage / model/model
```

Every `↕` is a join point in Marquez. If either side uses a different string, the graph breaks.

---

## 5. Dataset Registry

**Directory**: `dataset-registry/`

A FastAPI service + PostgreSQL backend that stores canonical dataset metadata including
OpenLineage namespace (`ol_namespace`) and name (`ol_name`). It has its own Python SDK client.

**Key endpoints**:
- `GET /api/v1/datasets/lookup?source=<uri>` — look up by source URI
- `GET /api/v1/datasets/lookup?name=<name>` — look up by human-readable name (added for SDK integration)

The registry is not used in the pipeline itself (the pipeline constructs identities from runtime
URIs), but it's integrated into the `openlineage-sdk` so that users writing custom steps can
resolve dataset identities by name instead of hardcoding URIs.

---

## 6. Problems Encountered and Solutions

### 6.1 Feast dataset identity mismatch
- **Symptom**: Spark and Feast trees disconnected in Marquez
- **Cause**: Feast mapper had no access to offline store connection details, fell back to `{namespace}/{source_name}`
- **Fix**: Threaded `RepoConfig` through `FeatureStore → Emitter → Mappers`, added `_resolve_data_source_identity()` to construct URI-based identities for `PostgreSQLSource`

### 6.2 Spark "bridge lineage" hack
- **Symptom**: Spark emitting `lineage:customer_features_source` instead of `postgres://postgres:5432/warehouse.customer_features`
- **Cause**: Previously deployed image contained a manual OL emitter overriding the native listener
- **Fix**: Rebuilt `spark-etl` image from clean local code

### 6.3 SDK events not emitting
- **Symptom**: `ds_data_extraction` and `ds_feature_engineering` logged "lineage emitted" but no events appeared in Marquez
- **Cause**: `OPENLINEAGE_URL` env var not set in KFP pods; SDK silently skipped emission
- **Fix**: Pass URL explicitly: `OLClient(url="http://marquez")`

### 6.4 MLflow input disconnect
- **Symptom**: MLflow training job appeared as an isolated node
- **Cause**: MLflow input was referencing a KFP artifact URI that no other job had declared as an output
- **Fix**: Added SDK `emit_job()` calls to intermediate steps that declare those KFP artifact URIs as outputs, completing the chain

### 6.5 Feast namespace confusion
- **Symptom**: Feast jobs appearing in a `feast/lineage` namespace instead of `lineage`
- **Cause**: Feast prepends `{namespace}/{project}` when a custom namespace is set
- **Fix**: Set Feast project name to the `OPENLINEAGE_NAMESPACE` env var (`lineage`), which makes Feast use the project name directly as the namespace

### 6.6 S3 URI scheme mismatch
- **Symptom**: Spark dataset namespace showing `s3a://raw-data` while other tools use `s3://raw-data`
- **Cause**: Spark uses `s3a://` for Hadoop filesystem access
- **Fix**: Spark config `replaceDatasetNamespacePattern: "s3a://->s3://"` normalises the scheme

### 6.7 KFP wrapper vs native tool: schema facets and output edges

#### The problem

When a KFP pipeline step wraps a tool that has its own OpenLineage integration (Spark,
Feast, MLflow), there are **two independent emitters** producing events for the **same
dataset**: the native tool and the KFP wrapper (`kfp_lineage`).

Marquez creates a new **dataset version** each time any job declares a dataset as an output
in a `COMPLETE` event. The facets on the latest version are whatever that event includes —
Marquez does not merge facets across versions. This means whichever emitter fires last
"wins" and determines what metadata the UI shows.

The execution order inside a KFP component is:

```
1. kfp_lineage.__enter__()  →  emits START event
2. Native tool runs          →  emits its own START → COMPLETE (with schema facets)
3. kfp_lineage.__exit__()   →  emits COMPLETE event
```

Because `kfp_lineage` COMPLETE fires **after** the native tool, it always creates the
newest dataset version. If that version includes no schema facet, it overwrites the native
tool's schema.

#### The trade-off

There is a direct conflict between two goals:

| Goal | Requires |
|---|---|
| **Dataset schema visible** in Marquez entity view | The *latest* dataset version must include a `SchemaDatasetFacet` |
| **KFP job shows output edge** in lineage graph | The KFP job's `COMPLETE` event must declare the dataset as an output |

You cannot achieve both simultaneously for platform steps (where a native tool is the
authoritative schema source) without workarounds, because:

- If KFP COMPLETE declares the output → edge appears, but creates a schema-less version
  that overwrites the native tool's schema.
- If KFP COMPLETE omits the output → schema preserved, but no edge from the KFP job to
  the dataset in the graph.
- Declaring outputs only in the START event does not work — Marquez only creates job I/O
  edges from COMPLETE (or FAIL) events.

#### Current state

The pipeline currently **includes outputs in KFP COMPLETE** for all steps, giving a
fully connected lineage tree. For platform steps (`kfp-spark_etl`, `kfp-feast_apply`,
`kfp-feast_materialize`) this means the KFP COMPLETE overwrites the native tool's
schema — those datasets show edges but no schema in the Marquez UI. For data science
steps where KFP is the sole emitter (`ds_data_extraction`, `ds_feature_engineering`),
the `kfp_output_with_schema()` helper extracts schema from the pandas DataFrame at
runtime and includes it in the COMPLETE event, so both the edge and schema are present.

#### Possible resolutions

1. **Accept missing edges on platform KFP jobs** — the native tool's job nodes still
   show correct I/O. The KFP jobs appear as orchestration wrappers without output edges,
   but the lineage chain is not broken because the native tool's job provides the
   connection.

2. **Accept missing schema on platform outputs** — restore outputs on KFP COMPLETE
   (edges appear), and accept that schema is only visible on datasets where KFP is the
   sole emitter. The lineage graph is fully connected; schema is a cosmetic loss.

3. **Fetch-and-forward schema** — before emitting KFP COMPLETE, query the Marquez API
   for each output's current schema facet and include it in the event. This preserves
   both edges and schema but adds runtime coupling to the Marquez API and complexity.

4. **Marquez-side fix** — contribute a change to Marquez so that COMPLETE events without
   a given facet do not overwrite that facet on the dataset. This would be a facet-merge
   semantic rather than the current replace semantic. This is the cleanest long-term
   solution but requires upstream changes.

5. **Emit KFP output events before the native tool** — restructure emission so KFP
   declares outputs early (e.g. via a RUNNING event), then the native tool's COMPLETE
   overwrites with schema. Requires testing whether Marquez creates edges from RUNNING
   events (it may not).

#### Recommendation for production

This trade-off exists for any platform that orchestrates tools with their own OpenLineage
integrations. It is not specific to this demo — any KFP/Airflow/Argo wrapper around
Spark/Feast/MLflow will encounter the same "last writer wins" behaviour in Marquez. The
engineering team should evaluate options 3 or 4 depending on whether the priority is a
quick fix (option 3) or a proper upstream solution (option 4).

---

## 7. Repository Structure

```
practice-mlops/
├── Dockerfile                    # fkm-app image (Python deps + Feast wheel + SDK + source)
├── Dockerfile.spark              # spark-etl image (PySpark + OL listener + ETL script)
├── requirements.txt              # Python deps (includes local Feast wheel, openlineage-oai)
├── customer_churn_pipeline.yaml  # Compiled KFP v2 pipeline YAML (upload to OpenShift AI)
├── wheels/                       # Local Feast wheel with mapper fix
├── src/
│   ├── etl/
│   │   └── spark_etl.py          # Spark ETL job (CSV → transform → Postgres)
│   ├── pipeline/
│   │   └── kfp_pipeline.py       # KFP v2 pipeline definition (all steps)
│   └── feature_store/
│       ├── feature_store.yaml    # Template (overwritten at runtime in each component)
│       └── features.py           # Feast feature definitions
├── openlineage-sdk/              # Manual OL emission SDK
│   ├── openlineage_sdk/
│   │   ├── __init__.py           # Exports OLClient, Dataset
│   │   ├── client.py             # OLClient: emit_job, track, dataset
│   │   └── models.py             # Dataset dataclass
│   ├── pyproject.toml
│   ├── Dockerfile                # SDK container image
│   └── README.md                 # Full API docs with examples
├── dataset-registry/             # Dataset Registry service
│   ├── backend/
│   │   ├── app.py                # FastAPI API (lookup by source or name)
│   │   └── db.py                 # PostgreSQL access layer
│   └── sdk/
│       └── dataset_registry/
│           └── client.py         # Python SDK client for the registry
├── feast/                        # Local Feast fork (upstream 0.61.x + our mapper fix)
│   └── sdk/python/feast/
│       ├── feature_store.py      # Modified: passes RepoConfig to emitter
│       └── openlineage/
│           ├── emitter.py        # Modified: accepts repo_config
│           └── mappers.py        # Modified: _resolve_data_source_identity()
├── openlineage-oai/              # MLflow OpenLineage adapter (openlineage-oai)
│   └── ...                       # Provides openlineage+http:// tracking store wrapper
├── configs/                      # Infrastructure manifests
└── data/
    └── customers.csv             # Sample dataset
```

---

## 8. Build and Deploy

### Rebuild fkm-app image
```bash
cd practice-mlops
oc start-build fkm-app --from-dir=. --follow -n lineage
```

### Rebuild spark-etl image
```bash
cd practice-mlops
oc start-build spark-etl --from-dir=. --follow -n lineage
```

### Recompile pipeline
```bash
cd practice-mlops
python -m src.pipeline.kfp_pipeline
# Produces: customer_churn_pipeline.yaml
```

Upload `customer_churn_pipeline.yaml` via the OpenShift AI Data Science Pipelines UI.

### Clean Marquez (for fresh runs)
Use the Marquez API to soft-delete jobs and datasets in each namespace before re-running:
```bash
MARQUEZ="http://marquez-lineage.apps.<cluster-domain>"
curl -sL -X DELETE "$MARQUEZ/api/v1/namespaces/lineage/jobs/<job-name>"
curl -sL -X DELETE "$MARQUEZ/api/v1/namespaces/<encoded-namespace>/datasets/<encoded-dataset>"
```

### Container image locations
| Image | Registry |
|---|---|
| fkm-app | `image-registry.openshift-image-registry.svc:5000/lineage/fkm-app:latest` |
| spark-etl | `image-registry.openshift-image-registry.svc:5000/lineage/spark-etl:latest` |
| Marquez | `quay.io/rh_et_wd/lineage/marquez` |
| Marquez Web | `quay.io/rh_et_wd/lineage/marquez-web` |
| OL SDK | `quay.io/rh_et_wd/lineage/sdk:latest` |

---

## 9. Known Gaps and Future Work

### KFP native OpenLineage integration
KFP v2 has no native OL support. The `kfp_lineage` context manager fills this gap but
requires manually hardcoding dataset identities (namespace + name) for every input and
output. These must exactly match the identities emitted by native tools (Spark, Feast,
MLflow) or the graph will fork into disconnected nodes for the same physical dataset.
KFP's artifact system (`dsl.Input`/`dsl.Output`) tracks URIs between steps but carries
no schema metadata and has no OpenLineage awareness. A proper integration would intercept
artifact creation at the executor level and emit events automatically, but this would
require changes to `kfp-backend` or the Argo workflow controller — a significant upstream
contribution.

### KFP parent-child lineage via environment variable override
The OpenLineage spec uses `ParentRunFacet` to link a child job's run to its parent. In
KFP on DSPA, the Argo workflow controller injects `KFP_RUN_ID` and `KFP_PIPELINE_NAME`
into every pod, pointing at the top-level pipeline run. Tools with their own OL
integrations (MLflow, Feast) read these env vars to build their parent facet, which means
they report the **pipeline** as their parent rather than the specific **KFP step** that
invoked them.

To fix this, `kfp_lineage.__enter__()` overwrites `OPENLINEAGE_PARENT_RUN_ID` and
`OPENLINEAGE_PARENT_JOB_NAME` with its own run ID and job name before yielding control
to the component body. Any nested emitter (MLflow tracking store, Feast emitter) then
picks up the KFP step as its parent. The original pipeline-level parent is captured
before the overwrite and used for the KFP step's own START/COMPLETE events.

This is a workaround — it relies on environment variable mutation within a single
process to propagate parent context. A cleaner solution would be an explicit parent
context object passed through the call chain, or an OpenLineage context propagation
standard (similar to OpenTelemetry's `traceparent`). Neither exists today.

### Feast `get_historical_features` lineage
Feast emits lineage for `apply` and `materialize`, but not for `get_historical_features`.
This is the operation used by `ds_data_extraction` to retrieve training data. The SDK
covers this gap, but ideally Feast would emit an event for this operation natively.

### Feast fork maintenance
The mapper fix is a local change to the Feast SDK. It should ideally be contributed
upstream so future Feast versions handle PostgreSQL dataset identity correctly out of
the box. The change is minimal (only affects OL event generation, not Feast's core
functionality) — `feature_store.py`, `emitter.py`, and `mappers.py`.

### Great Expectations
The `requirements.txt` includes `great-expectations>=1.0.0` but its native OL integration
is incompatible with GE 1.x. The comment notes "OL events emitted manually" — this would
use the SDK.

### Dataset Registry adoption
The registry is integrated into the SDK but not yet used in the pipeline itself (pipeline
steps construct identities from runtime URIs). Future work could have the registry serve as
the single source of truth for all dataset identities across the platform.

### Marquez "last writer wins" for dataset facets
When multiple OpenLineage emitters produce events for the same dataset, the last COMPLETE
event determines the dataset version shown in the UI. This creates a trade-off between
graph connectivity and metadata richness for any orchestrator wrapping native OL tools.
See **§6.7** for full analysis and resolution options.
