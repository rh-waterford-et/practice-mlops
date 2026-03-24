# OpenLineage Integration on OpenShift AI: Findings and Recommendations

This document captures the technical findings from integrating OpenLineage across a multi-tool KFP pipeline (Spark, Feast, MLflow) on OpenShift AI. It is intended for the OpenShift AI team as a reference for what works, what required workarounds, and what should be addressed at the platform level.

---

## 1. Automatic Namespace Injection via Argo Workflow Controller

### The requirement

Every tool in the pipeline needs to know which OpenLineage namespace to emit events into. This must happen transparently -- data scientists should not configure lineage, and pipeline definitions should not contain namespace parameters.

### The solution

The Argo Workflow Controller (managed by DSPA) supports a `mainContainer` field in its ConfigMap that applies default container settings to every pod it creates. We use this to inject `OPENLINEAGE_NAMESPACE` via the Kubernetes Downward API:

```yaml
data:
  mainContainer: |
    env:
      - name: OPENLINEAGE_NAMESPACE
        valueFrom:
          fieldRef:
            fieldPath: metadata.namespace
```

This means every KFP step pod automatically gets `OPENLINEAGE_NAMESPACE` set to its Kubernetes namespace (e.g. `lineage`). No pipeline code touches this value.

### How each tool reads it

| Tool | How it consumes `OPENLINEAGE_NAMESPACE` |
|---|---|
| **Spark** | Read into `spark.openlineage.namespace` via Python `os.getenv()` at session creation |
| **Feast** | Read from `os.environ` and written into the `feature_store.yaml` `openlineage.namespace` field at runtime |
| **MLflow** | The `openlineage-oai` tracking store adapter reads `OPENLINEAGE_NAMESPACE` from the environment and uses it as the default job namespace |

### Limitation

The ConfigMap `ds-pipeline-workflow-controller-dspa` is owned by the DSPA operator. If the operator reconciles or the DSPA CR is reapplied, the patch is overwritten. See [namespace-injection.md](../openshift/dsp/namespace-injection.md) for production alternatives (DSPA CR native support, MutatingAdmissionWebhook, GitOps post-patch).

### Recommendation to OpenShift AI team

Expose a `workflowController.mainContainer.env` field in the `DataSciencePipelinesApplication` CR spec so that platform teams can declaratively inject environment variables into all pipeline pods without patching operator-managed resources.

---

## 2. PostgreSQL Scheme Normalisation (`postgresql://` vs `postgres://`)

### The problem

The OpenLineage Spark integration includes a `PostgresJdbcExtractor` (added in [PR #2806](https://github.com/OpenLineage/OpenLineage/pull/2806)) that intentionally normalises the JDBC scheme `postgresql://` to `postgres://` when deriving dataset namespaces. This is the canonical OpenLineage convention for PostgreSQL.

However, PostgreSQL connection strings in application code universally use `postgresql://` (the libpq/JDBC standard). When our MLflow adapter parsed a source URI like `postgresql://host:5432/db.table`, it produced namespace `postgresql://host:5432` -- which did not match Spark's `postgres://host:5432`. The result was two separate dataset nodes in Marquez for the same physical table, breaking the lineage chain.

### The fix

We added a scheme alias table to the adapter's `parse_ol_identity()` function:

```python
_SCHEME_ALIASES: dict[str, str] = {
    "postgresql": "postgres",
}
```

Any source URI using `postgresql://`, `jdbc:postgresql://`, or `postgres://` now resolves to the canonical `postgres://` namespace. This is a one-line extension point -- other databases (e.g. `mysql` vs `mariadb`) can be added to the table as needed.

### Ramifications

- All tools that emit or consume PostgreSQL dataset identities must agree on `postgres://` as the scheme. The Spark listener does this natively; the MLflow adapter now does it via normalisation.
- If OpenShift AI ships a built-in OpenLineage adapter for any tool, it must follow the same convention or dataset identities will fragment.
- The normalisation is applied at identity derivation time, not at the source URI level. Users can continue to write `postgresql://` in their code; the adapter handles the translation.

### Recommendation to OpenShift AI team

Document `postgres://` as the canonical OpenLineage namespace scheme for PostgreSQL datasets. Any platform-level lineage tooling should normalise to this convention, matching the upstream OpenLineage Spark integration.

---

## 3. Feast Appends Project Name to Namespace

### The behaviour

When Feast emits OpenLineage events, it constructs the namespace as `{configured_namespace}/{feast_project_name}`. For example, with `OPENLINEAGE_NAMESPACE=lineage` and a Feast project named `customer_churn`, Feast emits to `lineage/customer_churn`.

This means Feast jobs and datasets live in a different OL namespace (`lineage/customer_churn`) than Spark and MLflow jobs (which use `lineage` directly).

### Why this is correct

Feast is infrastructure, not a pipeline step in the data scientist's workflow. It runs on a schedule (cron), materialises features into an online store, and makes them available for serving. It is a separate operational concern from the Spark ETL and MLflow training pipeline.

The split namespaces reflect this separation:

| Namespace | Contents | Owner |
|---|---|---|
| `lineage` | Spark ETL jobs, MLflow training jobs, model artifacts | Data science pipeline |
| `lineage/customer_churn` | Feast feature views, entity definitions, online store materialisation | Platform/infrastructure |
| `postgres://postgres:5432` | Physical PostgreSQL datasets (shared between tools) | Derived from storage authority |

The lineage graph still connects across namespaces through shared dataset identities. When Spark writes to `postgres://postgres:5432/warehouse.customer_features` and MLflow reads from the same dataset identity, Marquez links them regardless of job namespace.

### Recommendation to OpenShift AI team

Do not force all tools into a single OL namespace. The OpenLineage spec is designed for cross-namespace dataset correlation. Feast's project-scoped namespace is a feature, not a bug -- it allows infrastructure lineage and pipeline lineage to be viewed independently while still connecting through physical datasets.

---

## 4. Spark Native Listener: Configuration and `s3a://` Normalisation

### The configuration

The Spark OpenLineage listener requires explicit activation in the Spark session. The JAR must be on the classpath and the listener must be registered:

```python
.config("spark.jars", "/opt/spark/jars/openlineage-spark.jar,...")
.config("spark.driver.extraClassPath", "/opt/spark/jars/openlineage-spark.jar:...")
.config("spark.extraListeners", "io.openlineage.spark.agent.OpenLineageSparkListener")
.config("spark.openlineage.transport.type", "http")
.config("spark.openlineage.transport.url", OPENLINEAGE_URL)
.config("spark.openlineage.namespace", OPENLINEAGE_NAMESPACE)
```

The listener reads `OPENLINEAGE_URL` and `OPENLINEAGE_NAMESPACE` from environment variables or Spark config properties. On OpenShift AI, both are available in the pod environment (URL from the KFP component, namespace from the Argo injection).

### `s3a://` to `s3://` normalisation

Spark uses the Hadoop `s3a://` scheme for S3-compatible storage, but other tools (MLflow, dataset registries) use the canonical `s3://` scheme. The listener supports a pattern replacement config:

```python
.config("spark.openlineage.transport.urlParams.replaceDatasetNamespacePattern", "s3a://->s3://")
```

Without this, the same MinIO bucket appears as two different dataset namespaces (`s3a://raw-data` and `s3://raw-data`), breaking lineage correlation.

### Listener granularity

The native Spark listener emits one job per Spark execution stage (file scan, shuffle, JDBC write, etc.), not one job per Spark application. This produces a detailed but verbose lineage graph with ~8 jobs for a simple ETL. This is by design for Spark-centric environments where stage-level detail is valuable.

For multi-tool pipelines where Spark is one step among many, this level of detail may be excessive. A "coarse" mode that emits a single job per `SparkSession` lifecycle would be a useful upstream contribution to the OpenLineage Spark integration.

### Recommendation to OpenShift AI team

If OpenShift AI provides a managed Spark runtime, pre-configure the OpenLineage listener JAR on the classpath and set `spark.extraListeners` as a default. The `s3a://->s3://` pattern replacement should also be a default for any environment using S3-compatible storage (MinIO, Ceph, etc.). This eliminates two common integration pitfalls.

---

## 5. Dataset Identity is the Correlation Key

### The principle

OpenLineage connects lineage across tools through **dataset identity**, not through job namespaces or execution order. A dataset identity is the tuple `(namespace, name)`:

- **Namespace**: derived from the storage authority (e.g. `postgres://host:port`, `s3://bucket`)
- **Name**: derived from the path within that authority (e.g. `warehouse.customer_features`, `customers.csv`)

When Spark's JDBC write outputs `(postgres://postgres:5432, warehouse.customer_features)` and MLflow's training step inputs the same tuple, Marquez draws an edge between them. The tools do not need to know about each other.

### Where identity mismatches break lineage

Every identity mismatch we encountered was a naming convention disagreement:

| Mismatch | Cause | Fix |
|---|---|---|
| `postgresql://` vs `postgres://` | Spark normalises the scheme; MLflow adapter did not | Scheme alias table in `parse_ol_identity()` |
| `warehouse.public.customer_features` vs `warehouse.customer_features` | MLflow URI included default schema; Spark omits it | Removed `.public.` from source URI |
| `s3a://` vs `s3://` | Spark uses Hadoop scheme; other tools use canonical S3 | `replaceDatasetNamespacePattern` in Spark config |

### Recommendation to OpenShift AI team

The single most impactful thing the platform can do for lineage quality is **standardise dataset naming conventions** across managed tool integrations. Publish a convention document that specifies:

1. PostgreSQL datasets use `postgres://host:port` namespace (matching the OL Spark convention)
2. S3-compatible datasets use `s3://bucket` namespace (not `s3a://`)
3. Dataset names follow `database.table` format (default schema omitted)

Any OpenLineage adapter shipped or recommended by the platform should enforce these conventions through normalisation, as our MLflow adapter now does.

---

## Summary of Changes Made

| Component | Change | Why |
|---|---|---|
| Argo Workflow Controller ConfigMap | Inject `OPENLINEAGE_NAMESPACE` via `fieldRef: metadata.namespace` | Zero-config namespace for all pipeline pods |
| `spark_etl.py` | Removed manual bridge event; added native OL listener config | Use Spark's built-in OpenLineage integration |
| `Dockerfile.spark` | Bumped OL Spark JAR to 1.45.0 | Includes `PostgresJdbcExtractor` and latest fixes |
| `parse_ol_identity()` in MLflow adapter | Added `_SCHEME_ALIASES` to normalise `postgresql://` -> `postgres://` | Match Spark's canonical namespace convention |
| `kfp_pipeline.py` | Removed `.public.` from MLflow dataset source URI | Match Spark's `database.table` naming (no default schema) |
| Spark session config | Added `s3a://->s3://` namespace pattern replacement | Align S3 dataset namespaces with non-Spark tools |
