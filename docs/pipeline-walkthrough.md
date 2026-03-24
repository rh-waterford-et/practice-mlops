# Customer Churn Prediction - Use Case, Pipeline, and Lineage

## 1. The Business Problem

A telecom company has 5,000 customers. Some will leave (churn), some will stay. Every customer lost costs the company roughly 5-10x more to replace than to retain. The business wants to know, before a customer leaves, that they are likely to leave - so the retention team can act.

The data science team is asked to build a churn prediction model. They have a CSV export of historical customer data containing:

| Field | What it tells us |
|---|---|
| `entity_id` | Which customer |
| `tenure_months` | How long they have been with us |
| `monthly_charges` | What they pay each month |
| `total_charges` | What they have paid in total |
| `num_support_tickets` | How often they contacted support |
| `contract_type` | Month-to-month, one-year, or two-year |
| `internet_service` | DSL, fibre optic, or none |
| `payment_method` | Credit card, bank transfer, etc. |
| `churn` | Did they leave? (1 = yes, 0 = no) |

The goal: build a model that takes these features for a current customer and predicts whether they will churn.

## 2. Why Lineage Matters Here

In a regulated or auditable environment, it is not enough to have a model that works. You need to answer questions like:

- What data was this model trained on?
- Was that data validated before training?
- Where did the raw data come from, and what transformations were applied?
- If a customer disputes a decision made by this model, can we trace the exact data path?

OpenLineage provides this by recording every dataset transformation as a job with explicit inputs and outputs. Marquez stores and visualises these records as a directed acyclic graph (DAG) - a lineage graph.

## 3. The Pipeline

The pipeline runs on OpenShift AI (Kubeflow Pipelines) and has nine steps split into two categories:

- **Platform steps** (1-3): Managed by the infrastructure team. These are the same regardless of what model is being built. A data scientist does not need to write or modify these.
- **Data scientist steps** (4-9): Owned by the ML practitioner. These define what features to use, how to validate data, which algorithm to train, and what quality threshold gates deployment.

### 3.1 Platform Steps

#### Step 1: Spark ETL

**Tool:** Apache Spark (PySpark)

Raw customer data arrives as a CSV file (`customers.csv`) in MinIO object storage. Before anyone can use it, it needs cleaning. Spark reads the CSV, deduplicates rows, casts columns to proper types, fills missing values with medians, and normalises numeric features to a 0-1 range. The cleaned data is written to a PostgreSQL table called `customer_features`.

**Lineage in Marquez:**

```
DATASET customers.csv --> JOB spark_etl --> DATASET customer_features_source
```

This is the entry point of the lineage graph. The raw CSV is the leftmost node. Everything downstream traces back to it.

#### Step 2: Feast Apply

**Tool:** Feast (feature store)

The cleaned PostgreSQL table now exists, but the ML platform does not yet know what it contains. `feast apply` registers the table as a managed data source (`customer_features_source`), defines an entity (`customer`, identified by `entity_id`), and creates a feature view (`customer_features_view`) that declares which columns are features.

This is a metadata operation. No data moves. But after this step, any data scientist on the team can retrieve these features by name rather than writing SQL.

**Lineage in Marquez:**

```
DATASET customer_features_source + ENTITY customer --> JOB feast_feature_views --> DATASET customer_features_view
```

The feature view node now appears in the graph with all its fields listed: tenure_months, monthly_charges, total_charges, num_support_tickets, contract_type, internet_service, payment_method, churn, charges_per_month, ticket_rate.

#### Step 3: Feast Materialize

**Tool:** Feast (feature store)

For real-time inference (e.g. a serving endpoint checking whether a customer is about to churn right now), features need to be in a low-latency store. Materialize copies the feature values from PostgreSQL (the offline store) to Redis (the online store).

**Lineage in Marquez:**

```
DATASET customer_features_source --> JOB materialize_customer_churn --> DATASET online_store_customer_features_view
```

This branch in the lineage graph shows that the same source data feeds both the training path (via the feature view) and the serving path (via the online store).

### 3.2 Data Scientist Steps

#### Step 4: Data Extraction

**Tool:** Feast + SQLAlchemy

The data scientist calls `get_historical_features()` to retrieve feature values for each customer at the correct point in time. This is a point-in-time join - Feast ensures that feature values match the `event_timestamp` of each record, preventing future data from leaking into training.

The result is a single Parquet file with all features and the `churn` label, ready for validation.

This step does not emit lineage. It consumes the Feast feature view but passes data forward via a KFP artifact (a Parquet file in MinIO).

#### Step 5: Data Validation

**Tool:** Great Expectations

Before training, the data must be checked. Great Expectations runs 19 expectations against the extracted dataset:

- Every critical column exists
- Each column is at least 95% non-null
- Numeric columns are non-negative
- The `churn` column only contains 0 or 1

If any nulls are found, they are filled with 0 as remediation. The validated data is passed forward.

**Lineage in Marquez:**

```
DATASET customer_features_view --> JOB validate_customer_data --> DATASET customer_features_validated
```

The validation job includes data quality assertion facets recording exactly what was checked and whether it passed. This means anyone reviewing the lineage can see not just that validation happened, but what the rules were and whether the data met them.

#### Step 6: Feature Engineering

**Tool:** pandas / NumPy

Two derived features are created that do not exist in the raw data:

- **charges_per_month** = total_charges / tenure_months - Are they paying a lot relative to how long they have been here?
- **ticket_rate** = num_support_tickets / tenure_months - Are they contacting support more than expected?

These are strong churn signals. A customer on a month-to-month contract with high charges and a high ticket rate is much more likely to leave than a customer on a two-year contract with low charges and no support history.

This step does not emit lineage.

#### Step 7: Model Training

**Tool:** XGBoost + MLflow

This is the core ML step. An XGBoost gradient-boosted classifier is trained on the enriched feature set:

1. Categorical features are encoded to integers
2. Data is split 80/20 for training and testing (stratified to preserve the churn ratio)
3. The model is trained with 200 boosting rounds
4. It is evaluated on the held-out test set: ROC-AUC, F1, precision, recall
5. Everything is logged to MLflow: parameters, metrics, the input dataset, and the serialised model

The MLflow tracking URI uses the `openlineage+http://` prefix, which activates the `openlineage-oai` wrapper. Every MLflow operation automatically emits lineage events.

**Lineage in Marquez:**

```
DATASET customer_features_view --> JOB mlflow/experiment-{id}/{run_id} --> DATASET model/model
```

This is the rightmost branch of the lineage graph. The trained model is the terminal dataset. Anyone reviewing the lineage can trace this model back through the feature view, through Feast, through the Spark ETL, all the way to the original CSV.

#### Step 8: Evaluation

**Tool:** Python

The evaluation metrics (ROC-AUC, F1, precision, recall) are extracted and printed to the pipeline logs. In production, this step might compare against a baseline model or check for demographic bias. For this pipeline, it serves as a reporting gate.

This step does not emit lineage.

#### Step 9: Model Registration

**Tool:** MLflow Model Registry

If the model's ROC-AUC exceeds 0.70, it is registered in the MLflow Model Registry as `customer_churn_model` and given the `champion` alias. Downstream serving systems read from this alias to serve predictions.

If the model does not meet the threshold, it is not registered and the reason is logged.

This step does not emit lineage.

## 4. The Lineage Graph

After a pipeline run, Marquez shows the following lineage:

```
                                                              +-- JOB materialize --> DATASET online_store
                                                              |
DATASET customers.csv --> JOB spark_etl --> DATASET customer_features_source --> JOB feast_apply --> DATASET customer_features_view --+-- JOB mlflow/experiment --> DATASET model/model
                                                                                                                                     |
                                                                                                                                     +-- JOB validate_customer_data --> DATASET customer_features_validated
```

Reading this left to right:

1. **customers.csv** is the raw data origin
2. **spark_etl** cleans and loads it into PostgreSQL
3. **customer_features_source** is the cleaned table
4. **feast_apply** registers it as a managed feature set, producing **customer_features_view**
5. **materialize** copies features to the online store for serving
6. **mlflow/experiment** trains a model on the feature view, producing **model/model**
7. **validate_customer_data** validates the feature view, producing **customer_features_validated** with data quality facets

Four tools emit lineage events:

| Tool | How lineage is emitted | What it records |
|---|---|---|
| Apache Spark | Manual OpenLineage event (bridge) | CSV source, PostgreSQL destination |
| Feast | Native OpenLineage integration (`emit_on_apply`, `emit_on_materialize`) | Feature sources, views, entities, online store |
| Great Expectations | Manual OpenLineage event with DataQuality facets | Validation assertions, pass/fail per expectation |
| MLflow | `openlineage-oai` tracking store wrapper | Training dataset input, model artifact output |

### What the lineage proves

For any given model version, the lineage graph answers:

- **Where did the training data come from?** Trace left from `model/model` to `customers.csv`.
- **Was the data validated?** The `validate_customer_data` job shows what checks ran and whether they passed.
- **What transformations were applied?** Spark ETL (dedup, normalise, median imputation) and Feast (point-in-time feature retrieval).
- **Is the serving data consistent with the training data?** The materialize branch shows the same `customer_features_source` feeds both the training path and the online store.
- **When did this happen?** Every job in Marquez records start and completion timestamps.

### What is not covered by lineage

Steps 4 (data extraction), 6 (feature engineering), 8 (evaluation), and 9 (model registration) pass data via KFP artifacts (Parquet files in MinIO) and do not emit OpenLineage events. These are in-pipeline transformations that do not cross system boundaries. The lineage graph tracks the semantically significant transitions: raw data entering the platform, features being registered, data being validated, and a model being produced.

## 5. Integration Status

| Tool | OpenLineage method | Status |
|---|---|---|
| **Spark** | Native `OpenLineageSparkListener` exists but produces verbose sub-job lineage and uses JDBC-derived dataset names that do not align with Feast's namespace. A manual bridge event is used instead for a clean, single-node representation. | Workaround - native listener available but impractical for demo |
| **Feast** | Native support via `openlineage` config block in `feature_store.yaml`. Works out of the box with `emit_on_apply: true` and `emit_on_materialize: true`. | Production-ready |
| **Great Expectations** | `OpenLineageValidationAction` exists in `openlineage-integration-common` but has a compatibility bug with GE 1.x (the `data_context` parameter is rejected by the new action base class). A manual event is used instead, including DataQuality assertion facets. | Workaround - native action broken on GE 1.x |
| **MLflow** | `openlineage-oai` client-side tracking store wrapper, activated by the `openlineage+http://` URI prefix. Emits events on `start_run`, `log_input`, `log_model`, and `end_run`. | Functional (temporary solution pending MLflow upstream contribution) |
| **KFP (orchestrator)** | `openlineage-oai` KFP adapter exists but background tracker thread terminates before emitting completion events, leaving jobs as RUNNING in Marquez. Removed from the pipeline. KFP's own roadmap includes native lineage via MLMD/MLflow. | Not used - awaiting upstream support |
