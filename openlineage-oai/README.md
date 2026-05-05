# openlineage-oai (trimmed)

Runtime pieces used by this repo’s **Kubeflow churn pipeline** and **MLflow** (`openlineage+…` tracking URI):

- `openlineage_oai.adapters.kfp` — `kfp_lineage`, `kfp_output_with_schema`
- `openlineage_oai.adapters.mlflow` — `OpenLineageTrackingStore`, `URIDatasetSource`

Tests, examples, and docs were removed from this tree; install from the project root with `pip install ./openlineage-oai[mlflow]`.
