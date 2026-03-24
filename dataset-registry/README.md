# Dataset Registry

A single source of truth for canonical dataset identities, built to test and demonstrate how OpenLineage dataset lineage can be correlated across multiple tools (Spark, Feast, MLflow, Great Expectations) in an OpenShift AI pipeline.

The registry stores datasets by their physical source URI (e.g. `s3://raw-data/telco-subscribers.csv`, `postgres://host:5432/db.schema.table`) and automatically derives the OpenLineage-compliant `(namespace, name)` pair. This canonical identity is then used to query Marquez for lineage and discover which pipelines interact with each dataset.


## Architecture

```
                      +-------------------+
                      |  PatternFly React  |
                      |    Frontend (UI)   |
                      +--------+----------+
                               |
                        nginx reverse proxy
                               |
                      +--------v----------+
                      |   FastAPI Backend  |
                      |    (Python 3.11)   |
                      +--+-----+------+---+
                         |     |      |
              +----------+  +--+--+  ++-----------+
              |             |     |               |
         PostgreSQL     Marquez  MinIO        Introspect
        (registry DB)    (OL)   (S3)       (auto-schema)
```

**Backend** -- FastAPI application that handles CRUD operations for datasets, emits OpenLineage `DatasetEvent`s to Marquez on creation, auto-detects schema from S3 CSV files and PostgreSQL tables, and queries Marquez lineage to discover associated pipelines.

**Frontend** -- PatternFly 6 React application with three pages: a dataset list with create/delete, a dataset detail view with inline editing and pipeline discovery, and a lineage page embedding the Marquez UI with deep-linking support.

**SDK** -- Lightweight Python client (`dataset_registry.RegistryClient`) for programmatic CRUD from pipeline steps or notebooks.

**Database** -- PostgreSQL with a single `datasets` table. Each row stores the human-readable name, physical source URI, derived OL namespace/name, optional schema fields (JSONB), and tags.


## Project Structure

```
dataset-registry/
  backend/
    app.py            # FastAPI endpoints
    models.py         # Pydantic models + parse_ol_identity()
    db.py             # PostgreSQL CRUD + schema init
    lineage.py        # OpenLineage DatasetEvent emission
    introspect.py     # Auto-schema detection (S3 CSV, PostgreSQL)
    requirements.txt
    Dockerfile
  frontend/
    src/
      App.tsx         # Layout, routing, PatternFly shell
      api.ts          # TypeScript API client
      pages/
        Datasets.tsx      # List + create form
        DatasetDetail.tsx # Detail view, editing, pipeline discovery
        Lineage.tsx       # Marquez iframe with deep-linking
    nginx.conf
    Dockerfile
  sdk/
    dataset_registry/
      client.py       # RegistryClient (requests-based)
      models.py       # Dataclasses for Dataset, SchemaField
  openshift/
    base/
      db-deployment.yaml
      api-deployment.yaml
      ui-deployment.yaml
      route.yaml
    deploy.sh         # OpenShift build + deploy script
```


## Dataset Identity Model

When a dataset is registered, the backend derives its OpenLineage identity from the source URI using the `parse_ol_identity` function:

| Source URI | OL Namespace | OL Name |
|---|---|---|
| `s3://raw-data/telco-subscribers.csv` | `s3://raw-data` | `telco-subscribers.csv` |
| `postgres://host:5432/warehouse.public.features` | `postgres://host:5432` | `warehouse.public.features` |
| `jdbc:postgresql://host:5432/db.schema.table` | `postgres://host:5432` | `db.schema.table` |

This follows the OpenLineage naming specification, where dataset namespaces are derived from the physical storage location. Tools that comply with the spec (Spark with prefix transformations, Feast, etc.) will emit matching identities, enabling automatic cross-tool lineage correlation in Marquez.


## API Endpoints

All endpoints are under `/api/v1`.

| Method | Path | Description |
|---|---|---|
| `POST` | `/datasets` | Register a new dataset (auto-detects schema if not provided) |
| `GET` | `/datasets` | List all datasets (optional `?tag=` filter) |
| `GET` | `/datasets/lookup?source=` | Look up a dataset by its source URI |
| `GET` | `/datasets/{id}` | Get a dataset by ID |
| `PUT` | `/datasets/{id}` | Update name, description, schema, or tags |
| `DELETE` | `/datasets/{id}` | Delete a dataset |
| `GET` | `/datasets/{id}/pipelines` | Discover pipelines (Marquez job namespaces) that reference this dataset |

Interactive API docs are available at `/docs` (Swagger UI).


## Schema Introspection

When a dataset is created without explicit `schema_fields`, the backend attempts to auto-detect the schema:

- **S3 CSV files** -- Downloads the file from MinIO, reads the CSV header, and infers column types (INTEGER, FLOAT, TIMESTAMP, STRING) from a sample of 100 rows.
- **PostgreSQL tables** -- Queries `information_schema.columns` to retrieve column names and data types.


## Pipeline Discovery

The `GET /datasets/{id}/pipelines` endpoint queries Marquez's lineage API for the dataset's OpenLineage identity and extracts all unique job namespaces from the lineage graph. Each namespace represents a pipeline (following the convention that `pipeline = namespace`). The frontend displays these on the dataset detail page with links to view the dataset's lineage filtered by pipeline context.


## SDK Usage

```python
from dataset_registry import RegistryClient

client = RegistryClient(url="http://dataset-registry-api:8080")

ds = client.create_dataset(
    name="Telco Subscribers",
    source="s3://raw-data/telco-subscribers.csv",
    description="Raw subscriber data for churn prediction",
    tags=["raw", "telco"],
)

print(ds.ol_namespace)       # s3://raw-data
print(ds.ol_name)            # telco-subscribers.csv
print(ds.openlineage_node_id)  # dataset:s3://raw-data:telco-subscribers.csv

all_datasets = client.list_datasets()
client.delete_dataset(source="s3://raw-data/telco-subscribers.csv")
```


## Deployment

### OpenShift (production)

```bash
cd openshift
./deploy.sh
```

This creates BuildConfigs, ImageStreams, Deployments, Services, and Routes in the `lineage` namespace. The backend is built from `backend/` and the frontend from `frontend/` using binary builds.

### Local (Kind cluster)

```bash
cd ../local-kind
./setup.sh
```

Sets up a Kind cluster with Marquez, MLflow, MinIO, and the dataset registry. Uses Podman by default, falls back to Docker. See `local-kind/README.md` for details.


## Environment Variables

### Backend

| Variable | Default | Description |
|---|---|---|
| `DB_HOST` | `registry-db` | PostgreSQL host |
| `DB_PORT` | `5432` | PostgreSQL port |
| `DB_USER` | `registry` | PostgreSQL user |
| `DB_PASSWORD` | `registry` | PostgreSQL password |
| `DB_NAME` | `registry` | PostgreSQL database |
| `MARQUEZ_URL` | `http://marquez:80` | Marquez API base URL |
| `S3_ENDPOINT` | `http://mlflow-minio:9000` | MinIO/S3 endpoint for schema introspection |
| `S3_ACCESS_KEY` | `minioadmin` | S3 access key |
| `S3_SECRET_KEY` | `minioadmin123` | S3 secret key |
