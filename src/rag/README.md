# RAG Ingestion Pipeline

A complete RAG (Retrieval-Augmented Generation) document ingestion pipeline for OpenShift AI with automatic OpenLineage tracking.

## Overview

This pipeline ingests documents, chunks them, generates embeddings, and stores them in a PostgreSQL vector database (pgvector). All data lineage is tracked automatically through:

- **KFP Artifacts**: Track data flow between pipeline steps
- **MLflow**: Experiment tracking with automatic OpenLineage integration
- **Argo Workflow Controller**: Injects `OPENLINEAGE_NAMESPACE` automatically

**No manual OpenLineage event emission required!**

## Architecture

```
┌─────────────────┐
│  Sample Docs    │ (MinIO: s3://data/sample_docs/)
│  - ml_basics.md │
│  - mlops_guide  │
│  - feast_guide  │
└────────┬────────┘
         │
         ▼
┌─────────────────────────────────────────────────────────┐
│              RAG Ingestion Pipeline (KFP)               │
│                                                          │
│  ┌──────────┐   ┌──────────┐   ┌──────────┐            │
│  │  Load    │ → │  Chunk   │ → │  Embed   │            │
│  │  Docs    │   │  Text    │   │ (S-BERT) │            │
│  └──────────┘   └──────────┘   └──────────┘            │
│                                      │                  │
│                                      ▼                  │
│  ┌──────────────────┐   ┌────────────────────┐         │
│  │ Store in pgvector│ → │ Register in MLflow │         │
│  │  (PostgreSQL)    │   │  (Auto OL tracking)│         │
│  └──────────────────┘   └────────────────────┘         │
└─────────────────────────────────────────────────────────┘
         │                            │
         ▼                            ▼
┌─────────────────┐          ┌────────────────┐
│   PostgreSQL    │          │     MLflow     │
│   + pgvector    │          │   + Marquez    │
│                 │          │  (OpenLineage) │
│ Table: rag_*    │          └────────────────┘
└─────────────────┘
```

## Pipeline Steps

### 1. Load Documents (`load_documents`)
- Reads documents from MinIO (S3-compatible storage)
- Supports `.txt`, `.md`, and `.pdf` files
- Outputs: JSON array of documents with metadata
- **Tracked**: Input bucket/prefix → Output dataset artifact

### 2. Chunk Documents (`chunk_documents`)
- Splits documents into overlapping chunks
- Smart boundary detection (sentences, paragraphs)
- Default: 1000 chars per chunk, 200 char overlap
- Outputs: JSON array of chunks
- **Tracked**: Input docs → Output chunks artifact

### 3. Generate Embeddings (`generate_embeddings`)
- Uses sentence-transformers (default: `all-MiniLM-L6-v2`)
- Generates dense vector embeddings for each chunk
- 384-dimensional vectors (for all-MiniLM-L6-v2)
- Outputs: Chunks with embeddings
- **Tracked**: Input chunks → Output embeddings artifact

### 4. Store in Vector DB + Emit Lineage (`store_in_vectordb`)
- Creates PostgreSQL table with pgvector extension
- Stores chunks + embeddings
- Creates IVFFlat index for fast similarity search
- **Emits OpenLineage event** directly to Marquez
  - Input datasets: Source documents from MinIO
  - Output dataset: PostgreSQL vector table with schema
  - Job metadata: SQL DDL, statistics
- Outputs: Table metadata + OpenLineage run ID
- **Tracked**: Full dataset lineage from source documents to vector DB

## Data Lineage Flow

```
MinIO Bucket (s3://data/sample_docs/)
  ↓
[KFP Artifact: raw_documents]
  ↓
[KFP Artifact: document_chunks]
  ↓
[KFP Artifact: embeddings]
  ↓
PostgreSQL Table (rag_ml_docs) + OpenLineage Event
  ↓
OpenLineage/Marquez
  → Input: s3://data/sample_docs/ml_basics.md
  → Input: s3://data/sample_docs/mlops_guide.md
  → Input: s3://data/sample_docs/feast_features.md
  → Input: s3://data/sample_docs/openlineage_basics.txt
  → Output: postgresql://warehouse.rag_ml_docs
  → Job: rag_store_vectordb
  → Run: <openlineage_run_id>
```

## Deployment

### Prerequisites

1. **pgvector Extension**: Install in PostgreSQL
   ```bash
   kubectl apply -f openshift/jobs/00-setup-pgvector.yaml
   ```

2. **Sample Documents**: Upload to MinIO
   ```bash
   kubectl apply -f openshift/jobs/07-rag-seed-docs.yaml
   ```

### Compile Pipeline

```bash
python -m src.rag.rag_pipeline
```

This generates `rag_ingestion_pipeline.yaml` in the project root.

### Upload to OpenShift AI

**Option 1: Via Script**
```bash
export DSP_ENDPOINT="https://ds-pipeline-dspa-lineage.apps.your-cluster.com"
export DSP_TOKEN="your-token-here"
python -m src.rag.upload_rag_pipeline
```

**Option 2: Via UI**
1. Navigate to Data Science Pipelines in OpenShift AI
2. Click "Import Pipeline"
3. Upload `rag_ingestion_pipeline.yaml`
4. Create a new run with default parameters

**Option 3: Compile Only**
```bash
kubectl apply -f openshift/jobs/08-rag-pipeline.yaml
```

### Run Pipeline

In the OpenShift AI UI:
1. Go to **Data Science Pipelines** → **Pipelines**
2. Select "RAG Document Ingestion Pipeline"
3. Click **Create Run**
4. Use default parameters or customize:
   - `bucket_name`: MinIO bucket (default: `data`)
   - `document_prefix`: Path prefix (default: `sample_docs/`)
   - `chunk_size`: Characters per chunk (default: `1000`)
   - `chunk_overlap`: Overlap between chunks (default: `200`)
   - `embedding_model`: Model name (default: `all-MiniLM-L6-v2`)
   - `collection_name`: Vector DB collection (default: `ml_docs`)

## Querying the Vector Database

### Semantic Search

```bash
python -m src.rag.query "What is MLOps?" --collection ml_docs --top-k 5
```

### Port-forward for Local Queries

```bash
kubectl port-forward -n lineage svc/postgres 5432:5432
python -m src.rag.query "Explain feature stores" --pg-host localhost
```

### Example Queries

```bash
# MLOps concepts
python -m src.rag.query "How to monitor ML models in production?"

# Feast features
python -m src.rag.query "What is point-in-time correctness in Feast?"

# Machine learning
python -m src.rag.query "Difference between supervised and unsupervised learning"
```

## Sample Documents

Three comprehensive documents are included:

1. **ml_basics.md** (~2.5KB)
   - Machine learning fundamentals
   - Types of ML (supervised, unsupervised, reinforcement)
   - ML workflow and metrics
   - Best practices

2. **mlops_guide.md** (~6KB)
   - MLOps overview and architecture
   - CI/CD for ML
   - Monitoring and observability
   - Data lineage and governance
   - Best practices and tooling

3. **feast_features.md** (~5KB)
   - Feast feature store guide
   - Core concepts (entities, feature views)
   - Workflow and integration
   - OpenLineage integration

## OpenLineage Tracking

The pipeline automatically tracks:

### Datasets (in OpenLineage)
- **Inputs**:
  - `s3://mlflow-minio:9000/data/sample_docs/ml_basics.md`
  - `s3://mlflow-minio:9000/data/sample_docs/mlops_guide.md`
  - `s3://mlflow-minio:9000/data/sample_docs/feast_features.md`
  - `s3://mlflow-minio:9000/data/sample_docs/openlineage_basics.txt`
- **Output**: `postgresql://postgres:5432/warehouse.rag_ml_docs` (pgvector table with schema)

### Jobs
- **Job Name**: `rag_store_vectordb`
- **Namespace**: Injected automatically via Argo controller (e.g., `lineage`)
- **Run ID**: UUID generated during execution

### Metadata Captured
- **Schema**: Full table schema with column names and types
- **Statistics**: Row count, total text size
- **SQL**: Table creation DDL
- **Embedding Info**: Model name, dimension

### Lineage Graph
View in Marquez UI:
```bash
kubectl port-forward -n lineage svc/marquez-web 3000:3000
open http://localhost:3000
```

Navigate to the `lineage` namespace to see:
- Job runs and status
- Dataset dependencies
- Complete data lineage graph

## Configuration

### Embedding Models

You can use any sentence-transformers model:

```python
# Fast, small (default)
embedding_model="all-MiniLM-L6-v2"  # 384 dims, 80MB

# Better quality, larger
embedding_model="all-mpnet-base-v2"  # 768 dims, 420MB

# Domain-specific (technical)
embedding_model="allenai/scibert_scivocab_uncased"  # 768 dims
```

### Chunking Strategy

Adjust for your use case:

```python
# Code documentation (smaller chunks)
chunk_size=500
chunk_overlap=100

# Long-form content (larger chunks)
chunk_size=2000
chunk_overlap=400

# No overlap (faster, less context)
chunk_size=1000
chunk_overlap=0
```

### Vector Search

The pipeline uses cosine similarity with IVFFlat indexing:

```sql
-- Similarity search query
SELECT text, 1 - (embedding <=> query_embedding) AS similarity
FROM rag_ml_docs
ORDER BY embedding <=> query_embedding
LIMIT 5;
```

## Extending the Pipeline

### Add PDF Support

Install `pypdf` in requirements.txt:
```python
from pypdf import PdfReader

def load_pdf(file_path):
    reader = PdfReader(file_path)
    text = ""
    for page in reader.pages:
        text += page.extract_text()
    return text
```

### Add Metadata Filtering

Extend the schema:
```python
metadata JSONB,
tags TEXT[]
```

Query with filters:
```sql
WHERE metadata->>'category' = 'mlops'
  AND 'production' = ANY(tags)
```

### Add Reranking

Use cross-encoder after retrieval:
```python
from sentence_transformers import CrossEncoder

reranker = CrossEncoder('cross-encoder/ms-marco-MiniLM-L-6-v2')
scores = reranker.predict([(query, doc) for doc in results])
```

## Troubleshooting

### pgvector not found
```bash
# Install extension
kubectl apply -f openshift/jobs/00-setup-pgvector.yaml

# Verify
kubectl exec -n lineage -it postgres-0 -- \
  psql -U feast -d warehouse -c "SELECT * FROM pg_extension WHERE extname='vector';"
```

### Out of memory during embedding
Reduce batch size or use smaller model:
```python
# In generate_embeddings component
embeddings = model.encode(texts, batch_size=32, show_progress_bar=True)
```

### Slow similarity search
Create indexes:
```sql
-- Increase lists for larger datasets
CREATE INDEX USING ivfflat (embedding vector_cosine_ops) WITH (lists = 1000);
```

## Performance

Typical pipeline execution times (4 documents, ~350 chunks):

| Step                              | Duration |
|-----------------------------------|----------|
| Load documents                    | ~5s      |
| Chunk documents                   | ~2s      |
| Generate embeddings               | ~30s     |
| Store in vectordb + emit lineage  | ~12s     |
| **Total**                         | **~49s** |

## License

Part of the feast-kfp-mlflow MLOps demo project.
