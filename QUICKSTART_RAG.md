# RAG Pipeline Quick Start Guide

Get your RAG ingestion pipeline running in OpenShift AI in under 10 minutes.

## What You'll Build

A complete RAG (Retrieval-Augmented Generation) document ingestion pipeline that:
- ✅ Loads documents from object storage (MinIO)
- ✅ Chunks text intelligently with overlap
- ✅ Generates semantic embeddings (sentence-transformers)
- ✅ Stores vectors in PostgreSQL with pgvector
- ✅ Tracks full data lineage automatically via OpenLineage

Lineage tracking is simple and direct:
- KFP artifacts track data flow between steps
- OpenLineage event emitted directly from Step 4 (vector DB storage)
- Argo workflow controller injects namespace automatically

## Prerequisites

- OpenShift cluster with OpenShift AI installed
- `oc` CLI configured and logged in
- Project/namespace: `lineage`

## Step 1: Setup PostgreSQL with pgvector

The pipeline requires PostgreSQL with pgvector extension for vector similarity search.

**Option A: Use pgvector-enabled image (Recommended)**

Update `openshift/base/postgres.yaml`:
```yaml
image: ankane/pgvector:latest
```

**Option B: Keep existing image and install extension**

The current deployment uses `quay.io/sclorg/postgresql-15-c9s`. You'll need to build a custom image with pgvector or switch to the standard PostgreSQL image.

For quick testing with standard PostgreSQL:
```bash
# Replace image in postgres deployment
oc set image deployment/postgres postgres=postgres:16 -n lineage
oc rollout restart deployment/postgres -n lineage

# Install pgvector extension
oc apply -f openshift/jobs/00-setup-pgvector.yaml
```

## Step 2: Upload Sample Documents

Upload the sample ML/MLOps documentation to MinIO:

```bash
oc apply -f openshift/jobs/07-rag-seed-docs.yaml
oc logs -f job/rag-seed-docs -n lineage
```

This uploads:
- `ml_basics.md` - Machine learning fundamentals
- `mlops_guide.md` - MLOps best practices
- `feast_features.md` - Feast feature store guide
- `openlineage_basics.txt` - OpenLineage concepts

## Step 3: Update Docker Image

Add RAG dependencies to your application image:

```bash
# Already added to requirements.txt:
# - sentence-transformers
# - pgvector
# - torch

# Rebuild image (for OpenShift)
oc start-build fkm-app -n lineage --follow
```

## Step 4: Compile the RAG Pipeline

Compile the pipeline to YAML:

```bash
python -m src.rag.rag_pipeline
```

This creates `rag_ingestion_pipeline.yaml` in your project root.

## Step 5: Upload to OpenShift AI

**Via Web UI:**
1. Open OpenShift AI console
2. Navigate to **Data Science Pipelines**
3. Click **Import Pipeline**
4. Upload `rag_ingestion_pipeline.yaml`
5. Name it: "RAG Document Ingestion"

**Via API (if DSP endpoint is exposed):**
```bash
export DSP_ENDPOINT="https://ds-pipeline-dspa-lineage.apps.your-cluster.com"
export DSP_TOKEN=$(oc whoami -t)
python -m src.rag.upload_rag_pipeline
```

## Step 6: Run the Pipeline

1. In OpenShift AI, go to **Pipelines** → **RAG Document Ingestion**
2. Click **Create Run**
3. Use these parameters (or keep defaults):
   ```yaml
   bucket_name: data
   document_prefix: sample_docs/
   chunk_size: 1000
   chunk_overlap: 200
   embedding_model: all-MiniLM-L6-v2
   collection_name: ml_docs
   ```
4. Click **Start**

The pipeline will execute these steps:
1. **Load Documents** (~5s) - Fetch from MinIO
2. **Chunk Documents** (~2s) - Split into chunks
3. **Generate Embeddings** (~30s) - Create vector embeddings
4. **Store in Vector DB + Emit Lineage** (~12s) - Save to PostgreSQL and emit OpenLineage event

Total runtime: ~49 seconds

## Step 7: View Data Lineage

### In Marquez (OpenLineage UI)

```bash
# Port-forward to Marquez
oc port-forward -n lineage svc/marquez-web 3000:3000

# Open in browser
open http://localhost:3000
```

Navigate to namespace `lineage` to see:
- 📊 Complete lineage graph
- 📁 Dataset dependencies (MinIO → PostgreSQL)
- 🔄 Job runs and status
- 📈 Metrics and statistics

### In MLflow

```bash
# Port-forward to MLflow
oc port-forward -n lineage svc/mlflow-server 5000:5000

# Open in browser
open http://localhost:5000
```

You can also use MLflow for general experiment tracking if needed (though the pipeline now emits OpenLineage events directly)

## Step 8: Query the Vector Database

Test semantic search over your documents:

```bash
# Port-forward PostgreSQL
oc port-forward -n lineage svc/postgres 5432:5432

# Run a semantic search
python -m src.rag.query "What is MLOps?" \
  --collection ml_docs \
  --top-k 5 \
  --pg-host localhost
```

Example queries:
```bash
# Feast concepts
python -m src.rag.query "How does Feast ensure point-in-time correctness?"

# ML fundamentals
python -m src.rag.query "Explain the difference between precision and recall"

# OpenLineage
python -m src.rag.query "What are facets in OpenLineage?"
```

## What Gets Tracked

### 📊 Datasets (OpenLineage Event)
- **Inputs**:
  - `s3://mlflow-minio:9000/data/sample_docs/ml_basics.md`
  - `s3://mlflow-minio:9000/data/sample_docs/mlops_guide.md`
  - `s3://mlflow-minio:9000/data/sample_docs/feast_features.md`
  - `s3://mlflow-minio:9000/data/sample_docs/openlineage_basics.txt`
- **Output**: `postgresql://postgres:5432/warehouse.rag_ml_docs` (pgvector table with full schema)

### 🔄 Jobs (OpenLineage Event)
- **Job Name**: `rag_store_vectordb`
- **Namespace**: `lineage` (auto-injected by Argo controller)
- **Run ID**: UUID (e.g., `550e8400-e29b-41d4-a716-446655440000`)

### 📋 Metadata (OpenLineage Facets)
- **Schema Facet**: Complete table schema (9 columns with types)
- **Statistics Facet**: Row count (300-400 chunks), total text size
- **SQL Facet**: Table creation DDL
- Embedding model: all-MiniLM-L6-v2
- Embedding dimension: 384

### 🔗 Lineage Flow
```
MinIO Bucket
  ↓ (KFP artifact: raw_documents)
Document Chunks
  ↓ (KFP artifact: document_chunks)
Embeddings
  ↓ (KFP artifact: embeddings)
PostgreSQL Table
  ↓ (OpenLineage Event: COMPLETE)
Marquez
```

## Customization

### Use Different Embedding Model

```python
# Faster, smaller model (default)
embedding_model: "all-MiniLM-L6-v2"  # 384 dims, 80MB

# Better quality, larger model
embedding_model: "all-mpnet-base-v2"  # 768 dims, 420MB

# Domain-specific (scientific)
embedding_model: "allenai/scibert_scivocab_uncased"  # 768 dims
```

### Adjust Chunking Strategy

```python
# Smaller chunks (better for Q&A)
chunk_size: 500
chunk_overlap: 100

# Larger chunks (better for summarization)
chunk_size: 2000
chunk_overlap: 400
```

### Add Your Own Documents

```bash
# Upload to MinIO
oc port-forward -n lineage svc/mlflow-minio 9000:9000

# Use MinIO client or SDK
mc alias set myminio http://localhost:9000 minioadmin minioadmin123
mc cp my-document.md myminio/data/sample_docs/

# Or use Python:
from minio import Minio
client = Minio("localhost:9000", "minioadmin", "minioadmin123", secure=False)
client.fput_object("data", "sample_docs/my-doc.md", "/path/to/my-doc.md")
```

Then re-run the pipeline to ingest new documents.

## Troubleshooting

### pgvector extension not found
```bash
# Check PostgreSQL image supports pgvector
oc describe deployment/postgres -n lineage | grep Image

# If not, switch to pgvector-enabled image
oc set image deployment/postgres postgres=ankane/pgvector:latest -n lineage
oc rollout restart deployment/postgres -n lineage

# Run setup job
oc apply -f openshift/jobs/00-setup-pgvector.yaml
```

### Pipeline step fails
```bash
# Check pod logs
oc get pods -n lineage | grep rag-doc-ingestion
oc logs -n lineage <pod-name>

# Check pipeline run details in OpenShift AI UI
# Or use kubectl/oc:
oc describe pod <pod-name> -n lineage
```

### No documents in MinIO
```bash
# Verify documents were uploaded
oc logs job/rag-seed-docs -n lineage

# Check MinIO bucket
oc port-forward -n lineage svc/mlflow-minio 9001:9001
open http://localhost:9001  # Login: minioadmin / minioadmin123
```

### Embedding generation too slow
```bash
# Use smaller model
embedding_model: "all-MiniLM-L6-v2"  # Fast

# Or increase resources in pipeline step
# Edit generate_embeddings component resources
```

## Next Steps

1. **Add More Documents**: Upload PDFs, Word docs, code files
2. **Improve Chunking**: Implement smarter boundary detection
3. **Add Reranking**: Use cross-encoder for better results
4. **Build RAG Chain**: Connect to LLM for question answering
5. **Monitor Performance**: Track query latency and relevance
6. **Scale Up**: Increase pgvector index size, use dedicated vector DB

## Learn More

- Full documentation: `src/rag/README.md`
- OpenLineage spec: https://openlineage.io
- Sentence Transformers: https://www.sbert.net
- pgvector: https://github.com/pgvector/pgvector
- OpenShift AI: https://docs.redhat.com/en/documentation/red_hat_openshift_ai_self-managed

---

**Questions?** Check the main README or open an issue.

**Happy RAG building! 🚀**
