# RAG Inference Service Example

A real RAG (Retrieval-Augmented Generation) inference service that demonstrates:
- Querying Milvus vector database for document retrieval
- Fetching user features from Feast online store
- Configuration needed for production ML services

## Configuration Requirements

This service shows the **environment variables** needed to connect to external systems:

### Milvus (Vector Database)
```bash
MILVUS_HOST=milvus              # Hostname or IP
MILVUS_PORT=19530               # Default Milvus port
MILVUS_COLLECTION=ml_docs       # Collection name
```

### Feast (Feature Store)
```bash
FEAST_REPO_PATH=/tmp/feast_repo                              # Local repo path
FEAST_REGISTRY=postgresql://feast:feast@postgres:5432/warehouse  # SQL registry
FEAST_ONLINE_STORE=redis:6379                                # Redis online store
FEAST_PROJECT=customer_churn                                 # Feast project name
```

### Model & Inference
```bash
MODEL_NAME=llama3-70b-rag-v1    # Model identifier
TOP_K_DOCUMENTS=3               # Number of docs to retrieve
```

## Build & Deploy

### Step 1: Create Feast ConfigMap

```bash
cd openshift/webhook/examples

# Create ConfigMap with feature_store.yaml
oc apply -f feast-config-configmap.yaml
```

### Step 2: Build the image

```bash
cd rag-service

# Create BuildConfig
oc apply -f ../rag-service-buildconfig.yaml

# Build the image
oc start-build rag-inference-app -n lineage --from-dir=. --follow
```

### Step 3: Deploy the service

```bash
cd ..

# Deploy (lineage webhook will inject initContainer automatically)
oc apply -f rag-inference-service.yaml
```

### Step 3: Test the service

```bash
# Wait for pods to be ready
oc wait --for=condition=ready pod -l app=rag-inference -n lineage --timeout=120s

# Get the route
ROUTE=$(oc get route rag-inference -n lineage -o jsonpath='{.spec.host}')

# Test health endpoint
curl https://$ROUTE/health

# Test config endpoint (shows all configuration)
curl https://$ROUTE/config

# Test RAG query
curl -X POST https://$ROUTE/query \
  -H "Content-Type: application/json" \
  -d '{
    "query": "What is machine learning?",
    "user_id": "customer_123"
  }'
```

## API Endpoints

### `GET /health`
Health check showing service status

**Response:**
```json
{
  "status": "healthy",
  "milvus": true,
  "feast": true,
  "model": "llama3-70b-rag-v1"
}
```

### `GET /config`
Shows current configuration (useful for debugging)

**Response:**
```json
{
  "milvus": {
    "host": "milvus",
    "port": 19530,
    "collection": "ml_docs",
    "connected": true,
    "documents": 42
  },
  "feast": {
    "repo_path": "/tmp/feast_repo",
    "registry": "postgresql://feast:feast@postgres:5432/warehouse",
    "online_store": "redis:6379",
    "project": "customer_churn",
    "initialized": true
  },
  "model": {
    "name": "llama3-70b-rag-v1",
    "top_k_documents": 3
  }
}
```

### `POST /query`
Execute RAG query

**Request:**
```json
{
  "query": "What is machine learning?",
  "user_id": "customer_123"
}
```

**Response:**
```json
{
  "query": "What is machine learning?",
  "user_id": "customer_123",
  "user_features": {
    "tenure_months": 24,
    "monthly_charges": 79.99,
    "contract_type": "Month-to-month"
  },
  "retrieved_documents": [
    {
      "text": "Machine learning is a subset of AI...",
      "filename": "ml_intro.txt",
      "source": "s3://data/sample_docs/ml_intro.txt",
      "score": 0.89
    }
  ],
  "model": "llama3-70b-rag-v1",
  "timestamp": "2026-03-27T10:30:00.123456",
  "note": "This is a demo - in production, retrieved docs would be sent to LLM"
}
```

## How It Works

1. **User Query** → Service receives query text and user ID

2. **Feature Retrieval** → Fetches user context from Feast online store (Redis)
   - User demographics, preferences, history
   - Real-time features for personalization

3. **Document Retrieval** → Searches Milvus for relevant documents
   - Embeds query text using same model as ingestion
   - Finds top-K most similar documents using cosine similarity

4. **Response** → Returns user features + retrieved documents
   - In production, this would be sent to an LLM for generation
   - Demonstrates the complete RAG pipeline

## Lineage Tracking

The service has lineage automatically tracked via webhook:

**Inputs:**
- `milvus://milvus:19530/ml_docs` - Vector database
- `redis://redis:6379/customer_churn.customer_features_view` - Feast online store
- `s3://mlflow-minio:9000/models/llama3-70b-rag-v1` - Model artifacts

**Outputs:**
- `redis://redis:6379/rag_inference_cache` - Cached inference results

View in Marquez UI:
```bash
oc get route marquez-web -n lineage -o jsonpath='{.spec.host}'
# Search for: rag-inference-service
```

## Key Learnings

This example shows:

1. **Configuration Management**: All external service connections via env vars
2. **Service Dependencies**: Clear declaration of Milvus, Feast, Redis, PostgreSQL
3. **Health Checks**: Proper liveness/readiness probes
4. **API Design**: Simple REST endpoints for inference
5. **Lineage Tracking**: Automatic via webhook (no SDK in application code!)

## Extending This Example

For production use, you would add:
- **Authentication**: API keys, OAuth tokens
- **Rate Limiting**: Prevent abuse
- **Caching**: Redis cache for repeated queries
- **Monitoring**: Prometheus metrics, OpenTelemetry traces
- **LLM Integration**: Actually call the language model
- **Response Caching**: Store generated responses
- **Error Handling**: Retry logic, fallbacks
- **Model Loading**: Load model from MLflow/S3 on startup
