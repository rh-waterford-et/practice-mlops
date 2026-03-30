#!/usr/bin/env python3
"""
Simple RAG Inference Service

Demonstrates querying:
- Milvus vector database for document retrieval
- Feast online store for user features
- Configuration needed for real applications
"""

import json
import os
from datetime import datetime
from flask import Flask, request, jsonify
from pymilvus import connections, Collection
from feast import FeatureStore

app = Flask(__name__)

# Configuration from environment variables
MILVUS_HOST = os.getenv("MILVUS_HOST", "milvus")
MILVUS_PORT = int(os.getenv("MILVUS_PORT", "19530"))
MILVUS_COLLECTION = os.getenv("MILVUS_COLLECTION", "ml_docs")

# Feast repo path - should contain feature_store.yaml (mounted from ConfigMap)
FEAST_REPO_PATH = os.getenv("FEAST_REPO_PATH", "/app/feast_repo")

MODEL_NAME = os.getenv("MODEL_NAME", "llama3-70b-rag-v1")
TOP_K_DOCUMENTS = int(os.getenv("TOP_K_DOCUMENTS", "3"))


def init_feast():
    """Initialize Feast feature store from mounted configuration."""
    # FEAST_REPO_PATH should point to a directory containing feature_store.yaml
    # In Kubernetes, this will be mounted from a ConfigMap
    if not os.path.exists(f"{FEAST_REPO_PATH}/feature_store.yaml"):
        raise FileNotFoundError(
            f"feature_store.yaml not found at {FEAST_REPO_PATH}. "
            "Ensure ConfigMap is mounted correctly."
        )

    return FeatureStore(repo_path=FEAST_REPO_PATH)


def init_milvus():
    """Initialize Milvus connection and return collection."""
    connections.connect(
        alias="default",
        host=MILVUS_HOST,
        port=MILVUS_PORT,
    )
    return Collection(MILVUS_COLLECTION)


# Initialize on startup
print("Initializing RAG service...")
print(f"Milvus: {MILVUS_HOST}:{MILVUS_PORT}/{MILVUS_COLLECTION}")
print(f"Feast repo: {FEAST_REPO_PATH}")
print(f"Model: {MODEL_NAME}")

try:
    feast_store = init_feast()
    print("✓ Feast initialized")
except Exception as e:
    print(f"✗ Feast initialization failed: {e}")
    feast_store = None

try:
    milvus_collection = init_milvus()
    print(f"✓ Milvus connected ({milvus_collection.num_entities} documents)")
except Exception as e:
    print(f"✗ Milvus connection failed: {e}")
    milvus_collection = None


@app.route('/health', methods=['GET'])
def health():
    """Health check endpoint."""
    return jsonify({
        "status": "healthy",
        "milvus": milvus_collection is not None,
        "feast": feast_store is not None,
        "model": MODEL_NAME,
    }), 200


@app.route('/config', methods=['GET'])
def config():
    """Show current configuration."""
    import yaml

    # Read feast config from file
    feast_config = {}
    try:
        with open(f"{FEAST_REPO_PATH}/feature_store.yaml", "r") as f:
            feast_config = yaml.safe_load(f)
    except Exception as e:
        feast_config = {"error": str(e)}

    return jsonify({
        "milvus": {
            "host": MILVUS_HOST,
            "port": MILVUS_PORT,
            "collection": MILVUS_COLLECTION,
            "connected": milvus_collection is not None,
            "documents": milvus_collection.num_entities if milvus_collection else 0,
        },
        "feast": {
            "repo_path": FEAST_REPO_PATH,
            "config": feast_config,
            "initialized": feast_store is not None,
        },
        "model": {
            "name": MODEL_NAME,
            "top_k_documents": TOP_K_DOCUMENTS,
        },
    }), 200


@app.route('/query', methods=['POST'])
def query():
    """
    RAG query endpoint.

    Request body:
    {
        "query": "What is machine learning?",
        "user_id": "customer_123"
    }
    """
    try:
        data = request.get_json()
        query_text = data.get("query", "")
        user_id = data.get("user_id", "unknown")

        if not query_text:
            return jsonify({"error": "query field is required"}), 400

        # Step 1: Get user features from Feast
        user_features = {}
        if feast_store:
            try:
                # Get online features for the user
                features = feast_store.get_online_features(
                    features=[
                        "customer_features_view:tenure_months",
                        "customer_features_view:monthly_charges",
                        "customer_features_view:contract_type",
                    ],
                    entity_rows=[{"entity_id": user_id}],
                ).to_dict()

                user_features = {
                    "tenure_months": features.get("tenure_months", [None])[0],
                    "monthly_charges": features.get("monthly_charges", [None])[0],
                    "contract_type": features.get("contract_type", [None])[0],
                }
            except Exception as e:
                user_features = {"error": str(e)}

        # Step 2: Search Milvus for relevant documents
        # NOTE: In a real app, you'd embed the query_text first
        # For demo purposes, we'll just do a simple search
        documents = []
        if milvus_collection:
            try:
                # Load collection if not loaded
                milvus_collection.load()

                # For demonstration, create a dummy embedding (all zeros)
                # In production, use the same embedding model as ingestion
                from sentence_transformers import SentenceTransformer
                model = SentenceTransformer("all-MiniLM-L6-v2")
                query_embedding = model.encode([query_text])[0].tolist()

                # Search for similar documents
                results = milvus_collection.search(
                    data=[query_embedding],
                    anns_field="embedding",
                    param={"metric_type": "COSINE", "params": {"nprobe": 10}},
                    limit=TOP_K_DOCUMENTS,
                    output_fields=["text", "filename", "source"],
                )

                for hits in results:
                    for hit in hits:
                        documents.append({
                            "text": hit.entity.get("text", ""),
                            "filename": hit.entity.get("filename", ""),
                            "source": hit.entity.get("source", ""),
                            "score": float(hit.score),
                        })
            except Exception as e:
                documents = [{"error": str(e)}]

        # Step 3: Build response (in a real app, this would call the LLM)
        response = {
            "query": query_text,
            "user_id": user_id,
            "user_features": user_features,
            "retrieved_documents": documents,
            "model": MODEL_NAME,
            "timestamp": datetime.utcnow().isoformat(),
            "note": "This is a demo - in production, retrieved docs would be sent to LLM",
        }

        return jsonify(response), 200

    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route('/', methods=['GET'])
def root():
    """Root endpoint with API documentation."""
    return jsonify({
        "service": "RAG Inference Service",
        "endpoints": {
            "/health": "Health check",
            "/config": "Show configuration",
            "/query": "POST - RAG query (requires 'query' and optional 'user_id')",
        },
        "example_query": {
            "method": "POST",
            "url": "/query",
            "body": {
                "query": "What is machine learning?",
                "user_id": "customer_123"
            }
        }
    }), 200


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8000)
