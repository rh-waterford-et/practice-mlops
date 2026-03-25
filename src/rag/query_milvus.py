"""
RAG Query Interface for Milvus - Semantic search over ingested documents.

Usage:
    python -m src.rag.query_milvus "What is MLOps?" --collection ml_docs
"""
import argparse
import json
import os
from typing import List, Dict

from sentence_transformers import SentenceTransformer
from pymilvus import connections, Collection


def semantic_search(
    query: str,
    collection_name: str = "ml_docs",
    model_name: str = "all-MiniLM-L6-v2",
    top_k: int = 5,
    milvus_host: str = None,
    milvus_port: int = 19530,
) -> List[Dict]:
    """
    Perform semantic search over document chunks in Milvus.

    Args:
        query: Search query text
        collection_name: Name of the Milvus collection
        model_name: Sentence transformer model (must match ingestion)
        top_k: Number of results to return
        milvus_host: Milvus host (defaults to env var or localhost)
        milvus_port: Milvus port (default: 19530)

    Returns:
        List of matching chunks with similarity scores
    """
    # Get Milvus connection info
    if milvus_host is None:
        milvus_host = os.getenv("MILVUS_HOST", "localhost")

    # Load embedding model
    print(f"Loading embedding model: {model_name}")
    model = SentenceTransformer(model_name)

    # Generate query embedding
    print(f"Generating embedding for query: {query[:50]}...")
    query_embedding = model.encode(query).tolist()

    # Connect to Milvus
    print(f"Connecting to Milvus at {milvus_host}:{milvus_port}")
    connections.connect(
        alias="default",
        host=milvus_host,
        port=milvus_port,
    )

    # Load collection
    collection = Collection(collection_name)
    collection.load()

    # Define search parameters
    search_params = {
        "metric_type": "COSINE",
        "params": {"nprobe": 10},
    }

    # Perform search
    results = collection.search(
        data=[query_embedding],
        anns_field="embedding",
        param=search_params,
        limit=top_k,
        output_fields=["chunk_id", "source", "filename", "text"],
    )

    # Format results
    formatted_results = []
    for hits in results:
        for hit in hits:
            formatted_results.append({
                "chunk_id": hit.entity.get("chunk_id"),
                "source": hit.entity.get("source"),
                "filename": hit.entity.get("filename"),
                "text": hit.entity.get("text"),
                "similarity": hit.distance,  # Cosine similarity (higher is better)
            })

    # Disconnect from Milvus
    connections.disconnect("default")

    return formatted_results


def main():
    parser = argparse.ArgumentParser(
        description="Semantic search over RAG document collection in Milvus"
    )
    parser.add_argument("query", help="Search query")
    parser.add_argument(
        "--collection",
        default="ml_docs",
        help="Collection name (default: ml_docs)",
    )
    parser.add_argument(
        "--model",
        default="all-MiniLM-L6-v2",
        help="Embedding model (default: all-MiniLM-L6-v2)",
    )
    parser.add_argument(
        "--top-k",
        type=int,
        default=5,
        help="Number of results (default: 5)",
    )
    parser.add_argument("--milvus-host", help="Milvus host")
    parser.add_argument("--milvus-port", type=int, default=19530, help="Milvus port")

    args = parser.parse_args()

    print(f"\n🔍 Searching for: {args.query}")
    print(f"📚 Collection: {args.collection}")
    print(f"🎯 Top {args.top_k} results\n")

    results = semantic_search(
        query=args.query,
        collection_name=args.collection,
        model_name=args.model,
        top_k=args.top_k,
        milvus_host=args.milvus_host,
        milvus_port=args.milvus_port,
    )

    if not results:
        print("❌ No results found")
        return

    print("=" * 80)
    for i, result in enumerate(results, 1):
        print(f"\n📄 Result {i} - Similarity: {result['similarity']:.4f}")
        print(f"   Source: {result['source']}")
        print(f"   File: {result['filename']}")
        print(f"\n   {result['text'][:300]}...")
        print("=" * 80)


if __name__ == "__main__":
    main()
