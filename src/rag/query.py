"""
RAG Query Interface - Semantic search over ingested documents.

Usage:
    python -m src.rag.query "What is MLOps?" --collection ml_docs
"""
import argparse
import json
import os
from typing import List, Dict

import numpy as np
from sentence_transformers import SentenceTransformer
from sqlalchemy import create_engine, text


def semantic_search(
    query: str,
    collection_name: str = "ml_docs",
    model_name: str = "all-MiniLM-L6-v2",
    top_k: int = 5,
    pg_host: str = None,
    pg_user: str = "feast",
    pg_password: str = "feast",
    pg_database: str = "warehouse",
) -> List[Dict]:
    """
    Perform semantic search over document chunks.

    Args:
        query: Search query text
        collection_name: Name of the RAG collection (table suffix)
        model_name: Sentence transformer model (must match ingestion)
        top_k: Number of results to return
        pg_host: PostgreSQL host (defaults to env var or localhost)
        pg_user: PostgreSQL user
        pg_password: PostgreSQL password
        pg_database: PostgreSQL database

    Returns:
        List of matching chunks with similarity scores
    """
    # Get PostgreSQL connection info
    if pg_host is None:
        pg_host = os.getenv("PG_HOST", "localhost")

    # Load embedding model
    print(f"Loading embedding model: {model_name}")
    model = SentenceTransformer(model_name)

    # Generate query embedding
    print(f"Generating embedding for query: {query[:50]}...")
    query_embedding = model.encode(query)

    # Connect to database
    engine = create_engine(
        f"postgresql://{pg_user}:{pg_password}@{pg_host}:5432/{pg_database}"
    )

    table_name = f"rag_{collection_name}"

    with engine.connect() as conn:
        # Perform vector similarity search
        # Using cosine similarity: 1 - (embedding <=> query_embedding)
        result = conn.execute(text(f"""
            SELECT
                chunk_id,
                source,
                filename,
                text,
                1 - (embedding <=> :query_embedding::vector) AS similarity
            FROM {table_name}
            ORDER BY embedding <=> :query_embedding::vector
            LIMIT :top_k
        """), {
            "query_embedding": query_embedding.tolist(),
            "top_k": top_k,
        })

        results = []
        for row in result:
            results.append({
                "chunk_id": row.chunk_id,
                "source": row.source,
                "filename": row.filename,
                "text": row.text,
                "similarity": float(row.similarity),
            })

    engine.dispose()

    return results


def main():
    parser = argparse.ArgumentParser(
        description="Semantic search over RAG document collection"
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
    parser.add_argument("--pg-host", help="PostgreSQL host")
    parser.add_argument("--pg-user", default="feast", help="PostgreSQL user")
    parser.add_argument("--pg-password", default="feast", help="PostgreSQL password")
    parser.add_argument("--pg-database", default="warehouse", help="PostgreSQL database")

    args = parser.parse_args()

    print(f"\n🔍 Searching for: {args.query}")
    print(f"📚 Collection: {args.collection}")
    print(f"🎯 Top {args.top_k} results\n")

    results = semantic_search(
        query=args.query,
        collection_name=args.collection,
        model_name=args.model,
        top_k=args.top_k,
        pg_host=args.pg_host,
        pg_user=args.pg_user,
        pg_password=args.pg_password,
        pg_database=args.pg_database,
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
