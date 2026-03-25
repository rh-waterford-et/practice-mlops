"""
RAG Ingestion Pipeline for OpenShift AI

This pipeline ingests documents, chunks them, generates embeddings, and stores
them in a vector database (pgvector). All steps are tracked automatically via:
- KFP artifacts for data lineage
- MLflow for experiment tracking (with automatic OpenLineage integration)
- OPENLINEAGE_NAMESPACE injected by Argo workflow controller

Compile:
    python -m src.rag.rag_pipeline

Upload to OpenShift AI:
    python -m src.rag.upload_rag_pipeline
"""

from kfp import dsl, compiler

# Images built via OpenShift BuildConfig
FKM_IMAGE = (
    "image-registry.openshift-image-registry.svc:5000/lineage/fkm-app:latest"
)
RAG_IMAGE = (
    "image-registry.openshift-image-registry.svc:5000/lineage/rag-app:latest"
)


# =======================================================================
# STEP 1: Load Documents from MinIO
# =======================================================================
@dsl.component(base_image=FKM_IMAGE, packages_to_install=[])
def load_documents(
    minio_endpoint: str,
    bucket_name: str,
    prefix: str,
    aws_key: str,
    aws_secret: str,
    openlineage_url: str,
    output_docs: dsl.Output[dsl.Dataset],
) -> int:
    """Load documents from MinIO and save metadata."""
    import json
    import os
    from datetime import datetime, timezone
    from uuid import uuid4
    from minio import Minio
    from openlineage.client import OpenLineageClient
    from openlineage.client.run import RunEvent, RunState, Run, Job, Dataset
    from openlineage.client.facet import SchemaDatasetFacet, SchemaField, OutputStatisticsOutputDatasetFacet

    os.environ["AWS_ACCESS_KEY_ID"] = aws_key
    os.environ["AWS_SECRET_ACCESS_KEY"] = aws_secret

    client = Minio(
        minio_endpoint,
        access_key=aws_key,
        secret_key=aws_secret,
        secure=False,
    )

    documents = []
    source_files = []
    total_size = 0
    objects = client.list_objects(bucket_name, prefix=prefix, recursive=True)

    for obj in objects:
        if obj.object_name.endswith(('.txt', '.md', '.pdf')):
            # Download document
            response = client.get_object(bucket_name, obj.object_name)
            content = response.read().decode('utf-8')
            response.close()
            response.release_conn()

            documents.append({
                "source": f"s3://{bucket_name}/{obj.object_name}",
                "content": content,
                "size": len(content),
                "filename": obj.object_name.split('/')[-1],
            })
            source_files.append(obj.object_name)
            total_size += len(content)

    # Save documents as JSON (without indent to save memory)
    with open(output_docs.path, 'w') as f:
        json.dump(documents, f)

    print(f"Loaded {len(documents)} documents from MinIO")

    # Emit OpenLineage event
    namespace = os.environ.get("OPENLINEAGE_NAMESPACE", "default")
    ol_client = OpenLineageClient(url=openlineage_url)
    run_id = str(uuid4())

    # Input: grouped MinIO bucket prefix (all files as single source)
    input_dataset = Dataset(
        namespace=f"s3://{minio_endpoint}",
        name=f"{bucket_name}/{prefix}",
        facets={
            "schema": SchemaDatasetFacet(
                fields=[
                    SchemaField(name="filename", type="STRING"),
                    SchemaField(name="content", type="TEXT"),
                ]
            ),
        },
    )

    # Output: documents JSON dataset (stored as KFP artifact in MinIO)
    output_dataset = Dataset(
        namespace=f"s3://{minio_endpoint}",
        name="mlflow/artifacts/rag-ingestion/documents",
        facets={
            "schema": SchemaDatasetFacet(
                fields=[
                    SchemaField(name="source", type="STRING"),
                    SchemaField(name="content", type="TEXT"),
                    SchemaField(name="size", type="INTEGER"),
                    SchemaField(name="filename", type="STRING"),
                ]
            ),
            "outputStatistics": OutputStatisticsOutputDatasetFacet(
                rowCount=len(documents),
                size=total_size,
            ),
        },
    )

    event = RunEvent(
        eventType=RunState.COMPLETE,
        eventTime=datetime.now(timezone.utc).isoformat(),
        run=Run(runId=run_id, facets={}),
        job=Job(namespace=namespace, name="rag_load_documents", facets={}),
        inputs=[input_dataset],
        outputs=[output_dataset],
        producer="rag-ingestion-pipeline/load",
    )
    ol_client.emit(event)
    print(f"OpenLineage event emitted: {namespace}/rag_load_documents")

    return len(documents)


# =======================================================================
# STEP 2: Chunk Documents
# =======================================================================
@dsl.component(base_image=FKM_IMAGE, packages_to_install=[])
def chunk_documents(
    input_docs: dsl.Input[dsl.Dataset],
    chunk_size: int,
    chunk_overlap: int,
    minio_endpoint: str,
    openlineage_url: str,
    output_chunks: dsl.Output[dsl.Dataset],
) -> int:
    """Split documents into chunks with overlap."""
    import json
    import gc
    import os
    from datetime import datetime, timezone
    from uuid import uuid4
    from openlineage.client import OpenLineageClient
    from openlineage.client.run import RunEvent, RunState, Run, Job, Dataset
    from openlineage.client.facet import SchemaDatasetFacet, SchemaField, OutputStatisticsOutputDatasetFacet

    # Load documents
    with open(input_docs.path) as f:
        documents = json.load(f)

    print(f"Loaded {len(documents)} documents")

    # Process one document at a time and write incrementally to save memory
    chunks = []
    chunk_id = 0
    total_chars = 0

    for idx, doc in enumerate(documents):
        content = doc["content"]
        doc_chunks = []

        # Simple character-based chunking
        start = 0
        while start < len(content):
            end = min(start + chunk_size, len(content))
            chunk_text = content[start:end]

            # Try to break at sentence boundaries if possible
            if end < len(content):
                # Look for last period, newline, or space
                for delimiter in ['. ', '\n\n', '\n', ' ']:
                    last_delim = chunk_text.rfind(delimiter)
                    if last_delim > chunk_size // 2:  # At least halfway through
                        end = start + last_delim + len(delimiter)
                        chunk_text = content[start:end]
                        break

            chunk_stripped = chunk_text.strip()
            doc_chunks.append({
                "chunk_id": chunk_id,
                "source": doc["source"],
                "filename": doc["filename"],
                "text": chunk_stripped,
                "start_char": start,
                "end_char": end,
            })
            total_chars += len(chunk_stripped)

            chunk_id += 1
            start = max(end - chunk_overlap, start + 1)

        chunks.extend(doc_chunks)
        print(f"Processed document {idx + 1}/{len(documents)}: {len(doc_chunks)} chunks")

        # Clear references to free memory
        del doc_chunks
        del content
        gc.collect()

    # Save chunks
    print(f"Writing {len(chunks)} total chunks to output")
    with open(output_chunks.path, 'w') as f:
        json.dump(chunks, f)

    # Emit OpenLineage event
    namespace = os.environ.get("OPENLINEAGE_NAMESPACE", "default")
    ol_client = OpenLineageClient(url=openlineage_url)
    run_id = str(uuid4())

    input_dataset = Dataset(
        namespace=f"s3://{minio_endpoint}",
        name="mlflow/artifacts/rag-ingestion/documents",
        facets={},
    )

    output_dataset = Dataset(
        namespace=f"s3://{minio_endpoint}",
        name="mlflow/artifacts/rag-ingestion/chunks",
        facets={
            "schema": SchemaDatasetFacet(
                fields=[
                    SchemaField(name="chunk_id", type="INTEGER"),
                    SchemaField(name="source", type="STRING"),
                    SchemaField(name="filename", type="STRING"),
                    SchemaField(name="text", type="TEXT"),
                    SchemaField(name="start_char", type="INTEGER"),
                    SchemaField(name="end_char", type="INTEGER"),
                ]
            ),
            "outputStatistics": OutputStatisticsOutputDatasetFacet(
                rowCount=len(chunks),
                size=total_chars,
            ),
        },
    )

    event = RunEvent(
        eventType=RunState.COMPLETE,
        eventTime=datetime.now(timezone.utc).isoformat(),
        run=Run(runId=run_id, facets={}),
        job=Job(namespace=namespace, name="rag_chunk_documents", facets={}),
        inputs=[input_dataset],
        outputs=[output_dataset],
        producer="rag-ingestion-pipeline/chunk",
    )
    ol_client.emit(event)
    print(f"OpenLineage event emitted: {namespace}/rag_chunk_documents")

    del chunks
    gc.collect()

    print(f"Created {chunk_id} chunks from {len(documents)} documents")
    return chunk_id


# =======================================================================
# STEP 3: Generate Embeddings
# =======================================================================
@dsl.component(base_image=RAG_IMAGE, packages_to_install=[])
def generate_embeddings(
    input_chunks: dsl.Input[dsl.Dataset],
    model_name: str,
    minio_endpoint: str,
    openlineage_url: str,
    output_embeddings: dsl.Output[dsl.Dataset],
) -> str:
    """Generate embeddings using sentence-transformers."""
    import json
    import os
    from datetime import datetime, timezone
    from uuid import uuid4
    from sentence_transformers import SentenceTransformer
    from openlineage.client import OpenLineageClient
    from openlineage.client.run import RunEvent, RunState, Run, Job, Dataset
    from openlineage.client.facet import SchemaDatasetFacet, SchemaField, OutputStatisticsOutputDatasetFacet

    with open(input_chunks.path) as f:
        chunks = json.load(f)

    print(f"Loading model: {model_name}")
    model = SentenceTransformer(model_name)

    # Extract texts
    texts = [chunk["text"] for chunk in chunks]

    print(f"Generating embeddings for {len(texts)} chunks...")
    embeddings = model.encode(texts, show_progress_bar=True)

    # Add embeddings to chunks
    for chunk, embedding in zip(chunks, embeddings):
        chunk["embedding"] = embedding.tolist()

    # Save chunks with embeddings
    with open(output_embeddings.path, 'w') as f:
        json.dump(chunks, f)

    embedding_dim = embeddings.shape[1]
    print(f"Generated {len(embeddings)} embeddings of dimension {embedding_dim}")

    # Emit OpenLineage event
    namespace = os.environ.get("OPENLINEAGE_NAMESPACE", "default")
    ol_client = OpenLineageClient(url=openlineage_url)
    run_id = str(uuid4())

    input_dataset = Dataset(
        namespace=f"s3://{minio_endpoint}",
        name="mlflow/artifacts/rag-ingestion/chunks",
        facets={},
    )

    output_dataset = Dataset(
        namespace=f"s3://{minio_endpoint}",
        name="mlflow/artifacts/rag-ingestion/embeddings",
        facets={
            "schema": SchemaDatasetFacet(
                fields=[
                    SchemaField(name="chunk_id", type="INTEGER"),
                    SchemaField(name="source", type="STRING"),
                    SchemaField(name="filename", type="STRING"),
                    SchemaField(name="text", type="TEXT"),
                    SchemaField(name="start_char", type="INTEGER"),
                    SchemaField(name="end_char", type="INTEGER"),
                    SchemaField(name="embedding", type=f"FLOAT_VECTOR({embedding_dim})"),
                ]
            ),
            "outputStatistics": OutputStatisticsOutputDatasetFacet(
                rowCount=len(embeddings),
            ),
        },
    )

    event = RunEvent(
        eventType=RunState.COMPLETE,
        eventTime=datetime.now(timezone.utc).isoformat(),
        run=Run(runId=run_id, facets={}),
        job=Job(
            namespace=namespace,
            name="rag_generate_embeddings",
            facets={},
        ),
        inputs=[input_dataset],
        outputs=[output_dataset],
        producer=f"rag-ingestion-pipeline/embed/{model_name}",
    )
    ol_client.emit(event)
    print(f"OpenLineage event emitted: {namespace}/rag_generate_embeddings")

    return json.dumps({
        "model": model_name,
        "num_embeddings": len(embeddings),
        "embedding_dim": int(embedding_dim),
    })


# =======================================================================
# STEP 4: Store in Milvus + Emit OpenLineage Event
# =======================================================================
@dsl.component(base_image=RAG_IMAGE, packages_to_install=[])
def store_in_milvus(
    input_embeddings: dsl.Input[dsl.Dataset],
    milvus_host: str,
    milvus_port: int,
    collection_name: str,
    minio_endpoint: str,
    openlineage_url: str,
) -> str:
    """Store embeddings in Milvus vector database and emit OpenLineage event."""
    import json
    import os
    from datetime import datetime, timezone
    from uuid import uuid4
    from pymilvus import (
        connections,
        Collection,
        CollectionSchema,
        FieldSchema,
        DataType,
        utility,
    )
    from openlineage.client import OpenLineageClient
    from openlineage.client.run import RunEvent, RunState, Run, Job, Dataset
    from openlineage.client.facet import (
        SchemaDatasetFacet,
        SchemaField,
        OutputStatisticsOutputDatasetFacet,
    )

    with open(input_embeddings.path) as f:
        chunks = json.load(f)

    embedding_dim = len(chunks[0]["embedding"])

    # Connect to Milvus
    print(f"Connecting to Milvus at {milvus_host}:{milvus_port}")
    connections.connect(
        alias="default",
        host=milvus_host,
        port=milvus_port,
    )

    # Create collection if it doesn't exist
    if utility.has_collection(collection_name):
        print(f"Collection '{collection_name}' already exists, dropping it...")
        utility.drop_collection(collection_name)

    # Define collection schema
    fields = [
        FieldSchema(name="id", dtype=DataType.INT64, is_primary=True, auto_id=True),
        FieldSchema(name="chunk_id", dtype=DataType.INT64),
        FieldSchema(name="source", dtype=DataType.VARCHAR, max_length=512),
        FieldSchema(name="filename", dtype=DataType.VARCHAR, max_length=256),
        FieldSchema(name="text", dtype=DataType.VARCHAR, max_length=65535),
        FieldSchema(name="start_char", dtype=DataType.INT64),
        FieldSchema(name="end_char", dtype=DataType.INT64),
        FieldSchema(name="embedding", dtype=DataType.FLOAT_VECTOR, dim=embedding_dim),
    ]

    schema = CollectionSchema(
        fields=fields,
        description=f"RAG document chunks for {collection_name}",
    )

    print(f"Creating collection '{collection_name}' with {embedding_dim}-dim embeddings...")
    collection = Collection(
        name=collection_name,
        schema=schema,
    )

    # Prepare data for insertion
    data = [
        [chunk["chunk_id"] for chunk in chunks],  # chunk_id
        [chunk["source"] for chunk in chunks],  # source
        [chunk["filename"] for chunk in chunks],  # filename
        [chunk["text"] for chunk in chunks],  # text
        [chunk["start_char"] for chunk in chunks],  # start_char
        [chunk["end_char"] for chunk in chunks],  # end_char
        [chunk["embedding"] for chunk in chunks],  # embedding vectors
    ]

    # Insert data
    print(f"Inserting {len(chunks)} chunks into Milvus...")
    collection.insert(data)

    # Create index for vector similarity search
    print("Creating IVF_FLAT index for similarity search...")
    index_params = {
        "metric_type": "COSINE",
        "index_type": "IVF_FLAT",
        "params": {"nlist": 128},
    }
    collection.create_index(
        field_name="embedding",
        index_params=index_params,
    )

    # Load collection into memory
    collection.load()

    # Get collection stats
    num_entities = collection.num_entities
    print(f"Stored {num_entities} chunks in Milvus collection: {collection_name}")

    # Disconnect from Milvus
    connections.disconnect("default")

    # Get namespace from environment (injected by Argo workflow controller)
    namespace = os.environ.get("OPENLINEAGE_NAMESPACE", "default")

    # Emit OpenLineage event
    ol_client = OpenLineageClient(url=openlineage_url)

    run_id = str(uuid4())
    now = datetime.now(timezone.utc).isoformat()

    # Input: embeddings from previous step (stored in MinIO as KFP artifact)
    input_dataset = Dataset(
        namespace=f"s3://{minio_endpoint}",
        name="mlflow/artifacts/rag-ingestion/embeddings",
        facets={},
    )

    # Create schema facet for Milvus collection
    schema_fields = [
        SchemaField(name="id", type="INT64", description="Auto-generated primary key"),
        SchemaField(name="chunk_id", type="INT64", description="Chunk identifier"),
        SchemaField(name="source", type="VARCHAR(512)", description="Source document path"),
        SchemaField(name="filename", type="VARCHAR(256)", description="Source filename"),
        SchemaField(name="text", type="VARCHAR(65535)", description="Chunk text content"),
        SchemaField(name="start_char", type="INT64", description="Start character position"),
        SchemaField(name="end_char", type="INT64", description="End character position"),
        SchemaField(name="embedding", type=f"FLOAT_VECTOR({embedding_dim})", description="Vector embedding"),
    ]

    output_dataset = Dataset(
        namespace=f"milvus://{milvus_host}:{milvus_port}",
        name=collection_name,
        facets={
            "schema": SchemaDatasetFacet(fields=schema_fields),
            "outputStatistics": OutputStatisticsOutputDatasetFacet(
                rowCount=num_entities,
                size=sum(len(chunk["text"]) for chunk in chunks),
            ),
        },
    )

    # Create the job
    job = Job(
        namespace=namespace,
        name="rag_store_milvus",
        facets={},
    )

    # Create the run
    run = Run(runId=run_id, facets={})

    # Emit COMPLETE event
    event = RunEvent(
        eventType=RunState.COMPLETE,
        eventTime=now,
        run=run,
        job=job,
        inputs=[input_dataset],
        outputs=[output_dataset],
        producer="rag-ingestion-pipeline/milvus",
    )

    ol_client.emit(event)
    print(f"OpenLineage event emitted: {namespace}/rag_store_milvus (run: {run_id})")

    return json.dumps({
        "collection_name": collection_name,
        "num_chunks": num_entities,
        "embedding_dim": embedding_dim,
        "index_type": "IVF_FLAT",
        "metric_type": "COSINE",
        "openlineage_run_id": run_id,
    })


# =======================================================================
# Pipeline Definition
# =======================================================================
@dsl.pipeline(
    name="RAG Document Ingestion Pipeline",
    description=(
        "End-to-end RAG ingestion: load documents, chunk, embed, store in Milvus. "
        "Each step emits OpenLineage events tracking inputs/outputs. "
        "OPENLINEAGE_NAMESPACE injected by Argo workflow controller."
    ),
)
def rag_ingestion_pipeline(
    minio_endpoint: str = "mlflow-minio:9000",
    bucket_name: str = "data",
    document_prefix: str = "sample_docs/",
    chunk_size: int = 1000,
    chunk_overlap: int = 200,
    embedding_model: str = "all-MiniLM-L6-v2",
    milvus_host: str = "milvus",
    milvus_port: int = 19530,
    collection_name: str = "ml_docs",
    openlineage_url: str = "http://marquez",
    aws_key: str = "minioadmin",
    aws_secret: str = "minioadmin123",
):
    """RAG ingestion pipeline with Milvus and OpenLineage tracking."""

    # Step 1: Load documents from MinIO
    load_task = load_documents(
        minio_endpoint=minio_endpoint,
        bucket_name=bucket_name,
        prefix=document_prefix,
        aws_key=aws_key,
        aws_secret=aws_secret,
        openlineage_url=openlineage_url,
    )
    load_task.set_caching_options(False)

    # Step 2: Chunk documents
    chunk_task = chunk_documents(
        input_docs=load_task.outputs["output_docs"],
        chunk_size=chunk_size,
        chunk_overlap=chunk_overlap,
        minio_endpoint=minio_endpoint,
        openlineage_url=openlineage_url,
    )
    chunk_task.set_caching_options(False)

    # Step 3: Generate embeddings
    embed_task = generate_embeddings(
        input_chunks=chunk_task.outputs["output_chunks"],
        model_name=embedding_model,
        minio_endpoint=minio_endpoint,
        openlineage_url=openlineage_url,
    )
    embed_task.set_caching_options(False)

    # Step 4: Store in Milvus and emit OpenLineage event
    store_task = store_in_milvus(
        input_embeddings=embed_task.outputs["output_embeddings"],
        milvus_host=milvus_host,
        milvus_port=milvus_port,
        collection_name=collection_name,
        minio_endpoint=minio_endpoint,
        openlineage_url=openlineage_url,
    )
    store_task.set_caching_options(False)


# =======================================================================
# Compile to YAML
# =======================================================================
if __name__ == "__main__":
    compiler.Compiler().compile(
        pipeline_func=rag_ingestion_pipeline,
        package_path="rag_ingestion_pipeline.yaml",
    )
    print("Pipeline compiled to rag_ingestion_pipeline.yaml")
