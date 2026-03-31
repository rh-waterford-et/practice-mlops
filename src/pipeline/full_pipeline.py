"""
Full Pipeline: Customer Churn + RAG Ingestion

Combines the churn ML pipeline (8 steps) and RAG ingestion pipeline (4 steps)
into a single 12-step KFP pipeline.  RAG steps run sequentially after the
churn branch completes.

Compile:
    python -m src.pipeline.full_pipeline

The individual pipelines remain available for standalone use:
    python -m src.pipeline.kfp_pipeline
    python -m src.rag.rag_pipeline
"""

from kfp import compiler, dsl
from kfp import kubernetes

from src.pipeline.kfp_pipeline import (
    ds_data_extraction,
    ds_evaluation,
    ds_feature_engineering,
    ds_model_registration,
    ds_model_training,
    platform_feast_apply,
    platform_feast_materialize,
    platform_spark_etl,
)
from src.rag.rag_pipeline import (
    chunk_documents,
    generate_embeddings,
    load_documents,
    store_in_milvus,
    test_inference,
)

PIPELINE_NAME = "full-ml-rag-pipeline"


@dsl.pipeline(
    name="Full ML + RAG Pipeline",
    description=(
        "Combined pipeline: Customer Churn ML (Spark ETL, Feast, XGBoost/MLflow) "
        "followed by RAG document ingestion (load, chunk, embed, Milvus). "
        "All steps emit OpenLineage events."
    ),
)
def full_pipeline(
    # -- Churn parameters --
    pg_url: str = "postgresql://feast:feast@postgres:5432/warehouse",
    feast_repo_path: str = "/app/src/feature_store",
    table_name: str = "customer_features",
    pg_host: str = "postgres",
    redis_host: str = "redis",
    tracking_uri: str = "openlineage+http://mlflow-server:5000",
    experiment_name: str = "customer_churn_lineage",
    model_name: str = "customer_churn_model",
    s3_endpoint: str = "http://mlflow-minio:9000",
    aws_key: str = "minioadmin",
    aws_secret: str = "minioadmin123",
    roc_auc_threshold: float = 0.70,
    # -- RAG parameters --
    rag_bucket_name: str = "data",
    rag_document_prefix: str = "sample_docs/",
    rag_chunk_size: int = 1000,
    rag_chunk_overlap: int = 200,
    rag_embedding_model: str = "all-MiniLM-L6-v2",
    milvus_host: str = "milvus",
    milvus_port: int = 19530,
    milvus_collection: str = "ml_docs",
):
    # ==================================================================
    # CHURN BRANCH — Platform steps
    # ==================================================================

    etl_task = platform_spark_etl(
        minio_endpoint="mlflow-minio:9000",
        pg_host=pg_host,
        pg_user="feast",
        pg_password="feast",
        pg_database="warehouse",
        warehouse_table=table_name,
        openlineage_url="http://marquez",
        aws_access_key=aws_key,
        aws_secret_key=aws_secret,
    )
    etl_task.set_caching_options(False)

    apply_task = platform_feast_apply(
        feast_repo_path=feast_repo_path,
        pg_host=pg_host,
        redis_host=redis_host,
    )
    apply_task.after(etl_task)
    apply_task.set_caching_options(False)

    materialize_task = platform_feast_materialize(
        feast_repo_path=feast_repo_path,
        pg_host=pg_host,
        redis_host=redis_host,
        apply_done=apply_task.output,
    )
    materialize_task.set_caching_options(False)

    # ==================================================================
    # CHURN BRANCH — DS steps
    # ==================================================================

    extract_task = ds_data_extraction(
        pg_url=pg_url,
        feast_repo_path=feast_repo_path,
        table_name=table_name,
        pg_host=pg_host,
        redis_host=redis_host,
        materialize_done=materialize_task.output,
    )
    extract_task.set_caching_options(False)

    engineer_task = ds_feature_engineering(
        dataset=extract_task.outputs["output_path"],
    )
    engineer_task.set_caching_options(False)

    train_task = ds_model_training(
        dataset=engineer_task.outputs["output_path"],
        tracking_uri=tracking_uri,
        experiment_name=experiment_name,
        s3_endpoint=s3_endpoint,
        aws_key=aws_key,
        aws_secret=aws_secret,
    )
    train_task.set_caching_options(False)

    eval_task = ds_evaluation(
        train_result_json=train_task.output,
    )
    eval_task.set_caching_options(False)

    reg_task = ds_model_registration(
        train_result_json=train_task.output,
        metrics_json=eval_task.output,
        model_name=model_name,
        tracking_uri=tracking_uri,
        s3_endpoint=s3_endpoint,
        aws_key=aws_key,
        aws_secret=aws_secret,
        roc_auc_threshold=roc_auc_threshold,
    )
    reg_task.set_caching_options(False)

    # ==================================================================
    # RAG BRANCH — runs after churn completes
    # ==================================================================

    minio_endpoint = "mlflow-minio:9000"

    load_task = load_documents(
        minio_endpoint=minio_endpoint,
        bucket_name=rag_bucket_name,
        prefix=rag_document_prefix,
        aws_key=aws_key,
        aws_secret=aws_secret,
        openlineage_url="http://marquez",
    )
    load_task.after(reg_task)
    load_task.set_caching_options(False)

    chunk_task = chunk_documents(
        input_docs=load_task.outputs["output_docs"],
        chunk_size=rag_chunk_size,
        chunk_overlap=rag_chunk_overlap,
        minio_endpoint=minio_endpoint,
        openlineage_url="http://marquez",
    )
    chunk_task.set_caching_options(False)

    embed_task = generate_embeddings(
        input_chunks=chunk_task.outputs["output_chunks"],
        model_name=rag_embedding_model,
        minio_endpoint=minio_endpoint,
        openlineage_url="http://marquez",
    )
    embed_task.set_caching_options(False)

    store_task = store_in_milvus(
        input_embeddings=embed_task.outputs["output_embeddings"],
        milvus_host=milvus_host,
        milvus_port=milvus_port,
        collection_name=milvus_collection,
        minio_endpoint=minio_endpoint,
        openlineage_url="http://marquez",
    )
    store_task.set_caching_options(False)

    # ==================================================================
    # INFERENCE — connects both branches via shared datasets
    # ==================================================================

    inference_task = test_inference(
        inference_url="http://rag-inference",
        milvus_host=milvus_host,
        milvus_port=milvus_port,
        milvus_collection=milvus_collection,
        openlineage_url="http://marquez",
        store_done=store_task.output,
    )
    inference_task.set_caching_options(False)

    # ==================================================================
    # Inject parent run context for all 13 tasks
    # ==================================================================

    for task in [etl_task, apply_task, materialize_task, extract_task,
                 engineer_task, train_task, eval_task, reg_task,
                 load_task, chunk_task, embed_task, store_task,
                 inference_task]:
        kubernetes.use_field_path_as_env(
            task, "OPENLINEAGE_PARENT_RUN_ID",
            "metadata.labels['workflows.argoproj.io/workflow']",
        )
        task.set_env_variable(
            "OPENLINEAGE_PARENT_JOB_NAME", PIPELINE_NAME,
        )


if __name__ == "__main__":
    compiler.Compiler().compile(
        pipeline_func=full_pipeline,
        package_path="full_pipeline.yaml",
    )
    print("Pipeline compiled -> full_pipeline.yaml")
