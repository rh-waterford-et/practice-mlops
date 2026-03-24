"""
OpenLineage facets for enriching events with metadata.

This module provides builders for standard OpenLineage facets as well as
custom facets specific to ML workflows.

Design Decision: Dict-Based Facets
----------------------------------
We use dictionaries rather than dataclasses/Pydantic models because:
1. OpenLineage spec allows arbitrary facets - strict typing is limiting
2. Simpler to serialize to JSON
3. Easier to extend with custom facets
4. Matches how OpenLineage SDK works internally

Standard Facets (from OpenLineage spec):
- SchemaDatasetFacet: Column names and types
- DocumentationDatasetFacet: Human-readable description
- DocumentationJobFacet: Job description
- ErrorMessageRunFacet: Error information for failed runs
- JobTypeJobFacet: Classification of job type

Custom Facets (ML-specific):
- Defined in adapter-specific facets.py modules
"""

from typing import Any, Optional

# OpenLineage schema URLs
SCHEMA_BASE = "https://openlineage.io/spec/facets/1-0-0"
OPENLINEAGE_SPEC = "https://openlineage.io/spec/2-0-0/OpenLineage.json"

# Default producer for facets
DEFAULT_PRODUCER = "https://github.com/openlineage-oai"


def create_schema_facet(
    fields: list[dict[str, str]],
    producer: str = DEFAULT_PRODUCER,
) -> dict[str, Any]:
    """
    Create a SchemaDatasetFacet for describing dataset columns.

    Args:
        fields: List of field definitions, each with "name" and "type" keys.
                Optionally include "description".
        producer: Producer identifier

    Returns:
        SchemaDatasetFacet dictionary

    Example:
        schema = create_schema_facet([
            {"name": "id", "type": "int64"},
            {"name": "name", "type": "string", "description": "User name"},
        ])
    """
    return {
        "_producer": producer,
        "_schemaURL": f"{SCHEMA_BASE}/SchemaDatasetFacet.json",
        "fields": fields,
    }


def create_documentation_facet(
    description: str,
    producer: str = DEFAULT_PRODUCER,
) -> dict[str, Any]:
    """
    Create a DocumentationDatasetFacet for dataset description.

    Args:
        description: Human-readable description of the dataset
        producer: Producer identifier

    Returns:
        DocumentationDatasetFacet dictionary
    """
    return {
        "_producer": producer,
        "_schemaURL": f"{SCHEMA_BASE}/DocumentationDatasetFacet.json",
        "description": description,
    }


def create_job_documentation_facet(
    description: str,
    producer: str = DEFAULT_PRODUCER,
) -> dict[str, Any]:
    """
    Create a DocumentationJobFacet for job description.

    Args:
        description: Human-readable description of the job
        producer: Producer identifier

    Returns:
        DocumentationJobFacet dictionary
    """
    return {
        "_producer": producer,
        "_schemaURL": f"{SCHEMA_BASE}/DocumentationJobFacet.json",
        "description": description,
    }


def create_job_type_facet(
    job_type: str,
    integration: str = "OPENLINEAGE_ML",
    processing_type: str = "BATCH",
    producer: str = DEFAULT_PRODUCER,
) -> dict[str, Any]:
    """
    Create a JobTypeJobFacet for classifying job types.

    Args:
        job_type: Type of job (e.g., "ML_TRAINING", "ETL", "INFERENCE")
        integration: Integration name (e.g., "MLFLOW", "RAY", "KFP")
        processing_type: Processing type (e.g., "BATCH", "STREAMING")
        producer: Producer identifier

    Returns:
        JobTypeJobFacet dictionary
    """
    return {
        "_producer": producer,
        "_schemaURL": f"{SCHEMA_BASE}/JobTypeJobFacet.json",
        "jobType": job_type,
        "integration": integration,
        "processingType": processing_type,
    }


def create_source_code_location_facet(
    url: str,
    path: str = "",
    repo_type: str = "git",
    version: str = "",
    producer: str = DEFAULT_PRODUCER,
) -> dict[str, Any]:
    """
    Create a SourceCodeLocationJobFacet for linking to source code.

    Args:
        url: Repository URL (e.g., "https://github.com/org/repo")
        path: Path to file within repository (e.g., "src/train.py")
        repo_type: Repository type (e.g., "git", "svn")
        version: Version/commit/tag (e.g., "v1.0.0", "abc123")
        producer: Producer identifier

    Returns:
        SourceCodeLocationJobFacet dictionary
    """
    facet = {
        "_producer": producer,
        "_schemaURL": f"{SCHEMA_BASE}/SourceCodeLocationJobFacet.json",
        "type": repo_type,
        "url": url,
    }
    if path:
        facet["path"] = path
    if version:
        facet["version"] = version
    return facet


def create_error_facet(
    message: str,
    stack_trace: Optional[str] = None,
    programming_language: str = "python",
    producer: str = DEFAULT_PRODUCER,
) -> dict[str, Any]:
    """
    Create an ErrorMessageRunFacet for failed runs.

    Args:
        message: Error message
        stack_trace: Optional stack trace
        programming_language: Language (default: "python")
        producer: Producer identifier

    Returns:
        ErrorMessageRunFacet dictionary
    """
    facet = {
        "_producer": producer,
        "_schemaURL": f"{SCHEMA_BASE}/ErrorMessageRunFacet.json",
        "message": message,
        "programmingLanguage": programming_language,
    }
    if stack_trace:
        facet["stackTrace"] = stack_trace
    return facet


def create_external_query_facet(
    source: str,
    query: str,
    producer: str = DEFAULT_PRODUCER,
) -> dict[str, Any]:
    """
    Create an ExternalQueryRunFacet for SQL or other queries.

    Args:
        source: Query source/engine (e.g., "postgresql", "spark")
        query: The query string
        producer: Producer identifier

    Returns:
        ExternalQueryRunFacet dictionary
    """
    return {
        "_producer": producer,
        "_schemaURL": f"{SCHEMA_BASE}/ExternalQueryRunFacet.json",
        "externalQueryId": "",
        "source": source,
        "query": query,
    }


def create_parent_run_facet(
    run_id: str,
    job_name: str,
    job_namespace: str,
    producer: str = DEFAULT_PRODUCER,
) -> dict[str, Any]:
    """
    Create a ParentRunFacet to link child runs to parent runs.

    Args:
        run_id: Parent run ID
        job_name: Parent job name
        job_namespace: Parent job namespace
        producer: Producer identifier

    Returns:
        ParentRunFacet dictionary
    """
    return {
        "_producer": producer,
        "_schemaURL": f"{SCHEMA_BASE}/ParentRunFacet.json",
        "run": {
            "runId": run_id,
        },
        "job": {
            "namespace": job_namespace,
            "name": job_name,
        },
    }


def create_nominal_time_facet(
    nominal_start_time: str,
    nominal_end_time: Optional[str] = None,
    producer: str = DEFAULT_PRODUCER,
) -> dict[str, Any]:
    """
    Create a NominalTimeRunFacet for scheduled run times.

    Args:
        nominal_start_time: ISO 8601 timestamp for scheduled start
        nominal_end_time: ISO 8601 timestamp for scheduled end
        producer: Producer identifier

    Returns:
        NominalTimeRunFacet dictionary
    """
    facet = {
        "_producer": producer,
        "_schemaURL": f"{SCHEMA_BASE}/NominalTimeRunFacet.json",
        "nominalStartTime": nominal_start_time,
    }
    if nominal_end_time:
        facet["nominalEndTime"] = nominal_end_time
    return facet


# ============================================================================
# Input/Output Dataset Builders
# ============================================================================


def build_input_dataset(
    namespace: str,
    name: str,
    facets: Optional[dict[str, Any]] = None,
) -> dict[str, Any]:
    """
    Build an input dataset structure for RunEvents.

    Args:
        namespace: Dataset namespace (e.g., "s3", "postgresql")
        name: Dataset name (e.g., "bucket/path/data.parquet")
        facets: Optional facets to attach

    Returns:
        InputDataset dictionary
    """
    return {
        "namespace": namespace,
        "name": name,
        "facets": facets or {},
    }


def build_output_dataset(
    namespace: str,
    name: str,
    facets: Optional[dict[str, Any]] = None,
) -> dict[str, Any]:
    """
    Build an output dataset structure for RunEvents.

    Args:
        namespace: Dataset namespace
        name: Dataset name
        facets: Optional facets to attach

    Returns:
        OutputDataset dictionary
    """
    return {
        "namespace": namespace,
        "name": name,
        "facets": facets or {},
    }
