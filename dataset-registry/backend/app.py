"""Dataset Registry API - single source of truth for dataset identity."""

import json
import os
import urllib.request
from typing import Optional
from urllib.parse import quote
from uuid import UUID

from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware

from db import (
    create_dataset,
    delete_dataset,
    get_dataset_by_id,
    get_dataset_by_source,
    init_db,
    list_datasets,
    update_dataset,
)
from introspect import introspect_schema
from lineage import emit_dataset_registered
from models import Dataset, DatasetCreate, DatasetList, DatasetUpdate, parse_ol_identity

MARQUEZ_URL = os.getenv("MARQUEZ_URL", "http://marquez:80")

app = FastAPI(
    title="Dataset Registry",
    description="Single source of truth for dataset identity following the OpenLineage naming spec.",
    version="0.1.0",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.on_event("startup")
def startup():
    init_db()


@app.post("/api/v1/datasets", response_model=Dataset, status_code=201)
def create(body: DatasetCreate):
    existing = get_dataset_by_source(body.source)
    if existing:
        raise HTTPException(409, f"Dataset with source '{body.source}' already exists")
    ol_ns, ol_name = parse_ol_identity(body.source)
    sf = [f.model_dump() for f in body.schema_fields] if body.schema_fields else None
    if not sf:
        sf = introspect_schema(body.source)
    row = create_dataset(body.name, body.source, ol_ns, ol_name, body.description, sf, body.tags)
    emit_dataset_registered(ol_ns, ol_name, sf, body.description)
    return Dataset(**row)


@app.get("/api/v1/datasets", response_model=DatasetList)
def list_all(tag: Optional[str] = Query(None)):
    rows = list_datasets(tag=tag)
    return DatasetList(datasets=[Dataset(**r) for r in rows], total=len(rows))


@app.get("/api/v1/datasets/lookup", response_model=Dataset)
def lookup(source: str = Query(...)):
    row = get_dataset_by_source(source)
    if not row:
        raise HTTPException(404, "Dataset not found")
    return Dataset(**row)


@app.get("/api/v1/datasets/{dataset_id}", response_model=Dataset)
def get_by_id(dataset_id: UUID):
    row = get_dataset_by_id(dataset_id)
    if not row:
        raise HTTPException(404, "Dataset not found")
    return Dataset(**row)


@app.put("/api/v1/datasets/{dataset_id}", response_model=Dataset)
def update(dataset_id: UUID, body: DatasetUpdate):
    sf = [f.model_dump() for f in body.schema_fields] if body.schema_fields else None
    row = update_dataset(dataset_id, body.name, body.description, sf, body.tags)
    if not row:
        raise HTTPException(404, "Dataset not found")
    return Dataset(**row)


@app.get("/api/v1/datasets/{dataset_id}/pipelines")
def get_pipelines(dataset_id: UUID):
    """Query Marquez lineage to find job namespaces (pipelines) that reference this dataset."""
    row = get_dataset_by_id(dataset_id)
    if not row:
        raise HTTPException(404, "Dataset not found")

    node_id = f"dataset:{row['ol_namespace']}:{row['ol_name']}"
    url = f"{MARQUEZ_URL}/api/v1/lineage?nodeId={quote(node_id, safe='')}&depth=10"

    try:
        req = urllib.request.Request(url)
        with urllib.request.urlopen(req, timeout=5) as resp:
            data = json.loads(resp.read().decode())
    except Exception as e:
        print(f"Marquez lineage query failed: {e}")
        return {"pipelines": []}

    job_namespaces: set[str] = set()
    for node in data.get("graph", []):
        if node.get("type") == "JOB":
            ns = node.get("data", {}).get("namespace", "")
            if ns:
                job_namespaces.add(ns)

    return {
        "pipelines": sorted(
            [{"namespace": ns} for ns in job_namespaces],
            key=lambda p: p["namespace"],
        )
    }


@app.delete("/api/v1/datasets/{dataset_id}", status_code=204)
def delete(dataset_id: UUID):
    if not delete_dataset(dataset_id):
        raise HTTPException(404, "Dataset not found")
