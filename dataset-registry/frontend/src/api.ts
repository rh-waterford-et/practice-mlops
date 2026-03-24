export interface SchemaField {
  name: string;
  type: string;
}

export interface Dataset {
  id: string;
  name: string;
  source: string;
  ol_namespace: string;
  ol_name: string;
  description: string | null;
  schema_fields: SchemaField[] | null;
  tags: string[] | null;
  created_at: string;
  updated_at: string;
}

export interface DatasetList {
  datasets: Dataset[];
  total: number;
}

export interface DatasetCreate {
  name: string;
  source: string;
  description?: string;
  schema_fields?: SchemaField[];
  tags?: string[];
}

export interface DatasetUpdate {
  name?: string | null;
  description?: string | null;
  schema_fields?: SchemaField[] | null;
  tags?: string[] | null;
}

const BASE = "/api/v1";

async function handleResponse<T>(resp: Response): Promise<T> {
  if (!resp.ok) {
    const body = await resp.text();
    throw new Error(`HTTP ${resp.status}: ${body}`);
  }
  return resp.json();
}

export async function listDatasets(tag?: string): Promise<DatasetList> {
  const params = new URLSearchParams();
  if (tag) params.set("tag", tag);
  const qs = params.toString();
  const resp = await fetch(`${BASE}/datasets${qs ? `?${qs}` : ""}`);
  return handleResponse<DatasetList>(resp);
}

export async function getDataset(id: string): Promise<Dataset> {
  const resp = await fetch(`${BASE}/datasets/${id}`);
  return handleResponse<Dataset>(resp);
}

export async function createDataset(body: DatasetCreate): Promise<Dataset> {
  const resp = await fetch(`${BASE}/datasets`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body),
  });
  return handleResponse<Dataset>(resp);
}

export async function updateDataset(
  id: string,
  body: DatasetUpdate
): Promise<Dataset> {
  const resp = await fetch(`${BASE}/datasets/${id}`, {
    method: "PUT",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body),
  });
  return handleResponse<Dataset>(resp);
}

export interface PipelineRef {
  namespace: string;
}

export interface PipelineList {
  pipelines: PipelineRef[];
}

export async function getDatasetPipelines(
  id: string
): Promise<PipelineList> {
  const resp = await fetch(`${BASE}/datasets/${id}/pipelines`);
  return handleResponse<PipelineList>(resp);
}

export async function deleteDataset(id: string): Promise<void> {
  const resp = await fetch(`${BASE}/datasets/${id}`, { method: "DELETE" });
  if (!resp.ok) {
    const body = await resp.text();
    throw new Error(`HTTP ${resp.status}: ${body}`);
  }
}
