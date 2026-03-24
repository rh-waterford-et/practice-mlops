"""Dataset Registry SDK - CRUD client for the registry API."""

from typing import Optional
from uuid import UUID

import requests

from .models import Dataset, SchemaField


class RegistryError(Exception):
    pass


class DatasetNotFoundError(RegistryError):
    pass


class DatasetConflictError(RegistryError):
    pass


class RegistryClient:
    """Client for the Dataset Registry API.

    Example:
        client = RegistryClient(url="http://dataset-registry-api:8080")
        ds = client.create_dataset(
            name="Customer Features",
            source="postgres://postgres:5432/warehouse.public.customer_features",
        )
    """

    def __init__(self, url: str, timeout: int = 10):
        self.url = url.rstrip("/")
        self.timeout = timeout

    def _api(self, path: str) -> str:
        return f"{self.url}/api/v1{path}"

    def create_dataset(
        self,
        name: str,
        source: str,
        description: Optional[str] = None,
        schema_fields: Optional[list[dict]] = None,
        tags: Optional[list[str]] = None,
    ) -> Dataset:
        payload: dict = {"name": name, "source": source}
        if description:
            payload["description"] = description
        if schema_fields:
            payload["schema_fields"] = schema_fields
        if tags:
            payload["tags"] = tags

        resp = requests.post(self._api("/datasets"), json=payload, timeout=self.timeout)
        if resp.status_code == 409:
            raise DatasetConflictError(f"Dataset with source '{source}' already exists")
        if resp.status_code != 201:
            raise RegistryError(f"Create failed: HTTP {resp.status_code} - {resp.text}")
        return Dataset.from_dict(resp.json())

    def get_dataset(self, source: str) -> Dataset:
        resp = requests.get(
            self._api("/datasets/lookup"),
            params={"source": source},
            timeout=self.timeout,
        )
        if resp.status_code == 404:
            raise DatasetNotFoundError(f"Dataset with source '{source}' not found")
        if resp.status_code != 200:
            raise RegistryError(f"Lookup failed: HTTP {resp.status_code}")
        return Dataset.from_dict(resp.json())

    def get_dataset_by_id(self, dataset_id: str | UUID) -> Dataset:
        resp = requests.get(self._api(f"/datasets/{dataset_id}"), timeout=self.timeout)
        if resp.status_code == 404:
            raise DatasetNotFoundError(f"Dataset {dataset_id} not found")
        if resp.status_code != 200:
            raise RegistryError(f"Get failed: HTTP {resp.status_code}")
        return Dataset.from_dict(resp.json())

    def list_datasets(self, tag: Optional[str] = None) -> list[Dataset]:
        params: dict = {}
        if tag:
            params["tag"] = tag
        resp = requests.get(self._api("/datasets"), params=params, timeout=self.timeout)
        if resp.status_code != 200:
            raise RegistryError(f"List failed: HTTP {resp.status_code}")
        return [Dataset.from_dict(d) for d in resp.json()["datasets"]]

    def update_dataset(
        self,
        source: str,
        name: Optional[str] = None,
        description: Optional[str] = None,
        schema_fields: Optional[list[dict]] = None,
        tags: Optional[list[str]] = None,
    ) -> Dataset:
        ds = self.get_dataset(source)
        payload: dict = {}
        if name is not None:
            payload["name"] = name
        if description is not None:
            payload["description"] = description
        if schema_fields is not None:
            payload["schema_fields"] = schema_fields
        if tags is not None:
            payload["tags"] = tags
        resp = requests.put(
            self._api(f"/datasets/{ds.id}"), json=payload, timeout=self.timeout,
        )
        if resp.status_code == 404:
            raise DatasetNotFoundError(f"Dataset with source '{source}' not found")
        if resp.status_code != 200:
            raise RegistryError(f"Update failed: HTTP {resp.status_code}")
        return Dataset.from_dict(resp.json())

    def delete_dataset(self, source: str) -> None:
        ds = self.get_dataset(source)
        resp = requests.delete(self._api(f"/datasets/{ds.id}"), timeout=self.timeout)
        if resp.status_code == 404:
            raise DatasetNotFoundError(f"Dataset with source '{source}' not found")
        if resp.status_code != 204:
            raise RegistryError(f"Delete failed: HTTP {resp.status_code}")
