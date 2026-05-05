"""
Runtime-generated Feast `feature_store.yaml` used inside KFP component pods.

Keeps a single template for postgres + redis + OpenLineage transport settings.
"""

from __future__ import annotations

import os

DEFAULT_MARQUEZ_TRANSPORT_URL = "http://marquez"

CHURN_HISTORICAL_FEATURE_REFS = [
    "customer_features_view:tenure_months",
    "customer_features_view:monthly_charges",
    "customer_features_view:total_charges",
    "customer_features_view:num_support_tickets",
    "customer_features_view:contract_type",
    "customer_features_view:internet_service",
    "customer_features_view:payment_method",
]


def format_feast_feature_store_yaml(
    *,
    feast_project: str,
    pg_host: str,
    redis_host: str,
    marquez_transport_url: str = DEFAULT_MARQUEZ_TRANSPORT_URL,
) -> str:
    return f"""\
project: {feast_project}
provider: local
registry:
  registry_type: sql
  path: postgresql://feast:feast@{pg_host}:5432/warehouse
  cache_ttl_seconds: 60
offline_store:
  type: postgres
  host: {pg_host}
  port: 5432
  database: warehouse
  db_schema: public
  user: feast
  password: feast
online_store:
  type: redis
  connection_string: {redis_host}:6379
entity_key_serialization_version: 2
openlineage:
  enabled: true
  transport_type: http
  transport_url: {marquez_transport_url}
  emit_on_apply: true
  emit_on_materialize: true
"""


def write_feast_feature_store_yaml(
    feast_repo_path: str,
    *,
    feast_project: str,
    pg_host: str,
    redis_host: str,
    marquez_transport_url: str = DEFAULT_MARQUEZ_TRANSPORT_URL,
) -> str:
    """Write ``feature_store.yaml`` under ``feast_repo_path``; returns absolute path."""
    path = os.path.join(feast_repo_path, "feature_store.yaml")
    with open(path, "w") as f:
        f.write(
            format_feast_feature_store_yaml(
                feast_project=feast_project,
                pg_host=pg_host,
                redis_host=redis_host,
                marquez_transport_url=marquez_transport_url,
            )
        )
    return path
