# Feast Feature Store Guide

## What is Feast?

Feast (Feature Store) is an open-source feature store that helps teams manage and serve ML features consistently across training and inference.

## Core Concepts

### Entity
An entity is a collection of semantically related features. Think of it as the primary key in your feature data.

Example entities:
- Customer ID
- Product ID
- Transaction ID
- User ID

### Feature View
A feature view is a group of features that are computed and stored together. It defines:
- Feature schema
- Data source (where features come from)
- Entity relationships
- TTL (time-to-live)

### Data Source
The location where feature data is stored:
- **Offline Store**: Historical features for training (PostgreSQL, BigQuery, Snowflake)
- **Online Store**: Low-latency features for serving (Redis, DynamoDB, Cassandra)

### Feature Service
A logical grouping of features that are used together for a specific model or use case.

## Feast Architecture

```
┌─────────────────────────────────────────────────────────┐
│                    Feature Registry                      │
│  (Stores metadata about entities, features, sources)    │
└─────────────────────────────────────────────────────────┘
                            │
        ┌───────────────────┼───────────────────┐
        │                   │                   │
        ▼                   ▼                   ▼
┌──────────────┐    ┌──────────────┐    ┌──────────────┐
│   Offline    │    │    Online    │    │  Streaming   │
│    Store     │    │    Store     │    │   Pipeline   │
│ (PostgreSQL) │    │   (Redis)    │    │   (Kafka)    │
└──────────────┘    └──────────────┘    └──────────────┘
        │                   │
        ▼                   ▼
┌──────────────┐    ┌──────────────┐
│   Training   │    │  Inference   │
│   Pipeline   │    │   Service    │
└──────────────┘    └──────────────┘
```

## Workflow

### 1. Define Features

```python
from feast import Entity, FeatureView, Field, FileSource
from feast.types import Float32, Int64, String
from datetime import timedelta

# Define entity
customer = Entity(
    name="customer_id",
    description="Customer identifier",
)

# Define feature view
customer_features = FeatureView(
    name="customer_features_view",
    entities=[customer],
    ttl=timedelta(days=365),
    schema=[
        Field(name="tenure_months", dtype=Int64),
        Field(name="monthly_charges", dtype=Float32),
        Field(name="total_charges", dtype=Float32),
        Field(name="contract_type", dtype=String),
    ],
    source=FileSource(
        path="data/customer_features.parquet",
        timestamp_field="event_timestamp",
    ),
)
```

### 2. Apply Feature Definitions

```bash
feast apply
```

This registers your features in the feature registry.

### 3. Materialize Features

Move features from offline to online store:

```bash
feast materialize-incremental $(date +%Y-%m-%d)
```

Or programmatically:

```python
from feast import FeatureStore
from datetime import datetime, timedelta

store = FeatureStore(repo_path=".")
end_date = datetime.now()
start_date = end_date - timedelta(days=7)

store.materialize(start_date=start_date, end_date=end_date)
```

### 4. Retrieve Features for Training

```python
from feast import FeatureStore
import pandas as pd

store = FeatureStore(repo_path=".")

# Entity dataframe with timestamps
entity_df = pd.DataFrame({
    "customer_id": [1001, 1002, 1003],
    "event_timestamp": [
        datetime(2024, 1, 1),
        datetime(2024, 1, 2),
        datetime(2024, 1, 3),
    ]
})

# Get historical features
training_df = store.get_historical_features(
    entity_df=entity_df,
    features=[
        "customer_features_view:tenure_months",
        "customer_features_view:monthly_charges",
        "customer_features_view:total_charges",
    ],
).to_df()
```

### 5. Retrieve Features for Inference

```python
from feast import FeatureStore

store = FeatureStore(repo_path=".")

# Get online features (low latency)
feature_vector = store.get_online_features(
    entity_rows=[{"customer_id": 1001}],
    features=[
        "customer_features_view:tenure_months",
        "customer_features_view:monthly_charges",
        "customer_features_view:total_charges",
    ],
).to_dict()
```

## Feature Engineering Patterns

### Point-in-Time Correctness
Feast ensures you get the feature values as they existed at a specific point in time, preventing data leakage.

### Feature Sharing
Teams can discover and reuse features created by others, reducing duplicate work.

### Consistent Features
Features used in training match those used in production, eliminating training-serving skew.

## Integration with OpenLineage

Feast can emit OpenLineage events for:
- Feature registry updates (`feast apply`)
- Feature materialization
- Feature retrieval

Configuration:

```yaml
project: my_project
provider: local
registry: data/registry.db
online_store:
  type: redis
  connection_string: localhost:6379
offline_store:
  type: postgres
  host: localhost
  port: 5432
  database: feast
  user: feast
  password: feast
openlineage:
  enabled: true
  transport_type: http
  transport_url: http://marquez:5000
  namespace: production
  emit_on_apply: true
  emit_on_materialize: true
```

## Best Practices

### 1. Feature Naming
Use clear, descriptive names:
- `customer_total_purchases_30d` (good)
- `feature_1` (bad)

### 2. Feature Documentation
Add descriptions to your features:
```python
Field(
    name="tenure_months",
    dtype=Int64,
    description="Number of months customer has been with company"
)
```

### 3. Feature Versioning
- Version your feature definitions in Git
- Use semantic versioning for breaking changes
- Maintain backward compatibility when possible

### 4. Monitoring
Monitor:
- Feature freshness
- Feature distribution shifts
- Missing values
- Outliers

### 5. Testing
- Test feature transformations
- Validate feature schemas
- Check for data quality issues

### 6. Performance
- Index entity columns in offline store
- Use appropriate TTL values
- Batch feature retrieval when possible
- Cache frequently accessed features

## Common Use Cases

### Recommendation Systems
Features:
- User behavior (clicks, views, purchases)
- User demographics
- Item attributes
- Interaction history

### Fraud Detection
Features:
- Transaction amount patterns
- Geographic location
- Device fingerprints
- Historical fraud indicators

### Customer Churn Prediction
Features:
- Usage patterns
- Support ticket history
- Payment history
- Engagement metrics

### Demand Forecasting
Features:
- Historical sales
- Seasonal patterns
- Promotional events
- Inventory levels
