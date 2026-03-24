# Code Reference Documentation

This directory contains detailed documentation for the `openlineage-oai` codebase, organized by module.

## Overview

| Document | Description |
|----------|-------------|
| [sequence-diagram.md](./sequence-diagram.md) | **Start here!** Visual flow through all components |

## MLflow Adapter

| File | Description |
|------|-------------|
| [base.md](./base.md) | Abstract adapter interface (`ToolAdapter`) |
| [mlflow-init.md](./mlflow-init.md) | MLflow adapter entry point (`MLflowAdapter`) |
| [mlflow-tracking-store.md](./mlflow-tracking-store.md) | Core tracking store wrapper |
| [mlflow-facets.md](./mlflow-facets.md) | MLflow-specific OpenLineage facets |
| [mlflow-utils.md](./mlflow-utils.md) | Utility functions for data extraction |

## How to Use

Each document follows a consistent structure:

1. **File Purpose** - What the module does and why it exists
2. **Classes** - Class definitions with attributes
3. **Functions** - Function signatures, descriptions, args, returns, and usage examples

## Quick Links

- [Implementation Guide](../IMPLEMENTATION.md) - Architecture and design decisions
- [MLflow Storage Explained](../MLFLOW_STORAGE.md) - Understanding MLflow's two storage systems
