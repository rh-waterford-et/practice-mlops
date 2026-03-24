#!/usr/bin/env python3
"""
Iris Classification Training with Automatic OpenLineage

This example demonstrates ZERO-CODE OpenLineage integration.
The user writes NORMAL MLflow code - no OpenLineage imports needed!

How it works:
    1. Set MLFLOW_TRACKING_URI with "openlineage+" prefix
    2. Write normal MLflow code
    3. OpenLineage events are emitted automatically by our plugin

Configuration:
    # Required environment variables
    export MLFLOW_TRACKING_URI="openlineage+http://mlflow-server:5000"
    export OPENLINEAGE_URL="http://marquez:5000"
    export OPENLINEAGE_NAMESPACE="iris-demo"

    # Then just run your training script!
    python examples/iris_training.py

Prerequisites:
    pip install openlineage-oai[mlflow] scikit-learn pandas
"""

import os

import mlflow
import pandas as pd
from sklearn.datasets import load_iris
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import accuracy_score, f1_score
from sklearn.model_selection import train_test_split

# =============================================================================
# Configuration via environment variables
# =============================================================================
# The "openlineage+" prefix triggers our tracking store plugin
# Format: openlineage+<backend>://<connection>
#
# Examples:
#   openlineage+http://mlflow-server:5000      (REST backend)
#   openlineage+postgresql://user:pass@host/db (PostgreSQL backend)
#   openlineage+file:///tmp/mlruns             (Local file backend)

MLFLOW_URI = os.getenv(
    "MLFLOW_TRACKING_URI",
    # The openlineage+ prefix activates our tracking store plugin
    "openlineage+http://mlflow-server-lineage.apps.rosa.catoconn-ray-et.bo0z.p3.openshiftapps.com",
)

# =============================================================================
# OpenLineage Configuration - Set these BEFORE running the script
# =============================================================================
# The tracking store plugin reads these environment variables:
#
#   OPENLINEAGE_URL       - Marquez API endpoint
#   OPENLINEAGE_NAMESPACE - Namespace for ALL jobs and datasets
#
# Set them here for this demo (in production, set via shell/k8s):

os.environ["OPENLINEAGE_URL"] = (
    "http://marquez-lineage.apps.rosa.catoconn-ray-et.bo0z.p3.openshiftapps.com"
)
os.environ["OPENLINEAGE_NAMESPACE"] = "plugin-test-5"  # ← All lineage goes here


def main():
    print("=" * 60)
    print("Iris Classification - Standard MLflow Training")
    print("=" * 60)
    print(f"\nMLflow URI: {MLFLOW_URI}")
    print()

    # Check if plugin mode is enabled
    if MLFLOW_URI.startswith("openlineage+"):
        print("✓ OpenLineage plugin mode ENABLED")
        print("  Events will be emitted automatically!")
    else:
        print("ℹ️  Standard MLflow mode (no automatic lineage)")
        print("  To enable plugin: set MLFLOW_TRACKING_URI=openlineage+<backend>://...")
    print()

    # =========================================================================
    # Standard MLflow Training Code - NO OpenLineage imports!
    # =========================================================================

    # Load data
    print("Loading Iris dataset...")
    iris = load_iris()
    df = pd.DataFrame(iris.data, columns=iris.feature_names)
    df["target"] = iris.target

    X = df.drop("target", axis=1)
    y = df["target"]
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)
    print(f"  Training: {len(X_train)} samples")
    print(f"  Test: {len(X_test)} samples")

    # Configure MLflow
    mlflow.set_tracking_uri(MLFLOW_URI)

    # Use a local artifact location to avoid mlflow-artifacts:// URI validation
    # In production, configure artifact storage at the server/experiment level
    import tempfile

    artifact_dir = tempfile.mkdtemp(prefix="mlflow_artifacts_")

    experiment = mlflow.get_experiment_by_name("iris-classification-local")
    if experiment is None:
        mlflow.create_experiment("iris-classification-local", artifact_location=artifact_dir)
    mlflow.set_experiment("iris-classification-local")

    # Train with MLflow tracking
    print("\nTraining RandomForest classifier...")

    with mlflow.start_run(run_name="iris-rf-demo"):
        # =====================================================================
        # Log input dataset - this creates lineage in Marquez!
        # =====================================================================

        # Create an MLflow dataset object from our training data
        train_dataset = mlflow.data.from_pandas(
            df=X_train.assign(target=y_train.values),
            source="sklearn.datasets.load_iris",
            name="iris_training_data",
        )

        # Log it as an input - triggers our store.log_inputs()
        mlflow.log_input(train_dataset, context="training")
        print("  Input dataset logged: iris_training_data")

        # Hyperparameters
        n_estimators = 100
        max_depth = 5

        # Log parameters (automatically captured by plugin)
        mlflow.log_param("n_estimators", n_estimators)
        mlflow.log_param("max_depth", max_depth)
        mlflow.log_param("model_type", "RandomForestClassifier")

        # Train
        model = RandomForestClassifier(
            n_estimators=n_estimators,
            max_depth=max_depth,
            random_state=42,
        )
        model.fit(X_train, y_train)

        # Evaluate
        y_pred = model.predict(X_test)
        accuracy = accuracy_score(y_test, y_pred)
        f1 = f1_score(y_test, y_pred, average="weighted")

        # Log metrics (automatically captured by plugin)
        mlflow.log_metric("accuracy", accuracy)
        mlflow.log_metric("f1_score", f1)

        # Log model (optional)
        try:
            mlflow.sklearn.log_model(model, "model")
            print("  Model logged to MLflow")
        except Exception as e:
            print(f"  Model logging skipped: {e}")

        print(f"\n  Accuracy: {accuracy:.2%}")
        print(f"  F1 Score: {f1:.4f}")

        # Get run info for display
        run = mlflow.active_run()
        run_id = run.info.run_id
        exp_id = run.info.experiment_id

    # =========================================================================
    # Done! If plugin was enabled, lineage is already in Marquez
    # =========================================================================

    print("\n" + "=" * 60)
    print("Training Complete!")
    print("=" * 60)

    print("\n🔗 MLflow Run:")
    base_url = MLFLOW_URI.replace("openlineage+", "")
    print(f"   {base_url}/#/experiments/{exp_id}/runs/{run_id}")

    if MLFLOW_URI.startswith("openlineage+"):
        marquez_url = os.environ["OPENLINEAGE_URL"]
        namespace = os.environ["OPENLINEAGE_NAMESPACE"]
        print(f"\n🔗 Marquez Lineage (namespace: {namespace}):")
        print(f"   Job: mlflow/experiment-{exp_id}/iris-rf-demo")
        web_url = marquez_url.replace("marquez-lineage", "marquez-web-lineage")
        print(f"   View: {web_url}/lineage/{namespace}")


if __name__ == "__main__":
    main()
