#!/usr/bin/env bash
# ═══════════════════════════════════════════════════════════════════════
# OpenShift deployment script for the Feast-KFP-MLflow ML system
#
# Usage:
#   ./openshift/deploy.sh               # Full deploy (infra + jobs)
#   ./openshift/deploy.sh infra         # Deploy infrastructure only
#   ./openshift/deploy.sh build         # Build images only
#   ./openshift/deploy.sh jobs          # Run pipeline jobs only
#   ./openshift/deploy.sh teardown      # Delete the namespace
# ═══════════════════════════════════════════════════════════════════════
set -euo pipefail

NAMESPACE="fkm-test"
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
STAGE="${1:-all}"

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
CYAN='\033[0;36m'
NC='\033[0m'

banner()  { echo -e "\n${CYAN}═══════════════════════════════════════${NC}"; echo -e "${CYAN}  $1${NC}"; echo -e "${CYAN}═══════════════════════════════════════${NC}"; }
info()    { echo -e "${GREEN}[INFO]${NC}  $1"; }
warn()    { echo -e "${YELLOW}[WARN]${NC}  $1"; }
err()     { echo -e "${RED}[ERROR]${NC} $1"; }

wait_for_pod() {
    local label=$1 timeout=${2:-120}
    info "Waiting for pod with label app=$label to be Ready (timeout ${timeout}s) ..."
    if ! oc wait pod -l app="$label" -n "$NAMESPACE" \
        --for=condition=Ready --timeout="${timeout}s" 2>/dev/null; then
        warn "Pod $label not ready within ${timeout}s – check: oc get pods -n $NAMESPACE"
        return 1
    fi
    info "Pod $label is Ready"
}

wait_for_job() {
    local job_name=$1 timeout=${2:-300}
    info "Waiting for job/$job_name to complete (timeout ${timeout}s) ..."
    if ! oc wait job/"$job_name" -n "$NAMESPACE" \
        --for=condition=Complete --timeout="${timeout}s" 2>/dev/null; then
        err "Job $job_name did not complete. Checking logs ..."
        oc logs job/"$job_name" -n "$NAMESPACE" --tail=30 || true
        return 1
    fi
    info "Job $job_name completed successfully"
}

delete_job_if_exists() {
    local job_name=$1
    if oc get job "$job_name" -n "$NAMESPACE" &>/dev/null; then
        info "Deleting previous job/$job_name ..."
        oc delete job "$job_name" -n "$NAMESPACE" --ignore-not-found
    fi
}

# ═══════════════════════════════════════════════════════════════════════
# TEARDOWN
# ═══════════════════════════════════════════════════════════════════════
if [[ "$STAGE" == "teardown" ]]; then
    banner "TEARDOWN – deleting namespace $NAMESPACE"
    oc delete namespace "$NAMESPACE" --ignore-not-found
    info "Namespace $NAMESPACE deleted"
    exit 0
fi

# ═══════════════════════════════════════════════════════════════════════
# 1. CREATE NAMESPACE + BASE RESOURCES
# ═══════════════════════════════════════════════════════════════════════
deploy_infra() {
    banner "1/5 — Namespace & Config"

    # Create namespace (idempotent)
    oc apply -f "$SCRIPT_DIR/base/namespace.yaml"
    oc project "$NAMESPACE"

    # Secrets, ConfigMaps, Feast config
    oc apply -f "$SCRIPT_DIR/base/secret.yaml"
    oc apply -f "$SCRIPT_DIR/base/configmap.yaml"
    oc apply -f "$SCRIPT_DIR/base/feast-config.yaml"

    # PVCs
    oc apply -f "$SCRIPT_DIR/base/pvc.yaml"

    banner "2/5 — Build Images"

    # ImageStreams + BuildConfigs
    oc apply -f "$SCRIPT_DIR/base/buildconfig.yaml"

    # Build the unified app image (fail fast if build breaks)
    info "Building fkm-app image (this may take a few minutes) ..."
    if ! oc start-build fkm-app --from-dir="$PROJECT_ROOT" -n "$NAMESPACE" --follow; then
        err "fkm-app build FAILED – aborting"; exit 1
    fi

    # Build the MLflow server image
    info "Building mlflow-server image ..."
    if ! oc start-build mlflow-server --from-dir="$PROJECT_ROOT" -n "$NAMESPACE" --follow; then
        err "mlflow-server build FAILED – aborting"; exit 1
    fi

    # Build Marquez images (mirrors Docker Hub into the internal registry so
    # pods never hit Docker Hub rate limits)
    info "Building marquez-api image ..."
    if ! oc start-build marquez-api -n "$NAMESPACE" --follow; then
        err "marquez-api build FAILED – aborting"; exit 1
    fi
    info "Building marquez-web image ..."
    if ! oc start-build marquez-web -n "$NAMESPACE" --follow; then
        err "marquez-web build FAILED – aborting"; exit 1
    fi

    banner "3/5 — Deploy Infrastructure Services"

    # Deploy services in dependency order
    oc apply -f "$SCRIPT_DIR/base/minio.yaml"
    oc apply -f "$SCRIPT_DIR/base/postgres.yaml"
    oc apply -f "$SCRIPT_DIR/base/redis.yaml"

    info "Waiting for infrastructure pods ..."
    wait_for_pod "minio" 120
    wait_for_pod "postgres" 120
    wait_for_pod "redis" 60

    # MLflow depends on postgres + minio
    oc apply -f "$SCRIPT_DIR/base/mlflow.yaml"
    wait_for_pod "mlflow" 180

    # Marquez (OpenLineage backend) – grant anyuid SCC so the web UI
    # nginx container can run, then deploy postgres + api + web
    banner "3a/5 — Marquez (OpenLineage)"
    oc apply -f "$SCRIPT_DIR/base/marquez.yaml"
    oc adm policy add-scc-to-user anyuid \
        -z marquez -n "$NAMESPACE" 2>/dev/null || true
    wait_for_pod "marquez-postgres" 120
    wait_for_pod "marquez" 180
    wait_for_pod "marquez-web" 120

    # Routes (includes the new Marquez route)
    oc apply -f "$SCRIPT_DIR/base/routes.yaml"

    info "Infrastructure deployed"
}

# ═══════════════════════════════════════════════════════════════════════
# 2. BUILD IMAGES ONLY
# ═══════════════════════════════════════════════════════════════════════
build_images() {
    banner "Build Images"
    oc project "$NAMESPACE"

    oc apply -f "$SCRIPT_DIR/base/buildconfig.yaml"

    info "Building fkm-app image ..."
    oc start-build fkm-app --from-dir="$PROJECT_ROOT" -n "$NAMESPACE" --follow

    info "Building mlflow-server image ..."
    oc start-build mlflow-server --from-dir="$PROJECT_ROOT" -n "$NAMESPACE" --follow

    info "Building marquez-api image ..."
    oc start-build marquez-api -n "$NAMESPACE" --follow

    info "Building marquez-web image ..."
    oc start-build marquez-web -n "$NAMESPACE" --follow

    info "Images built"
}

# ═══════════════════════════════════════════════════════════════════════
# 3. RUN PIPELINE JOBS (sequential)
# ═══════════════════════════════════════════════════════════════════════
run_jobs() {
    banner "4/5 — Pipeline Jobs"
    oc project "$NAMESPACE"

    # Job 1: Seed MinIO with sample data
    delete_job_if_exists "minio-seed"
    oc apply -f "$SCRIPT_DIR/jobs/01-minio-seed.yaml"
    wait_for_job "minio-seed" 120

    # Job 2: ETL – MinIO → PostgreSQL
    delete_job_if_exists "etl-job"
    oc apply -f "$SCRIPT_DIR/jobs/02-etl.yaml"
    wait_for_job "etl-job" 180

    # Job 3: Feast apply
    delete_job_if_exists "feast-apply"
    oc apply -f "$SCRIPT_DIR/jobs/03-feast-apply.yaml"
    wait_for_job "feast-apply" 120

    # Job 4: Feast materialize (offline → online)
    delete_job_if_exists "feast-materialize"
    oc apply -f "$SCRIPT_DIR/jobs/04-feast-materialize.yaml"
    wait_for_job "feast-materialize" 180

    # Job 5: ML pipeline (train + evaluate + register)
    delete_job_if_exists "ml-pipeline"
    oc apply -f "$SCRIPT_DIR/jobs/05-ml-pipeline.yaml"
    wait_for_job "ml-pipeline" 900

    # Job 6: Promote model to Production
    delete_job_if_exists "promote-model"
    oc apply -f "$SCRIPT_DIR/jobs/06-promote-model.yaml"
    wait_for_job "promote-model" 60

    banner "5/5 — Deploy Inference API"

    oc apply -f "$SCRIPT_DIR/base/inference-api.yaml"
    wait_for_pod "inference-api" 120

    info "Inference API deployed"
}

# ═══════════════════════════════════════════════════════════════════════
# MAIN
# ═══════════════════════════════════════════════════════════════════════
case "$STAGE" in
    infra)    deploy_infra ;;
    build)    build_images ;;
    jobs)     run_jobs ;;
    all)
        deploy_infra
        run_jobs
        ;;
    *)
        err "Unknown stage: $STAGE"
        echo "Usage: $0 {all|infra|build|jobs|teardown}"
        exit 1
        ;;
esac

# ═══════════════════════════════════════════════════════════════════════
# Print summary
# ═══════════════════════════════════════════════════════════════════════
banner "DEPLOYMENT COMPLETE"

echo ""
info "Pods:"
oc get pods -n "$NAMESPACE"

echo ""
info "Routes:"
oc get routes -n "$NAMESPACE" -o custom-columns='NAME:.metadata.name,HOST:.spec.host'

echo ""
INFERENCE_HOST=$(oc get route inference-api -n "$NAMESPACE" -o jsonpath='{.spec.host}' 2>/dev/null || echo "<pending>")
MLFLOW_HOST=$(oc get route mlflow -n "$NAMESPACE" -o jsonpath='{.spec.host}' 2>/dev/null || echo "<pending>")
MINIO_HOST=$(oc get route minio-console -n "$NAMESPACE" -o jsonpath='{.spec.host}' 2>/dev/null || echo "<pending>")
MARQUEZ_HOST=$(oc get route marquez -n "$NAMESPACE" -o jsonpath='{.spec.host}' 2>/dev/null || echo "<pending>")

echo -e "${GREEN}Service URLs:${NC}"
echo "  Inference API  →  https://$INFERENCE_HOST/docs"
echo "  MLflow UI      →  https://$MLFLOW_HOST"
echo "  MinIO Console  →  https://$MINIO_HOST  (minioadmin/minioadmin)"
echo "  Marquez UI     →  https://$MARQUEZ_HOST"
echo ""
echo -e "${GREEN}Test the API:${NC}"
echo "  curl -X POST https://$INFERENCE_HOST/predict \\"
echo "    -H 'Content-Type: application/json' \\"
echo "    -d '{\"entity_ids\": [1, 2, 3]}'"
