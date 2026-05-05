#!/usr/bin/env bash
# ═══════════════════════════════════════════════════════════════════════
# OpenShift deployment — Feast, MLflow, Marquez, MinIO, OpenShift AI DSPA,
# bootstrap Jobs, inference API (single manifest + helper).
#
# Usage:
#   ./openshift/deploy.sh                         # Full deploy in OPENSHIFT_APP_NAMESPACE or lineage
#   OPENSHIFT_APP_NAMESPACE=fkm ./openshift/deploy.sh all
#   ./openshift/deploy.sh --namespace fkm all     # Same (preferred explicit flag)
#   ./openshift/deploy.sh infra                   # Infrastructure only (no Jobs)
#   ./openshift/deploy.sh build
#   ./openshift/deploy.sh jobs
#   ./openshift/deploy.sh --namespace fkm teardown
#
# Manifest: openshift/lineage-openshift-ai.yaml
# Splitter: openshift/lineage_manifest.py (infra vs Job documents)
# ═══════════════════════════════════════════════════════════════════════
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

MANIFEST="$SCRIPT_DIR/lineage-openshift-ai.yaml"
SPLITTER="$SCRIPT_DIR/lineage_manifest.py"
RENDER_SCRIPT="$SCRIPT_DIR/render_namespace.py"

NAMESPACE="${OPENSHIFT_APP_NAMESPACE:-lineage}"
POSITIONAL=()
while [[ $# -gt 0 ]]; do
    case "$1" in
        -n|--namespace)
            NAMESPACE="$2"
            shift 2
            ;;
        *)
            POSITIONAL+=("$1")
            shift
            ;;
    esac
done
export OPENSHIFT_APP_NAMESPACE="$NAMESPACE"
STAGE="${POSITIONAL[0]:-all}"

PYTHON3="${PYTHON3_EXEC:-$(command -v python3.11 2>/dev/null || command -v python3)}"

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
CYAN='\033[0;36m'
NC='\033[0m'

banner()  { echo -e "\n${CYAN}═══════════════════════════════════════${NC}"; echo -e "${CYAN}  $1${NC}"; echo -e "${CYAN}═══════════════════════════════════════${NC}"; }
info()    { echo -e "${GREEN}[INFO]${NC}  $1"; }
warn()    { echo -e "${YELLOW}[WARN]${NC}  $1"; }
err()     { echo -e "${RED}[ERROR]${NC} $1"; }

apply_infra() {
    info "Applying manifest for namespace $NAMESPACE (rendered from $MANIFEST)"
    "$PYTHON3" "$RENDER_SCRIPT" "$MANIFEST" "$NAMESPACE" | "$PYTHON3" "$SPLITTER" -f - emit-infra | oc apply -f -
}

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

wait_for_pod_selector() {
    local selector=$1 timeout=${2:-120}
    info "Waiting for pod with selector $selector to be Ready (timeout ${timeout}s) ..."
    if ! oc wait pod -l "$selector" -n "$NAMESPACE" \
        --for=condition=Ready --timeout="${timeout}s" 2>/dev/null; then
        warn "Pod ($selector) not ready within ${timeout}s – check: oc get pods -n $NAMESPACE"
        return 1
    fi
    info "Pod ($selector) is Ready"
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
# 1. INFRASTRUCTURE (namespace … routes, DSPA CR, inference — Jobs skipped)
# ═══════════════════════════════════════════════════════════════════════
deploy_infra() {
    banner "1/6 — Namespace & stack (from lineage-openshift-ai.yaml)"

    oc project "$NAMESPACE" 2>/dev/null || true
    apply_infra

    banner "2/6 — Build Images"

    info "Building fkm-app image (this may take a few minutes) ..."
    if ! oc start-build fkm-app --from-dir="$PROJECT_ROOT" -n "$NAMESPACE" --follow; then
        err "fkm-app build FAILED – aborting"; exit 1
    fi

    info "Building spark-etl image ..."
    if ! oc start-build spark-etl --from-dir="$PROJECT_ROOT" -n "$NAMESPACE" --follow; then
        err "spark-etl build FAILED – aborting"; exit 1
    fi

    banner "3/6 — Wait for storage and database pods"

    info "Waiting for storage and database pods ..."
    wait_for_pod "mlflow-minio" 120
    wait_for_pod "postgres" 120
    wait_for_pod "redis" 60
    wait_for_pod "mlflow-db" 120
    wait_for_pod "marquez-db" 120

    banner "4/6 — Deploy Marquez"

    wait_for_pod_selector "app.kubernetes.io/component=marquez,app.kubernetes.io/name=marquez" 180
    wait_for_pod_selector "app.kubernetes.io/component=web,app.kubernetes.io/name=marquez" 120

    banner "5/6 — Deploy MLflow"

    wait_for_pod "mlflow-server" 240

    banner "6/6 — Routes & auxiliary workloads"

    info "Infrastructure applied (includes Routes, DSPA, inference Deployment if present)"
}

# ═══════════════════════════════════════════════════════════════════════
# 2. BUILD IMAGES ONLY
# ═══════════════════════════════════════════════════════════════════════
build_images() {
    banner "Build Images"
    oc project "$NAMESPACE"

    apply_infra

    info "Building fkm-app image ..."
    oc start-build fkm-app --from-dir="$PROJECT_ROOT" -n "$NAMESPACE" --follow

    info "Building spark-etl image ..."
    oc start-build spark-etl --from-dir="$PROJECT_ROOT" -n "$NAMESPACE" --follow

    info "Images built"
}

# ═══════════════════════════════════════════════════════════════════════
# 3. BOOTSTRAP JOBS (sequential; must run after infra is healthy)
# ═══════════════════════════════════════════════════════════════════════
run_jobs() {
    banner "Pipeline Jobs"
    oc project "$NAMESPACE"

    JOB_ORDER=(minio-seed etl-job feast-apply feast-materialize ml-pipeline promote-model)
    JOB_TIMEOUTS=(120 180 120 180 900 60)

    jdir=$(mktemp -d)
    trap 'rm -rf "$jdir"' RETURN
    "$PYTHON3" "$RENDER_SCRIPT" "$MANIFEST" "$NAMESPACE" | "$PYTHON3" "$SPLITTER" -f - materialize-jobs "$jdir"

    for i in "${!JOB_ORDER[@]}"; do
        jn="${JOB_ORDER[$i]}"
        to="${JOB_TIMEOUTS[$i]}"
        jf="$jdir/job-${jn}.yaml"
        if [[ ! -f "$jf" ]]; then
            err "Missing job file $jf (manifest out of date?)"
            exit 1
        fi
        delete_job_if_exists "$jn"
        oc apply -f "$jf"
        wait_for_job "$jn" "$to"
    done

    banner "Inference API"

    wait_for_pod "inference-api" 120

    info "Bootstrap Jobs and inference check complete"
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
        echo "Usage: $0 [--namespace NS] {all|infra|build|jobs|teardown}"
        exit 1
        ;;
esac

banner "DEPLOYMENT COMPLETE"

echo ""
info "Pods:"
oc get pods -n "$NAMESPACE"

echo ""
info "Routes:"
oc get routes -n "$NAMESPACE" -o custom-columns='NAME:.metadata.name,HOST:.spec.host' 2>/dev/null || true

echo ""
INFERENCE_HOST=$(oc get route inference-api -n "$NAMESPACE" -o jsonpath='{.spec.host}' 2>/dev/null || echo "<pending>")
MLFLOW_HOST=$(oc get route mlflow-server -n "$NAMESPACE" -o jsonpath='{.spec.host}' 2>/dev/null || echo "<pending>")
MINIO_HOST=$(oc get route mlflow-minio-console -n "$NAMESPACE" -o jsonpath='{.spec.host}' 2>/dev/null || echo "<pending>")
MARQUEZ_HOST=$(oc get route marquez-web -n "$NAMESPACE" -o jsonpath='{.spec.host}' 2>/dev/null || echo "<pending>")

echo -e "${GREEN}Service URLs:${NC}"
echo "  Inference API  →  https://$INFERENCE_HOST/docs"
echo "  MLflow UI      →  https://$MLFLOW_HOST"
echo "  MinIO Console  →  https://$MINIO_HOST  (minioadmin/minioadmin123)"
echo "  Marquez UI     →  https://$MARQUEZ_HOST"
echo ""
echo -e "${GREEN}OpenShift AI pipelines:${NC}"
echo "  OPENSHIFT_APP_NAMESPACE=$NAMESPACE ./openshift/deploy-dsp.sh all"
echo ""
echo -e "${GREEN}Test the API:${NC}"
echo "  curl -X POST https://$INFERENCE_HOST/predict \\"
echo "    -H 'Content-Type: application/json' \\"
echo "    -d '{\"entity_ids\": [1, 2, 3]}'"
