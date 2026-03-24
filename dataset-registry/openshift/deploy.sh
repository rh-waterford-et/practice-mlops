#!/usr/bin/env bash
# ===================================================================
# Dataset Registry - OpenShift deployment script
#
# Usage:
#   ./openshift/deploy.sh             # Full deploy (infra + build)
#   ./openshift/deploy.sh infra       # Deploy DB, services, routes
#   ./openshift/deploy.sh build       # Build and push images
#   ./openshift/deploy.sh teardown    # Delete all registry resources
# ===================================================================
set -euo pipefail

NAMESPACE="lineage"
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
STAGE="${1:-all}"

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
CYAN='\033[0;36m'
NC='\033[0m'

banner()  { echo -e "\n${CYAN}===================================${NC}"; echo -e "${CYAN}  $1${NC}"; echo -e "${CYAN}===================================${NC}"; }
info()    { echo -e "${GREEN}[INFO]${NC}  $1"; }
warn()    { echo -e "${YELLOW}[WARN]${NC}  $1"; }
err()     { echo -e "${RED}[ERROR]${NC} $1"; }

wait_for_pod() {
    local label=$1 timeout=${2:-120}
    info "Waiting for pod with label app=$label to be Ready (timeout ${timeout}s) ..."
    if ! oc wait pod -l app="$label" -n "$NAMESPACE" \
        --for=condition=Ready --timeout="${timeout}s" 2>/dev/null; then
        warn "Pod $label not ready within ${timeout}s - check: oc get pods -n $NAMESPACE"
        return 1
    fi
    info "Pod $label is Ready"
}

deploy_infra() {
    banner "Deploying Dataset Registry infrastructure"

    info "Applying database deployment ..."
    oc apply -f "$SCRIPT_DIR/base/db-deployment.yaml"
    wait_for_pod registry-db

    info "Applying API deployment manifests ..."
    oc apply -f "$SCRIPT_DIR/base/api-deployment.yaml"

    info "Applying UI deployment manifests ..."
    oc apply -f "$SCRIPT_DIR/base/ui-deployment.yaml"

    info "Applying routes ..."
    oc apply -f "$SCRIPT_DIR/base/route.yaml"

    info "Infrastructure deployed"
}

build_images() {
    banner "Building Dataset Registry images"

    info "Building API image ..."
    oc start-build dataset-registry-api \
        --from-dir="$PROJECT_ROOT/backend" \
        --follow -n "$NAMESPACE"

    info "Building UI image ..."
    cd "$PROJECT_ROOT/frontend"
    npm ci --prefer-offline 2>/dev/null || npm install
    npm run build
    cd "$PROJECT_ROOT"
    oc start-build dataset-registry-ui \
        --from-dir="$PROJECT_ROOT/frontend" \
        --follow -n "$NAMESPACE"

    info "Restarting deployments ..."
    oc rollout restart deployment/dataset-registry-api -n "$NAMESPACE"
    oc rollout restart deployment/dataset-registry-ui -n "$NAMESPACE"

    wait_for_pod dataset-registry-api
    wait_for_pod dataset-registry-ui

    info "Images built and deployed"
}

teardown() {
    banner "Tearing down Dataset Registry"
    oc delete -f "$SCRIPT_DIR/base/route.yaml" --ignore-not-found
    oc delete -f "$SCRIPT_DIR/base/ui-deployment.yaml" --ignore-not-found
    oc delete -f "$SCRIPT_DIR/base/api-deployment.yaml" --ignore-not-found
    oc delete -f "$SCRIPT_DIR/base/db-deployment.yaml" --ignore-not-found
    info "Teardown complete"
}

case "$STAGE" in
    infra)    deploy_infra ;;
    build)    build_images ;;
    teardown) teardown ;;
    all)      deploy_infra && build_images ;;
    *)        err "Unknown stage: $STAGE"; exit 1 ;;
esac

API_ROUTE=$(oc get route dataset-registry-api -n "$NAMESPACE" -o jsonpath='{.spec.host}' 2>/dev/null || true)
UI_ROUTE=$(oc get route dataset-registry -n "$NAMESPACE" -o jsonpath='{.spec.host}' 2>/dev/null || true)

if [[ -n "$UI_ROUTE" ]]; then
    banner "Dataset Registry is ready"
    info "UI:  https://$UI_ROUTE"
    info "API: https://$API_ROUTE"
    info "API Docs: https://$API_ROUTE/docs"
fi
