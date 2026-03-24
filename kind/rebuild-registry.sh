#!/usr/bin/env bash
# ============================================================
# rebuild-registry.sh  –  Rebuild and hot-reload the Dataset
#                          Registry images in the running cluster
#
# Usage:
#   ./kind/rebuild-registry.sh [api|ui|both]   (default: both)
# ============================================================
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(dirname "$SCRIPT_DIR")"
CLUSTER_NAME="feast-kfp-mlflow"
NAMESPACE="lineage"
COMPONENT="${1:-both}"

GREEN='\033[0;32m'; YELLOW='\033[1;33m'; RED='\033[0;31m'; NC='\033[0m'
info()  { echo -e "${GREEN}[INFO]${NC}  $*"; }
warn()  { echo -e "${YELLOW}[WARN]${NC}  $*"; }
die()   { echo -e "${RED}[ERROR]${NC} $*" >&2; exit 1; }

# ── Detect container runtime ──────────────────────────────────
if command -v podman &>/dev/null; then
  CONTAINER_RT="podman"
  export KIND_EXPERIMENTAL_PROVIDER=podman
elif command -v docker &>/dev/null; then
  CONTAINER_RT="docker"
else
  die "Neither podman nor docker found."
fi
PLATFORM="linux/$(uname -m | sed 's/x86_64/amd64/;s/aarch64/arm64/')"

API_IMG="dataset-registry-api:local"
UI_IMG="dataset-registry-ui:local"
API_DIR="$ROOT_DIR/dataset-registry/backend"
UI_DIR="$ROOT_DIR/dataset-registry/frontend"

build_api() {
  [[ -d "$API_DIR" ]] || die "Backend not found at $API_DIR"
  info "Building ${API_IMG} (${PLATFORM})..."
  ${CONTAINER_RT} build --platform "${PLATFORM}" -t "${API_IMG}" "${API_DIR}"
  kind load docker-image "${API_IMG}" --name "${CLUSTER_NAME}"
  kubectl rollout restart deployment/dataset-registry-api -n "${NAMESPACE}"
  kubectl rollout status  deployment/dataset-registry-api -n "${NAMESPACE}" --timeout=120s
  info "dataset-registry-api redeployed"
}

build_ui() {
  [[ -d "$UI_DIR" ]] || die "Frontend not found at $UI_DIR"
  info "Building ${UI_IMG} (${PLATFORM})..."
  ${CONTAINER_RT} build --platform "${PLATFORM}" -t "${UI_IMG}" "${UI_DIR}"
  kind load docker-image "${UI_IMG}" --name "${CLUSTER_NAME}"
  kubectl rollout restart deployment/dataset-registry-ui -n "${NAMESPACE}"
  kubectl rollout status  deployment/dataset-registry-ui -n "${NAMESPACE}" --timeout=120s
  info "dataset-registry-ui redeployed"
}

case "$COMPONENT" in
  api|backend)  build_api ;;
  ui|frontend)  build_ui  ;;
  both|all)     build_api; build_ui ;;
  *) echo "Usage: $0 [api|ui|both]"; exit 1 ;;
esac

info "Done. Registry API: http://localhost:8080/docs  UI: http://localhost:8081"
