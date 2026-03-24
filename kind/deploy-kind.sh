#!/usr/bin/env bash
# ============================================================
# deploy-kind.sh  –  Deploy feast-kfp-mlflow on Kind (local)
#
# Usage:
#   ./kind/deploy-kind.sh [MODE]
#
# Modes:
#   all        (default) Full deploy: cluster → build → infra → jobs → api → registry
#   cluster    Create the Kind cluster only
#   build      Build & load fkm-app + spark-etl images
#   infra      Deploy infrastructure (DBs, MinIO, Redis, Marquez, MLflow)
#   jobs       Run the 6 sequential data/ML jobs
#   api        Deploy the FastAPI inference service
#   registry   Build & deploy the Dataset Registry (API + UI)
#   teardown   Delete the Kind cluster entirely
#
# Prerequisites (must be in PATH):
#   docker (or podman), kind, kubectl
# ============================================================
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(dirname "$SCRIPT_DIR")"
MODE="${1:-all}"
CLUSTER_NAME="feast-kfp-mlflow"
NAMESPACE="lineage"

# ── Colours ──────────────────────────────────────────────────
RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'
CYAN='\033[0;36m'; BOLD='\033[1m'; NC='\033[0m'

log()    { echo -e "${CYAN}[$(date +%H:%M:%S)]${NC} $*"; }
ok()     { echo -e "${GREEN}✓${NC} $*"; }
warn()   { echo -e "${YELLOW}⚠${NC}  $*"; }
die()    { echo -e "${RED}✗ ERROR:${NC} $*" >&2; exit 1; }
header() { echo -e "\n${BOLD}${CYAN}══════════════════════════════════════${NC}"; \
           echo -e "${BOLD}${CYAN}  $*${NC}"; \
           echo -e "${BOLD}${CYAN}══════════════════════════════════════${NC}"; }

# ── Container runtime detection (Docker or Podman) ───────────
detect_runtime() {
  if command -v podman &>/dev/null; then
    CONTAINER_RT="podman"
    export KIND_EXPERIMENTAL_PROVIDER=podman
  elif command -v docker &>/dev/null; then
    CONTAINER_RT="docker"
  else
    die "Neither podman nor docker found.\n  brew install podman  (or install Docker Desktop)"
  fi
  PLATFORM="linux/$(uname -m | sed 's/x86_64/amd64/;s/aarch64/arm64/')"
  ok "Container runtime: ${CONTAINER_RT}  platform: ${PLATFORM}"
}

# ── Prerequisite check ────────────────────────────────────────
check_prereqs() {
  header "Checking prerequisites"
  detect_runtime
  for cmd in kind kubectl; do
    if command -v "$cmd" &>/dev/null; then
      ok "$cmd found"
    else
      die "$cmd not found. Install with: brew install $cmd"
    fi
  done
  if [[ "${CONTAINER_RT}" == "podman" ]]; then
    if ! podman machine info &>/dev/null 2>&1; then
      die "Podman machine is not running. Start it with: podman machine start"
    fi
  else
    if ! docker info &>/dev/null; then
      die "Docker daemon is not running. Please start Docker Desktop."
    fi
  fi
  ok "All prerequisites satisfied"
}

# ── Cluster ───────────────────────────────────────────────────
create_cluster() {
  header "Creating Kind cluster: $CLUSTER_NAME"
  if kind get clusters 2>/dev/null | grep -q "^${CLUSTER_NAME}$"; then
    warn "Cluster '$CLUSTER_NAME' already exists – skipping creation"
  else
    log "Creating cluster from $SCRIPT_DIR/kind-config.yaml ..."
    kind create cluster --config "$SCRIPT_DIR/kind-config.yaml"
    ok "Cluster created"
  fi
  kubectl cluster-info --context "kind-${CLUSTER_NAME}"
}

# ── Podman image fix ──────────────────────────────────────────
# Podman loads images into Kind's containerd with a localhost/ prefix,
# but Kubernetes resolves bare names like "fkm-app:latest" to
# "docker.io/library/fkm-app:latest". Re-tag inside the node so both
# runtimes work transparently.
retag_if_podman() {
  local image="$1"
  if [[ "${CONTAINER_RT}" == "podman" ]]; then
    local node="${CLUSTER_NAME}-control-plane"
    log "Re-tagging localhost/${image} → docker.io/library/${image} (Podman fix)"
    podman exec "$node" ctr -n k8s.io images tag "localhost/${image}" "docker.io/library/${image}" 2>/dev/null || true
  fi
}

# ── Build & load images ───────────────────────────────────────
build_and_load_images() {
  header "Building app images"

  log "Building fkm-app:latest (main app image) ..."
  ${CONTAINER_RT} build --platform "${PLATFORM}" -t fkm-app:latest \
    -f "$ROOT_DIR/Dockerfile" "$ROOT_DIR"
  ok "fkm-app:latest built"

  log "Building spark-etl:latest (PySpark ETL image) ..."
  ${CONTAINER_RT} build --platform "${PLATFORM}" -t spark-etl:latest \
    -f "$ROOT_DIR/Dockerfile.spark" "$ROOT_DIR"
  ok "spark-etl:latest built"

  header "Loading images into Kind cluster"
  log "Loading fkm-app:latest ..."
  kind load docker-image fkm-app:latest --name "$CLUSTER_NAME"
  retag_if_podman "fkm-app:latest"
  ok "fkm-app:latest loaded"

  log "Loading spark-etl:latest ..."
  kind load docker-image spark-etl:latest --name "$CLUSTER_NAME"
  retag_if_podman "spark-etl:latest"
  ok "spark-etl:latest loaded"
}

# ── Build & load dataset-registry images ─────────────────────
build_and_load_registry_images() {
  header "Building dataset-registry images"

  local api_dir="$ROOT_DIR/dataset-registry/backend"
  local ui_dir="$ROOT_DIR/dataset-registry/frontend"

  if [[ ! -d "$api_dir" ]]; then
    warn "dataset-registry/backend not found – skipping registry build"
    return 0
  fi

  log "Building dataset-registry-api:local ..."
  ${CONTAINER_RT} build --platform "${PLATFORM}" \
    -t dataset-registry-api:local "$api_dir"
  ok "dataset-registry-api:local built"

  log "Building dataset-registry-ui:local ..."
  ${CONTAINER_RT} build --platform "${PLATFORM}" \
    -t dataset-registry-ui:local "$ui_dir"
  ok "dataset-registry-ui:local built"

  log "Loading registry images into Kind ..."
  kind load docker-image dataset-registry-api:local --name "$CLUSTER_NAME"
  retag_if_podman "dataset-registry-api:local"
  kind load docker-image dataset-registry-ui:local --name "$CLUSTER_NAME"
  retag_if_podman "dataset-registry-ui:local"
  ok "Registry images loaded"
}

# ── Namespace & config ────────────────────────────────────────
apply_config() {
  header "Applying namespace, secrets, and config maps"
  kubectl apply -f "$SCRIPT_DIR/base/namespace.yaml"
  kubectl apply -f "$SCRIPT_DIR/base/secret.yaml"
  kubectl apply -f "$SCRIPT_DIR/base/configmap.yaml"
  kubectl apply -f "$SCRIPT_DIR/base/feast-config.yaml"
  kubectl apply -f "$SCRIPT_DIR/base/pvc.yaml"
  ok "Config applied"
}

# ── Infrastructure ────────────────────────────────────────────
deploy_infra() {
  header "Deploying infrastructure"

  log "Deploying MinIO ..."
  kubectl apply -f "$SCRIPT_DIR/base/minio.yaml"

  log "Deploying PostgreSQL (app) ..."
  kubectl apply -f "$SCRIPT_DIR/base/postgres.yaml"

  log "Deploying Redis ..."
  kubectl apply -f "$SCRIPT_DIR/base/redis.yaml"

  log "Deploying MLflow DB ..."
  kubectl apply -f "$SCRIPT_DIR/base/mlflow-db.yaml"

  log "Deploying Marquez (OpenLineage) ..."
  kubectl apply -f "$SCRIPT_DIR/base/marquez.yaml"

  log "Waiting for storage services to be ready ..."
  kubectl rollout status deployment/mlflow-minio  -n "$NAMESPACE" --timeout=120s
  kubectl rollout status deployment/postgres      -n "$NAMESPACE" --timeout=120s
  kubectl rollout status deployment/redis         -n "$NAMESPACE" --timeout=60s
  kubectl rollout status deployment/mlflow-db     -n "$NAMESPACE" --timeout=120s
  ok "Storage services ready"

  log "Waiting for Marquez to be ready ..."
  kubectl rollout status deployment/marquez-db  -n "$NAMESPACE" --timeout=120s
  kubectl rollout status deployment/marquez     -n "$NAMESPACE" --timeout=120s
  kubectl rollout status deployment/marquez-web -n "$NAMESPACE" --timeout=120s
  ok "Marquez ready"

  log "Deploying MLflow server ..."
  kubectl apply -f "$SCRIPT_DIR/base/mlflow.yaml"
  log "Waiting for MLflow server (this takes ~2 minutes while pip installs psycopg2/boto3) ..."
  kubectl rollout status deployment/mlflow-server -n "$NAMESPACE" --timeout=300s
  ok "MLflow server ready"
}

# ── Jobs ──────────────────────────────────────────────────────
wait_job() {
  local job_name="$1"
  local timeout="${2:-300}"
  log "Waiting for job/$job_name to complete (timeout: ${timeout}s) ..."
  kubectl wait --for=condition=complete "job/$job_name" \
    -n "$NAMESPACE" --timeout="${timeout}s" || {
      warn "Job $job_name did not complete in ${timeout}s – showing logs:"
      kubectl logs -n "$NAMESPACE" -l "job=$job_name" --tail=50 || true
      die "Job $job_name failed"
    }
  ok "Job $job_name completed"
}

run_jobs() {
  header "Running data & ML jobs"

  log "01 – MinIO seed (create buckets + upload CSV) ..."
  kubectl apply -f "$SCRIPT_DIR/jobs/01-minio-seed.yaml"
  wait_job minio-seed 180

  log "02 – ETL (PySpark: MinIO → PostgreSQL) ..."
  kubectl apply -f "$SCRIPT_DIR/jobs/02-etl.yaml"
  wait_job etl-job 600

  log "03 – Feast apply (register feature definitions) ..."
  kubectl apply -f "$SCRIPT_DIR/jobs/03-feast-apply.yaml"
  wait_job feast-apply 180

  log "04 – Feast materialize (offline → Redis) ..."
  kubectl apply -f "$SCRIPT_DIR/jobs/04-feast-materialize.yaml"
  wait_job feast-materialize 300

  log "05 – ML pipeline (train + evaluate + register) ..."
  kubectl apply -f "$SCRIPT_DIR/jobs/05-ml-pipeline.yaml"
  wait_job ml-pipeline 600

  log "06 – Promote model (set champion alias) ..."
  kubectl apply -f "$SCRIPT_DIR/jobs/06-promote-model.yaml"
  wait_job promote-model 120

  ok "All jobs completed successfully"
}

# ── Inference API ─────────────────────────────────────────────
deploy_api() {
  header "Deploying inference API"
  kubectl apply -f "$SCRIPT_DIR/base/inference-api.yaml"
  log "Waiting for inference-api to be ready ..."
  kubectl rollout status deployment/inference-api -n "$NAMESPACE" --timeout=180s
  ok "Inference API ready"
}

# ── Dataset Registry ──────────────────────────────────────────
deploy_registry() {
  header "Deploying dataset registry"

  local api_dir="$ROOT_DIR/dataset-registry/backend"
  if [[ ! -d "$api_dir" ]]; then
    warn "dataset-registry/backend not found – skipping registry deploy"
    return 0
  fi

  kubectl apply -f "$SCRIPT_DIR/base/dataset-registry.yaml"
  log "Waiting for registry-db ..."
  kubectl rollout status deployment/registry-db -n "$NAMESPACE" --timeout=120s
  log "Waiting for dataset-registry-api ..."
  kubectl rollout status deployment/dataset-registry-api -n "$NAMESPACE" --timeout=120s
  log "Waiting for dataset-registry-ui ..."
  kubectl rollout status deployment/dataset-registry-ui -n "$NAMESPACE" --timeout=120s
  ok "Dataset Registry ready"
}

# ── Teardown ──────────────────────────────────────────────────
teardown() {
  header "Tearing down Kind cluster: $CLUSTER_NAME"
  kind delete cluster --name "$CLUSTER_NAME"
  ok "Cluster deleted"
}

# ── Print service URLs ────────────────────────────────────────
print_urls() {
  header "Service URLs"
  echo ""
  echo -e "  ${BOLD}MinIO Console${NC}        http://localhost:9001   (minioadmin / minioadmin123)"
  echo -e "  ${BOLD}MinIO S3 API${NC}         http://localhost:9000"
  echo -e "  ${BOLD}MLflow UI${NC}            http://localhost:5000"
  echo -e "  ${BOLD}Marquez Web UI${NC}       http://localhost:3000"
  echo -e "  ${BOLD}Inference API${NC}        http://localhost:8000"
  echo -e "  ${BOLD}Dataset Registry API${NC} http://localhost:8080/docs"
  echo -e "  ${BOLD}Dataset Registry UI${NC}  http://localhost:8081"
  echo ""
  echo -e "  Example prediction:"
  echo -e "  ${CYAN}curl -X POST http://localhost:8000/predict \\${NC}"
  echo -e "  ${CYAN}     -H 'Content-Type: application/json' \\${NC}"
  echo -e "  ${CYAN}     -d '{\"entity_ids\": [1, 2, 3]}'${NC}"
  echo ""
}

# ── Main ──────────────────────────────────────────────────────
case "$MODE" in
  all)
    check_prereqs
    create_cluster
    build_and_load_images
    build_and_load_registry_images
    apply_config
    deploy_infra
    run_jobs
    deploy_api
    deploy_registry
    print_urls
    ;;
  cluster)
    check_prereqs
    create_cluster
    ;;
  build)
    check_prereqs
    build_and_load_images
    ;;
  infra)
    apply_config
    deploy_infra
    ;;
  jobs)
    run_jobs
    ;;
  api)
    deploy_api
    print_urls
    ;;
  registry)
    check_prereqs
    build_and_load_registry_images
    deploy_registry
    print_urls
    ;;
  teardown)
    teardown
    ;;
  *)
    echo "Usage: $0 [all|cluster|build|infra|jobs|api|registry|teardown]"
    exit 1
    ;;
esac
