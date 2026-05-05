#!/usr/bin/env bash
# ═══════════════════════════════════════════════════════════════════════
# OpenShift AI — Data Science Pipelines (DSPA) + upload KFP pipeline
#
# Prerequisites:
#   - ./openshift/deploy.sh infra   (or full stack) so MinIO + namespace exist
#
# Usage:
#   ./openshift/deploy-dsp.sh                      # DSP in OPENSHIFT_APP_NAMESPACE or lineage
#   ./openshift/deploy-dsp.sh --namespace fkm all
#   OPENSHIFT_APP_NAMESPACE=fkm ./openshift/deploy-dsp.sh dspa
#   ./openshift/deploy-dsp.sh upload               # Compile + upload + start run
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

GREEN='\033[0;32m'
YELLOW='\033[1;33m'
CYAN='\033[0;36m'
RED='\033[0;31m'
NC='\033[0m'

banner() { echo -e "\n${CYAN}═══════════════════════════════════════${NC}"; echo -e "${CYAN}  $1${NC}"; echo -e "${CYAN}═══════════════════════════════════════${NC}"; }
info()   { echo -e "${GREEN}[INFO]${NC}  $1"; }
warn()   { echo -e "${YELLOW}[WARN]${NC}  $1"; }
err()    { echo -e "${RED}[ERROR]${NC} $1"; }

# ── Ensure the pipeline-artifacts bucket exists in MinIO ────────────
create_pipeline_bucket() {
    info "Ensuring 'pipeline-artifacts' bucket exists in MinIO ..."
    oc run minio-bucket-init --rm -i --restart=Never \
        --image=image-registry.openshift-image-registry.svc:5000/${NAMESPACE}/fkm-app:latest \
        -n "$NAMESPACE" -- python3 -c "
from minio import Minio
client = Minio('mlflow-minio:9000', access_key='minioadmin', secret_key='minioadmin123', secure=False)
if not client.bucket_exists('pipeline-artifacts'):
    client.make_bucket('pipeline-artifacts')
    print('Created bucket: pipeline-artifacts')
else:
    print('Bucket exists: pipeline-artifacts')
" 2>&1 || true
}

# ── Patch workflow controller: OPENLINEAGE_NAMESPACE from pod namespace ──
patch_workflow_controller_openlineage() {
    _dsp_workflow_patch_file=$(mktemp)
    trap 'rm -f "${_dsp_workflow_patch_file:-}"' RETURN
    cat >"$_dsp_workflow_patch_file" <<'PATCHYAML'
apiVersion: v1
kind: ConfigMap
metadata:
  name: ds-pipeline-workflow-controller-dspa
data:
  mainContainer: |
    env:
      - name: OPENLINEAGE_NAMESPACE
        valueFrom:
          fieldRef:
            fieldPath: metadata.namespace
PATCHYAML
    info "Patching workflow controller ConfigMap (OPENLINEAGE_NAMESPACE) ..."
    oc patch configmap ds-pipeline-workflow-controller-dspa \
        -n "$NAMESPACE" --type merge --patch-file "$_dsp_workflow_patch_file" || warn "patch skipped (ConfigMap missing yet?)"
    oc rollout restart deployment/ds-pipeline-workflow-controller-dspa -n "$NAMESPACE" 2>/dev/null || true
}

# ── Deploy DSPA (extract from single manifest) ───────────────────────
deploy_dspa() {
    banner "Deploy Data Science Pipelines"

    oc project "$NAMESPACE"

    create_pipeline_bucket

    info "Applying DSPA Secret + DataSciencePipelinesApplication (namespace $NAMESPACE) ..."
    "$PYTHON3" "$RENDER_SCRIPT" "$MANIFEST" "$NAMESPACE" | "$PYTHON3" "$SPLITTER" -f - emit-dspa | oc apply -f -

    info "Waiting for DSPA to become ready (this may take 2-3 minutes) ..."
    for i in $(seq 1 60); do
        READY=$(oc get dspa dspa -n "$NAMESPACE" -o jsonpath='{.status.conditions[?(@.type=="APIServerReady")].status}' 2>/dev/null || echo "Unknown")
        if [[ "$READY" == "True" ]]; then
            info "DSPA is ready"
            break
        fi
        if [[ $i -eq 60 ]]; then
            err "DSPA not ready after 5 minutes. Check: oc describe dspa dspa -n $NAMESPACE"
            exit 1
        fi
        sleep 5
    done

    patch_workflow_controller_openlineage

    DSP_ROUTE=$(oc get route "ds-pipeline-dspa" -n "$NAMESPACE" -o jsonpath='{.spec.host}' 2>/dev/null || echo "<pending>")
    info "DSP API: https://$DSP_ROUTE"
    info "View pipelines in the OpenShift AI dashboard under Data Science Pipelines"
}

# ── Compile + upload + run (shared with python -m src.pipeline.upload_pipeline) ──
upload_pipeline() {
    banner "Compile & upload pipeline (Python module)"

    cd "$PROJECT_ROOT"
    "$PYTHON3" -m src.pipeline.upload_pipeline

    info "Done. Open OpenShift AI → Data Science Pipelines → Runs"
}

# ── Main ────────────────────────────────────────────────────────────
case "$STAGE" in
    dspa)   deploy_dspa ;;
    upload) upload_pipeline ;;
    all)
        deploy_dspa
        upload_pipeline
        ;;
    *)
        err "Unknown stage: $STAGE"
        echo "Usage: $0 [--namespace NS] {all|dspa|upload}"
        exit 1
        ;;
esac

banner "DONE"
