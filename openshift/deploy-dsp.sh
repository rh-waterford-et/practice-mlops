#!/usr/bin/env bash
# ═══════════════════════════════════════════════════════════════════════
# Deploy Data Science Pipelines + upload the ML pipeline to OpenShift AI
#
# Prerequisites:
#   - Infrastructure already deployed (./openshift/deploy.sh)
#   - ETL, Feast apply, Feast materialize already completed
#
# Usage:
#   ./openshift/deploy-dsp.sh              # Full: DSPA + compile + upload
#   ./openshift/deploy-dsp.sh dspa         # Deploy DSPA only
#   ./openshift/deploy-dsp.sh upload       # Compile + upload only
# ═══════════════════════════════════════════════════════════════════════
set -euo pipefail

NAMESPACE="lineage"
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
STAGE="${1:-all}"

GREEN='\033[0;32m'
CYAN='\033[0;36m'
RED='\033[0;31m'
NC='\033[0m'

banner() { echo -e "\n${CYAN}═══════════════════════════════════════${NC}"; echo -e "${CYAN}  $1${NC}"; echo -e "${CYAN}═══════════════════════════════════════${NC}"; }
info()   { echo -e "${GREEN}[INFO]${NC}  $1"; }
err()    { echo -e "${RED}[ERROR]${NC} $1"; }

# ── Ensure the pipeline-artifacts bucket exists in MinIO ────────────
create_pipeline_bucket() {
    info "Ensuring 'pipeline-artifacts' bucket exists in MinIO ..."
    oc run minio-bucket-init --rm -i --restart=Never \
        --image=image-registry.openshift-image-registry.svc:5000/lineage/fkm-app:latest \
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

# ── Deploy DSPA ─────────────────────────────────────────────────────
deploy_dspa() {
    banner "Deploy Data Science Pipelines"

    oc project "$NAMESPACE"

    create_pipeline_bucket

    info "Applying DSPA secret + CR ..."
    oc apply -f "$SCRIPT_DIR/dsp/dspa-secret.yaml"
    oc apply -f "$SCRIPT_DIR/dsp/dspa.yaml"

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

    DSP_ROUTE=$(oc get route "ds-pipeline-dspa" -n "$NAMESPACE" -o jsonpath='{.spec.host}' 2>/dev/null || echo "<pending>")
    info "DSP API: https://$DSP_ROUTE"
    info "View pipelines in the OpenShift AI dashboard under Data Science Pipelines"
}

# ── Compile + Upload pipeline ───────────────────────────────────────
upload_pipeline() {
    banner "Compile & Upload Pipeline"

    cd "$PROJECT_ROOT"

    info "Compiling pipeline YAML ..."
    python3 -m src.pipeline.kfp_pipeline
    info "Compiled → customer_churn_pipeline.yaml"

    DSP_ROUTE=$(oc get route "ds-pipeline-dspa" -n "$NAMESPACE" -o jsonpath='{.spec.host}' 2>/dev/null)
    TOKEN=$(oc whoami -t)

    if [[ -z "$DSP_ROUTE" ]]; then
        err "Cannot find DSP route. Is the DSPA deployed?"
        exit 1
    fi

    info "Uploading pipeline to https://$DSP_ROUTE ..."
    python3 -c "
import urllib3, time
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

from kfp.client import Client

client = Client(
    host='https://${DSP_ROUTE}',
    existing_token='${TOKEN}',
    ssl_ca_cert=False,
)

pipeline_name = 'customer-churn-ml-pipeline'
yaml_file = 'customer_churn_pipeline.yaml'

# Upload pipeline definition
try:
    p = client.upload_pipeline(
        pipeline_package_path=yaml_file,
        pipeline_name=pipeline_name,
        description='End-to-end: Feast, Validate, Engineer, Train, MLflow',
    )
    print(f'Pipeline created: {p.pipeline_id}')
    pid = p.pipeline_id
except Exception as e:
    if 'already exist' in str(e).lower():
        print('Pipeline exists, uploading new version ...')
        all_p = client.list_pipelines()
        pid = None
        for pp in (all_p.pipelines or []):
            if pp.display_name == pipeline_name or pp.display_name == 'Customer Churn ML Pipeline':
                pid = pp.pipeline_id
                break
        if pid:
            v = client.upload_pipeline_version(
                pipeline_package_path=yaml_file,
                pipeline_version_name=f'v{int(time.time())}',
                pipeline_id=pid,
            )
            print(f'New version: {v.pipeline_version_id}')
        else:
            raise
    else:
        raise

# Create a run
run = client.create_run_from_pipeline_package(
    pipeline_file=yaml_file,
    arguments={},
    run_name=f'churn-run-{int(time.time())}',
    experiment_name='customer_churn_lineage',
)
print(f'Run started: {run.run_id}')
"

    info "Pipeline uploaded and run started"
    echo ""
    echo -e "${GREEN}Open the OpenShift AI dashboard:${NC}"
    echo "  → Data Science Pipelines → Pipeline definitions"
    echo "  → Runs tab to see the execution"
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
        echo "Usage: $0 {all|dspa|upload}"
        exit 1
        ;;
esac

banner "DONE"
