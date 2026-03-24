#!/usr/bin/env bash
# Smoke-test the inference API on OpenShift
set -euo pipefail

NS="lineage"
HOST=$(oc get route inference-api -n "$NS" -o jsonpath='{.spec.host}')

echo "Inference API host: $HOST"
echo ""

echo "── /health ──"
curl -sk "https://$HOST/health" | python3 -m json.tool

echo ""
echo "── /predict ──"
curl -sk -X POST "https://$HOST/predict" \
  -H "Content-Type: application/json" \
  -d '{"entity_ids": [1, 2, 3, 10, 50]}' | python3 -m json.tool
