#!/usr/bin/env bash
# Build and deploy the RAG inference service
set -euo pipefail

NAMESPACE=${1:-lineage}

echo "Building and deploying RAG inference service to namespace: $NAMESPACE"

# Step 1: Create BuildConfig
echo "Creating BuildConfig..."
oc apply -f rag-service-buildconfig.yaml

# Step 2: Build the image
echo "Building image..."
cd rag-service
oc start-build rag-inference-app -n $NAMESPACE --from-dir=. --follow

# Step 3: Deploy the service
echo "Deploying service..."
cd ..
oc apply -f rag-inference-service.yaml

# Step 4: Wait for rollout
echo "Waiting for deployment to complete..."
oc rollout status deployment/rag-inference-service -n $NAMESPACE

# Step 5: Get route
ROUTE=$(oc get route rag-inference -n $NAMESPACE -o jsonpath='{.spec.host}')

echo ""
echo "=============================================="
echo "RAG Inference Service Deployed Successfully!"
echo "=============================================="
echo ""
echo "Route: https://$ROUTE"
echo ""
echo "Test endpoints:"
echo "  Health:  curl https://$ROUTE/health"
echo "  Config:  curl https://$ROUTE/config"
echo "  Query:   curl -X POST https://$ROUTE/query -H 'Content-Type: application/json' -d '{\"query\":\"test\",\"user_id\":\"123\"}'"
echo ""
echo "Check lineage initContainer logs:"
echo "  oc logs -n $NAMESPACE -l app=rag-inference -c lineage-registration"
echo ""
