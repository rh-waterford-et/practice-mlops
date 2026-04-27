#!/bin/bash

# Build locally and push to OpenShift internal registry
set -e

NAMESPACE="lineage-operator-system"
IMAGE_NAME="lineage-operator"
LOCAL_TAG="lineage-operator:latest"

echo "🚀 Building and pushing Lineage Operator to OpenShift registry..."

# Check if oc is installed
if ! command -v oc &> /dev/null; then
    echo "❌ Error: oc CLI not found. Please install OpenShift CLI."
    exit 1
fi

# Check if we're logged in
if ! oc whoami &> /dev/null; then
    echo "❌ Error: Not logged in to OpenShift. Please run 'oc login' first."
    exit 1
fi

# Check if docker or podman is available
if command -v docker &> /dev/null; then
    CONTAINER_CLI="docker"
elif command -v podman &> /dev/null; then
    CONTAINER_CLI="podman"
else
    echo "❌ Error: Neither docker nor podman found. Please install one."
    exit 1
fi

echo "📦 Using container CLI: $CONTAINER_CLI"

# Step 1: Create namespace and ImageStream
echo "📋 Creating namespace and ImageStream..."
oc create namespace $NAMESPACE --dry-run=client -o yaml | oc apply -f -
cat <<EOF | oc apply -f -
apiVersion: image.openshift.io/v1
kind: ImageStream
metadata:
  name: $IMAGE_NAME
  namespace: $NAMESPACE
  labels:
    app: $IMAGE_NAME
EOF

# Step 2: Expose registry if not already exposed
echo "🔓 Checking if registry route exists..."
if ! oc get route default-route -n openshift-image-registry &> /dev/null; then
    echo "  Exposing OpenShift registry..."
    oc patch configs.imageregistry.operator.openshift.io/cluster \
        --patch '{"spec":{"defaultRoute":true}}' --type=merge

    # Wait for route to be created
    echo "  Waiting for registry route..."
    for i in {1..30}; do
        if oc get route default-route -n openshift-image-registry &> /dev/null; then
            break
        fi
        sleep 2
    done
fi

# Get registry hostname
REGISTRY_HOST=$(oc get route default-route -n openshift-image-registry -o jsonpath='{.spec.host}')
echo "📍 Registry host: $REGISTRY_HOST"

# Step 3: Login to OpenShift registry
echo "🔐 Logging into OpenShift registry..."
TOKEN=$(oc whoami -t)
echo $TOKEN | $CONTAINER_CLI login -u $(oc whoami) --password-stdin $REGISTRY_HOST

# Step 4: Build image locally for linux/amd64 platform
echo "🏗️  Building image locally for linux/amd64..."
$CONTAINER_CLI build --platform linux/amd64 -t $LOCAL_TAG .

# Step 5: Tag for OpenShift registry
REGISTRY_IMAGE="$REGISTRY_HOST/$NAMESPACE/$IMAGE_NAME:latest"
echo "🏷️  Tagging image: $REGISTRY_IMAGE"
$CONTAINER_CLI tag $LOCAL_TAG $REGISTRY_IMAGE

# Step 6: Push to OpenShift registry
echo "⬆️  Pushing image to OpenShift registry..."
$CONTAINER_CLI push $REGISTRY_IMAGE

# Step 7: Verify image was pushed
echo "✅ Verifying image in ImageStream..."
sleep 2
oc get imagestream $IMAGE_NAME -n $NAMESPACE

echo ""
echo "✨ Image successfully pushed! ✨"
echo ""
echo "📍 Image location: $REGISTRY_IMAGE"
echo ""
echo "🎯 Next steps:"
echo "  1. Deploy CRD and RBAC:"
echo "     oc apply -f config/crd/agentcard_crd.yaml"
echo "     oc apply -f config/rbac/service_account.yaml"
echo "     oc apply -f config/rbac/role.yaml"
echo "     oc apply -f config/rbac/role_binding.yaml"
echo ""
echo "  2. Deploy operator:"
echo "     oc apply -f config/openshift/deployment-openshift.yaml"
echo ""
echo "  Or run everything with:"
echo "     make openshift-deploy"
