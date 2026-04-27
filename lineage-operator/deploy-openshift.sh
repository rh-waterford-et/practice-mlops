#!/bin/bash

# Deploy Lineage Operator to OpenShift
# This script automates the complete deployment process

set -e

NAMESPACE="lineage-operator-system"
BUILD_NAME="lineage-operator"

echo "🚀 Starting Lineage Operator deployment to OpenShift..."

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

# Step 1: Create namespace
echo "📦 Creating namespace: $NAMESPACE"
oc create namespace $NAMESPACE --dry-run=client -o yaml | oc apply -f -

# Step 2: Create BuildConfig and ImageStream
echo "🔧 Setting up BuildConfig and ImageStream..."
oc apply -f config/openshift/buildconfig-binary.yaml

# Step 3: Start the build
echo "🏗️  Starting build from local source..."
oc start-build $BUILD_NAME --from-dir=. --follow -n $NAMESPACE

# Wait for build to complete
echo "⏳ Waiting for build to complete..."
BUILD_STATUS=$(oc get build -n $NAMESPACE --sort-by=.metadata.creationTimestamp -o jsonpath='{.items[-1].status.phase}')
while [ "$BUILD_STATUS" != "Complete" ] && [ "$BUILD_STATUS" != "Failed" ]; do
    sleep 5
    BUILD_STATUS=$(oc get build -n $NAMESPACE --sort-by=.metadata.creationTimestamp -o jsonpath='{.items[-1].status.phase}')
    echo "  Build status: $BUILD_STATUS"
done

if [ "$BUILD_STATUS" = "Failed" ]; then
    echo "❌ Build failed. Check logs with: oc logs -f bc/$BUILD_NAME -n $NAMESPACE"
    exit 1
fi

echo "✅ Build completed successfully!"

# Step 4: Deploy CRD
echo "📋 Deploying Custom Resource Definition..."
oc apply -f config/crd/agentcard_crd.yaml

# Step 5: Deploy RBAC
echo "🔐 Deploying RBAC resources..."
oc apply -f config/rbac/service_account.yaml
oc apply -f config/rbac/role.yaml
oc apply -f config/rbac/role_binding.yaml

# Step 6: Deploy the operator
echo "🎯 Deploying operator..."
oc apply -f config/openshift/deployment-openshift.yaml

# Wait for deployment to be ready
echo "⏳ Waiting for operator to be ready..."
oc rollout status deployment/lineage-operator-controller-manager -n $NAMESPACE --timeout=300s

echo ""
echo "✨ Deployment complete! ✨"
echo ""
echo "📊 Status:"
oc get pods -n $NAMESPACE
echo ""
echo "📝 View logs:"
echo "  oc logs -f deployment/lineage-operator-controller-manager -n $NAMESPACE"
echo ""
echo "🧪 Test with example pod:"
echo "  oc create namespace lineage"
echo "  oc apply -f config/samples/annotated_pod.yaml"
echo ""
echo "🗑️  To uninstall:"
echo "  make openshift-undeploy"
