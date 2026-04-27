# Local Build and Push to OpenShift Registry

This guide shows how to build the operator image locally and push it to the OpenShift internal registry, avoiding Docker Hub rate limits.

## Quick Start

### Automated (Recommended)

```bash
# Build locally, push to OpenShift registry, and deploy
make openshift-push-deploy
```

Or just push the image:

```bash
make openshift-push
```

### Manual Steps

If you prefer to run commands manually:

#### 1. Login to OpenShift

```bash
oc login <your-openshift-cluster>
```

#### 2. Create Namespace and ImageStream

```bash
oc create namespace lineage-operator-system

cat <<EOF | oc apply -f -
apiVersion: image.openshift.io/v1
kind: ImageStream
metadata:
  name: lineage-operator
  namespace: lineage-operator-system
  labels:
    app: lineage-operator
EOF
```

#### 3. Expose OpenShift Registry

```bash
# Check if registry route already exists
oc get route default-route -n openshift-image-registry

# If not, expose it
oc patch configs.imageregistry.operator.openshift.io/cluster \
    --patch '{"spec":{"defaultRoute":true}}' --type=merge

# Get registry hostname
REGISTRY_HOST=$(oc get route default-route -n openshift-image-registry -o jsonpath='{.spec.host}')
echo "Registry: $REGISTRY_HOST"
```

#### 4. Login to Registry

Using Docker:
```bash
TOKEN=$(oc whoami -t)
echo $TOKEN | docker login -u $(oc whoami) --password-stdin $REGISTRY_HOST
```

Or using Podman:
```bash
TOKEN=$(oc whoami -t)
echo $TOKEN | podman login -u $(oc whoami) --password-stdin $REGISTRY_HOST
```

#### 5. Build Image Locally

```bash
# From the lineage-operator directory
docker build -t lineage-operator:latest .

# Or with podman
podman build -t lineage-operator:latest .
```

#### 6. Tag for OpenShift Registry

```bash
# Get the registry host (from step 3)
REGISTRY_HOST=$(oc get route default-route -n openshift-image-registry -o jsonpath='{.spec.host}')

# Tag the image
docker tag lineage-operator:latest $REGISTRY_HOST/lineage-operator-system/lineage-operator:latest

# Or with podman
podman tag lineage-operator:latest $REGISTRY_HOST/lineage-operator-system/lineage-operator:latest
```

#### 7. Push to Registry

```bash
# Push with docker
docker push $REGISTRY_HOST/lineage-operator-system/lineage-operator:latest

# Or with podman
podman push $REGISTRY_HOST/lineage-operator-system/lineage-operator:latest
```

#### 8. Verify Image

```bash
oc get imagestream lineage-operator -n lineage-operator-system
oc describe imagestream lineage-operator -n lineage-operator-system
```

#### 9. Deploy the Operator

```bash
# Deploy CRD and RBAC
oc apply -f config/crd/agentcard_crd.yaml
oc apply -f config/rbac/service_account.yaml
oc apply -f config/rbac/role.yaml
oc apply -f config/rbac/role_binding.yaml

# Deploy operator
oc apply -f config/openshift/deployment-openshift.yaml
```

#### 10. Verify Deployment

```bash
# Check pod status
oc get pods -n lineage-operator-system

# View logs
oc logs -f deployment/lineage-operator-controller-manager -n lineage-operator-system
```

## Rebuilding After Changes

When you make code changes:

```bash
# Quick rebuild and push
make openshift-push

# The deployment will automatically pull the new image
oc rollout restart deployment/lineage-operator-controller-manager -n lineage-operator-system
```

## Troubleshooting

### Registry Login Fails

Make sure you're logged into OpenShift:
```bash
oc whoami
oc whoami -t  # Verify you have a token
```

### Registry Route Not Found

Ensure the registry is exposed:
```bash
oc get configs.imageregistry.operator.openshift.io cluster -o yaml | grep defaultRoute
```

Should show `defaultRoute: true`. If not, run:
```bash
oc patch configs.imageregistry.operator.openshift.io/cluster \
    --patch '{"spec":{"defaultRoute":true}}' --type=merge
```

### Image Not Pulling

Check the deployment image reference:
```bash
oc get deployment lineage-operator-controller-manager -n lineage-operator-system -o yaml | grep image:
```

Should be:
```
image: image-registry.openshift-image-registry.svc:5000/lineage-operator-system/lineage-operator:latest
```

### Build Fails Locally

Check Docker/Podman resources:
```bash
# For Docker Desktop, ensure you have enough memory/CPU allocated
# For Podman, check system resources

# Try building with verbose output
docker build --no-cache -t lineage-operator:latest .
```

## Using Different Registries

### External Registry (Quay.io, Docker Hub, etc.)

If you prefer to use an external registry:

```bash
# Build and tag
docker build -t quay.io/yourusername/lineage-operator:latest .

# Push to external registry
docker push quay.io/yourusername/lineage-operator:latest

# Update deployment to use external image
# Edit config/openshift/deployment-openshift.yaml
# Change image to: quay.io/yourusername/lineage-operator:latest
```

### Local Registry

If running a local registry:

```bash
# Build and tag
docker build -t localhost:5000/lineage-operator:latest .

# Push
docker push localhost:5000/lineage-operator:latest
```

## CI/CD Integration

You can integrate this into your CI/CD pipeline:

```yaml
# Example GitHub Actions
- name: Build and Push to OpenShift
  run: |
    oc login --token=${{ secrets.OPENSHIFT_TOKEN }} --server=${{ secrets.OPENSHIFT_SERVER }}
    make openshift-push
```

## Image Size Optimization

To reduce image size:

```bash
# Use multi-stage build (already in Dockerfile)
# Current size: ~83MB

# Further optimization options:
# 1. Use UPX to compress binary (in Dockerfile)
# 2. Trim dependencies (already using distroless base)
# 3. Use build args to skip test dependencies
```
