# OpenShift Deployment Guide

This directory contains OpenShift-specific deployment manifests for the Lineage Operator.

## Prerequisites

- Access to an OpenShift cluster
- `oc` CLI installed and authenticated
- Docker or Podman installed (for local build option)

## Deployment Options

> **⚠️ Docker Hub Rate Limits**: If you encounter Docker Hub rate limits during cluster builds, use **Option 1** (Local Build & Push) instead.

### Option 0: Local Build & Push (Recommended - Avoids Docker Hub Limits)

Build the image locally and push to OpenShift's internal registry:

```bash
# Quick automated method
make openshift-push-deploy

# Or just push the image
make openshift-push

# Then deploy manually
make openshift-deploy
```

See [LOCAL_BUILD_PUSH.md](LOCAL_BUILD_PUSH.md) for detailed manual steps.

### Option 1: Build from Local Source (Binary Build)

This is the recommended approach for development and testing.

1. **Create the namespace and BuildConfig:**

```bash
cd /Users/rcarroll/Documents/code/practice-mlops-kind/lineage-operator

# Create namespace
oc create namespace lineage-operator-system

# Create ImageStream and BuildConfig
oc apply -f config/openshift/buildconfig-binary.yaml
```

2. **Start a build from local source:**

```bash
# From the lineage-operator directory
oc start-build lineage-operator \
  --from-dir=. \
  --follow \
  -n lineage-operator-system
```

3. **Deploy the operator:**

```bash
# Apply CRD
oc apply -f config/crd/agentcard_crd.yaml

# Apply RBAC
oc apply -f config/rbac/service_account.yaml
oc apply -f config/rbac/role.yaml
oc apply -f config/rbac/role_binding.yaml

# Deploy operator (using OpenShift ImageStream)
oc apply -f config/openshift/deployment-openshift.yaml
```

### Option 2: Build from Git Repository

If your code is in a Git repository:

1. **Update the Git URI in `buildconfig.yaml`:**

Edit `config/openshift/buildconfig.yaml` and update:
- `git.uri`: Your Git repository URL
- `git.ref`: Your branch name
- `contextDir`: Path to lineage-operator directory (if not at repo root)

2. **Deploy:**

```bash
# Create namespace
oc create namespace lineage-operator-system

# Create ImageStream and BuildConfig
oc apply -f config/openshift/buildconfig.yaml

# The build will start automatically
oc logs -f bc/lineage-operator -n lineage-operator-system

# Deploy the operator
oc apply -f config/crd/agentcard_crd.yaml
oc apply -f config/rbac/service_account.yaml
oc apply -f config/rbac/role.yaml
oc apply -f config/rbac/role_binding.yaml
oc apply -f config/openshift/deployment-openshift.yaml
```

## Rebuilding

### Rebuild from local source:

```bash
oc start-build lineage-operator \
  --from-dir=. \
  --follow \
  -n lineage-operator-system
```

### Rebuild from Git (if using Git BuildConfig):

```bash
oc start-build lineage-operator -n lineage-operator-system
```

## Verify Deployment

```bash
# Check build status
oc get builds -n lineage-operator-system

# Check ImageStream
oc get imagestream lineage-operator -n lineage-operator-system

# Check operator pod
oc get pods -n lineage-operator-system

# View operator logs
oc logs -f deployment/lineage-operator-controller-manager -n lineage-operator-system
```

## Testing with Example Pod

```bash
# Create the lineage namespace
oc create namespace lineage

# Deploy the example pod
oc apply -f config/samples/annotated_pod.yaml
```

## Uninstall

```bash
# Delete deployment
oc delete -f config/openshift/deployment-openshift.yaml

# Delete RBAC
oc delete -f config/rbac/role_binding.yaml
oc delete -f config/rbac/role.yaml
oc delete -f config/rbac/service_account.yaml

# Delete CRD (this will delete all AgentCard resources)
oc delete -f config/crd/agentcard_crd.yaml

# Delete BuildConfig and ImageStream
oc delete buildconfig lineage-operator -n lineage-operator-system
oc delete imagestream lineage-operator -n lineage-operator-system

# Delete namespace
oc delete namespace lineage-operator-system
```

## Troubleshooting

### Build fails

Check build logs:
```bash
oc logs -f bc/lineage-operator -n lineage-operator-system
```

### Pod not starting

Check pod events and logs:
```bash
oc describe pod -l app=lineage-operator -n lineage-operator-system
oc logs -l app=lineage-operator -n lineage-operator-system
```

### RBAC issues

Ensure the ClusterRole and ClusterRoleBinding are created:
```bash
oc get clusterrole lineage-operator-role
oc get clusterrolebinding lineage-operator-rolebinding
```

## Image Registry Access

The deployment uses the OpenShift internal registry:
```
image-registry.openshift-image-registry.svc:5000/lineage-operator-system/lineage-operator:latest
```

If you need to pull the image from outside the cluster, you can expose the registry:
```bash
oc patch configs.imageregistry.operator.openshift.io/cluster --patch '{"spec":{"defaultRoute":true}}' --type=merge
```
