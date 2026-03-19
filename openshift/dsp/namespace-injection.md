# OpenLineage Namespace Injection: Demo vs Production

## Problem

Every pipeline pod needs `OPENLINEAGE_NAMESPACE` set to the Kubernetes
namespace it runs in, so that lineage events are attributed to the correct
project. This must happen without any pipeline code changes -- data
scientists should not have to configure lineage.

## Demo Approach (current)

We patch the Argo workflow controller's ConfigMap directly:

```bash
kubectl patch configmap ds-pipeline-workflow-controller-dspa \
  -n lineage --type merge \
  --patch-file openshift/dsp/workflow-controller-patch.yaml

kubectl rollout restart deployment/ds-pipeline-workflow-controller-dspa -n lineage
```

This adds a `mainContainer` section with a Kubernetes Downward API
`fieldRef` that injects `metadata.namespace` as the
`OPENLINEAGE_NAMESPACE` env var into the main container of every
workflow pod.

**Limitation**: The `ds-pipeline-workflow-controller-dspa` ConfigMap is
owned by the DSPA operator (`ownerReferences` point to the DSPA CR).
If the DSPA CR is reapplied or the operator reconciles, the patch may
be overwritten.

## Production Approaches

### 1. DSPA CR Native Support (preferred)

The DSPA operator should expose a field in the
`DataSciencePipelinesApplication` CR spec, e.g.:

```yaml
spec:
  workflowController:
    mainContainer:
      env:
        - name: OPENLINEAGE_NAMESPACE
          valueFrom:
            fieldRef:
              fieldPath: metadata.namespace
```

This would be merged into the generated ConfigMap and survive operator
reconciliation. Requires an upstream contribution to
`opendatahub-io/data-science-pipelines-operator`.

### 2. MutatingAdmissionWebhook

A namespace-scoped or cluster-scoped webhook that intercepts pod
creation for pods labelled with
`component: data-science-pipelines` and injects the env var with
`fieldRef`. Fully transparent to both pipeline authors and DSPA
configuration. Managed by platform operations.

### 3. Kustomize Post-Patch in GitOps

If DSPA is deployed via GitOps (ArgoCD / Flux), add a Kustomize
strategic merge patch that adds the `mainContainer` key to the
generated ConfigMap. This relies on the GitOps tool reconciling after
the DSPA operator, which may require ordering controls.

## Recommendation

For production OpenShift AI deployments, option 1 (DSPA CR native
support) is the cleanest path. It keeps the configuration declarative,
survives operator upgrades, and requires no additional infrastructure.
Until that is available upstream, option 2 (webhook) provides the most
robust alternative.
