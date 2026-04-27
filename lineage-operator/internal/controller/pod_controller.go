package controller

import (
	"context"
	"fmt"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	"github.com/ThijsKoot/openlineage-go/pkg/facets"
	"github.com/yourdomain/lineage-operator/api/v1alpha1"
	"github.com/yourdomain/lineage-operator/internal/agentcard"
	"github.com/yourdomain/lineage-operator/internal/lineage"
	"github.com/yourdomain/lineage-operator/internal/marquez"
	"github.com/yourdomain/lineage-operator/pkg/types"
)

const (
	LineageEnabledAnnotation = "ai.platform/lineage-enabled"
	LineageStatusAnnotation  = "ai.platform/lineage-status"
	AgentCardsAnnotation     = "ai.platform/agentcards"
	KagentiTypeLabel         = "kagenti.io/type"
)

// PodReconciler reconciles Pods with lineage annotations
type PodReconciler struct {
	client.Client
	Scheme        *runtime.Scheme
	MarquezClient *marquez.Client
	MarquezURL    string
	LineageDepth  int
}

// NewPodReconciler creates a new pod reconciler
func NewPodReconciler(client client.Client, scheme *runtime.Scheme, marquezURL string, lineageDepth int) *PodReconciler {
	return &PodReconciler{
		Client:        client,
		Scheme:        scheme,
		MarquezClient: marquez.NewClient(marquezURL),
		MarquezURL:    marquezURL,
		LineageDepth:  lineageDepth,
	}
}

// +kubebuilder:rbac:groups="",resources=pods,verbs=get;list;watch;update;patch
// +kubebuilder:rbac:groups=lineage.ai.platform,resources=agentcards,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=lineage.ai.platform,resources=agentcards/status,verbs=get;update;patch

// Reconcile handles pod reconciliation
func (r *PodReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := log.FromContext(ctx)

	// Fetch the pod
	pod := &corev1.Pod{}
	if err := r.Get(ctx, req.NamespacedName, pod); err != nil {
		if errors.IsNotFound(err) {
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, err
	}

	// Check if lineage is enabled via annotation OR kagenti agent label
	lineageEnabled := pod.Annotations[LineageEnabledAnnotation] == "true"
	isKagentiAgent := pod.Labels[KagentiTypeLabel] == "agent"

	if !lineageEnabled && !isKagentiAgent {
		return ctrl.Result{}, nil
	}

	// Check if already processed
	if pod.Annotations[LineageStatusAnnotation] == "registered" {
		return ctrl.Result{}, nil
	}

	// Only process running or succeeded pods
	if pod.Status.Phase != corev1.PodRunning && pod.Status.Phase != corev1.PodSucceeded {
		log.Info("Pod not ready for lineage processing", "phase", pod.Status.Phase)
		return ctrl.Result{}, nil
	}

	// Determine how lineage was enabled for logging
	enabledVia := "annotation"
	if isKagentiAgent {
		enabledVia = "kagenti.io/type=agent"
	}
	log.Info("Processing lineage for pod", "pod", pod.Name, "namespace", pod.Namespace, "enabledVia", enabledVia)

	// Parse datasets from annotations
	parser := lineage.NewAnnotationParser()
	inputs, outputs, err := parser.ParseDatasets(pod.Annotations)
	if err != nil {
		log.Error(err, "Failed to parse annotations")
		return ctrl.Result{}, err
	}

	log.Info("Parsed datasets", "inputs", len(inputs), "outputs", len(outputs))

	// Build job facets based on how lineage was enabled
	jobFacets := buildJobFacets(isKagentiAgent)

	// Register lineage in Marquez
	ownerName := getOwnerName(pod)
	err = r.MarquezClient.RegisterLineage(ctx, &marquez.RegistrationRequest{
		PodName:      pod.Name,
		PodNamespace: pod.Namespace,
		Inputs:       inputs,
		Outputs:      outputs,
		OwnerName:    ownerName,
		JobFacets:    jobFacets,
	})
	if err != nil {
		log.Error(err, "Failed to register lineage in Marquez")
		return ctrl.Result{}, err
	}

	log.Info("Successfully registered lineage in Marquez")

	// Create ONE AgentCard for this application/agent
	agentCardName, err := r.createOrUpdateAgentCard(ctx, ownerName, pod, inputs, outputs)
	if err != nil {
		log.Error(err, "Failed to create AgentCard for application")
		return ctrl.Result{}, err
	}

	log.Info("Created/updated AgentCard", "name", agentCardName, "inputs", len(inputs), "outputs", len(outputs))

	// Update pod annotations
	if pod.Annotations == nil {
		pod.Annotations = make(map[string]string)
	}
	pod.Annotations[LineageStatusAnnotation] = "registered"
	pod.Annotations[AgentCardsAnnotation] = agentCardName

	if err := r.Update(ctx, pod); err != nil {
		log.Error(err, "Failed to update pod annotations")
		return ctrl.Result{}, err
	}

	log.Info("Successfully processed lineage", "pod", pod.Name, "agentcard", agentCardName)
	return ctrl.Result{}, nil
}

// createOrUpdateAgentCard creates or updates an AgentCard for an application/agent
func (r *PodReconciler) createOrUpdateAgentCard(ctx context.Context, agentName string, pod *corev1.Pod, inputs, outputs []*types.Dataset) (string, error) {
	log := log.FromContext(ctx)

	// Build AgentCard for the application/agent
	builder := agentcard.NewBuilder()
	card, err := builder.BuildForAgent(agentName, pod, inputs, outputs)
	if err != nil {
		return "", fmt.Errorf("failed to build AgentCard: %w", err)
	}

	// Try to get existing card
	existingCard := &v1alpha1.AgentCard{}
	err = r.Get(ctx, client.ObjectKey{Name: card.ObjectMeta.Name}, existingCard)

	if err != nil {
		if errors.IsNotFound(err) {
			// Create new AgentCard
			if err := r.Create(ctx, card); err != nil {
				return "", fmt.Errorf("failed to create AgentCard: %w", err)
			}
			log.Info("Created new AgentCard", "name", card.ObjectMeta.Name)
		} else {
			return "", fmt.Errorf("failed to get AgentCard: %w", err)
		}
	} else {
		// Update existing AgentCard
		existingCard.Status.Card = card.Status.Card
		existingCard.Status.LastSyncTime = card.Status.LastSyncTime
		existingCard.Status.Protocol = card.Status.Protocol

		if err := r.Status().Update(ctx, existingCard); err != nil {
			return "", fmt.Errorf("failed to update AgentCard status: %w", err)
		}
		log.Info("Updated existing AgentCard", "name", card.ObjectMeta.Name)
	}

	return card.ObjectMeta.Name, nil
}

// buildJobFacets creates job facets based on how lineage was enabled
func buildJobFacets(isKagentiAgent bool) *facets.JobFacets {
	integration := "APPLICATION"
	if isKagentiAgent {
		integration = "AGENT"
	}

	jobTypeStr := "QUERY"

	return &facets.JobFacets{
		JobType: &facets.JobType{
			Producer:       "lineage-operator/openshift",
			SchemaURL:      "https://openlineage.io/spec/facets/1-0-1/JobTypeJobFacet.json",
			Integration:    integration,
			ProcessingType: "STREAM",
			JobType:        &jobTypeStr,
		},
	}
}

// getOwnerName extracts the application name from pod labels or owner references
func getOwnerName(pod *corev1.Pod) string {
	// Try to get app name from standard Kubernetes labels
	if appName, ok := pod.Labels["app.kubernetes.io/name"]; ok && appName != "" {
		return appName
	}

	// Fall back to legacy app label
	if appName, ok := pod.Labels["app"]; ok && appName != "" {
		return appName
	}

	// Fall back to owner reference (may be ReplicaSet for Deployments)
	for _, owner := range pod.OwnerReferences {
		return owner.Name
	}

	// Last resort: use pod name
	return pod.Name
}

// SetupWithManager sets up the controller with the Manager
func (r *PodReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&corev1.Pod{}).
		Complete(r)
}
