package agentcard

import (
	"fmt"
	"regexp"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/yourdomain/lineage-operator/api/v1alpha1"
	"github.com/yourdomain/lineage-operator/internal/lineage"
	"github.com/yourdomain/lineage-operator/internal/marquez"
	"github.com/yourdomain/lineage-operator/pkg/types"
)

// Builder builds AgentCard CRs with data lineage
type Builder struct{}

// NewBuilder creates a new AgentCard builder
func NewBuilder() *Builder {
	return &Builder{}
}

// BuildForAgent builds an AgentCard for an application/agent
func (b *Builder) BuildForAgent(agentName string, pod *corev1.Pod, inputs, outputs []*types.Dataset) (*v1alpha1.AgentCard, error) {
	// Build card name from agent/app name
	cardName := sanitizeForName(agentName)
	falseVal := false
	trueVal := true

	// Get namespace and labels from pod
	namespace := pod.Namespace
	appLabel := pod.Labels["app"]
	if appLabel == "" {
		appLabel = pod.Labels["app.kubernetes.io/name"]
	}

	// Build description based on annotations or labels
	description := fmt.Sprintf("Agent: %s", agentName)
	if desc, ok := pod.Annotations["ai.platform/description"]; ok && desc != "" {
		description = desc
	}

	// Build inputs/outputs lists for the card
	inputsList := make([]v1alpha1.LineageDataset, len(inputs))
	for i, input := range inputs {
		inputsList[i] = v1alpha1.LineageDataset{
			Namespace: input.Namespace,
			Name:      input.Name,
			ID:        input.ID(),
		}
	}

	outputsList := make([]v1alpha1.LineageDataset, len(outputs))
	for i, output := range outputs {
		outputsList[i] = v1alpha1.LineageDataset{
			Namespace: output.Namespace,
			Name:      output.Name,
			ID:        output.ID(),
		}
	}

	card := &v1alpha1.AgentCard{
		ObjectMeta: metav1.ObjectMeta{
			Name: cardName,
			Labels: map[string]string{
				"lineage.ai.platform/agent-name": sanitizeLabel(agentName),
				"lineage.ai.platform/namespace":  namespace,
				"kagenti.io/managed-by":          "lineage-operator",
			},
			Annotations: map[string]string{
				"lineage.ai.platform/agent-name-full": agentName,
				"lineage.ai.platform/pod-namespace":   namespace,
			},
		},
		Spec: v1alpha1.AgentCardSpec{
			SyncPeriod: "0", // Don't sync, we manage it directly
		},
		Status: v1alpha1.AgentCardStatus{
			Protocol:     "a2a",
			LastSyncTime: &metav1.Time{Time: time.Now()},
			Card: &v1alpha1.AgentCardData{
				Name:             agentName,
				Description:      description,
				Version:          "1.0.0",
				URL:              fmt.Sprintf("http://%s.%s.svc.cluster.local", appLabel, namespace),
				DocumentationURL: "https://openlineage.io/docs",
				Provider: &v1alpha1.AgentProvider{
					Organization: namespace,
					URL:          "https://github.com/openlineage",
				},
				Capabilities: &v1alpha1.AgentCapabilities{
					Streaming:         &falseVal,
					PushNotifications: &falseVal,
					ExtendedAgentCard: &trueVal,
				},
				Skills: []v1alpha1.AgentSkill{
					{
						ID:          "data_processing",
						Name:        "Data Processing",
						Description: fmt.Sprintf("Processes data using %d input dataset(s) and produces %d output dataset(s)", len(inputs), len(outputs)),
						Tags:        []string{"data-processing", "lineage", "openlineage"},
						InputModes:  []string{"application/json"},
						OutputModes: []string{"application/json"},
					},
				},

				// Agent dataset usage (extension to A2A spec)
				DataLineage: &v1alpha1.DataLineage{
					SchemaVersion: "1.0.0",
					SchemaURL:     "https://openlineage.io/agent-datasets/1.0.0",
					Summary: &v1alpha1.LineageSummary{
						InputDatasets:  inputsList,
						OutputDatasets: outputsList,
					},
				},
			},
		},
	}

	return card, nil
}

// BuildFromLineage builds an AgentCard from lineage data
func (b *Builder) BuildFromLineage(dataset *types.Dataset, lineageResp *marquez.LineageResponse, pod *corev1.Pod) (*v1alpha1.AgentCard, error) {
	// Trace to source
	tracer := lineage.NewTracer()
	source, transformations := tracer.TraceToSource(lineageResp, dataset)

	// Build transformations list
	transformsList := make([]v1alpha1.Transformation, len(transformations))
	for i, t := range transformations {
		transformsList[i] = v1alpha1.Transformation{
			InputDataset: &v1alpha1.LineageDataset{
				Namespace: t.InputDataset.Namespace,
				Name:      t.InputDataset.Name,
				ID:        t.InputDataset.ID(),
			},
			Job: &v1alpha1.LineageJob{
				Namespace: t.Job.Namespace,
				Name:      t.Job.Name,
				ID:        t.Job.ID(),
			},
		}
	}

	// Build data lineage
	dataLineage := &v1alpha1.DataLineage{
		SchemaVersion: "1.0.0",
		SchemaURL:     "https://openlineage.io/data-lineage/1.0.0",
		Dataset: &v1alpha1.LineageDataset{
			Namespace: dataset.Namespace,
			Name:      dataset.Name,
			ID:        dataset.ID(),
		},
		Transformations: transformsList,
		Summary: &v1alpha1.LineageSummary{
			TransformationCount: len(transformations),
			IsSourceDataset:     source == nil,
		},
	}

	if source != nil {
		dataLineage.Source = &v1alpha1.LineageDataset{
			Namespace: source.Namespace,
			Name:      source.Name,
			ID:        source.ID(),
		}
	}

	// Build AgentCard
	cardName := sanitizeName(dataset.Namespace, dataset.Name)
	falseVal := false
	trueVal := true

	card := &v1alpha1.AgentCard{
		ObjectMeta: metav1.ObjectMeta{
			Name: cardName,
			Labels: map[string]string{
				"lineage.ai.platform/dataset-namespace": sanitizeLabel(dataset.Namespace),
				"lineage.ai.platform/dataset-name":      sanitizeLabel(dataset.Name),
				"kagenti.io/managed-by":                 "lineage-operator",
			},
			Annotations: map[string]string{
				"lineage.ai.platform/dataset-namespace-full": dataset.Namespace,
				"lineage.ai.platform/dataset-name-full":      dataset.Name,
			},
		},
		Spec: v1alpha1.AgentCardSpec{
			SyncPeriod: "0", // Don't sync, we manage it directly
		},
		Status: v1alpha1.AgentCardStatus{
			Protocol:     "a2a",
			LastSyncTime: &metav1.Time{Time: time.Now()},
			Card: &v1alpha1.AgentCardData{
				Name:             fmt.Sprintf("%s-lineage", sanitizeForName(dataset.Name)),
				Description:      fmt.Sprintf("Data lineage for %s/%s", dataset.Namespace, dataset.Name),
				Version:          "1.0.0",
				URL:              "http://marquez.lineage.svc.cluster.local",
				DocumentationURL: "https://openlineage.io/docs",
				Provider: &v1alpha1.AgentProvider{
					Organization: "OpenLineage",
					URL:          "https://github.com/openlineage",
				},
				Capabilities: &v1alpha1.AgentCapabilities{
					Streaming:         &falseVal,
					PushNotifications: &falseVal,
					ExtendedAgentCard: &trueVal,
				},
				Skills: []v1alpha1.AgentSkill{
					{
						ID:          "query_dataset_lineage",
						Name:        "Query Dataset Lineage",
						Description: "Provides data lineage tracing from source to current dataset",
						Tags:        []string{"lineage", "data-governance", "openlineage", "marquez"},
						InputModes:  []string{"application/json"},
						OutputModes: []string{"application/json", "text/plain"},
					},
				},

				// DataLineage extension to A2A AgentCard spec
				DataLineage: dataLineage,
			},
		},
	}

	return card, nil
}

// sanitizeName creates a valid Kubernetes resource name from dataset namespace and name
func sanitizeName(namespace, name string) string {
	// Combine namespace and name
	combined := fmt.Sprintf("%s-%s", namespace, name)

	// Replace invalid characters with hyphens
	reg := regexp.MustCompile(`[^a-z0-9-]`)
	sanitized := reg.ReplaceAllString(strings.ToLower(combined), "-")

	// Remove leading/trailing hyphens
	sanitized = strings.Trim(sanitized, "-")

	// Ensure it starts with alphanumeric
	if len(sanitized) > 0 && !isAlphaNumeric(sanitized[0]) {
		sanitized = "dataset-" + sanitized
	}

	// Truncate to 253 characters (Kubernetes limit)
	if len(sanitized) > 253 {
		sanitized = sanitized[:253]
	}

	// Remove trailing hyphen if truncation created one
	sanitized = strings.TrimRight(sanitized, "-")

	return sanitized
}

// sanitizeLabel creates a valid Kubernetes label value
func sanitizeLabel(value string) string {
	// Label values must be 63 characters or less
	if len(value) > 63 {
		value = value[:63]
	}

	// Replace invalid characters
	reg := regexp.MustCompile(`[^a-zA-Z0-9._-]`)
	sanitized := reg.ReplaceAllString(value, "-")

	// Must start and end with alphanumeric
	sanitized = strings.Trim(sanitized, "-._")

	if len(sanitized) == 0 {
		sanitized = "unknown"
	}

	return sanitized
}

// sanitizeForName creates a valid name component
func sanitizeForName(name string) string {
	// Extract just the table/file name if it's a path
	parts := strings.Split(name, "/")
	simpleName := parts[len(parts)-1]

	// Remove extension if present
	simpleName = strings.TrimSuffix(simpleName, ".csv")
	simpleName = strings.TrimSuffix(simpleName, ".parquet")

	// Replace dots and other chars
	reg := regexp.MustCompile(`[^a-z0-9-]`)
	sanitized := reg.ReplaceAllString(strings.ToLower(simpleName), "-")

	return sanitized
}

// isAlphaNumeric checks if a byte is alphanumeric
func isAlphaNumeric(b byte) bool {
	return (b >= 'a' && b <= 'z') || (b >= '0' && b <= '9')
}
