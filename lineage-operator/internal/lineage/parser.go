package lineage

import (
	"fmt"
	"strings"

	"github.com/yourdomain/lineage-operator/pkg/types"
)

// AnnotationParser parses dataset URIs from pod annotations
type AnnotationParser struct{}

// NewAnnotationParser creates a new annotation parser
func NewAnnotationParser() *AnnotationParser {
	return &AnnotationParser{}
}

// ParseDatasets extracts input and output datasets from pod annotations
func (p *AnnotationParser) ParseDatasets(annotations map[string]string) ([]*types.Dataset, []*types.Dataset, error) {
	inputs := []*types.Dataset{}
	outputs := []*types.Dataset{}

	// Parse ai.platform/* annotations
	for key, value := range annotations {
		if strings.HasPrefix(key, "ai.platform/input-") ||
			strings.HasPrefix(key, "ai.platform/dataset-") ||
			strings.HasPrefix(key, "ai.platform/model-") {
			// Inputs: datasets, models (for inference), and explicit inputs
			dataset, err := p.parseDatasetURI(value)
			if err != nil {
				return nil, nil, fmt.Errorf("invalid input %s: %w", key, err)
			}
			inputs = append(inputs, dataset)
		} else if strings.HasPrefix(key, "ai.platform/output-") {
			// Outputs: explicit outputs
			dataset, err := p.parseDatasetURI(value)
			if err != nil {
				return nil, nil, fmt.Errorf("invalid output %s: %w", key, err)
			}
			outputs = append(outputs, dataset)
		}
	}

	return inputs, outputs, nil
}

// parseDatasetURI parses an OpenLineage dataset URI
// Format: scheme://authority/path
// Namespace: scheme://authority
// Name: path
func (p *AnnotationParser) parseDatasetURI(uri string) (*types.Dataset, error) {
	if uri == "" {
		return nil, fmt.Errorf("empty URI")
	}

	if !strings.Contains(uri, "://") {
		return nil, fmt.Errorf("missing scheme: %s", uri)
	}

	// Split on first ://
	parts := strings.SplitN(uri, "://", 2)
	if len(parts) != 2 {
		return nil, fmt.Errorf("invalid URI format: %s", uri)
	}

	scheme := parts[0]
	rest := parts[1]

	// Handle special cases for feast and model schemes
	// feast://namespace/name -> namespace="namespace", name="name"
	// model://namespace/name -> namespace="namespace", name="name"
	if scheme == "feast" || scheme == "model" {
		if !strings.Contains(rest, "/") {
			return nil, fmt.Errorf("%s URI must have format %s://namespace/name: %s", scheme, scheme, uri)
		}
		namespaceAndName := strings.SplitN(rest, "/", 2)
		return &types.Dataset{
			Namespace: namespaceAndName[0],
			Name:      namespaceAndName[1],
		}, nil
	}

	// Standard case: split on first /
	if strings.Contains(rest, "/") {
		authorityAndPath := strings.SplitN(rest, "/", 2)
		authority := authorityAndPath[0]
		path := authorityAndPath[1]

		return &types.Dataset{
			Namespace: fmt.Sprintf("%s://%s", scheme, authority),
			Name:      path,
		}, nil
	}

	// No path, entire rest is authority (name is empty)
	return &types.Dataset{
		Namespace: fmt.Sprintf("%s://%s", scheme, rest),
		Name:      "",
	}, nil
}
