package lineage

import (
	"github.com/yourdomain/lineage-operator/internal/marquez"
	"github.com/yourdomain/lineage-operator/pkg/types"
)

// Tracer traces lineage backwards to find source and transformation chain
type Tracer struct{}

// NewTracer creates a new lineage tracer
func NewTracer() *Tracer {
	return &Tracer{}
}

// TraceToSource traces a dataset back to its source
// Returns the source dataset and the transformation chain
func (t *Tracer) TraceToSource(lineageResp *marquez.LineageResponse, currentDataset *types.Dataset) (*types.Dataset, []types.Transformation) {
	// Build node map for quick lookup
	nodes := make(map[string]*marquez.LineageNode)
	for i := range lineageResp.Graph {
		node := &lineageResp.Graph[i]
		nodes[node.ID] = node
	}

	// Build upstream map (target -> sources)
	upstream := make(map[string][]string)
	for _, node := range lineageResp.Graph {
		for _, edge := range node.InEdges {
			if upstream[node.ID] == nil {
				upstream[node.ID] = []string{}
			}
			upstream[node.ID] = append(upstream[node.ID], edge.Origin)
		}
	}

	// Trace backwards from current dataset
	transformations := []types.Transformation{}
	visited := make(map[string]bool)
	current := currentDataset.ID()

	for {
		if visited[current] {
			break // Cycle detected
		}
		visited[current] = true

		// Find upstream nodes
		upstreamIDs := upstream[current]
		if len(upstreamIDs) == 0 {
			break // Reached source
		}

		// Find the job that produced this dataset
		var producerJob *marquez.LineageNode
		for _, upstreamID := range upstreamIDs {
			upstreamNode := nodes[upstreamID]
			if upstreamNode != nil && upstreamNode.Type == "JOB" {
				producerJob = upstreamNode
				break
			}
		}

		if producerJob == nil {
			break // No producer job found
		}

		// Find the input dataset to this job
		jobUpstream := upstream[producerJob.ID]
		var inputDataset *marquez.LineageNode
		for _, upstreamID := range jobUpstream {
			upstreamNode := nodes[upstreamID]
			if upstreamNode != nil && upstreamNode.Type == "DATASET" {
				inputDataset = upstreamNode
				break
			}
		}

		if inputDataset == nil {
			break // No input dataset found
		}

		// Add transformation
		transformations = append(transformations, types.Transformation{
			InputDataset: &types.Dataset{
				Namespace: inputDataset.Data.Namespace,
				Name:      inputDataset.Data.Name,
			},
			Job: &types.Job{
				Namespace: producerJob.Data.Namespace,
				Name:      producerJob.Data.Name,
			},
		})

		// Move to input dataset
		current = inputDataset.ID
	}

	// Find source dataset
	var source *types.Dataset
	if len(transformations) > 0 {
		// The input to the first transformation is the source
		source = transformations[len(transformations)-1].InputDataset

		// Reverse transformations to go from source -> current
		for i, j := 0, len(transformations)-1; i < j; i, j = i+1, j-1 {
			transformations[i], transformations[j] = transformations[j], transformations[i]
		}
	} else {
		// Current dataset is the source (no transformations)
		source = nil
	}

	return source, transformations
}
