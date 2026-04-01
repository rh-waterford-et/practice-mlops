package marquez

// LineageResponse represents the response from Marquez lineage API
type LineageResponse struct {
	Graph []LineageNode `json:"graph"`
}

// LineageNode represents a node in the lineage graph
type LineageNode struct {
	ID       string          `json:"id"`
	Type     string          `json:"type"` // "DATASET" or "JOB"
	Data     LineageNodeData `json:"data"`
	InEdges  []LineageEdge   `json:"inEdges"`
	OutEdges []LineageEdge   `json:"outEdges"`
}

// LineageNodeData contains node metadata
type LineageNodeData struct {
	Type      string `json:"type"`
	Namespace string `json:"namespace"`
	Name      string `json:"name"`
}

// LineageEdge represents an edge in the lineage graph
type LineageEdge struct {
	Origin      string `json:"origin"`
	Destination string `json:"destination"`
}
