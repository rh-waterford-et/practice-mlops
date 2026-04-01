package marquez

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/google/uuid"

	ol "github.com/ThijsKoot/openlineage-go"
	"github.com/ThijsKoot/openlineage-go/pkg/facets"
	"github.com/ThijsKoot/openlineage-go/pkg/transport"
	"github.com/yourdomain/lineage-operator/pkg/types"
)

// Client is an OpenLineage client wrapper
type Client struct {
	BaseURL    string
	OLClient   *ol.Client
	HTTPClient *http.Client
}

// NewClient creates a new OpenLineage client
func NewClient(baseURL string) *Client {
	// Configure OpenLineage client to send to Marquez
	cfg := ol.ClientConfig{
		Transport: transport.Config{
			Type: transport.TransportTypeHTTP,
			HTTP: &transport.HTTPConfig{
				URL: baseURL,
			},
		},
	}

	olClient, err := ol.NewClient(cfg)
	if err != nil {
		// Fallback to console if HTTP client creation fails
		cfg.Transport.Type = transport.TransportTypeConsole
		cfg.Transport.Console = &transport.ConsoleConfig{
			PrettyPrint: true,
		}
		olClient, _ = ol.NewClient(cfg)
	}

	return &Client{
		BaseURL:    baseURL,
		OLClient:   olClient,
		HTTPClient: &http.Client{Timeout: 30 * time.Second},
	}
}

// Tag represents a tag in the tags facet
type Tag struct {
	Key    string `json:"key"`
	Value  string `json:"value"`
	Source string `json:"source"`
}

// TagsRunFacet represents custom tags run facet following OpenLineage spec
type TagsRunFacet struct {
	Producer  string `json:"_producer"`
	SchemaURL string `json:"_schemaURL"`
	Tags      []Tag  `json:"tags"`
}

// CustomRunFacets extends RunFacets with tags
type CustomRunFacets struct {
	*facets.RunFacets
	Tags *TagsRunFacet `json:"tags,omitempty"`
}

// RegistrationRequest contains data for lineage registration
type RegistrationRequest struct {
	PodName      string
	PodNamespace string
	Inputs       []*types.Dataset
	Outputs      []*types.Dataset
	OwnerName    string
	JobFacets    *facets.JobFacets // Job facets (e.g., jobType facet)
}

// RegisterLineage registers lineage in Marquez using OpenLineage SDK
func (c *Client) RegisterLineage(ctx context.Context, req *RegistrationRequest) error {
	// Generate deterministic run ID based on pod
	runID := uuid.NewSHA1(uuid.NameSpaceOID, []byte(fmt.Sprintf("%s/%s", req.PodNamespace, req.PodName)))

	// Build input datasets
	inputs := []ol.InputElement{}
	for _, input := range req.Inputs {
		inputs = append(inputs, ol.NewInputElement(input.Name, input.Namespace))
	}

	// Build output datasets
	outputs := []ol.OutputElement{}
	for _, output := range req.Outputs {
		outputs = append(outputs, ol.NewOutputElement(output.Name, output.Namespace))
	}

	// Build run facets with tags
	customRunFacets := map[string]interface{}{
		"tags": map[string]interface{}{
			"_producer":  "lineage-operator/openshift",
			"_schemaURL": "https://openlineage.io/spec/facets/1-0-0/TagsRunFacet.json",
			"tags": []map[string]string{
				{
					"key":    "pod_name",
					"value":  req.PodName,
					"source": "KUBERNETES",
				},
				{
					"key":    "pod_namespace",
					"value":  req.PodNamespace,
					"source": "KUBERNETES",
				},
			},
		},
	}

	// Helper function to emit custom event with run tags
	emitCustomEvent := func(eventType ol.EventType) error {
		// Create base event
		event := ol.NewNamespacedRunEvent(
			eventType,
			runID,
			req.OwnerName,
			req.PodNamespace,
		).WithInputs(inputs...).
			WithOutputs(outputs...)

		// Add job facets
		if req.JobFacets != nil {
			event.Job.Facets = req.JobFacets
		}

		// Convert event to emittable format
		emittableEvent := event.AsEmittable()

		// Marshal to JSON
		eventJSON, err := json.Marshal(emittableEvent)
		if err != nil {
			return fmt.Errorf("failed to marshal event: %w", err)
		}

		// Parse JSON to map to add custom run facets
		var eventMap map[string]interface{}
		if err := json.Unmarshal(eventJSON, &eventMap); err != nil {
			return fmt.Errorf("failed to unmarshal event: %w", err)
		}

		// Add custom run facets
		if runMap, ok := eventMap["run"].(map[string]interface{}); ok {
			runMap["facets"] = customRunFacets
		}

		// Send directly via HTTP
		eventJSONWithTags, err := json.Marshal(eventMap)
		if err != nil {
			return fmt.Errorf("failed to marshal modified event: %w", err)
		}

		// Post to Marquez
		resp, err := c.HTTPClient.Post(
			fmt.Sprintf("%s/api/v1/lineage", c.BaseURL),
			"application/json",
			strings.NewReader(string(eventJSONWithTags)),
		)
		if err != nil {
			return fmt.Errorf("failed to post event: %w", err)
		}
		defer resp.Body.Close()

		if resp.StatusCode != 200 && resp.StatusCode != 201 {
			bodyBytes, _ := io.ReadAll(resp.Body)
			return fmt.Errorf("server responded with status %d: %s", resp.StatusCode, string(bodyBytes))
		}

		return nil
	}

	// Emit START event
	if err := emitCustomEvent(ol.EventTypeStart); err != nil {
		return fmt.Errorf("failed to emit START event: %w", err)
	}

	// Emit COMPLETE event
	if err := emitCustomEvent(ol.EventTypeComplete); err != nil {
		return fmt.Errorf("failed to emit COMPLETE event: %w", err)
	}

	return nil
}

// QueryLineage queries lineage for a dataset from Marquez
func (c *Client) QueryLineage(ctx context.Context, dataset *types.Dataset, depth int) (*LineageResponse, error) {
	nodeID := dataset.ID()
	queryURL := fmt.Sprintf("%s/api/v1/lineage?nodeId=%s&depth=%d",
		c.BaseURL,
		url.QueryEscape(nodeID),
		depth)

	req, err := http.NewRequestWithContext(ctx, "GET", queryURL, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}

	resp, err := c.HTTPClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("failed to query marquez: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		bodyBytes, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("marquez returned status %d: %s", resp.StatusCode, string(bodyBytes))
	}

	var lineageResp LineageResponse
	if err := json.NewDecoder(resp.Body).Decode(&lineageResp); err != nil {
		return nil, fmt.Errorf("failed to decode response: %w", err)
	}

	return &lineageResp, nil
}
