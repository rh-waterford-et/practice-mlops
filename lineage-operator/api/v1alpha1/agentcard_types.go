package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// AgentCard represents a Kagenti AgentCard with lineage extension
// Based on A2A specification: https://github.com/a2aproject/A2A/blob/main/specification/a2a.proto
// This extends the upstream Kagenti AgentCard CRD with DataLineage support
// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:scope=Cluster
type AgentCard struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   AgentCardSpec   `json:"spec,omitempty"`
	Status AgentCardStatus `json:"status,omitempty"`
}

// AgentCardSpec defines the desired state of AgentCard
type AgentCardSpec struct {
	// SyncPeriod defines how often to sync the card (0 = no auto-sync)
	SyncPeriod string `json:"syncPeriod,omitempty"`
}

// AgentCardStatus defines the observed state of AgentCard
type AgentCardStatus struct {
	// Protocol version (e.g., "a2a")
	Protocol string `json:"protocol,omitempty"`

	// LastSyncTime is the last time the card was synced
	LastSyncTime *metav1.Time `json:"lastSyncTime,omitempty"`

	// Card contains the actual agent card data
	Card *AgentCardData `json:"card,omitempty"`
}

// AgentCardData represents the A2A agent card content
type AgentCardData struct {
	// Name of the agent
	Name string `json:"name"`

	// Description of the agent
	Description string `json:"description,omitempty"`

	// Version of the agent
	Version string `json:"version,omitempty"`

	// URL of the agent
	URL string `json:"url,omitempty"`

	// DocumentationURL provides link to agent documentation
	DocumentationURL string `json:"documentationUrl,omitempty"`

	// Provider information
	Provider *AgentProvider `json:"provider,omitempty"`

	// Capabilities of the agent
	Capabilities *AgentCapabilities `json:"capabilities,omitempty"`

	// Skills provided by the agent
	Skills []AgentSkill `json:"skills,omitempty"`

	// DataLineage provides lineage information for data assets
	// This is our proposed extension to the A2A AgentCard spec
	DataLineage *DataLineage `json:"dataLineage,omitempty"`
}

// AgentProvider represents the agent provider information
type AgentProvider struct {
	// Organization name
	Organization string `json:"organization,omitempty"`

	// URL of the provider
	URL string `json:"url,omitempty"`
}

// AgentCapabilities represents agent capabilities
type AgentCapabilities struct {
	// Streaming indicates if agent supports streaming
	Streaming *bool `json:"streaming,omitempty"`

	// PushNotifications indicates if agent supports push notifications
	PushNotifications *bool `json:"pushNotifications,omitempty"`

	// Extensions lists supported protocol extensions
	// +optional
	Extensions []string `json:"extensions,omitempty"`

	// ExtendedAgentCard indicates if this card includes extensions beyond the base A2A spec
	// +optional
	ExtendedAgentCard *bool `json:"extendedAgentCard,omitempty"`
}

// AgentSkill represents a skill provided by the agent
type AgentSkill struct {
	// ID is the unique identifier for the skill
	ID string `json:"id"`

	// Name is the human-readable name
	Name string `json:"name"`

	// Description of what the skill does
	Description string `json:"description,omitempty"`

	// Tags for categorization
	Tags []string `json:"tags,omitempty"`

	// InputModes supported (e.g., "application/json", "text/plain")
	InputModes []string `json:"inputModes,omitempty"`

	// OutputModes supported
	OutputModes []string `json:"outputModes,omitempty"`
}

// AgentCardList contains a list of AgentCard
// +kubebuilder:object:root=true
type AgentCardList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []AgentCard `json:"items"`
}

func init() {
	SchemeBuilder.Register(&AgentCard{}, &AgentCardList{})
}
