/*
Copyright 2024.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package v1alpha1

// DataLineage provides data lineage information for datasets
// Following OpenLineage naming conventions
//
// This is proposed as an extension to Kagenti's AgentCardData struct
// to add first-class support for data lineage tracking.
type DataLineage struct {
	// Schema version for the data lineage format
	// +kubebuilder:validation:Required
	SchemaVersion string `json:"schemaVersion"`

	// Schema URL pointing to the data lineage specification
	// +kubebuilder:validation:Required
	SchemaURL string `json:"schemaUrl"`

	// Dataset being described by this lineage
	// Used when DataLineage represents a single dataset's lineage
	// +optional
	Dataset *LineageDataset `json:"dataset,omitempty"`

	// Source dataset (where the data originally came from)
	// Null if this dataset is itself a source
	// +optional
	Source *LineageDataset `json:"source,omitempty"`

	// Ordered list of transformations from source to current dataset
	// +optional
	Transformations []Transformation `json:"transformations,omitempty"`

	// Summary statistics about the lineage
	// +kubebuilder:validation:Required
	Summary *LineageSummary `json:"summary"`
}

// LineageDataset represents a dataset in the lineage graph
// Uses OpenLineage dataset identity: namespace + name
type LineageDataset struct {
	// Namespace identifies where the dataset lives
	// Examples: "postgres://host:port", "s3://bucket", "kafka://host:port"
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:MinLength=1
	Namespace string `json:"namespace"`

	// Name identifies what the dataset is
	// Examples: "database.table", "path/to/file", "topic-name"
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:MinLength=1
	Name string `json:"name"`

	// ID is the fully qualified dataset identifier
	// Format: "dataset:namespace:name"
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:Pattern=`^dataset:.+:.+$`
	ID string `json:"id"`
}

// Transformation represents a single processing step in the lineage chain
type Transformation struct {
	// Input dataset consumed by this transformation
	// +kubebuilder:validation:Required
	InputDataset *LineageDataset `json:"inputDataset"`

	// Job that performed the transformation
	// +kubebuilder:validation:Required
	Job *LineageJob `json:"job"`
}

// LineageJob represents a job/process in the lineage
type LineageJob struct {
	// Namespace identifies where the job runs
	// Typically the Kubernetes namespace
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:MinLength=1
	Namespace string `json:"namespace"`

	// Name of the job
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:MinLength=1
	Name string `json:"name"`

	// ID is the fully qualified job identifier
	// Format: "job:namespace:name"
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:Pattern=`^job:.+:.+$`
	ID string `json:"id"`
}

// LineageSummary provides quick statistics about the lineage
type LineageSummary struct {
	// Number of transformation steps from source to current dataset
	// +kubebuilder:validation:Minimum=0
	// +optional
	TransformationCount int `json:"transformationCount,omitempty"`

	// Whether this dataset is a source dataset (no upstream transformations)
	// +optional
	IsSourceDataset bool `json:"isSourceDataset,omitempty"`

	// InputDatasets lists all datasets consumed by the agent
	// Used when DataLineage represents an agent's dataset usage
	// +optional
	InputDatasets []LineageDataset `json:"inputDatasets,omitempty"`

	// OutputDatasets lists all datasets produced by the agent
	// Used when DataLineage represents an agent's dataset usage
	// +optional
	OutputDatasets []LineageDataset `json:"outputDatasets,omitempty"`
}
