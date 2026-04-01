package types

import "fmt"

// Dataset represents a dataset in OpenLineage format
type Dataset struct {
	Namespace string
	Name      string
}

// ID returns the fully qualified dataset identifier
func (d *Dataset) ID() string {
	return fmt.Sprintf("dataset:%s:%s", d.Namespace, d.Name)
}

// Job represents a job in OpenLineage format
type Job struct {
	Namespace string
	Name      string
}

// ID returns the fully qualified job identifier
func (j *Job) ID() string {
	return fmt.Sprintf("job:%s:%s", j.Namespace, j.Name)
}

// Transformation represents a single processing step
type Transformation struct {
	InputDataset *Dataset
	Job          *Job
}
