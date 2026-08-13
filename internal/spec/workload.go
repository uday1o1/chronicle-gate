package spec

type Workload struct {
	APIVersion string       `json:"apiVersion" yaml:"apiVersion"`
	Kind       string       `json:"kind" yaml:"kind"`
	Metadata   Metadata     `json:"metadata" yaml:"metadata"`
	Spec       WorkloadSpec `json:"spec" yaml:"spec"`
}

type WorkloadSpec struct {
	Events        map[string]CloudEvent `json:"events" yaml:"events"`
	Observations  []Observation         `json:"observations" yaml:"observations"`
	Invariants    []Invariant           `json:"invariants" yaml:"invariants"`
	Normalization []Normalization       `json:"normalization" yaml:"normalization"`
	Quiescence    Quiescence            `json:"quiescence" yaml:"quiescence"`
}
