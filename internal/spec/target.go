package spec

type Target struct {
	APIVersion string     `json:"apiVersion" yaml:"apiVersion"`
	Kind       string     `json:"kind" yaml:"kind"`
	Metadata   Metadata   `json:"metadata" yaml:"metadata"`
	Spec       TargetSpec `json:"spec" yaml:"spec"`
}

type TargetSpec struct {
	DatabaseSchemaVersion string    `json:"databaseSchemaVersion" yaml:"databaseSchemaVersion"`
	Services              []Service `json:"services" yaml:"services"`
}

type Service struct {
	Name              string            `json:"name" yaml:"name"`
	Image             string            `json:"image" yaml:"image"`
	Command           []string          `json:"command" yaml:"command"`
	Args              []string          `json:"args" yaml:"args"`
	Environment       map[string]string `json:"environment" yaml:"environment"`
	SecretEnvironment map[string]string `json:"secretEnvironment" yaml:"secretEnvironment"`
	Health            Health            `json:"health" yaml:"health"`
	Probe             ProbeDeclaration  `json:"probe" yaml:"probe"`
	Resources         Resources         `json:"resources" yaml:"resources"`
	Dependencies      []string          `json:"dependencies" yaml:"dependencies"`
}

type Health struct {
	Type     string   `json:"type" yaml:"type"`
	Path     string   `json:"path,omitempty" yaml:"path,omitempty"`
	Port     int      `json:"port,omitempty" yaml:"port,omitempty"`
	Timeout  Duration `json:"timeout" yaml:"timeout"`
	Interval Duration `json:"interval" yaml:"interval"`
}

// ProbeDeclaration is an authored capability claim.
// A runtime handshake must prove it before a precise fault executes.
type ProbeDeclaration struct {
	Enabled               bool     `json:"enabled" yaml:"enabled"`
	ProtocolVersion       string   `json:"protocolVersion,omitempty" yaml:"protocolVersion,omitempty"`
	CommitMode            string   `json:"commitMode,omitempty" yaml:"commitMode,omitempty"`
	MaxControlledInFlight int      `json:"maxControlledInFlight,omitempty" yaml:"maxControlledInFlight,omitempty"`
	Checkpoints           []string `json:"checkpoints,omitempty" yaml:"checkpoints,omitempty"`
	LogicalClock          bool     `json:"logicalClock,omitempty" yaml:"logicalClock,omitempty"`
}

type Resources struct {
	CPUs        float64 `json:"cpus" yaml:"cpus"`
	MemoryBytes int64   `json:"memoryBytes" yaml:"memoryBytes"`
	PIDs        int     `json:"pids" yaml:"pids"`
}
