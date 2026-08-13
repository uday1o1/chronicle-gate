package spec

type Bundle struct {
	APIVersion        string          `json:"apiVersion"`
	Kind              string          `json:"kind"`
	RunID             string          `json:"runId"`
	Scenario          string          `json:"scenario"`
	Targets           []string        `json:"targets"`
	Images            []BundleImage   `json:"images"`
	Resources         BundleResources `json:"resources"`
	Files             []BundleFile    `json:"files"`
	Safety            BundleSafety    `json:"safety"`
	ExpectedSignature string          `json:"expectedSignature"`
	Nonportable       bool            `json:"nonportable"`
}

type BundleImage struct {
	Name      string `json:"name"`
	Reference string `json:"reference"`
	Archive   string `json:"archive,omitempty"`
	Portable  bool   `json:"portable"`
}

type BundleResources struct {
	CPUs        float64 `json:"cpus"`
	MemoryBytes int64   `json:"memoryBytes"`
	DiskBytes   int64   `json:"diskBytes"`
}

type BundleFile struct {
	Path   string `json:"path"`
	SHA256 string `json:"sha256"`
	Size   int64  `json:"size"`
}

type BundleSafety struct {
	MaxFiles         int   `json:"maxFiles"`
	MaxExpandedBytes int64 `json:"maxExpandedBytes"`
	SymlinksAllowed  bool  `json:"symlinksAllowed"`
}
