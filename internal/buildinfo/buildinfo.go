// Package buildinfo exposes immutable CLI build metadata.
package buildinfo

// These values are replaced with linker flags for release builds.
var (
	Version   = "dev"
	Commit    = "unknown"
	BuildDate = "unknown"
)

// Info is the stable machine-readable version model.
type Info struct {
	Version   string `json:"version"`
	Commit    string `json:"commit"`
	BuildDate string `json:"buildDate"`
}

// Current returns the build metadata compiled into the executable.
func Current() Info {
	return Info{
		Version:   Version,
		Commit:    Commit,
		BuildDate: BuildDate,
	}
}
