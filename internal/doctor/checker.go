package doctor

import (
	"context"
	"encoding/json"
	"fmt"
	"path/filepath"
	"strings"
	"time"

	"github.com/uday1o1/chronicle-gate/internal/imagelock"
)

const defaultCommandTimeout = 30 * time.Second

// Options configures diagnostics and allows deterministic test substitution.
type Options struct {
	Workspace             string
	ImageLockPath         string
	DockerBinary          string
	MinimumAvailableBytes uint64
	CommandTimeout        time.Duration
	Runner                CommandRunner
}

// Checker evaluates the ordered doctor contract.
type Checker struct {
	options Options
}

// New creates a Checker with safe local defaults.
func New(options Options) *Checker {
	if options.Workspace == "" {
		options.Workspace = "."
	}
	if options.ImageLockPath == "" {
		options.ImageLockPath = filepath.Join(options.Workspace, "config", "images.lock.json")
	}
	if options.DockerBinary == "" {
		options.DockerBinary = "docker"
	}
	if options.MinimumAvailableBytes == 0 {
		options.MinimumAvailableBytes = defaultMinimumAvailableBytes
	}
	if options.CommandTimeout == 0 {
		options.CommandTimeout = defaultCommandTimeout
	}
	if options.Runner == nil {
		options.Runner = ExecRunner{}
	}
	return &Checker{options: options}
}

type dockerServer struct {
	Version      string `json:"Version"`
	OS           string `json:"Os"`
	Architecture string `json:"Arch"`
}

// Run performs every independent check and marks dependent checks as skipped.
func (checker *Checker) Run(ctx context.Context) Report {
	checks := []Check{
		checkWorkspaceDisk(checker.options.Workspace, checker.options.MinimumAvailableBytes),
		checkLoopbackPort(),
	}

	server, dockerCheck := checker.checkDocker(ctx)
	checks = append(checks, dockerCheck)
	if dockerCheck.Status != StatusPass {
		checks = append(checks,
			skipped("docker.server_os", "docker", "Docker server was not reachable"),
			skipped("docker.server_architecture", "docker", "Docker server was not reachable"),
		)
	} else {
		checks = append(checks, checkDockerOS(server), checkDockerArchitecture(server))
	}

	lock, lockCheck := checker.checkLock()
	checks = append(checks, lockCheck)
	if dockerCheck.Status != StatusPass || lockCheck.Status != StatusPass {
		reason := "image lock was invalid"
		if dockerCheck.Status != StatusPass {
			reason = "Docker server was not reachable"
		}
		for _, name := range lockedImageNames(lock) {
			checks = append(checks, skipped("image."+name, "registry", reason))
		}
		return newReport(checks)
	}

	checks = append(checks, checker.checkImages(ctx, lock)...)
	return newReport(checks)
}

func (checker *Checker) checkDocker(ctx context.Context) (dockerServer, Check) {
	commandContext, cancel := context.WithTimeout(ctx, checker.options.CommandTimeout)
	defer cancel()
	stdout, stderr, err := checker.options.Runner.Run(
		commandContext,
		checker.options.DockerBinary,
		"version",
		"--format",
		"{{json .Server}}",
	)
	if err != nil {
		message := boundedMessage(stderr)
		if message == "" {
			message = err.Error()
		}
		return dockerServer{}, Check{
			ID:      "docker.reachable",
			Scope:   "docker",
			Status:  StatusFail,
			Summary: "Docker server is not reachable: " + message,
		}
	}

	var server dockerServer
	if err := json.Unmarshal(stdout, &server); err != nil {
		return dockerServer{}, Check{
			ID:      "docker.reachable",
			Scope:   "docker",
			Status:  StatusFail,
			Summary: fmt.Sprintf("Docker returned an invalid server description: %v", err),
		}
	}
	if server.Version == "" || server.OS == "" || server.Architecture == "" {
		return dockerServer{}, Check{
			ID:      "docker.reachable",
			Scope:   "docker",
			Status:  StatusFail,
			Summary: "Docker returned an incomplete server description",
		}
	}

	return server, Check{
		ID:      "docker.reachable",
		Scope:   "docker",
		Status:  StatusPass,
		Summary: "Docker server is reachable",
		Details: map[string]any{"serverVersion": server.Version},
	}
}

func checkDockerOS(server dockerServer) Check {
	if strings.EqualFold(server.OS, "linux") {
		return Check{
			ID:      "docker.server_os",
			Scope:   "docker",
			Status:  StatusPass,
			Summary: "Docker server runs Linux containers",
			Details: map[string]any{"os": "linux"},
		}
	}
	return Check{
		ID:      "docker.server_os",
		Scope:   "docker",
		Status:  StatusFail,
		Summary: fmt.Sprintf("Docker server OS %q is unsupported; Linux is required", server.OS),
	}
}

func checkDockerArchitecture(server dockerServer) Check {
	architecture := NormalizeArchitecture(server.Architecture)
	if architecture == "arm64" || architecture == "amd64" {
		return Check{
			ID:      "docker.server_architecture",
			Scope:   "docker",
			Status:  StatusPass,
			Summary: "Docker server architecture is supported",
			Details: map[string]any{"architecture": architecture},
		}
	}
	return Check{
		ID:      "docker.server_architecture",
		Scope:   "docker",
		Status:  StatusFail,
		Summary: fmt.Sprintf("Docker server architecture %q is unsupported", architecture),
	}
}

// NormalizeArchitecture maps common host aliases to OCI architecture names.
func NormalizeArchitecture(architecture string) string {
	switch strings.ToLower(architecture) {
	case "aarch64":
		return "arm64"
	case "x86_64":
		return "amd64"
	default:
		return strings.ToLower(architecture)
	}
}

func (checker *Checker) checkLock() (imagelock.Lock, Check) {
	lock, err := imagelock.Load(checker.options.ImageLockPath)
	if err != nil {
		return imagelock.Lock{}, Check{
			ID:      "images.lock",
			Scope:   "repository",
			Status:  StatusFail,
			Summary: fmt.Sprintf("immutable image lock is invalid: %v", err),
		}
	}
	return lock, Check{
		ID:      "images.lock",
		Scope:   "repository",
		Status:  StatusPass,
		Summary: "immutable image lock is valid",
		Details: map[string]any{
			"imageCount":    len(lock.Images),
			"schemaVersion": lock.SchemaVersion,
		},
	}
}

func (checker *Checker) checkImages(ctx context.Context, lock imagelock.Lock) []Check {
	checks := make([]Check, len(lock.Images))
	type result struct {
		index int
		check Check
	}
	results := make(chan result, len(lock.Images))

	for index, image := range lock.Images {
		go func() {
			results <- result{index: index, check: checker.checkImage(ctx, image)}
		}()
	}
	for range lock.Images {
		result := <-results
		checks[result.index] = result.check
	}
	return checks
}

func (checker *Checker) checkImage(ctx context.Context, image imagelock.Image) Check {
	commandContext, cancel := context.WithTimeout(ctx, checker.options.CommandTimeout)
	defer cancel()
	stdout, stderr, err := checker.options.Runner.Run(
		commandContext,
		checker.options.DockerBinary,
		"manifest",
		"inspect",
		image.Reference,
	)
	if err != nil {
		message := boundedMessage(stderr)
		if message == "" {
			message = err.Error()
		}
		return Check{
			ID:      "image." + image.Name,
			Scope:   "registry",
			Status:  StatusFail,
			Summary: fmt.Sprintf("locked OCI index is unavailable: %s", message),
			Details: map[string]any{"reference": image.Reference},
		}
	}
	if err := imagelock.VerifyIndexManifest(image, stdout); err != nil {
		return Check{
			ID:      "image." + image.Name,
			Scope:   "registry",
			Status:  StatusFail,
			Summary: fmt.Sprintf("locked OCI index failed child verification: %v", err),
			Details: map[string]any{"reference": image.Reference},
		}
	}
	return Check{
		ID:      "image." + image.Name,
		Scope:   "registry",
		Status:  StatusPass,
		Summary: "locked OCI index and required platform manifests are available",
		Details: map[string]any{
			"platforms": imagelock.RequiredPlatforms(),
			"reference": image.Reference,
			"source":    image.Source,
		},
	}
}

func skipped(id string, scope string, reason string) Check {
	return Check{ID: id, Scope: scope, Status: StatusSkip, Summary: reason}
}

func lockedImageNames(lock imagelock.Lock) []string {
	if len(lock.Images) == 0 {
		return []string{"availability"}
	}
	names := make([]string, len(lock.Images))
	for index, image := range lock.Images {
		names[index] = image.Name
	}
	return names
}

func boundedMessage(data []byte) string {
	message := strings.TrimSpace(string(data))
	if len(message) > 512 {
		return message[:512] + "..."
	}
	return message
}
