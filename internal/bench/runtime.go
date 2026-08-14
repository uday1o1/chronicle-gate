package bench

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/netip"
	"net/url"
	"os"
	"os/exec"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/moby/moby/api/types/container"
	mobynetwork "github.com/moby/moby/api/types/network"
	mobyclient "github.com/moby/moby/client"
	"github.com/testcontainers/testcontainers-go"
	tclog "github.com/testcontainers/testcontainers-go/log"
	tcnetwork "github.com/testcontainers/testcontainers-go/network"
	"github.com/testcontainers/testcontainers-go/wait"
	"github.com/uday1o1/chronicle-gate/internal/imagelock"
	chronruntime "github.com/uday1o1/chronicle-gate/internal/runtime"
	"github.com/uday1o1/chronicle-gate/internal/spec"
)

const benchmarkRunLabel = "dev.chronicle.run"

type targetIdentity struct {
	Role           string `json:"role"`
	AuthoredImage  string `json:"authoredImage"`
	PlatformDigest string `json:"platformDigest,omitempty"`
	ExecutedImage  string `json:"executedImageId"`
	Portable       bool   `json:"portable"`
}

type benchmarkEnvironment struct {
	RunID       string
	Network     *testcontainers.DockerNetwork
	NetworkName string
	cleaned     bool
}

type benchmarkService struct {
	container  testcontainers.Container
	endpoint   string
	identity   targetIdentity
	reference  bool
	network    string
	healthPort int
}

type instrumentationEvidence struct {
	HarnessCorrectnessInstrumentationAbsent bool   `json:"harnessCorrectnessInstrumentationAbsent"`
	ProbeDisabled                           bool   `json:"probeDisabled"`
	TelemetryEnvironmentAbsent              bool   `json:"telemetryEnvironmentAbsent"`
	SecretMountsAbsent                      bool   `json:"secretMountsAbsent"`
	DockerHealthcheckDisabled               bool   `json:"dockerHealthcheckDisabled"`
	AutomaticCompressionDisabled            bool   `json:"automaticCompressionDisabled"`
	TargetBinaryInternalsProven             bool   `json:"targetBinaryInternalsProven"`
	Claim                                   string `json:"claim"`
}

func newBenchmarkEnvironment(ctx context.Context, runID string) (*benchmarkEnvironment, error) {
	tclog.SetDefault(tclog.NewNoopLogger())
	if err := os.Setenv("TESTCONTAINERS_RYUK_DISABLED", "true"); err != nil {
		return nil, fmt.Errorf("disable mutable Testcontainers reaper image: %w", err)
	}
	if err := chronruntime.ConfigureDockerHost(ctx); err != nil {
		return nil, err
	}
	network, err := tcnetwork.New(ctx, tcnetwork.WithLabels(map[string]string{benchmarkRunLabel: runID}))
	if err != nil {
		return nil, fmt.Errorf("create benchmark network: %w", err)
	}
	return &benchmarkEnvironment{RunID: runID, Network: network, NetworkName: network.Name}, nil
}

func (environment *benchmarkEnvironment) cleanup(ctx context.Context) error {
	if environment == nil || environment.cleaned {
		return nil
	}
	if environment.Network != nil {
		if err := environment.Network.Remove(ctx); err != nil {
			return fmt.Errorf("remove benchmark network %q: %w", environment.NetworkName, err)
		}
		environment.Network = nil
	}
	environment.cleaned = true
	return nil
}

func materializeTarget(ctx context.Context, role string, target spec.Target) (targetIdentity, error) {
	if err := chronruntime.ConfigureDockerHost(ctx); err != nil {
		return targetIdentity{}, err
	}
	declaration := target.Spec.Services[0]
	docker, err := mobyclient.New(mobyclient.FromEnv)
	if err != nil {
		return targetIdentity{}, fmt.Errorf("create Docker client: %w", err)
	}
	defer func() { _ = docker.Close() }()
	imageReference := declaration.Image
	if !imagelock.IsLocalImageID(declaration.Image) {
		imageReference, err = resolvePlatformImage(ctx, docker, declaration.Image)
		if err != nil {
			return targetIdentity{}, err
		}
	}
	inspected, err := docker.ImageInspect(ctx, imageReference)
	if err != nil {
		if imagelock.IsLocalImageID(declaration.Image) {
			return targetIdentity{}, fmt.Errorf("inspect local benchmark image %s: %w", declaration.Image, err)
		}
		response, pullErr := docker.ImagePull(ctx, imageReference, mobyclient.ImagePullOptions{})
		if pullErr != nil {
			return targetIdentity{}, fmt.Errorf("pull benchmark image %s: %w", imageReference, pullErr)
		}
		_, readErr := io.Copy(io.Discard, response)
		closeErr := response.Close()
		if err := errors.Join(readErr, closeErr); err != nil {
			return targetIdentity{}, fmt.Errorf("read benchmark image pull response: %w", err)
		}
		inspected, err = docker.ImageInspect(ctx, imageReference)
		if err != nil {
			return targetIdentity{}, fmt.Errorf("inspect pulled benchmark image: %w", err)
		}
	}
	if imagelock.IsLocalImageID(declaration.Image) && inspected.ID != declaration.Image {
		return targetIdentity{}, fmt.Errorf("local benchmark image identity is %s, want %s", inspected.ID, declaration.Image)
	}
	platformDigest := ""
	if !imagelock.IsLocalImageID(declaration.Image) {
		info, infoErr := docker.Info(ctx, mobyclient.InfoOptions{})
		if infoErr != nil {
			return targetIdentity{}, fmt.Errorf("reinspect Docker platform: %w", infoErr)
		}
		if !strings.EqualFold(inspected.Os, info.Info.OSType) || normalizeBenchmarkArchitecture(inspected.Architecture) != normalizeBenchmarkArchitecture(info.Info.Architecture) {
			return targetIdentity{}, fmt.Errorf("selected image platform %s/%s does not match Docker %s/%s", inspected.Os, inspected.Architecture, info.Info.OSType, info.Info.Architecture)
		}
		for _, digest := range inspected.RepoDigests {
			if digest == imageReference {
				platformDigest = imageReference
				break
			}
		}
		if platformDigest == "" {
			return targetIdentity{}, fmt.Errorf("docker image does not retain selected platform digest %s", imageReference)
		}
	}
	return targetIdentity{Role: role, AuthoredImage: declaration.Image, PlatformDigest: platformDigest, ExecutedImage: inspected.ID, Portable: !imagelock.IsLocalImageID(declaration.Image)}, nil
}

type manifestDocument struct {
	Manifests []struct {
		Digest   string `json:"digest"`
		Platform struct {
			OS           string `json:"os"`
			Architecture string `json:"architecture"`
		} `json:"platform"`
	} `json:"manifests"`
}

func resolvePlatformImage(ctx context.Context, docker *mobyclient.Client, authored string) (string, error) {
	repository, _, err := imagelock.ParseImmutableReference(authored)
	if err != nil {
		return "", fmt.Errorf("parse benchmark image reference: %w", err)
	}
	info, err := docker.Info(ctx, mobyclient.InfoOptions{})
	if err != nil {
		return "", fmt.Errorf("inspect Docker platform: %w", err)
	}
	stdout := newBoundedCommandBuffer(4 << 20)
	stderr := newBoundedCommandBuffer(16 << 10)
	command := exec.CommandContext(ctx, "docker", "manifest", "inspect", authored)
	command.Stdout = stdout
	command.Stderr = stderr
	if err := command.Run(); err != nil {
		return "", fmt.Errorf("inspect authored OCI manifest: %w: %s", err, strings.TrimSpace(stderr.String()))
	}
	var manifest manifestDocument
	if err := json.Unmarshal(stdout.Bytes(), &manifest); err != nil {
		return "", fmt.Errorf("decode authored OCI manifest: %w", err)
	}
	if len(manifest.Manifests) == 0 {
		return authored, nil
	}
	wantedOS := strings.ToLower(info.Info.OSType)
	wantedArchitecture := normalizeBenchmarkArchitecture(info.Info.Architecture)
	selected := ""
	for _, child := range manifest.Manifests {
		if strings.ToLower(child.Platform.OS) != wantedOS || normalizeBenchmarkArchitecture(child.Platform.Architecture) != wantedArchitecture {
			continue
		}
		if selected != "" {
			return "", fmt.Errorf("OCI index has duplicate runtime descriptors for %s/%s", wantedOS, wantedArchitecture)
		}
		candidate := repository + "@" + child.Digest
		if _, _, err := imagelock.ParseImmutableReference(candidate); err != nil {
			return "", fmt.Errorf("OCI index selected an invalid child digest: %w", err)
		}
		selected = candidate
	}
	if selected == "" {
		return "", fmt.Errorf("OCI index has no runtime descriptor for %s/%s", wantedOS, wantedArchitecture)
	}
	return selected, nil
}

func normalizeBenchmarkArchitecture(value string) string {
	switch strings.ToLower(value) {
	case "aarch64":
		return "arm64"
	case "x86_64":
		return "amd64"
	default:
		return strings.ToLower(value)
	}
}

type boundedCommandBuffer struct {
	buffer    bytes.Buffer
	remaining int
}

func newBoundedCommandBuffer(limit int) *boundedCommandBuffer {
	return &boundedCommandBuffer{remaining: limit}
}

func (buffer *boundedCommandBuffer) Write(document []byte) (int, error) {
	if len(document) > buffer.remaining {
		_, _ = buffer.buffer.Write(document[:buffer.remaining])
		buffer.remaining = 0
		return len(document), fmt.Errorf("command output exceeds configured limit")
	}
	buffer.remaining -= len(document)
	return buffer.buffer.Write(document)
}

func (buffer *boundedCommandBuffer) Bytes() []byte  { return buffer.buffer.Bytes() }
func (buffer *boundedCommandBuffer) String() string { return buffer.buffer.String() }

func startBenchmarkService(ctx context.Context, environment *benchmarkEnvironment, role string, round int, declaration spec.Service, identity targetIdentity) (*benchmarkService, instrumentationEvidence, error) {
	port := strconv.Itoa(declaration.Health.Port) + "/tcp"
	uid, gid := os.Getuid(), os.Getgid()
	if uid == 0 {
		uid, gid = 65532, 65532
	}
	options := []testcontainers.ContainerCustomizer{
		tcnetwork.WithNetwork([]string{declaration.Name}, environment.Network),
		testcontainers.WithLabels(map[string]string{benchmarkRunLabel: environment.RunID, "dev.chronicle.benchmark.role": role, "dev.chronicle.benchmark.round": strconv.Itoa(round)}),
		testcontainers.WithEnv(declaration.Environment),
		testcontainers.WithExposedPorts(port),
		testcontainers.WithWaitStrategy(wait.ForHTTP(declaration.Health.Path).WithPort(port).WithPollInterval(declaration.Health.Interval.Duration).WithStartupTimeout(declaration.Health.Timeout.Duration)),
		testcontainers.WithConfigModifier(func(configuration *container.Config) {
			configuration.User = fmt.Sprintf("%d:%d", uid, gid)
			configuration.Healthcheck = &container.HealthConfig{Test: []string{"NONE"}}
		}),
		testcontainers.WithHostConfigModifier(func(host *container.HostConfig) {
			host.ReadonlyRootfs = true
			host.NanoCPUs = int64(declaration.Resources.CPUs * 1_000_000_000)
			host.Memory = declaration.Resources.MemoryBytes
			pids := int64(declaration.Resources.PIDs)
			host.PidsLimit = &pids
			host.CapDrop = []string{"ALL"}
			host.SecurityOpt = []string{"no-new-privileges"}
			host.PortBindings = mobynetwork.PortMap{
				mobynetwork.MustParsePort(port): []mobynetwork.PortBinding{{HostIP: netip.MustParseAddr("127.0.0.1")}},
			}
		}),
	}
	if len(declaration.Command) != 0 {
		options = append(options, testcontainers.WithEntrypoint(declaration.Command...))
	}
	if len(declaration.Args) != 0 {
		options = append(options, testcontainers.WithCmd(declaration.Args...))
	}
	started, err := testcontainers.Run(ctx, declaration.Image, options...)
	if err != nil {
		return nil, instrumentationEvidence{}, fmt.Errorf("start %s benchmark service: %w", role, err)
	}
	service := &benchmarkService{container: started, identity: identity, network: environment.NetworkName, healthPort: declaration.Health.Port}
	fail := func(cause error) (*benchmarkService, instrumentationEvidence, error) {
		cleanupContext, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cancel()
		return nil, instrumentationEvidence{}, errors.Join(cause, service.terminate(cleanupContext))
	}
	evidence, err := service.inspect(ctx, declaration)
	if err != nil {
		return fail(err)
	}
	mapped, err := started.MappedPort(ctx, port)
	if err != nil {
		return fail(fmt.Errorf("resolve benchmark endpoint: %w", err))
	}
	endpoint := "http://127.0.0.1:" + mapped.Port()
	parsed, err := url.Parse(endpoint)
	if err != nil || parsed.Scheme != "http" || parsed.Hostname() != "127.0.0.1" || parsed.User != nil || parsed.RawQuery != "" || parsed.Fragment != "" {
		return fail(fmt.Errorf("benchmark endpoint %q is not an exact loopback HTTP endpoint", endpoint))
	}
	service.endpoint = strings.TrimSuffix(endpoint, "/")
	return service, evidence, nil
}

func (service *benchmarkService) inspect(ctx context.Context, declaration spec.Service) (instrumentationEvidence, error) {
	inspected, err := service.container.Inspect(ctx)
	if err != nil {
		return instrumentationEvidence{}, fmt.Errorf("inspect benchmark service: %w", err)
	}
	if inspected.Config == nil || inspected.HostConfig == nil || inspected.NetworkSettings == nil || inspected.State == nil {
		return instrumentationEvidence{}, fmt.Errorf("benchmark inspection is incomplete")
	}
	if inspected.Image != service.identity.ExecutedImage {
		return instrumentationEvidence{}, fmt.Errorf("benchmark executed image %s, want %s", inspected.Image, service.identity.ExecutedImage)
	}
	if !inspected.State.Running || inspected.State.Restarting || inspected.State.OOMKilled || inspected.RestartCount != 0 {
		return instrumentationEvidence{}, fmt.Errorf("benchmark service state is not measurement-safe")
	}
	if len(inspected.NetworkSettings.Networks) != 1 || inspected.NetworkSettings.Networks[service.network] == nil {
		return instrumentationEvidence{}, fmt.Errorf("benchmark service is not attached exclusively to network %q", service.network)
	}
	healthDisabled := inspected.Config.Healthcheck != nil && slices.Equal(inspected.Config.Healthcheck.Test, []string{"NONE"})
	if !healthDisabled {
		return instrumentationEvidence{}, fmt.Errorf("image-authored Docker healthcheck is not disabled")
	}
	host := inspected.HostConfig
	if inspected.Config.User == "" || strings.HasPrefix(inspected.Config.User, "0:") || inspected.Config.User == "0" || !host.ReadonlyRootfs || host.NanoCPUs <= 0 || host.Memory <= 0 || host.PidsLimit == nil || *host.PidsLimit <= 0 {
		return instrumentationEvidence{}, fmt.Errorf("benchmark hardening or resource limits are incomplete")
	}
	if len(host.CapDrop) != 1 || strings.ToUpper(host.CapDrop[0]) != "ALL" || !slices.Contains(host.SecurityOpt, "no-new-privileges") {
		return instrumentationEvidence{}, fmt.Errorf("benchmark capability hardening is incomplete")
	}
	if len(host.Mounts) != 0 || len(host.Binds) != 0 || len(inspected.Mounts) != 0 {
		return instrumentationEvidence{}, fmt.Errorf("benchmark service unexpectedly receives a mount")
	}
	bindings := host.PortBindings[mobynetwork.MustParsePort(strconv.Itoa(declaration.Health.Port)+"/tcp")]
	if len(host.PortBindings) != 1 || len(bindings) != 1 || bindings[0].HostIP != netip.MustParseAddr("127.0.0.1") {
		return instrumentationEvidence{}, fmt.Errorf("benchmark service is not bound exclusively to one loopback port")
	}
	telemetryAbsent := true
	correctnessAbsent := true
	for _, assignment := range inspected.Config.Env {
		name := strings.ToUpper(strings.SplitN(assignment, "=", 2)[0])
		if strings.HasPrefix(name, "OTEL_") || strings.Contains(name, "DEBUG") || strings.Contains(name, "PROFILE") {
			telemetryAbsent = false
		}
		if strings.HasPrefix(name, "CHRONICLE_") {
			correctnessAbsent = false
		}
	}
	if !telemetryAbsent || !correctnessAbsent || declaration.Probe.Enabled {
		return instrumentationEvidence{}, fmt.Errorf("benchmark measured path contains forbidden instrumentation")
	}
	reference := inspected.Config.Labels["dev.chronicle.benchmark.reference"] == "stdlib-only-v1"
	service.reference = reference
	return instrumentationEvidence{
		HarnessCorrectnessInstrumentationAbsent: true,
		ProbeDisabled:                           true,
		TelemetryEnvironmentAbsent:              true,
		SecretMountsAbsent:                      true,
		DockerHealthcheckDisabled:               true,
		AutomaticCompressionDisabled:            true,
		TargetBinaryInternalsProven:             reference,
		Claim:                                   "Container inspection proves harness-side correctness instrumentation is absent; target-binary internals are proven only for the repository stdlib-only reference image.",
	}, nil
}

func (service *benchmarkService) health(ctx context.Context, path string) error {
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, service.endpoint+path, nil)
	if err != nil {
		return err
	}
	client := &http.Client{
		Timeout: 2 * time.Second,
		CheckRedirect: func(*http.Request, []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}
	response, err := client.Do(request)
	if err != nil {
		return err
	}
	defer func() { _ = response.Body.Close() }()
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		return fmt.Errorf("benchmark health returned %s", response.Status)
	}
	return nil
}

func (service *benchmarkService) terminate(ctx context.Context) error {
	if service == nil || service.container == nil {
		return nil
	}
	err := testcontainers.TerminateContainer(service.container, testcontainers.StopContext(ctx))
	service.container = nil
	if err != nil {
		return fmt.Errorf("terminate benchmark service: %w", err)
	}
	return nil
}
