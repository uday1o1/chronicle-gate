package runtime

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/netip"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/moby/moby/api/types/container"
	"github.com/moby/moby/api/types/mount"
	mobynetwork "github.com/moby/moby/api/types/network"
	mobyclient "github.com/moby/moby/client"
	"github.com/testcontainers/testcontainers-go"
	tcnetwork "github.com/testcontainers/testcontainers-go/network"
	"github.com/testcontainers/testcontainers-go/wait"
	"github.com/uday1o1/chronicle-gate/internal/spec"
)

// ServiceRuntime is one target service container and its verified image identity.
type ServiceRuntime struct {
	Container       testcontainers.Container
	ImageID         string
	ClientID        string
	healthSpec      spec.Health
	secretPaths     []string
	secretDirectory string
}

// SecretMount is one runtime-owned secret delivered through a private file.
type SecretMount struct {
	Environment string
	Filename    string
	Value       string
}

type ServiceConfig struct {
	RunID           string
	AttemptID       string
	Service         spec.Service
	Network         *testcontainers.DockerNetwork
	DatabaseDSN     string
	InternalBroker  string
	TopicPrefix     string
	GroupPrefix     string
	SecretDirectory string
	Environment     map[string]string
	Secrets         []SecretMount
	ExposedPorts    []int
}

func StartService(ctx context.Context, config ServiceConfig) (*ServiceRuntime, error) {
	environment := make(map[string]string, len(config.Service.Environment)+len(config.Environment)+8)
	for key, value := range config.Service.Environment {
		environment[key] = value
	}
	for key, value := range config.Environment {
		environment[key] = value
	}
	clientID := "chronicle-" + config.AttemptID
	environment["CHRONICLE_BROKERS"] = config.InternalBroker
	environment["CHRONICLE_TOPIC_PREFIX"] = config.TopicPrefix
	environment["CHRONICLE_GROUP_PREFIX"] = config.GroupPrefix
	environment["CHRONICLE_RUN_ID"] = config.RunID
	environment["CHRONICLE_ATTEMPT_ID"] = config.AttemptID
	secretDirectory, err := filepath.Abs(config.SecretDirectory)
	if err != nil {
		return nil, fmt.Errorf("resolve target secret directory: %w", err)
	}
	config.SecretDirectory = secretDirectory
	if err := requireSupportedBindOwnership(ctx, len(config.Secrets) != 0); err != nil {
		return nil, err
	}
	runtimeUID, runtimeGID, err := prepareSecretDirectory(config.SecretDirectory)
	if err != nil {
		return nil, err
	}
	secretPaths := []string{}
	allSecrets := append([]SecretMount{{Environment: "CHRONICLE_DATABASE_DSN_FILE", Filename: "database-dsn", Value: config.DatabaseDSN}}, config.Secrets...)
	for _, secret := range allSecrets {
		if secret.Environment == "" || secret.Filename == "" || strings.ContainsAny(secret.Filename, `/\\`) || secret.Value == "" {
			cleanupSecretDirectory(config.SecretDirectory, secretPaths)
			return nil, errors.New("runtime secret mount is incomplete")
		}
		path, writeErr := writeSecret(config.SecretDirectory, secret.Filename, secret.Value, runtimeUID, runtimeGID)
		if writeErr != nil {
			cleanupSecretDirectory(config.SecretDirectory, secretPaths)
			return nil, writeErr
		}
		secretPaths = append(secretPaths, path)
		environment[secret.Environment] = "/run/chronicle/" + secret.Filename
	}
	port := strconv.Itoa(config.Service.Health.Port) + "/tcp"
	exposedPorts := []string{port}
	for _, exposed := range config.ExposedPorts {
		candidate := strconv.Itoa(exposed) + "/tcp"
		if candidate != port {
			exposedPorts = append(exposedPorts, candidate)
		}
	}
	health := wait.ForHTTP(config.Service.Health.Path).
		WithPort(port).
		WithPollInterval(config.Service.Health.Interval.Duration).
		WithStartupTimeout(config.Service.Health.Timeout.Duration)
	options := []testcontainers.ContainerCustomizer{
		tcnetwork.WithNetwork([]string{config.Service.Name}, config.Network),
		testcontainers.WithLabels(map[string]string{labelRun: config.RunID, "dev.chronicle.attempt": config.AttemptID}),
		testcontainers.WithEnv(environment),
		testcontainers.WithExposedPorts(exposedPorts...),
		testcontainers.WithWaitStrategy(health),
		testcontainers.WithConfigModifier(func(containerConfig *container.Config) {
			containerConfig.User = fmt.Sprintf("%d:%d", runtimeUID, runtimeGID)
		}),
		testcontainers.WithHostConfigModifier(func(host *container.HostConfig) {
			host.ReadonlyRootfs = true
			host.Mounts = append(host.Mounts, mount.Mount{Type: mount.TypeBind, Source: config.SecretDirectory, Target: "/run/chronicle", ReadOnly: true})
			host.NanoCPUs = int64(config.Service.Resources.CPUs * 1_000_000_000)
			host.Memory = config.Service.Resources.MemoryBytes
			pids := int64(config.Service.Resources.PIDs)
			host.PidsLimit = &pids
			host.CapDrop = []string{"ALL"}
			host.SecurityOpt = []string{"no-new-privileges"}
			if host.PortBindings == nil {
				host.PortBindings = mobynetwork.PortMap{}
			}
			for _, exposed := range exposedPorts {
				host.PortBindings[mobynetwork.MustParsePort(exposed)] = []mobynetwork.PortBinding{{HostIP: netip.MustParseAddr("127.0.0.1")}}
			}
		}),
	}
	if len(config.Service.Command) > 0 {
		options = append(options, testcontainers.WithEntrypoint(config.Service.Command...))
	}
	if len(config.Service.Args) > 0 {
		options = append(options, testcontainers.WithCmd(config.Service.Args...))
	}
	serviceContainer, err := testcontainers.Run(ctx, config.Service.Image, options...)
	if err != nil {
		cleanupSecretDirectory(config.SecretDirectory, secretPaths)
		return nil, fmt.Errorf("start target service %q: %w", config.Service.Name, err)
	}
	runtime := &ServiceRuntime{Container: serviceContainer, ClientID: clientID, healthSpec: config.Service.Health, secretPaths: secretPaths, secretDirectory: config.SecretDirectory}
	inspect, err := serviceContainer.Inspect(ctx)
	if err != nil {
		_ = runtime.Terminate(context.Background())
		return nil, fmt.Errorf("inspect target service: %w", err)
	}
	runtime.ImageID = inspect.Image
	if config.Service.Image != runtime.ImageID && len(config.Service.Image) == len("sha256:")+64 {
		_ = runtime.Terminate(context.Background())
		return nil, fmt.Errorf("local target image identity mismatch: authored %s, executed %s", config.Service.Image, runtime.ImageID)
	}
	return runtime, nil
}

// Kill sends SIGKILL to the service process and preserves the container for restart.
func (service *ServiceRuntime) Kill(ctx context.Context) error {
	client, err := mobyclient.New(mobyclient.FromEnv)
	if err != nil {
		return fmt.Errorf("create Docker client for SIGKILL: %w", err)
	}
	defer func() { _ = client.Close() }()
	if _, err := client.ContainerKill(ctx, service.Container.GetContainerID(), mobyclient.ContainerKillOptions{}); err != nil {
		return fmt.Errorf("SIGKILL target service: %w", err)
	}
	return nil
}

// PortEndpoint resolves a loopback-only HTTP endpoint for an exposed service port.
func (service *ServiceRuntime) PortEndpoint(ctx context.Context, port int) (string, error) {
	endpoint, err := service.Container.PortEndpoint(ctx, strconv.Itoa(port)+"/tcp", "http")
	if err != nil {
		return "", fmt.Errorf("resolve service port %d: %w", port, err)
	}
	return endpoint, nil
}

// AssertHardened verifies the security controls applied to a trusted service container.
func (service *ServiceRuntime) AssertHardened(ctx context.Context, expectedPorts ...int) error {
	inspect, err := service.Container.Inspect(ctx)
	if err != nil {
		return fmt.Errorf("inspect hardened service: %w", err)
	}
	if inspect.Config == nil || inspect.HostConfig == nil {
		return errors.New("service inspection omitted container configuration")
	}
	if inspect.Config.User == "" || strings.HasPrefix(inspect.Config.User, "0:") || inspect.Config.User == "0" {
		return fmt.Errorf("service user %q is not a fixed non-root identity", inspect.Config.User)
	}
	host := inspect.HostConfig
	if !host.ReadonlyRootfs || host.NanoCPUs <= 0 || host.Memory <= 0 || host.PidsLimit == nil || *host.PidsLimit <= 0 {
		return errors.New("service resource or read-only-root security controls are incomplete")
	}
	if len(host.CapDrop) != 1 || host.CapDrop[0] != "ALL" || !slices.Contains(host.SecurityOpt, "no-new-privileges") {
		return errors.New("service capability or privilege controls are incomplete")
	}
	for _, configuredMount := range host.Mounts {
		if configuredMount.Target == "/var/run/docker.sock" || configuredMount.Source == "/var/run/docker.sock" {
			return errors.New("service unexpectedly receives the Docker socket")
		}
	}
	if len(host.Mounts) != 1 || host.Mounts[0].Target != "/run/chronicle" || !host.Mounts[0].ReadOnly {
		return errors.New("service must receive exactly one read-only runtime secret mount")
	}
	loopback := netip.MustParseAddr("127.0.0.1")
	for _, port := range expectedPorts {
		bindings := host.PortBindings[mobynetwork.MustParsePort(strconv.Itoa(port)+"/tcp")]
		if len(bindings) != 1 || bindings[0].HostIP != loopback {
			return fmt.Errorf("service port %d is not bound only to loopback", port)
		}
	}
	return nil
}

// RecentLogs returns bounded service logs for infrastructure diagnostics.
func (service *ServiceRuntime) RecentLogs(ctx context.Context) string {
	if service == nil || service.Container == nil {
		return ""
	}
	reader, err := service.Container.Logs(ctx)
	if err != nil {
		return ""
	}
	defer func() { _ = reader.Close() }()
	document, err := io.ReadAll(io.LimitReader(reader, 64<<10))
	if err != nil {
		return ""
	}
	return strings.TrimSpace(string(document))
}

// Diagnostics returns bounded non-secret process and port state.
func (service *ServiceRuntime) Diagnostics(ctx context.Context) string {
	if service == nil || service.Container == nil {
		return "container unavailable"
	}
	inspect, err := service.Container.Inspect(ctx)
	if err != nil {
		return "inspect failed: " + err.Error()
	}
	running, exitCode, stateError := false, 0, ""
	if inspect.State != nil {
		running, exitCode, stateError = inspect.State.Running, inspect.State.ExitCode, inspect.State.Error
	}
	return fmt.Sprintf("running=%t exit=%d stateError=%q ports=%v", running, exitCode, stateError, inspect.NetworkSettings.Ports)
}

func (service *ServiceRuntime) Stop(ctx context.Context) error {
	timeout := 10 * time.Second
	if err := service.Container.Stop(ctx, &timeout); err != nil {
		return fmt.Errorf("stop target service: %w", err)
	}
	return nil
}

func (service *ServiceRuntime) Start(ctx context.Context) error {
	client, err := mobyclient.New(mobyclient.FromEnv)
	if err != nil {
		return fmt.Errorf("create Docker client for restart: %w", err)
	}
	defer func() { _ = client.Close() }()
	if _, err := client.ContainerStart(ctx, service.Container.GetContainerID(), mobyclient.ContainerStartOptions{}); err != nil {
		return fmt.Errorf("restart target service: %w", err)
	}
	if err := service.waitForRestartHealth(ctx); err != nil {
		return fmt.Errorf("wait for restarted target service: %w", err)
	}
	return nil
}

func (service *ServiceRuntime) waitForRestartHealth(ctx context.Context) error {
	waitContext, cancel := context.WithTimeout(ctx, service.healthSpec.Timeout.Duration)
	defer cancel()
	endpoint, err := service.PortEndpoint(waitContext, service.healthSpec.Port)
	if err != nil {
		return err
	}
	client := &http.Client{Timeout: time.Second}
	ticker := time.NewTicker(service.healthSpec.Interval.Duration)
	defer ticker.Stop()
	for {
		inspect, inspectErr := service.Container.Inspect(waitContext)
		if inspectErr != nil {
			return fmt.Errorf("inspect restarted service: %w", inspectErr)
		}
		if inspect.State == nil || !inspect.State.Running {
			exitCode := 0
			stateError := ""
			if inspect.State != nil {
				exitCode = inspect.State.ExitCode
				stateError = inspect.State.Error
			}
			return fmt.Errorf("restarted service exited before health: exit=%d error=%q logs=%q", exitCode, stateError, service.RecentLogs(context.Background()))
		}
		request, requestErr := http.NewRequestWithContext(waitContext, http.MethodGet, endpoint+service.healthSpec.Path, nil)
		if requestErr != nil {
			return requestErr
		}
		response, requestErr := client.Do(request)
		if requestErr == nil {
			_ = response.Body.Close()
			if response.StatusCode >= 200 && response.StatusCode < 300 {
				return nil
			}
		}
		select {
		case <-waitContext.Done():
			return waitContext.Err()
		case <-ticker.C:
		}
	}
}

func (service *ServiceRuntime) Terminate(ctx context.Context) error {
	if service == nil {
		return nil
	}
	var cleanupErr error
	if service.Container != nil {
		if err := testcontainers.TerminateContainer(service.Container, testcontainers.StopContext(ctx)); err != nil {
			cleanupErr = fmt.Errorf("terminate target service: %w", err)
		}
		service.Container = nil
	}
	for _, path := range service.secretPaths {
		if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
			cleanupErr = errors.Join(cleanupErr, fmt.Errorf("remove target secret file: %w", err))
		}
	}
	service.secretPaths = nil
	if err := os.Remove(service.secretDirectory); err != nil && !os.IsNotExist(err) {
		cleanupErr = errors.Join(cleanupErr, fmt.Errorf("remove target secret directory: %w", err))
	}
	service.secretDirectory = ""
	return cleanupErr
}

func prepareSecretDirectory(directory string) (int, int, error) {
	uid, gid := os.Getuid(), os.Getgid()
	if uid == 0 {
		uid, gid = 65532, 65532
	}
	if err := os.MkdirAll(directory, 0o700); err != nil {
		return 0, 0, fmt.Errorf("create target secret directory: %w", err)
	}
	if err := os.Chmod(directory, 0o700); err != nil {
		return 0, 0, fmt.Errorf("secure target secret directory: %w", err)
	}
	if os.Getuid() == 0 {
		if err := os.Chown(directory, uid, gid); err != nil {
			return 0, 0, fmt.Errorf("assign target secret directory owner: %w", err)
		}
	}
	return uid, gid, nil
}

func writeSecret(directory, filename, value string, uid, gid int) (string, error) {
	directory, err := filepath.Abs(directory)
	if err != nil {
		return "", fmt.Errorf("resolve target secret directory: %w", err)
	}
	path := filepath.Join(directory, filename)
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return "", fmt.Errorf("create target secret file: %w", err)
	}
	failed := true
	defer func() {
		_ = file.Close()
		if failed {
			_ = os.Remove(path)
		}
	}()
	if os.Getuid() == 0 {
		if err := file.Chown(uid, gid); err != nil {
			return "", fmt.Errorf("assign target secret file owner: %w", err)
		}
	}
	if err := file.Chmod(0o600); err != nil {
		return "", fmt.Errorf("secure target secret file: %w", err)
	}
	if _, err := file.WriteString(value); err != nil {
		return "", fmt.Errorf("write target secret file: %w", err)
	}
	if err := file.Sync(); err != nil {
		return "", fmt.Errorf("sync target secret file: %w", err)
	}
	if err := file.Close(); err != nil {
		return "", fmt.Errorf("close target secret file: %w", err)
	}
	failed = false
	return path, nil
}

func cleanupSecretDirectory(directory string, paths []string) {
	for _, path := range paths {
		_ = os.Remove(path)
	}
	_ = os.Remove(directory)
}

func requireSupportedBindOwnership(ctx context.Context, strict bool) error {
	if !strict {
		return nil
	}
	binary, err := exec.LookPath("docker")
	if err != nil {
		return fmt.Errorf("inspect Docker bind ownership support: %w", err)
	}
	command := exec.CommandContext(ctx, binary, "info", "--format", "{{json .SecurityOptions}}")
	document, err := command.Output()
	if err != nil {
		return fmt.Errorf("inspect Docker bind ownership support: %w", err)
	}
	security := strings.ToLower(string(document))
	if strings.Contains(security, "userns") || strings.Contains(security, "rootless") {
		return errors.New("precise probes do not support Docker rootless or user-namespace-remapped bind ownership")
	}
	return nil
}
