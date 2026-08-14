package runtime

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"errors"
	"fmt"
	"net/netip"
	"os"
	"os/exec"
	goruntime "runtime"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/containerd/errdefs"
	"github.com/moby/moby/api/types/container"
	"github.com/moby/moby/api/types/mount"
	mobynetwork "github.com/moby/moby/api/types/network"
	mobyclient "github.com/moby/moby/client"
	"github.com/testcontainers/testcontainers-go"
	tclog "github.com/testcontainers/testcontainers-go/log"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/modules/redpanda"
	tcnetwork "github.com/testcontainers/testcontainers-go/network"
	"github.com/testcontainers/testcontainers-go/wait"
	"github.com/uday1o1/chronicle-gate/internal/imagelock"
)

const labelRun = "dev.chronicle.run"

// CleanupError marks a failed exact-scope cleanup so it outranks timeout or interruption.
type CleanupError struct {
	Err error
}

func (err *CleanupError) Error() string {
	return err.Err.Error()
}

func (err *CleanupError) Unwrap() error {
	return err.Err
}

// Environment contains the shared broker and database for one run.
type Environment struct {
	RunID                  string
	Network                *testcontainers.DockerNetwork
	Redpanda               *redpanda.Container
	Postgres               *postgres.PostgresContainer
	HostBroker             string
	InternalBroker         string
	HostSchemaRegistry     string
	InternalSchemaRegistry string
	HostPostgresDSN        string
	InternalPostgres       string
	PostgresAdminUser      string
	PostgresAdminPassword  string
	NetworkName            string
	OwnedVolumes           []string
	cleanupMutex           sync.Mutex
	cleaned                bool
}

func StartEnvironment(ctx context.Context, runID, lockPath string) (_ *Environment, resultErr error) {
	tclog.SetDefault(tclog.NewNoopLogger())
	if err := os.Setenv("TESTCONTAINERS_RYUK_DISABLED", "true"); err != nil {
		return nil, fmt.Errorf("disable mutable Testcontainers reaper image: %w", err)
	}
	if err := configureDockerHost(ctx); err != nil {
		return nil, err
	}
	lock, err := imagelock.Load(lockPath)
	if err != nil {
		return nil, err
	}
	redpandaLock, err := lockedImage(lock, "redpanda")
	if err != nil {
		return nil, err
	}
	postgresLock, err := lockedImage(lock, "postgresql")
	if err != nil {
		return nil, err
	}
	platform := "linux/" + normalizeRuntimeArchitecture(goruntime.GOARCH)
	redpandaCaps, redpandaOK := redpandaLock.Hardening.CapAdd[platform]
	postgresCaps, postgresOK := postgresLock.Hardening.CapAdd[platform]
	if !redpandaOK || !postgresOK {
		return nil, fmt.Errorf("infrastructure hardening lock has no policy for %s", platform)
	}
	password, err := runtimeSecret()
	if err != nil {
		return nil, err
	}
	environment := &Environment{
		RunID:                  runID,
		InternalBroker:         "redpanda:9094",
		InternalSchemaRegistry: "http://redpanda:8081",
		InternalPostgres:       "postgres:5432",
		PostgresAdminUser:      "chronicle_admin",
		PostgresAdminPassword:  password,
	}
	defer func() {
		if resultErr != nil {
			cleanupContext, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			if cleanupErr := environment.Cleanup(cleanupContext); cleanupErr != nil {
				resultErr = errors.Join(resultErr, &CleanupError{Err: cleanupErr})
			}
		}
	}()
	labels := map[string]string{labelRun: runID}
	environment.Network, err = tcnetwork.New(ctx, tcnetwork.WithLabels(labels))
	if err != nil {
		return nil, fmt.Errorf("create run network: %w", err)
	}
	environment.NetworkName = environment.Network.Name

	environment.Redpanda, err = redpanda.Run(ctx, redpandaLock.Reference,
		tcnetwork.WithNetwork(nil, environment.Network),
		redpanda.WithListener(environment.InternalBroker),
		testcontainers.WithLabels(labels),
		testcontainers.WithHostConfigModifier(func(config *container.HostConfig) {
			config.NanoCPUs = 1_000_000_000
			config.Memory = 1536 << 20
			pids := int64(512)
			config.PidsLimit = &pids
			config.CapDrop = append([]string(nil), redpandaLock.Hardening.CapDrop...)
			config.CapAdd = append([]string(nil), redpandaCaps...)
			config.SecurityOpt = []string{"no-new-privileges"}
			bindLoopback(config, "8081/tcp", "8082/tcp", "9092/tcp", "9644/tcp")
		}),
	)
	if err != nil {
		return nil, fmt.Errorf("start Redpanda: %w", err)
	}
	if err := environment.recordOwnedVolumes(ctx, environment.Redpanda); err != nil {
		return nil, err
	}
	environment.HostBroker, err = environment.Redpanda.KafkaSeedBroker(ctx)
	if err != nil {
		return nil, fmt.Errorf("resolve host Redpanda endpoint: %w", err)
	}
	environment.HostSchemaRegistry, err = environment.Redpanda.SchemaRegistryAddress(ctx)
	if err != nil {
		return nil, fmt.Errorf("resolve host Redpanda Schema Registry endpoint: %w", err)
	}
	if err := assertInfrastructureHardened(ctx, environment.Redpanda, infrastructurePolicy{Name: "Redpanda", CapAdd: redpandaCaps}); err != nil {
		return nil, err
	}

	environment.Postgres, err = postgres.Run(ctx, postgresLock.Reference,
		postgres.WithDatabase("postgres"),
		postgres.WithUsername(environment.PostgresAdminUser),
		postgres.WithPassword(environment.PostgresAdminPassword),
		testcontainers.WithWaitStrategy(wait.ForLog("database system is ready to accept connections").WithOccurrence(2)),
		tcnetwork.WithNetwork([]string{"postgres"}, environment.Network),
		testcontainers.WithLabels(labels),
		testcontainers.WithHostConfigModifier(func(config *container.HostConfig) {
			config.NanoCPUs = 1_000_000_000
			config.Memory = 768 << 20
			pids := int64(256)
			config.PidsLimit = &pids
			config.CapDrop = append([]string(nil), postgresLock.Hardening.CapDrop...)
			config.CapAdd = append([]string(nil), postgresCaps...)
			config.SecurityOpt = []string{"no-new-privileges"}
			bindLoopback(config, "5432/tcp")
		}),
	)
	if err != nil {
		return nil, fmt.Errorf("start PostgreSQL: %w", err)
	}
	if err := environment.recordOwnedVolumes(ctx, environment.Postgres); err != nil {
		return nil, err
	}
	environment.HostPostgresDSN, err = environment.Postgres.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		return nil, fmt.Errorf("resolve host PostgreSQL endpoint: %w", err)
	}
	if err := assertInfrastructureHardened(ctx, environment.Postgres, infrastructurePolicy{
		Name: "PostgreSQL", CapAdd: postgresCaps,
	}); err != nil {
		return nil, err
	}
	return environment, nil
}

type infrastructurePolicy struct {
	Name   string
	CapAdd []string
}

func bindLoopback(host *container.HostConfig, ports ...string) {
	if host.PortBindings == nil {
		host.PortBindings = mobynetwork.PortMap{}
	}
	loopback := netip.MustParseAddr("127.0.0.1")
	for _, port := range ports {
		host.PortBindings[mobynetwork.MustParsePort(port)] = []mobynetwork.PortBinding{{HostIP: loopback}}
	}
}

func assertInfrastructureHardened(ctx context.Context, service testcontainers.Container, policy infrastructurePolicy) error {
	inspect, err := service.Inspect(ctx)
	if err != nil {
		return fmt.Errorf("inspect hardened %s container: %w", policy.Name, err)
	}
	if inspect.HostConfig == nil {
		return fmt.Errorf("%s inspection omitted host configuration", policy.Name)
	}
	return validateInfrastructureHostConfig(inspect.HostConfig, policy)
}

func validateInfrastructureHostConfig(host *container.HostConfig, policy infrastructurePolicy) error {
	if host.NanoCPUs <= 0 || host.Memory <= 0 || host.PidsLimit == nil || *host.PidsLimit <= 0 {
		return fmt.Errorf("%s resource limits are incomplete", policy.Name)
	}
	if len(host.CapDrop) != 1 || strings.ToUpper(host.CapDrop[0]) != "ALL" || !slices.Contains(host.SecurityOpt, "no-new-privileges") {
		return fmt.Errorf("%s capability or privilege controls are incomplete", policy.Name)
	}
	actualCaps := append([]string(nil), host.CapAdd...)
	wantCaps := append([]string(nil), policy.CapAdd...)
	for index := range actualCaps {
		actualCaps[index] = normalizeCapability(actualCaps[index])
	}
	for index := range wantCaps {
		wantCaps[index] = normalizeCapability(wantCaps[index])
	}
	slices.Sort(actualCaps)
	slices.Sort(wantCaps)
	if !slices.Equal(actualCaps, wantCaps) {
		return fmt.Errorf("%s capability allowlist is %v, want %v", policy.Name, actualCaps, wantCaps)
	}
	loopback := netip.MustParseAddr("127.0.0.1")
	for port, bindings := range host.PortBindings {
		for _, binding := range bindings {
			if binding.HostIP != loopback {
				return fmt.Errorf("%s port %s is not bound only to loopback", policy.Name, port)
			}
		}
	}
	for _, configuredMount := range host.Mounts {
		if configuredMount.Target == "/var/run/docker.sock" || configuredMount.Source == "/var/run/docker.sock" {
			return fmt.Errorf("%s unexpectedly receives the Docker socket", policy.Name)
		}
	}
	return nil
}

func normalizeCapability(capability string) string {
	return strings.TrimPrefix(strings.ToUpper(capability), "CAP_")
}

func configureDockerHost(ctx context.Context) error {
	if os.Getenv("DOCKER_HOST") != "" {
		return nil
	}
	dockerBinary, err := exec.LookPath("docker")
	if err != nil {
		return fmt.Errorf("locate Docker CLI for active-context resolution: %w", err)
	}
	command := exec.CommandContext(ctx, dockerBinary, "context", "inspect", "--format", "{{.Endpoints.docker.Host}}")
	document, err := command.Output()
	if err != nil {
		return fmt.Errorf("resolve active Docker context: %w", err)
	}
	host := strings.TrimSpace(string(document))
	if !strings.HasPrefix(host, "unix://") && !strings.HasPrefix(host, "tcp://") && !strings.HasPrefix(host, "http://") && !strings.HasPrefix(host, "https://") {
		return fmt.Errorf("active Docker context returned unsupported endpoint %q", host)
	}
	if err := os.Setenv("DOCKER_HOST", host); err != nil {
		return fmt.Errorf("configure Docker endpoint: %w", err)
	}
	if strings.Contains(host, "/.colima/") && os.Getenv("TESTCONTAINERS_DOCKER_SOCKET_OVERRIDE") == "" {
		if err := os.Setenv("TESTCONTAINERS_DOCKER_SOCKET_OVERRIDE", "/var/run/docker.sock"); err != nil {
			return fmt.Errorf("configure Colima socket override: %w", err)
		}
	}
	return nil
}

// ConfigureDockerHost resolves the active Docker CLI context for Moby API callers.
func ConfigureDockerHost(ctx context.Context) error {
	return configureDockerHost(ctx)
}

func (environment *Environment) Cleanup(ctx context.Context) error {
	environment.cleanupMutex.Lock()
	defer environment.cleanupMutex.Unlock()
	if environment.cleaned {
		return nil
	}
	var cleanupErrors []error
	if environment.Postgres != nil {
		if err := testcontainers.TerminateContainer(environment.Postgres, testcontainers.StopContext(ctx)); err != nil {
			cleanupErrors = append(cleanupErrors, fmt.Errorf("terminate PostgreSQL: %w", err))
		} else {
			environment.Postgres = nil
		}
	}
	if environment.Redpanda != nil {
		if err := testcontainers.TerminateContainer(environment.Redpanda, testcontainers.StopContext(ctx)); err != nil {
			cleanupErrors = append(cleanupErrors, fmt.Errorf("terminate Redpanda: %w", err))
		} else {
			environment.Redpanda = nil
		}
	}
	if len(environment.OwnedVolumes) != 0 {
		docker, err := mobyclient.New(mobyclient.FromEnv)
		if err != nil {
			cleanupErrors = append(cleanupErrors, fmt.Errorf("create Docker client for volume cleanup: %w", err))
		} else {
			remaining := make([]string, 0, len(environment.OwnedVolumes))
			for _, volume := range environment.OwnedVolumes {
				if _, removeErr := docker.VolumeRemove(ctx, volume, mobyclient.VolumeRemoveOptions{}); removeErr != nil && !errdefs.IsNotFound(removeErr) {
					cleanupErrors = append(cleanupErrors, fmt.Errorf("remove owned volume %q: %w", volume, removeErr))
					remaining = append(remaining, volume)
				}
			}
			environment.OwnedVolumes = remaining
			_ = docker.Close()
		}
	}
	if environment.Network != nil {
		if err := environment.Network.Remove(ctx); err != nil {
			cleanupErrors = append(cleanupErrors, fmt.Errorf("remove run network %q: %w", environment.NetworkName, err))
		} else {
			environment.Network = nil
		}
	}
	if len(cleanupErrors) == 0 {
		environment.cleaned = true
	}
	return errors.Join(cleanupErrors...)
}

func (environment *Environment) recordOwnedVolumes(ctx context.Context, service testcontainers.Container) error {
	inspect, err := service.Inspect(ctx)
	if err != nil {
		return fmt.Errorf("inspect infrastructure volumes: %w", err)
	}
	for _, mounted := range inspect.Mounts {
		if mounted.Type == mount.TypeVolume && mounted.Name != "" && !slices.Contains(environment.OwnedVolumes, mounted.Name) {
			environment.OwnedVolumes = append(environment.OwnedVolumes, mounted.Name)
		}
	}
	return nil
}

func lockedImage(lock imagelock.Lock, name string) (imagelock.Image, error) {
	for _, image := range lock.Images {
		if image.Name == name {
			return image, nil
		}
	}
	return imagelock.Image{}, fmt.Errorf("image lock has no %q entry", name)
}

func normalizeRuntimeArchitecture(architecture string) string {
	switch architecture {
	case "aarch64":
		return "arm64"
	case "x86_64":
		return "amd64"
	default:
		return architecture
	}
}

func runtimeSecret() (string, error) {
	value := make([]byte, 24)
	if _, err := rand.Read(value); err != nil {
		return "", fmt.Errorf("generate runtime secret: %w", err)
	}
	return base64.RawURLEncoding.EncodeToString(value), nil
}
