package runtime

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"time"

	"github.com/moby/moby/api/types/container"
	"github.com/moby/moby/api/types/mount"
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
	health          wait.Strategy
	secretPath      string
	secretDirectory string
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
}

func StartService(ctx context.Context, config ServiceConfig) (*ServiceRuntime, error) {
	environment := make(map[string]string, len(config.Service.Environment)+6)
	for key, value := range config.Service.Environment {
		environment[key] = value
	}
	clientID := "chronicle-" + config.AttemptID
	environment["CHRONICLE_BROKERS"] = config.InternalBroker
	environment["CHRONICLE_TOPIC_PREFIX"] = config.TopicPrefix
	environment["CHRONICLE_GROUP_PREFIX"] = config.GroupPrefix
	environment["CHRONICLE_RUN_ID"] = config.RunID
	environment["CHRONICLE_ATTEMPT_ID"] = config.AttemptID
	secretPath, err := writeSecret(config.SecretDirectory, config.DatabaseDSN)
	if err != nil {
		return nil, err
	}
	environment["CHRONICLE_DATABASE_DSN_FILE"] = "/database-dsn"
	port := strconv.Itoa(config.Service.Health.Port) + "/tcp"
	health := wait.ForHTTP(config.Service.Health.Path).
		WithPort(port).
		WithPollInterval(config.Service.Health.Interval.Duration).
		WithStartupTimeout(config.Service.Health.Timeout.Duration)
	options := []testcontainers.ContainerCustomizer{
		tcnetwork.WithNetwork([]string{config.Service.Name}, config.Network),
		testcontainers.WithLabels(map[string]string{labelRun: config.RunID, "dev.chronicle.attempt": config.AttemptID}),
		testcontainers.WithEnv(environment),
		testcontainers.WithExposedPorts(port),
		testcontainers.WithWaitStrategy(health),
		testcontainers.WithHostConfigModifier(func(host *container.HostConfig) {
			host.ReadonlyRootfs = true
			host.Mounts = append(host.Mounts, mount.Mount{Type: mount.TypeBind, Source: secretPath, Target: "/database-dsn", ReadOnly: true})
			host.NanoCPUs = int64(config.Service.Resources.CPUs * 1_000_000_000)
			host.Memory = config.Service.Resources.MemoryBytes
			pids := int64(config.Service.Resources.PIDs)
			host.PidsLimit = &pids
			host.CapDrop = []string{"ALL"}
			host.SecurityOpt = []string{"no-new-privileges"}
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
		_ = os.Remove(secretPath)
		_ = os.Remove(config.SecretDirectory)
		return nil, fmt.Errorf("start target service %q: %w", config.Service.Name, err)
	}
	runtime := &ServiceRuntime{Container: serviceContainer, ClientID: clientID, health: health, secretPath: secretPath, secretDirectory: config.SecretDirectory}
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

func (service *ServiceRuntime) Stop(ctx context.Context) error {
	timeout := 10 * time.Second
	if err := service.Container.Stop(ctx, &timeout); err != nil {
		return fmt.Errorf("stop target service: %w", err)
	}
	return nil
}

func (service *ServiceRuntime) Start(ctx context.Context) error {
	if err := service.Container.Start(ctx); err != nil {
		return fmt.Errorf("restart target service: %w", err)
	}
	if err := service.health.WaitUntilReady(ctx, service.Container); err != nil {
		return fmt.Errorf("wait for restarted target service: %w", err)
	}
	return nil
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
	if err := os.Remove(service.secretPath); err != nil && !os.IsNotExist(err) {
		cleanupErr = errors.Join(cleanupErr, fmt.Errorf("remove target secret file: %w", err))
	}
	service.secretPath = ""
	if err := os.Remove(service.secretDirectory); err != nil && !os.IsNotExist(err) {
		cleanupErr = errors.Join(cleanupErr, fmt.Errorf("remove target secret directory: %w", err))
	}
	service.secretDirectory = ""
	return cleanupErr
}

func writeSecret(directory, value string) (string, error) {
	if err := os.MkdirAll(directory, 0o700); err != nil {
		return "", fmt.Errorf("create target secret directory: %w", err)
	}
	directory, err := filepath.Abs(directory)
	if err != nil {
		return "", fmt.Errorf("resolve target secret directory: %w", err)
	}
	file, err := os.CreateTemp(directory, "database-dsn-*")
	if err != nil {
		return "", fmt.Errorf("create target secret file: %w", err)
	}
	path := file.Name()
	failed := true
	defer func() {
		_ = file.Close()
		if failed {
			_ = os.Remove(path)
		}
	}()
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
