package runtime

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"sync"

	"github.com/moby/moby/api/types/container"
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
	RunID                 string
	Network               *testcontainers.DockerNetwork
	Redpanda              *redpanda.Container
	Postgres              *postgres.PostgresContainer
	HostBroker            string
	InternalBroker        string
	HostPostgresDSN       string
	InternalPostgres      string
	PostgresAdminUser     string
	PostgresAdminPassword string
	NetworkName           string
	cleanupMutex          sync.Mutex
	cleaned               bool
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
	redpandaImage, err := lockedImage(lock, "redpanda")
	if err != nil {
		return nil, err
	}
	postgresImage, err := lockedImage(lock, "postgresql")
	if err != nil {
		return nil, err
	}
	password, err := runtimeSecret()
	if err != nil {
		return nil, err
	}
	environment := &Environment{
		RunID:                 runID,
		InternalBroker:        "redpanda:9094",
		InternalPostgres:      "postgres:5432",
		PostgresAdminUser:     "chronicle_admin",
		PostgresAdminPassword: password,
	}
	defer func() {
		if resultErr != nil {
			if cleanupErr := environment.Cleanup(context.Background()); cleanupErr != nil {
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

	environment.Redpanda, err = redpanda.Run(ctx, redpandaImage,
		tcnetwork.WithNetwork(nil, environment.Network),
		redpanda.WithListener(environment.InternalBroker),
		testcontainers.WithLabels(labels),
		testcontainers.WithHostConfigModifier(func(config *container.HostConfig) {
			config.NanoCPUs = 1_000_000_000
			config.Memory = 1536 << 20
			pids := int64(512)
			config.PidsLimit = &pids
			config.SecurityOpt = []string{"no-new-privileges"}
		}),
	)
	if err != nil {
		return nil, fmt.Errorf("start Redpanda: %w", err)
	}
	environment.HostBroker, err = environment.Redpanda.KafkaSeedBroker(ctx)
	if err != nil {
		return nil, fmt.Errorf("resolve host Redpanda endpoint: %w", err)
	}

	environment.Postgres, err = postgres.Run(ctx, postgresImage,
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
			config.SecurityOpt = []string{"no-new-privileges"}
		}),
	)
	if err != nil {
		return nil, fmt.Errorf("start PostgreSQL: %w", err)
	}
	environment.HostPostgresDSN, err = environment.Postgres.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		return nil, fmt.Errorf("resolve host PostgreSQL endpoint: %w", err)
	}
	return environment, nil
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
	environment.cleaned = true
	var cleanupErrors []error
	if environment.Postgres != nil {
		if err := testcontainers.TerminateContainer(environment.Postgres, testcontainers.StopContext(ctx)); err != nil {
			cleanupErrors = append(cleanupErrors, fmt.Errorf("terminate PostgreSQL: %w", err))
		}
	}
	if environment.Redpanda != nil {
		if err := testcontainers.TerminateContainer(environment.Redpanda, testcontainers.StopContext(ctx)); err != nil {
			cleanupErrors = append(cleanupErrors, fmt.Errorf("terminate Redpanda: %w", err))
		}
	}
	if environment.Network != nil {
		if err := environment.Network.Remove(ctx); err != nil {
			cleanupErrors = append(cleanupErrors, fmt.Errorf("remove run network %q: %w", environment.NetworkName, err))
		}
	}
	return errors.Join(cleanupErrors...)
}

func lockedImage(lock imagelock.Lock, name string) (string, error) {
	for _, image := range lock.Images {
		if image.Name == name {
			return image.Reference, nil
		}
	}
	return "", fmt.Errorf("image lock has no %q entry", name)
}

func runtimeSecret() (string, error) {
	value := make([]byte, 24)
	if _, err := rand.Read(value); err != nil {
		return "", fmt.Errorf("generate runtime secret: %w", err)
	}
	return base64.RawURLEncoding.EncodeToString(value), nil
}
