package runtime

import (
	"net/netip"
	"testing"

	"github.com/moby/moby/api/types/container"
	mobynetwork "github.com/moby/moby/api/types/network"
)

func TestInfrastructureHardeningPolicy(t *testing.T) {
	t.Parallel()
	pids := int64(64)
	host := &container.HostConfig{
		CapDrop: []string{"ALL"}, CapAdd: []string{"SETUID", "SETGID"}, SecurityOpt: []string{"no-new-privileges"},
		PortBindings: mobynetwork.PortMap{mobynetwork.MustParsePort("5432/tcp"): []mobynetwork.PortBinding{{HostIP: netip.MustParseAddr("127.0.0.1")}}},
	}
	host.NanoCPUs = 1_000_000_000
	host.Memory = 128 << 20
	host.PidsLimit = &pids
	policy := infrastructurePolicy{Name: "test", CapAdd: []string{"SETGID", "SETUID"}}
	if err := validateInfrastructureHostConfig(host, policy); err != nil {
		t.Fatal(err)
	}
	host.PortBindings[mobynetwork.MustParsePort("5432/tcp")][0].HostIP = netip.MustParseAddr("0.0.0.0")
	if err := validateInfrastructureHostConfig(host, policy); err == nil {
		t.Fatal("accepted a non-loopback infrastructure binding")
	}
}

func TestRuntimeArchitectureNormalization(t *testing.T) {
	t.Parallel()
	for input, want := range map[string]string{"aarch64": "arm64", "x86_64": "amd64", "arm64": "arm64"} {
		if got := normalizeRuntimeArchitecture(input); got != want {
			t.Fatalf("normalizeRuntimeArchitecture(%q) = %q, want %q", input, got, want)
		}
	}
}

func TestCapabilityNormalization(t *testing.T) {
	t.Parallel()
	if normalizeCapability("CAP_DAC_OVERRIDE") != "DAC_OVERRIDE" || normalizeCapability("dac_override") != "DAC_OVERRIDE" {
		t.Fatal("capability spelling was not normalized")
	}
}
