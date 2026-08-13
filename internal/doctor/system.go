package doctor

import (
	"fmt"
	"net"
	"syscall"
)

const defaultMinimumAvailableBytes = 10 << 30

func checkWorkspaceDisk(path string, minimum uint64) Check {
	var stats syscall.Statfs_t
	if err := syscall.Statfs(path, &stats); err != nil {
		return Check{
			ID:      "host.workspace_disk",
			Scope:   "host",
			Status:  StatusFail,
			Summary: fmt.Sprintf("could not inspect host workspace capacity: %v", err),
		}
	}

	available := uint64(stats.Bavail) * uint64(stats.Bsize)
	status := StatusPass
	summary := "host workspace has at least 10 GiB available"
	if available < minimum {
		status = StatusFail
		summary = "host workspace has less than 10 GiB available"
	}
	return Check{
		ID:      "host.workspace_disk",
		Scope:   "host",
		Status:  status,
		Summary: summary,
		Details: map[string]any{
			"availableBytes": available,
			"minimumBytes":   minimum,
			"measurement":    "host filesystem containing the current workspace",
		},
	}
}

func checkLoopbackPort() Check {
	listener, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		return Check{
			ID:      "host.loopback_port",
			Scope:   "host",
			Status:  StatusFail,
			Summary: fmt.Sprintf("could not allocate a host loopback port: %v", err),
		}
	}
	if err := listener.Close(); err != nil {
		return Check{
			ID:      "host.loopback_port",
			Scope:   "host",
			Status:  StatusFail,
			Summary: fmt.Sprintf("allocated host loopback port could not be released: %v", err),
		}
	}
	return Check{
		ID:      "host.loopback_port",
		Scope:   "host",
		Status:  StatusPass,
		Summary: "host loopback ephemeral port allocation succeeded",
		Details: map[string]any{
			"address":     "127.0.0.1",
			"ephemeral":   true,
			"measurement": "host socket bind and immediate close",
		},
	}
}
