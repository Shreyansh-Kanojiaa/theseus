package agent

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"strings"
	"time"

	schemav1 "github.com/Shreyansh-Kanojiaa/theseus/schema/v1"
)

// Docker talks to the Docker Engine API over its unix socket. Only the few
// endpoints the agent needs, so no SDK.
type Docker struct{ c *http.Client }

// NewDocker returns a client for the engine listening on socket.
func NewDocker(socket string) *Docker {
	return &Docker{c: &http.Client{Timeout: 10 * time.Second, Transport: &http.Transport{
		DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
			var d net.Dialer
			return d.DialContext(ctx, "unix", socket)
		},
	}}}
}

// Container is the subset of GET /containers/json the agent uses.
type Container struct {
	ID     string            `json:"Id"`
	Names  []string          `json:"Names"`
	Image  string            `json:"Image"`
	State  string            `json:"State"` // created, running, paused, restarting, exited, dead
	Labels map[string]string `json:"Labels"`
}

// Name is the container name without Docker's leading slash.
func (c Container) Name() string {
	if len(c.Names) == 0 {
		return c.ID[:min(12, len(c.ID))]
	}
	return strings.TrimPrefix(c.Names[0], "/")
}

// Containers lists all containers, running or not.
func (d *Docker) Containers(ctx context.Context) ([]Container, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://docker/containers/json?all=1", nil)
	if err != nil {
		return nil, err
	}
	resp, err := d.c.Do(req)
	if err != nil {
		return nil, fmt.Errorf("docker: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("docker: containers: %s", resp.Status)
	}
	var cs []Container
	return cs, json.NewDecoder(resp.Body).Decode(&cs)
}

// ContainerState emits container_running (1 or 0) for every container.
func ContainerState(d *Docker) CollectFunc {
	return func(ctx context.Context) ([]*schemav1.Record, error) {
		cs, err := d.Containers(ctx)
		recs := make([]*schemav1.Record, 0, len(cs))
		for _, c := range cs {
			up := 0.0
			if c.State == "running" {
				up = 1
			}
			recs = append(recs, metric("container_running", up, "container", c.Name(), "image", c.Image, "state", c.State))
		}
		return recs, err
	}
}
