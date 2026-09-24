package agent

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"maps"
	"net"
	"net/http"
	"slices"
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

	NetworkSettings struct {
		Networks map[string]struct {
			IPAddress string `json:"IPAddress"`
		} `json:"Networks"`
	} `json:"NetworkSettings"`
}

// IP is the container's address on its first network (by name), or "" when
// it has none, e.g. with --net=host or when stopped.
func (c Container) IP() string {
	for _, n := range slices.Sorted(maps.Keys(c.NetworkSettings.Networks)) {
		if ip := c.NetworkSettings.Networks[n].IPAddress; ip != "" {
			return ip
		}
	}
	return ""
}

// Name is the container name without Docker's leading slash.
func (c Container) Name() string {
	if len(c.Names) == 0 {
		return c.ID[:min(12, len(c.ID))]
	}
	return strings.TrimPrefix(c.Names[0], "/")
}

// call sends in (if not nil) as JSON and returns the response when it is 2xx.
func (d *Docker) call(ctx context.Context, method, path string, in any) (*http.Response, error) {
	var body io.Reader
	if in != nil {
		b, err := json.Marshal(in)
		if err != nil {
			return nil, err
		}
		body = bytes.NewReader(b)
	}
	req, err := http.NewRequestWithContext(ctx, method, "http://docker"+path, body)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := d.c.Do(req)
	if err != nil {
		return nil, fmt.Errorf("docker: %w", err)
	}
	if resp.StatusCode/100 != 2 {
		msg, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		_ = resp.Body.Close()
		return nil, fmt.Errorf("docker: %s %s: %s %s", method, path, resp.Status, bytes.TrimSpace(msg))
	}
	return resp, nil
}

// get decodes the JSON response of GET path into out.
func (d *Docker) get(ctx context.Context, path string, out any) error {
	resp, err := d.call(ctx, http.MethodGet, path, nil)
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()
	return json.NewDecoder(resp.Body).Decode(out)
}

// Containers lists all containers, running or not.
func (d *Docker) Containers(ctx context.Context) ([]Container, error) {
	var cs []Container
	return cs, d.get(ctx, "/containers/json?all=1", &cs)
}

// Exec runs cmd inside container id and returns its exit code and the first
// 1 KiB of its combined output. It returns when cmd exits or ctx is done.
func (d *Docker) Exec(ctx context.Context, id string, cmd []string) (int, string, error) {
	resp, err := d.call(ctx, http.MethodPost, "/containers/"+id+"/exec",
		map[string]any{"Cmd": cmd, "AttachStdout": true, "AttachStderr": true, "Tty": true})
	if err != nil {
		return 0, "", err
	}
	var ex struct {
		ID string `json:"Id"`
	}
	err = json.NewDecoder(resp.Body).Decode(&ex)
	_ = resp.Body.Close()
	if err != nil {
		return 0, "", err
	}
	// With Tty the stream is raw output (no multiplex headers); it ends when cmd exits.
	if resp, err = d.call(ctx, http.MethodPost, "/exec/"+ex.ID+"/start", map[string]any{"Tty": true}); err != nil {
		return 0, "", err
	}
	out, _ := io.ReadAll(io.LimitReader(resp.Body, 1024))
	_, err = io.Copy(io.Discard, resp.Body)
	_ = resp.Body.Close()
	if err != nil {
		return 0, "", fmt.Errorf("docker: exec: %w", err)
	}
	var st struct{ ExitCode int }
	if err := d.get(ctx, "/exec/"+ex.ID+"/json", &st); err != nil {
		return 0, "", err
	}
	return st.ExitCode, strings.TrimSpace(string(out)), nil
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
