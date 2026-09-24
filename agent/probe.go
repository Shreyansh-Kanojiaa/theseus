package agent

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	schemav1 "github.com/Shreyansh-Kanojiaa/theseus/schema/v1"
)

// ProbeLabel is the container label that declares a health probe:
//
//	http://:9090/-/healthy  GET, healthy on a status below 400
//	tcp://:5432             healthy if a TCP connection opens
//	exec:pg_isready -q      run inside the container, healthy on exit 0
//
// An empty host means the container's own IP (localhost with --net=host).
// The exec command is split on whitespace, with no shell.
const ProbeLabel = "theseus.probe"

const probeTimeout = 5 * time.Second

// New connection per probe, as the kubelet does: a kept-alive connection can
// hide a listener that no longer accepts.
var probeClient = &http.Client{Transport: &http.Transport{DisableKeepAlives: true}}

type probeSpec struct {
	typ  schemav1.ProbeType
	url  *url.URL // http and tcp
	argv []string // exec
}

func parseProbe(spec string) (probeSpec, error) {
	if cmd, ok := strings.CutPrefix(spec, "exec:"); ok {
		argv := strings.Fields(cmd)
		if len(argv) == 0 {
			return probeSpec{}, fmt.Errorf("probe %q: empty command", spec)
		}
		return probeSpec{typ: schemav1.ProbeType_PROBE_TYPE_EXEC, argv: argv}, nil
	}
	u, err := url.Parse(spec)
	if err != nil {
		return probeSpec{}, fmt.Errorf("probe %q: %w", spec, err)
	}
	switch {
	case u.Port() == "":
		return probeSpec{}, fmt.Errorf("probe %q: no port", spec)
	case u.Scheme == "http" || u.Scheme == "https":
		return probeSpec{typ: schemav1.ProbeType_PROBE_TYPE_HTTP, url: u}, nil
	case u.Scheme == "tcp":
		return probeSpec{typ: schemav1.ProbeType_PROBE_TYPE_TCP, url: u}, nil
	}
	return probeSpec{}, fmt.Errorf("probe %q: scheme must be http, https, tcp or exec", spec)
}

func (p probeSpec) run(ctx context.Context, d *Docker, c Container) error {
	if c.State != "running" {
		return fmt.Errorf("container %s", c.State)
	}
	var addr string
	if p.url != nil {
		host := p.url.Hostname()
		if host == "" {
			if host = c.IP(); host == "" {
				host = "localhost"
			}
		}
		addr = net.JoinHostPort(host, p.url.Port())
	}
	switch p.typ {
	case schemav1.ProbeType_PROBE_TYPE_HTTP:
		u := *p.url
		u.Host = addr
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil)
		if err != nil {
			return err
		}
		resp, err := probeClient.Do(req)
		if err != nil {
			return err
		}
		_ = resp.Body.Close()
		if resp.StatusCode >= 400 {
			return fmt.Errorf("http %s", resp.Status)
		}
	case schemav1.ProbeType_PROBE_TYPE_TCP:
		var dl net.Dialer
		conn, err := dl.DialContext(ctx, "tcp", addr)
		if err != nil {
			return err
		}
		_ = conn.Close()
	case schemav1.ProbeType_PROBE_TYPE_EXEC:
		code, out, err := d.Exec(ctx, c.ID, p.argv)
		if err != nil {
			return err
		}
		if code != 0 {
			return fmt.Errorf("exit %d: %s", code, out)
		}
	}
	return nil
}

// Probes finds containers labelled theseus.probe on every round, probes them
// concurrently and emits one ProbeResult each. The misses-th consecutive
// failure also emits a probe_fail event, and the first success after it a
// probe_recovered event. A stopped container fails its probe; a removed one
// is forgotten.
func Probes(d *Docker, misses uint32) CollectFunc {
	misses = max(misses, 1)
	fails := map[string]uint32{} // container name -> consecutive failures
	return func(ctx context.Context) ([]*schemav1.Record, error) {
		cs, err := d.Containers(ctx)
		if err != nil {
			return nil, err
		}
		var errs []error
		var results []*schemav1.ProbeResult
		var wg sync.WaitGroup
		for _, c := range cs {
			spec, ok := c.Labels[ProbeLabel]
			if !ok {
				continue
			}
			p, err := parseProbe(spec)
			if err != nil { // a bad label is a config error, not an unhealthy service
				errs = append(errs, fmt.Errorf("%s: %w", c.Name(), err))
				continue
			}
			r := &schemav1.ProbeResult{Target: c.Name(), Type: p.typ, Endpoint: spec}
			results = append(results, r)
			wg.Go(func() {
				ctx, cancel := context.WithTimeout(ctx, probeTimeout)
				defer cancel()
				start := time.Now()
				err := p.run(ctx, d, c)
				r.LatencyUs = uint64(time.Since(start).Microseconds())
				if r.Ok = err == nil; err != nil {
					r.Error = err.Error()
				}
			})
		}
		wg.Wait()

		recs := make([]*schemav1.Record, 0, len(results))
		seen := make(map[string]bool, len(results))
		for _, r := range results {
			seen[r.Target] = true
			prev := fails[r.Target]
			if r.Ok {
				fails[r.Target] = 0
			} else {
				fails[r.Target]++
			}
			r.ConsecutiveFailures = fails[r.Target]
			recs = append(recs, &schemav1.Record{Body: &schemav1.Record_ProbeResult{ProbeResult: r}})
			switch {
			case !r.Ok && r.ConsecutiveFailures == misses:
				recs = append(recs, probeEvent("probe_fail", r, r.ConsecutiveFailures,
					fmt.Sprintf("%d consecutive probe failures: %s", misses, r.Error)))
			case r.Ok && prev >= misses:
				recs = append(recs, probeEvent("probe_recovered", r, prev,
					fmt.Sprintf("probe ok after %d failures", prev)))
			}
		}
		for name := range fails {
			if !seen[name] {
				delete(fails, name)
			}
		}
		return recs, errors.Join(errs...)
	}
}

func probeEvent(kind string, r *schemav1.ProbeResult, failures uint32, msg string) *schemav1.Record {
	return &schemav1.Record{Body: &schemav1.Record_Event{Event: &schemav1.Event{
		Kind: kind, Source: r.Target, Message: msg,
		Attrs: map[string]string{"endpoint": r.Endpoint, "failures": strconv.FormatUint(uint64(failures), 10)},
	}}}
}
