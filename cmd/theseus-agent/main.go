// Command theseus-agent runs the edge agent: collectors writing into the local
// store, plus the retention job.
package main

import (
	"bytes"
	"context"
	"errors"
	"flag"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/Shreyansh-Kanojiaa/theseus/agent"
	tsync "github.com/Shreyansh-Kanojiaa/theseus/sync"
)

func main() {
	host, _ := os.Hostname()
	var (
		data        = flag.String("data", "data", "directory for the store")
		ballast     = flag.Int64("ballast", 64<<20, "bytes preallocated in <data>/ballast and freed when the disk fills; 0 disables")
		node        = flag.String("node", host, "node id")
		listen      = flag.String("listen", ":9101", "address serving the agent's own /metrics; empty disables")
		syncTo      = flag.String("sync", "", "control plane gRPC address (host:port) to upload the spool to; empty disables")
		syncEvery   = flag.Duration("sync-interval", 30*time.Second, "how often to upload the spool")
		proc        = flag.String("proc", "/proc", "procfs root (the host's /proc when containerised)")
		mounts      = flag.String("mounts", "/", "comma-separated mountpoints to report disk usage for")
		docker      = flag.String("docker", "/var/run/docker.sock", "Docker socket; empty disables container state")
		dockerLabel = flag.String("docker-label", "", "only see containers with this key=value label (one node's containers on a shared daemon)")
		scrape      = flag.String("scrape", "", "comma-separated Prometheus endpoints, e.g. http://localhost:9100/metrics")
		hostEvery   = flag.Duration("host-interval", 15*time.Second, "host metrics interval")
		dockerEvery = flag.Duration("docker-interval", 15*time.Second, "container state interval")
		scrapeEvery = flag.Duration("scrape-interval", 30*time.Second, "scrape interval")
		probeEvery  = flag.Duration("probe-interval", 10*time.Second, "health probe interval for containers labelled "+agent.ProbeLabel)
		probeMisses = flag.Uint("probe-misses", 3, "consecutive probe failures that emit a probe_fail event")
		logEvery    = flag.Duration("log-interval", 5*time.Second, "log tail interval for containers labelled "+agent.LogsLabel+" or "+agent.ProbeLabel)
		logLines    = flag.Int("log-lines", 100, "log lines kept per container")
		alertRules  = flag.String("alerts", "", "YAML alert rules file (default: built-in disk, memory and scrape rules)")
		alertOut    = flag.String("alert-output", "stdout", `where alert events go besides the store: "stdout" (JSON lines) or a webhook URL`)
		logKeywords = flag.String("log-keywords", "No space left on device,OOM,out of memory", "comma-separated case-sensitive substrings that emit a log_match event")
	)
	flag.Parse()

	var err error
	if err = os.MkdirAll(*data, 0o755); err != nil {
		log.Fatal(err)
	}
	rulesYAML := agent.DefaultAlertRules
	if *alertRules != "" {
		if rulesYAML, err = os.ReadFile(*alertRules); err != nil {
			log.Fatal(err)
		}
	}
	rules, err := agent.ParseAlertRules(rulesYAML)
	if err != nil {
		log.Fatal(err)
	}
	if *alertOut != "stdout" && !strings.HasPrefix(*alertOut, "http://") && !strings.HasPrefix(*alertOut, "https://") {
		log.Fatalf("-alert-output %q: want stdout or an http(s) URL", *alertOut)
	}
	alerts := agent.NewAlerts(rules, *alertOut)
	s, err := agent.OpenStore(*data, *node, *ballast)
	if err != nil {
		log.Fatal(err)
	}
	for _, r := range rules {
		s.KeepSamples(r.Metric) // degraded mode keeps what alerts watch
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	var wg sync.WaitGroup
	if *syncTo != "" {
		syncer, err := tsync.NewNaive(s, *syncTo)
		if err != nil {
			log.Fatal(err)
		}
		defer func() { _ = syncer.Close() }()
		wg.Go(func() { tsync.Run(ctx, syncer, *syncEvery, 5*time.Minute) })
	}
	if *listen != "" {
		wg.Go(func() { serveMetrics(ctx, *listen, s) })
	}
	wg.Go(func() { s.RunRetention(ctx, agent.Retention, time.Hour) })
	wg.Go(func() { s.RunDiskRecovery(ctx, 30*time.Second) })
	wg.Go(func() {
		agent.Collect(ctx, s, "host", *hostEvery, alerts.Watch(agent.HostMetrics(*proc, split(*mounts))))
	})
	if *docker != "" {
		d := agent.NewDocker(*docker)
		d.Label = *dockerLabel
		wg.Go(func() { agent.Collect(ctx, s, "docker", *dockerEvery, alerts.Watch(agent.ContainerState(d))) })
		wg.Go(func() { agent.Collect(ctx, s, "probe", *probeEvery, agent.Probes(d, uint32(*probeMisses))) })
		logs := agent.NewLogTail(d, *logLines, split(*logKeywords))
		wg.Go(func() { agent.Collect(ctx, s, "logs", *logEvery, logs.Collect) })
	}
	for _, t := range split(*scrape) {
		wg.Go(func() { agent.Collect(ctx, s, "scrape "+t, *scrapeEvery, alerts.Watch(agent.Scrape(t))) })
	}
	log.Printf("theseus-agent: node %s, store %s", *node, *data)
	wg.Wait()
	if err := s.Close(); err != nil {
		log.Fatal(err)
	}
}

// serveMetrics serves /metrics until ctx is done. A failure is logged, not
// fatal: the agent's job doesn't depend on being watched.
func serveMetrics(ctx context.Context, addr string, s *agent.Store) {
	mux := http.NewServeMux()
	mux.HandleFunc("/metrics", func(w http.ResponseWriter, _ *http.Request) {
		var b bytes.Buffer
		if err := s.WriteMetrics(&b); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "text/plain; version=0.0.4")
		_, _ = w.Write(b.Bytes())
	})
	srv := &http.Server{Addr: addr, Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	go func() {
		<-ctx.Done()
		_ = srv.Close()
	}()
	if err := srv.ListenAndServe(); !errors.Is(err, http.ErrServerClosed) {
		log.Printf("metrics: %v", err)
	}
}

func split(s string) []string {
	var out []string
	for f := range strings.SplitSeq(s, ",") {
		if f = strings.TrimSpace(f); f != "" {
			out = append(out, f)
		}
	}
	return out
}
