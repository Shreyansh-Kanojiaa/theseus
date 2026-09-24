// Command theseus-agent runs the edge agent: collectors writing into the local
// store, plus the retention job.
package main

import (
	"context"
	"flag"
	"log"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/Shreyansh-Kanojiaa/theseus/agent"
)

func main() {
	host, _ := os.Hostname()
	var (
		data        = flag.String("data", "data", "directory for the store")
		node        = flag.String("node", host, "node id")
		proc        = flag.String("proc", "/proc", "procfs root (the host's /proc when containerised)")
		mounts      = flag.String("mounts", "/", "comma-separated mountpoints to report disk usage for")
		docker      = flag.String("docker", "/var/run/docker.sock", "Docker socket; empty disables container state")
		scrape      = flag.String("scrape", "", "comma-separated Prometheus endpoints, e.g. http://localhost:9100/metrics")
		hostEvery   = flag.Duration("host-interval", 15*time.Second, "host metrics interval")
		dockerEvery = flag.Duration("docker-interval", 15*time.Second, "container state interval")
		scrapeEvery = flag.Duration("scrape-interval", 30*time.Second, "scrape interval")
		probeEvery  = flag.Duration("probe-interval", 10*time.Second, "health probe interval for containers labelled "+agent.ProbeLabel)
		probeMisses = flag.Uint("probe-misses", 3, "consecutive probe failures that emit a probe_fail event")
	)
	flag.Parse()

	if err := os.MkdirAll(*data, 0o755); err != nil {
		log.Fatal(err)
	}
	s, err := agent.OpenStore(*data, *node)
	if err != nil {
		log.Fatal(err)
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	var wg sync.WaitGroup
	wg.Go(func() { s.RunRetention(ctx, agent.Retention, time.Hour) })
	wg.Go(func() { agent.Collect(ctx, s, "host", *hostEvery, agent.HostMetrics(*proc, split(*mounts))) })
	if *docker != "" {
		d := agent.NewDocker(*docker)
		wg.Go(func() { agent.Collect(ctx, s, "docker", *dockerEvery, agent.ContainerState(d)) })
		wg.Go(func() { agent.Collect(ctx, s, "probe", *probeEvery, agent.Probes(d, uint32(*probeMisses))) })
	}
	for _, t := range split(*scrape) {
		wg.Go(func() { agent.Collect(ctx, s, "scrape "+t, *scrapeEvery, agent.Scrape(t)) })
	}
	log.Printf("theseus-agent: node %s, store %s", *node, *data)
	wg.Wait()
	if err := s.Close(); err != nil {
		log.Fatal(err)
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
