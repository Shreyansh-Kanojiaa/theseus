package agent

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"

	schemav1 "github.com/Shreyansh-Kanojiaa/theseus/schema/v1"
)

// byKey indexes samples as name{k=v,...} with labels in sorted order.
func byKey(recs []*schemav1.Record) map[string]float64 {
	out := map[string]float64{}
	for _, r := range recs {
		s := r.GetSample()
		out[s.Name+" "+fmt.Sprint(s.Labels)] = s.Value
	}
	return out
}

func TestHostMetrics(t *testing.T) {
	proc := t.TempDir()
	write := func(name, body string) {
		t.Helper()
		p := filepath.Join(proc, name)
		if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(p, []byte(body), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	write("stat", "cpu  100 0 100 700 100 0 0 0 0 0\ncpu0 1 2 3\n")
	write("meminfo", "MemTotal:       1000 kB\nMemFree:         100 kB\nMemAvailable:    250 kB\n")
	write("net/dev", `Inter-|   Receive                                                |  Transmit
 face |bytes    packets errs drop fifo frame compressed multicast|bytes    packets errs drop fifo colls carrier compressed
    lo:     500       5    0    0    0     0          0         0      500       5    0    0    0     0       0          0
  eth0:    1234      10    0    0    0     0          0         0     5678      20    0    0    0     0       0          0
`)
	collect := HostMetrics(proc, []string{t.TempDir()})

	recs, err := collect(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	got := byKey(recs)
	if _, ok := got["host_cpu_used_ratio map[]"]; ok {
		t.Error("cpu ratio on the first round, want none (needs a delta)")
	}
	for k, want := range map[string]float64{
		"host_memory_total_bytes map[]":                      1024000,
		"host_memory_used_ratio map[]":                       0.75,
		"host_network_receive_bytes_total map[device:eth0]":  1234,
		"host_network_transmit_bytes_total map[device:eth0]": 5678,
	} {
		if got[k] != want {
			t.Errorf("%s = %v, want %v", k, got[k], want)
		}
	}
	if _, ok := got["host_network_receive_bytes_total map[device:lo]"]; ok {
		t.Error("lo should be skipped")
	}
	var disk int
	for _, r := range recs {
		if s := r.GetSample(); s.Name == "host_disk_used_ratio" && s.Value >= 0 && s.Value <= 1 {
			disk++
		}
	}
	if disk != 1 {
		t.Errorf("want one sane disk ratio, got %d", disk)
	}

	// 1000 more jiffies, 250 of them idle or iowait: 75% busy.
	write("stat", "cpu  500 0 450 900 150 0 0 0 0 0\n")
	recs, err = collect(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if got := byKey(recs)["host_cpu_used_ratio map[]"]; got != 0.75 {
		t.Errorf("cpu ratio = %v, want 0.75", got)
	}
}

func TestScrape(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = fmt.Fprint(w, `# TYPE node_load1 gauge
node_load1 0.5
# TYPE node_filesystem_avail_bytes gauge
node_filesystem_avail_bytes{mountpoint="/",path="C:\\x \"q\""} 42
# TYPE req_seconds histogram
req_seconds_bucket{le="0.1"} 3
req_seconds_bucket{le="+Inf"} 5
req_seconds_sum 1.5
req_seconds_count 5
`)
	}))
	inst := srv.Listener.Addr().String()

	recs, err := Scrape(srv.URL + "/metrics")(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	got := byKey(recs)
	for k, want := range map[string]float64{
		"node_load1 map[instance:" + inst + "]":                                                0.5,
		"node_filesystem_avail_bytes map[instance:" + inst + " mountpoint:/ path:C:\\x \"q\"]": 42,
		"req_seconds_bucket map[instance:" + inst + " le:0.1]":                                 3,
		"req_seconds_bucket map[instance:" + inst + " le:+Inf]":                                5,
		"req_seconds_count map[instance:" + inst + "]":                                         5,
		"up map[instance:" + inst + "]":                                                        1,
	} {
		if v, ok := got[k]; !ok || v != want {
			t.Errorf("%s = %v (present %v), want %v", k, v, ok, want)
		}
	}
	if len(recs) != 7 {
		t.Errorf("got %d samples, want 7: %v", len(recs), got)
	}

	srv.Close()
	recs, err = Scrape(srv.URL + "/metrics")(context.Background())
	if err == nil || len(recs) != 1 || byKey(recs)["up map[instance:"+inst+"]"] != 0 {
		t.Fatalf("down target: want up=0 and an error, got %v, %v", byKey(recs), err)
	}
}

// fakeDocker serves h on a unix socket and returns a client for it.
func fakeDocker(t *testing.T, h http.HandlerFunc) *Docker {
	t.Helper()
	sock := filepath.Join(t.TempDir(), "docker.sock")
	l, err := net.Listen("unix", sock)
	if err != nil {
		t.Fatal(err)
	}
	srv := httptest.NewUnstartedServer(h)
	srv.Listener = l
	srv.Start()
	t.Cleanup(srv.Close)
	return NewDocker(sock)
}

func TestContainerState(t *testing.T) {
	d := fakeDocker(t, func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/containers/json" || r.URL.Query().Get("all") != "1" {
			http.NotFound(w, r)
			return
		}
		_, _ = fmt.Fprint(w, `[{"Id":"abc","Names":["/postgres"],"Image":"postgres:16","State":"running","Labels":{"theseus.probe":"tcp://:5432"}},
			{"Id":"def","Names":["/prometheus"],"Image":"prom/prometheus","State":"exited"}]`)
	})

	recs, err := ContainerState(d)(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	got := byKey(recs)
	if len(got) != 2 ||
		got["container_running map[container:postgres image:postgres:16 state:running]"] != 1 ||
		got["container_running map[container:prometheus image:prom/prometheus state:exited]"] != 0 {
		t.Fatalf("got %v", got)
	}
}
