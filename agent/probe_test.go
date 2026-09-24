package agent

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"slices"
	"sync/atomic"
	"testing"

	schemav1 "github.com/Shreyansh-Kanojiaa/theseus/schema/v1"
)

// TestProbes drives one probe of each type through a fake Docker and checks
// that the third consecutive miss (and only the third) emits probe_fail.
func TestProbes(t *testing.T) {
	var webOK atomic.Bool
	web := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		if !webOK.Load() {
			w.WriteHeader(http.StatusServiceUnavailable)
		}
	}))
	defer web.Close()
	_, webPort, _ := net.SplitHostPort(web.Listener.Addr().String())
	tcp, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tcp.Close() }()

	var execCmd atomic.Value
	d := fakeDocker(t, func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/containers/json":
			// web's label has no host: the probe must fill in its IP.
			_, _ = fmt.Fprintf(w, `[
				{"Id":"w1","Names":["/web"],"State":"running","Labels":{"theseus.probe":"http://:%s/healthz"},
				 "NetworkSettings":{"Networks":{"b":{"IPAddress":"127.0.0.1"},"a":{"IPAddress":""}}}},
				{"Id":"t1","Names":["/db"],"State":"running","Labels":{"theseus.probe":"tcp://%s"}},
				{"Id":"p1","Names":["/pg"],"State":"running","Labels":{"theseus.probe":"exec:pg_isready  -q"}},
				{"Id":"x1","Names":["/bad"],"State":"running","Labels":{"theseus.probe":"ftp://x:21"}},
				{"Id":"n1","Names":["/plain"],"State":"running"}]`, webPort, tcp.Addr())
		case "/containers/p1/exec":
			var body struct{ Cmd []string }
			_ = json.NewDecoder(r.Body).Decode(&body)
			execCmd.Store(body.Cmd)
			w.WriteHeader(http.StatusCreated)
			_, _ = fmt.Fprint(w, `{"Id":"e1"}`)
		case "/exec/e1/start":
			_, _ = fmt.Fprint(w, "accepting connections\r\n")
		case "/exec/e1/json":
			_, _ = fmt.Fprint(w, `{"ExitCode":0}`)
		default:
			http.NotFound(w, r)
		}
	})

	probe := Probes(d, 3)
	round := func() (map[string]*schemav1.ProbeResult, []*schemav1.Event) {
		t.Helper()
		recs, err := probe(context.Background())
		if err == nil {
			t.Fatal("want an error for the bad label")
		}
		res := map[string]*schemav1.ProbeResult{}
		var evs []*schemav1.Event
		for _, r := range recs {
			if p := r.GetProbeResult(); p != nil {
				res[p.Target] = p
			} else {
				evs = append(evs, r.GetEvent())
			}
		}
		return res, evs
	}

	for i := uint32(1); i <= 4; i++ {
		res, evs := round()
		if len(res) != 3 || !res["db"].Ok || !res["pg"].Ok {
			t.Fatalf("round %d: want web, db, pg with db and pg healthy, got %v", i, res)
		}
		if w := res["web"]; w.Ok || w.ConsecutiveFailures != i || w.Type != schemav1.ProbeType_PROBE_TYPE_HTTP {
			t.Fatalf("round %d: web = %v", i, w)
		}
		if want := i == 3; want != (len(evs) == 1) {
			t.Fatalf("round %d: events %v", i, evs)
		}
	}
	if got := execCmd.Load().([]string); !slices.Equal(got, []string{"pg_isready", "-q"}) {
		t.Errorf("exec cmd = %q", got)
	}

	webOK.Store(true)
	res, evs := round()
	if !res["web"].Ok || res["web"].ConsecutiveFailures != 0 ||
		len(evs) != 1 || evs[0].Kind != "probe_recovered" || evs[0].Attrs["failures"] != "4" {
		t.Fatalf("recovery: web = %v, events %v", res["web"], evs)
	}
	if _, evs := round(); len(evs) != 0 {
		t.Fatalf("steady healthy round emitted %v", evs)
	}
}

func TestProbeFailEvent(t *testing.T) {
	d := fakeDocker(t, func(w http.ResponseWriter, _ *http.Request) {
		_, _ = fmt.Fprint(w, `[{"Id":"a","Names":["/prometheus"],"State":"exited","Labels":{"theseus.probe":"http://:9090/-/healthy"}}]`)
	})
	probe := Probes(d, 3)
	var recs []*schemav1.Record
	for range 3 {
		var err error
		if recs, err = probe(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
	e := recs[len(recs)-1].GetEvent()
	if e == nil || e.Kind != "probe_fail" || e.Source != "prometheus" || e.Attrs["failures"] != "3" ||
		e.Message != "3 consecutive probe failures: container exited" {
		t.Fatalf("got %v", recs)
	}
}
