package agent

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"google.golang.org/protobuf/encoding/protojson"

	schemav1 "github.com/Shreyansh-Kanojiaa/theseus/schema/v1"
)

func TestParseAlertRules(t *testing.T) {
	for _, bad := range []string{
		"rules: [{name: a, metric: m, op: '>>'}]",
		"rules: [{name: a, op: '>'}]",
		"rules: [{name: a, metric: m, op: '>'}, {name: a, metric: n, op: '<'}]",
		"rules: [{name: a, metric: m, op: '>', threshold: 1}]", // unknown field
		"rules: [{name: a, metric: m, op: '>', for: soon}]",
	} {
		if _, err := ParseAlertRules([]byte(bad)); err == nil {
			t.Errorf("%s: want an error", bad)
		}
	}
	rs, err := ParseAlertRules([]byte("rules: [{name: a, metric: m, op: '>', value: 2, for: 30s, labels: {x: y}}]"))
	if err != nil || len(rs) != 1 || rs[0].For != 30*time.Second || rs[0].Labels["x"] != "y" || rs[0].Value != 2 {
		t.Fatalf("got %+v, %v", rs, err)
	}
}

// TestDiskAlert runs the default rules: disk above 85% on / fires once, stays
// quiet while it holds, resolves when it clears. The webhook gets both.
func TestDiskAlert(t *testing.T) {
	now := time.UnixMilli(1_000_000)
	wallClock = func() time.Time { return now }
	t.Cleanup(func() { wallClock = time.Now })

	hooked := make(chan *schemav1.Event, 10)
	hook := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		var e schemav1.Event
		if err := protojson.Unmarshal(b, &e); err != nil {
			t.Error(err)
		}
		hooked <- &e
	}))
	defer hook.Close()

	rules, err := ParseAlertRules(DefaultAlertRules)
	if err != nil {
		t.Fatal(err)
	}
	var disk, mem float64
	collect := NewAlerts(rules, hook.URL).Watch(func(context.Context) ([]*schemav1.Record, error) {
		return []*schemav1.Record{
			metric("host_disk_used_ratio", disk, "mountpoint", "/"),
			metric("host_disk_used_ratio", 0.1, "mountpoint", "/boot"),
			metric("host_memory_used_ratio", mem),
		}, nil
	})
	round := func() []*schemav1.Event {
		t.Helper()
		now = now.Add(15 * time.Second)
		recs, err := collect(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		var evs []*schemav1.Event
		for _, r := range recs[3:] {
			evs = append(evs, r.GetEvent())
		}
		return evs
	}

	disk, mem = 0.85, 0.95
	if evs := round(); len(evs) != 0 { // 0.85 is not > 0.85; memory needs 1m
		t.Fatalf("at threshold: %v", evs)
	}
	disk = 0.91
	evs := round()
	if len(evs) != 1 || evs[0].Kind != "alert" || evs[0].Source != "disk_high" ||
		evs[0].Attrs["series"] != `host_disk_used_ratio{mountpoint="/"}` || evs[0].Attrs["value"] != "0.91" ||
		evs[0].Attrs["threshold"] != "> 0.85" || evs[0].Attrs["severity"] != "critical" {
		t.Fatalf("disk over: %v", evs)
	}
	if e := <-hooked; e.Kind != "alert" || !strings.Contains(e.Message, "0.91 > 0.85") {
		t.Fatalf("webhook got %v", e)
	}
	for range 2 {
		if evs := round(); len(evs) != 0 {
			t.Fatalf("still over: %v", evs)
		}
	}
	if evs := round(); len(evs) != 1 || evs[0].Source != "memory_high" { // 60 s after it first held
		t.Fatalf("memory for 1m: %v", evs)
	}
	<-hooked
	disk = 0.5
	if evs := round(); len(evs) != 1 || evs[0].Kind != "alert_resolved" || evs[0].Source != "disk_high" {
		t.Fatalf("cleared: %v", evs)
	}
	if e := <-hooked; e.Kind != "alert_resolved" {
		t.Fatalf("webhook got %v", e)
	}
	disk = 0.99
	if evs := round(); len(evs) != 1 || evs[0].Kind != "alert" {
		t.Fatalf("fires again after resolving: %v", evs)
	}
}
