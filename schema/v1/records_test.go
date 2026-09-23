package schemav1

import (
	"testing"
	"time"

	"google.golang.org/protobuf/proto"
)

func TestRoundTrip(t *testing.T) {
	h := func(p Priority) *Header {
		return &Header{NodeId: "node-c", Seq: 42, Hlc: 1<<40 | 7, Priority: p}
	}
	msgs := []proto.Message{
		&Sample{Header: h(Priority_PRIORITY_METRIC), Name: "node_filesystem_avail_bytes",
			Labels: map[string]string{"mountpoint": "/"}, Value: 1.5e9},
		&Event{Header: h(Priority_PRIORITY_EVENT), Kind: "log_match", Source: "postgres",
			Message: "No space left on device", Attrs: map[string]string{"line": "812"}},
		&ProbeResult{Header: h(Priority_PRIORITY_EVENT), Target: "prometheus", Type: ProbeType_PROBE_TYPE_HTTP,
			Endpoint: "http://:9090/-/healthy", Error: "connection refused", LatencyUs: 1200, ConsecutiveFailures: 3},
		&Incident{Header: h(Priority_PRIORITY_INCIDENT), IncidentId: "node-c-42", State: IncidentState_INCIDENT_STATE_OPEN,
			Service: "postgres", Summary: "database unreachable", OpenedHlc: 1 << 40},
	}
	for _, m := range msgs {
		b, err := proto.Marshal(m)
		if err != nil {
			t.Fatal(err)
		}
		got := m.ProtoReflect().New().Interface()
		if err := proto.Unmarshal(b, got); err != nil {
			t.Fatal(err)
		}
		if !proto.Equal(m, got) {
			t.Errorf("%T round-trip mismatch:\n want %v\n got  %v", m, m, got)
		}
	}
}

func TestPriorityOrder(t *testing.T) {
	if Priority_PRIORITY_INCIDENT <= Priority_PRIORITY_ACTION ||
		Priority_PRIORITY_ACTION <= Priority_PRIORITY_EVENT ||
		Priority_PRIORITY_EVENT <= Priority_PRIORITY_METRIC {
		t.Fatal("priority must be incident > action > event > metric")
	}
}

func TestClock(t *testing.T) {
	wall := time.UnixMilli(1_000_000)
	c := NewClock(0, func() time.Time { return wall })

	a := c.Now()
	if a != 1_000_000<<16 || !HLCTime(a).Equal(wall) {
		t.Fatalf("fresh clock = %x, want physical time with logical 0", a)
	}
	if b := c.Now(); b != a+1 {
		t.Fatalf("same millisecond: got %x, want %x", b, a+1)
	}

	wall = wall.Add(-time.Hour) // wall clock jumps backwards
	if b := c.Now(); b <= a+1 {
		t.Fatalf("clock went backwards: %x <= %x", b, a+1)
	}

	remote := uint64(2_000_000)<<16 | 5 // peer is ahead
	if b := c.Update(remote); b != remote+1 {
		t.Fatalf("update: got %x, want %x", b, remote+1)
	}

	c = NewClock(uint64(1_000_000)<<16|0xffff, func() time.Time { return time.UnixMilli(1_000_000) })
	if b := c.Now(); b != 1_000_001<<16 {
		t.Fatalf("logical overflow should carry into physical: got %x", b)
	}
}
