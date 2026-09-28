package sync

import (
	"context"
	"fmt"
	"net"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/Shreyansh-Kanojiaa/theseus/agent"
	"github.com/Shreyansh-Kanojiaa/theseus/controlplane"
	schemav1 "github.com/Shreyansh-Kanojiaa/theseus/schema/v1"
)

// serve starts a control plane storing in dir and returns its address and a
// stop func.
func serve(t *testing.T, dir string) (*controlplane.Server, string, func()) {
	t.Helper()
	cp, err := controlplane.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	g := cp.GRPC()
	go func() { _ = g.Serve(l) }()
	return cp, l.Addr().String(), func() { g.Stop(); _ = cp.Close() }
}

func spooled(t *testing.T, s *agent.Store) int {
	t.Helper()
	n := 0
	if _, err := s.ScanSpool(context.Background(), func(*schemav1.Record) error { n++; return nil }); err != nil {
		t.Fatal(err)
	}
	return n
}

func appendN(t *testing.T, s *agent.Store, n int) {
	t.Helper()
	recs := make([]*schemav1.Record, 0, n)
	for i := range n {
		if i%100 == 0 {
			recs = append(recs, &schemav1.Record{Body: &schemav1.Record_Event{Event: &schemav1.Event{Kind: "probe_fail", Source: "pg"}}})
			continue
		}
		recs = append(recs, &schemav1.Record{Body: &schemav1.Record_Sample{Sample: &schemav1.Sample{
			Name: "node_cpu_seconds_total", Value: float64(i),
			Labels: map[string]string{"cpu": fmt.Sprint(i % 8), "mode": "idle", "instance": "node-exporter:9100"},
		}}})
	}
	if err := s.Append(recs...); err != nil {
		t.Fatal(err)
	}
}

func TestNaiveSync(t *testing.T) {
	ctx := context.Background()
	st, err := agent.OpenStore(t.TempDir(), "c", 0)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = st.Close() }()
	cpDir := t.TempDir()
	cp, addr, stop := serve(t, cpDir)

	for range 10 {
		appendN(t, st, 1000)
	}
	n, err := NewNaive(st, addr)
	if err != nil {
		t.Fatal(err)
	}
	r, err := n.Sync(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if r.Records != 10000 || r.Stored != 10000 || r.Duplicates != 0 || r.Payload < 2*chunkBytes || r.Wire <= r.Payload {
		t.Fatalf("first sync: %+v", r)
	}
	if got := spooled(t, st); got != 0 {
		t.Fatalf("spool after ack: %d records, want 0", got)
	}
	if held, first, last, err := cp.Held("c"); err != nil || held != 10000 || first != 1 || last != 10000 {
		t.Fatalf("held %d [%d, %d], %v", held, first, last, err)
	}

	// The same seq again is a duplicate; a record without a seq is refused.
	up, err := n.client.Upload(ctx)
	if err != nil {
		t.Fatal(err)
	}
	dup := &schemav1.Record{Body: &schemav1.Record_Event{Event: &schemav1.Event{Header: &schemav1.Header{NodeId: "c", Seq: 7}}}}
	if err := up.Send(&schemav1.UploadRequest{Records: []*schemav1.Record{dup}}); err != nil {
		t.Fatal(err)
	}
	if res, err := up.CloseAndRecv(); err != nil || res.Received != 1 || res.Duplicates != 1 {
		t.Fatalf("duplicate: %v, %v", res, err)
	}
	up, _ = n.client.Upload(ctx)
	_ = up.Send(&schemav1.UploadRequest{Records: []*schemav1.Record{{Body: &schemav1.Record_Event{Event: &schemav1.Event{}}}}})
	if _, err := up.CloseAndRecv(); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("headerless record: %v", err)
	}

	// Control plane down: the sync fails and the spool keeps everything.
	stop()
	appendN(t, st, 5)
	if _, err := n.Sync(ctx); err == nil {
		t.Fatal("sync with the control plane down succeeded")
	}
	if got := spooled(t, st); got != 5 {
		t.Fatalf("spool after a failed sync: %d records, want 5", got)
	}
	_ = n.Close()

	// Back up (same store, new address): the backlog goes up and nothing is missing.
	cp, addr, stop = serve(t, cpDir)
	defer stop()
	if n, err = NewNaive(st, addr); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = n.Close() }()
	if r, err = n.Sync(ctx); err != nil || r.Records != 5 || r.Stored != 5 {
		t.Fatalf("reconnect sync: %+v, %v", r, err)
	}
	if held, first, last, err := cp.Held("c"); err != nil || held != 10005 || last-first+1 != held {
		t.Fatalf("held %d [%d, %d], %v", held, first, last, err)
	}
}
