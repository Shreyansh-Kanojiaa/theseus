package agent

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"

	"golang.org/x/sys/unix"
	"google.golang.org/protobuf/proto"

	schemav1 "github.com/Shreyansh-Kanojiaa/theseus/schema/v1"
)

var errENOSPC = fmt.Errorf("write stamp: %w", &fs.PathError{Op: "write", Path: "stamp.tmp", Err: syscall.ENOSPC})

// stored returns every record in the spool, in seq order.
func stored(t testing.TB, s *Store) []*schemav1.Record {
	t.Helper()
	rows, err := s.db.Query(`SELECT seq, body FROM spool ORDER BY seq`)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = rows.Close() }()
	var out []*schemav1.Record
	for rows.Next() {
		var seq uint64
		var b []byte
		r := &schemav1.Record{}
		if err := rows.Scan(&seq, &b); err != nil {
			t.Fatal(err)
		}
		if err := proto.Unmarshal(b, r); err != nil {
			t.Fatal(err)
		}
		if r.Header().Seq != seq {
			t.Fatalf("row seq %d holds a record stamped %d", seq, r.Header().Seq)
		}
		out = append(out, r)
	}
	return out
}

// kinds names stored non-sample records in seq order: the event kind, "probe"
// or "incident".
func kinds(t testing.TB, s *Store) []string {
	var out []string
	for _, r := range stored(t, s) {
		switch b := r.Body.(type) {
		case *schemav1.Record_Event:
			out = append(out, b.Event.Kind)
		case *schemav1.Record_ProbeResult:
			out = append(out, "probe")
		case *schemav1.Record_Incident:
			out = append(out, "incident")
		}
	}
	return out
}

// allocated is how many bytes the file really occupies on disk (st_blocks).
func allocated(t testing.TB, path string) int64 {
	t.Helper()
	var st unix.Stat_t
	if err := unix.Stat(path, &st); err != nil {
		t.Fatal(err)
	}
	return st.Blocks * 512
}

func TestIsDiskFull(t *testing.T) {
	for _, c := range []struct {
		err  error
		want bool
	}{
		{errENOSPC, true},
		{syscall.ENOSPC, true},
		{fmt.Errorf("x: %w", os.ErrNotExist), false},
		{errors.New("no space left on device"), false}, // text is not enough
		{nil, false},
	} {
		if got := isDiskFull(c.err); got != c.want {
			t.Errorf("isDiskFull(%v) = %v, want %v", c.err, got, c.want)
		}
	}
	// SQLITE_FULL is covered with a real one in TestSQLiteFull.
}

func TestBallast(t *testing.T) {
	dir := t.TempDir()
	s, err := OpenStore(dir, "node-c", 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = s.Close() }()
	if got := allocated(t, filepath.Join(dir, ballastName)); got < 1<<20 {
		t.Fatalf("ballast has %d bytes of blocks, want >= 1 MiB (sparse?)", got)
	}
}

func TestDegradedMode(t *testing.T) {
	dir := t.TempDir()
	s, err := OpenStore(dir, "node-c", 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = s.Close() }()
	appendOK := func(recs ...*schemav1.Record) {
		t.Helper()
		if err := s.Append(recs...); err != nil {
			t.Fatal(err)
		}
	}
	probe := &schemav1.Record{Body: &schemav1.Record_ProbeResult{ProbeResult: &schemav1.ProbeResult{Target: "db"}}}

	appendOK(sample("before", 1))

	// Enter: the first ENOSPC frees the ballast and retries without the sample.
	calls := 0
	s.fault = func() error {
		if calls++; calls == 1 {
			return errENOSPC
		}
		return nil
	}
	appendOK(sample("lost", 2), event("a"))
	if !s.degraded || s.dropped != 1 {
		t.Fatalf("degraded %v dropped %d, want true 1", s.degraded, s.dropped)
	}
	if _, err := os.Stat(filepath.Join(dir, ballastName)); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("ballast should be gone: %v", err)
	}

	// Degraded: samples dropped before stamping, events still written.
	var failing bool
	s.fault = func() error {
		if failing {
			return errENOSPC
		}
		return nil
	}
	seq := s.stamp.seq
	appendOK(sample("x", 3), sample("y", 4))
	if s.stamp.seq != seq || s.dropped != 3 {
		t.Fatalf("dropped samples burned seqs (%d -> %d) or were not counted (%d)", seq, s.stamp.seq, s.dropped)
	}
	appendOK(event("b"))

	// Even events fail: they wait in memory and Append still succeeds.
	failing = true
	appendOK(event("c"), probe)
	appendOK(sample("z", 5))
	if len(s.pending) != 2 || s.dropped != 4 {
		t.Fatalf("pending %d dropped %d, want 2 4", len(s.pending), s.dropped)
	}
	failing = false

	// Recover: ballast back, queue flushed in order, then the recovered event.
	if err := s.tryRecover(); err != nil {
		t.Fatal(err)
	}
	if s.degraded || len(s.pending) != 0 {
		t.Fatalf("still degraded %v or pending %d", s.degraded, len(s.pending))
	}
	if got := allocated(t, filepath.Join(dir, ballastName)); got < 1<<20 {
		t.Fatalf("ballast not recreated: %d bytes", got)
	}
	appendOK(sample("after", 6))

	got := strings.Join(kinds(t, s), " ")
	if want := "incident a b c probe agent_disk_recovered"; got != want {
		t.Fatalf("stored %q, want %q", got, want)
	}
	recs := stored(t, s)
	var names []string
	for _, r := range recs {
		if sm := r.GetSample(); sm != nil {
			names = append(names, sm.Name)
		}
		if e := r.GetEvent(); e != nil && e.Kind == "agent_disk_recovered" && e.Attrs["dropped_samples"] != "4" {
			t.Errorf("recovered event attrs %v, want dropped_samples=4", e.Attrs)
		}
		if i := r.GetIncident(); i != nil && (i.Summary != "agent_disk_full" || r.Header().Priority != schemav1.Priority_PRIORITY_INCIDENT) {
			t.Errorf("incident %v", r)
		}
	}
	if fmt.Sprint(names) != "[before after]" {
		t.Fatalf("samples %v, want [before after]", names)
	}
}

func TestEvict(t *testing.T) {
	rec := func(p schemav1.Priority, name string) *schemav1.Record {
		return &schemav1.Record{Body: &schemav1.Record_Event{Event: &schemav1.Event{
			Kind: name, Header: &schemav1.Header{Priority: p}}}}
	}
	q := []*schemav1.Record{
		rec(schemav1.Priority_PRIORITY_INCIDENT, "inc1"),
		rec(schemav1.Priority_PRIORITY_EVENT, "ev1"),
		rec(schemav1.Priority_PRIORITY_METRIC, "probe1"),
		rec(schemav1.Priority_PRIORITY_ACTION, "act1"),
		rec(schemav1.Priority_PRIORITY_EVENT, "ev2"),
		rec(schemav1.Priority_PRIORITY_METRIC, "probe2"),
		rec(schemav1.Priority_PRIORITY_INCIDENT, "inc2"),
	}
	for _, c := range []struct {
		n    int
		want string
	}{
		{5, "inc1 ev1 act1 ev2 inc2"}, // metrics go first
		{4, "inc1 act1 ev2 inc2"},     // then the oldest event
		{2, "inc1 inc2"},
		{1, "inc2"}, // incidents last, oldest first
	} {
		q = evict(q, c.n)
		var got []string
		for _, r := range q {
			got = append(got, r.GetEvent().Kind)
		}
		if strings.Join(got, " ") != c.want {
			t.Fatalf("evict to %d: %v, want %s", c.n, got, c.want)
		}
	}
}

// TestSQLiteFull gets a real SQLITE_FULL from SQLite (max_page_count), checks
// it is recognised, and that the write connection works afterwards.
func TestSQLiteFull(t *testing.T) {
	s, err := OpenStore(t.TempDir(), "node-c", 0)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = s.Close() }()
	if err := s.Append(event("warm")); err != nil { // opens the write connection
		t.Fatal(err)
	}
	limit := func(pages string) {
		t.Helper()
		if s.w == nil {
			t.Fatal("write connection was discarded")
		}
		if _, err := s.w.ExecContext(t.Context(), "PRAGMA max_page_count = "+pages); err != nil {
			t.Fatal(err)
		}
	}
	limit("1") // below the current size: any new page fails
	big := func() []*schemav1.Record {
		var b []*schemav1.Record
		for range 200 {
			e := event("big")
			e.GetEvent().Message = strings.Repeat("x", 4096)
			b = append(b, e)
		}
		return b
	}

	err = s.write(big())
	if !isDiskFull(err) {
		t.Fatalf("write past max_page_count: %v, want SQLITE_FULL recognised", err)
	}
	if err := s.Append(big()...); err != nil {
		t.Fatal(err)
	}
	if !s.degraded || len(s.pending) != 201 { // the incident + 200 events
		t.Fatalf("degraded %v pending %d, want true 201", s.degraded, len(s.pending))
	}

	limit("1073741823")
	if err := s.Append(event("later")); err != nil {
		t.Fatalf("write after SQLITE_FULL: %v", err)
	}
	if len(s.pending) != 0 || len(kinds(t, s)) != 1+1+200+1 {
		t.Fatalf("pending %d stored %d, want the queue flushed", len(s.pending), len(kinds(t, s)))
	}
}

// TestDiskFullTinyFS fills a real, small filesystem under the store. It runs
// only when THESEUS_TINY_FS names a small (~16 MiB) tmpfs: CI mounts one, and
// `make test-diskfull` runs it in a container.
func TestDiskFullTinyFS(t *testing.T) {
	tiny := os.Getenv("THESEUS_TINY_FS")
	if tiny == "" {
		t.Skip("THESEUS_TINY_FS not set")
	}
	defer func(h uint64) { recoverHeadroom = h }(recoverHeadroom)
	recoverHeadroom = 1 << 20 // 16 MiB headroom cannot fit on a 16 MiB fs
	const ballast = 2 << 20

	dir, err := os.MkdirTemp(tiny, "store-")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = os.RemoveAll(dir) }()
	s, err := OpenStore(dir, "node-c", ballast)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = s.Close() }()
	if got := allocated(t, filepath.Join(dir, ballastName)); got < ballast {
		t.Fatalf("ballast has %d bytes of blocks, want %d", got, ballast)
	}

	var events []*schemav1.Record // every event appended, in order
	round := func(i int) {
		t.Helper()
		e := event(fmt.Sprint("tick", i))
		events = append(events, e)
		if err := s.Append(sample("m", float64(i)), sample("n", float64(i)), e); err != nil {
			t.Fatalf("round %d: %v", i, err)
		}
	}
	countEvents := func() int {
		n := 0
		for _, k := range kinds(t, s) {
			if strings.HasPrefix(k, "tick") {
				n++
			}
		}
		return n
	}
	fillPath := filepath.Join(tiny, "filler-"+filepath.Base(dir))
	filler, err := os.Create(fillPath)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = filler.Close(); _ = os.Remove(fillPath) }()
	fill := func() {
		t.Helper()
		buf := make([]byte, 64<<10)
		for {
			if _, err := filler.Write(buf); err != nil {
				if !errors.Is(err, syscall.ENOSPC) {
					t.Fatal(err)
				}
				return
			}
		}
	}

	i := 0
	for ; i < 20; i++ {
		round(i)
	}
	fill()
	for range 20 {
		round(i)
		i++
	}
	if !s.degraded || s.dropped == 0 {
		t.Fatalf("after filling: degraded %v, dropped %d", s.degraded, s.dropped)
	}
	if k := kinds(t, s); !strings.Contains(strings.Join(k, " "), "incident") {
		t.Fatal("agent_disk_full incident not stored")
	}
	if n := countEvents(); n != i || len(s.pending) != 0 {
		t.Fatalf("during the fill: %d of %d events stored, %d pending", n, i, len(s.pending))
	}
	t.Logf("filled: incident stored, %d/%d events stored, %d samples dropped", countEvents(), i, s.dropped)

	// The fault eats the space the ballast freed: events now wait in memory.
	fill()
	for range 10 {
		round(i)
		i++
	}
	if len(s.pending) == 0 {
		t.Fatal("refilled disk: want events held in memory")
	}
	t.Logf("refilled: %d records pending in memory", len(s.pending))
	dropped := s.dropped

	_ = filler.Close()
	if err := os.Remove(fillPath); err != nil {
		t.Fatal(err)
	}
	if err := s.tryRecover(); err != nil {
		t.Fatal(err)
	}
	if s.degraded {
		t.Fatal("still degraded after freeing the disk")
	}
	if got := allocated(t, filepath.Join(dir, ballastName)); got < ballast {
		t.Fatalf("ballast not recreated: %d bytes", got)
	}
	var samples int
	countSamples := func() {
		samples = 0
		for _, r := range stored(t, s) {
			if r.GetSample() != nil {
				samples++
			}
		}
	}
	countSamples()
	before := samples
	for range 5 {
		round(i)
		i++
	}
	if countSamples(); samples != before+10 {
		t.Fatalf("samples after recovery: %d -> %d, want +10", before, samples)
	}

	var recovered bool
	for _, r := range stored(t, s) {
		if e := r.GetEvent(); e != nil && e.Kind == "agent_disk_recovered" {
			recovered = e.Attrs["dropped_samples"] == fmt.Sprint(dropped)
		}
	}
	if !recovered {
		t.Fatalf("no agent_disk_recovered event with dropped_samples=%d", dropped)
	}
	// Every event is stored once, and seqs rise in the order they were appended.
	if n := countEvents(); n != len(events) {
		t.Fatalf("%d of %d events stored", n, len(events))
	}
	for j := 1; j < len(events); j++ {
		if events[j].Header().Seq <= events[j-1].Header().Seq {
			t.Fatalf("event %d has seq %d after %d", j, events[j].Header().Seq, events[j-1].Header().Seq)
		}
	}
	t.Logf("recovered: ballast back, %d events stored in seq order, %d samples dropped", len(events), dropped)
}
