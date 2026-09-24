package agent

import (
	"fmt"
	"math/rand/v2"
	"os"
	"os/exec"
	"testing"
	"time"

	schemav1 "github.com/Shreyansh-Kanojiaa/theseus/schema/v1"
)

func sample(name string, v float64) *schemav1.Record {
	return &schemav1.Record{Body: &schemav1.Record_Sample{Sample: &schemav1.Sample{
		Name: name, Value: v, Labels: map[string]string{"instance": "node-c:9100", "mountpoint": "/"}}}}
}

func event(kind string) *schemav1.Record {
	return &schemav1.Record{Body: &schemav1.Record_Event{Event: &schemav1.Event{Kind: kind, Source: "postgres"}}}
}

func count(t testing.TB, s *Store, table string) int {
	t.Helper()
	var n int
	if err := s.db.QueryRow(`SELECT count(*) FROM ` + table).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}

func TestRetention(t *testing.T) {
	now := time.UnixMilli(1_800_000_000_000)
	wallClock = func() time.Time { return now }
	t.Cleanup(func() { wallClock = time.Now })

	s, err := OpenStore(t.TempDir(), "node-c", 0)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = s.Close() }()
	if err := s.Append(sample("old", 1), sample("old", 2), event("old")); err != nil {
		t.Fatal(err)
	}
	now = now.Add(8 * 24 * time.Hour)
	if err := s.Append(sample("new", 3), event("new")); err != nil {
		t.Fatal(err)
	}

	n, err := s.Prune(now.Add(-Retention))
	if err != nil {
		t.Fatal(err)
	}
	if n != 3 || count(t, s, "samples") != 1 || count(t, s, "events") != 1 {
		t.Fatalf("pruned %d, left %d samples %d events; want 3, 1, 1",
			n, count(t, s, "samples"), count(t, s, "events"))
	}
	var name string
	if err := s.db.QueryRow(`SELECT name FROM samples`).Scan(&name); err != nil || name != "new" {
		t.Fatalf("kept %q, want the new sample (err %v)", name, err)
	}
	if count(t, s, "spool") != 5 {
		t.Fatal("retention must not touch the spool")
	}
	if n, _ := s.Prune(now.Add(time.Hour)); n != 2 || count(t, s, "samples")+count(t, s, "events") != 0 {
		t.Fatalf("cutoff after everything should empty the tables, pruned %d", n)
	}
}

// TestKill9MidWrite SIGKILLs a child that is appending in a tight loop, then
// checks the database is intact, every committed record reached both its table
// and the spool, and seq keeps rising after the crash.
func TestKill9MidWrite(t *testing.T) {
	if dir := os.Getenv("THESEUS_CRASH_DIR"); dir != "" {
		s, err := OpenStore(dir, "node-c", 0)
		if err != nil {
			os.Exit(2)
		}
		batch := []*schemav1.Record{event("tick")}
		for i := range 50 {
			batch = append(batch, sample(fmt.Sprint("m", i), float64(i)))
		}
		for {
			if err := s.Append(batch...); err != nil {
				os.Exit(3)
			}
		}
	}

	dir := t.TempDir()
	for range 5 {
		cmd := exec.Command(os.Args[0], "-test.run=^TestKill9MidWrite$")
		cmd.Env = append(os.Environ(), "THESEUS_CRASH_DIR="+dir)
		if err := cmd.Start(); err != nil {
			t.Fatal(err)
		}
		time.Sleep(time.Duration(300+rand.IntN(300)) * time.Millisecond)
		_ = cmd.Process.Kill() // SIGKILL
		_ = cmd.Wait()
		if code := cmd.ProcessState.ExitCode(); code != -1 {
			t.Fatalf("writer exited on its own with code %d before the kill", code)
		}

		s, err := OpenStore(dir, "node-c", 0)
		if err != nil {
			t.Fatal(err)
		}
		var check string
		if err := s.db.QueryRow(`PRAGMA integrity_check`).Scan(&check); err != nil || check != "ok" {
			t.Fatalf("integrity_check = %q, %v", check, err)
		}
		spooled := count(t, s, "spool")
		if spooled == 0 || count(t, s, "samples")+count(t, s, "events") != spooled {
			t.Fatalf("torn write: %d samples + %d events vs %d spooled",
				count(t, s, "samples"), count(t, s, "events"), spooled)
		}
		var maxSeq uint64
		if err := s.db.QueryRow(`SELECT max(seq) FROM spool`).Scan(&maxSeq); err != nil {
			t.Fatal(err)
		}
		r := event("restart")
		if err := s.Append(r); err != nil {
			t.Fatal(err)
		}
		if r.Header().Seq <= maxSeq {
			t.Fatalf("seq reused after kill -9: got %d, max stored %d", r.Header().Seq, maxSeq)
		}
		if err := s.Close(); err != nil {
			t.Fatal(err)
		}
	}
}

// BenchmarkAppend100k writes 100k samples per op in batches of 1000
// (about one node_exporter scrape per batch).
func BenchmarkAppend100k(b *testing.B) {
	s, err := OpenStore(b.TempDir(), "node-c", 0)
	if err != nil {
		b.Fatal(err)
	}
	defer func() { _ = s.Close() }()
	batch := make([]*schemav1.Record, 1000)
	for i := range batch {
		batch[i] = sample(fmt.Sprint("node_metric_", i), float64(i))
	}
	ops := 0
	for b.Loop() {
		for range 100 {
			if err := s.Append(batch...); err != nil {
				b.Fatal(err)
			}
		}
		ops++
	}
	b.ReportMetric(float64(ops*100_000)/b.Elapsed().Seconds(), "records/s")
}
