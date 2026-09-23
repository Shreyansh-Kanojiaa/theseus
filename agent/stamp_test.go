package agent

import (
	"path/filepath"
	"testing"
	"time"

	schemav1 "github.com/Shreyansh-Kanojiaa/theseus/schema/v1"
)

func TestSeqSurvivesRestart(t *testing.T) {
	path := filepath.Join(t.TempDir(), "stamp")
	wallClock = func() time.Time { return time.UnixMilli(5_000_000) }
	t.Cleanup(func() { wallClock = time.Now })

	s, err := OpenStamper(path, "node-c")
	if err != nil {
		t.Fatal(err)
	}
	a, b, c := &schemav1.Header{}, &schemav1.Header{}, &schemav1.Header{}
	if err := s.Stamp(a, b, c); err != nil {
		t.Fatal(err)
	}
	if a.Seq != 1 || c.Seq != 3 || a.NodeId != "node-c" || a.Hlc >= b.Hlc || b.Hlc >= c.Hlc {
		t.Fatalf("bad stamps: %v %v %v", a, b, c)
	}

	// Restart with the wall clock an hour behind (node rebooted without an RTC).
	wallClock = func() time.Time { return time.UnixMilli(5_000_000).Add(-time.Hour) }
	s, err = OpenStamper(path, "node-c")
	if err != nil {
		t.Fatal(err)
	}
	d := &schemav1.Header{}
	if err := s.Stamp(d); err != nil {
		t.Fatal(err)
	}
	if d.Seq != 4 {
		t.Errorf("seq after restart = %d, want 4", d.Seq)
	}
	if d.Hlc <= c.Hlc {
		t.Errorf("hlc went backwards across restart: %x <= %x", d.Hlc, c.Hlc)
	}
}
