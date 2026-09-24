package agent

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	schemav1 "github.com/Shreyansh-Kanojiaa/theseus/schema/v1"
)

var wallClock = time.Now // overridden in tests

// Stamper assigns node_id, seq and HLC to record headers. The last seq and
// HLC are made durable before a stamp is handed out, so after a restart
// (including kill -9 or power loss) a seq is never reused and the HLC never
// goes backwards. A crash can leave a gap in seq; gaps are allowed.
type Stamper struct {
	mu     sync.Mutex
	path   string
	nodeID string
	seq    uint64
	clock  *schemav1.Clock
}

// OpenStamper loads state from path, or starts at zero if it does not exist.
func OpenStamper(path, nodeID string) (*Stamper, error) {
	var seq, hlc uint64
	b, err := os.ReadFile(path)
	switch {
	case errors.Is(err, os.ErrNotExist):
	case err != nil:
		return nil, err
	default:
		if _, err := fmt.Sscan(string(b), &seq, &hlc); err != nil {
			return nil, fmt.Errorf("corrupt stamp state %s: %w", path, err)
		}
	}
	return &Stamper{path: path, nodeID: nodeID, seq: seq, clock: schemav1.NewClock(hlc, wallClock)}, nil
}

// Stamp fills node_id, seq and hlc on each header. Priority is left to the
// caller. On error the headers must be discarded.
// ponytail: two fsyncs per call; lease seq blocks if per-record stamping gets hot.
func (s *Stamper) Stamp(hs ...*schemav1.Header) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	var hlc uint64
	for _, h := range hs {
		s.seq++
		hlc = s.clock.Now()
		h.NodeId, h.Seq, h.Hlc = s.nodeID, s.seq, hlc
	}
	return s.persist(hlc)
}

// persist atomically replaces the state file and fsyncs it and its directory.
func (s *Stamper) persist(hlc uint64) error {
	tmp := s.path + ".tmp"
	f, err := os.Create(tmp)
	if err != nil {
		return err
	}
	_, err = fmt.Fprintf(f, "%d %d\n", s.seq, hlc)
	if err == nil {
		err = f.Sync()
	}
	if cerr := f.Close(); err == nil {
		err = cerr
	}
	if err == nil {
		err = os.Rename(tmp, s.path)
	}
	if err != nil {
		return err
	}
	return syncDir(filepath.Dir(s.path))
}

// syncDir fsyncs a directory so a create, rename or delete in it is durable.
func syncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	err = d.Sync()
	if cerr := d.Close(); err == nil {
		err = cerr
	}
	return err
}
