package agent

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io/fs"
	"log"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"syscall"
	"time"

	"golang.org/x/sys/unix"
	"google.golang.org/protobuf/proto"
	"modernc.org/sqlite"
	sqlite3 "modernc.org/sqlite/lib"

	schemav1 "github.com/Shreyansh-Kanojiaa/theseus/schema/v1"
)

// Disk-full survival. The store shares its disk with everything else on the
// node, so it keeps a ballast file of preallocated blocks. The first write
// that fails for lack of space deletes the ballast and puts the store in
// degraded mode: non-essential samples are dropped (and counted) before they
// are stamped, everything else is still written. If even that fails, records
// wait in a bounded in-memory queue. RunDiskRecovery leaves degraded mode once
// the disk has room again and resolves the agent_disk_full incident.

// ballastName is the ballast file in the data dir.
const ballastName = "ballast"

// pendingCap bounds the in-memory queue used while even non-sample writes fail.
const pendingCap = 10_000

// recoverHeadroom is the free space needed on top of twice the ballast before
// degraded mode ends. A var so the tiny-tmpfs test can lower it.
var recoverHeadroom uint64 = 16 << 20

// isDiskFull reports whether err means the filesystem is out of space: ENOSPC
// anywhere in the chain, or SQLite's SQLITE_FULL (plain or extended code).
func isDiskFull(err error) bool {
	var se *sqlite.Error
	return errors.Is(err, syscall.ENOSPC) ||
		errors.As(err, &se) && se.Code()&0xff == sqlite3.SQLITE_FULL
}

// makeBallast allocates the ballast file with real blocks (fallocate, never
// sparse). It is a no-op for a ballast that is already fully allocated.
// ponytail: never shrinks; delete the file to apply a smaller -ballast.
func (s *Store) makeBallast() error {
	if s.ballast <= 0 {
		return nil
	}
	p := filepath.Join(s.dir, ballastName)
	f, err := os.OpenFile(p, os.O_WRONLY|os.O_CREATE, 0o600)
	if err != nil {
		return fmt.Errorf("ballast: %w", err)
	}
	err = unix.Fallocate(int(f.Fd()), 0, 0, s.ballast)
	if err == nil {
		err = f.Sync()
	}
	if cerr := f.Close(); err == nil {
		err = cerr
	}
	if err == nil {
		err = syncDir(s.dir)
	}
	if err != nil {
		_ = os.Remove(p)
		return fmt.Errorf("ballast: %w", err)
	}
	return nil
}

// KeepSamples names metrics degraded mode keeps besides the built-in essentials,
// e.g. the ones alert rules watch. Call it before collection starts.
func (s *Store) KeepSamples(names ...string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.keep == nil {
		s.keep = map[string]bool{}
	}
	for _, n := range names {
		s.keep[n] = true
	}
}

// essential reports whether degraded mode keeps a sample: disk and memory
// readings (the evidence for the disk-full incident), scrape and container
// liveness, and whatever KeepSamples named.
func (s *Store) essential(name string) bool {
	return strings.HasPrefix(name, "host_disk_") || strings.HasPrefix(name, "host_memory_") ||
		name == "up" || name == "container_running" || s.keep[name]
}

// enterDegraded frees the ballast, switches to degraded mode and queues the
// agent_disk_full incident ahead of whatever is written next, unless one is
// already open (left by a run that restarted on a full disk).
func (s *Store) enterDegraded(cause error) {
	s.degraded = true
	freed := "no ballast to free"
	if s.ballast > 0 {
		freed = fmt.Sprintf("freed %d-byte ballast", s.ballast)
		err := os.Remove(filepath.Join(s.dir, ballastName))
		if err == nil {
			err = syncDir(s.dir)
		}
		if err != nil && !errors.Is(err, fs.ErrNotExist) {
			freed = fmt.Sprintf("could not free ballast: %v", err)
		}
	}
	log.Printf("store: disk full, degraded mode: dropping non-essential samples, keeping events (%s): %v", freed, cause)
	if s.open != nil {
		log.Printf("store: incident %s is still open, not opening another", s.open.GetIncident().IncidentId)
		return
	}
	s.open = diskFullIncident(fmt.Sprintf("agent_disk_full-%d", wallClock().UnixMilli()), schemav1.IncidentState_INCIDENT_STATE_OPEN, 0)
	s.openHLC = 0
	s.pending = append(s.pending, s.open)
}

func diskFullIncident(id string, state schemav1.IncidentState, openedHLC uint64) *schemav1.Record {
	return &schemav1.Record{Body: &schemav1.Record_Incident{Incident: &schemav1.Incident{
		IncidentId: id,
		State:      state,
		Service:    "agent",
		Summary:    "agent_disk_full",
		OpenedHlc:  openedHLC,
		Header:     &schemav1.Header{Priority: schemav1.Priority_PRIORITY_INCIDENT},
	}}}
}

// resolveOpen returns the RESOLVED version of the open incident, whose OPEN
// record must already be stored (openHLC set), and forgets it.
func (s *Store) resolveOpen() *schemav1.Record {
	r := diskFullIncident(s.open.GetIncident().IncidentId, schemav1.IncidentState_INCIDENT_STATE_RESOLVED, s.openHLC)
	s.open, s.openHLC = nil, 0
	return r
}

// loadOpen finds an agent_disk_full incident a previous run left open: the
// latest agent incident, if it is an OPEN agent_disk_full.
func (s *Store) loadOpen() error {
	var body []byte
	err := s.db.QueryRow(`SELECT body FROM events WHERE kind = 'incident' AND source = 'agent'
		ORDER BY seq DESC LIMIT 1`).Scan(&body)
	if errors.Is(err, sql.ErrNoRows) {
		return nil
	}
	if err != nil {
		return err
	}
	r := &schemav1.Record{}
	if err := proto.Unmarshal(body, r); err != nil {
		return fmt.Errorf("latest agent incident: %w", err)
	}
	if i := r.GetIncident(); i.GetSummary() == "agent_disk_full" && i.GetState() == schemav1.IncidentState_INCIDENT_STATE_OPEN {
		s.open, s.openHLC = r, r.Header().Hlc
	}
	return nil
}

// put writes recs behind anything pending. Disk-full errors are absorbed:
// the first enters degraded mode and retries without non-essential samples;
// later ones leave the batch in the pending queue. Other errors are returned.
func (s *Store) put(recs []*schemav1.Record) error {
	if s.degraded {
		recs = slices.DeleteFunc(slices.Clone(recs), func(r *schemav1.Record) bool {
			if sm := r.GetSample(); sm != nil && !s.essential(sm.Name) {
				s.dropped++
				s.droppedTotal++
				return true
			}
			return false
		})
	}
	batch := append(s.pending, recs...)
	if len(batch) == 0 {
		return nil
	}
	err := s.write(batch)
	switch {
	case err == nil:
		s.pending = nil
		s.count(batch)
		if s.open != nil && s.openHLC == 0 && slices.Contains(batch, s.open) {
			s.openHLC = s.open.Header().Hlc // as stored: a failed write may have stamped it before
		}
		return nil
	case !isDiskFull(err):
		return err
	case !s.degraded:
		s.enterDegraded(err)
		return s.put(recs)
	}
	s.pending = evict(batch, pendingCap)
	return nil
}

// evict drops records until q fits in n: lowest priority first, oldest first
// within a priority, so incidents go last.
func evict(q []*schemav1.Record, n int) []*schemav1.Record {
	for len(q) > n {
		v := 0
		for i, r := range q {
			if r.Header().Priority < q[v].Header().Priority {
				v = i
			}
		}
		q = slices.Delete(q, v, v+1)
	}
	return q
}

// tryRecover leaves degraded mode if the data dir has room for twice the
// ballast plus headroom: it recreates the ballast and flushes the pending
// queue, then appends the RESOLVED agent_disk_full incident and
// agent_disk_recovered (with the dropped-sample count) in one batch.
func (s *Store) tryRecover() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.degraded {
		return nil
	}
	var st unix.Statfs_t
	if err := unix.Statfs(s.dir, &st); err != nil {
		return err
	}
	if st.Bavail*uint64(st.Bsize) < 2*uint64(max(s.ballast, 0))+recoverHeadroom {
		return nil
	}
	if err := s.makeBallast(); err != nil {
		if isDiskFull(err) { // filled up again since statfs
			return nil
		}
		return err
	}
	if err := s.put(nil); err != nil || len(s.pending) > 0 {
		return err // still full: try again next round
	}
	s.degraded = false
	log.Printf("store: disk has room again, leaving degraded mode (%d samples dropped)", s.dropped)
	var recs []*schemav1.Record
	if s.open != nil {
		recs = append(recs, s.resolveOpen())
	}
	recs = append(recs, &schemav1.Record{Body: &schemav1.Record_Event{Event: &schemav1.Event{
		Kind: "agent_disk_recovered", Source: "agent",
		Message: fmt.Sprintf("disk has room again, %d samples dropped", s.dropped),
		Attrs:   map[string]string{"dropped_samples": strconv.FormatUint(s.dropped, 10)},
	}}})
	s.dropped = 0
	return s.put(recs)
}

// RunDiskRecovery checks every interval, while degraded, whether the disk has
// room again. Run it for the life of the store.
func (s *Store) RunDiskRecovery(ctx context.Context, every time.Duration) {
	t := time.NewTicker(every)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			if err := s.tryRecover(); err != nil {
				log.Printf("store: disk recovery: %v", err)
			}
		}
	}
}
