package agent

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"errors"
	"log"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"google.golang.org/protobuf/proto"
	_ "modernc.org/sqlite" // registers the "sqlite" driver

	schemav1 "github.com/Shreyansh-Kanojiaa/theseus/schema/v1"
)

// Retention is how long samples and events are kept locally.
const Retention = 7 * 24 * time.Hour

const ddl = `
CREATE TABLE IF NOT EXISTS samples (
	seq    INTEGER PRIMARY KEY,
	hlc    INTEGER NOT NULL,
	name   TEXT NOT NULL,
	labels TEXT NOT NULL, -- JSON object
	value  REAL NOT NULL
);
CREATE TABLE IF NOT EXISTS events (
	seq      INTEGER PRIMARY KEY,
	hlc      INTEGER NOT NULL,
	priority INTEGER NOT NULL,
	kind     TEXT NOT NULL, -- Event.kind, or "probe" / "incident"
	source   TEXT NOT NULL,
	body     BLOB NOT NULL  -- marshalled schemav1.Record
);
-- Records waiting for the control plane. Rows leave on ack, never by age.
CREATE TABLE IF NOT EXISTS spool (
	seq      INTEGER PRIMARY KEY,
	priority INTEGER NOT NULL,
	body     BLOB NOT NULL  -- marshalled schemav1.Record
);`

// Store is the agent's local SQLite store.
type Store struct {
	mu      sync.Mutex // one Append at a time, so commit order is seq order
	db      *sql.DB
	w       *sql.Conn // the write connection; nil until (re)opened
	stamp   *Stamper
	dir     string
	ballast int64

	// Disk-full state (see diskfull.go), guarded by mu.
	degraded bool
	dropped  uint64             // samples dropped since degraded mode began
	pending  []*schemav1.Record // unstamped, oldest first, flushed by the next write

	fault func() error // tests only: fails a write after stamping
}

// OpenStore opens (or creates) the store in dir and allocates a ballast of
// ballast bytes (0 for none). If the disk is too full for the ballast the
// store starts in degraded mode.
func OpenStore(dir, nodeID string, ballast int64) (*Store, error) {
	st, err := OpenStamper(filepath.Join(dir, "stamp"), nodeID)
	if err != nil {
		return nil, err
	}
	// synchronous=NORMAL: a power cut can lose the last commits but never
	// corrupts, and lost commits only leave seq gaps because stamps are durable first.
	db, err := sql.Open("sqlite", filepath.Join(dir, "theseus.db")+
		"?_pragma=journal_mode(WAL)&_pragma=synchronous(NORMAL)&_pragma=busy_timeout(5000)&_txlock=immediate")
	if err != nil {
		return nil, err
	}
	if _, err := db.Exec(ddl); err != nil {
		return nil, errors.Join(err, db.Close())
	}
	s := &Store{db: db, stamp: st, dir: dir, ballast: ballast}
	if err := s.makeBallast(); err != nil {
		if !isDiskFull(err) {
			return nil, errors.Join(err, db.Close())
		}
		s.enterDegraded(err)
		if err := s.put(nil); err != nil { // store the incident now if there is room
			return nil, errors.Join(err, s.Close())
		}
	}
	return s, nil
}

// Close closes the database.
func (s *Store) Close() error {
	if s.w != nil {
		_ = s.w.Close()
	}
	return s.db.Close()
}

// Append stamps the records and writes them to their table and the spool in
// one transaction. An unset priority defaults by record type. Appends are
// serialised: a reader never sees seq n+1 committed before seq n.
//
// When the disk is full Append does not fail: see diskfull.go. Samples are
// dropped and other records are written, or held in memory until they can be.
func (s *Store) Append(recs ...*schemav1.Record) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, r := range recs {
		h := r.Header()
		if h == nil {
			return errors.New("append: empty record")
		}
		if h.Priority == schemav1.Priority_PRIORITY_UNSPECIFIED {
			h.Priority = defaultPriority(r)
		}
	}
	return s.put(recs)
}

// write stamps recs and commits them in one transaction. After an error the
// headers are stale; the next write stamps them afresh, so a seq from a
// failed write is skipped, never reused.
func (s *Store) write(recs []*schemav1.Record) error {
	hs := make([]*schemav1.Header, len(recs))
	for i, r := range recs {
		hs[i] = r.Header()
	}
	if err := s.stamp.Stamp(hs...); err != nil {
		return err
	}
	if s.fault != nil {
		if err := s.fault(); err != nil {
			return err
		}
	}
	err := s.commit(recs, hs)
	if err != nil {
		s.rollback()
	}
	return err
}

// rollback clears any transaction a failed write left open on the write
// connection; SQLite can keep one open after SQLITE_FULL in COMMIT. If the
// ROLLBACK itself fails, the connection is discarded and the next write
// opens a fresh one.
func (s *Store) rollback() {
	if s.w == nil {
		return
	}
	_, err := s.w.ExecContext(context.Background(), "ROLLBACK")
	if err == nil || strings.Contains(err.Error(), "no transaction is active") {
		return
	}
	_ = s.w.Raw(func(any) error { return driver.ErrBadConn }) // closes the driver conn
	_ = s.w.Close()
	s.w = nil
}

func (s *Store) commit(recs []*schemav1.Record, hs []*schemav1.Header) error {
	if s.w == nil {
		w, err := s.db.Conn(context.Background())
		if err != nil {
			return err
		}
		s.w = w
	}
	tx, err := s.w.BeginTx(context.Background(), nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	for i, r := range recs {
		h := hs[i]
		body, err := proto.Marshal(r)
		if err != nil {
			return err
		}
		if _, err := tx.Exec(`INSERT INTO spool VALUES (?, ?, ?)`, h.Seq, h.Priority, body); err != nil {
			return err
		}
		var kind, source string
		switch b := r.Body.(type) {
		case *schemav1.Record_Sample:
			labels, _ := json.Marshal(b.Sample.Labels) // map[string]string cannot fail
			if _, err := tx.Exec(`INSERT INTO samples VALUES (?, ?, ?, ?, ?)`,
				h.Seq, h.Hlc, b.Sample.Name, string(labels), b.Sample.Value); err != nil {
				return err
			}
			continue
		case *schemav1.Record_Event:
			kind, source = b.Event.Kind, b.Event.Source
		case *schemav1.Record_ProbeResult:
			kind, source = "probe", b.ProbeResult.Target
		case *schemav1.Record_Incident:
			kind, source = "incident", b.Incident.Service
		}
		if _, err := tx.Exec(`INSERT INTO events VALUES (?, ?, ?, ?, ?, ?)`,
			h.Seq, h.Hlc, h.Priority, kind, source, body); err != nil {
			return err
		}
	}
	return tx.Commit()
}

func defaultPriority(r *schemav1.Record) schemav1.Priority {
	switch r.Body.(type) {
	case *schemav1.Record_Incident:
		return schemav1.Priority_PRIORITY_INCIDENT
	case *schemav1.Record_Event:
		return schemav1.Priority_PRIORITY_EVENT
	default:
		return schemav1.Priority_PRIORITY_METRIC
	}
}

// Prune deletes samples and events stamped before cutoff and returns how many
// rows it removed. The spool is not touched.
//
// seq and hlc are stamped together under one lock, so hlc rises with seq and a
// primary-key range delete does the job of an hlc index: the subquery scans
// exactly the rows being deleted and stops at the first one to keep.
func (s *Store) Prune(cutoff time.Time) (int64, error) {
	hlc := uint64(max(cutoff.UnixMilli(), 0)) << 16
	var n int64
	for _, t := range []string{"samples", "events"} {
		res, err := s.db.Exec(`DELETE FROM `+t+` WHERE seq < coalesce(
			(SELECT seq FROM `+t+` WHERE hlc >= ? ORDER BY seq LIMIT 1),
			(SELECT max(seq) + 1 FROM `+t+`))`, hlc)
		if err != nil {
			return n, err
		}
		k, _ := res.RowsAffected()
		n += k
	}
	return n, nil
}

// RunRetention prunes data older than keep every interval until ctx is done.
func (s *Store) RunRetention(ctx context.Context, keep, every time.Duration) {
	t := time.NewTicker(every)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			if n, err := s.Prune(wallClock().Add(-keep)); err != nil {
				log.Printf("retention: %v", err)
			} else if n > 0 {
				log.Printf("retention: pruned %d rows", n)
			}
		}
	}
}
