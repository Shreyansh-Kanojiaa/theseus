package agent

import (
	"fmt"
	"io"
	"maps"
	"slices"
	"strings"

	schemav1 "github.com/Shreyansh-Kanojiaa/theseus/schema/v1"
)

// count records a successfully written batch in the /metrics counters.
// Callers hold s.mu.
func (s *Store) count(recs []*schemav1.Record) {
	for _, r := range recs {
		switch b := r.Body.(type) {
		case *schemav1.Record_Sample:
			s.written["sample"]++
		case *schemav1.Record_Event:
			s.written["event"]++
		case *schemav1.Record_ProbeResult:
			s.written["probe_result"]++
			n := s.probeFails[b.ProbeResult.Target] // exported from the first probe, at 0 while healthy
			if !b.ProbeResult.Ok {
				n++
			}
			s.probeFails[b.ProbeResult.Target] = n
		case *schemav1.Record_Incident:
			s.written["incident"]++
		}
	}
}

var labelEscaper = strings.NewReplacer(`\`, `\\`, `"`, `\"`, "\n", `\n`)

// WriteMetrics writes the agent's own metrics in the Prometheus text format.
func (s *Store) WriteMetrics(w io.Writer) error {
	var depth int64
	if err := s.db.QueryRow(`SELECT count(*) FROM spool`).Scan(&depth); err != nil {
		return err
	}
	s.mu.Lock()
	written, fails, degraded := maps.Clone(s.written), maps.Clone(s.probeFails), s.degraded
	s.mu.Unlock()

	var b strings.Builder
	counter := func(name, help, label string, m map[string]uint64) {
		fmt.Fprintf(&b, "# HELP %s %s\n# TYPE %s counter\n", name, help, name)
		for _, k := range slices.Sorted(maps.Keys(m)) {
			fmt.Fprintf(&b, "%s{%s=\"%s\"} %d\n", name, label, labelEscaper.Replace(k), m[k])
		}
	}
	gauge := func(name, help string, v int64) {
		fmt.Fprintf(&b, "# HELP %s %s\n# TYPE %s gauge\n%s %d\n", name, help, name, name, v)
	}
	counter("theseus_agent_records_written_total", "Records written to the local store, by type.", "type", written)
	counter("theseus_agent_probe_failures_total", "Failed health probes, by target container.", "target", fails)
	gauge("theseus_agent_spool_depth", "Records in the sync spool, waiting for the control plane to ack them.", depth)
	d := int64(0)
	if degraded {
		d = 1
	}
	gauge("theseus_agent_disk_degraded", "1 while the store is in disk-full degraded mode, dropping samples.", d)
	_, err := io.WriteString(w, b.String())
	return err
}
