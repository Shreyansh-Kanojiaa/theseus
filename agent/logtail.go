package agent

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	schemav1 "github.com/Shreyansh-Kanojiaa/theseus/schema/v1"
)

// LogsLabel marks a container for log tailing (any value). Containers
// labelled ProbeLabel are tailed too.
const LogsLabel = "theseus.logs"

// maxLogLine bounds a line kept in the buffer or put in an event.
const maxLogLine = 1024

// LogTail keeps the last n log lines of every labelled container, stopped
// ones included, and turns keyword matches into log_match events. The
// buffers are the evidence handed to the recovery engine.
type LogTail struct {
	d        *Docker
	n        int
	keywords []string
	start    time.Time // lines older than this are buffered but never matched

	mu   sync.Mutex
	tail map[string]*logBuf // by container name
}

type logBuf struct {
	id    string
	last  time.Time // timestamp of the newest line read
	lines []string  // oldest first, at most n
}

// NewLogTail tails up to n lines per container and matches keywords as
// case-sensitive substrings.
func NewLogTail(d *Docker, n int, keywords []string) *LogTail {
	return &LogTail{d: d, n: max(n, 1), keywords: keywords, start: wallClock(), tail: map[string]*logBuf{}}
}

// Lines returns the buffered lines of container name, oldest first, each
// prefixed with its timestamp.
func (l *LogTail) Lines(name string) []string {
	l.mu.Lock()
	defer l.mu.Unlock()
	if b := l.tail[name]; b != nil {
		return slices.Clone(b.lines)
	}
	return nil
}

// Collect reads what each labelled container logged since the last round.
// It emits at most one log_match per container and keyword per round, with
// the first matching line as the message and the number of matches in attrs.
// ponytail: polls with tail=n, so a container writing more than n lines per
// round loses the excess; stream with follow=1 if that matters.
func (l *LogTail) Collect(ctx context.Context) ([]*schemav1.Record, error) {
	cs, err := l.d.Containers(ctx)
	if err != nil {
		return nil, err
	}
	var recs []*schemav1.Record
	var errs []error
	seen := map[string]bool{}
	for _, c := range cs {
		_, logs := c.Labels[LogsLabel]
		_, probe := c.Labels[ProbeLabel]
		if !logs && !probe {
			continue
		}
		name := c.Name()
		seen[name] = true
		l.mu.Lock()
		b := l.tail[name]
		if b == nil || b.id != c.ID { // new, or recreated under the same name
			b = &logBuf{id: c.ID}
			l.tail[name] = b
		}
		since := b.last
		l.mu.Unlock()

		lines, err := l.d.Logs(ctx, c.ID, since, l.n)
		if err != nil {
			errs = append(errs, fmt.Errorf("%s: %w", name, err))
			continue
		}
		var fresh []string
		first := map[string]string{}
		count := map[string]int{}
		for _, line := range lines {
			ts, msg, _ := strings.Cut(line, " ")
			t, err := time.Parse(time.RFC3339Nano, ts)
			if err != nil || !t.After(since) { // since is inclusive
				continue
			}
			since = t
			fresh = append(fresh, clip(line))
			if !t.After(l.start) {
				continue
			}
			for _, k := range l.keywords {
				if strings.Contains(msg, k) {
					if count[k]++; count[k] == 1 {
						first[k] = clip(msg)
					}
				}
			}
		}
		l.mu.Lock()
		b.last = since
		b.lines = append(b.lines, fresh...)
		b.lines = b.lines[max(0, len(b.lines)-l.n):]
		l.mu.Unlock()
		for _, k := range l.keywords {
			if count[k] > 0 {
				recs = append(recs, &schemav1.Record{Body: &schemav1.Record_Event{Event: &schemav1.Event{
					Kind: "log_match", Source: name, Message: first[k],
					Attrs: map[string]string{"keyword": k, "lines": strconv.Itoa(count[k])},
				}}})
			}
		}
	}
	l.mu.Lock()
	for name := range l.tail {
		if !seen[name] {
			delete(l.tail, name)
		}
	}
	l.mu.Unlock()
	return recs, errors.Join(errs...)
}

// clip shortens s to maxLogLine bytes and makes it valid UTF-8, which proto
// strings must be.
func clip(s string) string {
	return strings.ToValidUTF8(s[:min(len(s), maxLogLine)], "�")
}
