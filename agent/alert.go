package agent

import (
	"bytes"
	"context"
	_ "embed"
	"errors"
	"fmt"
	"io"
	"log"
	"maps"
	"net/http"
	"slices"
	"strings"
	"sync"
	"time"

	"go.yaml.in/yaml/v3"
	"google.golang.org/protobuf/encoding/protojson"

	schemav1 "github.com/Shreyansh-Kanojiaa/theseus/schema/v1"
)

// DefaultAlertRules is the rules file used when none is given.
//
//go:embed alerts.yaml
var DefaultAlertRules []byte

// AlertRule fires when a sample named Metric, carrying at least Labels,
// compares true (Op) against Value on every round for at least For.
type AlertRule struct {
	Name     string            `yaml:"name"`
	Metric   string            `yaml:"metric"`
	Labels   map[string]string `yaml:"labels"`
	Op       string            `yaml:"op"` // > >= < <= == !=
	Value    float64           `yaml:"value"`
	For      time.Duration     `yaml:"for"`
	Severity string            `yaml:"severity"`
}

func (r AlertRule) holds(v float64) bool {
	switch r.Op {
	case ">":
		return v > r.Value
	case ">=":
		return v >= r.Value
	case "<":
		return v < r.Value
	case "<=":
		return v <= r.Value
	case "==":
		return v == r.Value
	}
	return v != r.Value
}

// ParseAlertRules reads a YAML rules file: {rules: [AlertRule...]}.
func ParseAlertRules(b []byte) ([]AlertRule, error) {
	var f struct {
		Rules []AlertRule `yaml:"rules"`
	}
	dec := yaml.NewDecoder(bytes.NewReader(b))
	dec.KnownFields(true)
	if err := dec.Decode(&f); err != nil && !errors.Is(err, io.EOF) {
		return nil, fmt.Errorf("alert rules: %w", err)
	}
	names := map[string]bool{}
	for i, r := range f.Rules {
		switch {
		case r.Name == "" || r.Metric == "":
			return nil, fmt.Errorf("alert rule %d: name and metric are required", i+1)
		case names[r.Name]:
			return nil, fmt.Errorf("alert rule %q: duplicate name", r.Name)
		case !slices.Contains([]string{">", ">=", "<", "<=", "==", "!="}, r.Op):
			return nil, fmt.Errorf("alert rule %q: op %q, want one of > >= < <= == !=", r.Name, r.Op)
		}
		names[r.Name] = true
	}
	return f.Rules, nil
}

// Alerts evaluates rules against samples as collectors produce them, before
// the store sees them (which drops samples when the disk is full). Every
// transition becomes an event, alert or alert_resolved, appended with the
// round's records and also sent to the output.
type Alerts struct {
	rules  []AlertRule
	output string // "stdout" or a webhook URL

	mu     sync.Mutex
	active map[string]*alertState // rule name + series
}

type alertState struct {
	since  time.Time
	firing bool
}

// NewAlerts writes alert events as JSON lines to stdout when output is
// "stdout", or POSTs each one to output as a webhook.
func NewAlerts(rules []AlertRule, output string) *Alerts {
	return &Alerts{rules: rules, output: output, active: map[string]*alertState{}}
}

// Watch returns fn with the alert events its samples trigger appended.
func (a *Alerts) Watch(fn CollectFunc) CollectFunc {
	return func(ctx context.Context) ([]*schemav1.Record, error) {
		recs, err := fn(ctx)
		evs := a.eval(recs)
		for _, e := range evs {
			a.notify(ctx, e)
			recs = append(recs, &schemav1.Record{Body: &schemav1.Record_Event{Event: e}})
		}
		return recs, err
	}
}

func (a *Alerts) eval(recs []*schemav1.Record) []*schemav1.Event {
	now := wallClock()
	a.mu.Lock()
	defer a.mu.Unlock()
	var evs []*schemav1.Event
	for _, rec := range recs {
		s := rec.GetSample()
		if s == nil {
			continue
		}
		for _, r := range a.rules {
			if s.Name != r.Metric || !matches(s.Labels, r.Labels) {
				continue
			}
			ser := series(s)
			key := r.Name + " " + ser
			st := a.active[key]
			if !r.holds(s.Value) {
				if st != nil && st.firing {
					evs = append(evs, alertEvent("alert_resolved", r, ser, s.Value,
						fmt.Sprintf("resolved: %s = %s", ser, fmtFloat(s.Value))))
				}
				delete(a.active, key)
				continue
			}
			if st == nil {
				st = &alertState{since: now}
				a.active[key] = st
			}
			if !st.firing && now.Sub(st.since) >= r.For {
				st.firing = true
				evs = append(evs, alertEvent("alert", r, ser, s.Value,
					fmt.Sprintf("%s = %s %s %s", ser, fmtFloat(s.Value), r.Op, fmtFloat(r.Value))))
			}
		}
	}
	return evs
}

func alertEvent(kind string, r AlertRule, ser string, v float64, msg string) *schemav1.Event {
	return &schemav1.Event{Kind: kind, Source: r.Name, Message: msg, Attrs: map[string]string{
		"rule": r.Name, "severity": r.Severity, "series": ser,
		"value": fmtFloat(v), "threshold": r.Op + " " + fmtFloat(r.Value),
	}}
}

// notify delivers e to the output. Failures are logged; the event is in the
// store regardless.
// ponytail: synchronous, so a hung webhook delays its collector's round by up
// to 5 s per alert; move to a queue if webhooks get slow or alerts frequent.
func (a *Alerts) notify(ctx context.Context, e *schemav1.Event) {
	b, err := protojson.Marshal(e)
	if err != nil {
		log.Printf("alert: %v", err)
		return
	}
	if a.output == "stdout" {
		fmt.Printf("%s\n", b)
		return
	}
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, a.output, bytes.NewReader(b))
	if err != nil {
		log.Printf("alert webhook: %v", err)
		return
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		log.Printf("alert webhook: %v", err)
		return
	}
	_ = resp.Body.Close()
	if resp.StatusCode/100 != 2 {
		log.Printf("alert webhook: %s", resp.Status)
	}
}

func matches(labels, want map[string]string) bool {
	for k, v := range want {
		if labels[k] != v {
			return false
		}
	}
	return true
}

// series renders a sample as name{k="v",...} with labels sorted.
func series(s *schemav1.Sample) string {
	var kv []string
	for _, k := range slices.Sorted(maps.Keys(s.Labels)) {
		kv = append(kv, fmt.Sprintf("%s=%q", k, s.Labels[k]))
	}
	return s.Name + "{" + strings.Join(kv, ",") + "}"
}
