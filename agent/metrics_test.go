package agent

import (
	"strings"
	"testing"

	"github.com/prometheus/common/expfmt"
	"github.com/prometheus/common/model"

	schemav1 "github.com/Shreyansh-Kanojiaa/theseus/schema/v1"
)

func TestWriteMetrics(t *testing.T) {
	s, err := OpenStore(t.TempDir(), "n1", 0)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = s.Close() }()
	probe := func(target string, ok bool) *schemav1.Record {
		return &schemav1.Record{Body: &schemav1.Record_ProbeResult{ProbeResult: &schemav1.ProbeResult{Target: target, Ok: ok}}}
	}
	if err := s.Append(metric("a", 1), metric("b", 2), probe("pg", false), probe(`we"ird`, false), probe("pg", true), probe("web", true),
		&schemav1.Record{Body: &schemav1.Record_Event{Event: &schemav1.Event{Kind: "x"}}}); err != nil {
		t.Fatal(err)
	}
	if err := s.Append(probe("pg", false)); err != nil {
		t.Fatal(err)
	}

	var b strings.Builder
	if err := s.WriteMetrics(&b); err != nil {
		t.Fatal(err)
	}
	p := expfmt.NewTextParser(model.UTF8Validation)
	fams, err := p.TextToMetricFamilies(strings.NewReader(b.String()))
	if err != nil {
		t.Fatalf("%v\n%s", err, b.String())
	}
	got := map[string]float64{}
	for name, f := range fams {
		for _, m := range f.Metric {
			k := name
			for _, l := range m.Label {
				k += " " + l.GetValue()
			}
			got[k] = m.GetCounter().GetValue() + m.GetGauge().GetValue()
		}
	}
	for k, want := range map[string]float64{
		"theseus_agent_records_written_total sample":       2,
		"theseus_agent_records_written_total probe_result": 5,
		"theseus_agent_records_written_total event":        1,
		"theseus_agent_probe_failures_total pg":            2,
		`theseus_agent_probe_failures_total we"ird`:        1,
		"theseus_agent_probe_failures_total web":           0,
		"theseus_agent_spool_depth":                        8,
		"theseus_agent_disk_degraded":                      0,
	} {
		if v, ok := got[k]; !ok || v != want {
			t.Errorf("%s = %v, want %v", k, got[k], want)
		}
	}
}
