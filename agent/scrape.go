package agent

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"time"

	dto "github.com/prometheus/client_model/go"
	"github.com/prometheus/common/expfmt"
	"github.com/prometheus/common/model"

	schemav1 "github.com/Shreyansh-Kanojiaa/theseus/schema/v1"
)

var scrapeClient = &http.Client{Timeout: 10 * time.Second}

// Scrape reads a Prometheus text endpoint (node_exporter's /metrics) and
// emits every series as a sample labelled with instance, plus up=1, or only
// up=0 when the scrape fails. Histograms and summaries are flattened the way
// Prometheus stores them (_bucket/_sum/_count, quantile).
func Scrape(target string) CollectFunc {
	u, err := url.Parse(target)
	instance := target
	if err == nil {
		instance = u.Host
	}
	return func(ctx context.Context) ([]*schemav1.Record, error) {
		recs, err := scrape(ctx, target, instance)
		if err != nil {
			return []*schemav1.Record{metric("up", 0, "instance", instance)}, err
		}
		return append(recs, metric("up", 1, "instance", instance)), nil
	}
}

func scrape(ctx context.Context, target, instance string) ([]*schemav1.Record, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, target, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Accept", "text/plain;version=0.0.4")
	resp, err := scrapeClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("scrape %s: %w", instance, err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("scrape %s: %s", instance, resp.Status)
	}
	p := expfmt.NewTextParser(model.UTF8Validation)
	fams, err := p.TextToMetricFamilies(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("scrape %s: %w", instance, err)
	}

	var recs []*schemav1.Record
	for name, f := range fams {
		for _, m := range f.GetMetric() {
			emit := func(suffix string, v float64, kv ...string) {
				labels := map[string]string{"instance": instance}
				for _, l := range m.GetLabel() {
					labels[l.GetName()] = l.GetValue()
				}
				for i := 0; i+1 < len(kv); i += 2 {
					labels[kv[i]] = kv[i+1]
				}
				recs = append(recs, &schemav1.Record{Body: &schemav1.Record_Sample{Sample: &schemav1.Sample{
					Name: name + suffix, Value: v, Labels: labels}}})
			}
			switch f.GetType() {
			case dto.MetricType_COUNTER:
				emit("", m.GetCounter().GetValue())
			case dto.MetricType_GAUGE:
				emit("", m.GetGauge().GetValue())
			case dto.MetricType_SUMMARY:
				s := m.GetSummary()
				for _, q := range s.GetQuantile() {
					emit("", q.GetValue(), "quantile", fmtFloat(q.GetQuantile()))
				}
				emit("_sum", s.GetSampleSum())
				emit("_count", float64(s.GetSampleCount()))
			case dto.MetricType_HISTOGRAM:
				h := m.GetHistogram()
				for _, b := range h.GetBucket() {
					emit("_bucket", float64(b.GetCumulativeCount()), "le", fmtFloat(b.GetUpperBound()))
				}
				emit("_sum", h.GetSampleSum())
				emit("_count", float64(h.GetSampleCount()))
			default:
				emit("", m.GetUntyped().GetValue())
			}
		}
	}
	return recs, nil
}

func fmtFloat(f float64) string { return strconv.FormatFloat(f, 'g', -1, 64) }
