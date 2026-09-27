package agent

import (
	"context"
	"encoding/binary"
	"fmt"
	"net/http"
	"slices"
	"sync/atomic"
	"testing"
	"time"

	schemav1 "github.com/Shreyansh-Kanojiaa/theseus/schema/v1"
)

// frame is one multiplexed log frame, as sent for a container without a TTY.
func frame(stream byte, s string) []byte {
	h := []byte{stream, 0, 0, 0, 0, 0, 0, 0}
	binary.BigEndian.PutUint32(h[4:], uint32(len(s)))
	return append(h, s...)
}

func TestLogTail(t *testing.T) {
	t0 := time.Date(2026, 9, 27, 10, 0, 0, 0, time.UTC)
	line := func(sec int, msg string) string {
		return t0.Add(time.Duration(sec)*time.Second).Format(time.RFC3339Nano) + " " + msg
	}
	at := func(sec int, msg string) string { return line(sec, msg) + "\n" }
	var round atomic.Int32
	var appSince atomic.Value
	d := fakeDocker(t, func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/containers/json":
			app := `{"Id":"a1","Names":["/app"],"State":"exited","Labels":{"theseus.logs":""}},`
			if round.Load() == 3 {
				app = ""
			}
			_, _ = fmt.Fprintf(w, `[%s
				{"Id":"w1","Names":["/web"],"State":"running","Labels":{"theseus.probe":"tcp://:1"}},
				{"Id":"n1","Names":["/plain"],"State":"running"}]`, app)
		case "/containers/a1/logs":
			appSince.Store(r.URL.Query().Get("since"))
			if round.Load() == 1 {
				for i, l := range []string{
					at(-1, "No space left on device"), // before the agent started: buffered, not matched
					at(1, "write /data/x: No space left on device"),
					at(2, "retrying"),
					at(3, "No space left on device"),
					at(4, "OOM killer chose \xffpid 7"),
				} {
					_, _ = w.Write(frame(byte(1+i%2), l))
				}
			} else {
				_, _ = w.Write(frame(1, at(4, "OOM killer chose \xffpid 7"))) // since is inclusive
				_, _ = w.Write(frame(2, at(5, "fine")))
			}
		case "/containers/w1/logs": // TTY: raw stream
			_, _ = fmt.Fprint(w, at(1, "listening\r"))
		default:
			t.Errorf("unexpected %s", r.URL)
			http.NotFound(w, r)
		}
	})

	lt := NewLogTail(d, 3, []string{"No space left on device", "OOM"})
	lt.start = t0
	collect := func() []*schemav1.Event {
		t.Helper()
		round.Add(1)
		recs, err := lt.Collect(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		var evs []*schemav1.Event
		for _, r := range recs {
			evs = append(evs, r.GetEvent())
		}
		return evs
	}

	evs := collect()
	if len(evs) != 2 ||
		evs[0].Source != "app" || evs[0].Attrs["keyword"] != "No space left on device" || evs[0].Attrs["lines"] != "2" ||
		evs[0].Message != "write /data/x: No space left on device" ||
		evs[1].Attrs["keyword"] != "OOM" || evs[1].Attrs["lines"] != "1" || evs[1].Message != "OOM killer chose �pid 7" {
		t.Fatalf("round 1 events: %v", evs)
	}
	for _, e := range evs {
		if e.Kind != "log_match" {
			t.Fatalf("kind %q", e.Kind)
		}
	}
	if got, want := lt.Lines("app"), []string{
		line(2, "retrying"), line(3, "No space left on device"), line(4, "OOM killer chose \uFFFDpid 7"),
	}; !slices.Equal(got, want) {
		t.Fatalf("app lines: %q", got)
	}
	if got := lt.Lines("web"); !slices.Equal(got, []string{line(1, "listening")}) {
		t.Fatalf("web lines: %q", got)
	}
	if lt.Lines("plain") != nil {
		t.Fatal("unlabelled container tailed")
	}

	if evs := collect(); len(evs) != 0 {
		t.Fatalf("round 2 re-matched the inclusive line: %v", evs)
	}
	if got := appSince.Load(); got != fmt.Sprintf("%d.000000000", t0.Unix()+4) {
		t.Fatalf("round 2 since = %v", got)
	}
	if got := lt.Lines("app"); len(got) != 3 || got[2] != line(5, "fine") {
		t.Fatalf("round 2 app lines: %q", got)
	}

	collect()
	if lt.Lines("app") != nil {
		t.Fatal("removed container still buffered")
	}
}
