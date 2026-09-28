package chaos

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

// fakeDocker stands in for the docker CLI: containers are running or not, and
// kill takes effect only if killWorks. It returns the commands it was given.
func fakeDocker(t *testing.T, killWorks bool, volType string) *[]string {
	t.Helper()
	running := map[string]bool{"theseus-c-prometheus-1": true}
	var calls []string
	old := docker
	docker = func(_ context.Context, args ...string) (string, error) {
		calls = append(calls, strings.Join(args, " "))
		name := args[len(args)-1]
		switch args[0] {
		case "kill", "start":
			if _, ok := running[name]; !ok {
				return "", errors.New("no such container")
			}
			running[name] = args[0] == "start" || !killWorks
		case "inspect":
			v, ok := running[name]
			if !ok {
				return "", errors.New("no such container")
			}
			return strconv.FormatBool(v), nil
		case "volume":
			return volType, nil
		}
		return "", nil
	}
	t.Cleanup(func() { docker = old })
	return &calls
}

func TestInjectRevert(t *testing.T) {
	fakeDocker(t, true, "tmpfs")
	ctx, h := context.Background(), Harness{Dir: t.TempDir()}

	r, err := h.Inject(ctx, "kill", "c", "prometheus", nil)
	if err != nil || r.Start.IsZero() {
		t.Fatalf("inject: %+v, %v", r, err)
	}
	if _, err := h.Inject(ctx, "kill", "c", "prometheus", nil); err == nil || !strings.Contains(err.Error(), "already injected") {
		t.Fatalf("second inject: %v", err)
	}
	if act, err := h.Active(); err != nil || len(act) != 1 || act[0].Target != "prometheus" {
		t.Fatalf("active: %+v, %v", act, err)
	}

	if _, err := h.Revert(ctx, "kill", "c", "prometheus"); err != nil {
		t.Fatal(err)
	}
	if _, err := h.Revert(ctx, "kill", "c", "prometheus"); err == nil || !strings.Contains(err.Error(), "no kill injected") {
		t.Fatalf("second revert: %v", err)
	}
	if act, _ := h.Active(); len(act) != 0 {
		t.Fatalf("still active after revert: %+v", act)
	}

	f, err := os.Open(filepath.Join(h.Dir, "ground-truth.jsonl"))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = f.Close() }()
	var labels []Record
	for sc := bufio.NewScanner(f); sc.Scan(); {
		var l Record
		if err := json.Unmarshal(sc.Bytes(), &l); err != nil {
			t.Fatal(err)
		}
		labels = append(labels, l)
	}
	if len(labels) != 1 || labels[0].Fault != "kill" || labels[0].Node != "c" || labels[0].Target != "prometheus" ||
		!labels[0].Start.Equal(r.Start) || labels[0].End.Before(labels[0].Start) {
		t.Fatalf("ground truth: %+v", labels)
	}
}

// An inject that does not take effect is undone and leaves no trace.
func TestInjectWithoutEffect(t *testing.T) {
	calls := fakeDocker(t, false, "tmpfs")
	ctx, h := context.Background(), Harness{Dir: t.TempDir()}
	if _, err := h.Inject(ctx, "kill", "c", "prometheus", nil); err == nil || !strings.Contains(err.Error(), "in effect = false") {
		t.Fatalf("inject: %v", err)
	}
	if act, _ := h.Active(); len(act) != 0 {
		t.Fatalf("active: %+v", act)
	}
	if last := (*calls)[len(*calls)-1]; last != "start theseus-c-prometheus-1" {
		t.Fatalf("no best-effort revert, last call %q", last)
	}
}

// disk-fill must never run against anything but the testbed's tmpfs volume.
func TestDiskFillRefusesHostDisk(t *testing.T) {
	calls := fakeDocker(t, true, "")
	h := Harness{Dir: t.TempDir()}
	if _, err := h.Inject(context.Background(), "disk-fill", "d", "", nil); err == nil || !strings.Contains(err.Error(), "refusing") {
		t.Fatalf("inject: %v", err)
	}
	for _, c := range *calls {
		if strings.HasPrefix(c, "run") {
			t.Fatalf("ran a helper: %q", c)
		}
	}
}

func TestResolve(t *testing.T) {
	fakeDocker(t, true, "tmpfs")
	h := Harness{Dir: t.TempDir()}
	for _, c := range []struct{ fault, node, target, want string }{
		{"flood", "c", "", "unknown fault"},
		{"kill", "c", "", "needs --target"},
		{"kill", "C;rm", "prometheus", "--node"},
		{"disk-fill", "c", "prometheus", "always targets disk"},
		{"netem-loss", "c", "", "no such container"}, // node c has no agent here
	} {
		if _, err := h.Inject(context.Background(), c.fault, c.node, c.target, nil); err == nil || !strings.Contains(err.Error(), c.want) {
			t.Errorf("%s %s/%s: %v, want %q", c.fault, c.node, c.target, err, c.want)
		}
	}
}
