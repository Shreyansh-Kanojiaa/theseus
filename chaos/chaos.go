// Package chaos is the chaos engine: fault injection, revert and ground-truth JSONL labels.
package chaos

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"maps"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
	"time"
)

// Record is one fault's ground truth: what was broken where, and when. The
// window is conservative: Start is taken before the fault is injected and End
// after its revert is verified. It is Laya's training label.
type Record struct {
	Fault  string            `json:"fault"`
	Node   string            `json:"node"`
	Target string            `json:"target"`
	Params map[string]string `json:"params,omitempty"`
	Start  time.Time         `json:"start"`
	End    time.Time         `json:"end,omitzero"`
}

// fault is one kind of failure. active reports whether it is in effect, and
// the harness checks it after every inject and revert.
type fault struct {
	about  string
	target string // fixed target; "" means --target names the node's service to hit
	inject func(ctx context.Context, r *Record) error
	revert func(ctx context.Context, r *Record) error
	active func(ctx context.Context, r *Record) (bool, error)
}

// Faults describes each fault by name, for usage text.
func Faults() map[string]string {
	m := map[string]string{}
	for name, f := range faults {
		m[name] = f.about
	}
	return m
}

var lossRE = regexp.MustCompile(`^[0-9]{1,2}(\.[0-9]+)?%$|^100%$`)

var faults = map[string]fault{
	"kill": {
		about: "SIGKILL the node's --target container (postgres, prometheus, node-exporter, agent); revert starts it",
		inject: func(ctx context.Context, r *Record) error {
			_, err := docker(ctx, "kill", container(r.Node, r.Target))
			return err
		},
		revert: func(ctx context.Context, r *Record) error {
			_, err := docker(ctx, "start", container(r.Node, r.Target))
			return err
		},
		active: func(ctx context.Context, r *Record) (bool, error) {
			s, err := docker(ctx, "inspect", "-f", "{{.State.Running}}", container(r.Node, r.Target))
			return s == "false", err
		},
	},
	"netem-loss": {
		about:  "drop --loss (default 30%) of the packets the node's agent sends on its uplink (tc netem)",
		target: "uplink",
		inject: func(ctx context.Context, r *Record) error {
			if r.Params["loss"] == "" {
				r.Params = map[string]string{"loss": "30%"}
			}
			if !lossRE.MatchString(r.Params["loss"]) {
				return fmt.Errorf("--loss %q: want a percentage like 30%%", r.Params["loss"])
			}
			_, err := onUplink(ctx, r.Node, `tc qdisc add dev "$DEV" root netem loss `+r.Params["loss"])
			return err
		},
		revert: func(ctx context.Context, r *Record) error {
			_, err := onUplink(ctx, r.Node, `tc qdisc del dev "$DEV" root`)
			return err
		},
		active: func(ctx context.Context, r *Record) (bool, error) {
			s, err := onUplink(ctx, r.Node, `tc qdisc show dev "$DEV" | grep -q netem && echo yes || echo no`)
			return s == "yes", err
		},
	},
	"uplink-drop": {
		about:  "silently drop everything in and out of the node's uplink (iptables DROP): a partition, unlike make sever",
		target: "uplink",
		inject: func(ctx context.Context, r *Record) error {
			_, err := onUplink(ctx, r.Node, `
iptables -I INPUT -i "$DEV" -m comment --comment theseus-chaos -j DROP &&
iptables -I OUTPUT -o "$DEV" -m comment --comment theseus-chaos -j DROP`)
			return err
		},
		revert: func(ctx context.Context, r *Record) error {
			_, err := onUplink(ctx, r.Node, `
iptables -S | grep theseus-chaos | sed 's/^-A /-D /' | while read -r rule; do iptables $rule || exit 1; done`)
			return err
		},
		active: func(ctx context.Context, r *Record) (bool, error) {
			s, err := onUplink(ctx, r.Node, `iptables -S | grep -q theseus-chaos && echo yes || echo no`)
			return s == "yes", err
		},
	},
	"disk-fill": {
		about:  "fallocate every free byte of the node's shared /disk; revert deletes the file",
		target: "disk",
		inject: func(ctx context.Context, r *Record) error {
			n, err := onDisk(ctx, r.Node, `set -e
set -- $(stat -f -c '%a %S' /disk)
fallocate -l $(($1 * $2)) /disk/theseus-chaos-fill
echo $(($1 * $2))`)
			if err == nil {
				r.Params = map[string]string{"bytes": n}
			}
			return err
		},
		revert: func(ctx context.Context, r *Record) error {
			_, err := onDisk(ctx, r.Node, `rm -f /disk/theseus-chaos-fill`)
			return err
		},
		active: func(ctx context.Context, r *Record) (bool, error) {
			s, err := onDisk(ctx, r.Node, `[ -e /disk/theseus-chaos-fill ] && echo yes || echo no`)
			return s == "yes", err
		},
	},
}

// Testbed names (testbed/node.yaml): node c is compose project theseus-c.
func container(node, service string) string { return "theseus-" + node + "-" + service + "-1" }

// image is the helper image for network and disk faults: the testbed's own,
// which carries tc, iptables and fallocate.
const image = "theseus"

// docker runs the docker CLI and returns its trimmed stdout. Tests replace it.
var docker = func(ctx context.Context, args ...string) (string, error) {
	cmd := exec.CommandContext(ctx, "docker", args...)
	var out, stderr bytes.Buffer
	cmd.Stdout, cmd.Stderr = &out, &stderr
	if err := cmd.Run(); err != nil {
		return "", fmt.Errorf("docker %s: %w: %s", args[0], err, strings.TrimSpace(stderr.String()))
	}
	return strings.TrimSpace(out.String()), nil
}

// onUplink runs script in a helper container sharing the network namespace
// of node's agent (the only container on the uplink), with $DEV set to the
// agent's uplink interface. Rules and qdiscs outlive the helper.
func onUplink(ctx context.Context, node, script string) (string, error) {
	agent := container(node, "agent")
	ip, err := docker(ctx, "inspect", "-f",
		`{{with index .NetworkSettings.Networks "theseus-uplink"}}{{.IPAddress}}{{end}}`, agent)
	if err != nil {
		return "", err
	}
	if ip == "" {
		return "", fmt.Errorf("node %s has no uplink (severed?)", node)
	}
	return docker(ctx, "run", "--rm", "--network", "container:"+agent, "--cap-add", "NET_ADMIN",
		"--security-opt", "label=disable", "-e", "IP="+ip, "--entrypoint", "sh", image, "-c",
		`DEV=$(ip -o -4 addr show | awk -v ip="$IP" 'index($4, ip "/") == 1 {print $2}')
[ -n "$DEV" ] || { echo "no interface has $IP" >&2; exit 1; }
`+script)
}

// onDisk runs script in a helper container with node's disk volume at
// /disk. It refuses anything but the testbed's tmpfs volume: docker run would
// quietly create a missing volume on the host's disk and fill that instead.
func onDisk(ctx context.Context, node, script string) (string, error) {
	vol := "theseus-" + node + "_disk"
	typ, err := docker(ctx, "volume", "inspect", "-f", "{{.Options.type}}", vol)
	if err != nil {
		return "", fmt.Errorf("node %s has no disk volume: %w", node, err)
	}
	if typ != "tmpfs" {
		return "", fmt.Errorf("volume %s is %q, not the testbed's tmpfs; refusing to fill it", vol, typ)
	}
	return docker(ctx, "run", "--rm", "-v", vol+":/disk", "--security-opt", "label=disable",
		"--entrypoint", "sh", image, "-c", script)
}

var nameRE = regexp.MustCompile(`^[a-z0-9][a-z0-9-]*$`)

// Harness injects and reverts faults. An injected fault is kept in
// Dir/active until reverted, so a crashed or restarted harness can still
// revert it; each revert appends the fault's Record to Dir/ground-truth.jsonl.
type Harness struct{ Dir string }

func (h Harness) resolve(name, node, target string) (fault, Record, error) {
	f, ok := faults[name]
	switch {
	case !ok:
		return f, Record{}, fmt.Errorf("unknown fault %q; faults: %s", name, strings.Join(slices.Sorted(maps.Keys(faults)), ", "))
	case !nameRE.MatchString(node):
		return f, Record{}, fmt.Errorf("--node %q: want a node name like c", node)
	case f.target != "" && target != "" && target != f.target:
		return f, Record{}, fmt.Errorf("%s always targets %s", name, f.target)
	case f.target != "":
		target = f.target
	case !nameRE.MatchString(target):
		return f, Record{}, fmt.Errorf("%s needs --target, the service to hit, e.g. prometheus", name)
	}
	return f, Record{Fault: name, Node: node, Target: target}, nil
}

func (h Harness) activePath(r Record) string {
	return filepath.Join(h.Dir, "active", r.Fault+"_"+r.Node+"_"+r.Target+".json")
}

// Inject starts a fault and checks it took effect. params are fault
// options, e.g. {"loss": "10%"} for netem-loss.
func (h Harness) Inject(ctx context.Context, name, node, target string, params map[string]string) (Record, error) {
	f, r, err := h.resolve(name, node, target)
	if err != nil {
		return r, err
	}
	p := h.activePath(r)
	if _, err := os.Stat(p); err == nil {
		return r, fmt.Errorf("%s on %s/%s is already injected; revert it first", name, node, r.Target)
	}
	r.Params, r.Start = params, time.Now().UTC()
	// Recorded before injecting: a harness that dies mid-inject still leaves a
	// fault that can be reverted and labelled.
	if err := h.save(p, r); err != nil {
		return r, err
	}
	err = f.inject(ctx, &r)
	if err == nil {
		err = h.check(ctx, f, &r, true)
	}
	if err != nil {
		_ = f.revert(ctx, &r) // best effort: leave the node as it was
		return r, errors.Join(fmt.Errorf("inject %s: %w", name, err), os.Remove(p))
	}
	return r, h.save(p, r) // again, with params the inject filled in
}

// Revert ends an injected fault, checks it is gone and appends its ground
// truth. A failed revert leaves the fault recorded as active.
func (h Harness) Revert(ctx context.Context, name, node, target string) (Record, error) {
	f, r, err := h.resolve(name, node, target)
	if err != nil {
		return r, err
	}
	p := h.activePath(r)
	b, err := os.ReadFile(p)
	if errors.Is(err, fs.ErrNotExist) {
		return r, fmt.Errorf("no %s injected on %s/%s", name, node, r.Target)
	}
	if err != nil {
		return r, err
	}
	if err := json.Unmarshal(b, &r); err != nil {
		return r, fmt.Errorf("%s: %w", p, err)
	}
	if err := f.revert(ctx, &r); err != nil {
		return r, fmt.Errorf("revert %s: %w", name, err)
	}
	if err := h.check(ctx, f, &r, false); err != nil {
		return r, fmt.Errorf("revert %s: %w", name, err)
	}
	r.End = time.Now().UTC()
	if err := h.label(r); err != nil {
		return r, err
	}
	return r, os.Remove(p)
}

// Active lists the injected faults not yet reverted.
func (h Harness) Active() ([]Record, error) {
	paths, err := filepath.Glob(filepath.Join(h.Dir, "active", "*.json"))
	var out []Record
	for _, p := range paths {
		var r Record
		b, rerr := os.ReadFile(p)
		if rerr == nil {
			rerr = json.Unmarshal(b, &r)
		}
		if rerr != nil {
			return out, rerr
		}
		out = append(out, r)
	}
	return out, err
}

func (h Harness) check(ctx context.Context, f fault, r *Record, want bool) error {
	on, err := f.active(ctx, r)
	if err != nil {
		return fmt.Errorf("check: %w", err)
	}
	if on != want {
		return fmt.Errorf("check: fault in effect = %t, want %t", on, want)
	}
	return nil
}

func (h Harness) save(p string, r Record) error {
	if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
		return err
	}
	b, _ := json.Marshal(r) // a Record always marshals
	return os.WriteFile(p, b, 0o644)
}

// label appends r to the ground-truth file and syncs it.
func (h Harness) label(r Record) error {
	f, err := os.OpenFile(filepath.Join(h.Dir, "ground-truth.jsonl"), os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0o644)
	if err != nil {
		return err
	}
	b, _ := json.Marshal(r)
	_, err = f.Write(append(b, '\n'))
	if err == nil {
		err = f.Sync()
	}
	return errors.Join(err, f.Close())
}
