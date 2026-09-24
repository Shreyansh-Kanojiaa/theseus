package agent

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"

	schemav1 "github.com/Shreyansh-Kanojiaa/theseus/schema/v1"
)

// CollectFunc gathers one round of records. It may return records alongside
// an error (e.g. up=0 for a failed scrape).
type CollectFunc func(context.Context) ([]*schemav1.Record, error)

// Collect runs fn now and then every interval until ctx is done, appending
// what it returns. An error is logged when it first appears and when it clears,
// not on every round.
func Collect(ctx context.Context, s *Store, name string, every time.Duration, fn CollectFunc) {
	t := time.NewTicker(every)
	defer t.Stop()
	var last string
	for {
		recs, err := fn(ctx)
		if len(recs) > 0 {
			err = errors.Join(err, s.Append(recs...))
		}
		switch msg := fmt.Sprint(err); {
		case err != nil && msg != last:
			log.Printf("%s: %v", name, err)
			last = msg
		case err == nil && last != "":
			log.Printf("%s: recovered", name)
			last = ""
		}
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
	}
}

func metric(name string, v float64, kv ...string) *schemav1.Record {
	var labels map[string]string
	if len(kv) > 0 {
		labels = make(map[string]string, len(kv)/2)
		for i := 0; i+1 < len(kv); i += 2 {
			labels[kv[i]] = kv[i+1]
		}
	}
	return &schemav1.Record{Body: &schemav1.Record_Sample{Sample: &schemav1.Sample{Name: name, Value: v, Labels: labels}}}
}

// HostMetrics reads CPU, memory and network from procRoot (normally /proc,
// or the host's /proc mounted into a container) and disk usage of mounts.
// CPU utilisation is a delta, so the first round has none.
func HostMetrics(procRoot string, mounts []string) CollectFunc {
	var prevTotal, prevIdle uint64
	return func(context.Context) ([]*schemav1.Record, error) {
		var recs []*schemav1.Record
		var errs []error

		if total, idle, err := readCPU(filepath.Join(procRoot, "stat")); err != nil {
			errs = append(errs, err)
		} else {
			if prevTotal != 0 && total > prevTotal {
				recs = append(recs, metric("host_cpu_used_ratio", 1-float64(idle-prevIdle)/float64(total-prevTotal)))
			}
			prevTotal, prevIdle = total, idle
		}

		if total, avail, err := readMem(filepath.Join(procRoot, "meminfo")); err != nil {
			errs = append(errs, err)
		} else {
			recs = append(recs,
				metric("host_memory_total_bytes", total),
				metric("host_memory_available_bytes", avail),
				metric("host_memory_used_ratio", 1-avail/total))
		}

		for _, m := range mounts {
			var st syscall.Statfs_t
			if err := syscall.Statfs(m, &st); err != nil {
				errs = append(errs, fmt.Errorf("statfs %s: %w", m, err))
				continue
			}
			bs := float64(st.Frsize)
			used, avail := float64(st.Blocks-st.Bfree)*bs, float64(st.Bavail)*bs
			ratio := 0.0
			if used+avail > 0 {
				ratio = used / (used + avail) // what df reports
			}
			recs = append(recs,
				metric("host_disk_total_bytes", float64(st.Blocks)*bs, "mountpoint", m),
				metric("host_disk_available_bytes", avail, "mountpoint", m),
				metric("host_disk_used_ratio", ratio, "mountpoint", m))
		}

		if nets, err := readNetDev(filepath.Join(procRoot, "net/dev")); err != nil {
			errs = append(errs, err)
		} else {
			recs = append(recs, nets...)
		}
		return recs, errors.Join(errs...)
	}
}

// readCPU returns total and idle (idle+iowait) jiffies from the "cpu" line.
func readCPU(path string) (total, idle uint64, err error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return 0, 0, err
	}
	line, _, _ := strings.Cut(string(b), "\n")
	f := strings.Fields(line)
	if len(f) < 9 || f[0] != "cpu" {
		return 0, 0, fmt.Errorf("%s: unexpected cpu line %q", path, line)
	}
	for i, s := range f[1:9] { // user nice system idle iowait irq softirq steal; guest is inside user
		v, err := strconv.ParseUint(s, 10, 64)
		if err != nil {
			return 0, 0, fmt.Errorf("%s: %w", path, err)
		}
		total += v
		if i == 3 || i == 4 {
			idle += v
		}
	}
	return total, idle, nil
}

// readMem returns MemTotal and MemAvailable in bytes.
func readMem(path string) (total, avail float64, err error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return 0, 0, err
	}
	for line := range strings.Lines(string(b)) {
		f := strings.Fields(line)
		if len(f) < 2 {
			continue
		}
		v, _ := strconv.ParseFloat(f[1], 64)
		switch f[0] {
		case "MemTotal:":
			total = v * 1024
		case "MemAvailable:":
			avail = v * 1024
		}
	}
	if total == 0 {
		return 0, 0, fmt.Errorf("%s: no MemTotal", path)
	}
	return total, avail, nil
}

// readNetDev returns receive/transmit byte counters per interface, except lo.
func readNetDev(path string) ([]*schemav1.Record, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var recs []*schemav1.Record
	for line := range strings.Lines(string(b)) {
		dev, rest, ok := strings.Cut(line, ":")
		f := strings.Fields(rest)
		dev = strings.TrimSpace(dev)
		if !ok || len(f) < 16 || dev == "lo" { // the two header lines have no ':'
			continue
		}
		rx, err1 := strconv.ParseFloat(f[0], 64)
		tx, err2 := strconv.ParseFloat(f[8], 64)
		if err := errors.Join(err1, err2); err != nil {
			return recs, fmt.Errorf("%s: %w", path, err)
		}
		recs = append(recs,
			metric("host_network_receive_bytes_total", rx, "device", dev),
			metric("host_network_transmit_bytes_total", tx, "device", dev))
	}
	return recs, nil
}
