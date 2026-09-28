// Command theseus-chaos injects and reverts faults on the testbed, checks each
// one took effect and was undone, and appends every fault's ground truth to
// <dir>/ground-truth.jsonl.
//
//	theseus-chaos inject kill --node c --target prometheus
//	theseus-chaos revert kill --node c --target prometheus
//	theseus-chaos inject netem-loss --node c --loss 30%
//	theseus-chaos list
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"maps"
	"os"
	"os/signal"
	"path/filepath"
	"slices"
	"strings"
	"time"

	"github.com/Shreyansh-Kanojiaa/theseus/chaos"
)

func usage() {
	var b strings.Builder
	b.WriteString("usage: theseus-chaos inject|revert <fault> --node <node> [--target <service>] [--loss 30%] [--dir .chaos]\n")
	b.WriteString("       theseus-chaos list [--dir .chaos]\n\nfaults:\n")
	faults := chaos.Faults()
	for _, name := range slices.Sorted(maps.Keys(faults)) {
		fmt.Fprintf(&b, "  %-12s %s\n", name, faults[name])
	}
	fmt.Fprint(os.Stderr, b.String())
	os.Exit(2)
}

func main() {
	log.SetFlags(0)
	if len(os.Args) < 2 {
		usage()
	}
	cmd, args, fault := os.Args[1], os.Args[2:], ""
	if cmd == "inject" || cmd == "revert" {
		if len(args) == 0 || strings.HasPrefix(args[0], "-") {
			usage()
		}
		fault, args = args[0], args[1:]
	}
	fs := flag.NewFlagSet(cmd, flag.ExitOnError)
	fs.Usage = usage
	dir := fs.String("dir", ".chaos", "where active faults and ground-truth.jsonl are kept")
	node := fs.String("node", "", "testbed node, e.g. c")
	target := fs.String("target", "", "kill: the node's service to kill, e.g. prometheus")
	loss := fs.String("loss", "", "netem-loss: share of packets to drop (default 30%)")
	_ = fs.Parse(args) // ExitOnError

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()
	ctx, cancel := context.WithTimeout(ctx, 2*time.Minute)
	defer cancel()
	h := chaos.Harness{Dir: *dir}

	switch cmd {
	case "inject":
		var params map[string]string
		if *loss != "" {
			if fault != "netem-loss" {
				log.Fatalf("--loss only applies to netem-loss")
			}
			params = map[string]string{"loss": *loss}
		}
		r, err := h.Inject(ctx, fault, *node, *target, params)
		if err != nil {
			log.Fatal(err)
		}
		fmt.Printf("injected %s on %s/%s at %s %s\n", r.Fault, r.Node, r.Target, r.Start.Format(time.RFC3339), fmtParams(r.Params))
	case "revert":
		r, err := h.Revert(ctx, fault, *node, *target)
		if err != nil {
			log.Fatal(err)
		}
		fmt.Printf("reverted %s on %s/%s after %s; ground truth in %s\n", r.Fault, r.Node, r.Target,
			r.End.Sub(r.Start).Round(100*time.Millisecond), filepath.Join(*dir, "ground-truth.jsonl"))
	case "list":
		act, err := h.Active()
		if err != nil {
			log.Fatal(err)
		}
		for _, r := range act {
			fmt.Printf("%s on %s/%s since %s %s\n", r.Fault, r.Node, r.Target, r.Start.Format(time.RFC3339), fmtParams(r.Params))
		}
	default:
		usage()
	}
}

func fmtParams(p map[string]string) string {
	var kv []string
	for _, k := range slices.Sorted(maps.Keys(p)) {
		kv = append(kv, k+"="+p[k])
	}
	return strings.Join(kv, " ")
}
