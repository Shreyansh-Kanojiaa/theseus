// Command theseus-controlplane runs the control plane's sync endpoint: nodes
// upload their spooled records over gRPC and it keeps them in SQLite.
package main

import (
	"context"
	"flag"
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"

	"github.com/Shreyansh-Kanojiaa/theseus/controlplane"
)

func main() {
	var (
		listen = flag.String("listen", ":50051", "gRPC address for node uploads")
		data   = flag.String("data", "data-cp", "directory for the control plane's store")
	)
	flag.Parse()

	if err := os.MkdirAll(*data, 0o755); err != nil {
		log.Fatal(err)
	}
	cp, err := controlplane.Open(*data)
	if err != nil {
		log.Fatal(err)
	}
	l, err := net.Listen("tcp", *listen)
	if err != nil {
		log.Fatal(err)
	}
	g := cp.GRPC()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	go func() {
		<-ctx.Done()
		g.GracefulStop()
	}()
	log.Printf("theseus-controlplane: sync on %s, store %s", l.Addr(), *data)
	if err := g.Serve(l); err != nil {
		log.Fatal(err)
	}
	if err := cp.Close(); err != nil {
		log.Fatal(err)
	}
}
