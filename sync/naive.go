package sync

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"sync/atomic"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/backoff"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/protobuf/proto"

	"github.com/Shreyansh-Kanojiaa/theseus/agent"
	schemav1 "github.com/Shreyansh-Kanojiaa/theseus/schema/v1"
)

// Syncer ships a node's spooled records to the control plane. Naive is the
// month 1 baseline; delta sync (watermark handshake, resumable priority
// replay) will be a second implementation.
type Syncer interface {
	Sync(ctx context.Context) (Report, error)
}

// Report is what one sync moved and how long it took.
type Report struct {
	Records    int           // records sent
	Stored     uint64        // new at the control plane
	Duplicates uint64        // the control plane already had them
	Payload    int64         // the records' protobuf size
	Wire       int64         // bytes sent plus received on the TCP connection, framing included
	Took       time.Duration // from opening the upload to the control plane's answer
}

// chunkBytes is the payload per upload message, well under gRPC's 4 MiB limit.
const chunkBytes = 256 << 10

// Naive uploads the whole spool on every sync: no handshake, no priority
// order, no resume. Only a complete upload is acknowledged, after which the
// spool is trimmed up to what was sent; an interrupted one starts over from
// the first unacknowledged record, and the control plane drops the repeats.
type Naive struct {
	store  *agent.Store
	conn   *grpc.ClientConn
	client schemav1.SyncServiceClient
	wire   atomic.Int64
}

// NewNaive returns a Naive syncer for store uploading to target (host:port).
// It connects lazily, so a control plane that is down is not an error here.
// ponytail: plaintext and unauthenticated, fine inside the testbed; add TLS
// before a node talks to a control plane over a network it doesn't own.
func NewNaive(store *agent.Store, target string) (*Naive, error) {
	n := &Naive{store: store}
	conn, err := grpc.NewClient(target,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		// A cut uplink drops packets without resetting the connection. Pings
		// during an upload notice within ~15 s instead of waiting out the timeout.
		grpc.WithKeepaliveParams(keepalive.ClientParameters{Time: 10 * time.Second, Timeout: 5 * time.Second}),
		// gRPC's reconnect backoff grows to 2 min by default, so a healed uplink
		// could wait several sync intervals. Run already paces the retries.
		grpc.WithConnectParams(grpc.ConnectParams{Backoff: backoff.Config{
			BaseDelay: time.Second, Multiplier: 1.6, Jitter: 0.2, MaxDelay: 10 * time.Second,
		}, MinConnectTimeout: 5 * time.Second}),
		grpc.WithContextDialer(func(ctx context.Context, addr string) (net.Conn, error) {
			var d net.Dialer
			c, err := d.DialContext(ctx, "tcp", addr)
			if err != nil {
				return nil, err
			}
			return &countConn{Conn: c, n: &n.wire}, nil
		}))
	if err != nil {
		return nil, err
	}
	n.conn, n.client = conn, schemav1.NewSyncServiceClient(conn)
	return n, nil
}

// Close closes the connection to the control plane.
func (n *Naive) Close() error { return n.conn.Close() }

// Sync uploads every spooled record and trims the spool once the control
// plane has answered.
func (n *Naive) Sync(ctx context.Context) (Report, error) {
	ctx, cancel := context.WithCancel(ctx) // abandons the stream on an early return
	defer cancel()
	var r Report
	start, wire := time.Now(), n.wire.Load()
	stream, err := n.client.Upload(ctx)
	if err != nil {
		return r, err
	}
	var chunk []*schemav1.Record
	size := 0
	flush := func() error {
		if len(chunk) == 0 {
			return nil
		}
		err := stream.Send(&schemav1.UploadRequest{Records: chunk})
		chunk, size = nil, 0
		return err
	}
	upTo, err := n.store.ScanSpool(ctx, func(rec *schemav1.Record) error {
		k := proto.Size(rec)
		chunk, size = append(chunk, rec), size+k
		r.Records++
		r.Payload += int64(k)
		if size >= chunkBytes {
			return flush()
		}
		return nil
	})
	if err == nil {
		err = flush()
	}
	var res *schemav1.UploadResponse
	if err == nil || errors.Is(err, io.EOF) { // io.EOF from Send: the real error comes with the answer
		res, err = stream.CloseAndRecv()
	}
	if err != nil {
		return r, err
	}
	r.Took, r.Wire = time.Since(start), n.wire.Load()-wire
	r.Stored, r.Duplicates = res.Stored, res.Duplicates
	if res.Received != uint64(r.Records) {
		return r, fmt.Errorf("control plane received %d of %d records", res.Received, r.Records)
	}
	if upTo > 0 {
		if err := n.store.AckSpool(upTo); err != nil {
			return r, fmt.Errorf("trim spool: %w", err)
		}
	}
	return r, nil
}

// countConn counts the bytes read and written through a connection.
type countConn struct {
	net.Conn
	n *atomic.Int64
}

func (c *countConn) Read(b []byte) (int, error) {
	k, err := c.Conn.Read(b)
	c.n.Add(int64(k))
	return k, err
}

func (c *countConn) Write(b []byte) (int, error) {
	k, err := c.Conn.Write(b)
	c.n.Add(int64(k))
	return k, err
}

// Run syncs now and then every interval until ctx is done, giving each sync
// up to timeout. Every successful sync is logged with its size and duration,
// the first after an outage marked as a reconnect; a failure is logged when
// it first appears.
func Run(ctx context.Context, s Syncer, every, timeout time.Duration) {
	t := time.NewTicker(every)
	defer t.Stop()
	var down time.Time
	var last string
	for {
		start := time.Now()
		sctx, cancel := context.WithTimeout(ctx, timeout)
		r, err := s.Sync(sctx)
		cancel()
		switch {
		case ctx.Err() != nil:
			return
		case err != nil:
			if down.IsZero() {
				down = start
			}
			if msg := err.Error(); msg != last {
				log.Printf("sync: %v", msg)
				last = msg
			}
		default:
			what := "sync"
			if !down.IsZero() {
				what = fmt.Sprintf("sync: reconnected after %s", time.Since(down).Round(time.Second))
				down, last = time.Time{}, ""
			}
			log.Printf("%s: %d records (%d new, %d duplicate), %d B payload, %d B on the wire, %s",
				what, r.Records, r.Stored, r.Duplicates, r.Payload, r.Wire, r.Took.Round(time.Millisecond))
		}
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
	}
}
