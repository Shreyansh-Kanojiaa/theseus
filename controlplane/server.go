package controlplane

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"log"
	"path/filepath"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	_ "modernc.org/sqlite" // registers the "sqlite" driver

	schemav1 "github.com/Shreyansh-Kanojiaa/theseus/schema/v1"
)

const schema = `
CREATE TABLE IF NOT EXISTS records (
	node_id  TEXT NOT NULL,
	seq      INTEGER NOT NULL,
	hlc      INTEGER NOT NULL,
	priority INTEGER NOT NULL,
	body     BLOB NOT NULL, -- marshalled schemav1.Record
	PRIMARY KEY (node_id, seq)
) WITHOUT ROWID;`

// Server is the control plane's sync endpoint. It keeps every record nodes
// upload, once per (node_id, seq), in SQLite.
type Server struct {
	schemav1.UnimplementedSyncServiceServer
	db *sql.DB
}

// Open opens (or creates) the control plane's store in dir.
func Open(dir string) (*Server, error) {
	db, err := sql.Open("sqlite", filepath.Join(dir, "controlplane.db")+
		"?_pragma=journal_mode(WAL)&_pragma=synchronous(NORMAL)&_pragma=busy_timeout(5000)")
	if err != nil {
		return nil, err
	}
	// One connection: uploads from several nodes take turns per chunk
	// instead of failing with SQLITE_BUSY.
	db.SetMaxOpenConns(1)
	if _, err := db.Exec(schema); err != nil {
		return nil, errors.Join(err, db.Close())
	}
	return &Server{db: db}, nil
}

// GRPC returns a gRPC server serving s. It allows the keepalive pings nodes
// send every 10 s during an upload; the default policy would disconnect them.
func (s *Server) GRPC() *grpc.Server {
	g := grpc.NewServer(grpc.KeepaliveEnforcementPolicy(keepalive.EnforcementPolicy{MinTime: 5 * time.Second}))
	schemav1.RegisterSyncServiceServer(g, s)
	return g
}

// Close closes the store.
func (s *Server) Close() error { return s.db.Close() }

// Upload stores a streamed upload chunk by chunk, each chunk in one
// transaction, so an upload cut off halfway keeps what arrived and the
// node's retry only adds duplicates. It logs every upload's size and duration.
func (s *Server) Upload(stream grpc.ClientStreamingServer[schemav1.UploadRequest, schemav1.UploadResponse]) error {
	start := time.Now()
	var res schemav1.UploadResponse
	var node string
	var size int
	for {
		req, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			log.Printf("upload from %s: cut off after %d records, %d B, %s: %v",
				node, res.Received, size, time.Since(start).Round(time.Millisecond), err)
			return err
		}
		size += proto.Size(req)
		if node == "" && len(req.Records) > 0 {
			node = req.Records[0].Header().GetNodeId()
		}
		stored, err := s.store(stream.Context(), req.Records)
		if err != nil {
			log.Printf("upload from %s: rejected after %d records: %v", node, res.Received, err)
			return err
		}
		res.Received += uint64(len(req.Records))
		res.Stored += stored
	}
	res.Duplicates = res.Received - res.Stored
	log.Printf("upload from %s: %d records (%d new, %d duplicate), %d B, %s",
		node, res.Received, res.Stored, res.Duplicates, size, time.Since(start).Round(time.Millisecond))
	return stream.SendAndClose(&res)
}

// store writes recs in one transaction and returns how many were new.
func (s *Server) store(ctx context.Context, recs []*schemav1.Record) (uint64, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, err
	}
	defer func() { _ = tx.Rollback() }()
	var n uint64
	for _, r := range recs {
		h := r.Header()
		if h.GetNodeId() == "" || h.GetSeq() == 0 {
			return 0, status.Errorf(codes.InvalidArgument, "record without node_id and seq: %v", h)
		}
		body, err := proto.Marshal(r)
		if err != nil {
			return 0, err
		}
		res, err := tx.Exec(`INSERT OR IGNORE INTO records VALUES (?, ?, ?, ?, ?)`,
			h.NodeId, h.Seq, h.Hlc, h.Priority, body)
		if err != nil {
			return 0, err
		}
		k, _ := res.RowsAffected()
		n += uint64(k)
	}
	return n, tx.Commit()
}

// Held reports how many records the control plane holds for node and the
// lowest and highest seq among them.
func (s *Server) Held(node string) (n, first, last uint64, err error) {
	err = s.db.QueryRow(`SELECT count(*), coalesce(min(seq), 0), coalesce(max(seq), 0)
		FROM records WHERE node_id = ?`, node).Scan(&n, &first, &last)
	if err != nil {
		err = fmt.Errorf("held %s: %w", node, err)
	}
	return n, first, last, err
}
