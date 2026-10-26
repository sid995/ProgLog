package log

import (
	"context"
	"sync"

	"go.uber.org/zap"
	"google.golang.org/grpc"

	api "proglog/api/v1"
)

// Replicator implements discovery.Handler by copying a joining peer's log
// into LocalServer. Each peer is tracked by name so duplicate joins are ignored.
// It starts each copy at offset zero and does not resume from a saved position.
type Replicator struct {
	// DialOptions configure the outbound connection to each peer's RPC address.
	DialOptions []grpc.DialOption
	// LocalServer is the destination log that receives replicated records.
	LocalServer api.LogClient
	logger      *zap.Logger
	mu          sync.Mutex
	servers     map[string]chan struct{}
	closed      bool
	close       chan struct{}
}

// Join starts a background copy from addr unless that peer is already tracked
// or the replicator has been closed. The name identifies the connection for Leave.
func (r *Replicator) Join(name, addr string) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.init()

	if r.closed {
		return nil
	}
	if _, ok := r.servers[name]; ok {
		return nil
	}

	r.servers[name] = make(chan struct{})
	go r.replicate(addr, r.servers[name])
	return nil
}

// replicate consumes the peer from offset zero and appends each received record
// locally. Stream and append errors are logged and terminate this copy.
// The background context has no cancellation; the receive goroutine can remain
// blocked if this function exits while it is sending on records.
func (r *Replicator) replicate(addr string, leave chan struct{}) {
	cc, err := grpc.NewClient(addr, r.DialOptions...)
	if err != nil {
		r.logError(err, "failed to dial", addr)
		return
	}

	defer cc.Close()

	client := api.NewLogClient(cc)
	ctx := context.Background()
	stream, err := client.ConsumeStream(ctx, &api.ConsumeRequest{
		Offset: 0,
	})

	if err != nil {
		r.logError(err, "failed to consume", addr)
		return
	}

	records := make(chan *api.Record)
	go func() {
		for {
			recv, err := stream.Recv()
			if err != nil {
				r.logError(err, "failed to receive", addr)
				return
			}
			records <- recv.Record
		}
	}()

	for {
		select {
		case <-r.close:
			// Receiving close currently leaves the loop running; upgrade to return
			// here when shutdown semantics are fixed.
		case <-leave:
			return
		case record := <-records:
			_, err := r.LocalServer.Produce(ctx, &api.ProduceRequest{
				Record: record,
			})
			if err != nil {
				r.logError(err, "failed to produce", addr)
				return
			}
		}
	}
}

// Leave stops the tracked copy for name. Unknown names are harmless no-ops.
// Closing the per-peer signal stops the append loop but does not cancel its RPC.
func (r *Replicator) Leave(name string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.init()
	if _, ok := r.servers[name]; !ok {
		return nil
	}
	close(r.servers[name]) // closes channel for the specific connection in the server connection
	delete(r.servers, name)
	return nil
}

// init lazily creates the logger, peer signal map, and shared shutdown signal.
// It is called while mu is held by the public lifecycle methods.
func (r *Replicator) init() {
	if r.logger == nil {
		r.logger = zap.L().Named("replicator")
	}
	if r.servers == nil {
		r.servers = make(map[string]chan struct{})
	}
	if r.close == nil {
		r.close = make(chan struct{})
	}
}

// Close makes future joins no-ops and closes the shared shutdown signal.
// The current replicate loop does not exit on that signal, and Close does not
// wait for goroutines or close their gRPC connections synchronously.
func (r *Replicator) Close() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.init()

	if r.closed {
		return nil
	}
	r.closed = true
	close(r.close)
	return nil
}

// logError records a replication failure with the remote peer's RPC address.
func (r *Replicator) logError(err error, msg, addr string) {
	r.logger.Error(
		msg,
		zap.String("addr", addr),
		zap.Error(err),
	)
}
