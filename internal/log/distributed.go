package log

import (
	"bytes"
	"crypto/tls"
	"fmt"
	"io"
	"net"
	"os"
	"time"

	"path/filepath"

	"github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb"
	"google.golang.org/protobuf/proto"

	api "proglog/api/v1"
)

// DistributedLog applies writes through Raft and serves reads from its local log.
type DistributedLog struct {
	config Config
	log    *Log
	raft   *raft.Raft
}

// NewDistributedLog opens the local log and initializes the Raft stores and node.
func NewDistributedLog(dataDir string, config Config) (*DistributedLog, error) {
	l := &DistributedLog{
		config: config,
	}
	if err := l.setupLog(dataDir); err != nil {
		return nil, err
	}
	if err := l.setupRaft(dataDir); err != nil {
		return nil, err
	}
	return l, nil
}

// setupLog opens the application log under dataDir/log.
func (l *DistributedLog) setupLog(dataDir string) error {
	logDir := filepath.Join(dataDir, "log")
	if err := os.MkdirAll(logDir, 0755); err != nil {
		return err
	}
	var err error
	l.log, err = NewLog(logDir, l.config)
	return err
}

// setupRaft opens Raft's stores, creates its transport, and optionally bootstraps a cluster.
func (l *DistributedLog) setupRaft(dataDir string) error {
	fsm := &fsm{log: l.log}
	logDir := filepath.Join(dataDir, "raft", "log")
	if err := os.MkdirAll(logDir, 0755); err != nil {
		return err
	}
	logConfig := l.config
	// Raft reserves index 0; starting at 1 keeps its first real entry distinct from the empty-store sentinel.
	logConfig.Segment.InitialOffset = 1
	logStore, err := newLogStore(logDir, logConfig)
	if err != nil {
		return err
	}

	stableStore, err := raftboltdb.NewBoltStore(filepath.Join(dataDir, "raft", "stable"))
	if err != nil {
		return err
	}
	retain := 1
	snapshotStore, err := raft.NewFileSnapshotStore(
		filepath.Join(dataDir, "raft"),
		retain,
		os.Stderr,
	)
	if err != nil {
		return err
	}
	maxPool := 5
	timeout := 10 * time.Second
	transport := raft.NewNetworkTransport(
		l.config.Raft.StreamLayer,
		maxPool,
		timeout,
		os.Stderr,
	)

	// Keep Raft's safe defaults, then apply the timing and identity settings callers supplied.
	config := raft.DefaultConfig()
	config.LocalID = l.config.Raft.LocalID
	if l.config.Raft.HeartbeatTimeout != 0 {
		config.HeartbeatTimeout = l.config.Raft.HeartbeatTimeout
	}
	if l.config.Raft.ElectionTimeout != 0 {
		config.ElectionTimeout = l.config.Raft.ElectionTimeout
	}
	if l.config.Raft.LeaderLeaseTimeout != 0 {
		config.LeaderLeaseTimeout = l.config.Raft.LeaderLeaseTimeout
	}
	if l.config.Raft.CommitTimeout != 0 {
		config.CommitTimeout = l.config.Raft.CommitTimeout
	}

	l.raft, err = raft.NewRaft(
		config,
		fsm,
		logStore,
		stableStore,
		snapshotStore,
		transport,
	)
	if err != nil {
		return err
	}
	hasState, err := raft.HasExistingState(
		logStore,
		stableStore,
		snapshotStore,
	)
	if err != nil {
		return err
	}
	if l.config.Raft.Bootstrap && !hasState {
		// Bootstrap only a fresh store; bootstrapping persisted state would create a conflicting cluster.
		config := raft.Configuration{
			Servers: []raft.Server{{
				ID:      config.LocalID,
				Address: transport.LocalAddr(),
			}},
		}
		err = l.raft.BootstrapCluster(config).Error()
	}
	return err
}

// Append replicates a produce request and returns the offset assigned by the state machine.
func (l *DistributedLog) Append(record *api.Record) (uint64, error) {
	res, err := l.apply(
		AppendRequestType,
		&api.ProduceRequest{Record: record},
	)
	if err != nil {
		return 0, err
	}
	return res.(*api.ProduceResponse).Offset, nil
}

// apply encodes a typed request and waits for Raft to apply it to the state machine.
func (l *DistributedLog) apply(reqType RequestType, req proto.Message) (interface{}, error) {
	var buf bytes.Buffer
	// The leading byte lets every replica dispatch the remaining protobuf payload to the same FSM operation.
	_, err := buf.Write([]byte{byte(reqType)})
	if err != nil {
		return nil, err
	}
	b, err := proto.Marshal(req)
	if err != nil {
		return nil, err
	}
	_, err = buf.Write(b)
	if err != nil {
		return nil, err
	}
	timeout := 10 * time.Second
	future := l.raft.Apply(buf.Bytes(), timeout)
	if future.Error() != nil {
		return nil, future.Error()
	}
	res := future.Response()
	if err, ok := res.(error); ok {
		return nil, err
	}
	return res, nil
}

// Read returns a record from this node's local log at the requested offset.
func (l *DistributedLog) Read(offset uint64) (*api.Record, error) {
	return l.log.Read(offset)
}

// fsm applies committed Raft commands to the local application log.
var _ raft.FSM = (*fsm)(nil)

type fsm struct {
	log *Log
}

// RequestType identifies the command encoding stored in a Raft log entry.
type RequestType uint64

// AppendRequestType marks a serialized ProduceRequest command.
const AppendRequestType RequestType = 0

// Apply dispatches a committed Raft entry to the matching state machine operation.
func (l *fsm) Apply(record *raft.Log) interface{} {
	buf := record.Data
	// Every command starts with the type byte written by DistributedLog.apply.
	reqType := RequestType(buf[0])
	switch reqType {
	case AppendRequestType:
		return l.applyAppend(buf[1:])
	}
	return nil
}

// applyAppend decodes a produce command, appends its record, and returns its assigned offset.
func (l *fsm) applyAppend(b []byte) interface{} {
	var req api.ProduceRequest
	err := proto.Unmarshal(b, &req)
	if err != nil {
		return err
	}
	offset, err := l.log.Append(req.Record)
	if err != nil {
		return err
	}
	return &api.ProduceResponse{Offset: offset}
}

// Snapshot captures the log's framed record bytes for Raft snapshot persistence.
func (f *fsm) Snapshot() (raft.FSMSnapshot, error) {
	r := f.log.Reader()
	return &snapshot{reader: r}, nil
}

var _ raft.FSMSnapshot = (*snapshot)(nil)

// snapshot streams a captured log image to Raft's snapshot sink.
type snapshot struct {
	reader io.Reader
}

// Persist copies the log image into the sink and finalizes the snapshot.
func (s *snapshot) Persist(sink raft.SnapshotSink) error {
	if _, err := io.Copy(sink, s.reader); err != nil {
		// Cancel so Raft cannot treat a partial copy as a usable snapshot.
		_ = sink.Cancel()
		return err
	}
	return sink.Close()
}

// Release frees snapshot resources; the current snapshot holds only an io.Reader.
func (s *snapshot) Release() {}

// Restore replaces the local log with records from a Raft snapshot.
func (f *fsm) Restore(r io.ReadCloser) error {
	b := make([]byte, lenWidth)
	var buf bytes.Buffer
	for i := 0; ; i++ {
		_, err := io.ReadFull(r, b)
		if err == io.EOF {
			break
		} else if err != nil {
			return err
		}
		// The store framing is [8-byte size][protobuf record], matching store.Append.
		size := int64(enc.Uint64(b))
		if _, err := io.CopyN(&buf, r, size); err != nil {
			return err
		}
		record := &api.Record{}
		if err = proto.Unmarshal(buf.Bytes(), record); err != nil {
			return err
		}
		if i == 0 {
			// Recreate the log at the snapshot's first absolute offset before replaying records.
			f.log.Config.Segment.InitialOffset = record.Offset
			if err := f.log.Reset(); err != nil {
				return err
			}
		}
		if _, err = f.log.Append(record); err != nil {
			return err
		}
	}
	return nil
}

var _ raft.LogStore = (*logStore)(nil)

// logStore adapts the segmented application log to Raft's indexed log-store interface.
type logStore struct {
	*Log
}

// newLogStore opens the segmented store used for Raft entries.
func newLogStore(dir string, c Config) (*logStore, error) {
	log, err := NewLog(dir, c)
	if err != nil {
		return nil, err
	}
	return &logStore{log}, nil
}

// FirstIndex reports the lowest Raft log index retained in the store.
func (l *logStore) FirstIndex() (uint64, error) {
	return l.LowestOffset()
}

// LastIndex reports the highest Raft log index retained in the store.
func (l *logStore) LastIndex() (uint64, error) {
	return l.HighestOffset()
}

// GetLog loads one Raft entry from its stored record representation.
func (l *logStore) GetLog(index uint64, out *raft.Log) error {
	in, err := l.Read(index)
	if err != nil {
		return err
	}
	out.Data = in.Value
	out.Index = in.Offset
	out.Type = raft.LogType(in.Type)
	out.Term = in.Term
	return nil
}

// StoreLog persists one Raft entry using the batch storage path.
func (l *logStore) StoreLog(record *raft.Log) error {
	return l.StoreLogs([]*raft.Log{record})
}

// StoreLogs appends Raft entries to the segmented store in order.
func (l *logStore) StoreLogs(records []*raft.Log) error {
	// Persist Raft metadata beside each entry so it can be reconstructed after restart.
	for _, record := range records {
		_, err := l.Append(&api.Record{
			Value: record.Data,
			Term:  record.Term,
			Type:  uint32(record.Type),
		})
		if err != nil {
			return err
		}
	}
	return nil
}

// DeleteRange delegates its cutoff to Log.Truncate, which removes whole segments.
func (l *logStore) DeleteRange(min, max uint64) error {
	// The underlying log cannot trim records within a segment, so old whole segments are discarded.
	return l.Truncate(max)
}

var _ raft.StreamLayer = (*StreamLayer)(nil)

// StreamLayer separates Raft connections from other traffic on a shared listener.
type StreamLayer struct {
	ln              net.Listener
	serverTLSConfig *tls.Config
	peerTLSConfig   *tls.Config
}

// NewStreamLayer wraps a listener and optional inbound and outbound TLS settings.
func NewStreamLayer(
	ln net.Listener,
	serverTLSConfig,
	peerTLSConfig *tls.Config,
) *StreamLayer {
	return &StreamLayer{
		ln:              ln,
		serverTLSConfig: serverTLSConfig,
		peerTLSConfig:   peerTLSConfig,
	}
}

// RaftRPC is the one-byte protocol marker used to route accepted connections to Raft.
const RaftRPC = 1

// Dial opens a peer connection, writes the Raft marker, and applies client-side TLS when configured.
func (s *StreamLayer) Dial(
	addr raft.ServerAddress,
	timeout time.Duration,
) (net.Conn, error) {
	dialer := &net.Dialer{Timeout: timeout}
	var conn, err = dialer.Dial("tcp", string(addr))
	if err != nil {
		return nil, err
	}

	_, err = conn.Write([]byte{byte(RaftRPC)})
	if err != nil {
		return nil, err
	}
	if s.peerTLSConfig != nil {
		// Send the routing byte before TLS so the listener can select Raft before the handshake.
		conn = tls.Client(conn, s.peerTLSConfig)
	}
	return conn, err
}

// Accept reads the protocol marker and returns the connection with server-side TLS when configured.
func (s *StreamLayer) Accept() (net.Conn, error) {
	conn, err := s.ln.Accept()
	if err != nil {
		return nil, err
	}
	b := make([]byte, 1)

	_, err = conn.Read(b)
	if err != nil {
		return nil, err
	}

	if !bytes.Equal([]byte{byte(RaftRPC)}, b) {
		return nil, fmt.Errorf("not a raft rpc")
	}
	if s.serverTLSConfig != nil {
		// The routing byte has already been consumed in plaintext; TLS now protects Raft frames.
		return tls.Server(conn, s.serverTLSConfig), nil
	}
	return conn, nil
}

// Close closes the listener wrapped by the stream layer.
func (s *StreamLayer) Close() error {
	return s.ln.Close()
}

// Addr returns the wrapped listener's network address.
func (s *StreamLayer) Addr() net.Addr {
	return s.ln.Addr()
}

// Join adds a voter unless the exact ID and address already exist; it attempts to
// remove an existing server first when either identifier conflicts.
func (l *DistributedLog) Join(id, addr string) error {
	configFuture := l.raft.GetConfiguration()
	if err := configFuture.Error(); err != nil {
		return err
	}
	serverID := raft.ServerID(id)
	serverAddr := raft.ServerAddress(addr)
	for _, srv := range configFuture.Configuration().Servers {
		if srv.ID == serverID || srv.Address == serverAddr {
			if srv.ID == serverID && srv.Address == serverAddr {
				return nil
			}
			// Clear a conflicting prior membership before attempting to add the new voter.
			removeFuture := l.raft.RemoveServer(serverID, 0, 0)
			if err := removeFuture.Error(); err != nil {
				return err
			}
		}
	}
	addFuture := l.raft.AddVoter(serverID, serverAddr, 0, 0)
	if err := addFuture.Error(); err != nil {
		return err
	}
	return nil
}

// Leave removes a node from the Raft cluster configuration.
func (l *DistributedLog) Leave(id string) error {
	removeFuture := l.raft.RemoveServer(raft.ServerID(id), 0, 0)
	return removeFuture.Error()
}

// WaitForLeader polls until Raft reports a leader or timeout expires.
func (l *DistributedLog) WaitForLeader(timeout time.Duration) error {
	timeoutc := time.After(timeout)
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-timeoutc:
			return fmt.Errorf("timed out")
		case <-ticker.C:
			if l := l.raft.Leader(); l != "" {
				return nil
			}
		}
	}
}

// Close shuts down Raft and then closes the local log.
func (l *DistributedLog) Close() error {
	f := l.raft.Shutdown()
	if err := f.Error(); err != nil {
		return err
	}
	return l.log.Close()
}
