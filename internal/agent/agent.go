// Package agent assembles a persistent log, RPC server, discovery, and replication
// into a node managed through a single lifecycle.
package agent

import (
	"bytes"
	"crypto/tls"
	"fmt"
	"io"
	"net"
	"proglog/internal/discovery"
	"sync"
	"time"

	"proglog/internal/auth"
	"proglog/internal/log"
	"proglog/internal/server"

	"github.com/hashicorp/raft"
	"github.com/soheilhy/cmux"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

// Agent owns one node's persistent log, gRPC server, peer membership, and replicator.
type Agent struct {
	Config
	mux cmux.CMux
	// log and server are the local storage and RPC service used by this node.
	log    *log.DistributedLog
	server *grpc.Server
	// membership forwards Serf changes to replicator.
	membership *discovery.Membership
	// replicator *log.Replicator
	shutdown bool
	// shutdowns is closed once shutdown begins; no code currently waits on it.
	shutdowns chan struct{}
	// shutdownLock makes the idempotent shutdown path safe for concurrent callers.
	shutdownLock sync.Mutex
}

// Config contains the addresses, credentials, storage path, and ACL files for a node.
type Config struct {
	// ServerTLSConfig secures inbound gRPC connections when non-nil.
	ServerTLSConfig *tls.Config
	// PeerTLSConfig secures outbound gRPC connections to this node and its peers.
	PeerTLSConfig *tls.Config
	// DataDir stores this node's persistent segmented log.
	DataDir string
	// BindAddr is the host and gossip port used by Serf.
	BindAddr string
	// RPCPort is combined with BindAddr's host to form the advertised RPC address.
	RPCPort int
	// NodeName is the Serf identity advertised to the cluster.
	NodeName string
	// StartJoinAddrs optionally lists existing gossip nodes to contact at startup.
	StartJoinAddrs []string
	// ACLModelFile and ACLPolicyFile configure RPC authorization.
	ACLModelFile  string
	ACLPolicyFile string
	Bootstrap     bool
}

// New initializes the logger, log, RPC server, then gossip membership in order.
// Setup stops at the first error; resources created by earlier steps are not rolled back.
func New(config Config) (*Agent, error) {
	a := &Agent{
		Config:    config,
		shutdowns: make(chan struct{}),
	}
	setup := []func() error{
		a.setupLogger,
		a.setupMux,
		a.setupLog,
		a.setupServer,
		a.setupMembership,
	}
	for _, fn := range setup {
		if err := fn(); err != nil {
			return nil, err
		}
	}
	go a.serve()
	return a, nil
}

func (a *Agent) setupMux() error {
	rpcAddr := fmt.Sprintf(":%d", a.Config.RPCPort)
	ln, err := net.Listen("tcp", rpcAddr)
	if err != nil {
		return err
	}
	a.mux = cmux.New(ln)
	return nil
}

// RPCAddr reuses BindAddr's host and combines it with the separate RPC port.
func (c Config) RPCAddr() (string, error) {
	host, _, err := net.SplitHostPort(c.BindAddr)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("%s:%d", host, c.RPCPort), nil
}

// setupLogger installs a development logger as the process-wide zap logger.
func (a *Agent) setupLogger() error {
	logger, err := zap.NewDevelopment()
	if err != nil {
		return err
	}
	zap.ReplaceGlobals(logger)
	return nil
}

// setupLog opens or creates the persistent log in DataDir.
func (a *Agent) setupLog() error {
	// var err error
	// a.log, err = log.NewLog(a.Config.DataDir, log.Config{})
	// return err
	raftLN := a.mux.Match(func(reader io.Reader) bool {
		b := make([]byte, 1)
		if _, err := reader.Read(b); err != nil {
			return false
		}
		return bytes.Equal(b, []byte{byte(log.RaftRPC)})
	})
	logConfig := log.Config{}
	logConfig.Raft.StreamLayer = log.NewStreamLayer(
		raftLN,
		a.Config.ServerTLSConfig,
		a.Config.PeerTLSConfig,
	)
	logConfig.Raft.LocalID = raft.ServerID(a.Config.NodeName)
	logConfig.Raft.Bootstrap = a.Config.Bootstrap

	var err error
	a.log, err = log.NewDistributedLog(a.Config.DataDir, logConfig)

	if err != nil {
		return err
	}
	if a.Config.Bootstrap {
		err = a.log.WaitForLeader(3 * time.Second)
	}
	return err
}

// setupServer builds the authorized gRPC service and serves it on the RPC address.
// ServerTLSConfig nil leaves transport credentials unset.
func (a *Agent) setupServer() error {
	authorizer := auth.New(a.Config.ACLModelFile, a.Config.ACLPolicyFile)
	serverConfig := &server.Config{
		CommitLog:  a.log,
		Authorizer: authorizer,
	}

	var opts []grpc.ServerOption
	if a.Config.ServerTLSConfig != nil {
		creds := credentials.NewTLS(a.Config.ServerTLSConfig)
		opts = append(opts, grpc.Creds(creds))
	}

	var err error
	a.server, err = server.NewGRPCServer(serverConfig, opts...)
	if err != nil {
		return err
	}

	grpcLn := a.mux.Match(cmux.Any())

	// rpcAddr, err := a.RPCAddr()
	// if err != nil {
	// 	return err
	// }

	// ln, err := net.Listen("tcp", rpcAddr)
	// if err != nil {
	// 	return err
	// }

	go func() {
		if err := a.server.Serve(grpcLn); err != nil {
			_ = a.Shutdown()
		}
	}()
	return err
}

// setupMembership connects this node's replicator to its own RPC server, then
// advertises that RPC address through Serf so new peers can replicate from it.
// PeerTLSConfig controls both this local client and outbound peer connections.
func (a *Agent) setupMembership() error {
	rpcAddr, err := a.Config.RPCAddr()
	if err != nil {
		return err
	}
	// var opts []grpc.DialOption
	// if a.Config.PeerTLSConfig != nil {
	// 	opts = append(opts, grpc.WithTransportCredentials(credentials.NewTLS(a.Config.PeerTLSConfig)))
	// }

	// conn, err := grpc.NewClient(rpcAddr, opts...)
	// if err != nil {
	// 	return err
	// }
	// client := api.NewLogClient(conn)
	// a.replicator = &log.Replicator{
	// 	DialOptions: opts,
	// 	LocalServer: client,
	// }
	// a.membership, err = discovery.New(a.replicator, discovery.Config{
	// 	NodeName: a.Config.NodeName,
	// 	BindAddr: a.Config.BindAddr,
	// 	Tags: map[string]string{
	// 		"rpc_addr": rpcAddr,
	// 	},
	// 	StartJoinAddrs: a.Config.StartJoinAddrs,
	// })
	// return err
	a.membership, err = discovery.New(a.log, discovery.Config{
		NodeName: a.Config.NodeName,
		BindAddr: a.Config.BindAddr,
		Tags: map[string]string{
			"rpc_addr": rpcAddr,
		},
		StartJoinAddrs: a.Config.StartJoinAddrs,
	})
	return err
}

// Shutdown is idempotent and leaves the gossip group before stopping replication,
// the RPC server, and the log. It stops at the first cleanup error.
func (a *Agent) Shutdown() error {
	a.shutdownLock.Lock()
	defer a.shutdownLock.Unlock()

	if a.shutdown {
		return nil
	}
	a.shutdown = true
	close(a.shutdowns)

	shutdown := []func() error{
		// Notify peers first so their handlers can stop replication from this node.
		a.membership.Leave,
		func() error {
			a.server.GracefulStop()
			return nil
		},
		a.log.Close,
	}
	for _, fn := range shutdown {
		if err := fn(); err != nil {
			return err
		}
	}
	return nil
}

func (a *Agent) serve() error {
	if err := a.mux.Serve(); err != nil {
		_ = a.Shutdown()
		return err
	}
	return nil
}
