package discovery

import (
	"net"

	"github.com/hashicorp/serf/serf"
	"go.uber.org/zap"
)

// Membership discovers peers through Serf gossip and forwards changes to a handler.
type Membership struct {
	Config
	handler Handler
	serf    *serf.Serf
	events  chan serf.Event
	logger  *zap.Logger
}

// New starts gossip and event processing, then joins any configured seed peers.
func New(handler Handler, config Config) (*Membership, error) {
	c := &Membership{
		Config:  config,
		handler: handler,
		logger:  zap.L().Named("membership"),
	}
	if err := c.setupSerf(); err != nil {
		return nil, err
	}
	return c, nil
}

// Config identifies the gossip node and its seed peers.
// Tags should include rpc_addr, the application RPC address passed to Handler.Join;
// BindAddr is the separate host:port used for gossip.
type Config struct {
	NodeName       string
	BindAddr       string
	Tags           map[string]string
	StartJoinAddrs []string
}

// setupSerf binds Serf and starts consuming events before joining seed nodes.
// An error joining seeds is returned after Serf and its event goroutine have started.
func (m *Membership) setupSerf() (err error) {
	addr, err := net.ResolveTCPAddr("tcp", m.BindAddr)
	if err != nil {
		return err
	}
	config := serf.DefaultConfig()
	config.Init()
	config.MemberlistConfig.BindAddr = addr.IP.String()
	config.MemberlistConfig.BindPort = addr.Port
	m.events = make(chan serf.Event)
	config.EventCh = m.events
	config.Tags = m.Tags
	config.NodeName = m.NodeName
	m.serf, err = serf.Create(config)
	if err != nil {
		return err
	}
	go m.eventHandler()
	if m.StartJoinAddrs != nil {
		_, err = m.serf.Join(m.StartJoinAddrs, true)
		if err != nil {
			return err
		}
	}
	return nil
}

// Handler receives remote peer changes; callbacks run serially in the event loop.
type Handler interface {
	// Join receives a peer name and its rpc_addr tag, rather than its gossip address.
	Join(name, addr string) error
	// Leave handles both graceful departures and peers declared failed by gossip.
	Leave(name string) error
}

// eventHandler dispatches join, leave, and failure events for remote members.
// A local leave/failure stops dispatch entirely; unrelated Serf events are ignored.
func (m *Membership) eventHandler() {
	for e := range m.events {
		switch e.EventType() {
		case serf.EventMemberJoin:
			for _, member := range e.(serf.MemberEvent).Members {
				if m.isLocal(member) {
					continue
				}
				m.handleJoin(member)
			}
		case serf.EventMemberLeave, serf.EventMemberFailed:
			for _, member := range e.(serf.MemberEvent).Members {
				if m.isLocal(member) {
					return
				}
				m.handleLeave(member)
			}
		}
	}
}

// isLocal compares Serf node names to avoid notifying the handler about this node.
func (m *Membership) isLocal(member serf.Member) bool {
	return m.serf.LocalMember().Name == member.Name
}

// Members returns the current Serf membership view, including the local node.
func (m *Membership) Members() []serf.Member {
	return m.serf.Members()
}

// Leave gracefully notifies the cluster that this node is departing, then
// shuts down Serf's network resources. Shutdown runs even if broadcasting the
// leave fails, and the leave error takes precedence in that case.
func (m *Membership) Leave() error {
	leaveErr := m.serf.Leave()
	shutdownErr := m.serf.Shutdown()
	if leaveErr != nil {
		return leaveErr
	}
	return shutdownErr
}

// logError attaches the peer identity and advertised RPC address to callback failures.
func (m *Membership) logError(err error, msg string, member serf.Member) {
	m.logger.Error(
		msg,
		zap.Error(err),
		zap.String("name", member.Name),
		zap.String("rpc_addr", member.Tags["rpc_addr"]),
	)
}

// handleJoin forwards a discovered peer and logs callback errors without retrying.
func (m *Membership) handleJoin(member serf.Member) {
	if err := m.handler.Join(member.Name, member.Tags["rpc_addr"]); err != nil {
		m.logError(err, "failed to join", member)
	}
}

// handleLeave forwards a departed peer and logs callback errors without retrying.
func (m *Membership) handleLeave(member serf.Member) {
	if err := m.handler.Leave(member.Name); err != nil {
		m.logError(err, "failed to leave", member)
	}
}
