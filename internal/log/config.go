package log

import "github.com/hashicorp/raft"

// Config controls segment rotation and the first offset of a new log.
type Config struct {
	Raft struct {
		raft.Config
		// StreamLayer carries Raft traffic over the node's listener, optionally using TLS.
		StreamLayer *StreamLayer
		// Bootstrap initializes a new Raft cluster only when its stores have no state.
		Bootstrap bool
	}
	Segment struct {
		// MaxStoreBytes is checked after append, so a record may exceed this limit.
		MaxStoreBytes uint64
		// MaxIndexBytes reserves index space; each entry needs 12 bytes.
		MaxIndexBytes uint64
		// InitialOffset applies only when no existing segments are found.
		InitialOffset uint64
	}
}
