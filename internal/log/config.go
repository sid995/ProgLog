package log

// Config controls segment rotation and the first offset of a new log.
type Config struct {
	Segment struct {
		// MaxStoreBytes is checked after append, so a record may exceed this limit.
		MaxStoreBytes uint64
		// MaxIndexBytes reserves index space; each entry needs 12 bytes.
		MaxIndexBytes uint64
		// InitialOffset applies only when no existing segments are found.
		InitialOffset uint64
	}
}
