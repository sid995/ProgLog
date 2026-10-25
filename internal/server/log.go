package server

import (
	"fmt"
	"sync"
)

// Log is the mutex-protected, in-memory log used only by the HTTP demo.
type Log struct {
	mu      sync.Mutex
	records []Record
}

// Record is the HTTP representation; JSON encodes Value as base64.
type Record struct {
	Value  []byte `json:"value"`
	Offset uint64 `json:"offset"`
}

// ErrOffsetNotFound signals that no in-memory record has the requested offset.
var ErrOffsetNotFound = fmt.Errorf("offset not found")

// NewLog creates an empty HTTP log; its records do not survive a restart.
func NewLog() *Log {
	return &Log{}
}

// Append assigns the next slice index as the offset and stores a copy of the record.
// The Value byte slice still shares its underlying storage with the caller.
func (c *Log) Append(record Record) (uint64, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	record.Offset = uint64(len(c.records))
	c.records = append(c.records, record)
	return record.Offset, nil
}

// Read returns the record at offset, or ErrOffsetNotFound beyond the slice.
// The returned Value shares its backing bytes with the stored record.
func (c *Log) Read(offset uint64) (Record, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if offset >= uint64(len(c.records)) {
		return Record{}, ErrOffsetNotFound
	}

	return c.records[offset], nil
}
