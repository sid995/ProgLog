// Package log implements a segment-based append-only log with
// memory-mapped index files for fast random-access reads.
package log

import (
	"fmt"
	"os"
	"path"

	api "proglog/api/v1"

	"google.golang.org/protobuf/proto"
)

// segment is a single segment of the log. It owns a store file (record
// data) and an index file (offset → position map), and is responsible
// for appending records, reading them back by offset, and reporting
// whether it has reached its configured size limits.
//
// A segment covers the offset range [baseOffset, baseOffset + N) where
// N is the number of records appended. The baseOffset is the offset of
// the segment's first record and is also used as the file name prefix.
type segment struct {
	store                  *store
	index                  *index
	baseOffset, nextOffset uint64
	config                 Config
}

// newSegment creates a new segment in the given directory with the
// specified base offset. It opens (or creates) the .store and .index
// files, memory-maps the index, and determines the next offset to
// assign by reading the last index entry.
//
// If the index is empty (fresh segment), nextOffset starts at baseOffset.
// Otherwise it is set to one past the last recorded offset, supporting
// recovery after a restart.
func newSegment(dir string, baseOffset uint64, c Config) (*segment, error) {
	s := &segment{
		baseOffset: baseOffset,
		config:     c,
	}
	var err error
	storeFile, err := os.OpenFile(
		path.Join(dir, fmt.Sprintf("%d%s", baseOffset, ".store")),
		os.O_RDWR|os.O_CREATE|os.O_APPEND,
		0644,
	)
	if err != nil {
		return nil, err
	}
	if s.store, err = newStore(storeFile); err != nil {
		return nil, err
	}
	indexFile, err := os.OpenFile(
		path.Join(dir, fmt.Sprintf("%d%s", baseOffset, ".index")),
		os.O_RDWR|os.O_CREATE,
		0644,
	)
	if err != nil {
		return nil, err
	}
	if s.index, err = newIndex(indexFile, c); err != nil {
		return nil, err
	}
	if off, _, err := s.index.Read(-1); err != nil {
		s.nextOffset = baseOffset
	} else {
		s.nextOffset = baseOffset + uint64(off) + 1
	}
	return s, nil
}

// Append writes a record to the segment. It assigns the record the
// segment's next offset, marshals it to protobuf, appends it to the
// store, writes the corresponding index entry, and advances the
// nextOffset counter.
//
// Append returns the offset assigned to the record.
func (s *segment) Append(record *api.Record) (offset uint64, err error) {
	cur := s.nextOffset
	record.Offset = cur
	p, err := proto.Marshal(record)
	if err != nil {
		return 0, err
	}
	_, pos, err := s.store.Append(p)
	if err != nil {
		return 0, err
	}
	if err = s.index.Write(
		uint32(s.nextOffset-uint64(s.baseOffset)),
		pos,
	); err != nil {
		return 0, err
	}
	s.nextOffset++
	return cur, nil
}

// Read reads the record at the given absolute offset. It converts the
// absolute offset to a relative offset (relative to the segment's
// baseOffset), looks up the store position via the index, reads the
// record bytes from the store, and unmarshals the protobuf.
//
// Read returns the decoded record, or an error if the offset is out
// of range for this segment.
func (s *segment) Read(off uint64) (*api.Record, error) {
	_, pos, err := s.index.Read(int64(off - s.baseOffset))
	if err != nil {
		return nil, err
	}
	p, err := s.store.Read(pos)
	if err != nil {
		return nil, err
	}
	record := &api.Record{}
	err = proto.Unmarshal(p, record)
	return record, err
}

// IsMaxed reports whether the segment has reached its configured size
// limit — either the store file or the index file has grown to its
// maximum allowed size.
func (s *segment) IsMaxed() bool {
	return s.store.size >= s.config.Segment.MaxStoreBytes ||
		s.index.size >= s.config.Segment.MaxIndexBytes
}

// Remove closes the segment and deletes both its store and index files
// from disk.
func (s *segment) Remove() error {
	if err := s.Close(); err != nil {
		return err
	}
	if err := os.Remove(s.index.Name()); err != nil {
		return err
	}
	if err := os.Remove(s.store.Name()); err != nil {
		return err
	}
	return nil
}

// Close syncs and closes both the index and store files.
func (s *segment) Close() error {
	if err := s.index.Close(); err != nil {
		return err
	}
	if err := s.store.Close(); err != nil {
		return err
	}
	return nil
}

// nearestMultiple returns the largest multiple of k that is ≤ j.
// It is used to round an arbitrary offset down to the nearest segment
// boundary when locating which segment contains a given offset.
func nearestMultiple(j, k int64) uint64 {
	if j >= 0 {
		return uint64((j / k) * k)
	}
	return uint64(((j - k + 1) / k) * k)
}
