// Package log implements a segment-based append-only log with
// memory-mapped index files for fast random-access reads.
package log

import (
	"bufio"
	"encoding/binary"
	"os"
	"sync"
)

// enc is the byte order used for all store record length fields.
var enc = binary.BigEndian

// lenWidth is the number of bytes used to store a record's length
// prefix in the store file.
const lenWidth = 8

// store is a segment's data file. It holds records as length-prefixed
// entries: an 8-byte big-endian uint64 followed by the record's bytes.
//
// Appends are buffered via bufio.Writer for throughput. Reads use
// ReadAt (which does not advance the file offset) so that concurrent
// random-access reads do not interfere with the write cursor. The
// mutex serializes all access because the buffered writer and ReadAt
// share the same underlying file descriptor.
type store struct {
	*os.File
	mu   sync.Mutex
	buf  *bufio.Writer
	size uint64
}

// newStore opens an existing store file for reading and writing. It
// records the file's current size so that Append can continue from
// where a previous writer left off (supporting recovery after restart).
func newStore(f *os.File) (*store, error) {
	fi, err := os.Stat(f.Name())
	if err != nil {
		return nil, err
	}
	size := uint64(fi.Size())
	return &store{
		File: f,
		size: size,
		buf:  bufio.NewWriter(f),
	}, nil
}

// Append writes a record to the store. The record is length-prefixed
// with an 8-byte big-endian uint64 followed by the raw payload.
//
// Append returns the total number of bytes written (payload + length
// prefix) and the byte position at which the record was written.
func (s *store) Append(p []byte) (n uint64, pos uint64, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	pos = s.size
	if err := binary.Write(s.buf, enc, uint64(len(p))); err != nil {
		return 0, 0, err
	}
	w, err := s.buf.Write(p)
	if err != nil {
		return 0, 0, err
	}
	w += lenWidth
	s.size += uint64(w)
	return uint64(w), pos, nil
}

// Read reads the record stored at the given byte position. It first
// reads the 8-byte length prefix, then reads that many bytes of
// payload. The buffer is flushed before reading to ensure any
// pending writes are visible to ReadAt.
func (s *store) Read(pos uint64) ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if err := s.buf.Flush(); err != nil {
		return nil, err
	}
	size := make([]byte, lenWidth)
	if _, err := s.File.ReadAt(size, int64(pos)); err != nil {
		return nil, err
	}
	b := make([]byte, enc.Uint64(size))
	if _, err := s.File.ReadAt(b, int64(pos+lenWidth)); err != nil {
		return nil, err
	}
	return b, nil
}

// ReadAt reads len(p) bytes from the store at the given byte offset.
// It is a pass-through to the underlying file's ReadAt, used when the
// caller needs raw byte access (e.g., reading a length prefix at a
// known offset). The buffer is flushed before reading.
func (s *store) ReadAt(p []byte, off int64) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.buf.Flush(); err != nil {
		return 0, err
	}
	return s.File.ReadAt(p, off)
}

// Close flushes the buffered writer and closes the underlying file.
func (s *store) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	err := s.buf.Flush()
	if err != nil {
		return err
	}
	return s.File.Close()
}
