// Package log implements a segment-based append-only log with
// memory-mapped index files for fast random-access reads.
package log

import (
	"fmt"
	"io"
	"os"
	"path"
	"sort"
	"strconv"
	"strings"
	"sync"

	api "proglog/api/v1"
)

// Log is the top-level log type. It manages a collection of segments
// stored in a single directory, each covering a contiguous range of
// offsets. Appends go to the active (last) segment; when that segment
// reaches its configured size limit, a new segment is created
// automatically.
//
// Log is safe for concurrent use. Appends and structural mutations
// (segment creation, truncation) take the write lock; reads take the
// read lock.
type Log struct {
	mu            sync.RWMutex
	Dir           string
	Config        Config
	activeSegment *segment
	segments      []*segment
}

// NewLog creates a new Log in the given directory with the provided
// configuration. It calls setup to recover any existing segments on
// disk. If MaxStoreBytes or MaxIndexBytes are zero, they default to
// 1024.
func NewLog(dir string, c Config) (*Log, error) {
	if c.Segment.MaxStoreBytes == 0 {
		c.Segment.MaxStoreBytes = 1024
	}
	if c.Segment.MaxIndexBytes == 0 {
		c.Segment.MaxIndexBytes = 1024
	}
	l := &Log{
		Dir:    dir,
		Config: c,
	}
	return l, l.setup()
}

// setup recovers segments from the log directory. It reads all files,
// extracts base offsets from their names (e.g. "0.store" → 0), sorts
// them, and opens each as a segment. If no segments exist, it creates
// one at the configured InitialOffset.
func (l *Log) setup() error {
	files, err := os.ReadDir(l.Dir)
	if err != nil {
		return err
	}

	var baseOffsets []uint64
	for _, file := range files {
		offStr := strings.TrimSuffix(
			file.Name(),
			path.Ext(file.Name()),
		)
		off, _ := strconv.ParseUint(offStr, 10, 0)
		baseOffsets = append(baseOffsets, off)
	}
	sort.Slice(baseOffsets, func(i, j int) bool {
		return baseOffsets[i] < baseOffsets[j]
	})
	for i := 0; i < len(baseOffsets); i++ {
		if err = l.newSegment(baseOffsets[i]); err != nil {
			return nil
		}
		i++
	}
	if l.segments == nil {
		if err = l.newSegment(l.Config.Segment.InitialOffset); err != nil {
			return nil
		}
	}
	return nil
}

// Append writes a record to the active segment and returns the offset
// assigned to it. If the active segment becomes full after the append,
// a new segment is created at the next offset.
func (l *Log) Append(record *api.Record) (uint64, error) {
	l.mu.Lock()
	defer l.mu.Unlock()

	off, err := l.activeSegment.Append(record)
	if err != nil {
		return 0, err
	}
	if l.activeSegment.IsMaxed() {
		err = l.newSegment(off + 1)
	}
	return off, err
}

// Read reads the record at the given absolute offset. It scans the
// segment list to find the segment whose offset range contains off,
// then delegates to that segment's Read.
//
// Read returns an error if the offset does not fall within any segment's
// range.
func (l *Log) Read(off uint64) (*api.Record, error) {
	l.mu.RLock()
	defer l.mu.RUnlock()

	var s *segment
	for _, segment := range l.segments {
		if segment.baseOffset <= off && off < segment.nextOffset {
			s = segment
			break
		}
	}
	if s == nil || s.nextOffset <= off {
		return nil, fmt.Errorf("offset out of range: %d", off)
	}
	return s.Read(off)
}

// Close flushes and closes all segments.
func (l *Log) Close() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	for _, segment := range l.segments {
		if err := segment.Close(); err != nil {
			return err
		}
	}
	return nil
}

// Remove closes all segments and deletes the entire log directory and
// its contents from disk.
func (l *Log) Remove() error {
	if err := l.Close(); err != nil {
		return err
	}
	return os.RemoveAll(l.Dir)
}

// Reset removes all data and re-initializes the log from scratch,
// creating a fresh segment at the configured InitialOffset.
func (l *Log) Reset() error {
	if err := l.Remove(); err != nil {
		return err
	}
	return l.setup()
}

// LowestOffset returns the base offset of the first segment, i.e. the
// smallest offset currently available in the log.
func (l *Log) LowestOffset() (uint64, error) {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return l.segments[0].baseOffset, nil
}

// HighestOffset returns the offset of the last record in the log
// (nextOffset of the last segment minus one). It returns 0 if the log
// is empty.
func (l *Log) HighestOffset() (uint64, error) {
	l.mu.RLock()
	defer l.mu.RUnlock()
	off := l.segments[len(l.segments)-1].nextOffset
	if off == 0 {
		return 0, nil
	}
	return off - 1, nil
}

// Truncate removes all segments whose records have offsets strictly
// less than lowest. Segments that end at or before lowest are deleted
// from disk; the remaining segments become the new segment list.
func (l *Log) Truncate(lowest uint64) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	var segments []*segment
	for _, s := range l.segments {
		if s.nextOffset < lowest+1 {
			if err := s.Remove(); err != nil {
				return err
			}
			continue
		}
		segments = append(segments, s)
	}
	l.segments = segments
	return nil
}

// Reader returns an io.Reader that streams all record bytes from all
// segments in order, starting from the beginning of the first segment.
// It is used for replication (sending the full log to a follower).
func (l *Log) Reader() io.Reader {
	l.mu.Lock()
	defer l.mu.Unlock()
	readers := make([]io.Reader, len(l.segments))
	for i, segment := range l.segments {
		readers[i] = &originReader{segment.store, 0}
	}
	return io.MultiReader(readers...)
}

// originReader wraps a store to provide sequential io.Reader access by
// tracking the current read offset internally.
type originReader struct {
	*store
	off int64
}

// Read reads up to len(p) bytes from the store at the current internal
// offset and advances the offset by the number of bytes read.
func (o *originReader) Read(p []byte) (int, error) {
	n, err := o.ReadAt(p, o.off)
	o.off += int64(n)
	return n, err
}

// newSegment creates a new segment at the given offset, appends it to
// the segment list, and sets it as the active segment.
func (l *Log) newSegment(off uint64) error {
	s, err := newSegment(l.Dir, off, l.Config)
	if err != nil {
		return err
	}
	l.segments = append(l.segments, s)
	l.activeSegment = s
	return nil
}
