// Package log implements a segment-based append-only log with
// memory-mapped index files for fast random-access reads.
package log

import (
	"io"
	"os"

	"github.com/tysonmote/gommap"
)

// offWidth is the number of bytes used to store a record's relative offset.
const offWidth uint64 = 4

// posWidth is the number of bytes used to store a record's position in the store file.
const posWidth uint64 = 8

// entWidth is the total size in bytes of a single index entry.
const entWidth = offWidth + posWidth

// index is a memory-mapped index file that maps record offsets to their
// byte positions in the segment's store file. Each entry is a fixed-width
// record of entWidth (12) bytes containing a uint32 relative offset and a
// uint64 store position, both big-endian.
//
// The file is pre-allocated to the configured maximum size at creation
// time. The size field tracks how many bytes of valid index data have
// been written, which is always a multiple of entWidth.
type index struct {
	file *os.File
	mmap gommap.MMap
	size uint64
}

// newIndex creates an index for the given file. It records the current
// file size (to support recovery after a restart), truncates the file to
// the configured maximum index size, and memory-maps it with read-write
// access. Recovery relies on Close truncating unused capacity; this does not
// reconstruct the valid entry count after an unclean shutdown.
func newIndex(f *os.File, c Config) (*index, error) {
	idx := &index{file: f}

	fi, err := os.Stat(f.Name())
	if err != nil {
		return nil, err
	}

	idx.size = uint64(fi.Size())
	if err := os.Truncate(f.Name(), int64(c.Segment.MaxIndexBytes)); err != nil {
		return nil, err
	}

	if idx.mmap, err = gommap.Map(
		idx.file.Fd(),
		gommap.PROT_READ|gommap.PROT_WRITE,
		gommap.MAP_SHARED,
	); err != nil {
		return nil, err
	}

	return idx, nil
}

// Read reads the index entry at the given offset and returns the
// associated record's position in the store.
//
// The offset is relative to the segment's base offset: 0 is the offset
// of the first entry, 1 is the second, and so on. Relative offsets
// (uint32) are used instead of absolute offsets (uint64) to save four
// bytes per entry, which matters at the scale of trillions of records.
//
// If in is -1, the last entry in the index is returned.
//
// Read returns the offset (out) and the record's position in the store
// file (pos). It returns io.EOF if the index is empty or if the
// requested offset exceeds the number of entries currently written.
func (i *index) Read(in int64) (out uint32, pos uint64, err error) {
	if i.size == 0 {
		return 0, 0, io.EOF
	}
	if in == -1 {
		out = uint32((i.size / entWidth) - 1)
	} else {
		out = uint32(in)
	}
	pos = uint64(out) * entWidth
	if i.size < pos+entWidth {
		return 0, 0, io.EOF
	}
	out = enc.Uint32(i.mmap[pos : pos+offWidth])
	pos = enc.Uint64(i.mmap[pos+offWidth : pos+entWidth])
	return out, pos, nil
}

// Write appends a new index entry mapping the given record offset to its
// position in the store file. The entry is written at the next available
// slot (i.size) and the size is advanced by entWidth.
//
// Write returns io.EOF if there is no room left in the pre-allocated
// index file for another entry.
func (i *index) Write(off uint32, pos uint64) error {
	if uint64(len(i.mmap)) < i.size+entWidth {
		return io.EOF
	}
	enc.PutUint32(i.mmap[i.size:i.size+offWidth], off)
	enc.PutUint64(i.mmap[i.size+offWidth:i.size+entWidth], pos)
	i.size += uint64(entWidth)
	return nil
}

// Name returns the index's file path
func (i *index) Name() string {
	return i.file.Name()
}

// Close syncs the memory-mapped region to disk, syncs the file, truncates
// the file to the actual data size (removing the unused pre-allocated
// space), and closes the file descriptor.
func (i *index) Close() error {
	if err := i.mmap.Sync(gommap.MS_SYNC); err != nil {
		return err
	}
	if err := i.file.Sync(); err != nil {
		return err
	}
	if err := i.file.Truncate(int64(i.size)); err != nil {
		return err
	}
	return i.file.Close()
}
