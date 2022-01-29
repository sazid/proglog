package log

import (
	"io"
	"os"

	"github.com/tysonmote/gommap"
)

const (
	offWidth uint64 = 4
	posWidth uint64 = 8
	// index entry width
	entWidth = offWidth + posWidth
)

type index struct {
	file *os.File
	mmap gommap.MMap
	size uint64
}

func newIndex(f *os.File, c Config) (*index, error) {
	idx := &index{
		file: f,
	}
	fi, err := os.Stat(f.Name())
	if err != nil {
		return nil, err
	}
	// Set the index size to the number of bytes in the file.
	idx.size = uint64(fi.Size())
	// Resize the file to be exactly `MaxIndexBytes` because this cannot be
	// resized later on while the memory is mapped.
	if err := os.Truncate(f.Name(), int64(c.Segment.MaxIndexBytes)); err != nil {
		return nil, err
	}
	// Create a mmap, given the file descriptor. This reads the whole file
	// contents into memory (RAM) for really fast access.
	// Note: a file descriptor is basically an unsigned integer which identifies
	// an open file in the kernel. The kernel uses this FD to know which file to
	// operate on when open()/close() etc. calls are issued.
	if idx.mmap, err = gommap.Map(
		idx.file.Fd(),
		gommap.PROT_READ|gommap.PROT_WRITE,
		gommap.MAP_SHARED,
	); err != nil {
		return nil, err
	}
	return idx, nil
}

// Read takes an offset in index and returns the associated record's "offset" and
// "position" in the store. If `in` is "-1" it'll return the last index entry
// if available.
func (i *index) Read(in int64) (recordOffset uint32, recordPos uint64, err error) {
	// If there's no index entry, we simply return EOF.
	if i.size == 0 {
		return 0, 0, io.EOF
	}

	// recordOffset temporarily holds the offset of the index's entry.
	if in == -1 {
		// Offset of the last entry.
		recordOffset = uint32((i.size / entWidth) - 1)
	} else {
		// Or the asked entry if its valid.
		recordOffset = uint32(in)
	}

	// recordPos temporarily holds the position of the index's entry.
	recordPos = uint64(recordOffset) * entWidth

	// If the last entry is invalid, we return EOF.
	if i.size < recordPos+entWidth {
		return 0, 0, io.EOF
	}

	// Read the record's "offset" in store, from the first 4 bytes.
	recordOffset = enc.Uint32(i.mmap[recordPos : recordPos+offWidth])
	// Read the record's "position" in store, from the next 8 bytes.
	recordPos = enc.Uint64(i.mmap[recordPos+offWidth : recordPos+entWidth])

	return recordOffset, recordPos, nil
}

// Write appends the given offset and position of a record to the index.
func (i *index) Write(off uint32, pos uint64) error {
	// If there's not enough room to add another entry, we return EOF.
	if uint64(len(i.mmap)) < i.size+entWidth {
		return io.EOF
	}
	// Otherwise, we put the "offset" of a record in the first 4 bytes.
	enc.PutUint32(i.mmap[i.size:i.size+offWidth], off)
	// And the "position" in the next 8 bytes.
	enc.PutUint64(i.mmap[i.size+offWidth:i.size+entWidth], pos)
	// Increase the index size by a single entry's width.
	i.size += uint64(entWidth)
	return nil
}

// Name returns the underlying file's name.
func (i *index) Name() string {
	return i.file.Name()
}

// Close does a graceful close/shutdown of the index. In case of a catastrophic
// failure, we should do sanity check at the start of the service/index
// construction (maybe scan through all the available index entries and then
// discard the rest?).
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
