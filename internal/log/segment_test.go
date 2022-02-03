package log

import (
	"io"
	"io/ioutil"
	"os"
	"testing"

	log_v1 "github.com/sazid/proglog/api/v1"
	"github.com/stretchr/testify/require"
)

func TestSegment(t *testing.T) {
	dir, _ := ioutil.TempDir("", "segment_test*")
	defer os.RemoveAll(dir)

	want := &log_v1.Record{Value: []byte("hello world")}

	c := Config{}
	c.Segment.MaxStoreBytes = 1024
	c.Segment.MaxIndexBytes = entWidth * 3

	s, err := newSegment(dir, 16, c)
	require.NoError(t, err)
	require.Equal(t, uint64(16), s.nextOffset, s.nextOffset)
	require.False(t, s.IsMaxed())

	for i := uint64(0); i < 3; i++ {
		off, err := s.Append(want)
		require.NoError(t, err)
		require.Equal(t, 16+i, off)

		got, err := s.Read(off)
		require.NoError(t, err)
		require.Equal(t, want.Value, got.Value)
	}

	_, err = s.Append(want)
	// check to see if EOF is present in the error chain.
	require.ErrorIs(t, err, io.EOF)

	// maxed index.
	require.True(t, s.IsMaxed())

	// limit the store's max size to 3 times the `want` Record's byte size.
	c.Segment.MaxStoreBytes = uint64(len(want.Value) * 3)
	c.Segment.MaxIndexBytes = 1024

	// create a new segment with the same baseOffset, which means it should open
	// the same store and index files for read/write.
	s, err = newSegment(dir, 16, c)
	require.NoError(t, err)

	// maxed store, there should already be 3 records stored in the store.
	require.True(t, s.IsMaxed())

	// clean up and remove any files associated with this segment.
	err = s.Remove()
	require.NoError(t, err)
	// new segment will be initialized with a clean state.
	s, err = newSegment(dir, 16, c)
	require.NoError(t, err)
	require.False(t, s.IsMaxed())
}
