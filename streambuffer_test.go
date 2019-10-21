package iostream_test

import (
	"testing"

	"github.com/PullRequestInc/iostream"
	"github.com/stretchr/testify/require"
)

func TestFlushingEmpty(t *testing.T) {
	sb := iostream.NewStreamBuffer(2, 2)
	require.Len(t, sb.Flush(), 0)
}

func TestWritingEmpty(t *testing.T) {
	sb := iostream.NewStreamBuffer(2, 2)
	written, err := sb.WriteAt(nil, 0)
	require.NoError(t, err)
	require.Equal(t, 0, written)
	require.Len(t, sb.Flush(), 0)
}

func TestWritingBeforeBounds(t *testing.T) {
	sb := iostream.NewStreamBuffer(2, 2)
	_, err := sb.WriteAt([]byte("a"), -1)
	require.Equal(t, iostream.ErrBeforeBounds, err)
}

func TestWritingAfterBounds(t *testing.T) {
	sb := iostream.NewStreamBuffer(2, 2)
	_, err := sb.WriteAt([]byte("a"), 4)
	require.Equal(t, iostream.ErrAfterBounds, err)
}

func TestWritingOnceOnFirstBuffer(t *testing.T) {
	sb := iostream.NewStreamBuffer(2, 2)
	written, err := sb.WriteAt([]byte("a"), 0)
	require.NoError(t, err)
	require.Equal(t, 1, written)
	require.Equal(t, "a", string(sb.Flush()))
	require.Equal(t, "", string(sb.Flush()))
}

func TestWritingTwiceOnFirstBuffer(t *testing.T) {
	sb := iostream.NewStreamBuffer(2, 2)
	written, err := sb.WriteAt([]byte("a"), 0)
	require.NoError(t, err)
	require.Equal(t, 1, written)
	require.Equal(t, "a", string(sb.Flush()))
	written, err = sb.WriteAt([]byte("b"), 1)
	require.NoError(t, err)
	require.Equal(t, 1, written)
	require.Equal(t, "b", string(sb.Flush()))
	require.Equal(t, "", string(sb.Flush()))
}

func TestWritingTwiceOnFirstBufferBeforeFlush(t *testing.T) {
	sb := iostream.NewStreamBuffer(2, 2)
	written, err := sb.WriteAt([]byte("a"), 0)
	require.NoError(t, err)
	require.Equal(t, 1, written)
	written, err = sb.WriteAt([]byte("b"), 1)
	require.NoError(t, err)
	require.Equal(t, 1, written)
	require.Equal(t, "ab", string(sb.Flush()))
	require.Equal(t, "", string(sb.Flush()))
}

func TestWritingOnceAcrossTwoBuffers(t *testing.T) {
	sb := iostream.NewStreamBuffer(2, 2)
	written, err := sb.WriteAt([]byte("abc"), 0)
	require.NoError(t, err)
	require.Equal(t, 3, written)
	require.Equal(t, "abc", string(sb.Flush()))
	require.Equal(t, "", string(sb.Flush()))
}

func TestWritingTwiceAcrossTwoBuffersInOrder(t *testing.T) {
	sb := iostream.NewStreamBuffer(2, 2)
	written, err := sb.WriteAt([]byte("ab"), 0)
	require.NoError(t, err)
	require.Equal(t, 2, written)
	require.Equal(t, "ab", string(sb.Flush()))
	written, err = sb.WriteAt([]byte("cd"), 2)
	require.NoError(t, err)
	require.Equal(t, 2, written)
	require.Equal(t, "cd", string(sb.Flush()))
	require.Equal(t, "", string(sb.Flush()))
}

func TestWritingTwiceAcrossTwoBuffersOutOfOrder(t *testing.T) {
	sb := iostream.NewStreamBuffer(2, 2)
	written, err := sb.WriteAt([]byte("cd"), 2)
	require.NoError(t, err)
	require.Equal(t, 2, written)
	require.Equal(t, "", string(sb.Flush()))
	written, err = sb.WriteAt([]byte("ab"), 0)
	require.NoError(t, err)
	require.Equal(t, 2, written)
	require.Equal(t, "abcd", string(sb.Flush()))
	require.Equal(t, "", string(sb.Flush()))
}

func TestWritingOnRotatedBuffer(t *testing.T) {
	sb := iostream.NewStreamBuffer(2, 2)
	written, err := sb.WriteAt([]byte("abcd"), 0)
	require.NoError(t, err)
	require.Equal(t, 4, written)
	require.Equal(t, "abcd", string(sb.Flush()))
	written, err = sb.WriteAt([]byte("efgh"), 4)
	require.NoError(t, err)
	require.Equal(t, 4, written)
	require.Equal(t, "efgh", string(sb.Flush()))
	require.Equal(t, "", string(sb.Flush()))
}

func TestLotsOfWritesInVariousOrders(t *testing.T) {
	sb := iostream.NewStreamBuffer(2, 2)
	written, err := sb.WriteAt([]byte("c"), 2)
	require.NoError(t, err)
	require.Equal(t, 1, written)
	require.Equal(t, "", string(sb.Flush()))
	written, err = sb.WriteAt([]byte("a"), 0)
	require.NoError(t, err)
	require.Equal(t, 1, written)
	require.Equal(t, "a", string(sb.Flush()))
	written, err = sb.WriteAt([]byte("b"), 1)
	require.NoError(t, err)
	require.Equal(t, 1, written)
	written, err = sb.WriteAt([]byte("d"), 3)
	require.NoError(t, err)
	require.Equal(t, 1, written)
	require.Equal(t, "bcd", string(sb.Flush()))
	written, err = sb.WriteAt([]byte("efgh"), 4)
	require.NoError(t, err)
	require.Equal(t, 4, written)
	require.Equal(t, "efgh", string(sb.Flush()))
}
