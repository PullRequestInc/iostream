package iostream_test

import (
	"bytes"
	"sync"
	"testing"
	"time"

	"github.com/PullRequestInc/iostream"
	"github.com/stretchr/testify/require"
)

func TestSingleChunkSingleBuffer(t *testing.T) {
	buf := new(bytes.Buffer)
	stream := iostream.OpenWriterAtStream(buf, 4, 2)

	written, err := stream.WriteAt([]byte("1234"), 0)
	stream.Close()
	require.NoError(t, err)
	require.Equal(t, 4, written)
	require.Equal(t, "1234", buf.String())
}

func TestMultipleChunksSingleBuffer(t *testing.T) {
	buf := new(bytes.Buffer)
	stream := iostream.OpenWriterAtStream(buf, 4, 2)

	_, err := stream.WriteAt([]byte("12"), 0)
	require.NoError(t, err)
	_, err = stream.WriteAt([]byte("34"), 2)
	require.NoError(t, err)

	stream.Close()

	require.Equal(t, "1234", buf.String())
}

func TestMultipleChunksWithBlockedChunksBeyondBuffer(t *testing.T) {
	buf := new(bytes.Buffer)
	stream := iostream.OpenWriterAtStream(buf, 4, 2)

	// this write should get blocked so do it on a separate goroutine
	go func() {
		_, err := stream.WriteAt([]byte("90123456"), 8)
		require.NoError(t, err)
	}()

	// this write should get blocked so do it on a separate goroutine. it will never
	// actually get written so we will use it to test behavior of blocked writes getting
	// unblocked by the Close()
	cleanedUp := false
	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		stream.WriteAt([]byte("far away"), 100)
		cleanedUp = true
		wg.Done()
	}()

	// give the goroutine above a second to make sure it calls before the next two and exercises
	// the backed up blocking behavior
	time.Sleep(1 * time.Millisecond)

	_, err := stream.WriteAt([]byte("1234"), 0)
	require.NoError(t, err)

	// should only have written 1234 yet
	require.Equal(t, "1234", buf.String())

	_, err = stream.WriteAt([]byte("5678"), 4)
	require.NoError(t, err)

	require.False(t, cleanedUp)

	stream.Close()
	wg.Wait()

	require.True(t, cleanedUp)

	require.Equal(t, "1234567890123456", buf.String())
}
