package iostream_test

import (
	"bytes"
	"context"
	"testing"

	"github.com/PullRequestInc/cmn/pkg/iostream"
	"github.com/stretchr/testify/require"
)

func TestSingleChunk(t *testing.T) {
	// ctx, cancel := context.WithTimeout(context.Background(), time.Second*1)
	// defer cancel()
	ctx := context.Background()
	buf := new(bytes.Buffer)
	stream := iostream.OpenWriterAtStream(ctx, buf, 4)

	written, err := stream.WriteAt([]byte("1234"), 0)
	stream.Close()
	require.NoError(t, err)
	require.Equal(t, 4, written)

	require.NoError(t, err)
	require.Equal(t, "1234", buf.String())
}

func TestMultipleChunks(t *testing.T) {
	// ctx, cancel := context.WithTimeout(context.Background(), time.Second*1)
	// defer cancel()
	ctx := context.Background()
	buf := new(bytes.Buffer)
	stream := iostream.OpenWriterAtStream(ctx, buf, 4)

	_, err := stream.WriteAt([]byte("12"), 0)
	require.NoError(t, err)
	_, err = stream.WriteAt([]byte("34"), 2)
	require.NoError(t, err)

	stream.Close()

	require.NoError(t, err)
	require.Equal(t, "1234", buf.String())
}
