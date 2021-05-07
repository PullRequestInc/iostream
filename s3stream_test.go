package iostream_test

import (
	"bytes"
	"testing"

	"github.com/PullRequestInc/iostream"
	"github.com/stretchr/testify/assert"
)

func TestS3SingleChunkSingleBuffer(t *testing.T) {
	writer := new(bytes.Buffer)
	numBuffers := 1
	bufSize := 100
	pool := iostream.NewBufPool(bufSize)

	stream := iostream.OpenS3WriterAtStream(writer, pool, numBuffers, bufSize)
	written, err := stream.WriteAt([]byte("1234"), 0)
	stream.Close()
	assert.NoError(t, err)
	assert.Equal(t, 4, written)
	assert.Equal(t, "1234", writer.String())
}
