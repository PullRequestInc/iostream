package iostream_test

import (
	"bytes"
	"errors"
	"math/rand"
	"sync"
	"testing"
	"time"

	"github.com/PullRequestInc/iostream"
	"github.com/stretchr/testify/assert"
)

func TestS3SingleChunkSingleBuffer(t *testing.T) {
	writer := new(bytes.Buffer)
	numBuffers := 1
	bufSize := 10
	pool := iostream.NewBufPool(bufSize)

	stream := iostream.OpenS3WriterAtStream(writer, pool, numBuffers, bufSize)
	defer stream.Close()
	written, err := stream.WriteAt([]byte("1234"), 0)
	assert.NoError(t, err)
	assert.Equal(t, 4, written)
	assert.Equal(t, "1234", writer.String())
}

func TestS3WritingNotMultipleOfBufSize(t *testing.T) {
	writer := new(bytes.Buffer)
	numBuffers := 1
	bufSize := 10
	pool := iostream.NewBufPool(bufSize)

	stream := iostream.OpenS3WriterAtStream(writer, pool, numBuffers, bufSize)
	defer stream.Close()
	written, err := stream.WriteAt([]byte("1234"), 4)
	assert.EqualError(t, err, "unexpected offset 4 not multiple of 10")
	assert.Equal(t, 0, written)
	assert.Equal(t, "", writer.String())
}

type errorWriter struct {
	err error
}

func (ew *errorWriter) Write([]byte) (int, error) {
	return 0, ew.err
}

func TestS3WritingFails(t *testing.T) {
	// test with a writer that triggers a failure
	err := errors.New("test failure")
	writer := &errorWriter{err}
	numBuffers := 1
	bufSize := 10
	pool := iostream.NewBufPool(bufSize)

	stream := iostream.OpenS3WriterAtStream(writer, pool, numBuffers, bufSize)
	defer stream.Close()
	written, err := stream.WriteAt([]byte("1234"), 0)
	assert.EqualError(t, err, "test failure")
	assert.Equal(t, 0, written)
}

func TestS3WritingLargerThanBufferSize(t *testing.T) {
	writer := new(bytes.Buffer)
	numBuffers := 1
	bufSize := 10
	pool := iostream.NewBufPool(bufSize)

	stream := iostream.OpenS3WriterAtStream(writer, pool, numBuffers, bufSize)
	defer stream.Close()
	written, err := stream.WriteAt([]byte("1234567890123"), 0)
	assert.EqualError(t, err, "size of buffer being written 13 is greater than bufferSize 10")
	assert.Equal(t, 0, written)
	assert.Equal(t, "", writer.String())
}

func TestS3WriteSameOffsetTwiceErrors(t *testing.T) {
	writer := new(bytes.Buffer)
	numBuffers := 1
	bufSize := 10
	pool := iostream.NewBufPool(bufSize)

	stream := iostream.OpenS3WriterAtStream(writer, pool, numBuffers, bufSize)
	defer stream.Close()
	written, err := stream.WriteAt([]byte("1234567890"), 0)
	assert.NoError(t, err)
	assert.Equal(t, 10, written)
	written, err = stream.WriteAt([]byte("55555"), 0)
	assert.EqualError(t, err, "unexpectedly received same offset twice, 0")
	assert.Equal(t, 0, written)
}

func TestS3WriteMultipleChunksInOrder(t *testing.T) {
	writer := new(bytes.Buffer)
	numBuffers := 1
	bufSize := 10
	pool := iostream.NewBufPool(bufSize)

	stream := iostream.OpenS3WriterAtStream(writer, pool, numBuffers, bufSize)
	defer stream.Close()
	written, err := stream.WriteAt([]byte("1234567890"), 0)
	assert.NoError(t, err)
	assert.Equal(t, 10, written)
	written, err = stream.WriteAt([]byte("55555"), 10)
	assert.NoError(t, err)
	assert.Equal(t, 5, written)
	assert.Equal(t, "123456789055555", writer.String())
}

func TestS3WriteMultipleChunksOutOfOrder(t *testing.T) {
	bufSize := 4
	pool := iostream.NewBufPool(bufSize)

	chunks := []string{
		"1111",
		"2222",
		"3333",
		"4444",
		"5555",
		"6666",
		"7777",
		"8888",
		"9999",
		"00",
	}

	// this test simulates the behavior of the s3 downloader with concurrent workers pulling chunks off
	// with random amounts of delay in "downloading"
	run := func(numWorkers, numBuffers int) {
		writer := new(bytes.Buffer)
		stream := iostream.OpenS3WriterAtStream(writer, pool, numBuffers, bufSize)
		defer stream.Close()

		wg := sync.WaitGroup{}
		wg.Add(numWorkers)

		type block struct {
			data   []byte
			offset int64
		}

		ch := make(chan *block, numWorkers)

		for i := 0; i < numWorkers; i++ {
			go func() {
				defer wg.Done()
				for {
					// wait for random amount of time to simulate requests taking different amounts of time
					wait := rand.Intn(1000)
					time.Sleep(time.Microsecond * time.Duration(wait))

					select {
					case blk := <-ch:
						if blk == nil {
							// we're done, no more blocks of data
							return
						}
						written, err := stream.WriteAt(blk.data, blk.offset)
						assert.NoError(t, err)
						assert.Equal(t, len(blk.data), int(written))
					}
				}
			}()
		}

		for i, chunk := range chunks {
			b := &block{
				data:   []byte(chunk),
				offset: int64(i * bufSize),
			}
			ch <- b
		}

		close(ch)

		wg.Wait()

		assert.Equal(t, "11112222333344445555666677778888999900", writer.String())
	}

	// run with different worker numbers
	wg := sync.WaitGroup{}
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for numWorkers := 1; numWorkers < len(chunks)+1; numWorkers++ {
				// set the number of buffers to between numWorkers and 2x numWorkers
				for numBuffers := numWorkers; numBuffers < numWorkers*2; numBuffers++ {
					run(numWorkers, numBuffers)
				}
			}
		}()
	}
}
