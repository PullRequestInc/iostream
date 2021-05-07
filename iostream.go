package iostream

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"sort"
	"sync"
	"time"
)

// WriterAtStream is meant to convert an io.WriterAt to an io.Writer using an in memory buffer.
// It will use mutliple buffers, but prevent letting the entire file being written in memory.
// It should only be used if there is a guarantee that the next chunk of bytes that the writer
// needs will eventually be passed if WriteAt is blocking for other writes further in the stream.
// The io.WriterAt should also only attempt to write a chunk of bytes once, as once a chunk has
// been written, it is assumed that the buffer's offset can advance.
type WriterAtStream struct {
	bufferPool        BufPool
	writer            io.Writer
	newChunks         chan *byteChunk
	closed            chan int // signalled when Close is called
	done              chan int // signalled after close has been completed
	bufferSize        int
	numBuffers        int
	err               error
	lastWrittenOffset int64
	offsetLock        *sync.RWMutex
}

// OpenWriterAtStream opened a WriterAtStream. The stream should be closed by the caller to clean up
// resources. If an error occurs, all subsequent WriteAts will begin to error out.
func OpenWriterAtStream(writer io.Writer, bufferPool BufPool, numBuffers, bufferSize int) *WriterAtStream {
	stream := &WriterAtStream{
		bufferPool: bufferPool,
		writer:     writer,
		newChunks:  make(chan *byteChunk),
		closed:     make(chan int),
		done:       make(chan int),
		bufferSize: bufferSize,
		numBuffers: numBuffers,
		offsetLock: new(sync.RWMutex),
	}
	go stream.start()
	return stream
}

type byteChunk struct {
	bytes    []byte
	offset   int64
	response chan *writeAtResp
}

type toWriteChunk struct {
	buf    *bytes.Buffer
	offset int64
}

type wroteChunk struct {
	buf    *bytes.Buffer
	offset int64
	err    error
}

type writeAtResp struct {
	err error
}

func (s *WriterAtStream) writeChunkIfNeeded(doneWriting chan *wroteChunk, chunk *toWriteChunk) (successfullyWrote bool) {
	// if this is the first block, or the next block that we are supposed to write, then go ahead and write it
	if s.lastWrittenOffset == 0 && chunk.offset == 0 || s.lastWrittenOffset == chunk.offset-int64(s.bufferSize) {
		var written int
		toWriteBytes := chunk.buf.Bytes()
		for len(toWriteBytes) > written {
			w, err := s.writer.Write(toWriteBytes)
			if err != nil {
				wrote := &wroteChunk{
					buf: chunk.buf,
					err: err,
				}
				doneWriting <- wrote
				return false
			}
			written += w
		}

		// update the last written offset
		s.offsetLock.Lock()
		s.lastWrittenOffset = chunk.offset
		s.offsetLock.Unlock()

		// notify done writing without errors
		wrote := &wroteChunk{
			buf: chunk.buf,
		}
		doneWriting <- wrote
		return true
	}
	return false
}

func (s *WriterAtStream) start() {
	// backedUp holds byteChunks that are beyond our buffer and are currently being blocked
	// until the buffer grows to accomodate them as a backpressure mechanism.
	// TODO: Use linked list?
	var backedUp []*byteChunk

	toWrite := make(chan *toWriteChunk, s.numBuffers)
	doneWriting := make(chan *wroteChunk, s.numBuffers)

	// spin off a writer worker thread
	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()

		var pendingWrites []*toWriteChunk

		for {
			select {
			case chunk := <-toWrite:
				if chunk == nil {
					// done/channel closed
					return
				}

				successfullyWrote := s.writeChunkIfNeeded(doneWriting, chunk)
				if !successfullyWrote {
					// add this chunk to the pendingWrites if we weren't able to write it yet and we'll try again
					// later after a write succeeds.
					pendingWrites = append(pendingWrites, chunk)

					// TODO: would be potentially faster with a min heap but I'm lazy and there are only going to
					// be small number of items in here (should be less than or equal to numBuffers)
					sort.SliceStable(pendingWrites, func(i, j int) bool {
						return pendingWrites[i].offset < pendingWrites[j].offset
					})
					break
				}

				// if we successfully wrote, then lets try the pending chunks
				var removedOffsets map[int64]bool
				for _, c := range pendingWrites {
					successfullyWrote := s.writeChunkIfNeeded(doneWriting, c)
					if !successfullyWrote {
						break
					}
					removedOffsets[c.offset] = true
				}

				// remove the pending chunks that we successfully processed
				newPendingWrites := make([]*toWriteChunk, 0, len(pendingWrites)-len(removedOffsets))
				for _, c := range pendingWrites {
					if removedOffsets[c.offset] {
						continue
					}
					newPendingWrites = append(pendingWrites, c)
				}
				pendingWrites = newPendingWrites
			}
		}
	}()

	var latestChunk *byteChunk
	var chunksBeingWritten int

	for {
		select {
		case <-s.closed:
			closedErr := errors.New("WriterAtStream Closed")
			// set the error in case any more writes try to come in
			s.err = closedErr
			close(toWrite)

			// wait for the writer to complete
			wg.Wait()

			close(doneWriting)
			close(s.newChunks)

			for chunk := range s.newChunks {
				chunk.response <- &writeAtResp{
					err: closedErr,
				}
			}
			for _, chunk := range backedUp {
				chunk.response <- &writeAtResp{
					err: closedErr,
				}
			}

			close(s.done)
			return
		case chunk := <-s.newChunks:
			// copy to internal buffer
			buf := s.bufferPool.Get()
			copy(buf.Bytes(), chunk.bytes)

			// since we have a new chunk, we can unblock the previous latest one. this is mainly
			// just to prevent all of the WriteAts from continuing before the last write has occurred.
			if latestChunk != nil {
				latestChunk.response <- &writeAtResp{}
				latestChunk = chunk
			}

			chunksBeingWritten++

			writeChunk := &toWriteChunk{
				buf:    buf,
				offset: chunk.offset,
			}
			toWrite <- writeChunk
		case chunk := <-doneWriting:
			chunksBeingWritten--
			s.bufferPool.Put(chunk.buf)
			if latestChunk != nil && chunksBeingWritten == 0 {
				// finished writing the latest chunk, unblock the write on it
				latestChunk.response <- &writeAtResp{}
				latestChunk = nil
			}
		}
	}
}

// Close must be called to cleanup resources and finish flushing data to writer
func (s *WriterAtStream) Close() {
	close(s.closed)
	<-s.done
}

// WriteAt implements the io.WriterAt interface
func (s *WriterAtStream) WriteAt(p []byte, off int64) (n int, err error) {
	if off%int64(s.bufferSize) != 0 {
		// this this writerAtStream expects to only received blocks of exactly the buffersize
		// due to optimizations in the implementation.
		return 0, fmt.Errorf("unexpected offset %q not multiple of %q", off, s.bufferSize)
	}

	for {
		if s.err != nil {
			return 0, s.err
		}

		s.offsetLock.RLock()
		lastWrittenOffset := s.lastWrittenOffset
		s.offsetLock.RUnlock()

		// if we are attempting to write too far ahead in the buffer, then block and try again in a bit.
		// this will put backpressure on threads that are too far ahead for our "streaming buffer".
		allowedOffsetRange := s.bufferSize * s.bufferSize
		if off > lastWrittenOffset+int64(allowedOffsetRange) {
			time.Sleep(time.Nanosecond * 10)
			continue
		}
		break
	}

	// we're good to try and write

	response := make(chan *writeAtResp)
	defer close(response)

	chunk := &byteChunk{
		bytes:    p,
		offset:   off,
		response: response,
	}
	s.newChunks <- chunk

	// this response will block until either this write is completed or another write comes in after this one
	r := <-response
	return len(p), r.err
}
