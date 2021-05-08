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

// S3WriterAtStream is meant to convert an io.WriterAt to an io.Writer using an in memory buffer.
// It is an optimized version of WriterAtStream meant specifically for working s3 downloader and
// makes several assumptions specific to that usage:
// - "numBuffers" is greater than or equal to the number of concurrent workers in the downloader
// - all chunks written will be of "bufferSize" except the last chunk which can be smaller
// - all chunks will be written with offset multiples of "bufferSize"
// - the chunks are fed to the workers in ascending order
// - the same chunks/offsets are not attempted to be written more than once
type S3WriterAtStream struct {
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

// OpenS3WriterAtStream opens a S3WriterAtStream. The stream should be closed by the caller to clean up
// resources. If an error occurs, all subsequent WriteAts will begin to error out with the same error.
func OpenS3WriterAtStream(writer io.Writer, bufferPool BufPool, numBuffers, bufferSize int) *S3WriterAtStream {
	stream := &S3WriterAtStream{
		bufferPool:        bufferPool,
		writer:            writer,
		newChunks:         make(chan *byteChunk, numBuffers),
		closed:            make(chan int),
		done:              make(chan int),
		bufferSize:        bufferSize,
		numBuffers:        numBuffers,
		offsetLock:        new(sync.RWMutex),
		lastWrittenOffset: -int64(bufferSize), // initialize to negative of one buffer size
	}
	go stream.start()
	return stream
}

type toWriteChunk struct {
	buf    *bytes.Buffer
	offset int64
}

type wroteChunk struct {
	err error
}

// see if this chunk is the next one in line that needs to be written, and if so write it.
func (s *S3WriterAtStream) writeChunkIfNeeded(doneWriting chan *wroteChunk, chunk *toWriteChunk) (successfullyWrote bool) {
	// if this is the next block that we are supposed to write, then go ahead and write it
	if s.lastWrittenOffset == chunk.offset-int64(s.bufferSize) {
		if _, err := io.Copy(s.writer, chunk.buf); err != nil {
			// recycle buffer
			s.bufferPool.Put(chunk.buf)
			wrote := &wroteChunk{
				err: err,
			}
			doneWriting <- wrote
			return false
		}

		// update the last written offset
		s.offsetLock.Lock()
		s.lastWrittenOffset = chunk.offset
		s.offsetLock.Unlock()

		// recycle buffer
		s.bufferPool.Put(chunk.buf)
		// notify done writing without errors
		wrote := &wroteChunk{}
		doneWriting <- wrote
		return true
	}
	return false
}

func (s *S3WriterAtStream) start() {
	toWrite := make(chan *toWriteChunk, s.numBuffers)
	doneWriting := make(chan *wroteChunk, s.numBuffers)

	var pendingWrites []*toWriteChunk

	// spin off a writer worker thread
	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()

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
					continue
				}

				// if we successfully wrote, then lets try the pending chunks
				removedOffsets := map[int64]bool{}
				for _, c := range pendingWrites {
					successfullyWrote := s.writeChunkIfNeeded(doneWriting, c)
					if !successfullyWrote {
						break
					}
					removedOffsets[c.offset] = true
				}

				// remove the pending chunks that we successfully processed
				if len(removedOffsets) > 0 {
					newPendingWrites := make([]*toWriteChunk, 0, len(pendingWrites)-len(removedOffsets))
					for _, c := range pendingWrites {
						if removedOffsets[c.offset] {
							continue
						}
						newPendingWrites = append(newPendingWrites, c)
					}
					pendingWrites = newPendingWrites
				}
			}
		}
	}()

	var latestChunk *byteChunk
	var chunksBeingWritten int

	seenOffsets := map[int64]bool{}

	for {
		select {
		case <-s.closed:
			closedErr := errors.New("WriterAtStream Closed")
			// set the error in case any more writes try to come in
			close(toWrite)

			// wait for the writer to complete
			wg.Wait()

			close(doneWriting)
			close(s.newChunks)

			// clear out channels

			for chunk := range s.newChunks {
				chunk.response <- &writeAtResp{
					err: closedErr,
				}
			}

			for range doneWriting {
			}

			// recycle any outstanding buffers in pendingWrites
			for _, chunk := range pendingWrites {
				s.bufferPool.Put(chunk.buf)
			}
			pendingWrites = nil

			close(s.done)
			return
		case chunk := <-s.newChunks:
			// make sure we don't receive the same offset twice
			if seenOffsets[chunk.offset] {
				s.err = fmt.Errorf("unexpectedly received same offset twice, %d", chunk.offset)
				chunk.response <- &writeAtResp{s.err}
				continue
			}
			seenOffsets[chunk.offset] = true

			// get a recycled buffer
			buf := s.bufferPool.Get()
			// copy to internal buffer since once the WriteAt completes to the caller, they will likely
			// expect to be able to re-use their own buffer.
			buf.Write(chunk.bytes)

			// since we have a new chunk, we can unblock the previous latest one. this is mainly
			// just to prevent all of the WriteAts from continuing before the last write has occurred
			// similar to the behavior if this was a normal file or buffer being written to.
			if latestChunk != nil {
				latestChunk.response <- &writeAtResp{s.err}
			}
			latestChunk = chunk

			chunksBeingWritten++

			writeChunk := &toWriteChunk{
				buf:    buf,
				offset: chunk.offset,
			}
			toWrite <- writeChunk
		case chunk := <-doneWriting:
			chunksBeingWritten--

			if latestChunk != nil && chunksBeingWritten == 0 {
				// finished writing the latest chunk, unblock the write on it and notify of any
				// errors that might have occurred.
				latestChunk.response <- &writeAtResp{chunk.err}
				latestChunk = nil
			}
			if s.err == nil && chunk.err != nil {
				// set the "global" error in case any more writes try to come in so we can stop processing them
				s.err = chunk.err
			}
		}
	}
}

// Close must be called to cleanup resources and finish flushing data to writer
func (s *S3WriterAtStream) Close() {
	close(s.closed)
	<-s.done
}

// WriteAt implements the io.WriterAt interface
func (s *S3WriterAtStream) WriteAt(p []byte, off int64) (n int, err error) {
	if off%int64(s.bufferSize) != 0 {
		// this s3writerAtStream expects to only received blocks of exactly the buffersize
		// due to optimizations in the implementation.
		return 0, fmt.Errorf("unexpected offset %d not multiple of %d", off, s.bufferSize)
	}
	if len(p) > s.bufferSize {
		// this s3writerAtStream expects to only received blocks of exactly the buffersize
		// due to optimizations in the implementation. The last block may be smaller than the buffer size,
		// but none should be larger.
		return 0, fmt.Errorf("size of buffer being written %d is greater than bufferSize %d", len(p), s.bufferSize)
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
		allowedOffsetRange := s.numBuffers * s.bufferSize
		if off > lastWrittenOffset+int64(allowedOffsetRange) {
			time.Sleep(time.Microsecond * 10)
			continue
		}
		break
	}

	// we've verified that this chunk is as is expected and within the range we can buffer, continue to write
	response := make(chan *writeAtResp)
	defer close(response)

	chunk := &byteChunk{
		bytes:    p,
		offset:   off,
		response: response,
	}
	s.newChunks <- chunk

	// this response will block until either this write is completed or another write comes in after this one.
	r := <-response
	if r.err != nil {
		return 0, r.err
	}
	return len(p), nil
}
