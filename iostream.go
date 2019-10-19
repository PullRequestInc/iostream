package iostream

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
)

// WriterAtStream is meant to convert an io.WriterAt to an io.Writer using an in memory buffer.
// It will use mutliple buffers, but prevent letting the entire file being written in memory.
// It should only be used if there is a guarantee that the next chunk of bytes that the writer
// needs will eventually be passed if WriteAt is blocking for other writes further in the stream.
// The io.WriterAt should also only attempt to a chunk of bytes once, as once a chunk has been
// written, it is assumed that the buffer's offset can advance.
type WriterAtStream interface {
	WriteAt(p []byte, off int64) (n int, err error)
	Close()
}

// OpenWriterAtStream opened a WriterAtStream. The stream should be closed by the caller to clean up
// resources. If an error occurs, all subsequent WriteAts will begin to error out.
func OpenWriterAtStream(ctx context.Context, writer io.Writer, bufferSize int64, numBuffers int) WriterAtStream {
	stream := &writerAtStream{
		writer:        writer,
		bufferSize:    bufferSize,
		numBuffers:    numBuffers,
		newChunks:     make(chan *byteChunk),
		closed:        make(chan int),
		done:          make(chan int),
		writtenChunks: map[int64]int{},
	}
	go stream.start(ctx)
	return stream
}

type byteChunk struct {
	bytes    []byte
	offset   int64
	response chan *writeAtResp
}

type writeAtResp struct {
	bytesWritten int
	err          error
}

type writerAtStream struct {
	writer     io.Writer
	bufferSize int64
	numBuffers int
	newChunks  chan *byteChunk
	closed     chan int // signalled when Close is called
	done       chan int // signalled after close has been completed

	offset         int64
	writtenThrough int64
	buffers        [][]byte
	// writtenChunks are keyed by their offset and the value is their length
	writtenChunks map[int64]int
}

func (s *writerAtStream) writeChunk(chunk *byteChunk) {
	normalizedOffset := int(chunk.offset - s.offset)
	chunkStartIdx := normalizedOffset % s.numBuffers
	chunkEndIdx := normalizedOffset + len(chunk.bytes)%s.numBuffers

	// grow buffers array if needed
	for chunkEndIdx >= len(s.buffers) {
		s.buffers = append(s.buffers, make([]byte, s.bufferSize))
	}

	startIdx := normalizedOffset - chunkStartIdx*int(s.bufferSize)
	if chunkStartIdx == chunkEndIdx {
		// the full chunk fits in one byte array so just copy it all at once
		copy(s.buffers[chunkStartIdx][startIdx:int(startIdx)+len(chunk.bytes)], chunk.bytes)
		return
	}
	// the full chunk is split across 2 or more byte arrays
	chunkIdx := chunkStartIdx
	offset := normalizedOffset
	copiedLen := 0
	for chunkIdx <= chunkEndIdx {
		endIdx := int(s.bufferSize)
		if len(chunk.bytes)-copiedLen < int(s.bufferSize) {
			endIdx = len(chunk.bytes) - copiedLen
		}
		copy(s.buffers[chunkStartIdx][startIdx:endIdx], chunk.bytes[copiedLen:endIdx-startIdx])
		copiedLen += endIdx - startIdx

		// set next start index to the start of the next chunk
		chunkIdx++
		startIdx = int(s.offset) + int(s.bufferSize)*chunkIdx
	}
}

func (s *writerAtStream) canWriteChunk(chunk *byteChunk) (bool, error) {
	if chunk.offset < s.writtenThrough {
		return false, errors.New("attempting to write previously written chunk again")
	}
	chunkEnd := chunk.offset + int64(len(chunk.bytes))
	bufferEnd := s.offset + s.bufferSize*int64(s.numBuffers)
	if chunkEnd > bufferEnd {
		return false, nil
	}
	return true, nil
}

// flush all bytes from the beginning of the buffer that are complete
func (s *writerAtStream) flush() error {
	toWriteThrough := 0
	bufferIdx := 0
	for bufferIdx < len(s.buffers) {
		startIdx := int(s.writtenThrough - s.offset)
		dataLen := 0
		for {
			chunkLen, ok := s.writtenChunks[s.writtenThrough]
			if !ok {
				break
			}
			delete(s.writtenChunks, s.writtenThrough)

			endIdx := startIdx + chunkLen
			if startIdx+chunkLen >= int(s.bufferSize) {
				endIdx := int(s.bufferSize)
				// to write through on the next buffer idx
				toWriteThrough = startIdx + chunkLen - endIdx
			}

			s.writtenThrough += int64(endIdx - startIdx)
		}

		if dataLen > 0 {
			log.Println("writing: ", string(buf[0:dataLen]))
			if _, err := s.writer.Write(buf[0:dataLen]); err != nil {
				return offset, nil, fmt.Errorf("failed flushing buffer: %v", err)
			}
			log.Println("done writing")
			newBuf := make([]byte, s.bufferSize)
			copy(newBuf[:len(buf)-dataLen], buf[dataLen:])
			return offset, newBuf, nil
		}
	}
	return offset, buf, nil
}

func (s *writerAtStream) start(ctx context.Context) {
	// backedUp holds byteChunks that are beyond our buffer and are currently being blocked
	// until the buffer grows to accomodate them as a backpressure mechanism.
	var backedUp []*byteChunk

	var writeError error

	cleanUp := func(err error) {
		writeError = err
		for _, chunk := range backedUp {
			chunk.response <- &writeAtResp{
				bytesWritten: 0,
				err:          err,
			}
		}
		backedUp = nil
	}

	// flush any backed up chunks that we have't been able to write yet
	flushBackedUp := func() {
		if len(backedUp) == 0 {
			return
		}
		oldBackedUp := backedUp
		backedUp = make([]*byteChunk, 0, len(backedUp))
		addedChunks := false
		for _, chunk := range oldBackedUp {
			canWrite, err := s.canWriteChunk(chunk)
			if err != nil {
				backedUp = oldBackedUp
				cleanUp(fmt.Errorf("canWriteChunk failed while trying to write backedUp chunks: %v", err))
				return
			}
			if !canWrite {
				backedUp = append(backedUp, chunk)
				continue
			}
			s.writeChunk(chunk)
			writtenChunks[chunk.offset] = len(chunk.bytes)
			addedChunks = true
			chunk.response <- &writeAtResp{
				bytesWritten: len(chunk.bytes),
			}
		}
		if addedChunks {
			if err := s.flush(); err != nil {
				backedUp = oldBackedUp
				cleanUp(fmt.Errorf("canWriteChunk failed while trying to write backedUp chunks: %v", err))
			}
		}
	}

	processChunk := func(chunk *byteChunk) {
		if writeError != nil {
			chunk.response <- &writeAtResp{
				bytesWritten: 0,
				err:          writeError,
			}
			return
		}
		canWrite, err := s.canWriteChunk(chunk)
		if err != nil {
			cleanUp(fmt.Errorf("canWriteChunk failed while trying to write new chunk: %v", err))
			return
		}
		if !canWrite {
			backedUp = append(backedUp, chunk)
			return
		}
		log.Println("processing chunk: ", string(chunk.bytes), chunk.offset)
		s.writeChunk(chunk)
		writtenChunks[chunk.offset] = len(chunk.bytes)
		chunk.response <- &writeAtResp{
			bytesWritten: len(chunk.bytes),
		}
		if err := s.flush(); err != nil {
			cleanUp(err)
		}
	}

	for {
		flushBackedUp()

		select {
		case <-ctx.Done():
			cleanUp(ctx.Err())
			continue
		case <-s.closed:
			// attempt to flush any remaining writes and then clean up
			var done bool
			for !done {
				select {
				case chunk := <-s.newChunks:
					processChunk(chunk)
				default:
					done = true
				}
			}
			flushBackedUp()
			cleanUp(errors.New("writerAtStream closed"))
			close(s.newChunks)
			close(s.done)
			return
		case chunk := <-s.newChunks:
			processChunk(chunk)
		}
	}
}

func (s *writerAtStream) Close() {
	close(s.closed)
	<-s.done
}

func (s *writerAtStream) WriteAt(p []byte, off int64) (n int, err error) {
	response := make(chan *writeAtResp)
	defer close(response)

	chunk := &byteChunk{
		bytes:    p,
		offset:   off,
		response: response,
	}
	s.newChunks <- chunk

	log.Println("waiting on response")
	r := <-response
	log.Println("got a response")
	return r.bytesWritten, r.err
}
