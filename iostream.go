package iostream

import (
	"errors"
	"io"
)

// WriterAtStream is meant to convert an io.WriterAt to an io.Writer using an in memory buffer.
// It will use mutliple buffers, but prevent letting the entire file being written in memory.
// It should only be used if there is a guarantee that the next chunk of bytes that the writer
// needs will eventually be passed if WriteAt is blocking for other writes further in the stream.
// The io.WriterAt should also only attempt to write a chunk of bytes once, as once a chunk has
// been written, it is assumed that the buffer's offset can advance.
type WriterAtStream struct {
	buffer    *StreamBuffer
	writer    io.Writer
	newChunks chan *byteChunk
	closed    chan int // signalled when Close is called
	done      chan int // signalled after close has been completed
}

// OpenWriterAtStream opened a WriterAtStream. The stream should be closed by the caller to clean up
// resources. If an error occurs, all subsequent WriteAts will begin to error out.
func OpenWriterAtStream(writer io.Writer, bufferSize, numBuffers int) *WriterAtStream {
	stream := &WriterAtStream{
		buffer:    NewStreamBuffer(numBuffers, bufferSize),
		writer:    writer,
		newChunks: make(chan *byteChunk),
		closed:    make(chan int),
		done:      make(chan int),
	}
	go stream.start()
	return stream
}

type byteChunk struct {
	bytes    []byte
	offset   int64
	response chan *writeAtResp
}

type writeAtResp struct {
	err error
}

func (s *WriterAtStream) start() {
	// backedUp holds byteChunks that are beyond our buffer and are currently being blocked
	// until the buffer grows to accomodate them as a backpressure mechanism.
	// TODO: Use linked list?
	var backedUp []*byteChunk

	for {
		select {
		case <-s.closed:
			closedErr := errors.New("WriterAtStream Closed")

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
			var err error
			_, err = s.buffer.WriteAt(chunk.bytes, chunk.offset)
			if err == ErrAfterBounds {
				// this chunk is too far ahead, we need to block it being written for now
				backedUp = append(backedUp, chunk)
				continue
			}

			for {
				toWrite := s.buffer.Flush()
				if len(toWrite) == 0 {
					break
				}
				if _, err = s.writer.Write(toWrite); err != nil {
					break
				}
				var newBackedUp []*byteChunk
				for _, chunk := range backedUp {
					if _, err = s.buffer.WriteAt(chunk.bytes, chunk.offset); err != nil {
						if err == ErrAfterBounds {
							newBackedUp = append(newBackedUp, chunk)
							err = nil
						} else {
							break
						}
					}
				}
				backedUp = newBackedUp
			}

			chunk.response <- &writeAtResp{err}
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
	response := make(chan *writeAtResp)
	defer close(response)

	chunk := &byteChunk{
		bytes:    p,
		offset:   off,
		response: response,
	}
	s.newChunks <- chunk

	r := <-response
	return len(p), r.err
}
