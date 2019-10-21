package iostream

import (
	"bytes"
	"container/ring"
	"errors"
	"sync"
)

type StreamBuffer struct {
	sync.Mutex
	bufferSize    int
	offset        int64
	flushedOffset int64
	buffers       *ring.Ring
	// map from starting offset through ending offset
	writtenChunks map[int64]int64
}

// ErrBeforeBounds represents an attempt to WriteAt before the start of the buffer
var ErrBeforeBounds = errors.New("WriteAt attempt is before start of buffer")

// ErrAfterBounds represents an attempt to WriteAt beyond the current bounds of the buffer.
var ErrAfterBounds = errors.New("WriteAt attempt is after end of buffer")

// NewStreamBuffer creates a new StreamBuffer that has bufferCount internal
// rotating buffers of size bufferSize.
func NewStreamBuffer(bufferCount int, bufferSize int) *StreamBuffer {
	// Initialize all of the buffers at once.
	// We could eventually do this more dynamically and only initialiize new ones as needed
	// to optimize for smaller streams.
	buffers := ring.New(bufferCount)
	for i := 0; i < bufferSize; i++ {
		buffers.Value = bytes.NewBuffer(make([]byte, bufferSize))
		buffers = buffers.Next()
	}

	return &StreamBuffer{
		bufferSize:    bufferSize,
		buffers:       buffers,
		writtenChunks: map[int64]int64{},
	}
}

// WriteAt implements io.WriterAt interface
func (b *StreamBuffer) WriteAt(p []byte, off int64) (n int, err error) {
	if len(p) == 0 {
		return 0, nil
	}

	b.Lock()
	defer b.Unlock()

	if off < b.offset {
		return 0, ErrBeforeBounds
	}
	if off+int64(len(p)) > b.offset+int64(b.buffers.Len())*int64(b.bufferSize) {
		return 0, ErrAfterBounds
	}
	normalizedOffset := off - b.offset
	written := 0

	buffers := b.buffers
	for i := 0; i < b.buffers.Len(); i++ {
		// if the write is beyond the current buffer then continue
		if normalizedOffset+int64(written) > int64((i+1)*b.bufferSize) {
			buffers = buffers.Next()
			continue
		}
		// copy the portion of p bytes that fit in current buffer
		buf := buffers.Value.(*bytes.Buffer)
		data := buf.Bytes()

		bufferStart := i * b.bufferSize
		bufferEnd := (i + 1) * b.bufferSize

		copyStart := normalizedOffset + int64(written-bufferStart)
		copyEnd := copyStart + int64(len(p)-written)
		if copyEnd > int64(bufferEnd) {
			copyEnd = int64(bufferEnd - bufferStart)
		}
		copy(data[copyStart:copyEnd], p[written:copyEnd-copyStart+int64(written)])

		written += int(copyEnd - copyStart)

		if written >= len(p) {
			break
		}

		buffers = buffers.Next()
	}

	// update state for written chunks
	b.writtenChunks[off] = off + int64(len(p))

	return written, nil
}

// Flush will flush all bytes that are fully written from the start of the internal
// buffer. These bytes will not be returned again.
func (b *StreamBuffer) Flush() []byte {
	b.Lock()
	defer b.Unlock()

	normalizedFlushedOffset := b.flushedOffset - b.offset

	toWrite := 0

	for {
		newOffset, ok := b.writtenChunks[b.flushedOffset]
		if !ok {
			break
		}
		toWrite += int(newOffset - b.flushedOffset)
		delete(b.writtenChunks, b.flushedOffset)
		b.flushedOffset = newOffset
	}

	if toWrite == 0 {
		return nil
	}

	out := make([]byte, toWrite)

	written := 0

	for i := 0; i < b.buffers.Len(); i++ {
		buf := b.buffers.Value.(*bytes.Buffer)
		data := buf.Bytes()

		bufferStart := i * b.bufferSize
		bufferEnd := (i + 1) * b.bufferSize

		bufferCopyOffset := normalizedFlushedOffset + int64(written-bufferStart)

		toWriteFromBuffer := toWrite - written
		if toWrite > bufferEnd-int(bufferCopyOffset) {
			toWriteFromBuffer = bufferEnd - int(bufferCopyOffset)
		}

		copy(out[written:written+toWriteFromBuffer], data[bufferCopyOffset:int(bufferCopyOffset)+toWriteFromBuffer])

		written += toWriteFromBuffer

		// reset data in this flushed buffer and advance
		if b.flushedOffset >= b.offset+int64(b.bufferSize) {
			buf.Reset()
			b.buffers = b.buffers.Next()
			b.offset += int64(b.bufferSize)
		}

		if written >= toWrite {
			break
		}
	}

	return out
}
