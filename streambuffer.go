package iostream

import (
	"bytes"
	"container/ring"
	"errors"
	"sync"
)

// StreamBuffer represents a buffer for streaming data that consolidates random
// accesses into sequential accesses using a moving buffer. This buffer
// contains a series of pre-allocated buffers. Each time the user writes a
// chunk of data via the WriteAt function, it figures out where in the
// pre-allocated buffers that chunk of data should go. When the user wants to
// flush data, we'll return the largest contiguous chunk and advance to the
// next non-flushed buffer.
type StreamBuffer struct {
	sync.Mutex

	// bufferSize represents the size of each individual buffer in our buffers
	// ring buffer
	bufferSize int

	// buffers is the ring buffer holding all of our individual buffers
	buffers *ring.Ring

	// offset is the current absolute offset in the streaming operation of the
	// first buffer in the ring buffer
	offset int64

	// flushedOffset is the current absolute offset in the streaming operation
	// of the first chunk of data that hasn't yet been flushed. This value can
	// straddle buffers.
	flushedOffset int64

	// writtenChunks is a map of absolute write offsets to the absolute offset
	// of the next available byte after the write (offset + len(writeBuf))
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
	// We could eventually do this more dynamically and only initialize new ones as needed
	// to optimize for smaller streams.
	buffers := ring.New(bufferCount)
	for i := 0; i < bufferCount; i++ {
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

	// Check to see if the current write can fit into our current buffer window
	// denoted by [b.offset, b.offset + buffer length * buffer size)
	if off < b.offset {
		return 0, ErrBeforeBounds
	}
	if off+int64(len(p)) > b.offset+int64(b.buffers.Len())*int64(b.bufferSize) {
		return 0, ErrAfterBounds
	}

	// normalizedOffset is the offset of the write relative to the first buffer
	// in our ring of buffers
	normalizedOffset := off - b.offset

	// written is how much data we have written so far
	var written int

	buffers := b.buffers
	for i := 0; i < b.buffers.Len(); i++ {
		// if the write is beyond the current buffer, then continue
		if normalizedOffset+int64(written) > int64((i+1)*b.bufferSize) {
			buffers = buffers.Next()
			continue
		}

		// copy the portion of p bytes that fit in current buffer
		buf := buffers.Value.(*bytes.Buffer)
		data := buf.Bytes()

		// bufferStart is the normalized offset of the buf above relative to
		// the current offset of the overall buffer
		bufferStart := i * b.bufferSize

		// copyStart and copyEnd are the destination boundaries of the copy
		// operation into the current buffer like [copyStart, copyEnd)
		copyStart := int(normalizedOffset + int64(written) - int64(bufferStart))
		copyEnd := copyStart + len(p) - written

		// Don't copy more than the size of the current buffer. If we're going
		// to go beyond, limit it. We'll be sure to write to the next buffer
		// next.
		if copyEnd > b.bufferSize {
			copyEnd = b.bufferSize
		}

		// Actually perform the copy operation
		copyLength := copyEnd - copyStart
		copy(data[copyStart:copyEnd], p[written:copyLength+written])

		// If we've finished writing, go ahead and break out. If not, advance
		// to the next write that we're to perform.
		written += copyLength
		if written >= len(p) {
			break
		}
		buffers = buffers.Next()
	}

	// Update the state for written chunks. We'll use this mapping later to
	// figure out the largest contiguous chunk when we flush the data.
	// FIXME: Subsequent writes to this offset of a different length will break
	// this mechanism. This mechanism is only for mutually exclusive chunks of
	// data.
	b.writtenChunks[off] = off + int64(len(p))
	return written, nil
}

// advanceWritable should be called with the lock already taken. it will return the number of bytes
// that are available to write while also advancing the stream buffer's flushed offset
func (b *StreamBuffer) advanceWritable() int {
	toWrite := 0

	// Trace through our available sequential writes to figure out how much
	// data can be flushed to the writer. Each written chunk should map to the
	// potential next chunk that has been written.
	for {
		newOffset, ok := b.writtenChunks[b.flushedOffset]
		if !ok {
			break
		}
		toWrite += int(newOffset - b.flushedOffset)
		delete(b.writtenChunks, b.flushedOffset)
		b.flushedOffset = newOffset
	}

	return toWrite
}

// Flush will flush all bytes that are fully written from the start of the internal
// buffer. These bytes will not be returned again.
func (b *StreamBuffer) Flush() []byte {
	b.Lock()
	defer b.Unlock()

	// normalizedFlushOffset is the difference between the offset of the
	// non-flushed data and the current offset of our overarching buffer
	normalizedFlushedOffset := b.flushedOffset - b.offset

	// If there's no data to write, then we don't need to proceed.
	toWrite := b.advanceWritable()
	if toWrite == 0 {
		return nil
	}

	out := make([]byte, toWrite)
	var written int

	for i := 0; i < b.buffers.Len(); i++ {
		buf := b.buffers.Value.(*bytes.Buffer)
		data := buf.Bytes()

		// bufferStart is the normalized offset of the buf above relative to
		// the offset of the overall buffer when we entered this function.
		bufferStart := i * b.bufferSize

		// copyStart and copyEnd are the source boundaries of the copy
		// operation from the current buffer like [copyStart, copyEnd)
		copyStart := int(normalizedFlushedOffset + int64(written) - int64(bufferStart))
		copyEnd := int(copyStart + toWrite - written)

		// Don't copy more than the size of the current buffer. If we're going
		// to go beyond, limit it. We'll be sure to write to the next buffer
		// next.
		if copyEnd > b.bufferSize {
			copyEnd = b.bufferSize
		}

		copyLength := copyEnd - copyStart
		copy(out[written:copyLength+written], data[copyStart:copyEnd])

		// This buffer is fully flushed, so reset its data and advance the
		// overarching buffer to the next one.
		if b.flushedOffset >= b.offset+int64(b.bufferSize) {
			buf.Reset()
			b.buffers = b.buffers.Next()
			b.offset += int64(b.bufferSize)
		}

		// Advance to the next chunk if more data is available to be written or
		// quit out if not.
		written += copyLength
		if written >= toWrite {
			break
		}
	}

	return out
}
