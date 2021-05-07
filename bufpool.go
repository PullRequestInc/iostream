package iostream

import (
	"bytes"
	"sync"
)

// BufPool is a sync.Pool with bytes.Buffer typings
type BufPool interface {
	Get() *bytes.Buffer
	Put(*bytes.Buffer)
}

type bufPool struct {
	pool *sync.Pool
}

// NewBufPool creates a new BufPool
func NewBufPool(bytesPerBuffer int) BufPool {
	pool := &sync.Pool{
		New: func() interface{} {
			buf := make([]byte, bytesPerBuffer)
			return bytes.NewBuffer(buf)
		},
	}
	return &bufPool{
		pool: pool,
	}
}

func (p *bufPool) Get() *bytes.Buffer {
	return p.pool.Get().(*bytes.Buffer)
}

func (p *bufPool) Put(buf *bytes.Buffer) {
	buf.Reset()
	p.pool.Put(buf)
}
