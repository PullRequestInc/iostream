package iostream_test

import (
	"testing"

	"github.com/PullRequestInc/iostream"
	"github.com/stretchr/testify/assert"
)

func TestBufPool(t *testing.T) {
	pool := iostream.NewBufPool(100)
	// get a couple of buffers to test
	buf := pool.Get()
	assert.Equal(t, 100, buf.Cap())
	assert.Equal(t, 0, buf.Len())
	buf = pool.Get()
	assert.Equal(t, 100, buf.Cap())
	assert.Equal(t, 0, buf.Len())

	_, err := buf.WriteString("abc")
	assert.NoError(t, err)
	assert.Equal(t, "abc", buf.String())
	assert.Equal(t, 100, buf.Cap())
	assert.Equal(t, 3, buf.Len())

	pool.Put(buf)
	assert.Equal(t, 100, buf.Cap())
	assert.Equal(t, 0, buf.Len())

	buf = pool.Get()
	assert.Equal(t, 100, buf.Cap())
	assert.Equal(t, 0, buf.Len())
}
