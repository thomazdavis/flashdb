package sstable

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/thomazdavis/stratago/memtable"
)

func TestReader_Get(t *testing.T) {
	filename := "test_read.sst"
	defer os.Remove(filename)

	list := memtable.NewSkipList()
	list.Put([]byte("Td:1"), []byte("Camp Zoo"))
	list.Put([]byte("J:5"), []byte("M&T Bank Stadium"))

	builder, _ := NewBuilder(filename)
	builder.Flush(list)

	reader, err := NewReader(filename)
	assert.NoError(t, err)
	defer reader.Close()

	val, found := reader.Get([]byte("Td:1"))
	assert.True(t, found)
	assert.Equal(t, []byte("Camp Zoo"), val)

	val, found = reader.Get([]byte("J:5"))
	assert.True(t, found)
	assert.Equal(t, []byte("M&T Bank Stadium"), val)

	_, found = reader.Get([]byte("ATM:99"))
	assert.False(t, found)
}
