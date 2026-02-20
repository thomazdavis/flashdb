package sstable

import (
	"encoding/binary"
	"fmt"
	"os"
	"time"

	"github.com/thomazdavis/stratago/memtable"
)

const IndexInterval = 1024

type IndexEntry struct {
	Key    []byte
	Offset int64
}

type Builder struct {
	file          *os.File
	tmpFilename   string
	finalFilename string
	index         []IndexEntry
	bytesWritten  int64
	lastIndexPos  int64
}

func NewBuilder(filename string) (*Builder, error) {
	tmpFilename := fmt.Sprintf("%s.tmp.%d", filename, time.Now().UnixNano())

	file, err := os.Create(tmpFilename)
	if err != nil {
		return nil, err
	}
	return &Builder{
		file:          file,
		tmpFilename:   tmpFilename,
		finalFilename: filename,
		index:         make([]IndexEntry, 0),
	}, nil
}

// Flush writes the entire Skiplist to the SSTable file
func (b *Builder) Flush(skiplist *memtable.SkipList) error {

	iter := skiplist.NewIterator()

	// Iterate through every node
	for iter.Next() {
		key := iter.Key()
		val := iter.Value()
		startOffset := b.bytesWritten

		if startOffset == 0 || startOffset-b.lastIndexPos >= IndexInterval {
			keyCopy := make([]byte, len(key))
			copy(keyCopy, key)

			b.index = append(b.index, IndexEntry{
				Key:    keyCopy,
				Offset: startOffset,
			})
			b.lastIndexPos = startOffset
		}

		// Write Key Size (4 bytes)
		if err := binary.Write(b.file, binary.LittleEndian, uint32(len(key))); err != nil {
			b.cleanup()
			return err
		}

		// Write Value Size (4 bytes)
		if err := binary.Write(b.file, binary.LittleEndian, uint32(len(val))); err != nil {
			b.cleanup()
			return err
		}

		// Write Key Bytes
		if _, err := b.file.Write(key); err != nil {
			b.cleanup()
			return err
		}

		if _, err := b.file.Write(val); err != nil {
			b.cleanup()
			return err
		}

		b.bytesWritten += int64(8 + len(key) + len(val))
	}

	// Index block (sparse index)
	indexOffset := b.bytesWritten
	if err := b.writeIndex(); err != nil {
		b.cleanup()
	}

	// Footer (8 bytes containing the offset of the Index)
	if err := binary.Write(b.file, binary.LittleEndian, uint64(indexOffset)); err != nil {
		b.cleanup()
		return err
	}

	if err := b.file.Sync(); err != nil {
		b.cleanup()
		return err
	}

	if err := b.file.Close(); err != nil {
		b.cleanup()
		return err
	}

	if err := os.Rename(b.tmpFilename, b.finalFilename); err != nil {
		b.cleanup()
		return err
	}

	return nil
}

func (b *Builder) writeIndex() error {
	if err := binary.Write(b.file, binary.LittleEndian, uint32(len(b.index))); err != nil {
		return err
	}

	for _, entry := range b.index {
		if err := binary.Write(b.file, binary.LittleEndian, uint32(len(entry.Key))); err != nil {
			return err
		}
		if _, err := b.file.Write(entry.Key); err != nil {
			return err
		}
		if err := binary.Write(b.file, binary.LittleEndian, int64(entry.Offset)); err != nil {
			return err
		}
	}
	return nil
}

// cleanup removes the temporary file if something goes wrong
func (b *Builder) cleanup() {
	b.file.Close()
	os.Remove(b.tmpFilename)
}
