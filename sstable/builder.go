package sstable

import (
	"encoding/binary"
	"fmt"
	"os"
	"time"

	"github.com/thomazdavis/stratago/memtable"
)

type Builder struct {
	file          *os.File
	tmpFilename   string
	finalFilename string
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
	}, nil
}

// Flush writes the entire Skiplist to the SSTable file
func (b *Builder) Flush(skiplist *memtable.SkipList) error {

	iter := skiplist.NewIterator()

	// Iterate through every node
	for iter.Next() {
		key := iter.Key()
		val := iter.Value()

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

// cleanup removes the temporary file if something goes wrong
func (b *Builder) cleanup() {
	b.file.Close()
	os.Remove(b.tmpFilename)
}
