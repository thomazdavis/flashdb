package sstable

import (
	"bytes"
	"encoding/binary"
	"io"
	"os"
)

type Reader struct {
	file *os.File
}

// Opens an existing SSTable for reading
func NewReader(filename string) (*Reader, error) {
	file, err := os.Open(filename)
	if err != nil {
		return nil, err
	}
	return &Reader{file: file}, nil
}

func (r *Reader) Close() error {
	return r.file.Close()
}

// Get searches for a key in the SSTable
func (r *Reader) Get(searchKey []byte) ([]byte, bool) {
	_, err := r.file.Seek(0, 0)
	if err != nil {
		return nil, false
	}

	for {
		// Read key size (4 bytes)
		var keySize uint32
		err := binary.Read(r.file, binary.LittleEndian, &keySize)
		if err == io.EOF {
			break // Key not found
		}
		if err != nil {
			return nil, false // file corrupted
		}

		var valSize uint32
		err = binary.Read(r.file, binary.LittleEndian, &valSize)
		if err != nil {
			return nil, false
		}

		// Read Key Payload
		key := make([]byte, keySize)
		_, err = r.file.Read(key)
		if err != nil {
			return nil, false
		}

		if bytes.Equal(key, searchKey) {
			val := make([]byte, valSize)
			_, err = r.file.Read(val)
			return val, true
		} else {
			_, err = r.file.Seek(int64(valSize), 1)
			if err != nil {
				return nil, false
			}
		}
	}
	return nil, false
}
