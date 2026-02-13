package stratago

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/thomazdavis/stratago/memtable"
	"github.com/thomazdavis/stratago/sstable"
	"github.com/thomazdavis/stratago/wal"
)

const DefaultMemtableThreshold = 4 * 1024 * 1024 // 4MB

type StrataGo struct {
	mu                sync.RWMutex
	activeMemtable    *memtable.SkipList
	immutableMemtable *memtable.SkipList
	wal               *wal.WAL
	sstReaders        []*sstable.Reader
	dataDir           string
	isFlushing        bool
}

func Open(dataDir string) (*StrataGo, error) {
	if err := os.MkdirAll(dataDir, 0755); err != nil {
		return nil, err
	}

	mem := memtable.NewSkipList()
	walPath := filepath.Join(dataDir, "wal.log")
	walLog, err := wal.NewWAL(walPath)
	if err != nil {
		return nil, err
	}

	// Crash protection
	flushingPath := filepath.Join(dataDir, "wal.log.flushing")
	if _, err := os.Stat(flushingPath); err == nil {
		// Found an abandoned log, recovering it into the memory
		tempWAL, err := wal.NewWAL(flushingPath)
		if err == nil {
			restored, _ := tempWAL.Recover()
			for k, v := range restored {
				mem.Put([]byte(k), v)

				if err := walLog.WriteEntry([]byte(k), v); err != nil {
					return nil, fmt.Errorf("failed to persist recovered data: %w", err)
				}
			}
			tempWAL.Close()
			os.Remove(flushingPath)
		}
	}

	restored, err := walLog.Recover()
	if err != nil {
		return nil, fmt.Errorf("WAL recovery failed: %w", err)
	}

	for k, v := range restored {
		mem.Put([]byte(k), v)
	}

	files, err := os.ReadDir(dataDir)
	if err != nil {
		return nil, fmt.Errorf("failed to read data directory: %w", err)
	}
	var readers []*sstable.Reader
	for _, f := range files {
		if strings.HasSuffix(f.Name(), ".sst") {
			r, err := sstable.NewReader(filepath.Join(dataDir, f.Name()))
			if err == nil {
				readers = append(readers, r)
			}
		}
	}
	return &StrataGo{
		activeMemtable: mem,
		wal:            walLog,
		sstReaders:     readers,
		dataDir:        dataDir,
	}, nil
}

func (db *StrataGo) Put(key, value []byte) error {

	if err := db.wal.WriteEntry(key, value); err != nil {
		return err
	}

	db.mu.Lock()
	db.activeMemtable.Put(key, value)

	needsFlush := db.activeMemtable.SizeBytes >= DefaultMemtableThreshold

	if needsFlush && !db.isFlushing {
		go db.Flush()
	}
	db.mu.Unlock()

	return nil
}

func (db *StrataGo) Get(key []byte) ([]byte, bool) {
	db.mu.RLock()
	defer db.mu.RUnlock()

	if val, found := db.activeMemtable.Get(key); found {
		if val == nil {
			return nil, false
		}
		return val, true
	}

	if db.immutableMemtable != nil {
		if val, found := db.immutableMemtable.Get(key); found {
			if val == nil {
				return nil, false
			}
			return val, true
		}
	}

	for i := len(db.sstReaders) - 1; i >= 0; i-- {
		if val, found := db.sstReaders[i].Get(key); found {
			if len(val) == 0 {
				return nil, false
			}
			return val, true
		}
	}
	return nil, false
}

// Delete marks a key as deleted by inserting a tombstone
func (db *StrataGo) Delete(key []byte) error {

	// Writing the deletion to the WAL with value nil
	if err := db.wal.WriteEntry(key, nil); err != nil {
		return err
	}

	db.mu.Lock()
	db.activeMemtable.Put(key, nil)

	needsFlush := db.activeMemtable.SizeBytes >= DefaultMemtableThreshold
	if needsFlush && !db.isFlushing {
		go db.Flush()
	}
	db.mu.Unlock()

	return nil
}

func (db *StrataGo) Close() error {
	db.mu.Lock()
	defer db.mu.Unlock()

	db.wal.Close()
	for _, r := range db.sstReaders {
		r.Close()
	}
	return nil
}

// Purge closes the database, deletes all data files, and restarts the engine.
func (db *StrataGo) Purge() error {
	db.mu.Lock()
	defer db.mu.Unlock()

	// Close the WAL to release the file lock
	if err := db.wal.Close(); err != nil {
		return err
	}

	// Close all SSTable readers
	for _, reader := range db.sstReaders {
		if err := reader.Close(); err != nil {
			return err
		}
	}

	// Wipe the Data Directory
	if err := os.RemoveAll(db.dataDir); err != nil {
		return err
	}
	if err := os.MkdirAll(db.dataDir, 0755); err != nil {
		return err
	}

	// Re-initialize Memory and WAL
	db.activeMemtable = memtable.NewSkipList()
	db.immutableMemtable = nil
	db.sstReaders = nil // Reset readers slice

	walPath := filepath.Join(db.dataDir, "wal.log")
	newWal, err := wal.NewWAL(walPath)
	if err != nil {
		return err
	}
	db.wal = newWal

	// Reset flush state
	db.isFlushing = false

	return nil
}
