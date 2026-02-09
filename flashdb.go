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

	// Crash protection
	flushingPath := filepath.Join(dataDir, "wal.log.flushing")
	if _, err := os.Stat(flushingPath); err == nil {
		// Found an abandoned log, recovering it into the memory
		tempWAL, err := wal.NewWAL(flushingPath)
		if err == nil {
			restored, _ := tempWAL.Recover()
			for k, v := range restored {
				mem.Put([]byte(k), v)
			}
			tempWAL.Close()
			os.Remove(flushingPath)
		}
	}

	walPath := filepath.Join(dataDir, "wal.log")
	walLog, err := wal.NewWAL(walPath)
	if err != nil {
		return nil, err
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
		return val, true
	}

	if db.immutableMemtable != nil {
		if val, found := db.immutableMemtable.Get(key); found {
			return val, true
		}
	}

	for i := len(db.sstReaders) - 1; i >= 0; i-- {
		if val, found := db.sstReaders[i].Get(key); found {
			return val, true
		}
	}
	return nil, false
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

func (db *StrataGo) Flush() error {
	db.mu.Lock()

	// Concurrency Check
	if db.isFlushing || db.activeMemtable.Size == 0 {
		db.mu.Unlock()
		return nil
	}
	db.isFlushing = true

	// Rotate Memtable
	db.immutableMemtable = db.activeMemtable
	db.activeMemtable = memtable.NewSkipList()

	// Rotate WAL
	oldWAL := db.wal
	if err := oldWAL.Close(); err != nil {
		db.isFlushing = false
		db.mu.Unlock()
		return err
	}

	flushingWALPath := filepath.Join(db.dataDir, "wal.log.flushing")
	if err := os.Rename(filepath.Join(db.dataDir, "wal.log"), flushingWALPath); err != nil {
		db.isFlushing = false
		db.mu.Unlock()
		return err
	}

	newWal, err := wal.NewWAL(filepath.Join(db.dataDir, "wal.log"))
	if err != nil {
		db.isFlushing = false
		db.mu.Unlock()
		return err
	}
	db.wal = newWal

	sstID := len(db.sstReaders) + 1
	db.mu.Unlock()

	sstName := fmt.Sprintf("data_%d.sst", sstID)
	sstPath := filepath.Join(db.dataDir, sstName)

	handleError := func(err error) error {
		db.mu.Lock()
		db.isFlushing = false
		db.mu.Unlock()
		return err
	}

	builder, err := sstable.NewBuilder(sstPath)
	if err != nil {
		return handleError(err)
	}

	if err := builder.Flush(db.immutableMemtable); err != nil {
		return handleError(err)
	}

	reader, err := sstable.NewReader(sstPath)
	if err != nil {
		return handleError(err)
	}

	db.mu.Lock()
	db.sstReaders = append(db.sstReaders, reader)
	db.immutableMemtable = nil
	db.isFlushing = false
	db.mu.Unlock()

	os.Remove(flushingWALPath)

	return nil
}
