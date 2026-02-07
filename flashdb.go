package flashdb

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/thomazdavis/flashdb/memtable"
	"github.com/thomazdavis/flashdb/sstable"
	"github.com/thomazdavis/flashdb/wal"
)

type FlashDB struct {
	mu             sync.RWMutex
	activeMemtable *memtable.SkipList
	wal            *wal.WAL
	sstReaders     []*sstable.Reader
	dataDir        string
}

func Open(dataDir string) (*FlashDB, error) {
	if err := os.MkdirAll(dataDir, 0755); err != nil {
		return nil, err
	}

	walPath := filepath.Join(dataDir, "wal.log")
	walLog, err := wal.NewWAL(walPath)
	if err != nil {
		return nil, err
	}

	mem := memtable.NewSkipList()

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
	return &FlashDB{
		activeMemtable: mem,
		wal:            walLog,
		sstReaders:     readers,
		dataDir:        dataDir,
	}, nil
}

func (db *FlashDB) Put(key, value []byte) error {

	if err := db.wal.WriteEntry(key, value); err != nil {
		return err
	}

	db.mu.Lock()
	db.activeMemtable.Put(key, value)
	db.mu.Unlock()

	return nil
}

func (db *FlashDB) Get(key []byte) ([]byte, bool) {
	db.mu.RLock()
	defer db.mu.RUnlock()

	if val, found := db.activeMemtable.Get(key); found {
		return val, true
	}

	for i := len(db.sstReaders) - 1; i >= 0; i-- {
		if val, found := db.sstReaders[i].Get(key); found {
			return val, true
		}
	}
	return nil, false
}

func (db *FlashDB) Close() error {
	db.mu.Lock()
	defer db.mu.Unlock()

	db.wal.Close()
	for _, r := range db.sstReaders {
		r.Close()
	}
	return nil
}

func (db *FlashDB) Flush() error {
	db.mu.Lock()
	immutableMemtable := db.activeMemtable
	db.activeMemtable = memtable.NewSkipList()

	oldWAL := db.wal
	db.mu.Unlock()

	sstName := fmt.Sprintf("data_%d.sst", len(db.sstReaders)+1)
	sstPath := filepath.Join(db.dataDir, sstName)

	builder, err := sstable.NewBuilder(sstPath)
	if err != nil {
		return err
	}
	if err := builder.Flush(immutableMemtable); err != nil {
		return err
	}

	reader, err := sstable.NewReader(sstPath)
	if err != nil {
		return err
	}

	db.mu.Lock()
	db.sstReaders = append(db.sstReaders, reader)

	oldWAL.Close()
	os.Remove(filepath.Join(db.dataDir, "wal.log"))
	newWal, err := wal.NewWAL(filepath.Join(db.dataDir, "wal.log"))
	if err != nil {
		return err
	}
	db.wal = newWal
	db.mu.Unlock()

	return nil
}
