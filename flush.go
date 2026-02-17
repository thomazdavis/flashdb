package stratago

import (
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/thomazdavis/stratago/memtable"
	"github.com/thomazdavis/stratago/sstable"
	"github.com/thomazdavis/stratago/wal"
)

func (db *StrataGo) Flush() error {
	db.mu.Lock()

	if db.activeMemtable.Size == 0 {
		db.mu.Unlock()
		return nil
	}

	// Rotate Memtable and WAL
	db.immutableMemtable = db.activeMemtable
	db.activeMemtable = memtable.NewSkipList()

	newWal, err := wal.NewWAL(filepath.Join(db.dataDir, "wal.log"))
	if err != nil {
		db.mu.Unlock()
		return err
	}

	oldWAL := db.wal
	if err := oldWAL.Close(); err != nil {
		newWal.Close()
		db.mu.Unlock()
		return err
	}

	flushingWALPath := filepath.Join(db.dataDir, "wal.log.flushing")
	os.Rename(filepath.Join(db.dataDir, "wal.log"), flushingWALPath)

	db.wal = newWal
	db.mu.Unlock()

	sstName := fmt.Sprintf("data_%d.sst", time.Now().UnixNano())
	sstPath := filepath.Join(db.dataDir, sstName)

	builder, err := sstable.NewBuilder(sstPath)
	if err != nil {
		return db.handleFlushError(err)
	}

	if err := builder.Flush(db.immutableMemtable); err != nil {
		return db.handleFlushError(err)
	}

	reader, err := sstable.NewReader(sstPath)
	if err != nil {
		return db.handleFlushError(err)
	}

	db.mu.Lock()
	db.sstReaders = append(db.sstReaders, reader)
	db.immutableMemtable = nil
	db.mu.Unlock()

	// Verifying SSTable before deleting WAL
	verifyData, err := reader.ReadAll()
	if err != nil {
		return db.handleFlushError(fmt.Errorf("SSTable verification failed: %w", err))
	}
	if len(verifyData) != db.immutableMemtable.Size {
		return db.handleFlushError(fmt.Errorf("SSTable size mismatch"))
	}

	os.Remove(flushingWALPath)
	return nil
}

func (db *StrataGo) handleFlushError(err error) error {
	db.mu.Lock()
	db.mu.Unlock()
	return err
}
