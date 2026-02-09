package stratago

import (
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestStrataGo_Integration(t *testing.T) {
	dataDir := "test_data"
	defer os.RemoveAll(dataDir)

	// Test Open and Put
	db, err := Open(dataDir)
	assert.NoError(t, err)

	err = db.Put([]byte("key1"), []byte("value1"))
	assert.NoError(t, err)

	// Test Get from Memtable
	val, found := db.Get([]byte("key1"))
	assert.True(t, found)
	assert.Equal(t, []byte("value1"), val)

	// Test Flush
	err = db.Flush()
	assert.NoError(t, err)

	// Test Get from SSTable (Memtable is now empty)
	val, found = db.Get([]byte("key1"))
	assert.True(t, found)
	assert.Equal(t, []byte("value1"), val)

	// Test Recovery
	db.Close()
	db2, err := Open(dataDir)
	assert.NoError(t, err)
	defer db2.Close()

	val, found = db2.Get([]byte("key1"))
	assert.True(t, found)
	assert.Equal(t, []byte("value1"), val)
}

func TestStrataGo_ConcurrentAccess(t *testing.T) {
	dataDir := "concurrent_test"
	defer os.RemoveAll(dataDir)

	db, _ := Open(dataDir)
	defer db.Close()

	done := make(chan bool)

	// Background writer
	go func() {
		for range 100 {
			db.Put([]byte("key"), []byte("val"))
		}
		done <- true
	}()

	// Background flusher
	go func() {
		for range 5 {
			db.Flush()
		}
		done <- true
	}()

	<-done
	<-done

	_, found := db.Get([]byte("key"))
	assert.True(t, found)
}

func TestStrataGo_AutoFlush(t *testing.T) {
	dataDir := "test_autoflush"
	defer os.RemoveAll(dataDir)

	db, _ := Open(dataDir)
	defer db.Close()

	targetKey := []byte("target_key")
	value := make([]byte, 1024) // 1 KB

	db.Put(targetKey, value)

	for i := 0; i < 5000; i++ {
		loopKey := []byte(fmt.Sprintf("key-%d", i))
		db.Put(loopKey, value)
	}

	assert.Eventually(t, func() bool {
		db.mu.RLock()
		defer db.mu.RUnlock()
		return len(db.sstReaders) > 0
	}, 10*time.Second, 100*time.Millisecond)

	val, found := db.Get(targetKey)
	assert.True(t, found)
	assert.Equal(t, value, val)
}
