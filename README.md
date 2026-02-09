# StrataGo

StrataGo is a high-performance, embeddable key-value storage engine implemented in Go, utilizing a Log-Structured Merge-Tree (LSM-Tree) architecture. It is designed to provide crash-resilient persistence and high write throughput.

## Architecture Components

### 1. Write-Ahead Log (WAL)

To ensure durability, every write operation is appended to a WAL before being applied to the in-memory state.

* **Storage Format**: Each entry is serialized as `[SequenceNumber(8B)][KeySize(4B)][ValueSize(4B)][Checksum(4B)][Key][Value]`.
* **Data Integrity**: Uses CRC32 (IEEE) checksums to detect data corruption or partial writes resulting from system crashes.
* **Recovery**: On initialization, the engine replays the WAL to reconstruct the Memtable state. It specifically handles `wal.log.flushing` to recover data from interrupted flush cycles.

### 2. Memtable Layers

StrataGo utilizes two in-memory layers to ensure continuous availability during disk synchronization.

* **Active Memtable**: A Skip List data structure that maintains sorted key-value pairs, providing O(log N) search and insertion complexity. It uses a `sync.RWMutex` for thread-safe concurrent access.
* **Immutable Memtable**: When the active memtable reaches the 4MB threshold (`DefaultMemtableThreshold`), it is frozen into this read-only layer. This ensures data remains visible to readers while the background flush to disk is in progress.

### 3. SSTable (Sorted String Table)

SSTables are immutable, disk-based files containing sorted key-value pairs.

* **Atomic Writes**: Implements a temp-rename pattern where data is written to a temporary file, synced to physical storage, and then atomically renamed to the final destination to prevent partial state transitions.
* **Tombstones**: Deletions are supported via tombstones, represented as 0-length values within the SSTable.

## Data Path Operations

### Write Path

1. The operation is appended to the WAL and flushed to disk via `file.Sync()`.
2. The entry is inserted into the Active Memtable.
3. If the Active Memtable's size exceeds 4MB, an automated background flush is triggered.
4. During a flush, the engine rotates the WAL by renaming `wal.log` to `wal.log.flushing`, ensuring new writes are directed to a fresh log while the old data is persisted to a new SSTable.

### Read Path

To maintain version consistency and account for logical deletes, the engine performs a hierarchical search:

1. **Active Memtable**: Checks the most recent in-memory writes.
2. **Immutable Memtable**: Checks data currently undergoing a flush.
3. **SSTables**: Performs a reverse-chronological search through disk-based files, returning the first match or stopping if a tombstone is encountered.

## Operational Safety

* **Crash Consistency**: The engine handles interrupted flushes by replaying `wal.log.flushing` files during startup. WAL checksums verify the integrity of each recovered record.
* **Concurrency Control**: StrataGo employs fine-grained locking and an immutable memory layer to allow background I/O without blocking incoming read or write requests.

## Development and Testing

The project includes a comprehensive suite of unit and integration tests:

* `memtable/`: Correctness and concurrency of the Skip List.
* `wal/`: Durability, checksum validation, and recovery logic.
* `sstable/`: Atomic builder patterns and reader accuracy.
* `StrataGo_test.go`: End-to-end integration, automatic flushing, and concurrent access tests.