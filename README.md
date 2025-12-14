# Google File System (GFS) Architecture

A Python implementation of the Google File System (GFS) architecture, demonstrating the core concepts of distributed file storage including master-chunkserver coordination, chunk replication, and primary-secondary lease management.

## Overview

This project implements a simplified version of Google's distributed file system, which was designed for large-scale data processing workloads. The architecture consists of three main components:

1. **Master Server** - Manages metadata, coordinates operations, and handles chunk allocation
2. **Chunkservers** - Store actual data chunks with replication
3. **Client** - Provides the application interface for file operations

## Architecture

### Components

#### Master Server (`master.py`)
- Manages the file_registry (file and directory metadata)
- Tracks chunk locations across chunkservers
- Allocates chunks and selects replica locations
- Monitors chunkserver health via heartbeats
- Manages primary lease assignments for write operations

#### Chunkserver (`chunkserver.py`)
- Stores chunks (fixed-size data blocks)
- Responds to read/write requests from clients
- Sends periodic heartbeats to the master
- Handles chunk creation, appends, and reads

#### Client (`client.py`)
- Provides high-level file operations (create, append, read)
- Communicates with master for metadata operations
- Interacts with chunkservers for data operations
- Implements caching for improved performance

#### Metadata (`metadata.py`)
- `FileMetadata`: Tracks file information including chunk handles
- `ChunkMetadata`: Manages chunk version, locations, and lease information

### Key Features

- **Chunk Replication**: Each chunk is replicated across multiple chunkservers (default: 3 replicas)
- **Primary-Secondary Model**: Write operations go through a primary chunkserver that coordinates replication
- **Heartbeat Monitoring**: Master monitors chunkserver health and detects failures
- **Chunk Allocation**: Master allocates new chunks and selects replica locations
- **Record Append**: Supports append operations with automatic chunk allocation

## Configuration

The system can be configured via `config.py`:

- `CHUNK_SIZE`: Size of each chunk (default: 64 MB)
- `REPLICATION_FACTOR`: Number of replicas per chunk (default: 3)
- `LEASE_TIMEOUT`: Primary lease duration in seconds (default: 60)
- `HEARTBEAT_INTERVAL`: Heartbeat frequency in seconds (default: 10)

## Usage

### Running the Demo

```bash
python demo.py
```

The demo script demonstrates:
1. Initializing a GFS cluster with 1 master and 3 chunkservers
2. Creating a file
3. Appending data to the file
4. Reading the file back

### Programmatic Usage

```python
from master import GFSMaster
from chunkserver import GFSChunkserver
from client import GFSClient

# Initialize master
master = GFSMaster()

# Create chunkservers
cs1 = GFSChunkserver("chunkserver-1", master)
cs2 = GFSChunkserver("chunkserver-2", master)
cs3 = GFSChunkserver("chunkserver-3", master)

chunkservers = {
    "chunkserver-1": cs1,
    "chunkserver-2": cs2,
    "chunkserver-3": cs3
}

# Create client
client = GFSClient(master, chunkservers)

# Create a file
client.create("/data/logs.txt")

# Append data
client.append("/data/logs.txt", b"Log entry 1\n")
client.append("/data/logs.txt", b"Log entry 2\n")

# Read file
data = client.read("/data/logs.txt")
print(data.decode())
```

## File Operations

### Create File
Creates a new file in the file_registry.

```python
client.create("/path/to/file.txt")
```

### Append Data
Appends data to a file. If the current chunk is full, a new chunk is automatically allocated.

```python
client.append("/path/to/file.txt", b"data to append")
```

### Read File
Reads the entire file by reading all chunks sequentially.

```python
data = client.read("/path/to/file.txt")
```

## Implementation Details

### Write Path (Append Operation)
1. Client requests chunk allocation from master
2. Master allocates new chunk and selects replica locations (primary + secondaries)
3. Client creates chunk on all replicas
4. Client writes data to primary chunkserver
5. Primary replicates data to secondary chunkservers
6. Client confirms successful write

### Read Path
1. Client requests chunk locations from master
2. Client reads from any available replica (chunkservers are read from in order)
3. Data from all chunks is concatenated and returned

### Chunkserver Monitoring
- Master monitors chunkservers via periodic heartbeats
- Chunkservers send heartbeats every 10 seconds (configurable)
- Chunkservers not heard from for 30 seconds are considered dead
- Dead chunkservers trigger re-replication (placeholder in current implementation)

## Limitations

This is a simplified educational implementation. Production GFS would include:

- Persistent storage (currently in-memory)
- Fault tolerance and recovery mechanisms
- Snapshot and file_registry management
- Concurrent write handling with conflict resolution
- Network communication layer (currently in-process)
- Authorization and authentication
- Garbage collection of orphaned chunks

## Requirements

- Python 3.7+
- No external dependencies (uses only standard library)

## Project Structure

```
gfs-architecture/
├── master.py          # Master server implementation
├── chunkserver.py     # Chunkserver implementation
├── client.py          # Client interface
├── metadata.py        # Metadata data structures
├── config.py          # Configuration settings
├── demo.py            # Demonstration script
└── README.md          # This file
```

## License

This project is provided as-is for educational purposes.

## References

- [The Google File System](https://static.googleusercontent.com/media/research.google.com/en//archive/gfs-sosp2003.pdf) - Original GFS paper by Ghemawat, Gobioff, and Leung (2003)
