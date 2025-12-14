import time
import threading
from typing import Dict, Optional

from config import HEARTBEAT_INTERVAL
from master import GFSMaster


class GFSChunkserver:
    """The Chunkserver - stores actual data"""
    
    def __init__(self, chunkserver_id: str, master: GFSMaster):
        self.chunkserver_id = chunkserver_id
        self.master = master
        self.chunks: Dict[str, bytearray] = {}  # chunk_id -> data
        self.chunk_versions: Dict[str, int] = {}  # chunk_id -> version
        self.lock = threading.RLock()
        
        # Register with master
        self.master.register_chunkserver(self.chunkserver_id, list(self.chunks.keys()))
        
        # Start heartbeat
        self.running = True
        self.heartbeat_thread = threading.Thread(target=self._send_heartbeats, daemon=True)
        self.heartbeat_thread.start()
    
    def _send_heartbeats(self):
        """Send periodic heartbeats to master"""
        while self.running:
            self.master.heartbeat(self.chunkserver_id)
            time.sleep(HEARTBEAT_INTERVAL)
    
    def create_chunk(self, chunk_id: str, version: int):
        """Create a new chunk"""
        with self.lock:
            self.chunks[chunk_id] = bytearray()
            self.chunk_versions[chunk_id] = version
            print(f"[{self.chunkserver_id}] Created chunk: {chunk_id}")
    
    def append_data(self, chunk_id: str, data: bytes, offset: int) -> bool:
        """Append data to a chunk at specified offset"""
        with self.lock:
            if chunk_id not in self.chunks:
                return False
            
            chunk = self.chunks[chunk_id]
            
            # Extend chunk if necessary
            if offset > len(chunk):
                chunk.extend(b'\x00' * (offset - len(chunk)))
            
            # Append data
            if offset == len(chunk):
                chunk.extend(data)
            else:
                chunk[offset:offset+len(data)] = data
            
            print(f"[{self.chunkserver_id}] Appended {len(data)} bytes to {chunk_id} at offset {offset}")
            return True
    
    def read_data(self, chunk_id: str, offset: int, length: int) -> Optional[bytes]:
        """Read data from a chunk"""
        with self.lock:
            if chunk_id not in self.chunks:
                return None
            
            chunk = self.chunks[chunk_id]
            end = min(offset + length, len(chunk))
            return bytes(chunk[offset:end])
    
    def shutdown(self):
        """Shutdown the chunkserver"""
        self.running = False