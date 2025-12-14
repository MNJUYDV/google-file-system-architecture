import time
import threading
import hashlib
import random
from typing import Dict, List, Optional

from metadata import ChunkMetadata, FileMetadata
from config import REPLICATION_FACTOR, LEASE_TIMEOUT, HEARTBEAT_INTERVAL


class GFSMaster:
    """The Master server - coordinator and metadata manager"""
    
    def __init__(self):
        self.lock = threading.RLock()
        
        # Metadata stored in memory
        self.namespace: Dict[str, FileMetadata] = {}  # filename -> FileMetadata
        self.chunk_metadata: Dict[str, ChunkMetadata] = {}  # chunk_handle -> ChunkMetadata
        self.chunkservers: Dict[str, dict] = {}  # chunkserver_id -> info
        
        # Chunkserver heartbeats
        self.last_heartbeat: Dict[str, float] = {}
        
        # Start heartbeat monitor
        self.running = True
        self.monitor_thread = threading.Thread(target=self._monitor_chunkservers, daemon=True)
        self.monitor_thread.start()
    
    def register_chunkserver(self, chunkserver_id: str, chunks: List[str]):
        """Register a chunkserver and its chunks"""
        with self.lock:
            self.chunkservers[chunkserver_id] = {
                'id': chunkserver_id,
                'chunks': set(chunks)
            }
            self.last_heartbeat[chunkserver_id] = time.time()
            
            # Update chunk locations
            for chunk_handle in chunks:
                if chunk_handle in self.chunk_metadata:
                    self.chunk_metadata[chunk_handle].locations.add(chunkserver_id)
            
            print(f"[Master] Registered chunkserver: {chunkserver_id}")
    
    def heartbeat(self, chunkserver_id: str):
        """Process heartbeat from chunkserver"""
        with self.lock:
            self.last_heartbeat[chunkserver_id] = time.time()
    
    def create_file(self, filename: str) -> bool:
        """Create a new file in the namespace"""
        with self.lock:
            if filename in self.namespace:
                return False
            
            self.namespace[filename] = FileMetadata(filename=filename)
            print(f"[Master] Created file: {filename}")
            return True
    
    def _allocate_chunk(self) -> str:
        """Allocate a new chunk handle"""
        chunk_handle = hashlib.sha256(
            f"{time.time()}{random.random()}".encode()
        ).hexdigest()[:16]
        return chunk_handle
    
    def _select_chunkservers(self, count: int) -> List[str]:
        """Select chunkservers for chunk placement"""
        available = [cs_id for cs_id in self.chunkservers.keys() 
                    if time.time() - self.last_heartbeat.get(cs_id, 0) < 30]
        return random.sample(available, min(count, len(available)))
    
    def allocate_chunk_for_append(self, filename: str) -> Optional[Dict]:
        """Allocate a new chunk for append operation"""
        with self.lock:
            if filename not in self.namespace:
                return None
            
            # Create new chunk
            chunk_handle = self._allocate_chunk()
            locations = self._select_chunkservers(REPLICATION_FACTOR)
            
            if not locations:
                return None
            
            # Create chunk metadata
            self.chunk_metadata[chunk_handle] = ChunkMetadata(
                chunk_handle=chunk_handle,
                version=1,
                locations=set(locations),
                primary=locations[0],  # First server becomes primary
                lease_expiry=time.time() + LEASE_TIMEOUT
            )
            
            # Add to file
            file_meta = self.namespace[filename]
            file_meta.chunk_handles.append(chunk_handle)
            
            print(f"[Master] Allocated chunk {chunk_handle} for {filename}")
            print(f"[Master] Primary: {locations[0]}, Replicas: {locations}")
            
            return {
                'chunk_handle': chunk_handle,
                'locations': locations,
                'primary': locations[0],
                'version': 1
            }
    
    def get_chunk_locations(self, filename: str, chunk_index: int) -> Optional[Dict]:
        """Get locations for a specific chunk of a file"""
        with self.lock:
            if filename not in self.namespace:
                return None
            
            file_meta = self.namespace[filename]
            if chunk_index >= len(file_meta.chunk_handles):
                return None
            
            chunk_handle = file_meta.chunk_handles[chunk_index]
            chunk_meta = self.chunk_metadata[chunk_handle]
            
            return {
                'chunk_handle': chunk_handle,
                'locations': list(chunk_meta.locations),
                'primary': chunk_meta.primary,
                'version': chunk_meta.version
            }
    
    def get_file_info(self, filename: str) -> Optional[Dict]:
        """Get file metadata"""
        with self.lock:
            if filename not in self.namespace:
                return None
            
            file_meta = self.namespace[filename]
            return {
                'filename': filename,
                'num_chunks': len(file_meta.chunk_handles),
                'size': file_meta.size
            }
    
    def _monitor_chunkservers(self):
        """Monitor chunkserver health"""
        while self.running:
            time.sleep(HEARTBEAT_INTERVAL)
            with self.lock:
                current_time = time.time()
                dead_servers = []
                
                for cs_id, last_hb in self.last_heartbeat.items():
                    if current_time - last_hb > 30:
                        dead_servers.append(cs_id)
                
                for cs_id in dead_servers:
                    print(f"[Master] Chunkserver {cs_id} appears dead")
                    # In production: trigger re-replication
    
    def shutdown(self):
        """Shutdown the master"""
        self.running = False