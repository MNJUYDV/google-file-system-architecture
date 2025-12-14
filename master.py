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
        self.file_registry: Dict[str, FileMetadata] = {}  # filename -> FileMetadata
        self.chunk_metadata: Dict[str, ChunkMetadata] = {}  # chunk_id -> ChunkMetadata
        self.chunkservers: Dict[str, dict] = {}  # chunkserver_id -> info
        
        # Chunkserver heartbeats
        self.last_heartbeat: Dict[str, float] = {}
        self.heartbeat_monitoring_active = True
        
        # Start heartbeat monitor
        self.start_heartbeat_monitor()
    
    def start_heartbeat_monitor(self):
        """Start the background thread for monitoring chunkserver health"""
        self.heartbeat_monitor_thread = threading.Thread(target=self._monitor_chunkservers, daemon=True)
        self.heartbeat_monitor_thread.start()
    
    def register_chunkserver(self, chunkserver_id: str, chunks: List[str]):
        """Register a chunkserver and its chunks"""
        with self.lock:
            self.chunkservers[chunkserver_id] = {
                'id': chunkserver_id,
                'chunks': set(chunks)
            }
            self.last_heartbeat[chunkserver_id] = time.time()
            
            # Update chunk locations
            for chunk_id in chunks:
                if chunk_id in self.chunk_metadata:
                    self.chunk_metadata[chunk_id].locations.add(chunkserver_id)
            
            print(f"[Master] Registered chunkserver: {chunkserver_id}")
    
    def heartbeat(self, chunkserver_id: str):
        """Process heartbeat from chunkserver"""
        with self.lock:
            self.last_heartbeat[chunkserver_id] = time.time()
    
    def create_file(self, filename: str) -> bool:
        """Create a new file in the file_registry"""
        with self.lock:
            if filename in self.file_registry:
                return False
            
            self.file_registry[filename] = FileMetadata(filename=filename)
            print(f"[Master] Created file: {filename}")
            return True
    
    def _allocate_chunk(self) -> str:
        """Allocate a new chunk handle"""
        chunk_id = hashlib.sha256(
            f"{time.time()}{random.random()}".encode()
        ).hexdigest()[:16]
        return chunk_id
    
    def _select_chunkservers(self, count: int) -> List[str]:
        """Select chunkservers for chunk placement"""
        available = [cs_id for cs_id in self.chunkservers.keys() 
                    if time.time() - self.last_heartbeat.get(cs_id, 0) < 30]
        return random.sample(available, min(count, len(available)))
    
    def allocate_chunk_for_append(self, filename: str) -> Optional[Dict]:
        """Allocate a new chunk for append operation"""
        with self.lock:
            if filename not in self.file_registry:
                return None
            
            # Create new chunk
            chunk_id = self._allocate_chunk()
            locations = self._select_chunkservers(REPLICATION_FACTOR)
            
            if not locations:
                return None
            
            # Create chunk metadata
            self.chunk_metadata[chunk_id] = ChunkMetadata(
                chunk_id=chunk_id,
                version=1,
                locations=set(locations),
                primary=locations[0],  # First server becomes primary
                lease_expiry=time.time() + LEASE_TIMEOUT
            )
            
            # Add to file
            file_meta = self.file_registry[filename]
            file_meta.chunk_ids.append(chunk_id)
            
            print(f"[Master] Allocated chunk {chunk_id} for {filename}")
            print(f"[Master] Primary: {locations[0]}, Replicas: {locations}")
            
            return {
                'chunk_id': chunk_id,
                'locations': locations,
                'primary': locations[0],
                'version': 1
            }
    
    def get_chunk_locations(self, filename: str, chunk_index: int) -> Optional[Dict]:
        """Get locations for a specific chunk of a file"""
        with self.lock:
            if filename not in self.file_registry:
                return None
            
            file_meta = self.file_registry[filename]
            if chunk_index >= len(file_meta.chunk_ids):
                return None
            
            chunk_id = file_meta.chunk_ids[chunk_index]
            chunk_meta = self.chunk_metadata[chunk_id]
            
            return {
                'chunk_id': chunk_id,
                'locations': list(chunk_meta.locations),
                'primary': chunk_meta.primary,
                'version': chunk_meta.version
            }
    
    def get_file_info(self, filename: str) -> Optional[Dict]:
        """Get file metadata"""
        with self.lock:
            if filename not in self.file_registry:
                return None
            
            file_meta = self.file_registry[filename]
            return {
                'filename': filename,
                'num_chunks': len(file_meta.chunk_ids),
                'size': file_meta.size
            }
    
    def _monitor_chunkservers(self):
        """Monitor chunkserver health"""
        while self.heartbeat_monitoring_active:
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
    
    def shutdown_heartbeat_monitoring(self):
        """Shutdown heartbeat monitoring"""
        self.heartbeat_monitoring_active = False