from typing import Dict, Optional

from config import CHUNK_SIZE
from master import GFSMaster
from chunkserver import GFSChunkserver


class GFSClient:
    """The Client - application interface to GFS"""
    
    def __init__(self, master: GFSMaster, chunkservers: Dict[str, GFSChunkserver]):
        self.master = master
        self.chunkservers = chunkservers
        self.metadata_cache: Dict[str, dict] = {}  # Simple cache
    
    def create(self, filename: str) -> bool:
        """Create a new file"""
        return self.master.create_file(filename)
    
    def append(self, filename: str, data: bytes) -> bool:
        """Append data to a file (record append)"""
        # Get or allocate chunk
        chunk_info = self.master.allocate_chunk_for_append(filename)
        if not chunk_info:
            print(f"[Client] Failed to allocate chunk for {filename}")
            return False
        
        chunk_handle = chunk_info['chunk_handle']
        primary_id = chunk_info['primary']
        locations = chunk_info['locations']
        version = chunk_info['version']
        
        # Create chunk on all replicas
        for cs_id in locations:
            if cs_id in self.chunkservers:
                self.chunkservers[cs_id].create_chunk(chunk_handle, version)
        
        # Get primary chunkserver
        primary = self.chunkservers.get(primary_id)
        if not primary:
            print(f"[Client] Primary chunkserver {primary_id} not available")
            return False
        
        # Primary determines the offset (append point)
        offset = 0  # New chunk starts at 0
        
        # Primary writes data
        if not primary.append_data(chunk_handle, data, offset):
            return False
        
        # Replicate to secondaries
        for cs_id in locations:
            if cs_id != primary_id and cs_id in self.chunkservers:
                self.chunkservers[cs_id].append_data(chunk_handle, data, offset)
        
        print(f"[Client] Successfully appended {len(data)} bytes to {filename}")
        return True
    
    def read(self, filename: str) -> Optional[bytes]:
        """Read entire file"""
        file_info = self.master.get_file_info(filename)
        if not file_info:
            print(f"[Client] File not found: {filename}")
            return None
        
        result = bytearray()
        num_chunks = file_info['num_chunks']
        
        # Read all chunks sequentially
        for chunk_idx in range(num_chunks):
            chunk_info = self.master.get_chunk_locations(filename, chunk_idx)
            if not chunk_info:
                continue
            
            chunk_handle = chunk_info['chunk_handle']
            locations = chunk_info['locations']
            
            # Try to read from any replica
            for cs_id in locations:
                if cs_id in self.chunkservers:
                    data = self.chunkservers[cs_id].read_data(chunk_handle, 0, CHUNK_SIZE)
                    if data:
                        result.extend(data)
                        break
        
        print(f"[Client] Read {len(result)} bytes from {filename}")
        return bytes(result)