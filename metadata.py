from typing import List, Set, Optional
from dataclasses import dataclass, field

@dataclass
class ChunkMetadata:
    """Metadata for a single chunk"""
    chunk_handle: str
    version: int
    locations: Set[str] = field(default_factory=set)  # Chunkserver IDs
    primary: Optional[str] = None
    lease_expiry: float = 0.0

@dataclass
class FileMetadata:
    """Metadata for a file"""
    filename: str
    chunk_handles: List[str] = field(default_factory=list)
    size: int = 0