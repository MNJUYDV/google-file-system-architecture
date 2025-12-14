import time

from master import GFSMaster
from chunkserver import GFSChunkserver
from client import GFSClient


if __name__ == "__main__":
    print("=== GFS Implementation Demo ===\n")
    
    # Initialize GFS cluster
    master = GFSMaster()
    time.sleep(0.1)  # Let master initialize
    
    # Create chunkservers
    cs1 = GFSChunkserver("chunkserver-1", master)
    cs2 = GFSChunkserver("chunkserver-2", master)
    cs3 = GFSChunkserver("chunkserver-3", master)
    
    chunkservers = {
        "chunkserver-1": cs1,
        "chunkserver-2": cs2,
        "chunkserver-3": cs3
    }
    
    time.sleep(0.5)  # Let heartbeats establish
    
    # Create client
    client = GFSClient(master, chunkservers)
    
    # Create a file
    print("\n--- Creating file ---")
    client.create("/data/logs.txt")
    
    # Append data
    print("\n--- Appending data ---")
    client.append("/data/logs.txt", b"First log entry\n")
    client.append("/data/logs.txt", b"Second log entry\n")
    client.append("/data/logs.txt", b"Third log entry\n")
    
    # Read file
    print("\n--- Reading file ---")
    data = client.read("/data/logs.txt")
    if data:
        print(f"File contents:\n{data.decode()}")
    
    # Cleanup
    print("\n--- Shutting down ---")
    master.shutdown()
    cs1.shutdown()
    cs2.shutdown()
    cs3.shutdown()
    
    print("Demo complete!")