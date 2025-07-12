#!/usr/bin/env python3
import subprocess
import json
import socket
import time

def get_block_from_peer(host, port, height):
    """Get a specific block from a peer"""
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(10)
        sock.connect((host, port))
        
        # Request specific block
        request = {
            "type": "get_blocks",
            "start_height": height,
            "end_height": height,
            "timestamp": int(time.time() * 1000)
        }
        
        sock.send((json.dumps(request) + "\n").encode('utf-8'))
        
        # Read response
        data = b""
        while b'\n' not in data:
            chunk = sock.recv(4096)
            if not chunk:
                break
            data += chunk
        
        sock.close()
        
        if data:
            response = json.loads(data.decode('utf-8').strip())
            if response.get("type") == "blocks_response" and response.get("blocks"):
                return response["blocks"][0]
    except Exception as e:
        print(f"Error getting block from {host}:{port}: {e}")
    
    return None

def main():
    print("=== Diagnosing Block 7962 Fork ===\n")
    
    # Check local validator's block 7962
    print("1. Checking LOCAL validator's block 7962:")
    try:
        # Get block hash at height 7962
        result = subprocess.run(
            ["docker", "exec", "qbtc-redis-validator", "redis-cli", "HGET", "block_height_index", "7962"],
            capture_output=True, text=True
        )
        local_hash = result.stdout.strip()
        
        if local_hash:
            print(f"   Hash: {local_hash}")
            
            # Get block details
            result = subprocess.run(
                ["docker", "exec", "qbtc-redis-validator", "redis-cli", "GET", f"block:{local_hash}"],
                capture_output=True, text=True
            )
            if result.stdout:
                block = json.loads(result.stdout)
                print(f"   Previous: {block.get('previous_hash', 'N/A')}")
                print(f"   Timestamp: {block.get('timestamp', 'N/A')}")
                print(f"   Miner: {block.get('miner_address', 'N/A')}")
        else:
            print("   No block found at height 7962")
    except Exception as e:
        print(f"   Error: {e}")
    
    print("\n2. Checking BOOTSTRAP node's block 7962:")
    bootstrap_block = get_block_from_peer("api.bitcoinqs.org", 8002, 7962)
    if bootstrap_block:
        print(f"   Hash: {bootstrap_block.get('block_hash', 'N/A')}")
        print(f"   Previous: {bootstrap_block.get('previous_hash', 'N/A')}")
        print(f"   Timestamp: {bootstrap_block.get('timestamp', 'N/A')}")
        print(f"   Miner: {bootstrap_block.get('miner_address', 'N/A')}")
    else:
        print("   Failed to get block from bootstrap")
    
    print("\n3. Analysis:")
    if local_hash and bootstrap_block:
        bootstrap_hash = bootstrap_block.get('block_hash', '')
        if local_hash == bootstrap_hash:
            print("   ✓ Blocks match - no fork at 7962")
        else:
            print("   ✗ FORK DETECTED - blocks don't match!")
            print(f"   Local:     {local_hash}")
            print(f"   Bootstrap: {bootstrap_hash}")
    
    # Check for orphans
    print("\n4. Checking for orphan blocks:")
    try:
        result = subprocess.run(
            ["docker", "exec", "qbtc-redis-validator", "redis-cli", "HKEYS", "orphan_blocks"],
            capture_output=True, text=True
        )
        orphans = result.stdout.strip().split('\n') if result.stdout.strip() else []
        print(f"   Found {len(orphans)} orphan blocks")
        
        if orphans:
            # Check a few orphan blocks
            for orphan_hash in orphans[:5]:
                result = subprocess.run(
                    ["docker", "exec", "qbtc-redis-validator", "redis-cli", "HGET", "orphan_blocks", orphan_hash],
                    capture_output=True, text=True
                )
                if result.stdout:
                    orphan = json.loads(result.stdout)
                    print(f"   - Height {orphan.get('height', 'N/A')}: {orphan_hash[:16]}... (needs parent: {orphan.get('previous_hash', 'N/A')[:16]}...)")
    except Exception as e:
        print(f"   Error checking orphans: {e}")

if __name__ == "__main__":
    main()