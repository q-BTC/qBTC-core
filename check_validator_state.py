#!/usr/bin/env python3
import time
import json
import subprocess

def check_validator_state():
    """Check the current state of the validator node"""
    
    print("Checking validator state...")
    
    # Check Redis for current height
    try:
        result = subprocess.run(
            ["docker", "exec", "qbtc-redis-validator", "redis-cli", "GET", "current_height"],
            capture_output=True, text=True
        )
        current_height = result.stdout.strip()
        print(f"Current height in Redis: {current_height if current_height else 'Not set'}")
    except Exception as e:
        print(f"Error checking Redis: {e}")
    
    # Check for specific blocks
    for height in range(7960, 7970):
        try:
            result = subprocess.run(
                ["docker", "exec", "qbtc-redis-validator", "redis-cli", "HGET", "block_height_index", str(height)],
                capture_output=True, text=True
            )
            block_hash = result.stdout.strip()
            if block_hash:
                print(f"Block {height}: Found (hash: {block_hash[:16]}...)")
            else:
                print(f"Block {height}: MISSING")
        except Exception as e:
            print(f"Error checking block {height}: {e}")
    
    # Check orphan blocks
    try:
        result = subprocess.run(
            ["docker", "exec", "qbtc-redis-validator", "redis-cli", "HLEN", "orphan_blocks"],
            capture_output=True, text=True
        )
        orphan_count = result.stdout.strip()
        print(f"\nOrphan blocks count: {orphan_count}")
    except Exception as e:
        print(f"Error checking orphan blocks: {e}")
    
    # Check container logs for sync activity
    try:
        result = subprocess.run(
            ["docker", "logs", "qbtc-validator", "--tail=50"],
            capture_output=True, text=True, stderr=subprocess.STDOUT
        )
        lines = result.stdout.split('\n')
        sync_lines = [l for l in lines if 'SYNC' in l or 'gap' in l or '7962' in l]
        if sync_lines:
            print("\nRecent sync activity:")
            for line in sync_lines[-10:]:
                print(line)
    except Exception as e:
        print(f"Error checking logs: {e}")

if __name__ == "__main__":
    check_validator_state()