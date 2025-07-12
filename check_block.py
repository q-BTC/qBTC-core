#!/usr/bin/env python3
"""Check what block the bootstrap has at a specific height"""

import asyncio
import json
import sys

async def check_block_at_height(host, port, height):
    """Connect to a node and request a specific block by height"""
    try:
        print(f"Connecting to {host}:{port}...")
        reader, writer = await asyncio.open_connection(host, port)
        print("Connected!")
        
        # Request blocks at the specific height
        request = {
            "type": "get_blocks",
            "start_height": height,
            "end_height": height,
            "timestamp": 1234567890
        }
        
        print(f"Sending request: {json.dumps(request)}")
        writer.write((json.dumps(request) + "\n").encode('utf-8'))
        await writer.drain()
        print("Request sent, waiting for response...")
        
        # Read response
        response = await asyncio.wait_for(reader.readline(), timeout=30.0)
        if response:
            data = json.loads(response.decode('utf-8').strip())
            
            if data.get("type") == "blocks_response":
                blocks = data.get("blocks", [])
                if blocks:
                    block = blocks[0]
                    print(f"Block at height {height}:")
                    print(f"  Hash: {block.get('block_hash')}")
                    print(f"  Previous Hash: {block.get('previous_hash')}")
                    print(f"  Timestamp: {block.get('timestamp')}")
                    print(f"  Bits: {block.get('bits')}")
                    return block
                else:
                    print(f"No block found at height {height}")
            else:
                print(f"Unexpected response type: {data.get('type')}")
                print(f"Full response: {json.dumps(data, indent=2)}")
        
        writer.close()
        await writer.wait_closed()
        
    except Exception as e:
        print(f"Error: {e}")
        return None

async def main():
    # First check current height
    print("Checking bootstrap current height:")
    await check_height("api.bitcoinqs.org", 8002)
    
    # Check bootstrap
    print("\nChecking bootstrap (api.bitcoinqs.org:8002) block at height 7962:")
    await check_block_at_height("api.bitcoinqs.org", 8002, 7962)
    
    print("\nChecking bootstrap block at height 10828:")
    await check_block_at_height("api.bitcoinqs.org", 8002, 10828)

async def check_height(host, port):
    """Check current height of a node"""
    try:
        print(f"Connecting to {host}:{port}...")
        reader, writer = await asyncio.open_connection(host, port)
        print("Connected!")
        
        # Request height
        request = {
            "type": "get_height",
            "timestamp": 1234567890
        }
        
        print(f"Sending request: {json.dumps(request)}")
        writer.write((json.dumps(request) + "\n").encode('utf-8'))
        await writer.drain()
        print("Request sent, waiting for response...")
        
        # Read response
        response = await asyncio.wait_for(reader.readline(), timeout=5.0)
        if response:
            data = json.loads(response.decode('utf-8').strip())
            print(f"Response: {json.dumps(data, indent=2)}")
        else:
            print("No response received")
        
        writer.close()
        await writer.wait_closed()
        
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    asyncio.run(main())