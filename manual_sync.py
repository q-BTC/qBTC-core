#!/usr/bin/env python3
"""
Manual sync helper to force connection to bootstrap node
"""
import asyncio
import json
import time

async def manual_sync():
    """Manually connect to bootstrap and request genesis block"""
    bootstrap_ip = "api.bitcoinqs.org"
    bootstrap_port = 8002  # Gossip port
    
    print(f"Connecting to bootstrap at {bootstrap_ip}:{bootstrap_port}")
    
    try:
        # Connect to bootstrap
        reader, writer = await asyncio.open_connection(bootstrap_ip, bootstrap_port)
        print("Connected!")
        
        # First get height
        height_request = {
            "type": "get_height",
            "timestamp": int(time.time() * 1000)
        }
        
        writer.write((json.dumps(height_request) + "\n").encode('utf-8'))
        await writer.drain()
        
        response = await reader.readline()
        height_data = json.loads(response.decode('utf-8').strip())
        print(f"Bootstrap height: {height_data}")
        
        # Request genesis block (height 0)
        if height_data.get('height', -1) >= 0:
            blocks_request = {
                "type": "get_blocks",
                "start_height": 0,
                "end_height": 0,
                "timestamp": int(time.time() * 1000)
            }
            
            print("Requesting genesis block...")
            writer.write((json.dumps(blocks_request) + "\n").encode('utf-8'))
            await writer.drain()
            
            # Read response
            response = await reader.readline()
            blocks_data = json.loads(response.decode('utf-8').strip())
            print(f"Received response: {blocks_data.get('type')}")
            
            if blocks_data.get('type') == 'blocks_response':
                blocks = blocks_data.get('blocks', [])
                print(f"Received {len(blocks)} blocks")
                if blocks:
                    print(f"Genesis block hash: {blocks[0].get('block_hash')}")
                    print(f"Genesis block height: {blocks[0].get('height')}")
            
        writer.close()
        await writer.wait_closed()
        
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    asyncio.run(manual_sync())