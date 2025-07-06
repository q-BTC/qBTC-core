#!/usr/bin/env python3
"""
Manually trigger sync from bootstrap
"""
import asyncio
import json
import time

async def trigger_sync():
    """Connect to local validator and trigger sync"""
    # First get our blockchain height
    import urllib.request
    try:
        with urllib.request.urlopen('http://localhost:8080/health') as resp:
            health_data = resp.read().decode()
            if '-1' in health_data:
                print("Local blockchain height is -1, need to sync")
            else:
                print(f"Local blockchain height: {health_data}")
    except Exception as e:
        print(f"Could not check local health: {e}")
    
    # Now connect to bootstrap and request blocks
    bootstrap_ip = "api.bitcoinqs.org"
    bootstrap_port = 8002
    
    print(f"Connecting to bootstrap at {bootstrap_ip}:{bootstrap_port}")
    
    try:
        reader, writer = await asyncio.open_connection(bootstrap_ip, bootstrap_port)
        print("Connected!")
        
        # Request blocks 0-0 (genesis)
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
                # Process blocks locally by sending to our validator
                print("Sending blocks to local validator for processing...")
                
                # Connect to local gossip port
                local_reader, local_writer = await asyncio.open_connection('localhost', 8002)
                
                # Send blocks_response to ourselves
                local_writer.write((json.dumps(blocks_data) + "\n").encode('utf-8'))
                await local_writer.drain()
                
                local_writer.close()
                await local_writer.wait_closed()
                print("Blocks sent to local validator")
        
        writer.close()
        await writer.wait_closed()
        
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    asyncio.run(trigger_sync())