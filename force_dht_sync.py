#!/usr/bin/env python3
"""
Force DHT sync by directly calling the DHT functions
"""
import asyncio
import json
import time

async def force_sync():
    """Force DHT registration and peer discovery"""
    
    print("Forcing DHT sync...")
    
    # Connect to local validator's gossip port and send a special message
    try:
        reader, writer = await asyncio.open_connection('localhost', 8002)
        
        # First check height
        height_req = {
            "type": "get_height",
            "timestamp": int(time.time() * 1000)
        }
        writer.write((json.dumps(height_req) + "\n").encode('utf-8'))
        await writer.drain()
        
        response = await reader.readline()
        height_data = json.loads(response.decode('utf-8').strip())
        print(f"Local height: {height_data}")
        
        writer.close()
        await writer.wait_closed()
        
    except Exception as e:
        print(f"Error connecting to local validator: {e}")
    
    # Now force a sync from bootstrap by sending blocks_response
    print("\nForcing sync from bootstrap...")
    try:
        # Get blocks from bootstrap first
        reader, writer = await asyncio.open_connection('api.bitcoinqs.org', 8002)
        
        get_blocks = {
            "type": "get_blocks",
            "start_height": 0,
            "end_height": 0,
            "timestamp": int(time.time() * 1000)
        }
        writer.write((json.dumps(get_blocks) + "\n").encode('utf-8'))
        await writer.drain()
        
        response = await reader.readline()
        blocks_data = json.loads(response.decode('utf-8').strip())
        
        writer.close()
        await writer.wait_closed()
        
        if blocks_data.get('type') == 'blocks_response' and blocks_data.get('blocks'):
            print(f"Got {len(blocks_data['blocks'])} blocks from bootstrap")
            
            # Send to local validator
            local_reader, local_writer = await asyncio.open_connection('localhost', 8002)
            local_writer.write((json.dumps(blocks_data) + "\n").encode('utf-8'))
            await local_writer.drain()
            local_writer.close()
            await local_writer.wait_closed()
            
            print("Blocks sent to local validator")
            
            # Wait a bit and check height again
            await asyncio.sleep(2)
            
            reader, writer = await asyncio.open_connection('localhost', 8002)
            writer.write((json.dumps(height_req) + "\n").encode('utf-8'))
            await writer.drain()
            
            response = await reader.readline()
            height_data = json.loads(response.decode('utf-8').strip())
            print(f"New local height: {height_data}")
            
            writer.close()
            await writer.wait_closed()
            
    except Exception as e:
        print(f"Error during sync: {e}")

if __name__ == "__main__":
    asyncio.run(force_sync())