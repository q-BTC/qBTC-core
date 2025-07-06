#!/usr/bin/env python3
"""
Test DHT discovery between validator and bootstrap
"""
import asyncio
import json
import time
import urllib.request
import ssl

async def test_discovery():
    """Test if validator can discover bootstrap through DHT"""
    
    print("1. Checking local validator health...")
    try:
        with urllib.request.urlopen('http://localhost:8080/health') as resp:
            health_data = json.loads(resp.read().decode())
            print(f"Local validator health: {json.dumps(health_data, indent=2)}")
    except Exception as e:
        print(f"Error checking local health: {e}")
    
    print("\n2. Checking bootstrap health...")
    try:
        # Create SSL context that doesn't verify certificates
        ssl_context = ssl.create_default_context()
        ssl_context.check_hostname = False
        ssl_context.verify_mode = ssl.CERT_NONE
        
        with urllib.request.urlopen('https://api.bitcoinqs.org:8080/health', context=ssl_context) as resp:
            health_data = json.loads(resp.read().decode())
            print(f"Bootstrap health: {json.dumps(health_data, indent=2)}")
    except Exception as e:
        print(f"Error checking bootstrap health: {e}")
    
    print("\n3. Testing direct gossip connection to bootstrap...")
    try:
        reader, writer = await asyncio.open_connection('api.bitcoinqs.org', 8002)
        
        # Send get_height request
        height_request = {
            "type": "get_height",
            "timestamp": int(time.time() * 1000)
        }
        writer.write((json.dumps(height_request) + "\n").encode('utf-8'))
        await writer.drain()
        
        # Read response
        response = await asyncio.wait_for(reader.readline(), timeout=5)
        if response:
            height_data = json.loads(response.decode('utf-8').strip())
            print(f"Bootstrap gossip response: {height_data}")
        
        writer.close()
        await writer.wait_closed()
    except Exception as e:
        print(f"Error connecting to bootstrap gossip: {e}")
    
    print("\n4. Checking peer discovery...")
    try:
        # Check local validator's peers
        with urllib.request.urlopen('http://localhost:8080/peers') as resp:
            peers_data = json.loads(resp.read().decode())
            print(f"Local validator peers: {json.dumps(peers_data, indent=2)}")
    except Exception as e:
        print(f"Error checking peers: {e}")

if __name__ == "__main__":
    asyncio.run(test_discovery())