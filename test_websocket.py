#!/usr/bin/env python3
"""Test websocket connection to api.bitcoinqs.org"""

import asyncio
import websockets
import json
import time
import ssl
import certifi

async def test_websocket():
    uri = "wss://api.bitcoinqs.org/ws"
    
    # Create SSL context
    ssl_context = ssl.create_default_context()
    ssl_context.check_hostname = False
    ssl_context.verify_mode = ssl.CERT_NONE
    
    print(f"Connecting to {uri}...")
    
    try:
        async with websockets.connect(uri, ssl=ssl_context, ping_interval=20, ping_timeout=10) as websocket:
            print("Connected successfully!")
            
            # Send a subscription message
            subscribe_msg = {
                "update_type": "all_transactions",
                "wallet_address": None
            }
            
            print(f"Sending subscription: {subscribe_msg}")
            await websocket.send(json.dumps(subscribe_msg))
            
            # Listen for messages
            start_time = time.time()
            while time.time() - start_time < 30:  # Listen for 30 seconds
                try:
                    message = await asyncio.wait_for(websocket.recv(), timeout=5.0)
                    data = json.loads(message)
                    print(f"Received: {json.dumps(data, indent=2)}")
                    
                    # Send ping to keep connection alive
                    if time.time() - start_time > 10:
                        ping_msg = {"type": "ping"}
                        await websocket.send(json.dumps(ping_msg))
                        print("Sent ping")
                        
                except asyncio.TimeoutError:
                    print("No message received in 5 seconds")
                except websockets.exceptions.ConnectionClosed as e:
                    print(f"Connection closed: {e}")
                    break
                except Exception as e:
                    print(f"Error: {e}")
                    
    except Exception as e:
        print(f"Connection failed: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    asyncio.run(test_websocket())