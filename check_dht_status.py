#!/usr/bin/env python3
"""
Check DHT status and peer discovery
"""
import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

import asyncio
import json
from kademlia.network import Server as KademliaServer
from config.config import VALIDATORS_LIST_KEY

async def check_dht():
    """Check what's in the DHT"""
    # Create a temporary DHT client
    client = KademliaServer()
    await client.listen(9999)  # Use a different port
    
    # Bootstrap to the known nodes
    bootstrap_nodes = [
        ("api.bitcoinqs.org", 8001),  # Bootstrap DHT
        ("localhost", 8001)  # Local validator DHT
    ]
    
    for node in bootstrap_nodes:
        try:
            print(f"\nBootstrapping to {node}...")
            await client.bootstrap([node])
            await asyncio.sleep(2)
            
            neighbors = client.bootstrappable_neighbors()
            print(f"Found {len(neighbors)} neighbors")
            
            # Try to get validator list
            validators_json = await client.get(VALIDATORS_LIST_KEY)
            if validators_json:
                validators = json.loads(validators_json)
                print(f"Validators in DHT: {validators}")
                
                # Check gossip info for each validator
                for vid in validators:
                    gossip_key = f"gossip_{vid}"
                    gossip_info = await client.get(gossip_key)
                    if gossip_info:
                        info = json.loads(gossip_info)
                        print(f"  Validator {vid}: {info}")
                    else:
                        print(f"  Validator {vid}: No gossip info")
            else:
                print("No validator list found in DHT")
                
        except Exception as e:
            print(f"Error bootstrapping to {node}: {e}")
    
    client.stop()

if __name__ == "__main__":
    asyncio.run(check_dht())