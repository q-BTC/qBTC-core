#!/usr/bin/env python3
"""
Test automatic peer discovery without running full validator
"""
import asyncio
import json
import time
import sys
import os

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from dht.dht import run_kad_server, discover_peers_once
from gossip.gossip import GossipNode
from config.config import VALIDATOR_ID
from log_utils import get_logger

logger = get_logger(__name__)

async def test_auto_discovery():
    """Test automatic peer discovery"""
    
    print(f"Starting test with validator ID: {VALIDATOR_ID}")
    
    # Create a gossip node
    gossip_node = GossipNode(VALIDATOR_ID)
    
    # Start gossip server
    await gossip_node.start_server(port=8002)
    print("Gossip server started on port 8002")
    
    # Create a minimal wallet for DHT
    wallet = {"publicKey": "test_public_key"}
    
    # Start DHT with bootstrap
    print("Starting DHT and connecting to bootstrap...")
    bootstrap_nodes = [("api.bitcoinqs.org", 8001)]
    
    # Start the DHT server
    dht_task = asyncio.create_task(
        run_kad_server(
            port=8001,
            bootstrap_addr=bootstrap_nodes,
            wallet=wallet,
            gossip_node=gossip_node,
            ip_address="18.196.115.171",
            gossip_port=8002
        )
    )
    
    # Wait for DHT to initialize
    await asyncio.sleep(10)
    
    # Check peers periodically
    for i in range(6):  # Check for 30 seconds
        print(f"\n--- Check {i+1} ---")
        print(f"DHT peers: {gossip_node.dht_peers}")
        print(f"Client peers: {gossip_node.client_peers}")
        print(f"Failed peers: {gossip_node.failed_peers}")
        await asyncio.sleep(5)
    
    # Cancel DHT task
    dht_task.cancel()
    try:
        await dht_task
    except asyncio.CancelledError:
        pass
    
    # Stop gossip server
    await gossip_node.stop()

if __name__ == "__main__":
    asyncio.run(test_auto_discovery())