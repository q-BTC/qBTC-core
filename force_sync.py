#!/usr/bin/env python3
"""
Force sync by manually adding bootstrap as peer
"""
import sys
import asyncio

# This needs to be run within the validator's Python environment
async def force_sync():
    # Import the gossip node from the running validator
    try:
        gossip_node = sys.modules['__main__'].gossip_node
        print(f"Found gossip node: {gossip_node.node_id}")
        
        # Manually add bootstrap as peer
        bootstrap_ip = "api.bitcoinqs.org"
        bootstrap_port = 8002
        
        print(f"Current DHT peers: {gossip_node.dht_peers}")
        print(f"Adding bootstrap node as peer: {bootstrap_ip}:{bootstrap_port}")
        
        # Add peer with info that indicates it's a bootstrap
        peer_info = {
            "ip": bootstrap_ip,
            "port": bootstrap_port,
            "nat_type": "direct",
            "supports_nat_traversal": False,
            "is_bootstrap": True
        }
        
        gossip_node.add_peer(bootstrap_ip, bootstrap_port, peer_info=peer_info)
        print(f"Updated DHT peers: {gossip_node.dht_peers}")
        
        # Trigger block sync
        from dht.dht import push_blocks
        print("Triggering block sync...")
        await push_blocks(bootstrap_ip, bootstrap_port)
        
    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    asyncio.run(force_sync())