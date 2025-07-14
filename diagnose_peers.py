#!/usr/bin/env python3
import asyncio
import json
import time
from typing import Dict, Set, Tuple
import socket

async def connect_to_node(host: str, port: int) -> Dict:
    """Connect to a node and get its status"""
    try:
        reader, writer = await asyncio.open_connection(host, port)
        
        # Send status request
        status_req = json.dumps({
            "type": "get_status",
            "timestamp": int(time.time() * 1000)
        }) + "\n"
        
        writer.write(status_req.encode())
        await writer.drain()
        
        # Read response
        response = await asyncio.wait_for(reader.readline(), timeout=5)
        if response:
            data = json.loads(response.decode().strip())
            writer.close()
            await writer.wait_closed()
            return data
        
        writer.close()
        await writer.wait_closed()
        return None
        
    except Exception as e:
        print(f"Failed to connect to {host}:{port}: {e}")
        return None

async def get_peer_info(host: str, port: int) -> Dict:
    """Get detailed peer information from a node"""
    try:
        reader, writer = await asyncio.open_connection(host, port)
        
        # Send peer info request
        peer_req = json.dumps({
            "type": "get_peers",
            "timestamp": int(time.time() * 1000)
        }) + "\n"
        
        writer.write(peer_req.encode())
        await writer.drain()
        
        # Read response
        response = await asyncio.wait_for(reader.readline(), timeout=5)
        if response:
            data = json.loads(response.decode().strip())
            writer.close()
            await writer.wait_closed()
            return data
        
        writer.close()
        await writer.wait_closed()
        return None
        
    except Exception as e:
        print(f"Failed to get peer info from {host}:{port}: {e}")
        return None

async def check_block_propagation(host: str, port: int) -> Dict:
    """Check the latest blocks on a node"""
    try:
        reader, writer = await asyncio.open_connection(host, port)
        
        # Request latest blocks
        blocks_req = json.dumps({
            "type": "get_blocks",
            "start_height": -10,  # Last 10 blocks
            "timestamp": int(time.time() * 1000)
        }) + "\n"
        
        writer.write(blocks_req.encode())
        await writer.drain()
        
        # Read response
        response = await asyncio.wait_for(reader.readline(), timeout=5)
        if response:
            data = json.loads(response.decode().strip())
            writer.close()
            await writer.wait_closed()
            return data
        
        writer.close()
        await writer.wait_closed()
        return None
        
    except Exception as e:
        print(f"Failed to get blocks from {host}:{port}: {e}")
        return None

async def main():
    """Main diagnostic function"""
    print("qBTC Network Diagnostics")
    print("=" * 50)
    
    # Check bootstrap node
    bootstrap_host = "localhost"
    bootstrap_port = 8002
    
    print(f"\n1. Checking Bootstrap Node ({bootstrap_host}:{bootstrap_port})...")
    status = await connect_to_node(bootstrap_host, bootstrap_port)
    if status and status.get("type") == "status_response":
        print(f"   - Height: {status.get('height', 'Unknown')}")
        print(f"   - Version: {status.get('version', 'Unknown')}")
        print(f"   - Node Type: Bootstrap")
    else:
        print("   - Bootstrap node not responding to status requests")
    
    # Check peer information
    print("\n2. Checking Peer Information...")
    peer_info = await get_peer_info(bootstrap_host, bootstrap_port)
    if peer_info and peer_info.get("type") == "peers_response":
        peers = peer_info.get("peers", [])
        print(f"   - Total peers reported: {len(peers)}")
        if peers:
            print("   - Peer list:")
            for peer in peers[:5]:  # Show first 5 peers
                print(f"     * {peer}")
    else:
        print("   - Could not retrieve peer information")
    
    # Check block propagation
    print("\n3. Checking Block Information...")
    blocks = await check_block_propagation(bootstrap_host, bootstrap_port)
    if blocks and blocks.get("type") == "blocks_response":
        block_list = blocks.get("blocks", [])
        print(f"   - Received {len(block_list)} blocks")
        if block_list:
            latest = block_list[-1]
            print(f"   - Latest block height: {latest.get('height', 'Unknown')}")
            print(f"   - Latest block hash: {latest.get('hash', 'Unknown')[:16]}...")
            print(f"   - Latest block timestamp: {latest.get('timestamp', 'Unknown')}")
    else:
        print("   - Could not retrieve block information")
    
    # Check actual network connections
    print("\n4. Checking Network Connections...")
    try:
        # Get all connections on gossip port
        import subprocess
        result = subprocess.run(
            ["ss", "-ant", f"sport = :{bootstrap_port}"],
            capture_output=True,
            text=True
        )
        
        lines = result.stdout.strip().split('\n')
        established_connections = [l for l in lines if 'ESTAB' in l]
        
        print(f"   - Established connections on port {bootstrap_port}: {len(established_connections)}")
        
        # Parse unique peer IPs
        peer_ips = set()
        for conn in established_connections:
            parts = conn.split()
            if len(parts) >= 5:
                peer_addr = parts[4]
                if ':' in peer_addr:
                    ip = peer_addr.rsplit(':', 1)[0]
                    if ip != '127.0.0.1' and not ip.startswith('[::'):
                        peer_ips.add(ip)
        
        print(f"   - Unique peer IPs connected: {len(peer_ips)}")
        if peer_ips:
            print("   - Peer IPs:")
            for ip in list(peer_ips)[:5]:  # Show first 5
                print(f"     * {ip}")
                
    except Exception as e:
        print(f"   - Failed to check network connections: {e}")
    
    print("\n" + "=" * 50)
    print("Diagnostic Summary:")
    print("- The peer count discrepancy might be due to:")
    print("  1. Multiple connections from same IP (temporary connections)")
    print("  2. client_peers vs dht_peers distinction")
    print("  3. Failed/disconnected peers not being cleaned up")
    print("\n- If validators aren't receiving blocks, check:")
    print("  1. Are validators actually running and connected?")
    print("  2. Is the broadcast reaching the right peers?")
    print("  3. Are there network/firewall issues?")

if __name__ == "__main__":
    asyncio.run(main())